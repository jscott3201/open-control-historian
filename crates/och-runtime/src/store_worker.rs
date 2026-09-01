use crate::ingress::{
    AdmissionPriority, AppendIdentity, BarrierDemand, ByteReservationLimits, DurableBatchEntry,
    IngressShared, MAX_OUTSTANDING_COMMANDS,
};
use och_core::{
    CanonicalAdmission, CollectionEnvelope, DeclarationEvidence, DeclarationRevision,
    DeclaredCollectionEnvelope, SeriesBinding, SeriesDeclaration, SeriesDeclarationPayload,
    SeriesId, SeriesRetirement, StoreId,
};
use och_store::{
    ActiveJournalError, ActiveJournalInspection, ActiveJournalLimits, ActiveJournalOpenMode,
    GenerationInventory, ManifestCommit, ManifestIoEvidence, ManifestIoOperation, ManifestStore,
    ManifestStoreConfig, ManifestStoreError, ManifestStoreInspection, PendingRetryOutcome,
    PreparedAdmissionV1, RecoveredAdmissionV1, RecoveryReport, RegistryPersistenceOptions,
    RetryPersistenceOptions, RetryStateSnapshot, StoreIoEvidence, StoreIoOperation,
    StoreWriteState,
};
use std::fmt;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Validated bounded group-commit and rotation-demand policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupCommitPolicy {
    max_delay: Duration,
    max_records: usize,
    max_bytes: usize,
    rotation_age: Duration,
}

impl GroupCommitPolicy {
    /// Validates nonzero finite group and active-age bounds.
    ///
    /// # Errors
    ///
    /// Refuses zero delay/count/bytes/age or a count above the ingress window.
    pub const fn new(
        max_delay: Duration,
        max_records: usize,
        max_bytes: usize,
        rotation_age: Duration,
    ) -> Result<Self, StoreOptionsError> {
        if max_delay.is_zero()
            || max_records == 0
            || max_records > MAX_OUTSTANDING_COMMANDS
            || max_bytes == 0
            || rotation_age.is_zero()
        {
            return Err(StoreOptionsError::InvalidRelationships);
        }
        Ok(Self {
            max_delay,
            max_records,
            max_bytes,
            rotation_age,
        })
    }

    /// Returns maximum delay from first handled pending append to barrier.
    #[must_use]
    pub const fn max_delay(self) -> Duration {
        self.max_delay
    }

    /// Returns maximum pending records before a barrier.
    #[must_use]
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    /// Returns maximum pending exact encoded bytes before a barrier.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns nonempty active-generation age that demands safe-boundary rotation.
    #[must_use]
    pub const fn rotation_age(self) -> Duration {
        self.rotation_age
    }
}

/// Fully validated filesystem-backed runtime open options.
pub struct StoreOptions {
    directory: PathBuf,
    store_id: StoreId,
    mode: ActiveJournalOpenMode,
    journal_limits: ActiveJournalLimits,
    byte_limits: ByteReservationLimits,
    group_commit: GroupCommitPolicy,
    registry: RegistryPersistenceOptions,
    retry: RetryPersistenceOptions,
    #[cfg(feature = "m03-pr03e-native-harness")]
    evidence_session: Option<och_store::__m03_pr03e_native_harness::NativeEvidenceSession>,
    #[cfg(test)]
    cleanup_on_reap: bool,
    #[cfg(test)]
    pressure_hook: Option<TestPressureHook>,
}

impl StoreOptions {
    /// Validates all bounded relationships before worker or filesystem activity.
    ///
    /// # Errors
    ///
    /// Refuses invalid directory/options or group bytes above global reservations.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        directory: PathBuf,
        store_id: StoreId,
        mode: ActiveJournalOpenMode,
        journal_limits: ActiveJournalLimits,
        byte_limits: ByteReservationLimits,
        group_commit: GroupCommitPolicy,
        registry: RegistryPersistenceOptions,
        retry: RetryPersistenceOptions,
    ) -> Result<Self, StoreOptionsError> {
        let directory_length = directory.as_os_str().as_encoded_bytes().len();
        if directory_length == 0 || directory_length > och_store::MAX_STORE_DIRECTORY_BYTES {
            return Err(StoreOptionsError::Store(ManifestStoreError::InvalidOptions));
        }
        ManifestStoreConfig::new(
            directory.clone(),
            store_id,
            mode,
            journal_limits,
            registry.clone(),
            retry,
        )
        .map_err(StoreOptionsError::Store)?;
        if group_commit.max_bytes > byte_limits.max_outstanding_bytes() {
            return Err(StoreOptionsError::InvalidRelationships);
        }
        Ok(Self {
            directory,
            store_id,
            mode,
            journal_limits,
            byte_limits,
            group_commit,
            registry,
            retry,
            #[cfg(feature = "m03-pr03e-native-harness")]
            evidence_session: None,
            #[cfg(test)]
            cleanup_on_reap: false,
            #[cfg(test)]
            pressure_hook: None,
        })
    }

    /// Returns the immutable store scope.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns create-new or open-existing mode.
    #[must_use]
    pub const fn mode(&self) -> ActiveJournalOpenMode {
        self.mode
    }

    /// Returns active-journal bounds.
    #[must_use]
    pub const fn journal_limits(&self) -> ActiveJournalLimits {
        self.journal_limits
    }

    /// Returns outstanding byte-reservation bounds.
    #[must_use]
    pub const fn byte_limits(&self) -> ByteReservationLimits {
        self.byte_limits
    }

    /// Returns group-commit and age policy.
    #[must_use]
    pub const fn group_commit(&self) -> GroupCommitPolicy {
        self.group_commit
    }

    /// Returns bounded canonical registry persistence options.
    #[must_use]
    pub const fn registry(&self) -> &RegistryPersistenceOptions {
        &self.registry
    }

    /// Returns bounded durable retry persistence options.
    #[must_use]
    pub const fn retry(&self) -> RetryPersistenceOptions {
        self.retry
    }

    /// Attaches one unsupported temporary native evidence session.
    ///
    /// This rustdoc-hidden feature-only builder is not a product activation or
    /// extension point. It is available only under `m03-pr03e-native-harness`.
    #[cfg(feature = "m03-pr03e-native-harness")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_native_evidence_session(
        mut self,
        session: crate::__m03_pr03e_native_harness::NativeEvidenceSession,
    ) -> Self {
        self.evidence_session = Some(session.into_store_session());
        self
    }

    #[cfg(feature = "m03-pr03e-native-harness")]
    pub(crate) fn evidence_session(
        &self,
    ) -> Option<och_store::__m03_pr03e_native_harness::NativeEvidenceSession> {
        self.evidence_session.clone()
    }

    pub(crate) fn manifest_config(&self) -> Result<ManifestStoreConfig, ManifestStoreError> {
        ManifestStoreConfig::new(
            self.directory.clone(),
            self.store_id,
            self.mode,
            self.journal_limits,
            self.registry.clone(),
            self.retry,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_test_cleanup(mut self) -> Self {
        self.cleanup_on_reap = true;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_pressure(mut self, hook: TestPressureHook) -> Self {
        self.pressure_hook = Some(hook);
        self
    }

    #[cfg(test)]
    fn take_test_pressure(&mut self, boundary: TestPressureBoundary) -> Option<TestPressureHook> {
        let hook = self.pressure_hook.as_mut()?;
        if hook.boundary != boundary {
            return None;
        }
        if hook.skip > 0 {
            hook.skip -= 1;
            return None;
        }
        self.pressure_hook.take()
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestPressureBoundary {
    Append,
    Flush,
    Registry,
}

#[cfg(test)]
pub(crate) struct TestPressureHook {
    boundary: TestPressureBoundary,
    skip: usize,
    evidence: RuntimePressureEvidence,
    fired: Arc<AtomicBool>,
    release: Option<Receiver<()>>,
}

#[cfg(test)]
pub(crate) struct TestPressureControl {
    fired: Arc<AtomicBool>,
    release: Option<SyncSender<()>>,
}

#[cfg(test)]
impl TestPressureHook {
    pub(crate) fn new(
        boundary: TestPressureBoundary,
        skip: usize,
        evidence: RuntimePressureEvidence,
        hold_after_response: bool,
    ) -> (Self, TestPressureControl) {
        let fired = Arc::new(AtomicBool::new(false));
        let (release, wait) = std::sync::mpsc::sync_channel(0);
        (
            Self {
                boundary,
                skip,
                evidence,
                fired: Arc::clone(&fired),
                release: hold_after_response.then_some(wait),
            },
            TestPressureControl {
                fired,
                release: hold_after_response.then_some(release),
            },
        )
    }

    fn after_response(self) {
        self.fired.store(true, Ordering::Release);
        if let Some(release) = self.release {
            let _ = release.recv();
        }
    }
}

#[cfg(test)]
impl TestPressureControl {
    pub(crate) fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    pub(crate) fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl fmt::Debug for StoreOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreOptions")
            .field("store_id", &self.store_id)
            .field("mode", &self.mode)
            .field("journal_limits", &self.journal_limits)
            .field("byte_limits", &self.byte_limits)
            .field("group_commit", &self.group_commit)
            .field("registry", &self.registry)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

/// Sanitized invalid runtime options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOptionsError {
    /// Manifest-rooted store options were invalid.
    Store(ManifestStoreError),
    /// Cross-option bounds were invalid.
    InvalidRelationships,
}

impl fmt::Display for StoreOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid historian store options")
    }
}

impl std::error::Error for StoreOptionsError {}

/// One canonical registry lifecycle operation serialized by the sole store writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryOperation {
    /// Register initial declaration revision one.
    Register {
        /// Stable new series identity.
        series_id: SeriesId,
        /// Immutable logical-point binding.
        binding: SeriesBinding,
        /// Initial revisionable interpretation payload.
        payload: SeriesDeclarationPayload,
        /// Initial declaration evidence.
        evidence: DeclarationEvidence,
    },
    /// Append one metadata correction revision.
    Revise {
        /// Existing series identity.
        series_id: SeriesId,
        /// Exact currently expected revision.
        expected_revision: DeclarationRevision,
        /// Corrected revisionable payload.
        payload: SeriesDeclarationPayload,
        /// Correction evidence.
        evidence: DeclarationEvidence,
    },
    /// Terminally retire one series.
    Retire {
        /// Existing series identity.
        series_id: SeriesId,
        /// Exact final active revision.
        expected_revision: DeclarationRevision,
        /// Retirement evidence.
        evidence: DeclarationEvidence,
    },
}

/// Canonical result of one committed registry lifecycle operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryOutcome {
    /// Initial or revised immutable declaration.
    Declaration(Box<SeriesDeclaration>),
    /// Terminal immutable retirement tombstone.
    Retirement(SeriesRetirement),
}

/// Manifest-backed registry lifecycle success.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryCommit {
    outcome: RegistryOutcome,
    committed: ManifestCommit,
}

impl RegistryCommit {
    /// Returns the exact core lifecycle outcome.
    #[must_use]
    pub const fn outcome(&self) -> &RegistryOutcome {
        &self.outcome
    }

    /// Returns the manifest state committed before this result was reported.
    #[must_use]
    pub const fn manifest_commit(&self) -> ManifestCommit {
        self.committed
    }
}

/// Typed registry-control refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// The fixed nonblocking lifecycle/bind admission bound is full.
    Capacity,
    /// The runtime or its sole writer is closed.
    Closed,
    /// Canonical or durable store authority refused the operation.
    Store(ManifestStoreError),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Capacity => "historian registry control capacity is full",
            Self::Closed => "historian registry control is closed",
            Self::Store(_) => "historian registry operation refused",
        })
    }
}

impl std::error::Error for RegistryError {}

/// Store-owned family that reported runtime storage pressure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePressureSource {
    /// The active journal or its mechanical checkpoint reported pressure.
    ActiveJournal,
    /// The composed manifest, registry, retry, catalog, or recovery layer reported pressure.
    ManifestStore,
}

/// Existing store operation attached to runtime pressure evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePressureOperation {
    /// Active-journal operation evidence.
    ActiveJournal(StoreIoOperation),
    /// Composed manifest-store operation evidence.
    ManifestStore(ManifestIoOperation),
}

/// Bounded path- and content-free evidence for the first runtime pressure event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimePressureEvidence {
    source: RuntimePressureSource,
    operation: RuntimePressureOperation,
    kind: ErrorKind,
    raw_os_error: Option<i32>,
}

impl RuntimePressureEvidence {
    const fn from_active(evidence: StoreIoEvidence) -> Self {
        Self {
            source: RuntimePressureSource::ActiveJournal,
            operation: RuntimePressureOperation::ActiveJournal(evidence.operation()),
            kind: evidence.kind(),
            raw_os_error: evidence.raw_os_error(),
        }
    }

    const fn from_manifest(evidence: ManifestIoEvidence) -> Self {
        Self {
            source: RuntimePressureSource::ManifestStore,
            operation: RuntimePressureOperation::ManifestStore(evidence.operation()),
            kind: evidence.kind(),
            raw_os_error: evidence.raw_os_error(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn from_active_parts(
        operation: StoreIoOperation,
        kind: ErrorKind,
        raw_os_error: Option<i32>,
    ) -> Self {
        Self {
            source: RuntimePressureSource::ActiveJournal,
            operation: RuntimePressureOperation::ActiveJournal(operation),
            kind,
            raw_os_error,
        }
    }

    #[cfg(test)]
    pub(crate) const fn from_manifest_parts(
        operation: ManifestIoOperation,
        kind: ErrorKind,
        raw_os_error: Option<i32>,
    ) -> Self {
        Self {
            source: RuntimePressureSource::ManifestStore,
            operation: RuntimePressureOperation::ManifestStore(operation),
            kind,
            raw_os_error,
        }
    }

    pub(crate) fn project(error: ManifestStoreError) -> Option<Self> {
        match error {
            ManifestStoreError::StoragePressure(evidence) => Some(Self::from_manifest(evidence)),
            ManifestStoreError::Active(ActiveJournalError::StoragePressure(evidence)) => {
                Some(Self::from_active(evidence))
            }
            _ => None,
        }
    }

    /// Returns the store-owned source family.
    #[must_use]
    pub const fn source(self) -> RuntimePressureSource {
        self.source
    }

    /// Returns the exact existing store operation.
    #[must_use]
    pub const fn operation(self) -> RuntimePressureOperation {
        self.operation
    }

    /// Returns the standard-library error kind retained by the store.
    #[must_use]
    pub const fn kind(self) -> ErrorKind {
        self.kind
    }

    /// Returns optional platform-native error evidence.
    #[must_use]
    pub const fn raw_os_error(self) -> Option<i32> {
        self.raw_os_error
    }
}

/// Coarse sanitized runtime health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealth {
    /// Worker is accepting or draining work.
    Healthy,
    /// Active size, record, or session-age policy requires a future successor.
    RotationRequired,
    /// A store-owned mutating boundary observed storage pressure.
    StoragePressure,
    /// The writer stopped on a terminal fault.
    Faulted,
    /// Graceful shutdown completed.
    Stopped,
}

/// Sanitized bounded runtime/store inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeInspection {
    store: ActiveJournalInspection,
    committed: ManifestCommit,
    generations: GenerationInventory,
    recovery: Option<RecoveryReport>,
    write_state: StoreWriteState,
    pressure_evidence: Option<RuntimePressureEvidence>,
    pending_count: usize,
    pending_bytes: usize,
    health: RuntimeHealth,
}

impl RuntimeInspection {
    /// Returns active-journal identity, bytes, records, and cutoff.
    #[must_use]
    pub const fn store(self) -> ActiveJournalInspection {
        self.store
    }

    /// Returns the current manifest-backed committed state.
    #[must_use]
    pub const fn committed(self) -> ManifestCommit {
        self.committed
    }

    /// Returns bounded path-free active and sealed generation facts.
    #[must_use]
    pub const fn generations(self) -> GenerationInventory {
        self.generations
    }

    /// Returns the latest manifest-bound durable recovery event, if any.
    ///
    /// This retained report does not imply that recovery occurred during the
    /// current runtime open.
    #[must_use]
    pub const fn latest_recovery(self) -> Option<RecoveryReport> {
        self.recovery
    }

    /// Returns composed manifest-store write custody.
    #[must_use]
    pub const fn write_state(self) -> StoreWriteState {
        self.write_state
    }

    /// Returns first-wins path- and content-free runtime pressure evidence.
    #[must_use]
    pub const fn pressure_evidence(self) -> Option<RuntimePressureEvidence> {
        self.pressure_evidence
    }

    /// Returns commands retaining an outstanding slot.
    #[must_use]
    pub const fn pending_count(self) -> usize {
        self.pending_count
    }

    /// Returns exact encoded bytes retaining reservation.
    #[must_use]
    pub const fn pending_bytes(self) -> usize {
        self.pending_bytes
    }

    /// Returns coarse path-free health.
    #[must_use]
    pub const fn health(self) -> RuntimeHealth {
        self.health
    }
}

#[derive(Clone)]
pub(crate) struct InspectionShared {
    state: Arc<Mutex<InspectionState>>,
}

struct InspectionState {
    store: Option<ManifestStoreInspection>,
    write_state: Option<StoreWriteState>,
    pressure_evidence: Option<RuntimePressureEvidence>,
    health: RuntimeHealth,
}

impl InspectionShared {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InspectionState {
                store: None,
                write_state: None,
                pressure_evidence: None,
                health: RuntimeHealth::Healthy,
            })),
        }
    }

    fn update(&self, store: &ManifestStoreInspection, health: RuntimeHealth) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.store = Some(*store);
        state.write_state = Some(store.write_state());
        if state.health != RuntimeHealth::StoragePressure {
            state.health = health;
        }
    }

    fn update_store(&self, store: &ManifestStoreInspection) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.store = Some(*store);
        state.write_state = Some(store.write_state());
    }

    fn set_health(&self, health: RuntimeHealth) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.health != RuntimeHealth::StoragePressure {
            state.health = health;
        }
    }

    fn store_terminal(&self, store: &ManifestStoreInspection, failure: StoreTerminalFailure) {
        let pressure = failure.pressure_evidence();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.store = Some(*store);
        state.write_state = Some(failure.write_state(store));
        if let Some(evidence) = pressure {
            if state.pressure_evidence.is_none() {
                state.pressure_evidence = Some(evidence);
                state.health = RuntimeHealth::StoragePressure;
            }
        } else if state.health != RuntimeHealth::StoragePressure {
            state.health = failure.non_pressure_health();
        }
    }

    pub(crate) fn coordinator_fault(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.health == RuntimeHealth::Healthy {
            state.health = RuntimeHealth::Faulted;
        }
    }

    pub(crate) fn accepts_control(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .health
            == RuntimeHealth::Healthy
    }

    pub(crate) fn pressure_evidence(&self) -> Option<RuntimePressureEvidence> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.health == RuntimeHealth::StoragePressure)
            .then_some(state.pressure_evidence)
            .flatten()
    }

    pub(crate) fn snapshot(
        &self,
        ingress: &IngressShared,
        fallback: &ManifestStoreInspection,
    ) -> RuntimeInspection {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let store = state.store.unwrap_or(*fallback);
        let write_state = state.write_state.unwrap_or_else(|| store.write_state());
        let pressure_evidence = state.pressure_evidence;
        let health = state.health;
        drop(state);
        let (pending_count, pending_bytes) = ingress.pending_counts();
        RuntimeInspection {
            store: store.active(),
            committed: store.committed(),
            generations: store.generations(),
            recovery: store.latest_recovery(),
            write_state,
            pressure_evidence,
            pending_count,
            pending_bytes,
            health,
        }
    }
}

pub(crate) struct WorkerReady {
    pub(crate) inspection: ManifestStoreInspection,
    pub(crate) recovered: Vec<RecoveredAdmissionV1>,
    pub(crate) retry: RetryStateSnapshot,
}

pub(crate) struct AppendResult {
    pub(crate) admission: CanonicalAdmission,
    pub(crate) append: AppendIdentity,
}

pub(crate) enum WorkerMessage {
    Append {
        slot: usize,
        prepared: Box<PreparedAdmissionV1>,
        response: oneshot::Sender<Result<AppendResult, ManifestStoreError>>,
    },
    Published {
        slot: usize,
        priority: AdmissionPriority,
        barrier: BarrierDemand,
        frame_bytes: usize,
        qualification: Box<och_core::RetryQualification>,
        append: AppendIdentity,
    },
    Barrier {
        response: oneshot::Sender<Result<(), ManifestStoreError>>,
    },
    Registry {
        operation: Box<RegistryOperation>,
        response: oneshot::Sender<Result<RegistryCommit, ManifestStoreError>>,
    },
    Bind {
        envelope: CollectionEnvelope,
        response: oneshot::Sender<Result<DeclaredCollectionEnvelope, ManifestStoreError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), ManifestStoreError>>,
    },
    Abort,
}

struct PendingDurable {
    slot: usize,
    bytes: usize,
    qualification: och_core::RetryQualification,
    append: AppendIdentity,
}

#[derive(Clone, Copy)]
enum StoreTerminalFailure {
    Store(ManifestStoreError),
    #[cfg(test)]
    Pressure(RuntimePressureEvidence),
}

impl StoreTerminalFailure {
    fn pressure_evidence(self) -> Option<RuntimePressureEvidence> {
        match self {
            Self::Store(error) => RuntimePressureEvidence::project(error),
            #[cfg(test)]
            Self::Pressure(evidence) => Some(evidence),
        }
    }

    #[cfg(test)]
    const fn response_error(self) -> ManifestStoreError {
        match self {
            Self::Store(error) => error,
            #[cfg(test)]
            Self::Pressure(_) => ManifestStoreError::ReopenRequired,
        }
    }

    const fn write_state(self, store: &ManifestStoreInspection) -> StoreWriteState {
        match self {
            Self::Store(_) => store.write_state(),
            #[cfg(test)]
            Self::Pressure(_) => StoreWriteState::ReopenRequired,
        }
    }

    const fn non_pressure_health(self) -> RuntimeHealth {
        match self {
            Self::Store(
                ManifestStoreError::GenerationCatalogFull
                | ManifestStoreError::Active(ActiveJournalError::RotationRequired),
            ) => RuntimeHealth::RotationRequired,
            Self::Store(_) => RuntimeHealth::Faulted,
            #[cfg(test)]
            Self::Pressure(_) => RuntimeHealth::StoragePressure,
        }
    }
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run_store_worker(
    mut options: StoreOptions,
    receiver: Receiver<WorkerMessage>,
    readiness: oneshot::Sender<Result<WorkerReady, ManifestStoreError>>,
    ingress: Arc<IngressShared>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
) {
    #[cfg(feature = "m03-pr03e-native-harness")]
    let _evidence_guard = options
        .evidence_session()
        .map(och_store::__m03_pr03e_native_harness::install_worker_session);
    let config = match options.manifest_config() {
        Ok(config) => config,
        Err(error) => {
            let _ = readiness.send(Err(error));
            ingress.stop();
            return;
        }
    };
    let mut store = match ManifestStore::open(config) {
        Ok(store) => store,
        Err(error) => {
            let _ = readiness.send(Err(error));
            ingress.stop();
            return;
        }
    };
    let mut generation_opened_at = Instant::now();
    let initial_health = RuntimeHealth::Healthy;
    inspection.update(&store.inspection(), initial_health);
    if readiness
        .send(Ok(WorkerReady {
            inspection: store.inspection(),
            recovered: store.recovered_records().to_vec(),
            retry: store.retry_state_snapshot(),
        }))
        .is_err()
    {
        ingress.stop();
        return;
    }

    let mut pending: Vec<PendingDurable> = Vec::with_capacity(MAX_OUTSTANDING_COMMANDS);
    let mut pending_since: Option<Instant> = None;
    let mut unpublished: Option<(usize, usize)> = None;
    loop {
        if stop.load(Ordering::Acquire) {
            ingress.stop();
            return;
        }
        let timeout = pending_since.map_or(options.group_commit.max_delay, |started| {
            options
                .group_commit
                .max_delay
                .saturating_sub(started.elapsed())
        });
        let message_result = if unpublished.is_some() {
            receiver.recv().map_err(|_| RecvTimeoutError::Disconnected)
        } else {
            receiver.recv_timeout(timeout)
        };
        match message_result {
            Ok(WorkerMessage::Append {
                slot,
                prepared,
                response,
            }) => {
                if unpublished.is_some() {
                    fail_worker(&ingress, &inspection);
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    return;
                }
                let frame_len = prepared.frame_len();
                let sequence = match store.next_append_sequence() {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        stop_for_store_failure(
                            &store,
                            &ingress,
                            &inspection,
                            StoreTerminalFailure::Store(error),
                        );
                        let _ = response.send(Err(error));
                        return;
                    }
                };
                let frame = match (*prepared).into_frame(sequence) {
                    Ok(frame) => frame,
                    Err(error) => {
                        fail_worker(&ingress, &inspection);
                        let _ = response.send(Err(ManifestStoreError::Active(
                            ActiveJournalError::Journal(error.error()),
                        )));
                        return;
                    }
                };
                if let Err(error) =
                    store.preflight_historical_declaration(frame.admission().declaration())
                {
                    stop_for_store_failure(
                        &store,
                        &ingress,
                        &inspection,
                        StoreTerminalFailure::Store(error),
                    );
                    let _ = response.send(Err(error));
                    return;
                }
                let age_rotation = store.inspection().active().active_records() > 0
                    && generation_opened_at.elapsed() >= options.group_commit.rotation_age;
                let fit_rotation = match store.requires_rotation(frame_len) {
                    Ok(required) => required,
                    Err(error) => {
                        stop_for_store_failure(
                            &store,
                            &ingress,
                            &inspection,
                            StoreTerminalFailure::Store(error),
                        );
                        let _ = response.send(Err(error));
                        return;
                    }
                };
                #[cfg(feature = "m03-pr03e-native-harness")]
                let rotation_demand =
                    record_rotation_decision(age_rotation || fit_rotation, sequence.get());
                #[cfg(not(feature = "m03-pr03e-native-harness"))]
                let rotation_demand = age_rotation || fit_rotation;
                if rotation_demand {
                    if let Err(error) = flush_pending(
                        &mut store,
                        &mut pending,
                        &ingress,
                        &inspection,
                        &mut options,
                    ) {
                        let _ = response.send(Err(error));
                        return;
                    }
                    #[cfg(feature = "m03-pr03e-native-harness")]
                    let rotation = rotate_with_evidence(&mut store);
                    #[cfg(not(feature = "m03-pr03e-native-harness"))]
                    let rotation = store.rotate();
                    match rotation {
                        Ok(_) => {
                            generation_opened_at = Instant::now();
                            inspection.update(&store.inspection(), RuntimeHealth::Healthy);
                        }
                        Err(error) => {
                            stop_for_store_failure(
                                &store,
                                &ingress,
                                &inspection,
                                StoreTerminalFailure::Store(error),
                            );
                            let _ = response.send(Err(error));
                            return;
                        }
                    }
                }
                #[cfg(test)]
                if let Some(hook) = options.take_test_pressure(TestPressureBoundary::Append) {
                    let failure = StoreTerminalFailure::Pressure(hook.evidence);
                    stop_for_store_failure(&store, &ingress, &inspection, failure);
                    let _ = response.send(Err(failure.response_error()));
                    hook.after_response();
                    return;
                }
                let end_offset = match store.append(&frame) {
                    Ok(end_offset) => end_offset,
                    Err(error) => {
                        // Registry history is reachable only on this writer. A
                        // mismatch after synchronous resource admission cannot
                        // be downgraded to handled evidence, so the existing
                        // fail-stop receipt contract closes the authority.
                        stop_for_store_failure(
                            &store,
                            &ingress,
                            &inspection,
                            StoreTerminalFailure::Store(error),
                        );
                        let _ = response.send(Err(error));
                        return;
                    }
                };
                let append = AppendIdentity::new(
                    store.inspection().active().journal(),
                    sequence.get(),
                    end_offset,
                );
                inspection.update_store(&store.inspection());
                let admission = frame.into_admission();
                unpublished = Some((slot, frame_len));
                if response
                    .send(Ok(AppendResult { admission, append }))
                    .is_err()
                {
                    ingress.stop();
                    return;
                }
            }
            Ok(WorkerMessage::Published {
                slot,
                priority,
                barrier,
                frame_bytes,
                qualification,
                append,
            }) => {
                if unpublished.take() != Some((slot, frame_bytes)) {
                    fail_worker(&ingress, &inspection);
                    return;
                }
                if pending.is_empty() {
                    pending_since = Some(Instant::now());
                }
                pending.push(PendingDurable {
                    slot,
                    bytes: frame_bytes,
                    qualification: *qualification,
                    append,
                });
                let pending_bytes = pending.iter().map(|entry| entry.bytes).sum::<usize>();
                let rotation_demand = rotation_required(&store, &options, generation_opened_at);
                let forced = priority == AdmissionPriority::Protected
                    || barrier == BarrierDemand::Immediate
                    || pending.len() >= options.group_commit.max_records
                    || pending_bytes >= options.group_commit.max_bytes
                    || rotation_demand;
                if forced
                    && flush_pending(
                        &mut store,
                        &mut pending,
                        &ingress,
                        &inspection,
                        &mut options,
                    )
                    .is_err()
                {
                    return;
                }
                if rotation_demand && pending.is_empty() {
                    #[cfg(feature = "m03-pr03e-native-harness")]
                    let rotation = rotate_with_evidence(&mut store);
                    #[cfg(not(feature = "m03-pr03e-native-harness"))]
                    let rotation = store.rotate();
                    match rotation {
                        Ok(_) => {
                            generation_opened_at = Instant::now();
                            inspection.update(&store.inspection(), RuntimeHealth::Healthy);
                        }
                        Err(ManifestStoreError::GenerationCatalogFull) => {
                            stop_for_store_failure(
                                &store,
                                &ingress,
                                &inspection,
                                StoreTerminalFailure::Store(
                                    ManifestStoreError::GenerationCatalogFull,
                                ),
                            );
                            return;
                        }
                        Err(error) => {
                            stop_for_store_failure(
                                &store,
                                &ingress,
                                &inspection,
                                StoreTerminalFailure::Store(error),
                            );
                            return;
                        }
                    }
                }
                if pending.is_empty() {
                    pending_since = None;
                }
            }
            Ok(WorkerMessage::Barrier { response }) => {
                if unpublished.is_some() {
                    let result = Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    ));
                    fail_worker(&ingress, &inspection);
                    let _ = response.send(result);
                    return;
                }
                let result = flush_pending(
                    &mut store,
                    &mut pending,
                    &ingress,
                    &inspection,
                    &mut options,
                );
                pending_since = None;
                let _ = response.send(result);
                if result.is_err() {
                    return;
                }
            }
            Ok(WorkerMessage::Registry {
                operation,
                response,
            }) => {
                if unpublished.is_some() {
                    fail_worker(&ingress, &inspection);
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    return;
                }
                #[cfg(test)]
                if let Some(hook) = options.take_test_pressure(TestPressureBoundary::Registry) {
                    let failure = StoreTerminalFailure::Pressure(hook.evidence);
                    stop_for_store_failure(&store, &ingress, &inspection, failure);
                    let _ = response.send(Err(failure.response_error()));
                    hook.after_response();
                    return;
                }
                let result = match *operation {
                    RegistryOperation::Register {
                        series_id,
                        binding,
                        payload,
                        evidence,
                    } => store.register(series_id, binding, payload, evidence).map(
                        |(declaration, committed)| RegistryCommit {
                            outcome: RegistryOutcome::Declaration(Box::new(declaration)),
                            committed,
                        },
                    ),
                    RegistryOperation::Revise {
                        series_id,
                        expected_revision,
                        payload,
                        evidence,
                    } => store
                        .revise(series_id, expected_revision, payload, evidence)
                        .map(|(declaration, committed)| RegistryCommit {
                            outcome: RegistryOutcome::Declaration(Box::new(declaration)),
                            committed,
                        }),
                    RegistryOperation::Retire {
                        series_id,
                        expected_revision,
                        evidence,
                    } => store.retire(series_id, expected_revision, evidence).map(
                        |(retirement, committed)| RegistryCommit {
                            outcome: RegistryOutcome::Retirement(retirement),
                            committed,
                        },
                    ),
                };
                let terminal = result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, ManifestStoreError::Model(_)));
                if result.as_ref().is_ok() {
                    inspection.update_store(&store.inspection());
                }
                if let Err(error) = result.as_ref()
                    && terminal
                {
                    stop_for_store_failure(
                        &store,
                        &ingress,
                        &inspection,
                        StoreTerminalFailure::Store(*error),
                    );
                }
                let _ = response.send(result);
                if terminal {
                    return;
                }
            }
            Ok(WorkerMessage::Bind { envelope, response }) => {
                if unpublished.is_some() {
                    fail_worker(&ingress, &inspection);
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    return;
                }
                let result = store.bind(envelope);
                let terminal = result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, ManifestStoreError::Model(_)));
                if let Err(error) = result.as_ref()
                    && terminal
                {
                    stop_for_store_failure(
                        &store,
                        &ingress,
                        &inspection,
                        StoreTerminalFailure::Store(*error),
                    );
                }
                let _ = response.send(result);
                if terminal {
                    return;
                }
            }
            Ok(WorkerMessage::Shutdown { response }) => {
                if unpublished.is_some() {
                    let result = Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    ));
                    fail_worker(&ingress, &inspection);
                    let _ = response.send(result);
                    return;
                }
                let result = flush_pending(
                    &mut store,
                    &mut pending,
                    &ingress,
                    &inspection,
                    &mut options,
                );
                let _ = response.send(result);
                if result.is_ok() {
                    inspection.update(&store.inspection(), RuntimeHealth::Stopped);
                }
                return;
            }
            Ok(WorkerMessage::Abort) | Err(RecvTimeoutError::Disconnected) => {
                ingress.stop();
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !pending.is_empty()
                    && flush_pending(
                        &mut store,
                        &mut pending,
                        &ingress,
                        &inspection,
                        &mut options,
                    )
                    .is_err()
                {
                    return;
                }
                pending_since = None;
            }
        }
    }
}

fn flush_pending(
    store: &mut ManifestStore,
    pending: &mut Vec<PendingDurable>,
    ingress: &IngressShared,
    inspection: &InspectionShared,
    options: &mut StoreOptions,
) -> Result<(), ManifestStoreError> {
    if pending.is_empty() {
        return Ok(());
    }
    #[cfg(not(test))]
    let _ = options;
    #[cfg(test)]
    if let Some(hook) = options.take_test_pressure(TestPressureBoundary::Flush) {
        let failure = StoreTerminalFailure::Pressure(hook.evidence);
        stop_for_store_failure(store, ingress, inspection, failure);
        hook.after_response();
        return Err(failure.response_error());
    }
    let retry_pending = pending
        .iter()
        .map(|entry| {
            PendingRetryOutcome::new(
                entry.qualification.clone(),
                entry.append.append_sequence(),
                entry.append.end_offset(),
            )
        })
        .collect::<Vec<_>>();
    #[cfg(feature = "m03-pr03e-native-harness")]
    let _batch_id = och_store::__m03_pr03e_native_harness::start_worker_batch();
    let (committed, retry) = match store.sync_pending(&retry_pending) {
        Ok(committed) => committed,
        Err(error) => {
            #[cfg(feature = "m03-pr03e-native-harness")]
            och_store::__m03_pr03e_native_harness::clear_worker_batch();
            stop_for_store_failure(
                store,
                ingress,
                inspection,
                StoreTerminalFailure::Store(error),
            );
            return Err(error);
        }
    };
    // Inspection must name the committed manifest before any covered receipt
    // can wake and observe runtime state.
    #[cfg(feature = "m03-pr03e-native-harness")]
    let inspection_evidence = och_store::__m03_pr03e_native_harness::begin_worker_boundary(
        och_store::__m03_pr03e_native_harness::BoundaryId::InspectionUpdate,
        committed.durable_cutoff().append_sequence(),
        1,
    );
    inspection.update_store(&store.inspection());
    #[cfg(feature = "m03-pr03e-native-harness")]
    och_store::__m03_pr03e_native_harness::finish_worker_boundary(
        inspection_evidence,
        och_store::__m03_pr03e_native_harness::BoundaryOutcome::Success,
    );
    let completed = pending
        .iter()
        .map(|entry| DurableBatchEntry {
            slot: entry.slot,
            qualification: entry.qualification.clone(),
            append: entry.append,
        })
        .collect::<Vec<_>>();
    if !ingress.complete_durable_batch(&completed, committed, retry) {
        #[cfg(feature = "m03-pr03e-native-harness")]
        och_store::__m03_pr03e_native_harness::clear_worker_batch();
        fail_worker(ingress, inspection);
        return Err(ManifestStoreError::Active(
            ActiveJournalError::InvalidLayout,
        ));
    }
    #[cfg(feature = "m03-pr03e-native-harness")]
    och_store::__m03_pr03e_native_harness::clear_worker_batch();
    pending.clear();
    Ok(())
}

fn rotation_required(store: &ManifestStore, options: &StoreOptions, opened_at: Instant) -> bool {
    let active = store.inspection().active();
    let required = active.active_bytes() >= options.journal_limits.max_active_bytes()
        || active.active_records() >= options.journal_limits.max_active_records()
        || (active.active_records() > 0
            && opened_at.elapsed() >= options.group_commit.rotation_age);
    #[cfg(feature = "m03-pr03e-native-harness")]
    return record_rotation_decision(required, active.last_append_sequence());
    #[cfg(not(feature = "m03-pr03e-native-harness"))]
    required
}

#[cfg(feature = "m03-pr03e-native-harness")]
fn record_rotation_decision(required: bool, subject: u64) -> bool {
    let evidence = och_store::__m03_pr03e_native_harness::begin_worker_boundary(
        och_store::__m03_pr03e_native_harness::BoundaryId::RotationDecision,
        subject,
        1,
    );
    och_store::__m03_pr03e_native_harness::finish_worker_boundary(
        evidence,
        och_store::__m03_pr03e_native_harness::BoundaryOutcome::Success,
    );
    required
}

#[cfg(feature = "m03-pr03e-native-harness")]
fn rotate_with_evidence(store: &mut ManifestStore) -> Result<ManifestCommit, ManifestStoreError> {
    let subject = store.inspection().active().last_append_sequence();
    let evidence = och_store::__m03_pr03e_native_harness::begin_worker_boundary(
        och_store::__m03_pr03e_native_harness::BoundaryId::RotationDelay,
        subject,
        1,
    );
    och_store::__m03_pr03e_native_harness::suspend_worker_ordinary_publication();
    let result = store.rotate();
    och_store::__m03_pr03e_native_harness::resume_worker_ordinary_publication();
    och_store::__m03_pr03e_native_harness::finish_worker_boundary(
        evidence,
        if result.is_ok() {
            och_store::__m03_pr03e_native_harness::BoundaryOutcome::Success
        } else {
            och_store::__m03_pr03e_native_harness::BoundaryOutcome::Error
        },
    );
    result
}

fn stop_for_store_failure(
    store: &ManifestStore,
    ingress: &IngressShared,
    inspection: &InspectionShared,
    failure: StoreTerminalFailure,
) {
    inspection.store_terminal(&store.inspection(), failure);
    ingress.stop();
}

fn fail_worker(ingress: &IngressShared, inspection: &InspectionShared) {
    inspection.set_health(RuntimeHealth::Faulted);
    ingress.stop();
}

pub(crate) fn spawn_worker_and_reaper(
    options: StoreOptions,
    receiver: Receiver<WorkerMessage>,
    readiness: oneshot::Sender<Result<WorkerReady, ManifestStoreError>>,
    ingress: Arc<IngressShared>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
    reaped: oneshot::Sender<()>,
) -> Result<(), std::io::Error> {
    #[cfg(test)]
    let cleanup = options.cleanup_on_reap.then(|| options.directory.clone());
    std::thread::Builder::new()
        .name("och-journal-reaper".to_owned())
        .spawn(move || {
            let worker = std::thread::Builder::new()
                .name("och-active-journal".to_owned())
                .spawn(move || {
                    run_store_worker(options, receiver, readiness, ingress, inspection, stop);
                });
            if let Ok(worker) = worker {
                let _ = worker.join();
            }
            #[cfg(test)]
            if let Some(directory) = cleanup {
                let _ = std::fs::remove_dir_all(directory);
            }
            let _ = reaped.send(());
        })?;
    Ok(())
}

pub(crate) fn try_send(
    sender: &SyncSender<WorkerMessage>,
    message: WorkerMessage,
) -> Result<(), WorkerMessage> {
    sender.try_send(message).map_err(|error| match error {
        std::sync::mpsc::TrySendError::Full(message)
        | std::sync::mpsc::TrySendError::Disconnected(message) => message,
    })
}
