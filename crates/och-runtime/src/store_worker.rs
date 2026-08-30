use crate::ingress::{
    AdmissionPriority, AppendIdentity, BarrierDemand, ByteReservationLimits, IngressShared,
    MAX_OUTSTANDING_COMMANDS,
};
use och_core::{
    CanonicalAdmission, CollectionEnvelope, DeclarationEvidence, DeclarationRevision,
    DeclaredCollectionEnvelope, SeriesBinding, SeriesDeclaration, SeriesDeclarationPayload,
    SeriesId, SeriesRetirement, StoreId,
};
use och_store::{
    ActiveJournalError, ActiveJournalInspection, ActiveJournalLimits, ActiveJournalOpenMode,
    ManifestCommit, ManifestStore, ManifestStoreConfig, ManifestStoreError,
    ManifestStoreInspection, PreparedAdmissionV1, RecoveredAdmissionV1, RegistryPersistenceOptions,
};
use std::fmt;
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

    /// Returns session age that demands pre-manifest rotation.
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
    #[cfg(test)]
    cleanup_on_reap: bool,
}

impl StoreOptions {
    /// Validates all bounded relationships before worker or filesystem activity.
    ///
    /// # Errors
    ///
    /// Refuses invalid directory/options or group bytes above global reservations.
    pub fn new(
        directory: PathBuf,
        store_id: StoreId,
        mode: ActiveJournalOpenMode,
        journal_limits: ActiveJournalLimits,
        byte_limits: ByteReservationLimits,
        group_commit: GroupCommitPolicy,
        registry: RegistryPersistenceOptions,
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
            #[cfg(test)]
            cleanup_on_reap: false,
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

    pub(crate) fn manifest_config(&self) -> Result<ManifestStoreConfig, ManifestStoreError> {
        ManifestStoreConfig::new(
            self.directory.clone(),
            self.store_id,
            self.mode,
            self.journal_limits,
            self.registry.clone(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_test_cleanup(mut self) -> Self {
        self.cleanup_on_reap = true;
        self
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

/// Coarse sanitized runtime health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealth {
    /// Worker is accepting or draining work.
    Healthy,
    /// Active size, record, or session-age policy requires a future successor.
    RotationRequired,
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
    health: RuntimeHealth,
}

impl InspectionShared {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(InspectionState {
                store: None,
                health: RuntimeHealth::Healthy,
            })),
        }
    }

    fn update(&self, store: ManifestStoreInspection, health: RuntimeHealth) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.store = Some(store);
        state.health = health;
    }

    fn update_store(&self, store: ManifestStoreInspection) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .store = Some(store);
    }

    fn set_health(&self, health: RuntimeHealth) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .health = health;
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

    pub(crate) fn snapshot(
        &self,
        ingress: &IngressShared,
        fallback: ManifestStoreInspection,
    ) -> RuntimeInspection {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let store = state.store.unwrap_or(fallback);
        let health = state.health;
        drop(state);
        let (pending_count, pending_bytes) = ingress.pending_counts();
        RuntimeInspection {
            store: store.active(),
            committed: store.committed(),
            pending_count,
            pending_bytes,
            health,
        }
    }
}

pub(crate) struct WorkerReady {
    pub(crate) inspection: ManifestStoreInspection,
    pub(crate) recovered: Vec<RecoveredAdmissionV1>,
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
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run_store_worker(
    options: StoreOptions,
    receiver: Receiver<WorkerMessage>,
    readiness: oneshot::Sender<Result<WorkerReady, ManifestStoreError>>,
    ingress: Arc<IngressShared>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
) {
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
    let opened_at = Instant::now();
    let initial_health = if rotation_required(&store, &options, opened_at) {
        RuntimeHealth::RotationRequired
    } else {
        RuntimeHealth::Healthy
    };
    inspection.update(store.inspection(), initial_health);
    if readiness
        .send(Ok(WorkerReady {
            inspection: store.inspection(),
            recovered: store.recovered_records().to_vec(),
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
        if opened_at.elapsed() >= options.group_commit.rotation_age {
            inspection.set_health(RuntimeHealth::RotationRequired);
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
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    fail_worker(&ingress, &inspection);
                    return;
                }
                if opened_at.elapsed() >= options.group_commit.rotation_age {
                    let admission = prepared.into_admission();
                    drop(admission);
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::RotationRequired,
                    )));
                    inspection.set_health(RuntimeHealth::RotationRequired);
                    ingress.stop();
                    return;
                }
                let sequence = match store.next_append_sequence() {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        let _ = response.send(Err(error));
                        fail_worker(&ingress, &inspection);
                        return;
                    }
                };
                let frame_len = prepared.frame_len();
                let frame = match (*prepared).into_frame(sequence) {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = response.send(Err(ManifestStoreError::Active(
                            ActiveJournalError::Journal(error.error()),
                        )));
                        fail_worker(&ingress, &inspection);
                        return;
                    }
                };
                let end_offset = match store.append(&frame) {
                    Ok(end_offset) => end_offset,
                    Err(error) => {
                        // Registry history is reachable only on this writer. A
                        // mismatch after synchronous resource admission cannot
                        // be downgraded to handled evidence, so the existing
                        // fail-stop receipt contract closes the authority.
                        let _ = response.send(Err(error));
                        if error == ManifestStoreError::Active(ActiveJournalError::RotationRequired)
                        {
                            inspection.set_health(RuntimeHealth::RotationRequired);
                        } else {
                            fail_worker(&ingress, &inspection);
                        }
                        return;
                    }
                };
                let append = AppendIdentity::new(
                    store.inspection().active().journal(),
                    sequence.get(),
                    end_offset,
                );
                inspection.update_store(store.inspection());
                if rotation_required(&store, &options, opened_at) {
                    inspection.set_health(RuntimeHealth::RotationRequired);
                }
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
                });
                let pending_bytes = pending.iter().map(|entry| entry.bytes).sum::<usize>();
                let forced = priority == AdmissionPriority::Protected
                    || barrier == BarrierDemand::Immediate
                    || pending.len() >= options.group_commit.max_records
                    || pending_bytes >= options.group_commit.max_bytes;
                if forced && flush_pending(&mut store, &mut pending, &ingress, &inspection).is_err()
                {
                    return;
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
                    let _ = response.send(result);
                    fail_worker(&ingress, &inspection);
                    return;
                }
                let result = flush_pending(&mut store, &mut pending, &ingress, &inspection);
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
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    fail_worker(&ingress, &inspection);
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
                    inspection.update_store(store.inspection());
                }
                let _ = response.send(result);
                if terminal {
                    fail_worker(&ingress, &inspection);
                    return;
                }
            }
            Ok(WorkerMessage::Bind { envelope, response }) => {
                if unpublished.is_some() {
                    let _ = response.send(Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    )));
                    fail_worker(&ingress, &inspection);
                    return;
                }
                let result = store.bind(envelope);
                let terminal = result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, ManifestStoreError::Model(_)));
                let _ = response.send(result);
                if terminal {
                    fail_worker(&ingress, &inspection);
                    return;
                }
            }
            Ok(WorkerMessage::Shutdown { response }) => {
                if unpublished.is_some() {
                    let result = Err(ManifestStoreError::Active(
                        ActiveJournalError::InvalidLayout,
                    ));
                    let _ = response.send(result);
                    fail_worker(&ingress, &inspection);
                    return;
                }
                let result = flush_pending(&mut store, &mut pending, &ingress, &inspection);
                let _ = response.send(result);
                if result.is_ok() {
                    inspection.update(store.inspection(), RuntimeHealth::Stopped);
                }
                return;
            }
            Ok(WorkerMessage::Abort) | Err(RecvTimeoutError::Disconnected) => {
                ingress.stop();
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !pending.is_empty()
                    && flush_pending(&mut store, &mut pending, &ingress, &inspection).is_err()
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
) -> Result<(), ManifestStoreError> {
    if pending.is_empty() {
        return Ok(());
    }
    let committed = match store.sync_pending() {
        Ok(committed) => committed,
        Err(error) => {
            fail_worker(ingress, inspection);
            return Err(error);
        }
    };
    // Inspection must name the committed manifest before any covered receipt
    // can wake and observe runtime state.
    inspection.update_store(store.inspection());
    for entry in pending.drain(..) {
        if !ingress.complete_durable(entry.slot, committed) {
            fail_worker(ingress, inspection);
            return Err(ManifestStoreError::Active(
                ActiveJournalError::InvalidLayout,
            ));
        }
    }
    Ok(())
}

fn rotation_required(store: &ManifestStore, options: &StoreOptions, opened_at: Instant) -> bool {
    let active = store.inspection().active();
    active.active_bytes() >= options.journal_limits.max_active_bytes()
        || active.active_records() >= options.journal_limits.max_active_records()
        || opened_at.elapsed() >= options.group_commit.rotation_age
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
