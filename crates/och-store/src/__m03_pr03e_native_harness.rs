//! Unsupported temporary M03-PR03e native evidence plumbing.
//!
//! This feature-only module is public solely so `och-runtime` can expose the
//! reviewed tooling facade. It is not a product extension API or durable-format
//! authority.

use std::cell::RefCell;
use std::io::ErrorKind;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError};
use std::time::{Duration, Instant};

/// Hard event-record capacity accepted by one temporary evidence session.
pub const MAX_NATIVE_EVIDENCE_EVENTS: usize = 65_536;

const BOUNDARY_COUNT: usize = 16;
const MAX_BOUNDARY_OCCURRENCES: u32 = 262_144;

/// Closed current-V1 boundary identity.
///
/// This unsupported feature-only type deliberately contains no future V2 ID,
/// wildcard, dynamic name, or path-derived identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum BoundaryId {
    /// Active Journal V1 frame write.
    JournalAppendWrite = 1,
    /// Active Journal V1 synchronization.
    JournalSync = 2,
    /// Alternate mechanical checkpoint write.
    CheckpointWrite = 3,
    /// Mechanical checkpoint synchronization.
    CheckpointSync = 4,
    /// Synchronized mechanical cutoff adoption.
    CheckpointAdopt = 5,
    /// Retry State V1 publication transaction.
    RetryStatePublish = 6,
    /// Ordinary Manifest V1 staging preparation.
    ManifestPrepare = 7,
    /// Ordinary Manifest V1 rename commit.
    ManifestRenameCommit = 8,
    /// Ordinary Manifest V1 post-rename directory synchronization.
    ManifestPostcommit = 9,
    /// Validated ordinary Manifest V1 in-memory adoption.
    ManifestAdopt = 10,
    /// Existing first-wins typed reopen-custody transition.
    StorePressureTransition = 11,
    /// Runtime handled-stage visibility transition.
    HandledVisibility = 12,
    /// Runtime committed inspection publication.
    InspectionUpdate = 13,
    /// Runtime atomic durable-batch receipt resolution.
    DurableBatchReceiptResolution = 14,
    /// Sole-writer current-V1 automatic rotation decision.
    RotationDecision = 15,
    /// Sole-writer current-V1 rotation delay.
    RotationDelay = 16,
}

impl TryFrom<u8> for BoundaryId {
    type Error = BoundaryLookupError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        ALL_BOUNDARIES
            .iter()
            .copied()
            .find(|boundary| *boundary as u8 == value)
            .ok_or(BoundaryLookupError)
    }
}

/// Closed-boundary lookup refusal for unknown, zero, or wildcard-like values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundaryLookupError;

/// Native owner of one registered boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundaryOwner {
    /// `och-store` owns the source operation.
    Store,
    /// `och-runtime` owns the source transition.
    Runtime,
}

/// Closed operation class for current source-bound evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundaryOperation {
    /// Write bytes to an existing artifact.
    Write,
    /// Synchronize an artifact.
    Synchronize,
    /// Adopt already-proven in-memory state.
    Adopt,
    /// Publish one current durable authority artifact.
    Publish,
    /// Prepare publication before its authority-changing rename.
    Prepare,
    /// Perform the authority-changing rename.
    Commit,
    /// Validate or synchronize after commit.
    Postcommit,
    /// Publish runtime inspection.
    Inspection,
    /// Resolve one runtime receipt stage.
    Receipt,
    /// Select a closed writer transition.
    Decision,
    /// Measure writer delay around current rotation.
    Delay,
    /// Enter existing typed reopen custody.
    PressureTransition,
}

/// One-to-one static source-site identity for the current registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundarySource {
    /// `ActiveJournal::append` frame-write site.
    ActiveJournalAppend,
    /// `ActiveJournal::sync_pending` journal-sync site.
    ActiveJournalJournalSync,
    /// `ActiveJournal::sync_pending` checkpoint-write site.
    ActiveJournalCheckpointWrite,
    /// `ActiveJournal::sync_pending` checkpoint-sync site.
    ActiveJournalCheckpointSync,
    /// `ActiveJournal::sync_pending` checkpoint-adoption site.
    ActiveJournalCheckpointAdopt,
    /// `publish_reusable_slot` Retry State V1 transaction site.
    ManifestRetryPublication,
    /// `publish_reusable_slot` Manifest V1 preparation site.
    ManifestPreparation,
    /// `publish_reusable_slot` Manifest V1 rename site.
    ManifestRename,
    /// `publish_reusable_slot` Manifest V1 postcommit site.
    ManifestPostcommit,
    /// `ManifestStore::publish_and_adopt_prepared_manifest` adoption site.
    ManifestAdoption,
    /// `pressure::record_pressure_transition` custody site.
    StorePressureCustody,
    /// `IngressShared::complete_handled` transition site.
    RuntimeHandled,
    /// `flush_pending` inspection-update site.
    RuntimeInspection,
    /// `IngressShared::complete_durable_batch` transition site.
    RuntimeDurableBatch,
    /// `record_rotation_decision` sole-writer decision site.
    RuntimeRotationDecision,
    /// `rotate_with_evidence` sole-writer delay site.
    RuntimeRotationDelay,
}

/// Immutable metadata for one closed current source boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BoundaryDescriptor {
    id: BoundaryId,
    source: BoundarySource,
    owner: BoundaryOwner,
    operation: BoundaryOperation,
    mutating: bool,
    partial_write: bool,
    pressure: bool,
    pre_operation_error: bool,
    max_occurrence: NonZeroU32,
    terminal_success: bool,
}

impl BoundaryDescriptor {
    /// Returns the closed boundary identity.
    #[must_use]
    pub const fn id(self) -> BoundaryId {
        self.id
    }

    /// Returns the exact static source-site identity.
    #[must_use]
    pub const fn source(self) -> BoundarySource {
        self.source
    }

    /// Returns the native owner.
    #[must_use]
    pub const fn owner(self) -> BoundaryOwner {
        self.owner
    }

    /// Returns the closed operation class.
    #[must_use]
    pub const fn operation(self) -> BoundaryOperation {
        self.operation
    }

    /// Returns whether the boundary mutates filesystem or in-memory authority.
    #[must_use]
    pub const fn is_mutating(self) -> bool {
        self.mutating
    }

    /// Returns whether a deterministic nonzero short write is applicable.
    #[must_use]
    pub const fn allows_partial_write(self) -> bool {
        self.partial_write
    }

    /// Returns whether typed pressure injection is applicable.
    #[must_use]
    pub const fn allows_pressure(self) -> bool {
        self.pressure
    }

    /// Returns whether a deterministic pre-operation error is applicable.
    #[must_use]
    pub const fn allows_pre_operation_error(self) -> bool {
        self.pre_operation_error
    }

    /// Returns the exact nonzero per-session occurrence bound.
    #[must_use]
    pub const fn max_occurrence(self) -> NonZeroU32 {
        self.max_occurrence
    }

    /// Returns whether a successful trace may terminate at this boundary.
    #[must_use]
    pub const fn allows_terminal_success(self) -> bool {
        self.terminal_success
    }

    /// Returns whether `next` is a closed legal direct successor.
    #[must_use]
    pub const fn allows_successor(self, next: BoundaryId) -> bool {
        use BoundaryId as B;
        match self.id {
            B::JournalAppendWrite => {
                matches!(next, B::HandledVisibility | B::StorePressureTransition)
            }
            B::HandledVisibility => matches!(
                next,
                B::JournalAppendWrite | B::JournalSync | B::RotationDecision
            ),
            B::JournalSync => matches!(next, B::CheckpointWrite | B::StorePressureTransition),
            B::CheckpointWrite => matches!(next, B::CheckpointSync | B::StorePressureTransition),
            B::CheckpointSync => matches!(next, B::CheckpointAdopt | B::StorePressureTransition),
            B::CheckpointAdopt => matches!(next, B::RetryStatePublish),
            B::RetryStatePublish => matches!(next, B::ManifestPrepare | B::StorePressureTransition),
            B::ManifestPrepare => {
                matches!(next, B::ManifestRenameCommit | B::StorePressureTransition)
            }
            B::ManifestRenameCommit => {
                matches!(next, B::ManifestPostcommit | B::StorePressureTransition)
            }
            B::ManifestPostcommit => matches!(
                next,
                B::ManifestAdopt
                    | B::ManifestPrepare
                    | B::RotationDecision
                    | B::JournalAppendWrite
                    | B::StorePressureTransition
            ),
            B::ManifestAdopt => matches!(
                next,
                B::InspectionUpdate
                    | B::ManifestPrepare
                    | B::RotationDecision
                    | B::JournalAppendWrite
            ),
            B::InspectionUpdate => matches!(next, B::DurableBatchReceiptResolution),
            B::DurableBatchReceiptResolution => matches!(
                next,
                B::RotationDecision | B::RotationDelay | B::JournalAppendWrite | B::JournalSync
            ),
            B::RotationDecision => matches!(
                next,
                B::RotationDelay | B::JournalAppendWrite | B::JournalSync | B::HandledVisibility
            ),
            B::RotationDelay => matches!(
                next,
                B::JournalAppendWrite | B::RotationDecision | B::HandledVisibility
            ),
            B::StorePressureTransition => false,
        }
    }
}

const MAX_OCCURRENCE: NonZeroU32 = NonZeroU32::new(MAX_BOUNDARY_OCCURRENCES).unwrap();

#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
const fn descriptor(
    id: BoundaryId,
    source: BoundarySource,
    owner: BoundaryOwner,
    operation: BoundaryOperation,
    mutating: bool,
    partial_write: bool,
    pressure: bool,
    pre_operation_error: bool,
    terminal_success: bool,
) -> BoundaryDescriptor {
    BoundaryDescriptor {
        id,
        source,
        owner,
        operation,
        mutating,
        partial_write,
        pressure,
        pre_operation_error,
        max_occurrence: MAX_OCCURRENCE,
        terminal_success,
    }
}

const ALL_BOUNDARIES: [BoundaryId; BOUNDARY_COUNT] = [
    BoundaryId::JournalAppendWrite,
    BoundaryId::JournalSync,
    BoundaryId::CheckpointWrite,
    BoundaryId::CheckpointSync,
    BoundaryId::CheckpointAdopt,
    BoundaryId::RetryStatePublish,
    BoundaryId::ManifestPrepare,
    BoundaryId::ManifestRenameCommit,
    BoundaryId::ManifestPostcommit,
    BoundaryId::ManifestAdopt,
    BoundaryId::StorePressureTransition,
    BoundaryId::HandledVisibility,
    BoundaryId::InspectionUpdate,
    BoundaryId::DurableBatchReceiptResolution,
    BoundaryId::RotationDecision,
    BoundaryId::RotationDelay,
];

const REGISTRY: [BoundaryDescriptor; BOUNDARY_COUNT] = [
    descriptor(
        BoundaryId::JournalAppendWrite,
        BoundarySource::ActiveJournalAppend,
        BoundaryOwner::Store,
        BoundaryOperation::Write,
        true,
        true,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::JournalSync,
        BoundarySource::ActiveJournalJournalSync,
        BoundaryOwner::Store,
        BoundaryOperation::Synchronize,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::CheckpointWrite,
        BoundarySource::ActiveJournalCheckpointWrite,
        BoundaryOwner::Store,
        BoundaryOperation::Write,
        true,
        true,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::CheckpointSync,
        BoundarySource::ActiveJournalCheckpointSync,
        BoundaryOwner::Store,
        BoundaryOperation::Synchronize,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::CheckpointAdopt,
        BoundarySource::ActiveJournalCheckpointAdopt,
        BoundaryOwner::Store,
        BoundaryOperation::Adopt,
        true,
        false,
        false,
        false,
        false,
    ),
    descriptor(
        BoundaryId::RetryStatePublish,
        BoundarySource::ManifestRetryPublication,
        BoundaryOwner::Store,
        BoundaryOperation::Publish,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::ManifestPrepare,
        BoundarySource::ManifestPreparation,
        BoundaryOwner::Store,
        BoundaryOperation::Prepare,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::ManifestRenameCommit,
        BoundarySource::ManifestRename,
        BoundaryOwner::Store,
        BoundaryOperation::Commit,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::ManifestPostcommit,
        BoundarySource::ManifestPostcommit,
        BoundaryOwner::Store,
        BoundaryOperation::Postcommit,
        true,
        false,
        true,
        true,
        false,
    ),
    descriptor(
        BoundaryId::ManifestAdopt,
        BoundarySource::ManifestAdoption,
        BoundaryOwner::Store,
        BoundaryOperation::Adopt,
        true,
        false,
        false,
        false,
        true,
    ),
    descriptor(
        BoundaryId::StorePressureTransition,
        BoundarySource::StorePressureCustody,
        BoundaryOwner::Store,
        BoundaryOperation::PressureTransition,
        true,
        false,
        false,
        false,
        true,
    ),
    descriptor(
        BoundaryId::HandledVisibility,
        BoundarySource::RuntimeHandled,
        BoundaryOwner::Runtime,
        BoundaryOperation::Receipt,
        true,
        false,
        false,
        false,
        true,
    ),
    descriptor(
        BoundaryId::InspectionUpdate,
        BoundarySource::RuntimeInspection,
        BoundaryOwner::Runtime,
        BoundaryOperation::Inspection,
        true,
        false,
        false,
        false,
        false,
    ),
    descriptor(
        BoundaryId::DurableBatchReceiptResolution,
        BoundarySource::RuntimeDurableBatch,
        BoundaryOwner::Runtime,
        BoundaryOperation::Receipt,
        true,
        false,
        false,
        false,
        true,
    ),
    descriptor(
        BoundaryId::RotationDecision,
        BoundarySource::RuntimeRotationDecision,
        BoundaryOwner::Runtime,
        BoundaryOperation::Decision,
        false,
        false,
        false,
        false,
        true,
    ),
    descriptor(
        BoundaryId::RotationDelay,
        BoundarySource::RuntimeRotationDelay,
        BoundaryOwner::Runtime,
        BoundaryOperation::Delay,
        true,
        false,
        false,
        false,
        true,
    ),
];

/// Returns the complete immutable current-V1 source registry.
#[must_use]
pub const fn boundary_registry() -> &'static [BoundaryDescriptor] {
    &REGISTRY
}

fn boundary_descriptor(id: BoundaryId) -> BoundaryDescriptor {
    REGISTRY[usize::from(id as u8 - 1)]
}

/// Closed deterministic injected standard-library error kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InjectedErrorKind {
    /// Non-pressure deterministic operation error.
    Other,
    /// Typed `StorageFull` pressure.
    StorageFull,
    /// Typed `QuotaExceeded` pressure.
    QuotaExceeded,
}

impl InjectedErrorKind {
    pub(crate) const fn error_kind(self) -> ErrorKind {
        match self {
            Self::Other => ErrorKind::Other,
            Self::StorageFull => ErrorKind::StorageFull,
            Self::QuotaExceeded => ErrorKind::QuotaExceeded,
        }
    }

    const fn is_pressure(self) -> bool {
        matches!(self, Self::StorageFull | Self::QuotaExceeded)
    }
}

/// One closed deterministic action at one exact boundary occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultAction {
    /// Return the selected error before the operation mutates state.
    PreOperationError(InjectedErrorKind),
    /// Perform a nonzero short write and then return the selected error.
    ShortPartialWrite {
        /// Requested nonzero short-write byte count, clamped below operation length.
        bytes: NonZeroUsize,
        /// Error returned after the real partial write.
        error: InjectedErrorKind,
    },
    /// Record success, make crash readiness visible, and block before returning.
    CrashAfterSuccess,
}

/// Refusal to construct an illegal closed fault plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPlanError {
    /// The occurrence exceeds the descriptor's fixed nonzero bound.
    OccurrenceOutOfRange,
    /// The selected action is not applicable to the boundary operation.
    ActionNotApplicable,
    /// Typed pressure was selected for a boundary that cannot classify pressure.
    PressureNotApplicable,
}

/// At most one exact fault target/action for one evidence session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultPlan {
    target: Option<FaultTarget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FaultTarget {
    boundary: BoundaryId,
    occurrence: NonZeroU32,
    action: FaultAction,
}

impl FaultPlan {
    /// Returns a session plan with observation only and no injected fault.
    #[must_use]
    pub const fn none() -> Self {
        Self { target: None }
    }

    /// Validates one exact boundary, nonzero occurrence, and closed action.
    ///
    /// # Errors
    ///
    /// Refuses out-of-range occurrences, non-applicable partial/pre-operation
    /// actions, and pressure at non-pressure boundaries.
    pub fn single(
        boundary: BoundaryId,
        occurrence: NonZeroU32,
        action: FaultAction,
    ) -> Result<Self, FaultPlanError> {
        let descriptor = boundary_descriptor(boundary);
        if occurrence > descriptor.max_occurrence() {
            return Err(FaultPlanError::OccurrenceOutOfRange);
        }
        let error = match action {
            FaultAction::PreOperationError(error) => {
                if !descriptor.allows_pre_operation_error() {
                    return Err(FaultPlanError::ActionNotApplicable);
                }
                Some(error)
            }
            FaultAction::ShortPartialWrite { error, .. } => {
                if !descriptor.allows_partial_write() {
                    return Err(FaultPlanError::ActionNotApplicable);
                }
                Some(error)
            }
            FaultAction::CrashAfterSuccess => None,
        };
        if error.is_some_and(InjectedErrorKind::is_pressure) && !descriptor.allows_pressure() {
            return Err(FaultPlanError::PressureNotApplicable);
        }
        Ok(Self {
            target: Some(FaultTarget {
                boundary,
                occurrence,
                action,
            }),
        })
    }
}

impl Default for FaultPlan {
    fn default() -> Self {
        Self::none()
    }
}

/// Closed event completion outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundaryOutcome {
    /// The actual operation or transition succeeded.
    Success,
    /// The operation returned an error without an injected partial write.
    Error,
    /// A real nonzero partial write completed before the injected error.
    PartialWrite,
}

/// Fixed-size numeric/enumerated evidence record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundaryEvent {
    boundary: BoundaryId,
    occurrence: NonZeroU32,
    batch_id: u64,
    subject: u64,
    item_count: u32,
    start_ns: u64,
    stop_ns: u64,
    elapsed_ns: u64,
    outcome: BoundaryOutcome,
    injected_error: Option<InjectedErrorKind>,
}

impl BoundaryEvent {
    /// Returns the closed boundary identity.
    #[must_use]
    pub const fn boundary(self) -> BoundaryId {
        self.boundary
    }
    /// Returns the exact nonzero occurrence ordinal.
    #[must_use]
    pub const fn occurrence(self) -> NonZeroU32 {
        self.occurrence
    }
    /// Returns the nonzero durability batch ID, or zero when not applicable.
    #[must_use]
    pub const fn batch_id(self) -> u64 {
        self.batch_id
    }
    /// Returns the boundary-specific numeric correlation subject.
    #[must_use]
    pub const fn subject(self) -> u64 {
        self.subject
    }
    /// Returns the bounded item count attached to the event.
    #[must_use]
    pub const fn item_count(self) -> u32 {
        self.item_count
    }
    /// Returns process-origin-relative start nanoseconds.
    #[must_use]
    pub const fn start_ns(self) -> u64 {
        self.start_ns
    }
    /// Returns process-origin-relative stop nanoseconds.
    #[must_use]
    pub const fn stop_ns(self) -> u64 {
        self.stop_ns
    }
    /// Returns the checked stop-minus-start nanoseconds.
    #[must_use]
    pub const fn elapsed_ns(self) -> u64 {
        self.elapsed_ns
    }
    /// Returns the closed operation outcome.
    #[must_use]
    pub const fn outcome(self) -> BoundaryOutcome {
        self.outcome
    }
    /// Returns the selected injected error kind, if this was the target.
    #[must_use]
    pub const fn injected_error(self) -> Option<InjectedErrorKind> {
        self.injected_error
    }
}

/// First structural evidence failure retained by a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceFailure {
    /// The fixed event capacity was exhausted without overwrite.
    EventOverflow,
    /// A boundary began or finished with invalid nesting/token state.
    InvalidNesting,
    /// The measured event path observed recorder lock contention.
    LockContention,
    /// Checked time, occurrence, batch, or token arithmetic overflowed.
    ArithmeticOverflow,
    /// Recorder synchronization was poisoned.
    Poisoned,
    /// A worker session was installed over an existing session.
    DuplicateInstallation,
    /// A boundary token was presented to a different originating session.
    SessionMismatch,
}

/// Structural status of one copied evidence snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceStatus {
    /// No structural failure or currently open boundary is present.
    Complete,
    /// A boundary or crash gate is still open.
    Incomplete,
    /// The first structural failure is retained.
    Failed(EvidenceFailure),
}

/// Exact successful boundary that armed the parent-owned crash gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrashReady {
    boundary: BoundaryId,
    occurrence: NonZeroU32,
}

impl CrashReady {
    /// Returns the exact successful boundary.
    #[must_use]
    pub const fn boundary(self) -> BoundaryId {
        self.boundary
    }
    /// Returns its exact occurrence ordinal.
    #[must_use]
    pub const fn occurrence(self) -> NonZeroU32 {
        self.occurrence
    }
}

/// Owned bounded snapshot copied outside the measured event path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceSnapshot {
    capacity: usize,
    events: Vec<BoundaryEvent>,
    status: EvidenceStatus,
    crash_ready: Option<CrashReady>,
}

impl EvidenceSnapshot {
    /// Returns the fixed configured event capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
    /// Returns recorded events in exact completion order.
    #[must_use]
    pub fn events(&self) -> &[BoundaryEvent] {
        &self.events
    }
    /// Returns complete, incomplete, or failed structural status.
    #[must_use]
    pub const fn status(&self) -> EvidenceStatus {
        self.status
    }
    /// Returns current crash-gate readiness, if armed.
    #[must_use]
    pub const fn crash_ready(&self) -> Option<CrashReady> {
        self.crash_ready
    }

    /// Validates timestamps, occurrence bounds, direct successors, and terminal law.
    ///
    /// # Errors
    ///
    /// Refuses incomplete/failed snapshots, malformed timing arithmetic,
    /// unexpected successors, or an illegal successful terminal boundary.
    pub fn validate_closed_trace(&self) -> Result<(), TraceValidationError> {
        if self.status != EvidenceStatus::Complete {
            return Err(TraceValidationError::Incomplete);
        }
        for event in &self.events {
            if event.stop_ns < event.start_ns
                || event.stop_ns - event.start_ns != event.elapsed_ns
                || event.occurrence > boundary_descriptor(event.boundary).max_occurrence()
            {
                return Err(TraceValidationError::InvalidEvent);
            }
        }
        for pair in self.events.windows(2) {
            if !boundary_descriptor(pair[0].boundary).allows_successor(pair[1].boundary) {
                return Err(TraceValidationError::UnexpectedSuccessor);
            }
        }
        if let Some(last) = self.events.last()
            && last.outcome == BoundaryOutcome::Success
            && !boundary_descriptor(last.boundary).allows_terminal_success()
        {
            return Err(TraceValidationError::UnexpectedTerminal);
        }
        Ok(())
    }
}

/// Closed structural trace-validation refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceValidationError {
    /// The recorder is failed or still structurally incomplete.
    Incomplete,
    /// One event violates fixed timing or occurrence bounds.
    InvalidEvent,
    /// Two adjacent events violate the closed successor registry.
    UnexpectedSuccessor,
    /// A successful final boundary is not terminal-capable.
    UnexpectedTerminal,
}

/// Session-construction refusal before store opening or mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceSessionError {
    /// Capacity is zero or exceeds the hard bound.
    InvalidCapacity,
    /// Exact preallocation failed.
    CapacityUnavailable,
}

/// One cloneable process-local, fixed-capacity evidence session.
///
/// This unsupported feature-only session has one monotonic origin and one
/// optional exact fault target. It performs no filesystem or process-control I/O.
#[derive(Clone)]
pub struct NativeEvidenceSession {
    inner: Arc<SessionInner>,
}

struct SessionInner {
    origin: Instant,
    capacity: usize,
    plan: FaultPlan,
    state: Mutex<SessionState>,
    crash: Condvar,
    crash_ready: AtomicBool,
    failure: AtomicU8,
}

struct SessionState {
    events: Vec<BoundaryEvent>,
    occurrences: [u32; BOUNDARY_COUNT],
    active: Option<ActiveBoundary>,
    next_token: u64,
    next_batch: u64,
    crash_ready: Option<CrashReady>,
}

#[derive(Clone, Copy)]
struct ActiveBoundary {
    token: u64,
    boundary: BoundaryId,
    occurrence: NonZeroU32,
    batch_id: u64,
    subject: u64,
    item_count: u32,
    start_ns: u64,
    action: Option<FaultAction>,
}

impl NativeEvidenceSession {
    /// Validates and preallocates one fixed-capacity session.
    ///
    /// # Errors
    ///
    /// Refuses zero/excessive capacity or preallocation failure before native open.
    pub fn new(capacity: usize, plan: FaultPlan) -> Result<Self, EvidenceSessionError> {
        if capacity == 0 || capacity > MAX_NATIVE_EVIDENCE_EVENTS {
            return Err(EvidenceSessionError::InvalidCapacity);
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(capacity)
            .map_err(|_| EvidenceSessionError::CapacityUnavailable)?;
        Ok(Self {
            inner: Arc::new(SessionInner {
                origin: Instant::now(),
                capacity,
                plan,
                state: Mutex::new(SessionState {
                    events,
                    occurrences: [0; BOUNDARY_COUNT],
                    active: None,
                    next_token: 0,
                    next_batch: 0,
                    crash_ready: None,
                }),
                crash: Condvar::new(),
                crash_ready: AtomicBool::new(false),
                failure: AtomicU8::new(0),
            }),
        })
    }

    /// Copies the bounded recorder state outside native product semantics.
    #[must_use]
    pub fn snapshot(&self) -> EvidenceSnapshot {
        let state = self.inner.state.lock().unwrap_or_else(|poisoned| {
            self.mark_failure(EvidenceFailure::Poisoned);
            poisoned.into_inner()
        });
        let failure = decode_failure(self.inner.failure.load(Ordering::Acquire));
        let status = failure.map_or_else(
            || {
                if state.active.is_some() || state.crash_ready.is_some() {
                    EvidenceStatus::Incomplete
                } else {
                    EvidenceStatus::Complete
                }
            },
            EvidenceStatus::Failed,
        );
        EvidenceSnapshot {
            capacity: self.inner.capacity,
            events: state.events.clone(),
            status,
            crash_ready: state.crash_ready,
        }
    }

    /// Waits for the exact in-memory crash gate without writing control files.
    #[must_use]
    pub fn wait_for_crash_ready(&self, timeout: Duration) -> Option<CrashReady> {
        let started = Instant::now();
        let mut state = self.inner.state.lock().ok()?;
        loop {
            if let Some(ready) = state.crash_ready {
                return Some(ready);
            }
            let remaining = timeout.checked_sub(started.elapsed())?;
            let (next, result) = self.inner.crash.wait_timeout(state, remaining).ok()?;
            state = next;
            if result.timed_out() && state.crash_ready.is_none() {
                return None;
            }
        }
    }

    /// Begins one explicit feature-only boundary span.
    pub fn begin_boundary(
        &self,
        boundary: BoundaryId,
        batch_id: u64,
        subject: u64,
        item_count: u32,
    ) -> BoundaryToken {
        self.wait_before_next_boundary();
        let mut state = match self.inner.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => {
                self.mark_failure(EvidenceFailure::LockContention);
                return BoundaryToken::inactive();
            }
            Err(TryLockError::Poisoned(_)) => {
                self.mark_failure(EvidenceFailure::Poisoned);
                return BoundaryToken::inactive();
            }
        };
        if state.active.is_some() {
            self.mark_failure(EvidenceFailure::InvalidNesting);
            return BoundaryToken::inactive();
        }
        let index = usize::from(boundary as u8 - 1);
        let Some(occurrence) = state.occurrences[index].checked_add(1) else {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            return BoundaryToken::inactive();
        };
        let Some(occurrence) = NonZeroU32::new(occurrence) else {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            return BoundaryToken::inactive();
        };
        if occurrence > boundary_descriptor(boundary).max_occurrence() {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            return BoundaryToken::inactive();
        }
        let Some(token) = state.next_token.checked_add(1) else {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            return BoundaryToken::inactive();
        };
        let Some(start_ns) = self.relative_ns() else {
            return BoundaryToken::inactive();
        };
        let action = self.inner.plan.target.and_then(|target| {
            (target.boundary == boundary && target.occurrence == occurrence)
                .then_some(target.action)
        });
        state.occurrences[index] = occurrence.get();
        state.next_token = token;
        state.active = Some(ActiveBoundary {
            token,
            boundary,
            occurrence,
            batch_id,
            subject,
            item_count,
            start_ns,
            action,
        });
        BoundaryToken {
            session: Some(self.clone()),
            token,
            action,
        }
    }

    /// Explicitly finishes one boundary span and appends at most one record.
    #[allow(clippy::needless_pass_by_value)]
    pub fn finish_boundary(&self, token: BoundaryToken, outcome: BoundaryOutcome) {
        let Some(originating_session) = token.session.as_ref() else {
            return;
        };
        if !Arc::ptr_eq(&self.inner, &originating_session.inner) {
            self.mark_failure(EvidenceFailure::SessionMismatch);
            originating_session.mark_failure(EvidenceFailure::SessionMismatch);
            return;
        }
        let mut state = match self.inner.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => {
                self.mark_failure(EvidenceFailure::LockContention);
                return;
            }
            Err(TryLockError::Poisoned(_)) => {
                self.mark_failure(EvidenceFailure::Poisoned);
                return;
            }
        };
        let Some(active) = state.active else {
            self.mark_failure(EvidenceFailure::InvalidNesting);
            return;
        };
        if active.token != token.token {
            self.mark_failure(EvidenceFailure::InvalidNesting);
            return;
        }
        let Some(stop_ns) = self.relative_ns() else {
            state.active = None;
            return;
        };
        let Some(elapsed_ns) = stop_ns.checked_sub(active.start_ns) else {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            state.active = None;
            return;
        };
        state.active = None;
        if state.events.len() == self.inner.capacity {
            self.mark_failure(EvidenceFailure::EventOverflow);
        } else {
            state.events.push(BoundaryEvent {
                boundary: active.boundary,
                occurrence: active.occurrence,
                batch_id: active.batch_id,
                subject: active.subject,
                item_count: active.item_count,
                start_ns: active.start_ns,
                stop_ns,
                elapsed_ns,
                outcome,
                injected_error: injected_error(active.action),
            });
        }
        if active.action == Some(FaultAction::CrashAfterSuccess)
            && outcome == BoundaryOutcome::Success
        {
            state.crash_ready = Some(CrashReady {
                boundary: active.boundary,
                occurrence: active.occurrence,
            });
            self.inner.crash_ready.store(true, Ordering::Release);
            self.inner.crash.notify_all();
            drop(state);
            // The condition-variable wait releases the recorder mutex while
            // armed, allowing the parent-owned supervisor to snapshot the
            // exact successful event. No native path can clear this gate;
            // spurious wakeups recheck the still-present readiness state.
            self.wait_before_next_boundary();
        }
    }

    fn next_batch(&self) -> u64 {
        let Ok(mut state) = self.inner.state.try_lock() else {
            self.mark_failure(EvidenceFailure::LockContention);
            return 0;
        };
        let Some(next) = state.next_batch.checked_add(1) else {
            self.mark_failure(EvidenceFailure::ArithmeticOverflow);
            return 0;
        };
        state.next_batch = next;
        next
    }

    fn relative_ns(&self) -> Option<u64> {
        u64::try_from(self.inner.origin.elapsed().as_nanos())
            .map_err(|_| self.mark_failure(EvidenceFailure::ArithmeticOverflow))
            .ok()
    }

    fn wait_before_next_boundary(&self) {
        if !self.inner.crash_ready.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| {
            self.mark_failure(EvidenceFailure::Poisoned);
            poisoned.into_inner()
        });
        while state.crash_ready.is_some() {
            state = self.inner.crash.wait(state).unwrap_or_else(|poisoned| {
                self.mark_failure(EvidenceFailure::Poisoned);
                poisoned.into_inner()
            });
        }
    }

    fn mark_failure(&self, failure: EvidenceFailure) {
        let _ = self.inner.failure.compare_exchange(
            0,
            encode_failure(failure),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

impl std::fmt::Debug for NativeEvidenceSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeEvidenceSession")
            .field("capacity", &self.inner.capacity)
            .finish_non_exhaustive()
    }
}

/// Explicit boundary token; it never emits an event from `Drop`.
#[must_use]
pub struct BoundaryToken {
    session: Option<NativeEvidenceSession>,
    token: u64,
    action: Option<FaultAction>,
}

impl BoundaryToken {
    const fn inactive() -> Self {
        Self {
            session: None,
            token: 0,
            action: None,
        }
    }

    pub(crate) const fn pre_operation_error(&self) -> Option<InjectedErrorKind> {
        match self.action {
            Some(FaultAction::PreOperationError(error)) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn short_partial_write(&self, total: usize) -> Option<(usize, InjectedErrorKind)> {
        let Some(FaultAction::ShortPartialWrite { bytes, error }) = self.action else {
            return None;
        };
        (total > 1).then(|| (bytes.get().min(total - 1), error))
    }
}

fn injected_error(action: Option<FaultAction>) -> Option<InjectedErrorKind> {
    match action {
        Some(
            FaultAction::PreOperationError(error) | FaultAction::ShortPartialWrite { error, .. },
        ) => Some(error),
        Some(FaultAction::CrashAfterSuccess) | None => None,
    }
}

const fn encode_failure(failure: EvidenceFailure) -> u8 {
    match failure {
        EvidenceFailure::EventOverflow => 1,
        EvidenceFailure::InvalidNesting => 2,
        EvidenceFailure::LockContention => 3,
        EvidenceFailure::ArithmeticOverflow => 4,
        EvidenceFailure::Poisoned => 5,
        EvidenceFailure::DuplicateInstallation => 6,
        EvidenceFailure::SessionMismatch => 7,
    }
}

const fn decode_failure(value: u8) -> Option<EvidenceFailure> {
    match value {
        1 => Some(EvidenceFailure::EventOverflow),
        2 => Some(EvidenceFailure::InvalidNesting),
        3 => Some(EvidenceFailure::LockContention),
        4 => Some(EvidenceFailure::ArithmeticOverflow),
        5 => Some(EvidenceFailure::Poisoned),
        6 => Some(EvidenceFailure::DuplicateInstallation),
        7 => Some(EvidenceFailure::SessionMismatch),
        _ => None,
    }
}

struct WorkerContext {
    session: NativeEvidenceSession,
    batch_id: u64,
    ordinary_publication: bool,
}

std::thread_local! {
    static WORKER_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
}

/// Worker-thread installation guard for one explicitly supplied session.
///
/// Dropping this guard only removes thread-local routing; boundary stop events
/// are always explicit through [`finish_worker_boundary`].
pub struct WorkerSessionGuard {
    installed: bool,
}

impl Drop for WorkerSessionGuard {
    fn drop(&mut self) {
        if self.installed {
            WORKER_CONTEXT.with(|context| *context.borrow_mut() = None);
        }
    }
}

/// Installs one supplied session on the sole store worker before store open.
#[must_use]
pub fn install_worker_session(session: NativeEvidenceSession) -> WorkerSessionGuard {
    let installed = WORKER_CONTEXT.with(|context| {
        let mut context = context.borrow_mut();
        if context.is_some() {
            session.mark_failure(EvidenceFailure::DuplicateInstallation);
            false
        } else {
            *context = Some(WorkerContext {
                session,
                batch_id: 0,
                ordinary_publication: true,
            });
            true
        }
    });
    WorkerSessionGuard { installed }
}

/// Starts one nonzero durability batch in the installed worker session.
#[must_use]
pub fn start_worker_batch() -> u64 {
    WORKER_CONTEXT.with(|context| {
        let mut context = context.borrow_mut();
        let Some(context) = context.as_mut() else {
            return 0;
        };
        let batch = context.session.next_batch();
        context.batch_id = batch;
        batch
    })
}

/// Clears the current durability batch after its exact terminal transition.
pub fn clear_worker_batch() {
    WORKER_CONTEXT.with(|context| {
        if let Some(context) = context.borrow_mut().as_mut() {
            context.batch_id = 0;
        }
    });
}

/// Suppresses ordinary-durability publication IDs during current V1 rotation.
///
/// Rotation retains its distinct aggregate boundary and must not relabel its
/// Manifest V1 publication as the prior ordinary durability transaction.
pub fn suspend_worker_ordinary_publication() {
    WORKER_CONTEXT.with(|context| {
        if let Some(context) = context.borrow_mut().as_mut() {
            context.ordinary_publication = false;
        }
    });
}

/// Restores ordinary-durability publication IDs after current V1 rotation.
pub fn resume_worker_ordinary_publication() {
    WORKER_CONTEXT.with(|context| {
        if let Some(context) = context.borrow_mut().as_mut() {
            context.ordinary_publication = true;
        }
    });
}

/// Returns whether the installed worker is in an ordinary durability transaction.
#[must_use]
pub fn records_worker_ordinary_publication() -> bool {
    WORKER_CONTEXT.with(|context| {
        context
            .borrow()
            .as_ref()
            .is_none_or(|context| context.ordinary_publication)
    })
}

/// Begins one boundary on the installed sole-worker session, if present.
#[must_use]
pub fn begin_worker_boundary(
    boundary: BoundaryId,
    subject: u64,
    item_count: u32,
) -> Option<BoundaryToken> {
    WORKER_CONTEXT.with(|context| {
        let context = context.borrow();
        let context = context.as_ref()?;
        Some(
            context
                .session
                .begin_boundary(boundary, context.batch_id, subject, item_count),
        )
    })
}

/// Explicitly finishes a worker boundary when a session is installed.
pub fn finish_worker_boundary(token: Option<BoundaryToken>, outcome: BoundaryOutcome) {
    if let Some(token) = token
        && let Some(session) = token.session.clone()
    {
        session.finish_boundary(token, outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_closed_unique_and_has_nonzero_bounds() {
        assert_eq!(boundary_registry().len(), BOUNDARY_COUNT);
        for (index, descriptor) in boundary_registry().iter().copied().enumerate() {
            assert_eq!(usize::from(descriptor.id() as u8), index + 1);
            assert!(descriptor.max_occurrence().get() > 0);
            assert_eq!(
                BoundaryId::try_from(descriptor.id() as u8),
                Ok(descriptor.id())
            );
            assert_eq!(
                boundary_registry()
                    .iter()
                    .filter(|row| row.source() == descriptor.source())
                    .count(),
                1
            );
        }
        assert_eq!(BoundaryId::try_from(0), Err(BoundaryLookupError));
        assert_eq!(BoundaryId::try_from(u8::MAX), Err(BoundaryLookupError));
    }

    #[test]
    fn illegal_fault_actions_and_successors_are_refused() {
        let first = NonZeroU32::new(1).expect("nonzero");
        assert_eq!(
            FaultPlan::single(
                BoundaryId::HandledVisibility,
                first,
                FaultAction::PreOperationError(InjectedErrorKind::Other),
            ),
            Err(FaultPlanError::ActionNotApplicable)
        );
        assert!(
            FaultPlan::single(
                BoundaryId::JournalAppendWrite,
                first,
                FaultAction::ShortPartialWrite {
                    bytes: NonZeroUsize::new(1).expect("nonzero"),
                    error: InjectedErrorKind::StorageFull,
                },
            )
            .is_ok()
        );
        assert!(
            !boundary_descriptor(BoundaryId::JournalSync)
                .allows_successor(BoundaryId::HandledVisibility)
        );
        assert!(!boundary_descriptor(BoundaryId::CheckpointWrite).allows_terminal_success());
    }

    #[test]
    fn overflow_is_visible_without_overwrite_or_product_result() {
        let session = NativeEvidenceSession::new(1, FaultPlan::none()).expect("session");
        for _ in 0..2 {
            let token = session.begin_boundary(BoundaryId::RotationDecision, 0, 0, 0);
            session.finish_boundary(token, BoundaryOutcome::Success);
        }
        let snapshot = session.snapshot();
        assert_eq!(snapshot.events().len(), 1);
        assert_eq!(
            snapshot.status(),
            EvidenceStatus::Failed(EvidenceFailure::EventOverflow)
        );
    }

    #[test]
    fn native_evidence_token_session_mismatch_fails_both_without_completion() {
        let first = NativeEvidenceSession::new(2, FaultPlan::none()).expect("first session");
        let second = NativeEvidenceSession::new(2, FaultPlan::none()).expect("second session");
        let first_token = first.begin_boundary(BoundaryId::RotationDecision, 0, 1, 1);
        let second_token = second.begin_boundary(BoundaryId::RotationDecision, 0, 2, 1);

        first.finish_boundary(second_token, BoundaryOutcome::Success);

        assert_eq!(
            first.snapshot().status(),
            EvidenceStatus::Failed(EvidenceFailure::SessionMismatch)
        );
        assert_eq!(
            second.snapshot().status(),
            EvidenceStatus::Failed(EvidenceFailure::SessionMismatch)
        );
        assert!(first.snapshot().events().is_empty());
        assert!(second.snapshot().events().is_empty());
        assert_eq!(
            first.snapshot().validate_closed_trace(),
            Err(TraceValidationError::Incomplete)
        );
        assert_eq!(
            second.snapshot().validate_closed_trace(),
            Err(TraceValidationError::Incomplete)
        );

        // The reciprocal misuse also cannot complete the still-open numeric
        // token in either failed session.
        second.finish_boundary(first_token, BoundaryOutcome::Success);
        assert!(first.snapshot().events().is_empty());
        assert!(second.snapshot().events().is_empty());
    }

    #[test]
    fn native_evidence_cloned_session_tokens_and_inactive_tokens_remain_valid() {
        let session = NativeEvidenceSession::new(2, FaultPlan::none()).expect("session");
        let cloned = session.clone();
        let token = session.begin_boundary(BoundaryId::RotationDecision, 0, 1, 1);
        cloned.finish_boundary(token, BoundaryOutcome::Success);
        session.finish_boundary(BoundaryToken::inactive(), BoundaryOutcome::Error);

        let snapshot = session.snapshot();
        assert_eq!(snapshot.status(), EvidenceStatus::Complete);
        assert_eq!(snapshot.events().len(), 1);
        snapshot
            .validate_closed_trace()
            .expect("same-session clone token should close normally");
    }
}
