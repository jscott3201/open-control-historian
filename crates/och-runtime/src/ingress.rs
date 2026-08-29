use crate::latest::{
    LatestReadError, LatestSnapshot, LatestState, PreparedPublication, PublicationFault,
    PublishedObservation,
};
use och_core::{CanonicalAdmission, RetryClassification, RetryQualification, StoreId};
use och_store::{
    DurableCutoff, JournalIdentity, JournalV1Error, PreparedAdmissionV1, admission_frame_len_v1,
};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Maximum distinct commands that may be queued, appended, or awaiting durability.
pub const MAX_OUTSTANDING_COMMANDS: usize = 16;

/// Admission class used only for byte reservation and barrier demand.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionPriority {
    /// Capacity protected from lower classes and an immediate barrier demand.
    Protected,
    /// Capacity protected from bulk work.
    Normal,
    /// Best-effort capacity not reserved for higher classes.
    Bulk,
}

/// Per-command barrier timing; it never changes FIFO semantic ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BarrierDemand {
    /// Participate in the bounded group-commit window.
    Group,
    /// Force a barrier after this command is handled.
    Immediate,
}

/// Validated exact encoded-byte reservation law.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteReservationLimits {
    total: usize,
    protected_reserve: usize,
    normal_reserve: usize,
}

impl ByteReservationLimits {
    /// Validates a finite global maximum and nested higher-class reserves.
    ///
    /// # Errors
    ///
    /// Refuses zero global capacity or reserves whose sum exceeds it.
    pub const fn new(
        max_outstanding_bytes: usize,
        protected_reserved_bytes: usize,
        normal_reserved_bytes: usize,
    ) -> Result<Self, ReservationOptionsError> {
        if max_outstanding_bytes == 0
            || protected_reserved_bytes > max_outstanding_bytes
            || normal_reserved_bytes > max_outstanding_bytes - protected_reserved_bytes
        {
            return Err(ReservationOptionsError);
        }
        Ok(Self {
            total: max_outstanding_bytes,
            protected_reserve: protected_reserved_bytes,
            normal_reserve: normal_reserved_bytes,
        })
    }

    /// Returns the global exact encoded-byte maximum.
    #[must_use]
    pub const fn max_outstanding_bytes(self) -> usize {
        self.total
    }

    /// Returns bytes unavailable to normal and bulk admissions.
    #[must_use]
    pub const fn protected_reserved_bytes(self) -> usize {
        self.protected_reserve
    }

    /// Returns additional bytes unavailable to bulk admissions.
    #[must_use]
    pub const fn normal_reserved_bytes(self) -> usize {
        self.normal_reserve
    }

    const fn class_ceiling(self, priority: AdmissionPriority) -> usize {
        match priority {
            AdmissionPriority::Protected => self.total,
            AdmissionPriority::Normal => self.total - self.protected_reserve,
            AdmissionPriority::Bulk => self.total - self.protected_reserve - self.normal_reserve,
        }
    }
}

/// Sanitized invalid byte-reservation configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationOptionsError;

impl fmt::Display for ReservationOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid byte reservation limits")
    }
}

impl Error for ReservationOptionsError {}

/// One declaration-authorized canonical admission and scheduling policy.
///
/// Core has already validated semantic authority. Policy controls resources
/// only and cannot reorder or reinterpret evidence.
pub struct IngressCommand {
    admission: CanonicalAdmission,
    priority: AdmissionPriority,
    barrier: BarrierDemand,
}

impl IngressCommand {
    /// Constructs normal-priority group-commit work.
    #[must_use]
    pub const fn new(admission: CanonicalAdmission) -> Self {
        Self::with_policy(admission, AdmissionPriority::Normal, BarrierDemand::Group)
    }

    /// Constructs work with explicit reservation class and barrier demand.
    #[must_use]
    pub const fn with_policy(
        admission: CanonicalAdmission,
        priority: AdmissionPriority,
        barrier: BarrierDemand,
    ) -> Self {
        Self {
            admission,
            priority,
            barrier,
        }
    }

    /// Borrows the complete canonical admission.
    #[must_use]
    pub const fn admission(&self) -> &CanonicalAdmission {
        &self.admission
    }

    /// Returns the resource reservation class.
    #[must_use]
    pub const fn priority(&self) -> AdmissionPriority {
        self.priority
    }

    /// Returns requested barrier timing.
    #[must_use]
    pub const fn barrier_demand(&self) -> BarrierDemand {
        self.barrier
    }

    /// Recovers all owned command components.
    #[must_use]
    pub fn into_parts(self) -> (CanonicalAdmission, AdmissionPriority, BarrierDemand) {
        (self.admission, self.priority, self.barrier)
    }

    /// Recovers the complete canonical admission.
    #[must_use]
    pub fn into_admission(self) -> CanonicalAdmission {
        self.admission
    }
}

/// A cloneable synchronous handle to one durable runtime's bounded ingress.
#[derive(Clone)]
pub struct HistorianIngress {
    shared: Arc<IngressShared>,
}

impl HistorianIngress {
    pub(crate) fn new_with_limits(store_id: StoreId, byte_limits: ByteReservationLimits) -> Self {
        Self {
            shared: Arc::new(IngressShared::new(store_id, byte_limits)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(store_id: StoreId) -> Self {
        Self::new_with_limits(
            store_id,
            ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("test byte limits"),
        )
    }

    /// Returns the immutable store scope of this ingress instance.
    #[must_use]
    pub fn store_id(&self) -> StoreId {
        self.shared.store_id()
    }

    /// Counts bytes, reserves atomically, then encodes without waiting.
    ///
    /// Equivalent outstanding retries share both receipt stages.
    ///
    /// # Errors
    ///
    /// Returns a sanitized refusal retaining the exact incoming command.
    pub fn try_submit(&self, command: IngressCommand) -> Result<Submission, TrySubmitError> {
        self.shared.try_submit(command)
    }

    pub(crate) fn shared(&self) -> Arc<IngressShared> {
        Arc::clone(&self.shared)
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close_admission();
    }

    pub(crate) fn stop(&self) {
        self.shared.stop();
    }

    #[cfg(test)]
    pub(crate) fn test_counts(&self) -> (usize, usize, usize) {
        self.shared.test_counts()
    }

    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self
                .shared
                .state
                .lock()
                .expect("test should acquire an unpoisoned ingress lock");
            panic!("injected ingress lock poison");
        }));
        assert!(result.is_err(), "test poison panic should be caught");
    }
}

impl fmt::Debug for HistorianIngress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistorianIngress")
            .finish_non_exhaustive()
    }
}

/// Whether submission created new work or joined equivalent outstanding work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionDisposition {
    /// The command became a new outstanding FIFO work item.
    Queued,
    /// The incoming admission was discarded and the first command's receipt shared.
    Coalesced,
}

/// A successful admission result and shared two-stage receipt.
pub struct Submission {
    disposition: SubmissionDisposition,
    receipt: Receipt,
}

impl Submission {
    /// Reports whether this call queued or coalesced work.
    #[must_use]
    pub const fn disposition(&self) -> SubmissionDisposition {
        self.disposition
    }

    /// Consumes the result and returns its receipt.
    #[must_use]
    pub fn into_receipt(self) -> Receipt {
        self.receipt
    }

    /// Consumes the result and returns both components.
    #[must_use]
    pub fn into_parts(self) -> (SubmissionDisposition, Receipt) {
        (self.disposition, self.receipt)
    }
}

impl fmt::Debug for Submission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Submission")
            .field("disposition", &self.disposition)
            .finish_non_exhaustive()
    }
}

/// The reason an immediate submission was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrySubmitErrorKind {
    /// Admission has closed or its state could not be trusted.
    Closed,
    /// The canonical admission belongs to another store.
    StoreMismatch,
    /// Canonical evidence could not be represented as Journal V1.
    Encoding,
    /// The class/global exact encoded-byte law refused the frame.
    ByteCapacity,
    /// All 16 distinct slots are outstanding.
    Full,
    /// The same retry scope and key carry different content.
    RetryConflict,
}

/// A sanitized immediate-admission failure retaining the incoming command.
pub struct TrySubmitError {
    kind: TrySubmitErrorKind,
    encoding: Option<JournalV1Error>,
    command: Box<IngressCommand>,
}

impl TrySubmitError {
    fn new(kind: TrySubmitErrorKind, command: IngressCommand) -> Self {
        Self {
            kind,
            encoding: None,
            command: Box::new(command),
        }
    }

    fn encoding(error: JournalV1Error, command: IngressCommand) -> Self {
        Self {
            kind: TrySubmitErrorKind::Encoding,
            encoding: Some(error),
            command: Box::new(command),
        }
    }

    /// Returns the rejection reason.
    #[must_use]
    pub const fn kind(&self) -> TrySubmitErrorKind {
        self.kind
    }

    /// Returns the closed Journal V1 error for an encoding refusal.
    #[must_use]
    pub const fn encoding_error(&self) -> Option<JournalV1Error> {
        self.encoding
    }

    /// Recovers the exact incoming command.
    #[must_use]
    pub fn into_command(self) -> IngressCommand {
        *self.command
    }
}

impl fmt::Debug for TrySubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrySubmitError")
            .field("kind", &self.kind)
            .field("encoding", &self.encoding)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TrySubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            TrySubmitErrorKind::Closed => "historian ingress is closed",
            TrySubmitErrorKind::StoreMismatch => {
                "canonical admission store does not match historian runtime"
            }
            TrySubmitErrorKind::Encoding => "canonical admission cannot be framed",
            TrySubmitErrorKind::ByteCapacity => "encoded-byte admission capacity is full",
            TrySubmitErrorKind::Full => "historian ingress is full",
            TrySubmitErrorKind::RetryConflict => "outstanding retry conflict",
        })
    }
}

impl Error for TrySubmitError {}

/// Exact identity assigned after append and volatile publication decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendIdentity {
    journal: JournalIdentity,
    append_sequence: u64,
    end_offset: u64,
}

impl AppendIdentity {
    pub(crate) const fn new(
        journal: JournalIdentity,
        append_sequence: u64,
        end_offset: u64,
    ) -> Self {
        Self {
            journal,
            append_sequence,
            end_offset,
        }
    }

    /// Returns the stable active-journal identity.
    #[must_use]
    pub const fn journal(self) -> JournalIdentity {
        self.journal
    }

    /// Returns the writer-assigned append sequence.
    #[must_use]
    pub const fn append_sequence(self) -> u64 {
        self.append_sequence
    }

    /// Returns the exact frame end offset.
    #[must_use]
    pub const fn end_offset(self) -> u64 {
        self.end_offset
    }
}

/// Non-durable handled-stage result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandledOutcome {
    /// Append and volatile publication decision completed.
    WriterHandled(AppendIdentity),
    /// The writer stopped before reaching this stage.
    WriterStopped,
}

/// Durable-stage proof covering one exact append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableCommit {
    append: AppendIdentity,
    cutoff: DurableCutoff,
}

impl DurableCommit {
    /// Returns this work's exact append identity.
    #[must_use]
    pub const fn append(self) -> AppendIdentity {
        self.append
    }

    /// Returns the synchronized cutoff covering the append.
    #[must_use]
    pub const fn durable_cutoff(self) -> DurableCutoff {
        self.cutoff
    }
}

/// Durable-stage terminal result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableOutcome {
    /// Journal and checkpoint synchronization cover the append.
    Durable(DurableCommit),
    /// The writer stopped without proving durability.
    WriterStopped,
}

/// Compatibility handled-stage result returned by [`Receipt::wait`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptOutcome {
    /// Handled and published, but not necessarily durable.
    WriterHandled,
    /// The writer stopped before handling.
    WriterStopped,
}

/// Cloneable wait handles for distinct handled and durable stages.
#[derive(Clone)]
pub struct Receipt {
    terminal: Arc<ReceiptTerminal>,
}

impl Receipt {
    /// Waits for the legacy explicitly non-durable handled stage.
    #[must_use]
    pub async fn wait(self) -> ReceiptOutcome {
        match self.wait_handled().await {
            HandledOutcome::WriterHandled(_) => ReceiptOutcome::WriterHandled,
            HandledOutcome::WriterStopped => ReceiptOutcome::WriterStopped,
        }
    }

    /// Waits for append plus volatile publication decision.
    #[must_use]
    pub async fn wait_handled(self) -> HandledOutcome {
        loop {
            let mut notified = Box::pin(self.terminal.notify.notified());
            notified.as_mut().enable();
            if let Some(outcome) = self.terminal.handled() {
                return outcome;
            }
            notified.await;
        }
    }

    /// Waits for a synchronized cutoff or terminal stop.
    #[must_use]
    pub async fn wait_durable(self) -> DurableOutcome {
        loop {
            let mut notified = Box::pin(self.terminal.notify.notified());
            notified.as_mut().enable();
            if let Some(outcome) = self.terminal.durable() {
                return outcome;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.terminal, &other.terminal)
    }
}

impl fmt::Debug for Receipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Receipt").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct ReceiptState {
    handled: Option<HandledOutcome>,
    durable: Option<DurableOutcome>,
}

struct ReceiptTerminal {
    state: Mutex<ReceiptState>,
    notify: Notify,
}

impl ReceiptTerminal {
    fn new() -> Self {
        Self {
            state: Mutex::new(ReceiptState::default()),
            notify: Notify::new(),
        }
    }

    fn resolve_handled(&self, append: AppendIdentity) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.handled.is_some() {
            return false;
        }
        state.handled = Some(HandledOutcome::WriterHandled(append));
        true
    }

    fn resolve_durable(&self, commit: DurableCommit) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.durable.is_some() {
            return false;
        }
        state.durable = Some(DurableOutcome::Durable(commit));
        true
    }

    fn resolve_stopped(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = state.handled.is_none() || state.durable.is_none();
        state.handled.get_or_insert(HandledOutcome::WriterStopped);
        state.durable.get_or_insert(DurableOutcome::WriterStopped);
        changed
    }

    fn handled(&self) -> Option<HandledOutcome> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .handled
    }

    fn durable(&self) -> Option<DurableOutcome> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .durable
    }
}

struct TerminalNotifications {
    terminals: [Option<Arc<ReceiptTerminal>>; MAX_OUTSTANDING_COMMANDS],
    len: usize,
}

impl TerminalNotifications {
    fn new() -> Self {
        Self {
            terminals: std::array::from_fn(|_| None),
            len: 0,
        }
    }

    fn push(&mut self, terminal: Arc<ReceiptTerminal>) {
        if self.len < MAX_OUTSTANDING_COMMANDS {
            self.terminals[self.len] = Some(terminal);
            self.len += 1;
        }
    }

    fn wake(self) {
        for terminal in self.terminals.into_iter().flatten() {
            terminal.notify.notify_waiters();
        }
    }
}

pub(crate) struct IngressShared {
    store_id: StoreId,
    state: Mutex<IngressState>,
    notify: Notify,
}

impl IngressShared {
    fn new(store_id: StoreId, byte_limits: ByteReservationLimits) -> Self {
        Self {
            store_id,
            state: Mutex::new(IngressState::new(store_id, byte_limits)),
            notify: Notify::new(),
        }
    }

    pub(crate) const fn store_id(&self) -> StoreId {
        self.store_id
    }

    #[allow(clippy::too_many_lines)]
    fn try_submit(&self, command: IngressCommand) -> Result<Submission, TrySubmitError> {
        // Preserve the public closed/store precedence before potentially walking
        // the complete canonical record for exact byte measurement.
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
            }
        };
        if state.closed {
            drop(state);
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        }
        if command.admission.store_id() != self.store_id {
            drop(state);
            return Err(TrySubmitError::new(
                TrySubmitErrorKind::StoreMismatch,
                command,
            ));
        }
        drop(state);

        let measured = admission_frame_len_v1(command.admission());
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
            }
        };
        if state.closed {
            drop(state);
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        }
        if command.admission.store_id() != self.store_id {
            drop(state);
            return Err(TrySubmitError::new(
                TrySubmitErrorKind::StoreMismatch,
                command,
            ));
        }
        let frame_len = match measured {
            Ok(frame_len) => frame_len,
            Err(error) => {
                drop(state);
                return Err(TrySubmitError::encoding(error, command));
            }
        };
        for slot in state.slots.iter().flatten() {
            match slot.qualification.classify(command.admission.retry()) {
                RetryClassification::Equivalent => {
                    let receipt = Receipt {
                        terminal: Arc::clone(&slot.terminal),
                    };
                    drop(state);
                    drop(command);
                    return Ok(Submission {
                        disposition: SubmissionDisposition::Coalesced,
                        receipt,
                    });
                }
                RetryClassification::Conflict => {
                    drop(state);
                    return Err(TrySubmitError::new(
                        TrySubmitErrorKind::RetryConflict,
                        command,
                    ));
                }
                RetryClassification::Distinct => {}
            }
        }
        if state.active_count() == MAX_OUTSTANDING_COMMANDS {
            drop(state);
            return Err(TrySubmitError::new(TrySubmitErrorKind::Full, command));
        }
        if !state.can_reserve(command.priority, frame_len) {
            drop(state);
            return Err(TrySubmitError::new(
                TrySubmitErrorKind::ByteCapacity,
                command,
            ));
        }
        let Some(slot_index) = state.slots.iter().position(Option::is_none) else {
            drop(state);
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        };
        let qualification = command.admission.retry().clone();
        let priority = command.priority;
        let barrier = command.barrier;
        let terminal = Arc::new(ReceiptTerminal::new());
        state.reserved_bytes += frame_len;
        state.slots[slot_index] = Some(Slot {
            qualification,
            prepared: None,
            retained_admission: None,
            terminal: Arc::clone(&terminal),
            phase: SlotPhase::Preparing,
            reservation: frame_len,
            priority,
            barrier,
            append: None,
        });
        drop(state);

        let prepared = match PreparedAdmissionV1::new(command.admission) {
            Ok(prepared) if prepared.frame_len() == frame_len => prepared,
            Ok(prepared) => {
                let command =
                    IngressCommand::with_policy(prepared.into_admission(), priority, barrier);
                self.rollback_preparation(slot_index);
                return Err(TrySubmitError::encoding(
                    JournalV1Error::InvalidCanonicalData,
                    command,
                ));
            }
            Err(error) => {
                let journal_error = error.error();
                let command =
                    IngressCommand::with_policy(error.into_admission(), priority, barrier);
                self.rollback_preparation(slot_index);
                return Err(TrySubmitError::encoding(journal_error, command));
            }
        };
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                return Err(TrySubmitError::new(
                    TrySubmitErrorKind::Closed,
                    IngressCommand::with_policy(prepared.into_admission(), priority, barrier),
                ));
            }
        };
        let valid = state
            .slots
            .get(slot_index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| slot.phase == SlotPhase::Preparing);
        if !valid || !state.queue.can_push() {
            let command = IngressCommand::with_policy(prepared.into_admission(), priority, barrier);
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        }
        let slot = state.slots[slot_index].as_mut().expect("validated slot");
        slot.prepared = Some(prepared);
        slot.phase = SlotPhase::Queued;
        state.queue.push(slot_index);
        drop(state);
        self.notify.notify_one();
        Ok(Submission {
            disposition: SubmissionDisposition::Queued,
            receipt: Receipt { terminal },
        })
    }

    fn rollback_preparation(&self, slot_index: usize) {
        let terminal = match self.state.lock() {
            Ok(mut state) => {
                let Some(slot) = state.slots.get_mut(slot_index).and_then(Option::take) else {
                    return;
                };
                state.reserved_bytes = state.reserved_bytes.saturating_sub(slot.reservation);
                slot.terminal
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return;
            }
        };
        if terminal.resolve_stopped() {
            terminal.notify.notify_waiters();
        }
        self.notify.notify_one();
    }

    pub(crate) fn close_admission(&self) {
        let notifications = match self.state.lock() {
            Ok(mut state) => {
                state.closed = true;
                TerminalNotifications::new()
            }
            Err(poisoned) => poisoned.into_inner().stop_all(),
        };
        notifications.wake();
        self.notify.notify_one();
    }

    pub(crate) fn stop(&self) {
        let notifications = match self.state.lock() {
            Ok(mut state) => state.stop_all(),
            Err(poisoned) => poisoned.into_inner().stop_all(),
        };
        notifications.wake();
        self.notify.notify_one();
    }

    pub(crate) fn notified(&self) -> impl Future<Output = ()> + '_ {
        self.notify.notified()
    }

    pub(crate) fn take_next(self: &Arc<Self>) -> NextWork {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                return NextWork::Failed;
            }
        };
        if let Some(slot_index) = state.queue.pop() {
            let Some(slot) = state.slots.get_mut(slot_index).and_then(Option::as_mut) else {
                return NextWork::Failed;
            };
            if slot.phase != SlotPhase::Queued {
                return NextWork::Failed;
            }
            let Some(prepared) = slot.prepared.take() else {
                return NextWork::Failed;
            };
            slot.phase = SlotPhase::InFlight;
            let priority = slot.priority;
            let barrier = slot.barrier;
            drop(state);
            return NextWork::Work(Box::new(InFlightCommand {
                prepared: Some(prepared),
                slot_index,
                priority,
                barrier,
                shared: Some(Arc::clone(self)),
            }));
        }
        if state.closed && state.active_count() == 0 {
            return if state.latest.seal() {
                NextWork::Drained
            } else {
                NextWork::Failed
            };
        }
        if state.closed && !state.barrier_requested {
            state.barrier_requested = true;
            return NextWork::BarrierRequired;
        }
        NextWork::Empty
    }

    pub(crate) fn latest_snapshot(&self) -> Result<LatestSnapshot, LatestReadError> {
        let state = self.state.lock().map_err(|poisoned| {
            let mut state = poisoned.into_inner();
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            LatestReadError::unavailable()
        })?;
        state
            .latest
            .snapshot()
            .ok_or_else(LatestReadError::unavailable)
    }

    fn prepare_publication(
        &self,
        slot_index: usize,
        candidate: Option<PublishedObservation>,
    ) -> Result<PreparedPublication, PublicationFault> {
        let state = self.state.lock().map_err(|poisoned| {
            let mut state = poisoned.into_inner();
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            PublicationFault
        })?;
        if !state
            .slots
            .get(slot_index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| slot.phase == SlotPhase::InFlight)
        {
            return Err(PublicationFault);
        }
        state
            .latest
            .plan(candidate)
            .map(crate::latest::PublicationPlan::stage)
    }

    fn complete_handled(
        &self,
        slot_index: usize,
        admission: CanonicalAdmission,
        append: AppendIdentity,
        preparation: PreparedPublication,
        fault: CompletionFaultInjection,
    ) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                drop(admission);
                notifications.wake();
                return false;
            }
        };
        let valid = state
            .slots
            .get(slot_index)
            .and_then(Option::as_ref)
            .is_some_and(|slot| slot.phase == SlotPhase::InFlight);
        if !valid
            || !state.latest.can_complete(&preparation)
            || fault == CompletionFaultInjection::BeforeSwap
        {
            let notifications = state.stop_all();
            drop(state);
            drop(admission);
            notifications.wake();
            return false;
        }
        state.latest.commit(preparation);
        if fault == CompletionFaultInjection::AfterSwap {
            let notifications = state.stop_all();
            drop(state);
            drop(admission);
            notifications.wake();
            return false;
        }
        let slot = state.slots[slot_index]
            .as_mut()
            .expect("validated in-flight slot");
        slot.retained_admission = Some(admission);
        slot.append = Some(append);
        slot.phase = SlotPhase::AwaitingDurability;
        if !slot.terminal.resolve_handled(append) {
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            return false;
        }
        let terminal = Arc::clone(&slot.terminal);
        drop(state);
        terminal.notify.notify_waiters();
        true
    }

    pub(crate) fn complete_durable(&self, slot_index: usize, cutoff: DurableCutoff) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                return false;
            }
        };
        let Some(slot) = state.slots.get_mut(slot_index).and_then(Option::take) else {
            return false;
        };
        let Some(append) = slot.append else {
            state.slots[slot_index] = Some(slot);
            return false;
        };
        if slot.phase != SlotPhase::AwaitingDurability
            || append.journal != cutoff.journal()
            || append.append_sequence > cutoff.append_sequence()
            || append.end_offset > cutoff.end_offset()
        {
            state.slots[slot_index] = Some(slot);
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            return false;
        }
        state.reserved_bytes = state.reserved_bytes.saturating_sub(slot.reservation);
        if !slot
            .terminal
            .resolve_durable(DurableCommit { append, cutoff })
        {
            state.slots[slot_index] = Some(slot);
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            return false;
        }
        let terminal = slot.terminal;
        drop(state);
        terminal.notify.notify_waiters();
        self.notify.notify_one();
        true
    }

    pub(crate) fn pending_counts(&self) -> (usize, usize) {
        self.state
            .lock()
            .map_or((0, 0), |state| (state.active_count(), state.reserved_bytes))
    }

    #[cfg(test)]
    fn test_counts(&self) -> (usize, usize, usize) {
        self.state.lock().map_or((0, 0, 0), |state| {
            let queued = state
                .slots
                .iter()
                .flatten()
                .filter(|slot| slot.phase == SlotPhase::Queued)
                .count();
            let in_flight = state
                .slots
                .iter()
                .flatten()
                .filter(|slot| slot.phase == SlotPhase::InFlight)
                .count();
            (state.active_count(), queued, in_flight)
        })
    }
}

pub(crate) enum NextWork {
    Work(Box<InFlightCommand>),
    BarrierRequired,
    Empty,
    Drained,
    Failed,
}

pub(crate) struct InFlightCommand {
    prepared: Option<PreparedAdmissionV1>,
    slot_index: usize,
    priority: AdmissionPriority,
    barrier: BarrierDemand,
    shared: Option<Arc<IngressShared>>,
}

impl InFlightCommand {
    pub(crate) const fn slot_index(&self) -> usize {
        self.slot_index
    }

    pub(crate) const fn priority(&self) -> AdmissionPriority {
        self.priority
    }

    pub(crate) const fn barrier(&self) -> BarrierDemand {
        self.barrier
    }

    pub(crate) fn frame_len(&self) -> usize {
        self.prepared
            .as_ref()
            .map_or(0, PreparedAdmissionV1::frame_len)
    }

    pub(crate) fn take_prepared(&mut self) -> Option<PreparedAdmissionV1> {
        self.prepared.take()
    }

    pub(crate) fn prepare_publication(
        &self,
        admission: &CanonicalAdmission,
    ) -> Result<PreparedPublication, PublicationFault> {
        let envelope = admission.envelope();
        let candidate = envelope.observations().last().and_then(|observation| {
            observation.producer_position().map(|position| {
                PublishedObservation::new(envelope.series().clone(), observation.clone(), position)
            })
        });
        self.shared
            .as_ref()
            .ok_or(PublicationFault)?
            .prepare_publication(self.slot_index, candidate)
    }

    pub(crate) fn finish_handled(
        mut self,
        admission: CanonicalAdmission,
        append: AppendIdentity,
        preparation: PreparedPublication,
        fault: CompletionFaultInjection,
    ) -> bool {
        self.shared.take().is_some_and(|shared| {
            shared.complete_handled(self.slot_index, admission, append, preparation, fault)
        })
    }

    #[cfg(test)]
    pub(crate) fn test_tag(&self) -> u8 {
        self.prepared
            .as_ref()
            .expect("prepared command")
            .admission()
            .retry()
            .content()
            .sha256()[0]
    }
}

impl Drop for InFlightCommand {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            drop(self.prepared.take());
            shared.stop();
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum CompletionFaultInjection {
    None,
    BeforeSwap,
    AfterSwap,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SlotPhase {
    Preparing,
    Queued,
    InFlight,
    AwaitingDurability,
}

struct Slot {
    qualification: RetryQualification,
    prepared: Option<PreparedAdmissionV1>,
    retained_admission: Option<CanonicalAdmission>,
    terminal: Arc<ReceiptTerminal>,
    phase: SlotPhase,
    reservation: usize,
    priority: AdmissionPriority,
    barrier: BarrierDemand,
    append: Option<AppendIdentity>,
}

struct IngressState {
    closed: bool,
    slots: Vec<Option<Slot>>,
    queue: FixedQueue,
    latest: LatestState,
    byte_limits: ByteReservationLimits,
    reserved_bytes: usize,
    barrier_requested: bool,
}

impl IngressState {
    fn new(store_id: StoreId, byte_limits: ByteReservationLimits) -> Self {
        let mut slots = Vec::with_capacity(MAX_OUTSTANDING_COMMANDS);
        slots.resize_with(MAX_OUTSTANDING_COMMANDS, || None);
        Self {
            closed: false,
            slots,
            queue: FixedQueue::new(),
            latest: LatestState::new(store_id),
            byte_limits,
            reserved_bytes: 0,
            barrier_requested: false,
        }
    }

    fn active_count(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    fn can_reserve(&self, priority: AdmissionPriority, bytes: usize) -> bool {
        self.reserved_bytes
            .checked_add(bytes)
            .is_some_and(|total| total <= self.byte_limits.class_ceiling(priority))
    }

    fn stop_all(&mut self) -> TerminalNotifications {
        self.closed = true;
        self.latest.make_unavailable();
        self.queue.clear();
        self.reserved_bytes = 0;
        let mut notifications = TerminalNotifications::new();
        for slot in &mut self.slots {
            if let Some(slot) = slot.take()
                && slot.terminal.resolve_stopped()
            {
                notifications.push(slot.terminal);
            }
        }
        notifications
    }
}

struct FixedQueue {
    entries: [Option<usize>; MAX_OUTSTANDING_COMMANDS],
    head: usize,
    len: usize,
}

impl FixedQueue {
    const fn new() -> Self {
        Self {
            entries: [None; MAX_OUTSTANDING_COMMANDS],
            head: 0,
            len: 0,
        }
    }

    fn can_push(&self) -> bool {
        self.len < MAX_OUTSTANDING_COMMANDS
            && self.entries[(self.head + self.len) % MAX_OUTSTANDING_COMMANDS].is_none()
    }

    fn push(&mut self, slot_index: usize) {
        let tail = (self.head + self.len) % MAX_OUTSTANDING_COMMANDS;
        self.entries[tail] = Some(slot_index);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let value = self.entries[self.head].take();
        self.head = (self.head + 1) % MAX_OUTSTANDING_COMMANDS;
        self.len -= 1;
        value
    }

    fn clear(&mut self) {
        self.entries = [None; MAX_OUTSTANDING_COMMANDS];
        self.head = 0;
        self.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionPriority, BarrierDemand, ByteReservationLimits, DurableOutcome, HandledOutcome,
        IngressShared, ReceiptTerminal, Slot, SlotPhase,
    };
    use och_core::{
        ContentFormat, ContentIdentity, ContentVersion, ProducerId, RetryKey, RetryQualification,
        SeriesId, StoreId,
    };
    use std::sync::Arc;

    fn uuid_bytes(tag: u8) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = tag;
        bytes
    }

    #[test]
    fn preparation_rollback_stops_coalesced_receipts_and_releases_exact_bytes() {
        let shared = IngressShared::new(
            StoreId::from_bytes(uuid_bytes(1)).expect("store UUIDv7"),
            ByteReservationLimits::new(1_024, 0, 0).expect("byte limits"),
        );
        let terminal = Arc::new(ReceiptTerminal::new());
        let qualification = RetryQualification::new(
            SeriesId::from_bytes(uuid_bytes(2)).expect("series UUIDv7"),
            ProducerId::from_bytes(uuid_bytes(3)).expect("producer UUIDv7"),
            RetryKey::new("preparing".to_owned()).expect("retry key"),
            ContentIdentity::new(
                ContentFormat::new("application/x-och-test".to_owned()).expect("content format"),
                ContentVersion::new(1),
                [4; 32],
            ),
        );
        {
            let mut state = shared.state.lock().expect("unpoisoned state");
            state.reserved_bytes = 123;
            state.slots[0] = Some(Slot {
                qualification,
                prepared: None,
                retained_admission: None,
                terminal: Arc::clone(&terminal),
                phase: SlotPhase::Preparing,
                reservation: 123,
                priority: AdmissionPriority::Normal,
                barrier: BarrierDemand::Group,
                append: None,
            });
        }

        shared.rollback_preparation(0);

        assert_eq!(terminal.handled(), Some(HandledOutcome::WriterStopped));
        assert_eq!(terminal.durable(), Some(DurableOutcome::WriterStopped));
        let state = shared.state.lock().expect("unpoisoned state");
        assert_eq!(state.reserved_bytes, 0);
        assert!(state.slots[0].is_none());
    }
}
