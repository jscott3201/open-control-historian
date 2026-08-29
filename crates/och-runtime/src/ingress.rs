use och_core::{CollectionEnvelope, RetryClassification, RetryQualification};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Maximum distinct commands that may be queued or in flight for one runtime.
pub const MAX_OUTSTANDING_COMMANDS: usize = 16;

/// One atomic envelope and its caller-supplied retry qualification.
///
/// Construction verifies only that the envelope and retry qualification name
/// the same series and producer. The runtime trusts the external content
/// identity for retry comparison; it never hashes or verifies envelope content.
pub struct IngressCommand {
    envelope: CollectionEnvelope,
    retry: RetryQualification,
}

impl IngressCommand {
    /// Constructs one command after verifying its series and producer scope.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ScopeMismatchError`] retaining both inputs when
    /// either scope component differs.
    pub fn new(
        envelope: CollectionEnvelope,
        retry: RetryQualification,
    ) -> Result<Self, ScopeMismatchError> {
        if envelope.series().series_id() != retry.series_id()
            || envelope.series().producer_id() != retry.producer_id()
        {
            return Err(ScopeMismatchError {
                parts: Box::new((envelope, retry)),
            });
        }
        Ok(Self { envelope, retry })
    }

    /// Recovers the owned envelope and retry qualification.
    #[must_use]
    pub fn into_parts(self) -> (CollectionEnvelope, RetryQualification) {
        (self.envelope, self.retry)
    }
}

/// A sanitized command-construction failure caused by mismatched scope.
pub struct ScopeMismatchError {
    parts: Box<(CollectionEnvelope, RetryQualification)>,
}

impl ScopeMismatchError {
    /// Recovers the exact envelope and retry qualification supplied by the caller.
    #[must_use]
    pub fn into_parts(self) -> (CollectionEnvelope, RetryQualification) {
        *self.parts
    }
}

impl fmt::Debug for ScopeMismatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeMismatchError")
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ScopeMismatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("envelope and retry qualification scopes differ")
    }
}

impl Error for ScopeMismatchError {}

/// A cloneable, synchronous handle to one runtime's bounded volatile ingress.
///
/// Clones share the same fixed admission window but do not keep the writer task
/// alive. A handle that outlives its runtime rejects every command as closed.
#[derive(Clone)]
pub struct HistorianIngress {
    shared: Arc<IngressShared>,
}

impl HistorianIngress {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(IngressShared::new()),
        }
    }

    /// Immediately attempts to admit one owned command without waiting.
    ///
    /// Equivalent outstanding retries share the first command's receipt and do
    /// not take another slot. Conflicts are rejected without replacing evidence.
    /// For distinct work, the fixed bound includes queued and in-flight commands.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`TrySubmitError`] that retains the incoming command
    /// when admission is full, conflicting, or closed.
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
    /// The incoming envelope was discarded and the first command's receipt shared.
    Coalesced,
}

/// A successful admission result and its shared terminal receipt.
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

    /// Consumes the result and returns its terminal receipt.
    #[must_use]
    pub fn into_receipt(self) -> Receipt {
        self.receipt
    }

    /// Consumes the result and returns both public result components.
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

/// The closed reason an immediate submission was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrySubmitErrorKind {
    /// All 16 distinct slots are outstanding.
    Full,
    /// The same retry scope and key are outstanding with different content identity.
    RetryConflict,
    /// Admission has closed or its private state could not be trusted.
    Closed,
}

/// A sanitized immediate-admission failure retaining the incoming command.
pub struct TrySubmitError {
    kind: TrySubmitErrorKind,
    command: Box<IngressCommand>,
}

impl TrySubmitError {
    fn new(kind: TrySubmitErrorKind, command: IngressCommand) -> Self {
        Self {
            kind,
            command: Box::new(command),
        }
    }

    /// Returns the closed rejection reason.
    #[must_use]
    pub const fn kind(&self) -> TrySubmitErrorKind {
        self.kind
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
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TrySubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            TrySubmitErrorKind::Full => "historian ingress is full",
            TrySubmitErrorKind::RetryConflict => {
                "retry qualification conflicts with outstanding work"
            }
            TrySubmitErrorKind::Closed => "historian ingress is closed",
        })
    }
}

impl Error for TrySubmitError {}

/// The terminal result shared by all receipts for one accepted work item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptOutcome {
    /// The private writer consumed and dropped the volatile command.
    ///
    /// This does not mean persisted, durable, published, queryable, or restart-safe.
    WriterHandled,
    /// The writer stopped before handling the command.
    WriterStopped,
}

/// An awaitable shared receipt for one accepted volatile work item.
///
/// Cloning, dropping, or cancelling a wait changes no accepted work. The receipt
/// retains only shared terminal state and exposes no writer, queue, or Tokio type.
#[derive(Clone)]
pub struct Receipt {
    terminal: Arc<ReceiptTerminal>,
}

impl Receipt {
    /// Waits until the work item reaches exactly one terminal outcome.
    #[must_use]
    pub async fn wait(self) -> ReceiptOutcome {
        loop {
            // Establish the notification before inspecting state so completion
            // cannot fall between the check and await. The terminal atomic is
            // latest state for receipts created after completion as well.
            let mut notified = Box::pin(self.terminal.notify.notified());
            notified.as_mut().enable();
            if let Some(outcome) = self.terminal.outcome() {
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

const RECEIPT_PENDING: u8 = 0;
const RECEIPT_HANDLED: u8 = 1;
const RECEIPT_STOPPED: u8 = 2;

struct ReceiptTerminal {
    state: AtomicU8,
    notify: Notify,
}

impl ReceiptTerminal {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(RECEIPT_PENDING),
            notify: Notify::new(),
        }
    }

    fn resolve(&self, outcome: ReceiptOutcome) -> bool {
        let terminal = match outcome {
            ReceiptOutcome::WriterHandled => RECEIPT_HANDLED,
            ReceiptOutcome::WriterStopped => RECEIPT_STOPPED,
        };
        self.state
            .compare_exchange(
                RECEIPT_PENDING,
                terminal,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn outcome(&self) -> Option<ReceiptOutcome> {
        match self.state.load(Ordering::Acquire) {
            RECEIPT_HANDLED => Some(ReceiptOutcome::WriterHandled),
            RECEIPT_STOPPED => Some(ReceiptOutcome::WriterStopped),
            _ => None,
        }
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
            // Waking receipt futures happens only after the admission lock has
            // been released, so no caller waker runs in the critical section.
            terminal.notify.notify_waiters();
        }
    }
}

pub(crate) struct IngressShared {
    state: Mutex<IngressState>,
    notify: Notify,
}

impl IngressShared {
    fn new() -> Self {
        Self {
            state: Mutex::new(IngressState::new()),
            notify: Notify::new(),
        }
    }

    fn try_submit(&self, command: IngressCommand) -> Result<Submission, TrySubmitError> {
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

        for slot in state.slots.iter().flatten() {
            match slot.qualification.classify(&command.retry) {
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

        let Some(slot_index) = state.slots.iter().position(Option::is_none) else {
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            self.notify.notify_one();
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        };
        if !state.queue.can_push() {
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            self.notify.notify_one();
            return Err(TrySubmitError::new(TrySubmitErrorKind::Closed, command));
        }

        let qualification = command.retry.clone();
        let terminal = Arc::new(ReceiptTerminal::new());
        state.slots[slot_index] = Some(Slot {
            qualification,
            command: Some(command),
            terminal: Arc::clone(&terminal),
            phase: SlotPhase::Queued,
        });
        state.queue.push(slot_index);
        drop(state);
        self.notify.notify_one();
        Ok(Submission {
            disposition: SubmissionDisposition::Queued,
            receipt: Receipt { terminal },
        })
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
                self.notify.notify_one();
                return NextWork::Failed;
            }
        };

        if let Some(slot_index) = state.queue.pop() {
            let Some(slot) = state.slots.get_mut(slot_index).and_then(Option::as_mut) else {
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return NextWork::Failed;
            };
            if slot.phase != SlotPhase::Queued {
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return NextWork::Failed;
            }
            let Some(command) = slot.command.take() else {
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                return NextWork::Failed;
            };
            slot.phase = SlotPhase::InFlight;
            drop(state);
            return NextWork::Work(Box::new(InFlightCommand {
                command: Some(command),
                slot_index,
                shared: Some(Arc::clone(self)),
            }));
        }

        if state.closed {
            if state.active_count() == 0 {
                NextWork::Drained
            } else {
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                self.notify.notify_one();
                NextWork::Failed
            }
        } else {
            NextWork::Empty
        }
    }

    fn complete(
        &self,
        slot_index: usize,
        outcome: ReceiptOutcome,
        mut command: Option<IngressCommand>,
    ) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                drop(command.take());
                notifications.wake();
                self.notify.notify_one();
                return;
            }
        };
        let Some(slot) = state.slots.get_mut(slot_index).and_then(Option::take) else {
            drop(state);
            drop(command.take());
            return;
        };
        if slot.phase != SlotPhase::InFlight {
            state.slots[slot_index] = Some(slot);
            let notifications = state.stop_all();
            drop(state);
            drop(command.take());
            notifications.wake();
            self.notify.notify_one();
            return;
        }
        // Destroy the volatile command while its slot is still protected, then
        // single-assign terminal state and release the key in the same critical
        // section. Runtime stop therefore linearizes wholly before or after
        // handling; it cannot turn already-handled work into WriterStopped.
        drop(command.take());
        if !slot.terminal.resolve(outcome) {
            let terminal = Arc::clone(&slot.terminal);
            state.slots[slot_index] = Some(slot);
            let notifications = state.stop_all();
            drop(state);
            notifications.wake();
            terminal.notify.notify_waiters();
            self.notify.notify_one();
            return;
        }
        let terminal = slot.terminal;
        drop(state);
        terminal.notify.notify_waiters();
    }

    #[cfg(test)]
    fn test_counts(&self) -> (usize, usize, usize) {
        match self.state.lock() {
            Ok(state) => {
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
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let notifications = state.stop_all();
                drop(state);
                notifications.wake();
                (0, 0, 0)
            }
        }
    }
}

pub(crate) enum NextWork {
    Work(Box<InFlightCommand>),
    Empty,
    Drained,
    Failed,
}

pub(crate) struct InFlightCommand {
    command: Option<IngressCommand>,
    slot_index: usize,
    shared: Option<Arc<IngressShared>>,
}

impl InFlightCommand {
    pub(crate) fn finish_handled(mut self) {
        if let Some(shared) = self.shared.take() {
            shared.complete(
                self.slot_index,
                ReceiptOutcome::WriterHandled,
                self.command.take(),
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn test_tag(&self) -> u8 {
        self.command
            .as_ref()
            .expect("in-flight command should exist before completion")
            .retry
            .content()
            .sha256()[0]
    }
}

impl Drop for InFlightCommand {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            shared.complete(
                self.slot_index,
                ReceiptOutcome::WriterStopped,
                self.command.take(),
            );
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SlotPhase {
    Queued,
    InFlight,
}

struct Slot {
    qualification: RetryQualification,
    command: Option<IngressCommand>,
    terminal: Arc<ReceiptTerminal>,
    phase: SlotPhase,
}

struct IngressState {
    closed: bool,
    slots: Vec<Option<Slot>>,
    queue: FixedQueue,
}

impl IngressState {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(MAX_OUTSTANDING_COMMANDS);
        slots.resize_with(MAX_OUTSTANDING_COMMANDS, || None);
        Self {
            closed: false,
            slots,
            queue: FixedQueue::new(),
        }
    }

    fn active_count(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    fn stop_all(&mut self) -> TerminalNotifications {
        self.closed = true;
        self.queue.clear();
        let mut notifications = TerminalNotifications::new();
        for slot in &mut self.slots {
            if let Some(slot) = slot.take()
                && slot.terminal.resolve(ReceiptOutcome::WriterStopped)
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
