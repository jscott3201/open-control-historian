use crate::ingress::{
    AdmissionPriority, AppendIdentity, BarrierDemand, ByteReservationLimits, IngressShared,
    MAX_OUTSTANDING_COMMANDS,
};
use och_core::{CanonicalAdmission, StoreId};
use och_store::{
    ActiveJournal, ActiveJournalConfig, ActiveJournalError, ActiveJournalInspection,
    ActiveJournalLimits, ActiveJournalOpenMode, PreparedAdmissionV1, RecoveredAdmissionV1,
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
    ) -> Result<Self, StoreOptionsError> {
        let directory_length = directory.as_os_str().as_encoded_bytes().len();
        if directory_length == 0 || directory_length > och_store::MAX_STORE_DIRECTORY_BYTES {
            return Err(StoreOptionsError::Journal(
                ActiveJournalError::InvalidOptions,
            ));
        }
        ActiveJournalConfig::new(directory.clone(), store_id, mode, journal_limits)
            .map_err(StoreOptionsError::Journal)?;
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

    pub(crate) fn active_config(&self) -> Result<ActiveJournalConfig, ActiveJournalError> {
        ActiveJournalConfig::new(
            self.directory.clone(),
            self.store_id,
            self.mode,
            self.journal_limits,
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
            .finish_non_exhaustive()
    }
}

/// Sanitized invalid runtime options.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOptionsError {
    /// Active-journal options were invalid.
    Journal(ActiveJournalError),
    /// Cross-option bounds were invalid.
    InvalidRelationships,
}

impl fmt::Display for StoreOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid historian store options")
    }
}

impl std::error::Error for StoreOptionsError {}

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
    store: Option<ActiveJournalInspection>,
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

    fn update(&self, store: ActiveJournalInspection, health: RuntimeHealth) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.store = Some(store);
        state.health = health;
    }

    fn update_store(&self, store: ActiveJournalInspection) {
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
        fallback: ActiveJournalInspection,
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
            store,
            pending_count,
            pending_bytes,
            health,
        }
    }
}

pub(crate) struct WorkerReady {
    pub(crate) inspection: ActiveJournalInspection,
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
        response: oneshot::Sender<Result<AppendResult, ActiveJournalError>>,
    },
    Published {
        slot: usize,
        priority: AdmissionPriority,
        barrier: BarrierDemand,
        frame_bytes: usize,
    },
    Barrier {
        response: oneshot::Sender<Result<(), ActiveJournalError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), ActiveJournalError>>,
    },
}

struct PendingDurable {
    slot: usize,
    bytes: usize,
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run_store_worker(
    options: StoreOptions,
    receiver: Receiver<WorkerMessage>,
    readiness: oneshot::Sender<Result<WorkerReady, ActiveJournalError>>,
    ingress: Arc<IngressShared>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
) {
    let config = match options.active_config() {
        Ok(config) => config,
        Err(error) => {
            let _ = readiness.send(Err(error));
            ingress.stop();
            return;
        }
    };
    let mut journal = match ActiveJournal::open(config) {
        Ok(journal) => journal,
        Err(error) => {
            let _ = readiness.send(Err(error));
            ingress.stop();
            return;
        }
    };
    let opened_at = Instant::now();
    let initial_health = if rotation_required(&journal, &options, opened_at) {
        RuntimeHealth::RotationRequired
    } else {
        RuntimeHealth::Healthy
    };
    inspection.update(journal.inspection(), initial_health);
    if readiness
        .send(Ok(WorkerReady {
            inspection: journal.inspection(),
            recovered: journal.recovered_records().to_vec(),
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
                    let _ = response.send(Err(ActiveJournalError::InvalidLayout));
                    fail_worker(&ingress, &inspection);
                    return;
                }
                if opened_at.elapsed() >= options.group_commit.rotation_age {
                    let admission = prepared.into_admission();
                    drop(admission);
                    let _ = response.send(Err(ActiveJournalError::RotationRequired));
                    inspection.set_health(RuntimeHealth::RotationRequired);
                    ingress.stop();
                    return;
                }
                let sequence = match journal.next_append_sequence() {
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
                        let _ = response.send(Err(ActiveJournalError::Journal(error.error())));
                        fail_worker(&ingress, &inspection);
                        return;
                    }
                };
                let end_offset = match journal.append(&frame) {
                    Ok(end_offset) => end_offset,
                    Err(error) => {
                        let _ = response.send(Err(error));
                        if error == ActiveJournalError::RotationRequired {
                            inspection.set_health(RuntimeHealth::RotationRequired);
                        } else {
                            fail_worker(&ingress, &inspection);
                        }
                        return;
                    }
                };
                let append =
                    AppendIdentity::new(journal.inspection().journal(), sequence.get(), end_offset);
                inspection.update_store(journal.inspection());
                if rotation_required(&journal, &options, opened_at) {
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
                if forced
                    && flush_pending(&mut journal, &mut pending, &ingress, &inspection).is_err()
                {
                    return;
                }
                if pending.is_empty() {
                    pending_since = None;
                }
            }
            Ok(WorkerMessage::Barrier { response }) => {
                if unpublished.is_some() {
                    let result = Err(ActiveJournalError::InvalidLayout);
                    let _ = response.send(result);
                    fail_worker(&ingress, &inspection);
                    return;
                }
                let result = flush_pending(&mut journal, &mut pending, &ingress, &inspection);
                pending_since = None;
                let _ = response.send(result);
                if result.is_err() {
                    return;
                }
            }
            Ok(WorkerMessage::Shutdown { response }) => {
                if unpublished.is_some() {
                    let result = Err(ActiveJournalError::InvalidLayout);
                    let _ = response.send(result);
                    fail_worker(&ingress, &inspection);
                    return;
                }
                let result = flush_pending(&mut journal, &mut pending, &ingress, &inspection);
                let _ = response.send(result);
                if result.is_ok() {
                    inspection.update(journal.inspection(), RuntimeHealth::Stopped);
                }
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !pending.is_empty()
                    && flush_pending(&mut journal, &mut pending, &ingress, &inspection).is_err()
                {
                    return;
                }
                pending_since = None;
            }
            Err(RecvTimeoutError::Disconnected) => {
                ingress.stop();
                return;
            }
        }
    }
}

fn flush_pending(
    journal: &mut ActiveJournal,
    pending: &mut Vec<PendingDurable>,
    ingress: &IngressShared,
    inspection: &InspectionShared,
) -> Result<(), ActiveJournalError> {
    if pending.is_empty() {
        return Ok(());
    }
    let cutoff = match journal.sync_pending() {
        Ok(cutoff) => cutoff,
        Err(error) => {
            fail_worker(ingress, inspection);
            return Err(error);
        }
    };
    for entry in pending.drain(..) {
        if !ingress.complete_durable(entry.slot, cutoff) {
            fail_worker(ingress, inspection);
            return Err(ActiveJournalError::InvalidLayout);
        }
    }
    inspection.update_store(journal.inspection());
    Ok(())
}

fn rotation_required(journal: &ActiveJournal, options: &StoreOptions, opened_at: Instant) -> bool {
    let store = journal.inspection();
    store.active_bytes() >= options.journal_limits.max_active_bytes()
        || store.active_records() >= options.journal_limits.max_active_records()
        || opened_at.elapsed() >= options.group_commit.rotation_age
}

fn fail_worker(ingress: &IngressShared, inspection: &InspectionShared) {
    inspection.set_health(RuntimeHealth::Faulted);
    ingress.stop();
}

pub(crate) fn spawn_worker_and_reaper(
    options: StoreOptions,
    receiver: Receiver<WorkerMessage>,
    readiness: oneshot::Sender<Result<WorkerReady, ActiveJournalError>>,
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
