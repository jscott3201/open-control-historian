#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Bounded durable active-journal ingress and volatile latest snapshots.
//!
//! Each [`HistorianRuntime`] owns one immutable store scope, one Tokio
//! coordinator, and one dedicated blocking active-journal writer joined by a
//! fixed reaper. Handled and durable receipt stages are distinct. Latest state
//! remains volatile and restarts empty.

mod ingress;
mod latest;
mod store_worker;

pub use ingress::{
    AdmissionPriority, AppendIdentity, BarrierDemand, ByteReservationLimits, DurableCommit,
    DurableOutcome, HandledOutcome, HistorianIngress, IngressCommand, MAX_OUTSTANDING_COMMANDS,
    Receipt, ReceiptOutcome, ReservationOptionsError, Submission, SubmissionDisposition,
    TrySubmitError, TrySubmitErrorKind,
};
pub use latest::{
    LatestReadError, LatestReadHandle, LatestSnapshot, MAX_PUBLISHED_SERIES, PublishedObservation,
};
pub use store_worker::{
    GroupCommitPolicy, RegistryCommit, RegistryError, RegistryOperation, RegistryOutcome,
    RuntimeHealth, RuntimeInspection, StoreOptions, StoreOptionsError,
};

use ingress::{CompletionFaultInjection, IngressShared, NextWork};
use och_core::StoreId;
use och_store::{ManifestStoreError, RecoveredAdmissionV1};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use store_worker::{
    AppendResult, InspectionShared, WorkerMessage, spawn_worker_and_reaper, try_send,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex as AsyncMutex, Semaphore, oneshot};
use tokio::task::{JoinError, JoinHandle};

/// One open filesystem-backed Historian runtime.
///
/// Drop signals fail-stop without joining. The fixed reaper remains responsible
/// for eventually joining the blocking writer. Use [`HistorianRuntime::shutdown`]
/// for FIFO drain, forced durability, sealed latest state, and joined completion.
pub struct HistorianRuntime {
    ingress: HistorianIngress,
    writer: Option<JoinHandle<WriterExit>>,
    store_sender: Option<SyncSender<WorkerMessage>>,
    stop: Arc<AtomicBool>,
    reaped: Option<oneshot::Receiver<()>>,
    inspection: InspectionShared,
    initial_inspection: och_store::ManifestStoreInspection,
    recovered: Arc<[RecoveredAdmissionV1]>,
    control_gate: Arc<AsyncMutex<()>>,
    control_admission: Arc<Semaphore>,
    shutdown_complete: bool,
}

impl fmt::Debug for HistorianRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistorianRuntime")
            .finish_non_exhaustive()
    }
}

impl HistorianRuntime {
    /// Opens one fixed active journal and its durable runtime authority.
    ///
    /// All filesystem activity occurs on the dedicated blocking writer. This
    /// future returns only after create/open, lock, scan, recovery convergence,
    /// and coordinator readiness complete.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`StartError`] for executor, worker, or store refusal.
    pub async fn open(options: StoreOptions) -> Result<Self, StartError> {
        Self::open_inner(options, WriterOptions::production()).await
    }

    async fn open_inner(
        store_options: StoreOptions,
        options: WriterOptions,
    ) -> Result<Self, StartError> {
        let executor = Handle::try_current().map_err(|_| StartError::NoActiveRuntime)?;
        let store_id = store_options.store_id();
        let ingress = HistorianIngress::new_with_limits(
            store_id,
            store_options.byte_limits(),
            store_options.retry(),
        );
        let inspection = InspectionShared::new();
        let stop = Arc::new(AtomicBool::new(false));
        let (store_sender, store_receiver) = sync_channel(MAX_OUTSTANDING_COMMANDS);
        let (store_ready_tx, store_ready_rx) = oneshot::channel();
        let (reaped_tx, reaped_rx) = oneshot::channel();
        let control_gate = Arc::new(AsyncMutex::new(()));
        let control_admission = Arc::new(Semaphore::new(MAX_OUTSTANDING_COMMANDS));
        spawn_worker_and_reaper(
            store_options,
            store_receiver,
            store_ready_tx,
            ingress.shared(),
            inspection.clone(),
            Arc::clone(&stop),
            reaped_tx,
        )
        .map_err(|_| StartError::WorkerThreadUnavailable)?;
        let mut open_guard = OpenGuard::new(Arc::clone(&stop), store_sender.clone());
        let (readiness_tx, readiness_rx) = oneshot::channel();
        #[cfg(test)]
        let cancel_before_readiness = options.behavior == TestBehavior::CancelBeforeReadiness;
        let writer = executor.spawn(run_writer(
            options,
            readiness_tx,
            store_ready_rx,
            ingress.shared(),
            store_sender.clone(),
            inspection.clone(),
            Arc::clone(&stop),
            Arc::clone(&control_gate),
        ));
        let mut startup = StartupGuard::new(writer);

        #[cfg(test)]
        if cancel_before_readiness {
            startup.abort();
        }

        match readiness_rx.await {
            Ok(Ok(ready)) => {
                open_guard.disarm();
                Ok(Self {
                    ingress,
                    writer: Some(startup.transfer()),
                    store_sender: Some(store_sender),
                    stop,
                    reaped: Some(reaped_rx),
                    inspection,
                    initial_inspection: ready.inspection,
                    recovered: ready.recovered.into(),
                    control_gate,
                    control_admission,
                    shutdown_complete: false,
                })
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                let result = startup.join().await;
                startup.disarm();
                match result {
                    Ok(_) => Err(StartError::WriterExitedBeforeReadiness),
                    Err(error) => Err(classify_start_join_error(&error)),
                }
            }
        }
    }

    /// Returns a cloneable handle to bounded durable ingress.
    ///
    /// The handle contains no executor or public Tokio primitive. It may outlive
    /// this runtime, but after shutdown or Drop it rejects commands as closed.
    #[must_use]
    pub fn ingress(&self) -> HistorianIngress {
        self.ingress.clone()
    }

    /// Returns the immutable store scope of this runtime instance.
    #[must_use]
    pub fn store_id(&self) -> StoreId {
        self.ingress.store_id()
    }

    /// Returns a cloneable synchronous reader for this runtime's latest registry.
    ///
    /// The handle is available only after writer readiness, contains no Tokio
    /// type, and never keeps the writer task alive. It may outlive graceful
    /// shutdown and continue to capture the sealed final immutable snapshot.
    #[must_use]
    pub fn read_handle(&self) -> LatestReadHandle {
        LatestReadHandle::new(self.ingress.shared())
    }

    /// Returns current bounded path-free store and reservation inspection.
    #[must_use]
    pub fn inspection(&self) -> RuntimeInspection {
        self.inspection
            .snapshot(&self.ingress.shared(), &self.initial_inspection)
    }

    /// Returns bounded decoded reopen evidence without authorizing it.
    #[must_use]
    pub fn recovered_records(&self) -> &[RecoveredAdmissionV1] {
        &self.recovered
    }

    /// Applies one bounded canonical registry lifecycle operation through the
    /// sole durable writer and returns only after manifest commit.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Capacity`] without waiting when the fixed
    /// control-admission bound is full, [`RegistryError::Closed`] after writer
    /// closure, or the exact canonical/persistence refusal from the store
    /// authority.
    pub async fn apply_registry(
        &self,
        operation: RegistryOperation,
    ) -> Result<RegistryCommit, RegistryError> {
        let _admission = Arc::clone(&self.control_admission)
            .try_acquire_owned()
            .map_err(|_| RegistryError::Capacity)?;
        let _control = self.control_gate.lock().await;
        let sender = self.store_sender.as_ref().ok_or(RegistryError::Closed)?;
        let (response_tx, response_rx) = oneshot::channel();
        if try_send(
            sender,
            WorkerMessage::Registry {
                operation: Box::new(operation),
                response: response_tx,
            },
        )
        .is_err()
        {
            return Err(RegistryError::Closed);
        }
        response_rx
            .await
            .map_err(|_| RegistryError::Closed)?
            .map_err(RegistryError::Store)
    }

    /// Binds one envelope through the current active canonical registry.
    ///
    /// This operation is serialized with append publication and lifecycle
    /// commits, but performs no durable mutation.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Capacity`] without waiting when the fixed
    /// control-admission bound is full, [`RegistryError::Closed`] after writer
    /// closure, or the exact current-active core binding refusal.
    pub async fn bind_envelope(
        &self,
        envelope: och_core::CollectionEnvelope,
    ) -> Result<och_core::DeclaredCollectionEnvelope, RegistryError> {
        let _admission = Arc::clone(&self.control_admission)
            .try_acquire_owned()
            .map_err(|_| RegistryError::Capacity)?;
        let _control = self.control_gate.lock().await;
        let sender = self.store_sender.as_ref().ok_or(RegistryError::Closed)?;
        let (response_tx, response_rx) = oneshot::channel();
        if try_send(
            sender,
            WorkerMessage::Bind {
                envelope,
                response: response_tx,
            },
        )
        .is_err()
        {
            return Err(RegistryError::Closed);
        }
        response_rx
            .await
            .map_err(|_| RegistryError::Closed)?
            .map_err(RegistryError::Store)
    }

    /// Gracefully stops and joins this instance's private writer task.
    ///
    /// Admission closes synchronously when this future is first polled. Commands
    /// accepted before that close are appended and published FIFO, a final
    /// journal/checkpoint barrier covers them, latest state is sealed, and both
    /// coordinator and blocking writer are joined.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ShutdownError`] when the writer had already exited,
    /// was cancelled, or panicked. If this future is cancelled, its owned handle
    /// is dropped and requests nonblocking writer abortion rather than detaching
    /// the retained task.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.ingress.close_admission();
        let Some(writer) = self.writer.as_mut() else {
            return Err(ShutdownError::WriterExitedBeforeShutdown);
        };
        let result = writer.await;
        self.writer = None;

        match result {
            Ok(WriterExit::Shutdown) => {
                self.store_sender = None;
                let Some(reaped) = self.reaped.take() else {
                    return Err(ShutdownError::WriterExitedBeforeShutdown);
                };
                if reaped.await.is_err() {
                    return Err(ShutdownError::WriterExitedBeforeShutdown);
                }
                self.shutdown_complete = true;
                Ok(())
            }
            Ok(_) => Err(ShutdownError::WriterExitedBeforeShutdown),
            Err(error) => Err(classify_shutdown_join_error(&error)),
        }
    }

    #[cfg(test)]
    async fn start(store_id: StoreId) -> Result<Self, StartError> {
        Self::start_with_options(store_id, WriterOptions::production()).await
    }

    #[cfg(test)]
    async fn start_with_options(
        store_id: StoreId,
        options: WriterOptions,
    ) -> Result<Self, StartError> {
        let seed_default_registry = matches!(
            options.behavior,
            TestBehavior::Normal
                | TestBehavior::FaultBeforePublicationSwap
                | TestBehavior::FaultAfterPublicationSwap
        );
        let directory = test_directory();
        let journal_limits = och_store::ActiveJournalLimits::new(
            och_store::MAX_ADMISSION_PAYLOAD_V1,
            64 * 1_024 * 1_024,
            4_096,
        )
        .expect("test journal limits");
        let byte_limits =
            ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("test byte limits");
        let group = GroupCommitPolicy::new(
            std::time::Duration::from_millis(1),
            MAX_OUTSTANDING_COMMANDS,
            64 * 1_024 * 1_024,
            std::time::Duration::from_secs(3_600),
        )
        .expect("test group policy");
        let store = StoreOptions::new(
            directory,
            store_id,
            och_store::ActiveJournalOpenMode::CreateNew,
            journal_limits,
            byte_limits,
            group,
            och_store::RegistryPersistenceOptions::new(och_core::SeriesRegistryLimits::new(
                256, 512,
            ))
            .expect("test registry persistence options"),
            och_store::RetryPersistenceOptions::new(8, 8).expect("test retry persistence options"),
        )
        .expect("test store options")
        .with_test_cleanup();
        let runtime = Self::open_inner(store, options).await?;
        if seed_default_registry {
            runtime
                .apply_registry(default_test_registry_operation(store_id))
                .await
                .expect("default test registry declaration should commit");
        }
        Ok(runtime)
    }
}

#[cfg(test)]
fn default_test_registry_operation(_store_id: StoreId) -> RegistryOperation {
    let mut series_bytes = [0_u8; 16];
    series_bytes[6] = 0x70;
    series_bytes[8] = 0x80;
    series_bytes[15] = 1;
    let mut producer_bytes = series_bytes;
    producer_bytes[15] = 2;
    let reference = |value: &str| {
        och_core::DeclarationReference::new(value.to_owned())
            .expect("default test declaration reference")
    };
    RegistryOperation::Register {
        series_id: och_core::SeriesId::from_bytes(series_bytes)
            .expect("default test series identity"),
        binding: och_core::SeriesBinding::new(och_core::SourceReference::with_projection(
            reference("provider:test"),
            och_core::SourceProjection::new("projection:test".to_owned())
                .expect("default test projection"),
            reference("locator:test"),
        )),
        payload: och_core::SeriesDeclarationPayload::new(
            och_core::ProducerId::from_bytes(producer_bytes)
                .expect("default test producer identity"),
            och_core::CollectionMode::Sampled,
            och_core::ValueFamily::Unsigned,
            och_core::QuantityEvidence::Absent,
            och_core::UnitEvidence::Absent,
            None,
        ),
        evidence: och_core::DeclarationEvidence::new(
            och_core::Timestamp::new(0, 0).expect("default test timestamp"),
            None,
        ),
    }
}

impl Drop for HistorianRuntime {
    fn drop(&mut self) {
        if self.shutdown_complete {
            return;
        }
        // Resolve receipts before aborting so cancellation cannot strand work if
        // the caller's executor never polls the writer again.
        self.ingress.stop();
        if let Some(store_sender) = &self.store_sender {
            signal_store_worker_stop(&self.stop, store_sender);
        } else {
            self.stop.store(true, Ordering::Release);
        }
        self.store_sender = None;
        if let Some(writer) = self.writer.take() {
            writer.abort();
        }
    }
}

/// Sanitized failures that can prevent writer startup.
///
/// Writer panic classification is observable when Tokio can report an unwind,
/// including debug and test builds. The workspace release profile uses
/// `panic = "abort"`, where a process abort is not recoverable as this error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartError {
    /// No Tokio runtime was active while the startup future was polled.
    NoActiveRuntime,
    /// The writer terminated normally before reporting initialized readiness.
    WriterExitedBeforeReadiness,
    /// Tokio reported that the writer task was cancelled before readiness.
    WriterTaskCancelled,
    /// Tokio reported that the writer task panicked before readiness.
    WriterTaskPanicked,
    /// The dedicated reaper thread could not be created.
    WorkerThreadUnavailable,
    /// The blocking worker exited before store readiness.
    WorkerExitedBeforeReadiness,
    /// Active-journal create/open/scan/lock refused.
    Store(ManifestStoreError),
}

impl fmt::Display for StartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NoActiveRuntime => "no active Tokio runtime",
            Self::WriterExitedBeforeReadiness => "writer exited before readiness",
            Self::WriterTaskCancelled => "writer task was cancelled before readiness",
            Self::WriterTaskPanicked => "writer task panicked before readiness",
            Self::WorkerThreadUnavailable => "journal worker thread unavailable",
            Self::WorkerExitedBeforeReadiness => "journal worker exited before readiness",
            Self::Store(_) => "active journal open failed",
        })
    }
}

impl Error for StartError {}

struct OpenGuard {
    stop: Arc<AtomicBool>,
    sender: Option<SyncSender<WorkerMessage>>,
}

impl OpenGuard {
    fn new(stop: Arc<AtomicBool>, sender: SyncSender<WorkerMessage>) -> Self {
        Self {
            stop,
            sender: Some(sender),
        }
    }

    fn disarm(&mut self) {
        self.sender = None;
    }
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            signal_store_worker_stop(&self.stop, &sender);
        }
    }
}

/// Sanitized failures that can prevent graceful writer shutdown.
///
/// Writer panic classification is observable when Tokio can report an unwind,
/// including debug and test builds. The workspace release profile uses
/// `panic = "abort"`, where a process abort is not recoverable as this error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownError {
    /// The writer exited before accepting and completing graceful shutdown.
    WriterExitedBeforeShutdown,
    /// Tokio reported that the writer task was cancelled.
    WriterTaskCancelled,
    /// Tokio reported that the writer task panicked.
    WriterTaskPanicked,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WriterExitedBeforeShutdown => "writer exited before graceful shutdown",
            Self::WriterTaskCancelled => "writer task was cancelled during shutdown",
            Self::WriterTaskPanicked => "writer task panicked before shutdown",
        })
    }
}

impl Error for ShutdownError {}

#[derive(Debug)]
struct StartupGuard {
    writer: Option<JoinHandle<WriterExit>>,
}

impl StartupGuard {
    fn new(writer: JoinHandle<WriterExit>) -> Self {
        Self {
            writer: Some(writer),
        }
    }

    #[cfg(test)]
    fn abort(&self) {
        if let Some(writer) = &self.writer {
            writer.abort();
        }
    }

    async fn join(&mut self) -> Result<WriterExit, JoinError> {
        self.writer
            .as_mut()
            .expect("startup guard must retain its writer")
            .await
    }

    fn transfer(mut self) -> JoinHandle<WriterExit> {
        self.writer
            .take()
            .expect("startup guard must retain its writer")
    }

    fn disarm(&mut self) {
        self.writer = None;
    }
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(writer) = &self.writer {
            writer.abort();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriterExit {
    Shutdown,
    StartupReceiverClosed,
    IngressFailed,
    PublicationFault,
    StoreFault,
    #[cfg(test)]
    BeforeReadiness,
    #[cfg(test)]
    BeforeShutdown,
}

struct WriterState {
    #[cfg(test)]
    probe: Option<Arc<LifecycleProbe>>,
}

impl WriterState {
    fn initialize(options: &WriterOptions) -> Self {
        #[cfg(not(test))]
        let _ = options;
        #[cfg(test)]
        if let Some(probe) = &options.probe {
            probe.state_initialized.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            #[cfg(test)]
            probe: options.probe.clone(),
        }
    }
}

#[cfg(test)]
impl Drop for WriterState {
    fn drop(&mut self) {
        if let Some(probe) = &self.probe {
            probe.state_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct WriterOptions {
    #[cfg(test)]
    initialization_gate: Option<oneshot::Receiver<()>>,
    #[cfg(test)]
    shutdown_gate: Option<oneshot::Receiver<()>>,
    #[cfg(test)]
    command_gate: Option<oneshot::Receiver<()>>,
    #[cfg(test)]
    publication_gate: Option<oneshot::Receiver<()>>,
    #[cfg(test)]
    publication_gate_after: usize,
    #[cfg(test)]
    behavior: TestBehavior,
    #[cfg(test)]
    probe: Option<Arc<LifecycleProbe>>,
}

impl WriterOptions {
    const fn production() -> Self {
        Self {
            #[cfg(test)]
            initialization_gate: None,
            #[cfg(test)]
            shutdown_gate: None,
            #[cfg(test)]
            command_gate: None,
            #[cfg(test)]
            publication_gate: None,
            #[cfg(test)]
            publication_gate_after: 0,
            #[cfg(test)]
            behavior: TestBehavior::Normal,
            #[cfg(test)]
            probe: None,
        }
    }
}

#[cfg(test)]
fn test_directory() -> std::path::PathBuf {
    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(1);
    let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let directory =
        std::env::temp_dir().join(format!("och-runtime-{}-{sequence}", std::process::id()));
    std::fs::create_dir(&directory).expect("unique runtime test directory");
    directory
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    options: WriterOptions,
    readiness: oneshot::Sender<Result<store_worker::WorkerReady, StartError>>,
    store_readiness: oneshot::Receiver<Result<store_worker::WorkerReady, ManifestStoreError>>,
    ingress: Arc<IngressShared>,
    store_sender: SyncSender<WorkerMessage>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
    control_gate: Arc<AsyncMutex<()>>,
) -> WriterExit {
    #[cfg(test)]
    let mut options = options;
    #[cfg(test)]
    let _task_guard = TaskGuard::new(options.probe.clone());
    let mut failure_guard =
        WriterFailureGuard::new(Arc::clone(&ingress), inspection, stop, store_sender.clone());

    let store_ready = match store_readiness.await {
        Ok(Ok(ready)) => ready,
        Ok(Err(error)) => {
            let _ = readiness.send(Err(StartError::Store(error)));
            return WriterExit::StoreFault;
        }
        Err(_) => {
            let _ = readiness.send(Err(StartError::WorkerExitedBeforeReadiness));
            return WriterExit::StoreFault;
        }
    };
    if store_ready.retry.reference() != store_ready.inspection.committed().retry_state()
        || !ingress.install_opened_retry(store_ready.retry.clone())
    {
        let _ = readiness.send(Err(StartError::WorkerExitedBeforeReadiness));
        return WriterExit::StoreFault;
    }

    #[cfg(test)]
    let initialization_failed = match options.initialization_gate.take() {
        Some(initialization_gate) => initialization_gate.await.is_err(),
        None => false,
    };
    #[cfg(test)]
    if initialization_failed {
        return WriterExit::BeforeReadiness;
    }
    #[cfg(test)]
    match options.behavior {
        TestBehavior::ExitBeforeReadiness => return WriterExit::BeforeReadiness,
        TestBehavior::PanicBeforeReadiness => panic!("hostile writer panic payload"),
        TestBehavior::Normal
        | TestBehavior::CancelBeforeReadiness
        | TestBehavior::ExitBeforeShutdown
        | TestBehavior::PanicBeforeShutdown
        | TestBehavior::ExitWhileHandling
        | TestBehavior::PanicWhileHandling
        | TestBehavior::FaultBeforePublicationSwap
        | TestBehavior::FaultAfterPublicationSwap => {}
    }

    let _state = WriterState::initialize(&options);
    if readiness.send(Ok(store_ready)).is_err() {
        return WriterExit::StartupReceiverClosed;
    }

    #[cfg(test)]
    match options.behavior {
        TestBehavior::ExitBeforeShutdown => return WriterExit::BeforeShutdown,
        TestBehavior::PanicBeforeShutdown => panic!("hostile writer panic payload"),
        TestBehavior::Normal
        | TestBehavior::CancelBeforeReadiness
        | TestBehavior::ExitBeforeReadiness
        | TestBehavior::PanicBeforeReadiness
        | TestBehavior::ExitWhileHandling
        | TestBehavior::PanicWhileHandling
        | TestBehavior::FaultBeforePublicationSwap
        | TestBehavior::FaultAfterPublicationSwap => {}
    }

    writer_loop(
        options,
        ingress,
        store_sender,
        control_gate,
        &mut failure_guard,
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn writer_loop(
    options: WriterOptions,
    ingress: Arc<IngressShared>,
    store_sender: SyncSender<WorkerMessage>,
    control_gate: Arc<AsyncMutex<()>>,
    failure_guard: &mut WriterFailureGuard,
) -> WriterExit {
    #[cfg(test)]
    let mut options = options;
    #[cfg(test)]
    let mut appended_count = 0_usize;
    #[cfg(not(test))]
    let _ = options;

    loop {
        // Register interest before inspecting the one-consumer queue. Notify's
        // retained permit closes the submit-between-check-and-await race.
        let notified = ingress.notified();
        match ingress.take_next() {
            NextWork::Work(mut work) => {
                let _control = control_gate.lock().await;
                #[cfg(test)]
                if let Some(probe) = &options.probe {
                    probe.commands_started.fetch_add(1, Ordering::SeqCst);
                }
                #[cfg(test)]
                if let Some(command_gate) = options.command_gate.take()
                    && command_gate.await.is_err()
                {
                    return WriterExit::BeforeShutdown;
                }
                #[cfg(test)]
                match options.behavior {
                    TestBehavior::ExitWhileHandling => return WriterExit::BeforeShutdown,
                    TestBehavior::PanicWhileHandling => panic!("hostile writer panic payload"),
                    TestBehavior::Normal
                    | TestBehavior::CancelBeforeReadiness
                    | TestBehavior::ExitBeforeReadiness
                    | TestBehavior::ExitBeforeShutdown
                    | TestBehavior::PanicBeforeReadiness
                    | TestBehavior::PanicBeforeShutdown
                    | TestBehavior::FaultBeforePublicationSwap
                    | TestBehavior::FaultAfterPublicationSwap => {}
                }
                #[cfg(test)]
                if let Some(probe) = &options.probe {
                    probe
                        .handled_order
                        .lock()
                        .expect("test handled-order probe should not be poisoned")
                        .push(work.test_tag());
                }
                let frame_bytes = work.frame_len();
                let Some(prepared) = work.take_prepared() else {
                    return WriterExit::StoreFault;
                };
                let slot = work.slot_index();
                let priority = work.priority();
                let barrier = work.barrier();
                let (append_tx, append_rx) = oneshot::channel();
                if try_send(
                    &store_sender,
                    WorkerMessage::Append {
                        slot,
                        prepared: Box::new(prepared),
                        response: append_tx,
                    },
                )
                .is_err()
                {
                    return WriterExit::StoreFault;
                }
                let Ok(Ok(AppendResult { admission, append })) = append_rx.await else {
                    return WriterExit::StoreFault;
                };
                let qualification = admission.retry().clone();
                let Ok(preparation) = work.prepare_publication(&admission) else {
                    return WriterExit::PublicationFault;
                };
                #[cfg(test)]
                {
                    let gate_this_append = appended_count == options.publication_gate_after;
                    appended_count = appended_count.saturating_add(1);
                    if gate_this_append
                        && let Some(publication_gate) = options.publication_gate.take()
                        && publication_gate.await.is_err()
                    {
                        return WriterExit::BeforeShutdown;
                    }
                }
                #[cfg(test)]
                let fault_injection = match options.behavior {
                    TestBehavior::FaultBeforePublicationSwap => {
                        CompletionFaultInjection::BeforeSwap
                    }
                    TestBehavior::FaultAfterPublicationSwap => CompletionFaultInjection::AfterSwap,
                    _ => CompletionFaultInjection::None,
                };
                #[cfg(not(test))]
                let fault_injection = CompletionFaultInjection::None;
                if !(*work).finish_handled(admission, append, preparation, fault_injection) {
                    return WriterExit::PublicationFault;
                }
                if try_send(
                    &store_sender,
                    WorkerMessage::Published {
                        slot,
                        priority,
                        barrier,
                        frame_bytes,
                        qualification: Box::new(qualification),
                        append,
                    },
                )
                .is_err()
                {
                    return WriterExit::StoreFault;
                }
            }
            NextWork::BarrierRequired => {
                let _control = control_gate.lock().await;
                let (barrier_tx, barrier_rx) = oneshot::channel();
                if try_send(
                    &store_sender,
                    WorkerMessage::Barrier {
                        response: barrier_tx,
                    },
                )
                .is_err()
                    || !matches!(barrier_rx.await, Ok(Ok(())))
                {
                    return WriterExit::StoreFault;
                }
            }
            NextWork::Empty => notified.await,
            NextWork::Drained => {
                let _control = control_gate.lock().await;
                #[cfg(test)]
                if let Some(probe) = &options.probe {
                    probe.shutdown_received.fetch_add(1, Ordering::SeqCst);
                }
                #[cfg(test)]
                let shutdown_gate_failed = match options.shutdown_gate.take() {
                    Some(shutdown_gate) => shutdown_gate.await.is_err(),
                    None => false,
                };
                #[cfg(test)]
                if shutdown_gate_failed {
                    return WriterExit::BeforeShutdown;
                }
                #[cfg(test)]
                if let Some(probe) = &options.probe {
                    probe.normal_exits.fetch_add(1, Ordering::SeqCst);
                }
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                if try_send(
                    &store_sender,
                    WorkerMessage::Shutdown {
                        response: shutdown_tx,
                    },
                )
                .is_err()
                    || !matches!(shutdown_rx.await, Ok(Ok(())))
                {
                    return WriterExit::StoreFault;
                }
                failure_guard.disarm();
                return WriterExit::Shutdown;
            }
            NextWork::Failed => return WriterExit::IngressFailed,
        }
    }
}

struct WriterFailureGuard {
    ingress: Arc<IngressShared>,
    inspection: InspectionShared,
    stop: Arc<AtomicBool>,
    store_sender: SyncSender<WorkerMessage>,
    armed: bool,
}

impl WriterFailureGuard {
    fn new(
        ingress: Arc<IngressShared>,
        inspection: InspectionShared,
        stop: Arc<AtomicBool>,
        store_sender: SyncSender<WorkerMessage>,
    ) -> Self {
        Self {
            ingress,
            inspection,
            stop,
            store_sender,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WriterFailureGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inspection.coordinator_fault();
            self.ingress.stop();
            signal_store_worker_stop(&self.stop, &self.store_sender);
        }
    }
}

fn signal_store_worker_stop(stop: &AtomicBool, store_sender: &SyncSender<WorkerMessage>) {
    stop.store(true, Ordering::Release);
    let _ = try_send(store_sender, WorkerMessage::Abort);
}

fn classify_start_join_error(error: &JoinError) -> StartError {
    if error.is_panic() {
        StartError::WriterTaskPanicked
    } else {
        StartError::WriterTaskCancelled
    }
}

fn classify_shutdown_join_error(error: &JoinError) -> ShutdownError {
    if error.is_panic() {
        ShutdownError::WriterTaskPanicked
    } else {
        ShutdownError::WriterTaskCancelled
    }
}

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

#[cfg(test)]
#[derive(Debug, Default)]
struct LifecycleProbe {
    task_started: AtomicUsize,
    task_dropped: AtomicUsize,
    state_initialized: AtomicUsize,
    state_dropped: AtomicUsize,
    shutdown_received: AtomicUsize,
    normal_exits: AtomicUsize,
    commands_started: AtomicUsize,
    handled_order: std::sync::Mutex<Vec<u8>>,
}

#[cfg(test)]
struct TaskGuard(Option<Arc<LifecycleProbe>>);

#[cfg(test)]
impl TaskGuard {
    fn new(probe: Option<Arc<LifecycleProbe>>) -> Self {
        if let Some(probe) = &probe {
            probe.task_started.fetch_add(1, Ordering::SeqCst);
        }
        Self(probe)
    }
}

#[cfg(test)]
impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(probe) = &self.0 {
            probe.task_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestBehavior {
    Normal,
    CancelBeforeReadiness,
    ExitBeforeReadiness,
    ExitBeforeShutdown,
    PanicBeforeReadiness,
    PanicBeforeShutdown,
    ExitWhileHandling,
    PanicWhileHandling,
    FaultBeforePublicationSwap,
    FaultAfterPublicationSwap,
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionPriority, BarrierDemand, ByteReservationLimits, DurableOutcome, GroupCommitPolicy,
        HandledOutcome, HistorianIngress, HistorianRuntime, IngressCommand, LatestReadError,
        LatestReadHandle, LifecycleProbe, MAX_OUTSTANDING_COMMANDS, MAX_PUBLISHED_SERIES,
        ReceiptOutcome, RegistryError, RegistryOperation, RegistryOutcome, RuntimeHealth,
        ShutdownError, StartError, StoreOptions, StoreOptionsError, SubmissionDisposition,
        TestBehavior, TrySubmitErrorKind, WriterOptions, default_test_registry_operation,
        test_directory,
    };
    use och_core::{
        ArtifactId, ArtifactReference, CanonicalAdmission, CaptureLifecycle, CaptureRunEvidence,
        CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
        DeclarationEvidence, DeclarationReference, DeclarationRevision, EvidenceId, EvidenceKind,
        ExactValue, Gap, GapReason, NativeStatus, NoChange, NormalizedRecordEvidence, Observation,
        ObservationId, ObservationTimes, ProducerEpoch, ProducerId, ProducerPosition,
        ProducerSequence, Quality, QualityFlags, QualityLevel, QuantityEvidence, RawRecordEvidence,
        RetryKey, RetryQualification, SeriesBinding, SeriesDeclarationPayload, SeriesId,
        SeriesMetadata, SeriesRegistry, SeriesRegistryLimits, SourceBatchMetadata,
        SourceEndpointEvidence, SourceGapEvidence, SourceGapReason, SourceInterpretation,
        SourceIntervalKind, SourceObservationContext, SourceObservationEvidence, SourceProjection,
        SourceReference, SourceSchemaIdentity, SourceSchemaVersion, SourceSnapshotEvidence,
        SourceSystemEvidence, SourceTransport, StoreId, TimeInterval, Timestamp, UnitEvidence,
        ValueFamily,
    };
    use std::fs;
    use std::future::{Future, poll_fn};
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, Waker};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    const TEST_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    type RegistryControlFuture<'a> =
        Pin<Box<dyn Future<Output = Result<u128, RegistryError>> + 'a>>;

    fn harness() -> Runtime {
        Builder::new_current_thread()
            .build()
            .expect("current-thread Tokio test harness should build")
    }

    fn directory_bytes(directory: &Path) -> Vec<(String, Vec<u8>)> {
        let mut artifacts = fs::read_dir(directory)
            .expect("read runtime store inventory")
            .map(|entry| {
                let entry = entry.expect("read runtime store artifact");
                (
                    entry
                        .file_name()
                        .into_string()
                        .expect("runtime store artifact name should be Unicode"),
                    fs::read(entry.path()).expect("read runtime store artifact bytes"),
                )
            })
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| left.0.cmp(&right.0));
        artifacts
    }

    fn durable_options(
        directory: PathBuf,
        store: StoreId,
        mode: och_store::ActiveJournalOpenMode,
        byte_limits: ByteReservationLimits,
        group: GroupCommitPolicy,
    ) -> StoreOptions {
        durable_options_with_retry(
            directory,
            store,
            mode,
            byte_limits,
            group,
            och_store::RetryPersistenceOptions::new(2, 2).expect("test retry persistence options"),
        )
    }

    fn durable_options_with_retry(
        directory: PathBuf,
        store: StoreId,
        mode: och_store::ActiveJournalOpenMode,
        byte_limits: ByteReservationLimits,
        group: GroupCommitPolicy,
        retry: och_store::RetryPersistenceOptions,
    ) -> StoreOptions {
        StoreOptions::new(
            directory,
            store,
            mode,
            och_store::ActiveJournalLimits::new(
                och_store::MAX_ADMISSION_PAYLOAD_V1,
                64 * 1_024 * 1_024,
                4_096,
            )
            .expect("test journal limits"),
            byte_limits,
            group,
            och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(16, 64))
                .expect("test registry persistence options"),
            retry,
        )
        .expect("test durable options")
    }

    async fn open_durable_test(options: StoreOptions) -> Result<HistorianRuntime, StartError> {
        let runtime = HistorianRuntime::open(options).await?;
        runtime
            .apply_registry(default_test_registry_operation(runtime.store_id()))
            .await
            .expect("default durable-test registry declaration should commit");
        Ok(runtime)
    }

    async fn register_metadata(runtime: &HistorianRuntime, metadata: &SeriesMetadata) {
        runtime
            .apply_registry(RegistryOperation::Register {
                series_id: metadata.series_id(),
                binding: SeriesBinding::new(source_reference()),
                payload: SeriesDeclarationPayload::new(
                    metadata.producer_id(),
                    metadata.collection_mode(),
                    ValueFamily::Unsigned,
                    QuantityEvidence::Absent,
                    UnitEvidence::Absent,
                    None,
                ),
                evidence: DeclarationEvidence::new(timestamp(0), None),
            })
            .await
            .expect("test registry declaration should commit");
    }

    fn group_policy(delay: std::time::Duration, records: usize, bytes: usize) -> GroupCommitPolicy {
        GroupCommitPolicy::new(delay, records, bytes, std::time::Duration::from_secs(3_600))
            .expect("test group policy")
    }

    fn options(probe: &Arc<LifecycleProbe>, behavior: TestBehavior) -> WriterOptions {
        WriterOptions {
            initialization_gate: None,
            shutdown_gate: None,
            command_gate: None,
            publication_gate: None,
            publication_gate_after: 0,
            behavior,
            probe: Some(Arc::clone(probe)),
        }
    }

    fn uuid_bytes(tag: u8) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = tag;
        bytes
    }

    fn series_id(tag: u8) -> SeriesId {
        SeriesId::from_bytes(uuid_bytes(tag)).expect("test series identity should be UUIDv7")
    }

    fn store_id(tag: u8) -> StoreId {
        StoreId::from_bytes(uuid_bytes(tag)).expect("test store identity should be UUIDv7")
    }

    fn producer_id(tag: u8) -> ProducerId {
        ProducerId::from_bytes(uuid_bytes(tag)).expect("test producer identity should be UUIDv7")
    }

    fn observation_id(tag: u8) -> ObservationId {
        ObservationId::from_bytes(uuid_bytes(tag))
            .expect("test observation identity should be UUIDv7")
    }

    fn evidence_id(tag: u8) -> EvidenceId {
        EvidenceId::from_bytes(uuid_bytes(tag)).expect("test evidence identity should be UUIDv7")
    }

    fn declaration_reference(value: &str) -> DeclarationReference {
        DeclarationReference::new(value.to_owned()).expect("test reference should be valid")
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("test timestamp should be normalized")
    }

    fn position(epoch: u128, sequence: u128) -> ProducerPosition {
        ProducerPosition::new(ProducerEpoch::new(epoch), ProducerSequence::new(sequence))
    }

    fn observation(
        mode: CollectionMode,
        tag: u8,
        value: u64,
        producer_position: Option<ProducerPosition>,
        receive_seconds: i64,
        effective_seconds: i64,
    ) -> Observation {
        let interval = (mode == CollectionMode::Interval).then(|| {
            TimeInterval::new(
                timestamp(effective_seconds),
                timestamp(effective_seconds + 1),
            )
            .expect("test interval should be nonempty")
        });
        Observation::new(
            observation_id(tag),
            ExactValue::Unsigned(value),
            ObservationTimes::new(
                Some(timestamp(effective_seconds + 10)),
                timestamp(receive_seconds),
                timestamp(effective_seconds),
            ),
            Quality::new(QualityLevel::Unknown, QualityFlags::none()),
            NativeStatus::absent(),
            producer_position,
            interval,
        )
    }

    fn content(digest_tag: u8) -> ContentIdentity {
        ContentIdentity::new(
            ContentFormat::new("application/x-och-test".to_owned())
                .expect("test content format should be valid"),
            ContentVersion::new(1),
            [digest_tag; 32],
        )
    }

    fn artifact(tag: u8, digest_tag: u8) -> ArtifactReference {
        ArtifactReference::new(
            ArtifactId::from_bytes(uuid_bytes(tag)).expect("test artifact should be UUIDv7"),
            content(digest_tag),
        )
    }

    fn retry(series: &SeriesMetadata, key: &str, digest_tag: u8) -> RetryQualification {
        RetryQualification::new(
            series.series_id(),
            series.producer_id(),
            RetryKey::new(key.to_owned()).expect("test retry key should be valid"),
            content(digest_tag),
        )
    }

    fn source_reference() -> SourceReference {
        SourceReference::with_projection(
            declaration_reference("provider:test"),
            SourceProjection::new("projection:test".to_owned())
                .expect("test projection should be valid"),
            declaration_reference("locator:test"),
        )
    }

    fn capture_lifecycle() -> CaptureLifecycle {
        let system = SourceSystemEvidence::new(
            evidence_id(200),
            declaration_reference("provider:test"),
            SourceProjection::new("projection:test".to_owned())
                .expect("test projection should be valid"),
        );
        let endpoint = SourceEndpointEvidence::new(
            evidence_id(201),
            system.evidence_id(),
            declaration_reference("locator:test"),
        );
        let run = CaptureRunEvidence::new(
            evidence_id(202),
            endpoint.evidence_id(),
            timestamp(1),
            Some(timestamp(2)),
        )
        .expect("test capture times should be ordered");
        let snapshot =
            SourceSnapshotEvidence::new(evidence_id(203), run.evidence_id(), artifact(204, 204));
        CaptureLifecycle::new(system, endpoint, run, snapshot)
            .expect("test capture lifecycle should be linked")
    }

    #[allow(clippy::too_many_lines)]
    fn canonical_admission(
        store: StoreId,
        envelope: CollectionEnvelope,
        retry: RetryQualification,
    ) -> CanonicalAdmission {
        let source = source_reference();
        let series = envelope.series().clone();
        let contexts = envelope
            .observations()
            .iter()
            .enumerate()
            .map(|(index, observation)| {
                let ordinal = u8::try_from(index).expect("test observation ordinal should fit");
                let base = ordinal
                    .checked_mul(3)
                    .and_then(|value| value.checked_add(10))
                    .expect("test evidence tag should fit");
                let source_observation = SourceObservationEvidence::new(
                    evidence_id(base),
                    Some(artifact(base.saturating_add(100), base)),
                    SourceTransport::New,
                    None,
                );
                let raw = RawRecordEvidence::new(
                    evidence_id(base + 1),
                    evidence_id(203),
                    artifact(base.saturating_add(101), base + 1),
                    None,
                );
                let normalized = NormalizedRecordEvidence::new(
                    evidence_id(base + 2),
                    raw.evidence_id(),
                    content(base + 2),
                    source_observation.evidence_id(),
                );
                SourceObservationContext::new(
                    ordinal,
                    observation.observation_id(),
                    SourceInterpretation::new(
                        source.clone(),
                        None,
                        QuantityEvidence::Absent,
                        UnitEvidence::Absent,
                    ),
                    source_observation,
                    raw,
                    normalized,
                )
            })
            .collect::<Vec<_>>();
        let source_gaps = envelope
            .gaps()
            .iter()
            .map(|gap| {
                SourceGapEvidence::new(
                    gap.epoch(),
                    gap.start(),
                    gap.end(),
                    SourceGapReason::Unknown,
                )
                .expect("test source gap should be valid")
            })
            .collect::<Vec<_>>();
        let interval = match envelope.evidence_kind() {
            EvidenceKind::Observed => SourceIntervalKind::Observed,
            EvidenceKind::NoChange => SourceIntervalKind::NoChange,
        };
        let batch = SourceBatchMetadata::new(
            SourceSchemaIdentity::new("runtime.test".to_owned())
                .expect("test schema should be valid"),
            SourceSchemaVersion::new(1).expect("test schema version should be valid"),
            interval,
        );
        let lifecycle = capture_lifecycle();
        let mut registry = SeriesRegistry::new(store, SeriesRegistryLimits::new(1, 1));
        registry
            .register(
                series.series_id(),
                SeriesBinding::new(source),
                SeriesDeclarationPayload::new(
                    series.producer_id(),
                    series.collection_mode(),
                    ValueFamily::Unsigned,
                    QuantityEvidence::Absent,
                    UnitEvidence::Absent,
                    None,
                ),
                DeclarationEvidence::new(timestamp(0), None),
            )
            .expect("test declaration should register");
        let declared = registry
            .bind(envelope)
            .expect("test envelope should bind to its declaration");

        match interval {
            SourceIntervalKind::Observed => CanonicalAdmission::observed(
                declared,
                retry,
                batch,
                lifecycle,
                contexts,
                source_gaps,
            )
            .expect("test observed admission should validate"),
            SourceIntervalKind::NoChange => {
                CanonicalAdmission::no_change(declared, retry, batch, lifecycle)
                    .expect("test no-change admission should validate")
            }
        }
    }

    fn envelope_command(envelope: CollectionEnvelope, key: &str, digest_tag: u8) -> IngressCommand {
        let qualification = retry(envelope.series(), key, digest_tag);
        IngressCommand::new(canonical_admission(store_id(1), envelope, qualification))
    }

    fn observed_command(
        series: SeriesMetadata,
        observations: Vec<Observation>,
        key: &str,
        digest_tag: u8,
    ) -> IngressCommand {
        let envelope = CollectionEnvelope::observed(series, observations, Vec::new())
            .expect("test observed envelope should be valid");
        envelope_command(envelope, key, digest_tag)
    }

    fn positioned_command(
        series_tag: u8,
        producer_tag: u8,
        mode: CollectionMode,
        observation_tag: u8,
        sequence: u128,
        value: u64,
        key: &str,
    ) -> IngressCommand {
        let series = SeriesMetadata::new(series_id(series_tag), producer_id(producer_tag), mode);
        let observation = observation(
            mode,
            observation_tag,
            value,
            Some(position(1, sequence)),
            i64::from(observation_tag),
            i64::from(observation_tag),
        );
        observed_command(series, vec![observation], key, observation_tag)
    }

    fn model_parts(
        series_id: SeriesId,
        producer_id: ProducerId,
        key: &str,
        digest_tag: u8,
        gap_start: u128,
    ) -> (CollectionEnvelope, RetryQualification) {
        let series = SeriesMetadata::new(series_id, producer_id, CollectionMode::Sampled);
        let gap = Gap::new(
            ProducerEpoch::new(0),
            ProducerSequence::new(gap_start),
            ProducerSequence::new(gap_start + 1),
            GapReason::Unknown,
        )
        .expect("test gap should be nonempty");
        let envelope = CollectionEnvelope::observed(series, Vec::new(), vec![gap])
            .expect("test envelope should be valid");
        let retry = RetryQualification::new(
            series_id,
            producer_id,
            RetryKey::new(key.to_owned()).expect("test retry key should be valid"),
            ContentIdentity::new(
                ContentFormat::new("application/x-och-test".to_owned())
                    .expect("test content format should be valid"),
                ContentVersion::new(1),
                [digest_tag; 32],
            ),
        );
        (envelope, retry)
    }

    fn command_for_store(
        store: StoreId,
        key: &str,
        digest_tag: u8,
        gap_start: u128,
    ) -> IngressCommand {
        let (envelope, retry) =
            model_parts(series_id(1), producer_id(2), key, digest_tag, gap_start);
        IngressCommand::new(canonical_admission(store, envelope, retry))
    }

    fn command(key: &str, digest_tag: u8, gap_start: u128) -> IngressCommand {
        command_for_store(store_id(1), key, digest_tag, gap_start)
    }

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
    }

    async fn complete_bounded<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let started = std::time::Instant::now();
        loop {
            if let Poll::Ready(output) = poll_once(future.as_mut()).await {
                return output;
            }
            assert!(
                started.elapsed() < TEST_WAIT_TIMEOUT,
                "lifecycle future did not complete within the elapsed-time bound"
            );
            std::thread::sleep(std::time::Duration::from_micros(50));
            yield_now().await;
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        let started = std::time::Instant::now();
        loop {
            if condition() {
                return;
            }
            assert!(
                started.elapsed() < TEST_WAIT_TIMEOUT,
                "condition did not hold within the elapsed-time bound"
            );
            std::thread::sleep(std::time::Duration::from_micros(50));
            yield_now().await;
        }
    }

    async fn assert_worker_reaped_and_lock_reopens(
        runtime: &mut HistorianRuntime,
        directory: &Path,
    ) {
        let reaped = runtime
            .reaped
            .take()
            .expect("failed runtime should retain its reaper completion");
        complete_bounded(reaped)
            .await
            .expect("coordinator failure must wake and reap the store worker");
        assert_eq!(runtime.inspection().health(), RuntimeHealth::Faulted);
        assert_eq!(
            runtime.read_handle().snapshot(),
            Err(LatestReadError::unavailable())
        );
        let config = och_store::ManifestStoreConfig::new(
            directory.to_path_buf(),
            runtime.store_id(),
            och_store::ActiveJournalOpenMode::OpenExisting,
            och_store::ActiveJournalLimits::new(
                och_store::MAX_ADMISSION_PAYLOAD_V1,
                64 * 1_024 * 1_024,
                4_096,
            )
            .expect("reopen journal limits"),
            och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(256, 512))
                .expect("reopen registry options"),
            och_store::RetryPersistenceOptions::new(2, 2).expect("reopen retry options"),
        )
        .expect("reopen configuration");
        let reopened = och_store::ManifestStore::open(config)
            .expect("reaped coordinator failure must release the store lock");
        drop(reopened);
    }

    #[test]
    fn completion_helper_is_not_limited_by_the_legacy_poll_count() {
        harness().block_on(async {
            let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let observed = Arc::clone(&polls);
            complete_bounded(poll_fn(move |context| {
                let poll = observed.fetch_add(1, Ordering::SeqCst);
                if poll > 4_096 {
                    Poll::Ready(())
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }))
            .await;
            assert!(polls.load(Ordering::SeqCst) > 4_096);
        });
    }

    #[test]
    fn no_active_runtime_is_a_sanitized_start_error() {
        let mut start = Box::pin(HistorianRuntime::start(store_id(1)));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let Poll::Ready(Err(error)) = start.as_mut().poll(&mut context) else {
            panic!("startup without an executor must return an error immediately");
        };
        assert_eq!(error, StartError::NoActiveRuntime);
        assert_eq!(
            StartError::NoActiveRuntime.to_string(),
            "no active Tokio runtime"
        );
    }

    #[test]
    fn readiness_follows_private_state_initialization() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_gate, initialization_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.initialization_gate = Some(initialization_gate);
            let mut start = Box::pin(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ));

            assert!(poll_once(start.as_mut()).await.is_pending());
            wait_until(|| probe.task_started.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.state_initialized.load(Ordering::SeqCst), 0);
            assert!(poll_once(start.as_mut()).await.is_pending());

            open_gate
                .send(())
                .expect("initialization gate should be open");
            let runtime = complete_bounded(start)
                .await
                .expect("writer should report readiness");
            assert_eq!(probe.state_initialized.load(Ordering::SeqCst), 1);
            assert_eq!(
                runtime
                    .ingress()
                    .shared()
                    .current_retry()
                    .expect("installed startup retry projection")
                    .reference(),
                runtime.inspection().committed().retry_state()
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("ready writer should shut down");
        });
    }

    #[test]
    fn cancelled_startup_aborts_and_reclaims_the_writer() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (_keep_gate_open, initialization_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.initialization_gate = Some(initialization_gate);
            let mut start = Box::pin(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ));

            assert!(poll_once(start.as_mut()).await.is_pending());
            wait_until(|| probe.task_started.load(Ordering::SeqCst) == 1).await;
            drop(start);
            wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.state_initialized.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn graceful_shutdown_waits_for_termination_and_cleanup() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (finish_shutdown, shutdown_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.shutdown_gate = Some(shutdown_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let mut shutdown = Box::pin(runtime.shutdown());

            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            wait_until(|| probe.shutdown_received.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(probe.task_dropped.load(Ordering::SeqCst), 0);

            finish_shutdown
                .send(())
                .expect("shutdown gate should be open");
            complete_bounded(shutdown)
                .await
                .expect("shutdown should join normal termination");
            assert_eq!(probe.normal_exits.load(Ordering::SeqCst), 1);
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(probe.task_dropped.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn cancelled_shutdown_aborts_and_reclaims_the_writer() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (_keep_gate_open, shutdown_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.shutdown_gate = Some(shutdown_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let read = runtime.read_handle();
            let mut shutdown = Box::pin(runtime.shutdown());

            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            wait_until(|| probe.shutdown_received.load(Ordering::SeqCst) == 1).await;
            let sealed_before_cancel = read
                .snapshot()
                .expect("drained registry is sealed while join remains gated");
            drop(shutdown);
            wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.normal_exits.load(Ordering::SeqCst), 0);
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
            assert!(sealed_before_cancel.is_empty());
        });
    }

    #[test]
    fn plain_handle_drop_is_abort_only_and_non_graceful() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&probe, TestBehavior::Normal),
            ))
            .await
            .expect("writer should start");
            let read = runtime.read_handle();

            drop(runtime);
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
            wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.normal_exits.load(Ordering::SeqCst), 0);
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn premature_writer_exits_are_distinguished() {
        harness().block_on(async {
            let before_ready_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&before_ready_probe, TestBehavior::ExitBeforeReadiness),
            ))
            .await
            .expect_err("premature startup exit must fail");
            assert_eq!(start_error, StartError::WriterExitedBeforeReadiness);

            let before_shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&before_shutdown_probe, TestBehavior::ExitBeforeShutdown),
            ))
            .await
            .expect("readiness should precede the injected exit");
            let read = runtime.read_handle();
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("premature writer exit must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterExitedBeforeShutdown);
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
        });
    }

    #[test]
    fn task_cancellation_maps_to_closed_errors() {
        harness().block_on(async {
            let start_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&start_probe, TestBehavior::CancelBeforeReadiness),
            ))
            .await
            .expect_err("aborted startup writer should be cancelled");
            assert_eq!(start_error, StartError::WriterTaskCancelled);

            let shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&shutdown_probe, TestBehavior::Normal),
            ))
            .await
            .expect("writer should start");
            let read = runtime.read_handle();
            runtime
                .writer
                .as_ref()
                .expect("runtime should retain writer")
                .abort();
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("aborted writer must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterTaskCancelled);
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
        });
    }

    #[test]
    fn task_panics_map_without_exposing_the_payload() {
        harness().block_on(async {
            let start_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&start_probe, TestBehavior::PanicBeforeReadiness),
            ))
            .await
            .expect_err("writer panic must fail startup");
            assert_eq!(start_error, StartError::WriterTaskPanicked);
            assert!(!start_error.to_string().contains("hostile"));

            let shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&shutdown_probe, TestBehavior::PanicBeforeShutdown),
            ))
            .await
            .expect("readiness should precede the injected panic");
            let read = runtime.read_handle();
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("writer panic must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterTaskPanicked);
            assert!(!shutdown_error.to_string().contains("hostile"));
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
        });
    }

    #[test]
    fn two_instances_own_isolated_single_writers() {
        harness().block_on(async {
            let first_probe = Arc::new(LifecycleProbe::default());
            let second_probe = Arc::new(LifecycleProbe::default());
            let first = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&first_probe, TestBehavior::Normal),
            ))
            .await
            .expect("first writer should start");
            let second = complete_bounded(HistorianRuntime::start_with_options(
                store_id(2),
                options(&second_probe, TestBehavior::Normal),
            ))
            .await
            .expect("second writer should start");

            assert_eq!(first_probe.task_started.load(Ordering::SeqCst), 1);
            assert_eq!(second_probe.task_started.load(Ordering::SeqCst), 1);
            complete_bounded(first.shutdown())
                .await
                .expect("first writer should stop independently");
            assert_eq!(second_probe.state_dropped.load(Ordering::SeqCst), 0);
            complete_bounded(second.shutdown())
                .await
                .expect("second writer should stop independently");
        });
    }

    #[test]
    fn repeated_hostile_lifecycle_sequences_are_bounded() {
        harness().block_on(async {
            for _ in 0..16 {
                let probe = Arc::new(LifecycleProbe::default());
                let start_error = complete_bounded(HistorianRuntime::start_with_options(
                    store_id(1),
                    options(&probe, TestBehavior::ExitBeforeReadiness),
                ))
                .await
                .expect_err("injected early exit must remain bounded");
                assert_eq!(start_error, StartError::WriterExitedBeforeReadiness);

                let runtime = complete_bounded(HistorianRuntime::start_with_options(
                    store_id(1),
                    options(&probe, TestBehavior::ExitBeforeShutdown),
                ))
                .await
                .expect("injected post-readiness exit should start");
                assert_eq!(
                    complete_bounded(runtime.shutdown()).await,
                    Err(ShutdownError::WriterExitedBeforeShutdown)
                );

                let runtime = complete_bounded(HistorianRuntime::start_with_options(
                    store_id(1),
                    options(&probe, TestBehavior::Normal),
                ))
                .await
                .expect("normal writer should start");
                drop(runtime);
                wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 3).await;
            }
        });
    }

    #[test]
    fn command_round_trip_preserves_the_complete_canonical_admission() {
        let series = SeriesMetadata::new(series_id(1), producer_id(2), CollectionMode::Sampled);
        let envelope = CollectionEnvelope::observed(
            series.clone(),
            vec![observation(
                CollectionMode::Sampled,
                3,
                17,
                Some(position(1, 1)),
                1,
                1,
            )],
            Vec::new(),
        )
        .expect("round-trip envelope should be valid");
        let retry = retry(&series, "round-trip", 3);
        let expected = canonical_admission(store_id(1), envelope, retry);
        assert_eq!(expected.observations().len(), 1);
        assert_eq!(
            expected.lifecycle().system().evidence_id(),
            evidence_id(200)
        );
        let command = IngressCommand::new(expected.clone());

        assert_eq!(command.admission(), &expected);
        assert_eq!(command.into_admission(), expected);
    }

    #[test]
    fn store_mismatch_precedes_retry_and_capacity_and_recovers_exact_admission() {
        let ingress = HistorianIngress::new(store_id(1));
        let read = LatestReadHandle::new(ingress.shared());
        let empty = read
            .snapshot()
            .expect("new latest state should be readable");
        assert_eq!(empty.store_id(), store_id(1));
        assert!(empty.is_empty());

        let expected = command_for_store(store_id(2), "hostile-store", 7, 0).into_admission();
        let mismatch = ingress
            .try_submit(IngressCommand::new(expected.clone()))
            .expect_err("another store must be rejected");
        assert_eq!(mismatch.kind(), TrySubmitErrorKind::StoreMismatch);
        assert_eq!(mismatch.into_command().into_admission(), expected);
        assert_eq!(ingress.test_counts(), (0, 0, 0));
        assert_eq!(
            read.snapshot().expect("mismatch leaves latest available"),
            empty
        );

        for index in 0..MAX_OUTSTANDING_COMMANDS {
            ingress
                .try_submit(command(
                    &format!("fill-{index}"),
                    u8::try_from(index).expect("bounded index"),
                    u128::try_from(index).expect("bounded index"),
                ))
                .expect("same-store command should fill one slot");
        }
        assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 16, 0));

        for mismatch in [
            command_for_store(store_id(2), "fill-0", 0, 100),
            command_for_store(store_id(2), "fill-0", 99, 101),
            command_for_store(store_id(2), "distinct-while-full", 99, 102),
        ] {
            let error = ingress
                .try_submit(mismatch)
                .expect_err("store mismatch must precede retry and capacity");
            assert_eq!(error.kind(), TrySubmitErrorKind::StoreMismatch);
            assert_eq!(
                error.to_string(),
                "canonical admission store does not match historian runtime"
            );
        }
        assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 16, 0));
        assert_eq!(read.snapshot().expect("refusals do not publish"), empty);

        ingress.stop();
        let closed = ingress
            .try_submit(command_for_store(store_id(2), "closed-store", 9, 103))
            .expect_err("closed must precede store mismatch");
        assert_eq!(closed.kind(), TrySubmitErrorKind::Closed);
    }

    #[test]
    fn receipt_stays_pending_until_the_gated_writer_handles_work() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let receipt = ingress
                .try_submit(command("pending", 1, 0))
                .expect("command should queue")
                .into_receipt();

            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;
            let mut wait = Box::pin(receipt.clone().wait());
            let mut shared_wait = Box::pin(receipt.wait());
            assert!(poll_once(wait.as_mut()).await.is_pending());
            assert!(poll_once(shared_wait.as_mut()).await.is_pending());
            open_command.send(()).expect("command gate should open");
            assert_eq!(complete_bounded(wait).await, ReceiptOutcome::WriterHandled);
            assert_eq!(
                complete_bounded(shared_wait).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("handled writer should shut down");
        });
    }

    #[test]
    fn full_conflict_and_equivalent_admission_follow_retry_precedence() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();

            let first = ingress
                .try_submit(command("key-0", 10, 0))
                .expect("first command should queue");
            assert_eq!(first.disposition(), SubmissionDisposition::Queued);
            let first_receipt = first.into_receipt();
            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;

            let mut receipts = vec![first_receipt.clone()];
            for index in 1..MAX_OUTSTANDING_COMMANDS {
                let submission = ingress
                    .try_submit(command(
                        &format!("key-{index}"),
                        u8::try_from(index).expect("test index fits u8"),
                        index as u128,
                    ))
                    .expect("distinct command should fill one slot");
                assert_eq!(submission.disposition(), SubmissionDisposition::Queued);
                receipts.push(submission.into_receipt());
            }
            assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 15, 1));

            let full = ingress
                .try_submit(command("seventeenth-hostile", 99, 99))
                .expect_err("seventeenth distinct command must be recoverably full");
            assert_eq!(full.kind(), TrySubmitErrorKind::Full);
            assert!(!format!("{full:?}").contains("seventeenth-hostile"));
            assert!(!full.to_string().contains("seventeenth-hostile"));
            let recovered = full.into_command().into_admission();
            assert_eq!(recovered.retry().key().as_str(), "seventeenth-hostile");

            let equivalent = ingress
                .try_submit(command("key-0", 10, 1_000))
                .expect("equivalent retry should coalesce even while full");
            assert_eq!(equivalent.disposition(), SubmissionDisposition::Coalesced);
            let equivalent_receipt = equivalent.into_receipt();
            assert!(first_receipt.shares_state_with(&equivalent_receipt));

            let conflict = ingress
                .try_submit(command("key-0", 11, 2_000))
                .expect_err("different content for an outstanding key must conflict");
            assert_eq!(conflict.kind(), TrySubmitErrorKind::RetryConflict);
            let recovered_conflict = conflict.into_command().into_admission();
            assert_eq!(recovered_conflict.retry().content().sha256(), &[11; 32]);
            assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 15, 1));

            open_command.send(()).expect("command gate should open");
            for receipt in receipts {
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
            }
            assert_eq!(
                complete_bounded(equivalent_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("full runtime should drain and join");
        });
    }

    #[test]
    fn equivalent_storm_retains_one_runtime_work_item_and_shared_state() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let first_receipt = ingress
                .try_submit(command("storm", 7, 0))
                .expect("first storm command should queue")
                .into_receipt();
            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;

            for index in 0..1_024 {
                let duplicate = ingress
                    .try_submit(command("storm", 7, index + 10))
                    .expect("equivalent storm command should coalesce");
                assert_eq!(duplicate.disposition(), SubmissionDisposition::Coalesced);
                assert!(
                    first_receipt.shares_state_with(&duplicate.into_receipt()),
                    "every duplicate must share the first fixed terminal state"
                );
            }
            assert_eq!(ingress.test_counts(), (1, 0, 1));

            open_command.send(()).expect("command gate should open");
            assert_eq!(
                complete_bounded(first_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("storm runtime should shut down");
        });
    }

    #[test]
    fn in_flight_retry_window_releases_only_after_terminal_completion() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let original = ingress
                .try_submit(command("window", 4, 0))
                .expect("original should queue")
                .into_receipt();
            wait_until(|| ingress.test_counts() == (1, 0, 1)).await;

            let equivalent = ingress
                .try_submit(command("window", 4, 20))
                .expect("equivalent in-flight retry should coalesce");
            assert_eq!(equivalent.disposition(), SubmissionDisposition::Coalesced);
            let conflict = ingress
                .try_submit(command("window", 5, 30))
                .expect_err("conflicting in-flight retry should reject");
            assert_eq!(conflict.kind(), TrySubmitErrorKind::RetryConflict);

            open_command.send(()).expect("command gate should open");
            assert_eq!(
                complete_bounded(original.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            wait_until(|| ingress.test_counts() == (0, 0, 0)).await;

            let after_terminal = ingress
                .try_submit(command("window", 4, 40))
                .expect("qualification after durability should replay");
            assert_eq!(
                after_terminal.disposition(),
                SubmissionDisposition::Replayed
            );
            assert!(matches!(
                complete_bounded(after_terminal.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            complete_bounded(runtime.shutdown())
                .await
                .expect("retry-window runtime should shut down");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn durable_retry_horizon_replays_guards_and_expires_fifo_without_refresh() {
        harness().block_on(async {
            let directory = test_directory();
            let runtime = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("retry byte limits"),
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("open retry horizon runtime");
            let ingress = runtime.ingress();
            let submit_durable = |key: &'static str, digest: u8| {
                let ingress = ingress.clone();
                async move {
                    let submission = ingress
                        .try_submit(IngressCommand::with_policy(
                            command(key, digest, u128::from(digest)).into_admission(),
                            AdmissionPriority::Normal,
                            BarrierDemand::Immediate,
                        ))
                        .expect("fresh horizon command");
                    assert_eq!(submission.disposition(), SubmissionDisposition::Queued);
                    let DurableOutcome::Durable(commit) =
                        complete_bounded(submission.into_receipt().wait_durable()).await
                    else {
                        panic!("fresh horizon command should become durable");
                    };
                    commit
                }
            };

            let a = submit_durable("horizon-a", 1).await;
            let b = submit_durable("horizon-b", 2).await;
            let latest_before_replay = runtime
                .read_handle()
                .snapshot()
                .expect("latest before replay");
            let replay_a = ingress
                .try_submit(command("horizon-a", 1, 99))
                .expect("oldest replay hit");
            assert_eq!(replay_a.disposition(), SubmissionDisposition::Replayed);
            let replay_a = replay_a.into_receipt();
            assert_eq!(
                complete_bounded(replay_a.clone().wait_handled()).await,
                HandledOutcome::WriterHandled(a.append())
            );
            let DurableOutcome::Durable(replayed_a) =
                complete_bounded(replay_a.wait_durable()).await
            else {
                panic!("replay must be immediately durable");
            };
            assert_eq!(replayed_a, a);
            assert_eq!(
                runtime
                    .read_handle()
                    .snapshot()
                    .expect("latest after replay"),
                latest_before_replay,
                "replay cannot republish latest"
            );
            let replay_conflict = ingress
                .try_submit(command("horizon-a", 9, 100))
                .expect_err("changed replay content conflicts");
            assert_eq!(replay_conflict.kind(), TrySubmitErrorKind::RetryConflict);
            assert_eq!(
                replay_conflict.into_command().admission().retry(),
                command("horizon-a", 9, 100).admission().retry()
            );

            let _c = submit_durable("horizon-c", 3).await;
            let projected = ingress
                .shared()
                .current_retry()
                .expect("committed retry projection");
            assert_eq!(projected.replay().len(), 2);
            assert_eq!(projected.guard().len(), 1);
            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown first horizon runtime");

            let reopened = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::OpenExisting,
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                    .expect("reopen retry byte limits"),
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("reopen retry horizon runtime");
            let ingress = reopened.ingress();
            let latest_after_reopen = reopened
                .read_handle()
                .snapshot()
                .expect("latest after horizon reopen");
            assert!(latest_after_reopen.is_empty());
            let replay_b = ingress
                .try_submit(command("horizon-b", 2, 101))
                .expect("replay tier restores after reopen");
            assert_eq!(replay_b.disposition(), SubmissionDisposition::Replayed);
            assert_eq!(
                complete_bounded(replay_b.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(b)
            );
            assert_eq!(
                reopened
                    .read_handle()
                    .snapshot()
                    .expect("latest after reopened replay"),
                latest_after_reopen
            );
            let expired_a = ingress
                .try_submit(command("horizon-a", 1, 100))
                .expect_err("promoted replay remains guarded after reopen");
            assert_eq!(expired_a.kind(), TrySubmitErrorKind::RetryExpired);
            assert_eq!(
                expired_a.into_command().admission().retry(),
                command("horizon-a", 1, 100).admission().retry()
            );
            assert_eq!(
                ingress
                    .try_submit(command("horizon-a", 9, 101))
                    .expect_err("guard changed content conflicts")
                    .kind(),
                TrySubmitErrorKind::RetryConflict
            );

            let submit_durable = |key: &'static str, digest: u8| {
                let ingress = ingress.clone();
                async move {
                    let submission = ingress
                        .try_submit(IngressCommand::with_policy(
                            command(key, digest, u128::from(digest)).into_admission(),
                            AdmissionPriority::Normal,
                            BarrierDemand::Immediate,
                        ))
                        .expect("fresh reopened horizon command");
                    assert_eq!(submission.disposition(), SubmissionDisposition::Queued);
                    assert!(matches!(
                        complete_bounded(submission.into_receipt().wait_durable()).await,
                        DurableOutcome::Durable(_)
                    ));
                }
            };
            let _d = submit_durable("horizon-d", 4).await;
            // This second hit must not refresh A ahead of B in the guard FIFO.
            assert_eq!(
                ingress
                    .try_submit(command("horizon-a", 1, 102))
                    .expect_err("guard hit remains expired")
                    .kind(),
                TrySubmitErrorKind::RetryExpired
            );
            let _e = submit_durable("horizon-e", 5).await;
            complete_bounded(reopened.shutdown())
                .await
                .expect("shutdown reopened horizon runtime");

            let final_runtime = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::OpenExisting,
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                    .expect("final retry byte limits"),
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("final reopen retry horizon runtime");
            let fresh_a = final_runtime
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("horizon-a", 1, 103).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("A is fresh only after replay and guard eviction");
            assert_eq!(fresh_a.disposition(), SubmissionDisposition::Queued);
            assert!(matches!(
                complete_bounded(fresh_a.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(_)
            ));

            complete_bounded(final_runtime.shutdown())
                .await
                .expect("shutdown final horizon runtime");
            fs::remove_dir_all(directory).expect("remove retry horizon directory");
        });
    }

    #[test]
    fn original_replay_commit_survives_manifest_slot_reuse_and_reopen_exactly() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                .expect("retention byte limits");
            let group = group_policy(
                std::time::Duration::from_secs(5),
                MAX_OUTSTANDING_COMMANDS,
                64 * 1_024 * 1_024,
            );
            let retry =
                och_store::RetryPersistenceOptions::new(8, 8).expect("retention retry options");
            let runtime = complete_bounded(open_durable_test(durable_options_with_retry(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group,
                retry,
            )))
            .await
            .expect("open retained-outcome runtime");
            let ingress = runtime.ingress();
            let durable = |key: &'static str, digest: u8| {
                let ingress = ingress.clone();
                async move {
                    let submission = ingress
                        .try_submit(IngressCommand::with_policy(
                            command(key, digest, u128::from(digest)).into_admission(),
                            AdmissionPriority::Normal,
                            BarrierDemand::Immediate,
                        ))
                        .expect("retained outcome submission");
                    let DurableOutcome::Durable(commit) =
                        complete_bounded(submission.into_receipt().wait_durable()).await
                    else {
                        panic!("retained outcome should become durable");
                    };
                    commit
                }
            };
            let original = durable("retained-original", 21).await;
            let _ = durable("retained-2", 22).await;
            let _ = durable("retained-3", 23).await;
            let _ = durable("retained-4", 24).await;
            assert!(
                runtime.inspection().committed().manifest_generation()
                    >= original.manifest_commit().manifest_generation() + 3
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown retained-outcome runtime");

            let reopened = complete_bounded(open_durable_test(durable_options_with_retry(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::OpenExisting,
                bytes,
                group,
                retry,
            )))
            .await
            .expect("reopen retained-outcome runtime");
            let latest_before = reopened
                .read_handle()
                .snapshot()
                .expect("empty latest before retained replay");
            let replay = reopened
                .ingress()
                .try_submit(command("retained-original", 21, 999))
                .expect("original retained replay after reopen");
            assert_eq!(replay.disposition(), SubmissionDisposition::Replayed);
            let DurableOutcome::Durable(actual) =
                complete_bounded(replay.into_receipt().wait_durable()).await
            else {
                panic!("retained replay should be durable");
            };
            assert_eq!(actual, original);
            assert_eq!(
                reopened
                    .read_handle()
                    .snapshot()
                    .expect("latest after retained replay"),
                latest_before
            );
            complete_bounded(reopened.shutdown())
                .await
                .expect("shutdown reopened retained runtime");
            fs::remove_dir_all(directory).expect("remove retained outcome directory");
        });
    }

    #[test]
    fn durable_replay_precedes_count_saturation_without_refresh_or_new_work() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_publication, publication_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.publication_gate = Some(publication_gate);
            writer_options.publication_gate_after = 1;
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("open count saturation runtime");
            let ingress = runtime.ingress();
            let first = ingress
                .try_submit(IngressCommand::with_policy(
                    command("count-replay", 31, 0).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("queue durable count replay seed");
            assert!(matches!(
                complete_bounded(first.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(_)
            ));

            let held = ingress
                .try_submit(command("count-held-00", 40, 1))
                .expect("queue publication-held command")
                .into_receipt();
            wait_until(|| ingress.test_counts() == (1, 0, 1)).await;
            let mut queued = Vec::new();
            for index in 1_u8..16 {
                queued.push(
                    ingress
                        .try_submit(command(
                            &format!("count-held-{index:02}"),
                            40_u8.saturating_add(index),
                            u128::from(index),
                        ))
                        .expect("fill exact count window")
                        .into_receipt(),
                );
            }
            assert_eq!(ingress.test_counts().0, MAX_OUTSTANDING_COMMANDS);
            let replay = ingress
                .try_submit(command("count-replay", 31, 999))
                .expect("durable replay precedes full window");
            assert_eq!(replay.disposition(), SubmissionDisposition::Replayed);
            assert!(matches!(
                complete_bounded(replay.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            assert_eq!(
                ingress
                    .try_submit(command("count-replay", 32, 1_000))
                    .expect_err("durable conflict precedes full window")
                    .kind(),
                TrySubmitErrorKind::RetryConflict
            );
            assert_eq!(
                ingress
                    .try_submit(command("count-overflow", 90, 2_000))
                    .expect_err("distinct work sees full window")
                    .kind(),
                TrySubmitErrorKind::Full
            );
            open_publication.send(()).expect("release publication gate");
            assert_eq!(
                complete_bounded(held.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown count saturation runtime");
            for receipt in queued {
                assert!(matches!(
                    complete_bounded(receipt.wait_durable()).await,
                    DurableOutcome::Durable(_)
                ));
            }
        });
    }

    #[test]
    fn durable_replay_precedes_exact_byte_saturation() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_publication, publication_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.publication_gate = Some(publication_gate);
            writer_options.publication_gate_after = 1;
            let directory = test_directory();
            let held_command = command("byte-new-00", 52, 1);
            let exact_bytes = och_store::admission_frame_len_v1(held_command.admission())
                .expect("measure held byte frame");
            let byte_limits =
                ByteReservationLimits::new(exact_bytes, 0, 0).expect("exact byte limit");
            let group = group_policy(
                std::time::Duration::from_secs(5),
                MAX_OUTSTANDING_COMMANDS,
                exact_bytes,
            );
            let runtime = complete_bounded(HistorianRuntime::open_inner(
                durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::CreateNew,
                    byte_limits,
                    group,
                ),
                writer_options,
            ))
            .await
            .expect("open byte saturation runtime");
            runtime
                .apply_registry(default_test_registry_operation(store_id(1)))
                .await
                .expect("seed byte saturation registry");
            let ingress = runtime.ingress();
            let seed = ingress
                .try_submit(IngressCommand::with_policy(
                    command("byte-old-00", 51, 0).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("queue byte replay seed");
            assert!(matches!(
                complete_bounded(seed.into_receipt().wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            let held = ingress
                .try_submit(held_command)
                .expect("queue exact byte-held command")
                .into_receipt();
            wait_until(|| ingress.test_counts() == (1, 0, 1)).await;
            let replay = ingress
                .try_submit(command("byte-old-00", 51, 99))
                .expect("replay precedes byte saturation");
            assert_eq!(replay.disposition(), SubmissionDisposition::Replayed);
            assert_eq!(
                ingress
                    .try_submit(command("byte-new-01", 53, 2))
                    .expect_err("fresh work sees exact byte saturation")
                    .kind(),
                TrySubmitErrorKind::ByteCapacity
            );
            open_publication
                .send(())
                .expect("release byte publication gate");
            assert!(matches!(
                complete_bounded(held.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown byte saturation runtime");
            fs::remove_dir_all(directory).expect("remove byte saturation directory");
        });
    }

    #[test]
    fn sequential_distinct_admissions_are_handled_fifo() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let mut receipts = Vec::new();
            for tag in 1_u8..=6 {
                receipts.push(
                    ingress
                        .try_submit(command(&format!("fifo-{tag}"), tag, u128::from(tag)))
                        .expect("sequential distinct work should queue")
                        .into_receipt(),
                );
            }
            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;
            open_command.send(()).expect("command gate should open");
            for receipt in receipts {
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
            }
            assert_eq!(
                *probe
                    .handled_order
                    .lock()
                    .expect("handled-order probe should not be poisoned"),
                vec![1, 2, 3, 4, 5, 6]
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("FIFO runtime should shut down");
        });
    }

    #[test]
    fn cancelling_a_receipt_wait_does_not_cancel_accepted_work() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let receipt = ingress
                .try_submit(command("cancel-wait", 8, 0))
                .expect("command should queue")
                .into_receipt();
            let verifier = receipt.clone();
            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;
            let mut cancelled_wait = Box::pin(receipt.wait());
            assert!(poll_once(cancelled_wait.as_mut()).await.is_pending());
            drop(cancelled_wait);

            open_command.send(()).expect("command gate should open");
            assert_eq!(
                complete_bounded(verifier.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("receipt cancellation should not prevent shutdown");
        });
    }

    #[test]
    fn graceful_shutdown_closes_clones_drains_receipts_then_joins() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let (finish_shutdown, shutdown_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            writer_options.shutdown_gate = Some(shutdown_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let cloned_ingress = ingress.clone();
            let mut receipts = Vec::new();
            for tag in 1_u8..=3 {
                receipts.push(
                    ingress
                        .try_submit(command(&format!("drain-{tag}"), tag, u128::from(tag)))
                        .expect("pre-close command should queue")
                        .into_receipt(),
                );
            }

            let mut shutdown = Box::pin(runtime.shutdown());
            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            let closed = cloned_ingress
                .try_submit(command("after-close-hostile", 9, 90))
                .expect_err("submission linearized after close must recover as closed");
            assert_eq!(closed.kind(), TrySubmitErrorKind::Closed);
            assert_eq!(
                closed
                    .into_command()
                    .into_admission()
                    .retry()
                    .key()
                    .as_str(),
                "after-close-hostile"
            );

            open_command.send(()).expect("command gate should open");
            for receipt in receipts {
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
            }
            wait_until(|| probe.shutdown_received.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.task_dropped.load(Ordering::SeqCst), 0);
            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            finish_shutdown
                .send(())
                .expect("shutdown join gate should open");
            complete_bounded(shutdown)
                .await
                .expect("drained writer should join");
            assert_eq!(probe.task_dropped.load(Ordering::SeqCst), 1);

            let still_closed = cloned_ingress
                .try_submit(command("outlived", 10, 100))
                .expect_err("outliving ingress clone must stay closed");
            assert_eq!(still_closed.kind(), TrySubmitErrorKind::Closed);
        });
    }

    #[test]
    fn runtime_drop_and_cancelled_shutdown_stop_unhandled_receipts() {
        harness().block_on(async {
            let handled_probe = Arc::new(LifecycleProbe::default());
            let handled_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&handled_probe, TestBehavior::Normal),
            ))
            .await
            .expect("writer should start");
            let handled_receipt = handled_runtime
                .ingress()
                .try_submit(command("already-handled", 1, 0))
                .expect("command should queue")
                .into_receipt();
            let handled_verifier = handled_receipt.clone();
            assert_eq!(
                complete_bounded(handled_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            drop(handled_runtime);
            assert_eq!(
                complete_bounded(handled_verifier.wait()).await,
                ReceiptOutcome::WriterHandled,
                "terminal handled state must not be overwritten by Drop"
            );

            let drop_probe = Arc::new(LifecycleProbe::default());
            let (_keep_drop_gate_closed, drop_gate) = oneshot::channel();
            let mut drop_options = options(&drop_probe, TestBehavior::Normal);
            drop_options.command_gate = Some(drop_gate);
            let drop_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                drop_options,
            ))
            .await
            .expect("drop writer should start");
            let drop_ingress = drop_runtime.ingress();
            let in_flight = drop_ingress
                .try_submit(command("drop-in-flight", 2, 0))
                .expect("first drop command should queue")
                .into_receipt();
            let queued = drop_ingress
                .try_submit(command("drop-queued", 3, 1))
                .expect("second drop command should queue")
                .into_receipt();
            wait_until(|| drop_ingress.test_counts() == (2, 1, 1)).await;
            drop(drop_runtime);
            assert_eq!(
                complete_bounded(in_flight.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(queued.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                drop_ingress
                    .try_submit(command("drop-closed", 4, 2))
                    .expect_err("Drop must synchronously close admission")
                    .kind(),
                TrySubmitErrorKind::Closed
            );
            wait_until(|| drop_probe.task_dropped.load(Ordering::SeqCst) == 1).await;

            let cancel_probe = Arc::new(LifecycleProbe::default());
            let (_keep_cancel_gate_closed, cancel_gate) = oneshot::channel();
            let mut cancel_options = options(&cancel_probe, TestBehavior::Normal);
            cancel_options.command_gate = Some(cancel_gate);
            let cancel_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                cancel_options,
            ))
            .await
            .expect("cancel writer should start");
            let cancel_ingress = cancel_runtime.ingress();
            let cancel_in_flight = cancel_ingress
                .try_submit(command("cancel-in-flight", 5, 0))
                .expect("cancel command should queue")
                .into_receipt();
            let cancel_queued = cancel_ingress
                .try_submit(command("cancel-queued", 6, 1))
                .expect("cancel queued command should queue")
                .into_receipt();
            wait_until(|| cancel_ingress.test_counts() == (2, 1, 1)).await;
            let mut shutdown = Box::pin(cancel_runtime.shutdown());
            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            drop(shutdown);
            assert_eq!(
                complete_bounded(cancel_in_flight.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(cancel_queued.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            wait_until(|| cancel_probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(cancel_probe.normal_exits.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn early_exit_and_panic_stop_receipts_and_remain_sanitized() {
        harness().block_on(async {
            let exit_probe = Arc::new(LifecycleProbe::default());
            let exit_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&exit_probe, TestBehavior::ExitWhileHandling),
            ))
            .await
            .expect("early-exit writer should start");
            let exit_read = exit_runtime.read_handle();
            let exit_receipt = exit_runtime
                .ingress()
                .try_submit(command("exit-hostile", 1, 0))
                .expect("early-exit work should be accepted")
                .into_receipt();
            assert_eq!(
                complete_bounded(exit_receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(exit_runtime.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );
            assert_eq!(exit_read.snapshot(), Err(LatestReadError::unavailable()));

            let panic_probe = Arc::new(LifecycleProbe::default());
            let panic_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&panic_probe, TestBehavior::PanicWhileHandling),
            ))
            .await
            .expect("panic writer should report readiness first");
            let panic_read = panic_runtime.read_handle();
            let panic_receipt = panic_runtime
                .ingress()
                .try_submit(command("panic-hostile", 2, 0))
                .expect("panic work should be accepted")
                .into_receipt();
            assert_eq!(
                complete_bounded(panic_receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            let panic_error = complete_bounded(panic_runtime.shutdown())
                .await
                .expect_err("writer panic must fail shutdown");
            assert_eq!(panic_error, ShutdownError::WriterTaskPanicked);
            assert!(!panic_error.to_string().contains("hostile"));
            assert_eq!(panic_read.snapshot(), Err(LatestReadError::unavailable()));
        });
    }

    #[test]
    fn every_coordinator_failure_class_wakes_and_reaps_the_store_worker() {
        harness().block_on(async {
            let byte_limits =
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("byte limits");
            let group = group_policy(
                std::time::Duration::from_millis(2),
                MAX_OUTSTANDING_COMMANDS,
                64 * 1_024 * 1_024,
            );

            for (label, behavior, expected_shutdown) in [
                (
                    "exit",
                    TestBehavior::ExitBeforeShutdown,
                    ShutdownError::WriterExitedBeforeShutdown,
                ),
                (
                    "panic",
                    TestBehavior::PanicBeforeShutdown,
                    ShutdownError::WriterTaskPanicked,
                ),
            ] {
                let directory = test_directory();
                let probe = Arc::new(LifecycleProbe::default());
                let mut runtime = complete_bounded(HistorianRuntime::open_inner(
                    durable_options(
                        directory.clone(),
                        store_id(1),
                        och_store::ActiveJournalOpenMode::CreateNew,
                        byte_limits,
                        group,
                    ),
                    options(&probe, behavior),
                ))
                .await
                .unwrap_or_else(|error| panic!("{label} runtime should start: {error:?}"));
                assert_worker_reaped_and_lock_reopens(&mut runtime, &directory).await;
                assert_eq!(
                    complete_bounded(runtime.shutdown()).await,
                    Err(expected_shutdown)
                );
                fs::remove_dir_all(directory).expect("remove coordinator failure directory");
            }

            let directory = test_directory();
            let probe = Arc::new(LifecycleProbe::default());
            let mut cancelled = complete_bounded(HistorianRuntime::open_inner(
                durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::CreateNew,
                    byte_limits,
                    group,
                ),
                options(&probe, TestBehavior::Normal),
            ))
            .await
            .expect("cancelled runtime should start");
            cancelled
                .writer
                .as_ref()
                .expect("runtime retains coordinator")
                .abort();
            assert_worker_reaped_and_lock_reopens(&mut cancelled, &directory).await;
            assert_eq!(
                complete_bounded(cancelled.shutdown()).await,
                Err(ShutdownError::WriterTaskCancelled)
            );
            fs::remove_dir_all(directory).expect("remove cancelled coordinator directory");

            let directory = test_directory();
            let probe = Arc::new(LifecycleProbe::default());
            let mut publication = complete_bounded(HistorianRuntime::open_inner(
                durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::CreateNew,
                    byte_limits,
                    group,
                ),
                options(&probe, TestBehavior::FaultBeforePublicationSwap),
            ))
            .await
            .expect("publication-fault runtime should start");
            let receipt = publication
                .ingress()
                .try_submit(command("publication-reap", 10, 0))
                .expect("publication-fault command should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_worker_reaped_and_lock_reopens(&mut publication, &directory).await;
            assert_eq!(
                complete_bounded(publication.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );
            fs::remove_dir_all(directory).expect("remove publication-fault directory");
        });
    }

    #[test]
    fn task_abort_stops_in_flight_receipt_and_maps_shutdown() {
        harness().block_on(async {
            let cancel_probe = Arc::new(LifecycleProbe::default());
            let (_keep_cancel_gate_closed, cancel_gate) = oneshot::channel();
            let mut cancel_options = options(&cancel_probe, TestBehavior::Normal);
            cancel_options.command_gate = Some(cancel_gate);
            let cancel_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                cancel_options,
            ))
            .await
            .expect("cancel writer should start");
            let cancel_read = cancel_runtime.read_handle();
            let cancel_receipt = cancel_runtime
                .ingress()
                .try_submit(command("cancel-hostile", 3, 0))
                .expect("cancel work should be accepted")
                .into_receipt();
            wait_until(|| cancel_probe.commands_started.load(Ordering::SeqCst) == 1).await;
            cancel_runtime
                .writer
                .as_ref()
                .expect("runtime should retain writer")
                .abort();
            assert_eq!(
                complete_bounded(cancel_receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(cancel_runtime.shutdown()).await,
                Err(ShutdownError::WriterTaskCancelled)
            );
            assert_eq!(cancel_read.snapshot(), Err(LatestReadError::unavailable()));
        });
    }

    #[test]
    fn poisoned_admission_stops_receipts_and_fails_closed() {
        harness().block_on(async {
            let poison_probe = Arc::new(LifecycleProbe::default());
            let (open_poison_gate, poison_gate) = oneshot::channel();
            let mut poison_options = options(&poison_probe, TestBehavior::Normal);
            poison_options.command_gate = Some(poison_gate);
            let poison_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                poison_options,
            ))
            .await
            .expect("poison writer should start");
            let poison_ingress = poison_runtime.ingress();
            let poison_read = poison_runtime.read_handle();
            let old_snapshot = poison_read
                .snapshot()
                .expect("pre-poison snapshot should be available");
            let poison_receipt = poison_ingress
                .try_submit(command("poison-hostile", 4, 0))
                .expect("poison work should be accepted")
                .into_receipt();
            wait_until(|| poison_ingress.test_counts() == (1, 0, 1)).await;
            poison_ingress.poison_for_test();
            let poison_rejection = poison_ingress
                .try_submit(command("poison-recovery-hostile", 5, 1))
                .expect_err("poison recovery must close admission");
            assert_eq!(poison_rejection.kind(), TrySubmitErrorKind::Closed);
            assert!(!format!("{poison_rejection:?}").contains("hostile"));
            assert_eq!(poison_read.snapshot(), Err(LatestReadError::unavailable()));
            assert!(old_snapshot.is_empty());
            assert_eq!(
                complete_bounded(poison_receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            open_poison_gate
                .send(())
                .expect("poison command gate should open");
            assert_eq!(
                complete_bounded(poison_runtime.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );
        });
    }

    #[test]
    fn runtimes_and_ingress_handles_are_isolated() {
        harness().block_on(async {
            let first_probe = Arc::new(LifecycleProbe::default());
            let second_probe = Arc::new(LifecycleProbe::default());
            let (_keep_first_closed, first_gate) = oneshot::channel();
            let (open_second, second_gate) = oneshot::channel();
            let mut first_options = options(&first_probe, TestBehavior::Normal);
            first_options.command_gate = Some(first_gate);
            let mut second_options = options(&second_probe, TestBehavior::Normal);
            second_options.command_gate = Some(second_gate);
            let first = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                first_options,
            ))
            .await
            .expect("first writer should start");
            let second = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                second_options,
            ))
            .await
            .expect("second writer should start");
            let first_ingress = first.ingress();
            let second_ingress = second.ingress();
            let first_receipt = first_ingress
                .try_submit(command("same-key", 1, 0))
                .expect("first isolated command should queue")
                .into_receipt();
            let second_receipt = second_ingress
                .try_submit(command("same-key", 2, 0))
                .expect("same key in another runtime should be independent")
                .into_receipt();
            wait_until(|| first_ingress.test_counts() == (1, 0, 1)).await;
            wait_until(|| second_ingress.test_counts() == (1, 0, 1)).await;

            drop(first);
            assert_eq!(
                complete_bounded(first_receipt.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(second_ingress.test_counts(), (1, 0, 1));
            open_second
                .send(())
                .expect("second command gate should open");
            assert_eq!(
                complete_bounded(second_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(second.shutdown())
                .await
                .expect("second runtime should shut down independently");
        });
    }

    #[test]
    fn repeated_hostile_ingress_sequences_remain_fixed_and_closed() {
        harness().block_on(async {
            for round in 0_u8..16 {
                let ingress = HistorianIngress::new(store_id(1));
                let first_receipt = ingress
                    .try_submit(command("repeat-0", round, 0))
                    .expect("first repeated command should queue")
                    .into_receipt();
                let mut receipts = vec![first_receipt.clone()];
                for index in 1..MAX_OUTSTANDING_COMMANDS {
                    receipts.push(
                        ingress
                            .try_submit(command(
                                &format!("repeat-{index}"),
                                u8::try_from(index).expect("test index fits u8"),
                                index as u128,
                            ))
                            .expect("repeated distinct command should fill one slot")
                            .into_receipt(),
                    );
                }
                assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 16, 0));

                let equivalent = ingress
                    .try_submit(command("repeat-0", round, 10_000))
                    .expect("repeated equivalent should coalesce at full capacity");
                assert_eq!(equivalent.disposition(), SubmissionDisposition::Coalesced);
                assert!(first_receipt.shares_state_with(&equivalent.into_receipt()));
                assert_eq!(
                    ingress
                        .try_submit(command("repeat-0", round.wrapping_add(1), 20_000))
                        .expect_err("repeated conflict should reject")
                        .kind(),
                    TrySubmitErrorKind::RetryConflict
                );
                assert_eq!(
                    ingress
                        .try_submit(command("repeat-full", 99, 30_000))
                        .expect_err("repeated distinct overflow should reject")
                        .kind(),
                    TrySubmitErrorKind::Full
                );
                assert_eq!(ingress.test_counts(), (MAX_OUTSTANDING_COMMANDS, 16, 0));

                ingress.stop();
                assert_eq!(ingress.test_counts(), (0, 0, 0));
                for receipt in receipts {
                    assert_eq!(
                        complete_bounded(receipt.wait()).await,
                        ReceiptOutcome::WriterStopped
                    );
                }
                assert_eq!(
                    ingress
                        .try_submit(command("repeat-closed", 100, 40_000))
                        .expect_err("stopped repeated ingress must stay closed")
                        .kind(),
                    TrySubmitErrorKind::Closed
                );
            }
        });
    }

    #[test]
    fn readiness_exposes_empty_isolated_snapshots_and_drop_closes_reads() {
        harness().block_on(async {
            let first_probe = Arc::new(LifecycleProbe::default());
            let second_probe = Arc::new(LifecycleProbe::default());
            let first = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&first_probe, TestBehavior::Normal),
            ))
            .await
            .expect("first writer should start");
            let second = complete_bounded(HistorianRuntime::start_with_options(
                store_id(2),
                options(&second_probe, TestBehavior::Normal),
            ))
            .await
            .expect("second writer should start");
            let first_read = first.read_handle();
            let first_read_clone = first_read.clone();
            let second_read = second.read_handle();
            assert_eq!(first.store_id(), store_id(1));
            assert_eq!(first.ingress().store_id(), store_id(1));
            assert_eq!(first_read.store_id(), store_id(1));
            assert_eq!(second.store_id(), store_id(2));
            assert_eq!(second.ingress().store_id(), store_id(2));
            assert_eq!(second_read.store_id(), store_id(2));

            let first_empty = first_read
                .snapshot()
                .expect("ready registry should be available");
            assert!(first_empty.is_empty());
            assert_eq!(first_empty.len(), 0);
            assert_eq!(first_empty.as_slice(), &[]);
            assert_eq!(first_empty.iter().count(), 0);
            assert_eq!(first_empty.store_id(), store_id(1));
            let second_empty = second_read.snapshot().expect("second registry");
            assert_eq!(second_empty.store_id(), store_id(2));
            assert!(second_empty.is_empty());
            assert_eq!(format!("{first_read:?}"), "LatestReadHandle { .. }");

            register_metadata(
                &first,
                &SeriesMetadata::new(series_id(30), producer_id(31), CollectionMode::Sampled),
            )
            .await;

            let receipt = first
                .ingress()
                .try_submit(positioned_command(
                    30,
                    31,
                    CollectionMode::Sampled,
                    32,
                    1,
                    100,
                    "isolated-latest",
                ))
                .expect("positioned command should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            let published = first_read
                .snapshot()
                .expect("first snapshot should advance");
            assert_eq!(published.store_id(), store_id(1));
            assert_eq!(published.len(), 1);
            let second_still_empty = second_read.snapshot().expect("second snapshot");
            assert_eq!(second_still_empty.store_id(), store_id(2));
            assert!(second_still_empty.is_empty());

            drop(first);
            assert_eq!(first_read.snapshot(), Err(LatestReadError::unavailable()));
            assert_eq!(
                first_read_clone.snapshot(),
                Err(LatestReadError::unavailable())
            );
            assert_eq!(published.len(), 1, "an acquired snapshot stays usable");
            assert!(
                first_empty.is_empty(),
                "the old empty snapshot stays immutable"
            );

            complete_bounded(second.shutdown())
                .await
                .expect("second writer should seal independently");
            let second_sealed = second_read.snapshot().expect("sealed snapshot");
            assert_eq!(second_sealed.store_id(), store_id(2));
            assert!(second_sealed.is_empty());
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn producer_position_alone_selects_exact_observations_for_all_modes() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&probe, TestBehavior::Normal),
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let read = runtime.read_handle();
            let metadata =
                SeriesMetadata::new(series_id(40), producer_id(41), CollectionMode::Sampled);
            register_metadata(&runtime, &metadata).await;
            let timestamp_and_id_later = observation(
                CollectionMode::Sampled,
                250,
                1,
                Some(position(7, 10)),
                20_000,
                20_000,
            );
            let greatest_position = observation(
                CollectionMode::Sampled,
                1,
                2,
                Some(position(7, 11)),
                -20_000,
                -20_000,
            );
            let receipt = ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![timestamp_and_id_later, greatest_position.clone()],
                    "multi-position",
                    1,
                ))
                .expect("multi-observation command should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            let after_multi = read.snapshot().expect("multi snapshot should be readable");
            let published = after_multi
                .get(&metadata.series_id())
                .expect("series should be published");
            assert_eq!(published.series_metadata(), &metadata);
            assert_eq!(published.observation(), &greatest_position);
            assert_eq!(published.producer_position(), position(7, 11));
            assert!(!format!("{published:?}").contains(&metadata.series_id().to_string()));

            let equal = ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![greatest_position.clone()],
                    "equal-identical",
                    2,
                ))
                .expect("equal-identical command should queue")
                .into_receipt();
            let stale = observation(
                CollectionMode::Sampled,
                255,
                99,
                Some(position(1, 1)),
                30_000,
                30_000,
            );
            let stale_receipt = ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![stale],
                    "stale-adversarial",
                    3,
                ))
                .expect("stale command should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(equal.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            assert_eq!(
                complete_bounded(stale_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            assert_eq!(
                read.snapshot()
                    .expect("no-op snapshot")
                    .get(&metadata.series_id())
                    .expect("series should remain")
                    .observation(),
                &greatest_position
            );

            let old_snapshot = read.snapshot().expect("old snapshot should capture");
            let greater = observation(
                CollectionMode::Sampled,
                0,
                3,
                Some(position(7, 12)),
                -30_000,
                -30_000,
            );
            let greater_receipt = ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![greater.clone()],
                    "greater-adversarial",
                    4,
                ))
                .expect("greater command should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(greater_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            assert_eq!(
                read.snapshot()
                    .expect("advance must be visible after receipt")
                    .get(&metadata.series_id())
                    .expect("series should remain")
                    .observation(),
                &greater
            );
            assert_eq!(
                old_snapshot
                    .get(&metadata.series_id())
                    .expect("old snapshot should retain series")
                    .observation(),
                &greatest_position,
                "held snapshots never change"
            );

            let modes = [
                CollectionMode::Sampled,
                CollectionMode::ChangeOnly,
                CollectionMode::Cumulative,
                CollectionMode::Interval,
                CollectionMode::Event,
            ];
            for (index, mode) in modes.into_iter().enumerate() {
                let tag = u8::try_from(50 + index).expect("mode tag should fit");
                register_metadata(
                    &runtime,
                    &SeriesMetadata::new(series_id(tag), producer_id(tag + 10), mode),
                )
                .await;
                let receipt = ingress
                    .try_submit(positioned_command(
                        tag,
                        tag + 10,
                        mode,
                        tag + 20,
                        1,
                        u64::from(tag),
                        &format!("mode-{index}"),
                    ))
                    .expect("each mode should queue")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
                let snapshot = read.snapshot().expect("mode snapshot should be available");
                let entry = snapshot
                    .get(&series_id(tag))
                    .expect("positioned mode should publish");
                assert_eq!(entry.series_metadata().collection_mode(), mode);
                assert_eq!(
                    entry.observation().producer_position(),
                    Some(position(1, 1))
                );
            }

            complete_bounded(runtime.shutdown())
                .await
                .expect("mode runtime should shut down");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn ineligible_evidence_neither_binds_metadata_nor_consumes_capacity() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&probe, TestBehavior::Normal),
            ))
            .await
            .expect("writer should start");
            let ingress = runtime.ingress();
            let read = runtime.read_handle();
            let gap_metadata =
                SeriesMetadata::new(series_id(70), producer_id(71), CollectionMode::Sampled);
            let gap = Gap::new(
                ProducerEpoch::new(0),
                ProducerSequence::new(1),
                ProducerSequence::new(2),
                GapReason::Unknown,
            )
            .expect("test gap should be nonempty");
            let gap_only = CollectionEnvelope::observed(gap_metadata, Vec::new(), vec![gap])
                .expect("gap-only envelope should be valid");
            let unpositioned_metadata =
                SeriesMetadata::new(series_id(71), producer_id(72), CollectionMode::Event);
            let unpositioned = CollectionEnvelope::observed(
                unpositioned_metadata,
                vec![observation(CollectionMode::Event, 73, 1, None, 1, 1)],
                Vec::new(),
            )
            .expect("unpositioned envelope should be valid");
            let no_change_metadata =
                SeriesMetadata::new(series_id(72), producer_id(74), CollectionMode::ChangeOnly);
            let no_change = CollectionEnvelope::no_change(
                no_change_metadata,
                NoChange::new(
                    TimeInterval::new(timestamp(0), timestamp(1))
                        .expect("no-change interval should be nonempty"),
                ),
            )
            .expect("no-change envelope should be valid");

            for metadata in [
                SeriesMetadata::new(series_id(70), producer_id(71), CollectionMode::Sampled),
                SeriesMetadata::new(series_id(71), producer_id(72), CollectionMode::Event),
                SeriesMetadata::new(series_id(72), producer_id(74), CollectionMode::ChangeOnly),
            ] {
                register_metadata(&runtime, &metadata).await;
            }
            for (index, envelope) in [gap_only, unpositioned, no_change].into_iter().enumerate() {
                let receipt = ingress
                    .try_submit(envelope_command(
                        envelope,
                        &format!("ineligible-{index}"),
                        u8::try_from(index).expect("index should fit"),
                    ))
                    .expect("ineligible evidence should queue")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
                assert!(
                    read.snapshot()
                        .expect("snapshot should stay available")
                        .is_empty()
                );
            }

            let bound_metadata =
                SeriesMetadata::new(series_id(73), producer_id(75), CollectionMode::Cumulative);
            register_metadata(&runtime, &bound_metadata).await;
            let bound_observation = observation(
                CollectionMode::Cumulative,
                76,
                10,
                Some(position(3, 1)),
                1,
                1,
            );
            let bind = ingress
                .try_submit(observed_command(
                    bound_metadata.clone(),
                    vec![bound_observation.clone()],
                    "bind-after-ineligible",
                    10,
                ))
                .expect("first eligible metadata should bind")
                .into_receipt();
            assert_eq!(
                complete_bounded(bind.wait()).await,
                ReceiptOutcome::WriterHandled
            );

            let later_mismatch =
                SeriesMetadata::new(series_id(74), producer_id(77), CollectionMode::Event);
            register_metadata(&runtime, &later_mismatch).await;
            let later_gap = Gap::new(
                ProducerEpoch::new(0),
                ProducerSequence::new(5),
                ProducerSequence::new(6),
                GapReason::Unknown,
            )
            .expect("later gap should be nonempty");
            let mismatch_noop =
                CollectionEnvelope::observed(later_mismatch, Vec::new(), vec![later_gap])
                    .expect("mismatched gap-only evidence should be valid");
            let mismatch_receipt = ingress
                .try_submit(envelope_command(mismatch_noop, "mismatch-noop", 11))
                .expect("ineligible mismatch should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(mismatch_receipt.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            let snapshot = read
                .snapshot()
                .expect("bound snapshot should remain available");
            assert_eq!(snapshot.len(), 1);
            let entry = snapshot
                .get(&bound_metadata.series_id())
                .expect("eligible metadata should bind");
            assert_eq!(entry.series_metadata(), &bound_metadata);
            assert_eq!(entry.observation(), &bound_observation);

            complete_bounded(runtime.shutdown())
                .await
                .expect("ineligible runtime should shut down");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn equal_conflict_and_each_metadata_mismatch_fail_closed() {
        harness().block_on(async {
            let equal_probe = Arc::new(LifecycleProbe::default());
            let equal_runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&equal_probe, TestBehavior::Normal),
            ))
            .await
            .expect("equal-conflict writer should start");
            let equal_ingress = equal_runtime.ingress();
            let equal_read = equal_runtime.read_handle();
            let metadata =
                SeriesMetadata::new(series_id(80), producer_id(81), CollectionMode::Sampled);
            register_metadata(&equal_runtime, &metadata).await;
            let original = observation(CollectionMode::Sampled, 82, 1, Some(position(1, 9)), 1, 1);
            let seed = equal_ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![original.clone()],
                    "equal-seed",
                    1,
                ))
                .expect("seed should queue")
                .into_receipt();
            assert_eq!(
                complete_bounded(seed.wait()).await,
                ReceiptOutcome::WriterHandled
            );
            let old = equal_read
                .snapshot()
                .expect("seed snapshot should be readable");
            let different = observation(CollectionMode::Sampled, 83, 2, Some(position(1, 9)), 2, 2);
            let conflict = equal_ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![different],
                    "equal-conflict",
                    2,
                ))
                .expect("conflict reaches publication decision")
                .into_receipt();
            let unresolved = equal_ingress
                .try_submit(positioned_command(
                    84,
                    85,
                    CollectionMode::Event,
                    86,
                    1,
                    3,
                    "after-conflict",
                ))
                .expect("work queued behind the fault should be accepted")
                .into_receipt();
            assert_eq!(
                complete_bounded(conflict.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(unresolved.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(equal_read.snapshot(), Err(LatestReadError::unavailable()));
            assert_eq!(
                old.get(&metadata.series_id())
                    .expect("old snapshot remains valid")
                    .observation(),
                &original
            );
            assert_eq!(
                complete_bounded(equal_runtime.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );

            for (index, conflicting_metadata) in [
                SeriesMetadata::new(series_id(90), producer_id(92), CollectionMode::Sampled),
                SeriesMetadata::new(series_id(90), producer_id(91), CollectionMode::Event),
            ]
            .into_iter()
            .enumerate()
            {
                let probe = Arc::new(LifecycleProbe::default());
                let runtime = complete_bounded(HistorianRuntime::start_with_options(
                    store_id(1),
                    options(&probe, TestBehavior::Normal),
                ))
                .await
                .expect("metadata-conflict writer should start");
                let ingress = runtime.ingress();
                let read = runtime.read_handle();
                let bound =
                    SeriesMetadata::new(series_id(90), producer_id(91), CollectionMode::Sampled);
                register_metadata(&runtime, &bound).await;
                let seed = ingress
                    .try_submit(observed_command(
                        bound,
                        vec![observation(
                            CollectionMode::Sampled,
                            93,
                            1,
                            Some(position(1, 1)),
                            1,
                            1,
                        )],
                        &format!("metadata-seed-{index}"),
                        3,
                    ))
                    .expect("metadata seed should queue")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(seed.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
                let mode = conflicting_metadata.collection_mode();
                let fault = ingress
                    .try_submit(observed_command(
                        conflicting_metadata,
                        vec![observation(mode, 94, 2, Some(position(1, 2)), 2, 2)],
                        &format!("metadata-conflict-{index}"),
                        4,
                    ))
                    .expect("eligible mismatch should reach publication")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(fault.wait()).await,
                    ReceiptOutcome::WriterStopped
                );
                assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
                assert_eq!(
                    complete_bounded(runtime.shutdown()).await,
                    Err(ShutdownError::WriterExitedBeforeShutdown)
                );
            }
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn fixed_series_capacity_allows_existing_and_ineligible_work_then_faults() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                options(&probe, TestBehavior::Normal),
            ))
            .await
            .expect("capacity writer should start");
            let ingress = runtime.ingress();
            let read = runtime.read_handle();
            assert_eq!(MAX_PUBLISHED_SERIES, 16);
            assert_eq!(MAX_OUTSTANDING_COMMANDS, 16);

            for index in 0..MAX_PUBLISHED_SERIES {
                let tag = u8::try_from(index + 2).expect("series index should fit");
                let metadata = SeriesMetadata::new(
                    series_id(tag),
                    producer_id(tag + 32),
                    CollectionMode::Sampled,
                );
                register_metadata(&runtime, &metadata).await;
                let receipt = ingress
                    .try_submit(positioned_command(
                        tag,
                        tag + 32,
                        CollectionMode::Sampled,
                        tag + 64,
                        1,
                        u64::from(tag),
                        &format!("capacity-{index}"),
                    ))
                    .expect("bounded series should queue")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
            }
            assert_eq!(read.snapshot().expect("full snapshot").len(), 16);

            let update = ingress
                .try_submit(positioned_command(
                    2,
                    34,
                    CollectionMode::Sampled,
                    100,
                    2,
                    999,
                    "capacity-update",
                ))
                .expect("existing series may update at capacity")
                .into_receipt();
            let stale = ingress
                .try_submit(positioned_command(
                    3,
                    35,
                    CollectionMode::Sampled,
                    101,
                    0,
                    888,
                    "capacity-stale",
                ))
                .expect("existing stale series may no-op at capacity")
                .into_receipt();
            let new_metadata =
                SeriesMetadata::new(series_id(18), producer_id(50), CollectionMode::Sampled);
            register_metadata(&runtime, &new_metadata).await;
            let ineligible_gap = Gap::new(
                ProducerEpoch::new(0),
                ProducerSequence::new(5),
                ProducerSequence::new(6),
                GapReason::Unknown,
            )
            .expect("capacity gap should be nonempty");
            let ineligible = ingress
                .try_submit(envelope_command(
                    CollectionEnvelope::observed(new_metadata, Vec::new(), vec![ineligible_gap])
                        .expect("capacity gap-only envelope should be valid"),
                    "capacity-ineligible",
                    5,
                ))
                .expect("new ineligible series remains valid at capacity")
                .into_receipt();
            for receipt in [update, stale, ineligible] {
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterHandled
                );
            }
            let before_fault = read.snapshot().expect("capacity view should be readable");
            assert_eq!(before_fault.len(), 16);
            assert_eq!(
                before_fault
                    .get(&series_id(2))
                    .expect("updated series remains")
                    .producer_position(),
                position(1, 2)
            );
            assert!(before_fault.get(&series_id(18)).is_none());

            let overflow = ingress
                .try_submit(positioned_command(
                    18,
                    50,
                    CollectionMode::Sampled,
                    102,
                    1,
                    17,
                    "capacity-overflow",
                ))
                .expect("seventeenth eligible command is admitted before publication")
                .into_receipt();
            assert_eq!(
                complete_bounded(overflow.wait()).await,
                ReceiptOutcome::WriterStopped
            );
            assert_eq!(read.snapshot(), Err(LatestReadError::unavailable()));
            assert_eq!(before_fault.len(), 16);
            assert!(before_fault.get(&series_id(18)).is_none());
            assert_eq!(
                ingress
                    .try_submit(command("closed-after-capacity", 9, 0))
                    .expect_err("publication fault closes ingress")
                    .kind(),
                TrySubmitErrorKind::Closed
            );
            assert_eq!(
                complete_bounded(runtime.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );
        });
    }

    #[test]
    fn publication_gate_coalescing_and_graceful_seal_preserve_atomic_ordering() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_publication, publication_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.publication_gate = Some(publication_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(
                store_id(1),
                writer_options,
            ))
            .await
            .expect("gated writer should start");
            let ingress = runtime.ingress();
            let read = runtime.read_handle();
            let dropped_read_clone = read.clone();
            let metadata =
                SeriesMetadata::new(series_id(110), producer_id(111), CollectionMode::Event);
            register_metadata(&runtime, &metadata).await;
            let admitted = observation(CollectionMode::Event, 112, 1, Some(position(2, 1)), 1, 1);
            let first = ingress
                .try_submit(observed_command(
                    metadata.clone(),
                    vec![admitted.clone()],
                    "publication-storm",
                    7,
                ))
                .expect("first positioned command should queue");
            assert_eq!(first.disposition(), SubmissionDisposition::Queued);
            let first_receipt = first.into_receipt();
            for index in 0..1_024_u64 {
                let tag = u8::try_from(index % 100 + 120).expect("storm tag should fit");
                let duplicate = ingress
                    .try_submit(observed_command(
                        metadata.clone(),
                        vec![observation(
                            CollectionMode::Event,
                            tag,
                            index + 10,
                            Some(position(9, u128::from(index) + 10)),
                            9,
                            9,
                        )],
                        "publication-storm",
                        7,
                    ))
                    .expect("equivalent retry should coalesce");
                assert_eq!(duplicate.disposition(), SubmissionDisposition::Coalesced);
                assert!(first_receipt.shares_state_with(&duplicate.into_receipt()));
            }
            assert_eq!(ingress.test_counts(), (1, 1, 0));
            wait_until(|| probe.commands_started.load(Ordering::SeqCst) == 1).await;
            assert_eq!(ingress.test_counts(), (1, 0, 1));

            let old = read.snapshot().expect("old view should be available");
            assert_eq!(old.store_id(), store_id(1));
            assert!(old.is_empty(), "publication gate precedes atomic swap");
            let mut pending = Box::pin(first_receipt.clone().wait());
            assert!(poll_once(pending.as_mut()).await.is_pending());
            drop(dropped_read_clone);
            open_publication
                .send(())
                .expect("publication gate should open");
            assert_eq!(
                complete_bounded(pending).await,
                ReceiptOutcome::WriterHandled
            );
            let after_receipt = read
                .snapshot()
                .expect("advance must precede handled receipt visibility");
            assert_eq!(after_receipt.store_id(), store_id(1));
            assert_eq!(after_receipt.len(), 1);
            let entry = after_receipt
                .get(&metadata.series_id())
                .expect("first admitted observation should publish");
            assert_eq!(entry.observation(), &admitted);
            assert_eq!(entry.producer_position(), position(2, 1));
            assert!(old.is_empty(), "old snapshot remains immutable");

            complete_bounded(runtime.shutdown())
                .await
                .expect("graceful shutdown should seal the final view");
            let sealed = read
                .snapshot()
                .expect("read handle should outlive shutdown");
            assert_eq!(sealed.store_id(), store_id(1));
            assert_eq!(sealed, after_receipt);
        });
    }

    #[test]
    fn injected_pre_and_post_swap_faults_never_expose_candidate_snapshots() {
        harness().block_on(async {
            for behavior in [
                TestBehavior::FaultBeforePublicationSwap,
                TestBehavior::FaultAfterPublicationSwap,
            ] {
                let probe = Arc::new(LifecycleProbe::default());
                let runtime = complete_bounded(HistorianRuntime::start_with_options(
                    store_id(1),
                    options(&probe, behavior),
                ))
                .await
                .expect("fault-injected writer should start");
                let read = runtime.read_handle();
                let old = read
                    .snapshot()
                    .expect("pre-fault snapshot should be available");
                register_metadata(
                    &runtime,
                    &SeriesMetadata::new(
                        series_id(200),
                        producer_id(201),
                        CollectionMode::Cumulative,
                    ),
                )
                .await;
                let receipt = runtime
                    .ingress()
                    .try_submit(positioned_command(
                        200,
                        201,
                        CollectionMode::Cumulative,
                        202,
                        1,
                        1,
                        "injected-publication-fault",
                    ))
                    .expect("faulting command should queue")
                    .into_receipt();
                assert_eq!(
                    complete_bounded(receipt.wait()).await,
                    ReceiptOutcome::WriterStopped
                );
                assert_eq!(runtime.inspection().health(), RuntimeHealth::Faulted);
                let error = read
                    .snapshot()
                    .expect_err("future snapshots must be unavailable");
                assert_eq!(error, LatestReadError::unavailable());
                assert_eq!(format!("{error:?}"), "LatestReadError");
                assert_eq!(
                    error.to_string(),
                    "latest observation snapshot is unavailable"
                );
                assert!(!error.to_string().contains("injected"));
                assert!(old.is_empty(), "old snapshots remain valid after faults");
                assert_eq!(
                    complete_bounded(runtime.shutdown()).await,
                    Err(ShutdownError::WriterExitedBeforeShutdown)
                );
            }
        });
    }

    #[test]
    fn handled_and_durable_stages_reopen_with_restart_retry_authority() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("byte limits");
            let runtime = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("create durable runtime");
            let read = runtime.read_handle();
            let receipt = runtime
                .ingress()
                .try_submit(command("durable-reopen", 42, 10))
                .expect("queue durable admission")
                .into_receipt();
            let handled = complete_bounded(receipt.clone().wait_handled()).await;
            let HandledOutcome::WriterHandled(append) = handled else {
                panic!("writer should reach handled stage");
            };
            assert_eq!(append.append_sequence(), 1);
            let mut durable = Box::pin(receipt.clone().wait_durable());
            assert!(poll_once(durable.as_mut()).await.is_pending());
            assert_eq!(runtime.inspection().pending_count(), 1);
            assert!(runtime.inspection().pending_bytes() > 0);
            assert_eq!(runtime.inspection().store().sync_count(), 0);

            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown forces final barrier and joins");
            let DurableOutcome::Durable(commit) = complete_bounded(durable).await else {
                panic!("shutdown should make accepted work durable");
            };
            assert_eq!(commit.append(), append);
            assert!(commit.durable_cutoff().append_sequence() >= append.append_sequence());
            assert_eq!(commit.durable_cutoff().checkpoint_generation(), 2);
            let _sealed = read
                .snapshot()
                .expect("sealed snapshot remains readable after shutdown");

            let reopened = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::OpenExisting,
                bytes,
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("reopen durable prefix");
            assert_eq!(reopened.recovered_records().len(), 1);
            assert_eq!(
                reopened
                    .inspection()
                    .store()
                    .durable_cutoff()
                    .checkpoint_generation(),
                2
            );
            assert_eq!(
                reopened.recovered_records()[0].retry(),
                command("durable-reopen", 42, 10).admission().retry()
            );
            assert!(
                reopened
                    .read_handle()
                    .snapshot()
                    .expect("latest restarts available and empty")
                    .is_empty()
            );
            let equivalent = reopened
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("durable-reopen", 42, 10).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("restart restores completed retry projection");
            assert_eq!(equivalent.disposition(), SubmissionDisposition::Replayed);
            let DurableOutcome::Durable(second) =
                complete_bounded(equivalent.into_receipt().wait_durable()).await
            else {
                panic!("restored active-scope admission should replay durably");
            };
            assert_eq!(second.append().append_sequence(), 1);
            assert_eq!(second.durable_cutoff().checkpoint_generation(), 2);
            complete_bounded(reopened.shutdown())
                .await
                .expect("reopened runtime shutdown");
            fs::remove_dir_all(directory).expect("remove durable test directory");
        });
    }

    #[test]
    fn exact_class_and_global_byte_reservations_refuse_without_mutation() {
        harness().block_on(async {
            let first = command("class-0", 1, 0);
            let second = command("class-1", 2, 1);
            let third = command("class-2", 3, 2);
            let fourth = command("class-3", 4, 3);
            let frame_bytes =
                och_store::admission_frame_len_v1(first.admission()).expect("count first frame");
            assert_eq!(
                och_store::admission_frame_len_v1(second.admission()),
                Ok(frame_bytes)
            );
            let limits = ByteReservationLimits::new(frame_bytes * 3, frame_bytes, frame_bytes)
                .expect("nested class law");
            let ingress = HistorianIngress::new_with_limits(
                store_id(1),
                limits,
                och_store::RetryPersistenceOptions::new(2, 2).expect("retry options"),
            );
            let first_receipt = ingress
                .try_submit(IngressCommand::with_policy(
                    first.into_admission(),
                    AdmissionPriority::Bulk,
                    BarrierDemand::Group,
                ))
                .expect("bulk exact class maximum")
                .into_receipt();
            let error = ingress
                .try_submit(IngressCommand::with_policy(
                    second.into_admission(),
                    AdmissionPriority::Bulk,
                    BarrierDemand::Group,
                ))
                .expect_err("bulk max plus one refuses");
            assert_eq!(error.kind(), TrySubmitErrorKind::ByteCapacity);
            let second = error.into_command();
            let second_receipt = ingress
                .try_submit(IngressCommand::with_policy(
                    second.into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Group,
                ))
                .expect("normal reserve remains available")
                .into_receipt();
            let third_receipt = ingress
                .try_submit(IngressCommand::with_policy(
                    third.into_admission(),
                    AdmissionPriority::Protected,
                    BarrierDemand::Group,
                ))
                .expect("protected reserve reaches exact global max")
                .into_receipt();
            assert_eq!(ingress.shared().pending_counts(), (3, frame_bytes * 3));
            let error = ingress
                .try_submit(IngressCommand::with_policy(
                    fourth.into_admission(),
                    AdmissionPriority::Protected,
                    BarrierDemand::Group,
                ))
                .expect_err("global byte max plus one refuses");
            assert_eq!(error.kind(), TrySubmitErrorKind::ByteCapacity);
            assert_eq!(ingress.shared().pending_counts(), (3, frame_bytes * 3));
            ingress.stop();
            for receipt in [first_receipt, second_receipt, third_receipt] {
                assert_eq!(
                    complete_bounded(receipt.wait_durable()).await,
                    DurableOutcome::WriterStopped
                );
            }
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn record_time_explicit_and_protected_barriers_are_bounded_and_fifo() {
        harness().block_on(async {
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("byte limits");

            let records_directory = test_directory();
            let records_runtime = complete_bounded(open_durable_test(durable_options(
                records_directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(std::time::Duration::from_secs(5), 2, 64 * 1_024 * 1_024),
            )))
            .await
            .expect("record-trigger runtime");
            let first = records_runtime
                .ingress()
                .try_submit(command("records-0", 1, 0))
                .expect("first record")
                .into_receipt();
            let second = records_runtime
                .ingress()
                .try_submit(command("records-1", 2, 1))
                .expect("second record")
                .into_receipt();
            let DurableOutcome::Durable(first_commit) =
                complete_bounded(first.wait_durable()).await
            else {
                panic!("record threshold should make first durable");
            };
            let DurableOutcome::Durable(second_commit) =
                complete_bounded(second.wait_durable()).await
            else {
                panic!("record threshold should make second durable");
            };
            assert_eq!(first_commit.append().append_sequence(), 1);
            assert_eq!(second_commit.append().append_sequence(), 2);
            assert_eq!(records_runtime.inspection().store().sync_count(), 1);
            complete_bounded(records_runtime.shutdown())
                .await
                .expect("record runtime shutdown");
            fs::remove_dir_all(records_directory).expect("remove record directory");

            let bytes_directory = test_directory();
            let first_bytes = command("bytes-0", 6, 5);
            let second_bytes = command("bytes-1", 7, 6);
            let frame_bytes = och_store::admission_frame_len_v1(first_bytes.admission())
                .expect("count byte-trigger frame");
            assert_eq!(
                och_store::admission_frame_len_v1(second_bytes.admission()),
                Ok(frame_bytes)
            );
            let bytes_runtime = complete_bounded(open_durable_test(durable_options(
                bytes_directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_secs(5),
                    MAX_OUTSTANDING_COMMANDS,
                    frame_bytes * 2,
                ),
            )))
            .await
            .expect("byte-trigger runtime");
            let first = bytes_runtime
                .ingress()
                .try_submit(first_bytes)
                .expect("first byte-trigger frame")
                .into_receipt();
            let second = bytes_runtime
                .ingress()
                .try_submit(second_bytes)
                .expect("second byte-trigger frame")
                .into_receipt();
            assert!(matches!(
                complete_bounded(first.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            assert!(matches!(
                complete_bounded(second.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            assert_eq!(bytes_runtime.inspection().store().sync_count(), 1);
            complete_bounded(bytes_runtime.shutdown())
                .await
                .expect("byte runtime shutdown");
            fs::remove_dir_all(bytes_directory).expect("remove byte directory");

            let time_directory = test_directory();
            let time_runtime = complete_bounded(open_durable_test(durable_options(
                time_directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_millis(2),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("time-trigger runtime");
            let timed = time_runtime
                .ingress()
                .try_submit(command("time-barrier", 3, 2))
                .expect("timed admission")
                .into_receipt();
            assert!(matches!(
                complete_bounded(timed.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            assert_eq!(time_runtime.inspection().store().sync_count(), 1);

            let immediate = time_runtime
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("explicit-barrier", 4, 3).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("explicit barrier")
                .into_receipt();
            assert!(matches!(
                complete_bounded(immediate.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            let protected = time_runtime
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("protected-barrier", 5, 4).into_admission(),
                    AdmissionPriority::Protected,
                    BarrierDemand::Group,
                ))
                .expect("protected barrier")
                .into_receipt();
            assert!(matches!(
                complete_bounded(protected.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            assert_eq!(time_runtime.inspection().store().sync_count(), 3);
            complete_bounded(time_runtime.shutdown())
                .await
                .expect("time runtime shutdown");
            fs::remove_dir_all(time_directory).expect("remove time directory");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn lifecycle_and_bind_control_waiters_are_bounded_ordered_and_recover() {
        harness().block_on(async {
            let runtime = complete_bounded(HistorianRuntime::start(store_id(1)))
                .await
                .expect("bounded-control runtime");
            let before = runtime.inspection().committed();
            let envelope = command("bounded-control", 240, 240)
                .admission()
                .envelope()
                .clone();
            let revision = RegistryOperation::Revise {
                series_id: series_id(1),
                expected_revision: DeclarationRevision::FIRST,
                payload: SeriesDeclarationPayload::new(
                    producer_id(2),
                    CollectionMode::Sampled,
                    ValueFamily::Unsigned,
                    QuantityEvidence::Unresolved(
                        DeclarationReference::new("quantity:bounded-control".to_owned())
                            .expect("bounded quantity reference"),
                    ),
                    UnitEvidence::Absent,
                    None,
                ),
                evidence: DeclarationEvidence::new(timestamp(1), None),
            };
            let revision_replay = revision.clone();

            let gate = runtime.control_gate.lock().await;
            let mut admitted: Vec<RegistryControlFuture<'_>> =
                Vec::with_capacity(MAX_OUTSTANDING_COMMANDS);
            let runtime_ref = &runtime;
            admitted.push(Box::pin(async move {
                let commit = runtime_ref.apply_registry(revision).await?;
                Ok(match commit.outcome() {
                    RegistryOutcome::Declaration(declaration) => declaration.revision().get(),
                    RegistryOutcome::Retirement(_) => {
                        panic!("revision control must return a declaration")
                    }
                })
            }));
            for _ in 1..MAX_OUTSTANDING_COMMANDS {
                let runtime_ref = &runtime;
                let envelope = envelope.clone();
                admitted.push(Box::pin(async move {
                    runtime_ref
                        .bind_envelope(envelope)
                        .await
                        .map(|binding| binding.declaration().revision().get())
                }));
            }

            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            for future in &mut admitted {
                assert!(
                    future.as_mut().poll(&mut context).is_pending(),
                    "every admitted control waits behind the held ordering gate"
                );
            }
            assert_eq!(runtime.control_admission.available_permits(), 0);

            let mut excess_bind = Box::pin(runtime.bind_envelope(envelope.clone()));
            assert!(matches!(
                poll_once(excess_bind.as_mut()).await,
                Poll::Ready(Err(RegistryError::Capacity))
            ));
            let mut excess_lifecycle = Box::pin(runtime.apply_registry(revision_replay.clone()));
            assert!(matches!(
                poll_once(excess_lifecycle.as_mut()).await,
                Poll::Ready(Err(RegistryError::Capacity))
            ));
            drop(excess_bind);
            drop(excess_lifecycle);
            assert_eq!(runtime.inspection().committed(), before);
            assert_eq!(runtime.control_admission.available_permits(), 0);

            drop(
                admitted
                    .pop()
                    .expect("one admitted bind remains cancellation-owned"),
            );
            assert_eq!(runtime.control_admission.available_permits(), 1);
            let runtime_ref = &runtime;
            let replacement_envelope = envelope.clone();
            let mut replacement: RegistryControlFuture<'_> = Box::pin(async move {
                runtime_ref
                    .bind_envelope(replacement_envelope)
                    .await
                    .map(|binding| binding.declaration().revision().get())
            });
            assert!(replacement.as_mut().poll(&mut context).is_pending());
            admitted.push(replacement);
            assert_eq!(runtime.control_admission.available_permits(), 0);

            drop(gate);
            let mut results: Vec<Option<Result<u128, RegistryError>>> =
                (0..MAX_OUTSTANDING_COMMANDS).map(|_| None).collect();
            poll_fn(|context| {
                let mut pending = false;
                for (future, result) in admitted.iter_mut().zip(&mut results) {
                    if result.is_none() {
                        match future.as_mut().poll(context) {
                            Poll::Ready(output) => *result = Some(output),
                            Poll::Pending => pending = true,
                        }
                    }
                }
                if pending {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            for result in results {
                assert_eq!(result, Some(Ok(2)));
            }
            drop(admitted);
            assert_eq!(
                runtime.inspection().committed().manifest_generation(),
                before.manifest_generation() + 1
            );
            assert_eq!(
                runtime.control_admission.available_permits(),
                MAX_OUTSTANDING_COMMANDS
            );

            let recovered_binding = runtime
                .bind_envelope(envelope)
                .await
                .expect("released bind capacity must recover");
            assert_eq!(recovered_binding.declaration().revision().get(), 2);
            let replay = runtime
                .apply_registry(revision_replay)
                .await
                .expect("released lifecycle capacity must recover");
            assert!(matches!(replay.outcome(), RegistryOutcome::Declaration(_)));
            assert_eq!(
                runtime.control_admission.available_permits(),
                MAX_OUTSTANDING_COMMANDS
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("bounded-control shutdown");
        });
    }

    #[test]
    fn group_timeout_never_checkpoints_an_unpublished_append() {
        harness().block_on(async {
            let directory = test_directory();
            let probe = Arc::new(LifecycleProbe::default());
            let (release_publication, publication_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.publication_gate = Some(publication_gate);
            writer_options.publication_gate_after = 1;
            let group_delay = std::time::Duration::from_millis(500);
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                .expect("unpublished byte limits");
            let runtime = complete_bounded(HistorianRuntime::open_inner(
                durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::CreateNew,
                    bytes,
                    group_policy(group_delay, MAX_OUTSTANDING_COMMANDS, 64 * 1_024 * 1_024),
                ),
                writer_options,
            ))
            .await
            .expect("unpublished-cutoff runtime");
            runtime
                .apply_registry(default_test_registry_operation(runtime.store_id()))
                .await
                .expect("unpublished-cutoff registry seed");
            let ingress = runtime.ingress();
            let first = ingress
                .try_submit(command("published-first", 90, 90))
                .expect("queue first group member")
                .into_receipt();
            assert!(matches!(
                complete_bounded(first.clone().wait_handled()).await,
                HandledOutcome::WriterHandled(_)
            ));
            let second = ingress
                .try_submit(command("unpublished-second", 91, 91))
                .expect("queue gated second group member")
                .into_receipt();
            wait_until(|| runtime.inspection().store().last_append_sequence() == 2).await;
            assert_eq!(
                runtime
                    .inspection()
                    .store()
                    .durable_cutoff()
                    .append_sequence(),
                0
            );
            let started = std::time::Instant::now();
            while started.elapsed() <= group_delay {
                yield_now().await;
            }
            assert_eq!(runtime.inspection().store().sync_count(), 0);
            assert_eq!(
                runtime
                    .inspection()
                    .store()
                    .durable_cutoff()
                    .append_sequence(),
                0,
                "timeout cannot cover a frame awaiting publication acknowledgement"
            );
            let mut first_durable = Box::pin(first.wait_durable());
            assert!(poll_once(first_durable.as_mut()).await.is_pending());

            release_publication
                .send(())
                .expect("release second publication acknowledgement");
            let DurableOutcome::Durable(first_commit) = complete_bounded(first_durable).await
            else {
                panic!("first group member becomes durable after publication");
            };
            let DurableOutcome::Durable(second_commit) =
                complete_bounded(second.wait_durable()).await
            else {
                panic!("second group member becomes durable after publication");
            };
            assert_eq!(first_commit.durable_cutoff().append_sequence(), 2);
            assert_eq!(second_commit.durable_cutoff().append_sequence(), 2);
            assert_eq!(second_commit.durable_cutoff().checkpoint_generation(), 2);
            assert_eq!(runtime.inspection().store().sync_count(), 1);
            complete_bounded(runtime.shutdown())
                .await
                .expect("unpublished-cutoff shutdown");
            fs::remove_dir_all(directory).expect("remove unpublished-cutoff directory");
        });
    }

    #[test]
    fn concurrent_retirement_and_append_share_one_order_and_preserve_historical_authority() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                .expect("concurrency byte limits");
            let runtime = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_secs(60),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("concurrency runtime");
            let receipt = runtime
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("historic-concurrency", 230, 230).into_admission(),
                    AdmissionPriority::Normal,
                    BarrierDemand::Immediate,
                ))
                .expect("queue revision-one admission")
                .into_receipt();
            let retirement = RegistryOperation::Retire {
                series_id: series_id(1),
                expected_revision: DeclarationRevision::FIRST,
                evidence: DeclarationEvidence::new(timestamp(5), None),
            };
            let mut registry_future = Box::pin(runtime.apply_registry(retirement));
            let mut durable_future = Box::pin(receipt.wait_durable());
            let mut registry_result = None;
            let mut durable_result = None;
            poll_fn(|context| {
                if registry_result.is_none()
                    && let Poll::Ready(result) = registry_future.as_mut().poll(context)
                {
                    registry_result = Some(result);
                }
                if durable_result.is_none()
                    && let Poll::Ready(result) = durable_future.as_mut().poll(context)
                {
                    durable_result = Some(result);
                }
                if registry_result.is_some() && durable_result.is_some() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            drop(registry_future);
            drop(durable_future);
            let registry_commit = registry_result
                .expect("registry result")
                .expect("retirement commits");
            assert!(matches!(
                registry_commit.outcome(),
                RegistryOutcome::Retirement(_)
            ));
            let durable = match durable_result.expect("durable result") {
                DurableOutcome::Durable(commit) => commit,
                DurableOutcome::WriterStopped => panic!("writer must retain historical authority"),
            };
            assert_eq!(durable.append().append_sequence(), 1);
            assert_eq!(durable.durable_cutoff().append_sequence(), 1);
            assert!(
                runtime.inspection().committed().manifest_generation()
                    >= registry_commit.manifest_commit().manifest_generation()
            );
            assert!(
                runtime.inspection().committed().manifest_generation()
                    >= durable.manifest_commit().manifest_generation()
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("shutdown concurrent runtime");

            let reopened = complete_bounded(HistorianRuntime::open(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::OpenExisting,
                bytes,
                group_policy(
                    std::time::Duration::from_millis(2),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("reopen concurrent store");
            assert_eq!(reopened.recovered_records().len(), 1);
            complete_bounded(reopened.shutdown())
                .await
                .expect("shutdown reopened runtime");
            fs::remove_dir_all(directory).expect("remove concurrency directory");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn unknown_historical_declaration_at_rotation_boundary_refuses_without_mutation() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                .expect("historical-refusal byte limits");
            let first_command = IngressCommand::with_policy(
                command("rotation-boundary-prior", 239, 239).into_admission(),
                AdmissionPriority::Normal,
                BarrierDemand::Immediate,
            );
            let first_frame_len = och_store::admission_frame_len_v1(first_command.admission())
                .expect("prior admission frame length");
            let active_bytes = u64::try_from(och_store::JOURNAL_V1_HEADER_LEN)
                .expect("journal header length should fit")
                .checked_add(u64::try_from(first_frame_len).expect("frame length should fit"))
                .and_then(|length| length.checked_add(1))
                .expect("test active byte limit should fit");
            let options_for = |mode| {
                StoreOptions::new(
                    directory.clone(),
                    store_id(1),
                    mode,
                    och_store::ActiveJournalLimits::new(
                        och_store::MAX_ADMISSION_PAYLOAD_V1,
                        active_bytes,
                        8,
                    )
                    .expect("historical-refusal journal limits"),
                    bytes,
                    group_policy(
                        std::time::Duration::from_secs(60),
                        MAX_OUTSTANDING_COMMANDS,
                        64 * 1_024 * 1_024,
                    ),
                    och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(16, 64))
                        .expect("historical-refusal registry options"),
                    och_store::RetryPersistenceOptions::new(2, 2)
                        .expect("historical-refusal retry options"),
                )
                .expect("historical-refusal store options")
            };
            let runtime = complete_bounded(open_durable_test(options_for(
                och_store::ActiveJournalOpenMode::CreateNew,
            )))
            .await
            .expect("historical-refusal runtime");
            let first = runtime
                .ingress()
                .try_submit(first_command)
                .expect("prior admission reaches the sole writer")
                .into_receipt();
            assert!(matches!(
                complete_bounded(first.wait_durable()).await,
                DurableOutcome::Durable(_)
            ));
            let before = runtime.inspection();
            assert_eq!(before.store().active_records(), 1);
            assert_eq!(before.generations().active_generation(), 1);
            assert_eq!(before.generations().sealed_count(), 0);
            assert_eq!(before.store().active_bytes() + 1, active_bytes);
            let artifacts_before = directory_bytes(&directory);
            let (envelope, retry) = model_parts(
                series_id(240),
                producer_id(241),
                "unknown-history",
                240,
                240,
            );
            let unknown = canonical_admission(store_id(1), envelope, retry);
            let unknown_frame_len = och_store::admission_frame_len_v1(&unknown)
                .expect("unknown admission frame length");
            assert!(
                before.store().active_bytes()
                    + u64::try_from(unknown_frame_len).expect("unknown frame length should fit")
                    > active_bytes,
                "the refused command must reach the automatic fit-rotation boundary"
            );
            let receipt = runtime
                .ingress()
                .try_submit(IngressCommand::new(unknown))
                .expect("bounded admission reaches sole registry authority")
                .into_receipt();
            assert_eq!(
                complete_bounded(receipt.clone().wait_handled()).await,
                HandledOutcome::WriterStopped
            );
            assert_eq!(
                complete_bounded(receipt.wait_durable()).await,
                DurableOutcome::WriterStopped
            );
            let after = runtime.inspection();
            assert_eq!(after.store(), before.store());
            assert_eq!(after.committed(), before.committed());
            assert_eq!(after.generations(), before.generations());
            assert_eq!(after.health(), RuntimeHealth::Faulted);
            assert_eq!(after.pending_count(), 0);
            assert_eq!(after.pending_bytes(), 0);
            assert_eq!(directory_bytes(&directory), artifacts_before);
            assert_eq!(
                complete_bounded(runtime.shutdown()).await,
                Err(ShutdownError::WriterExitedBeforeShutdown)
            );

            let reopened = complete_bounded(HistorianRuntime::open(options_for(
                och_store::ActiveJournalOpenMode::OpenExisting,
            )))
            .await
            .expect("terminal refusal leaves committed store reopenable");
            assert_eq!(reopened.recovered_records().len(), 1);
            assert_eq!(reopened.inspection().committed(), before.committed());
            assert_eq!(reopened.inspection().generations(), before.generations());
            assert_eq!(reopened.inspection().generations().sealed_count(), 0);
            complete_bounded(reopened.shutdown())
                .await
                .expect("shutdown reopened store");
            fs::remove_dir_all(directory).expect("remove historical-refusal directory");
        });
    }

    #[test]
    fn option_relationships_and_active_age_rotation_commit_successor() {
        assert_eq!(
            GroupCommitPolicy::new(
                std::time::Duration::ZERO,
                1,
                1,
                std::time::Duration::from_secs(1),
            ),
            Err(StoreOptionsError::InvalidRelationships)
        );
        assert_eq!(
            GroupCommitPolicy::new(
                std::time::Duration::from_secs(1),
                MAX_OUTSTANDING_COMMANDS + 1,
                1,
                std::time::Duration::from_secs(1),
            ),
            Err(StoreOptionsError::InvalidRelationships)
        );
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                .expect("age rotation byte limits");
            let options = durable_options(
                directory,
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                GroupCommitPolicy::new(
                    std::time::Duration::from_secs(1),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                    std::time::Duration::from_nanos(1),
                )
                .expect("finite age rotation policy"),
            )
            .with_test_cleanup();
            let runtime = complete_bounded(open_durable_test(options))
                .await
                .expect("age rotation runtime opens at genesis");
            assert_eq!(runtime.inspection().health(), RuntimeHealth::Healthy);
            let ingress = runtime.ingress();
            let receipt = ingress
                .try_submit(command("age-rotation", 88, 80))
                .expect("rotation demand is decided by the sole worker")
                .into_receipt();
            let outcome = complete_bounded(receipt.wait_durable()).await;
            let DurableOutcome::Durable(commit) = outcome else {
                panic!("age-triggered append must retain ordinary durability: {outcome:?}");
            };
            assert_eq!(commit.append().journal().generation(), 1);
            complete_bounded(async {
                loop {
                    if runtime.inspection().generations().active_generation() == 2 {
                        break;
                    }
                    yield_now().await;
                }
            })
            .await;
            let inspection = runtime.inspection();
            assert_eq!(inspection.health(), RuntimeHealth::Healthy);
            assert_eq!(inspection.generations().active_generation(), 2);
            assert_eq!(inspection.generations().sealed_count(), 1);
            complete_bounded(runtime.shutdown())
                .await
                .expect("rotated runtime shuts down cleanly");
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn repeated_count_rotation_preserves_global_sequences_and_cross_generation_replay() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes =
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("rotation byte limits");
            let options_for = |mode| {
                StoreOptions::new(
                    directory.clone(),
                    store_id(1),
                    mode,
                    och_store::ActiveJournalLimits::new(
                        och_store::MAX_ADMISSION_PAYLOAD_V1,
                        64 * 1_024 * 1_024,
                        1,
                    )
                    .expect("one-record rotation limit"),
                    bytes,
                    group_policy(
                        std::time::Duration::from_secs(5),
                        MAX_OUTSTANDING_COMMANDS,
                        64 * 1_024 * 1_024,
                    ),
                    och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(16, 64))
                        .expect("rotation registry options"),
                    och_store::RetryPersistenceOptions::new(4, 4).expect("rotation retry options"),
                )
                .expect("rotation store options")
            };
            let runtime = complete_bounded(open_durable_test(options_for(
                och_store::ActiveJournalOpenMode::CreateNew,
            )))
            .await
            .expect("open rotating runtime");
            let ingress = runtime.ingress();
            let submit = |key: &'static str, digest: u8| {
                let ingress = ingress.clone();
                async move {
                    let submission = ingress
                        .try_submit(IngressCommand::with_policy(
                            command(key, digest, u128::from(digest)).into_admission(),
                            AdmissionPriority::Normal,
                            BarrierDemand::Immediate,
                        ))
                        .expect("fresh rotating command");
                    let DurableOutcome::Durable(commit) =
                        complete_bounded(submission.into_receipt().wait_durable()).await
                    else {
                        panic!("rotating command must become durable");
                    };
                    commit
                }
            };
            let first = submit("rotate-first", 31).await;
            complete_bounded(async {
                while runtime.inspection().generations().active_generation() != 2 {
                    assert_eq!(
                        runtime.inspection().health(),
                        RuntimeHealth::Healthy,
                        "first rotation must not fault"
                    );
                    yield_now().await;
                }
            })
            .await;
            let second = submit("rotate-second", 32).await;
            complete_bounded(async {
                while runtime.inspection().generations().active_generation() != 3 {
                    assert_eq!(
                        runtime.inspection().health(),
                        RuntimeHealth::Healthy,
                        "second rotation must not fault"
                    );
                    yield_now().await;
                }
            })
            .await;
            assert_eq!(first.append().journal().generation(), 1);
            assert_eq!(first.append().append_sequence(), 1);
            assert_eq!(second.append().journal().generation(), 2);
            assert_eq!(second.append().append_sequence(), 2);
            assert!(second.append().end_offset() > och_store::JOURNAL_V1_HEADER_LEN as u64);
            assert!(second.manifest_commit().generation_catalog().is_some());
            assert_eq!(runtime.inspection().generations().sealed_count(), 2);
            complete_bounded(runtime.shutdown())
                .await
                .expect("rotating runtime shutdown");

            let reopened = complete_bounded(open_durable_test(options_for(
                och_store::ActiveJournalOpenMode::OpenExisting,
            )))
            .await
            .expect("reopen rotating runtime");
            assert!(reopened.recovered_records().is_empty());
            assert!(
                reopened
                    .read_handle()
                    .snapshot()
                    .expect("latest remains available")
                    .is_empty()
            );
            for (key, digest, expected) in [
                ("rotate-first", 31_u8, first),
                ("rotate-second", 32_u8, second),
            ] {
                let replay = reopened
                    .ingress()
                    .try_submit(command(key, digest, 99))
                    .expect("cross-generation retry must replay");
                assert_eq!(replay.disposition(), SubmissionDisposition::Replayed);
                let DurableOutcome::Durable(actual) =
                    complete_bounded(replay.into_receipt().wait_durable()).await
                else {
                    panic!("cross-generation replay remains durable");
                };
                assert_eq!(actual, expected);
            }
            complete_bounded(reopened.shutdown())
                .await
                .expect("reopened rotating runtime shutdown");
            fs::remove_dir_all(directory).expect("remove rotating directory");
        });
    }

    #[test]
    fn store_options_validate_borrowed_path_before_cloning() {
        let journal_limits = och_store::ActiveJournalLimits::new(
            och_store::MAX_ADMISSION_PAYLOAD_V1,
            64 * 1_024 * 1_024,
            4_096,
        )
        .expect("path-bound journal limits");
        let byte_limits = ByteReservationLimits::new(1_024, 0, 0).expect("path-bound byte limits");
        let group = GroupCommitPolicy::new(
            std::time::Duration::from_secs(1),
            1,
            1_024,
            std::time::Duration::from_secs(60),
        )
        .expect("path-bound group policy");
        assert!(
            StoreOptions::new(
                PathBuf::from("x".repeat(och_store::MAX_STORE_DIRECTORY_BYTES)),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                journal_limits,
                byte_limits,
                group,
                och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(16, 64))
                    .expect("path-bound registry options"),
                och_store::RetryPersistenceOptions::default(),
            )
            .is_ok(),
            "exact path bound is retained"
        );
        assert!(matches!(
            StoreOptions::new(
                PathBuf::from("x".repeat(och_store::MAX_STORE_DIRECTORY_BYTES + 1)),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                journal_limits,
                byte_limits,
                group,
                och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(16, 64))
                    .expect("path-bound registry options"),
                och_store::RetryPersistenceOptions::default(),
            ),
            Err(StoreOptionsError::Store(
                och_store::ManifestStoreError::InvalidOptions
            ))
        ));
    }

    #[test]
    fn nonblocking_drop_has_a_concrete_reaper_that_releases_the_lock() {
        harness().block_on(async {
            for _ in 0..16 {
                let directory = test_directory();
                let journal_limits = och_store::ActiveJournalLimits::new(
                    och_store::MAX_ADMISSION_PAYLOAD_V1,
                    64 * 1_024 * 1_024,
                    4_096,
                )
                .expect("journal limits");
                let bytes =
                    ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("byte limits");
                let runtime = complete_bounded(open_durable_test(durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::CreateNew,
                    bytes,
                    group_policy(
                        std::time::Duration::from_millis(2),
                        MAX_OUTSTANDING_COMMANDS,
                        64 * 1_024 * 1_024,
                    ),
                )))
                .await
                .expect("drop runtime");
                drop(runtime);

                let mut reopened = None;
                let started = std::time::Instant::now();
                while started.elapsed() < TEST_WAIT_TIMEOUT {
                    let config = och_store::ManifestStoreConfig::new(
                        directory.clone(),
                        store_id(1),
                        och_store::ActiveJournalOpenMode::OpenExisting,
                        journal_limits,
                        och_store::RegistryPersistenceOptions::new(SeriesRegistryLimits::new(
                            256, 512,
                        ))
                        .expect("reopen registry options"),
                        och_store::RetryPersistenceOptions::new(2, 2)
                            .expect("reopen retry options"),
                    )
                    .expect("reopen config");
                    match och_store::ManifestStore::open(config) {
                        Ok(store) => {
                            reopened = Some(store);
                            break;
                        }
                        Err(och_store::ManifestStoreError::AlreadyOpen) => {
                            std::thread::sleep(std::time::Duration::from_micros(50));
                        }
                        Err(error) => panic!("unexpected reopen error: {error:?}"),
                    }
                }
                assert!(reopened.is_some(), "reaper must eventually release lock");
                drop(reopened);
                fs::remove_dir_all(directory).expect("remove drop directory");
            }
        });
    }

    #[test]
    fn inspection_and_failures_do_not_expose_store_paths_or_canonical_content() {
        harness().block_on(async {
            let directory = test_directory();
            let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("byte limits");
            let runtime = complete_bounded(open_durable_test(durable_options(
                directory.clone(),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_millis(2),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("inspection runtime");
            let debug = format!("{runtime:?} {:?}", runtime.inspection());
            assert!(!debug.contains(directory.to_string_lossy().as_ref()));
            assert!(!debug.contains("historian-request"));
            assert_eq!(runtime.inspection().health(), RuntimeHealth::Healthy);
            complete_bounded(runtime.shutdown())
                .await
                .expect("inspection shutdown");
            fs::remove_dir_all(directory).expect("remove inspection directory");
        });
    }

    #[test]
    fn child_process_kill_helper() {
        let Ok(directory) = std::env::var("OCH_RUNTIME_KILL_DIRECTORY") else {
            return;
        };
        let stage = std::env::var("OCH_RUNTIME_KILL_STAGE").expect("child kill stage");
        harness().block_on(async {
            let bytes =
                ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0).expect("child byte limits");
            let runtime = complete_bounded(open_durable_test(durable_options(
                PathBuf::from(&directory),
                store_id(1),
                och_store::ActiveJournalOpenMode::CreateNew,
                bytes,
                group_policy(
                    std::time::Duration::from_secs(60),
                    MAX_OUTSTANDING_COMMANDS,
                    64 * 1_024 * 1_024,
                ),
            )))
            .await
            .expect("child runtime open");
            let barrier = if stage == "durable" {
                BarrierDemand::Immediate
            } else {
                BarrierDemand::Group
            };
            let receipt = runtime
                .ingress()
                .try_submit(IngressCommand::with_policy(
                    command("child-kill", 77, 70).into_admission(),
                    AdmissionPriority::Normal,
                    barrier,
                ))
                .expect("child admission")
                .into_receipt();
            if stage == "durable" {
                assert!(matches!(
                    complete_bounded(receipt.wait_durable()).await,
                    DurableOutcome::Durable(_)
                ));
            } else {
                assert!(matches!(
                    complete_bounded(receipt.wait_handled()).await,
                    HandledOutcome::WriterHandled(_)
                ));
            }
            fs::write(
                PathBuf::from(directory).with_extension("child-ready"),
                stage.as_bytes(),
            )
            .expect("publish child readiness marker outside the bounded store inventory");
            loop {
                std::thread::park();
            }
        });
    }

    #[test]
    fn real_process_kill_reopens_durable_and_handled_suffixes_truthfully() {
        for stage in ["durable", "handled"] {
            let directory = test_directory();
            let marker = directory.with_extension("child-ready");
            let mut child = Command::new(std::env::current_exe().expect("runtime test executable"))
                .args(["--exact", "tests::child_process_kill_helper", "--nocapture"])
                .env("OCH_RUNTIME_KILL_DIRECTORY", &directory)
                .env("OCH_RUNTIME_KILL_STAGE", stage)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn child runtime");
            let mut ready = false;
            for _ in 0..5_000 {
                if marker.is_file() {
                    ready = true;
                    break;
                }
                assert!(
                    child.try_wait().expect("inspect child").is_none(),
                    "child exited before readiness"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(ready, "child must report the requested receipt stage");
            child.kill().expect("kill child after receipt stage");
            let status = child.wait().expect("reap killed child");
            assert!(!status.success());

            harness().block_on(async {
                let bytes = ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
                    .expect("reopen byte limits");
                let reopened = complete_bounded(open_durable_test(durable_options(
                    directory.clone(),
                    store_id(1),
                    och_store::ActiveJournalOpenMode::OpenExisting,
                    bytes,
                    group_policy(
                        std::time::Duration::from_millis(2),
                        MAX_OUTSTANDING_COMMANDS,
                        64 * 1_024 * 1_024,
                    ),
                )))
                .await;
                if stage == "handled" {
                    assert!(matches!(
                        reopened,
                        Err(StartError::Store(och_store::ManifestStoreError::Active(
                            och_store::ActiveJournalError::InvalidLayout
                        )))
                    ));
                    return;
                }
                let reopened = reopened.expect("durable manifest cutoff must reopen");
                assert_eq!(reopened.recovered_records().len(), 1);
                assert_eq!(
                    reopened
                        .inspection()
                        .store()
                        .durable_cutoff()
                        .append_sequence(),
                    1
                );
                assert!(
                    reopened
                        .read_handle()
                        .snapshot()
                        .expect("empty latest")
                        .is_empty()
                );
                complete_bounded(reopened.shutdown())
                    .await
                    .expect("shutdown reopened child journal");
            });
            fs::remove_file(marker).expect("remove child readiness marker");
            fs::remove_dir_all(directory).expect("remove child-kill directory");
        }
    }
}
