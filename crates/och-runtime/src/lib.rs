#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Caller-executor lifecycle and bounded volatile ingress for the `OpenControl`
//! Historian writer.
//!
//! Each [`HistorianRuntime`] owns exactly one private writer task and a fixed
//! 16-command admission window on the caller's active Tokio executor. Handling
//! means only that this volatile writer consumed a command; this crate exposes no
//! persistence, state observation, publication, query, or restart mechanism.

mod ingress;

pub use ingress::{
    HistorianIngress, IngressCommand, MAX_OUTSTANDING_COMMANDS, Receipt, ReceiptOutcome,
    ScopeMismatchError, Submission, SubmissionDisposition, TrySubmitError, TrySubmitErrorKind,
};

use ingress::{IngressShared, NextWork};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

/// A running private Historian writer task.
///
/// Each handle owns one writer task and one isolated ingress state. Independent
/// instances are valid concurrently; there is no global runtime registry. A
/// stopped or failed instance cannot be restarted—call [`HistorianRuntime::start`]
/// to construct a new one.
///
/// Dropping this handle requests task cancellation without blocking. Call
/// [`HistorianRuntime::shutdown`] and await its result when joined normal
/// termination is required.
pub struct HistorianRuntime {
    ingress: HistorianIngress,
    writer: Option<JoinHandle<WriterExit>>,
}

impl fmt::Debug for HistorianRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistorianRuntime")
            .finish_non_exhaustive()
    }
}

impl HistorianRuntime {
    /// Starts one private writer task on the caller's active Tokio executor.
    ///
    /// The future returns only after the writer's private mutable state has been
    /// initialized. The caller remains responsible for the executor; this method
    /// never constructs an executor, starts a thread, or blocks.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`StartError`] when there is no active Tokio runtime
    /// or the writer fails before readiness. Dropping this future after it has
    /// spawned the writer aborts that writer rather than detaching it.
    pub async fn start() -> Result<Self, StartError> {
        Self::start_inner(WriterOptions::production()).await
    }

    async fn start_inner(options: WriterOptions) -> Result<Self, StartError> {
        let executor = Handle::try_current().map_err(|_| StartError::NoActiveRuntime)?;
        let (readiness_tx, readiness_rx) = oneshot::channel();
        let ingress = HistorianIngress::new();
        #[cfg(test)]
        let cancel_before_readiness = options.behavior == TestBehavior::CancelBeforeReadiness;
        let writer = executor.spawn(run_writer(options, readiness_tx, ingress.shared()));
        let mut startup = StartupGuard::new(writer);

        #[cfg(test)]
        if cancel_before_readiness {
            startup.abort();
        }

        if readiness_rx.await.is_ok() {
            Ok(Self {
                ingress,
                writer: Some(startup.transfer()),
            })
        } else {
            let result = startup.join().await;
            startup.disarm();
            match result {
                Ok(_) => Err(StartError::WriterExitedBeforeReadiness),
                Err(error) => Err(classify_start_join_error(&error)),
            }
        }
    }

    /// Returns a cloneable handle to this instance's bounded volatile ingress.
    ///
    /// The handle contains no executor or public Tokio primitive. It may outlive
    /// this runtime, but after shutdown or Drop it rejects commands as closed.
    #[must_use]
    pub fn ingress(&self) -> HistorianIngress {
        self.ingress.clone()
    }

    /// Gracefully stops and joins this instance's private writer task.
    ///
    /// Admission closes synchronously when this future is first polled. Commands
    /// accepted before that close are drained FIFO, their receipts resolve as
    /// [`ReceiptOutcome::WriterHandled`], and then the retained task is joined.
    /// `Ok(())` proves volatile handling and join only; it is not persistence,
    /// durability, publication, queryability, or restart evidence.
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
            Ok(WriterExit::Shutdown) => Ok(()),
            Ok(_) => Err(ShutdownError::WriterExitedBeforeShutdown),
            Err(error) => Err(classify_shutdown_join_error(&error)),
        }
    }

    #[cfg(test)]
    async fn start_with_options(options: WriterOptions) -> Result<Self, StartError> {
        Self::start_inner(options).await
    }
}

impl Drop for HistorianRuntime {
    fn drop(&mut self) {
        // Resolve receipts before aborting so cancellation cannot strand work if
        // the caller's executor never polls the writer again.
        self.ingress.stop();
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
}

impl fmt::Display for StartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NoActiveRuntime => "no active Tokio runtime",
            Self::WriterExitedBeforeReadiness => "writer exited before readiness",
            Self::WriterTaskCancelled => "writer task was cancelled before readiness",
            Self::WriterTaskPanicked => "writer task panicked before readiness",
        })
    }
}

impl Error for StartError {}

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
            behavior: TestBehavior::Normal,
            #[cfg(test)]
            probe: None,
        }
    }
}

async fn run_writer(
    options: WriterOptions,
    readiness: oneshot::Sender<()>,
    ingress: Arc<IngressShared>,
) -> WriterExit {
    #[cfg(test)]
    let mut options = options;
    #[cfg(test)]
    let _task_guard = TaskGuard::new(options.probe.clone());
    let mut failure_guard = WriterFailureGuard::new(Arc::clone(&ingress));

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
        | TestBehavior::PanicWhileHandling => {}
    }

    let _state = WriterState::initialize(&options);
    if readiness.send(()).is_err() {
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
        | TestBehavior::PanicWhileHandling => {}
    }

    writer_loop(options, ingress, &mut failure_guard).await
}

async fn writer_loop(
    options: WriterOptions,
    ingress: Arc<IngressShared>,
    failure_guard: &mut WriterFailureGuard,
) -> WriterExit {
    #[cfg(test)]
    let mut options = options;
    #[cfg(not(test))]
    let _ = options;

    loop {
        // Register interest before inspecting the one-consumer queue. Notify's
        // retained permit closes the submit-between-check-and-await race.
        let notified = ingress.notified();
        match ingress.take_next() {
            NextWork::Work(work) => {
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
                    | TestBehavior::PanicBeforeShutdown => {}
                }
                #[cfg(test)]
                if let Some(probe) = &options.probe {
                    probe
                        .handled_order
                        .lock()
                        .expect("test handled-order probe should not be poisoned")
                        .push(work.test_tag());
                }
                (*work).finish_handled();
            }
            NextWork::Empty => notified.await,
            NextWork::Drained => {
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
                failure_guard.disarm();
                return WriterExit::Shutdown;
            }
            NextWork::Failed => return WriterExit::IngressFailed,
        }
    }
}

struct WriterFailureGuard {
    ingress: Arc<IngressShared>,
    armed: bool,
}

impl WriterFailureGuard {
    fn new(ingress: Arc<IngressShared>) -> Self {
        Self {
            ingress,
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
            self.ingress.stop();
        }
    }
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
use std::sync::atomic::{AtomicUsize, Ordering};

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
}

#[cfg(test)]
mod tests {
    use super::{
        HistorianIngress, HistorianRuntime, IngressCommand, LifecycleProbe,
        MAX_OUTSTANDING_COMMANDS, ReceiptOutcome, ShutdownError, StartError, SubmissionDisposition,
        TestBehavior, TrySubmitErrorKind, WriterOptions,
    };
    use och_core::{
        CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion, Gap,
        GapReason, ProducerEpoch, ProducerId, ProducerSequence, RetryKey, RetryQualification,
        SeriesId, SeriesMetadata,
    };
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, Waker};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    const MAX_YIELDS: usize = 64;

    fn harness() -> Runtime {
        Builder::new_current_thread()
            .build()
            .expect("current-thread Tokio test harness should build")
    }

    fn options(probe: &Arc<LifecycleProbe>, behavior: TestBehavior) -> WriterOptions {
        WriterOptions {
            initialization_gate: None,
            shutdown_gate: None,
            command_gate: None,
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

    fn producer_id(tag: u8) -> ProducerId {
        ProducerId::from_bytes(uuid_bytes(tag)).expect("test producer identity should be UUIDv7")
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

    fn command(key: &str, digest_tag: u8, gap_start: u128) -> IngressCommand {
        let (envelope, retry) =
            model_parts(series_id(1), producer_id(2), key, digest_tag, gap_start);
        IngressCommand::new(envelope, retry).expect("test command scope should match")
    }

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
    }

    async fn complete_bounded<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        for _ in 0..MAX_YIELDS {
            if let Poll::Ready(output) = poll_once(future.as_mut()).await {
                return output;
            }
            yield_now().await;
        }
        panic!("lifecycle future did not complete within the deterministic yield bound");
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        for _ in 0..MAX_YIELDS {
            if condition() {
                return;
            }
            yield_now().await;
        }
        assert!(condition(), "condition did not hold within the yield bound");
    }

    #[test]
    fn no_active_runtime_is_a_sanitized_start_error() {
        let mut start = Box::pin(HistorianRuntime::start());
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
            let mut start = Box::pin(HistorianRuntime::start_with_options(writer_options));

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
            let mut start = Box::pin(HistorianRuntime::start_with_options(writer_options));

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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
                .await
                .expect("writer should start");
            let mut shutdown = Box::pin(runtime.shutdown());

            assert!(poll_once(shutdown.as_mut()).await.is_pending());
            wait_until(|| probe.shutdown_received.load(Ordering::SeqCst) == 1).await;
            drop(shutdown);
            wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.normal_exits.load(Ordering::SeqCst), 0);
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn plain_handle_drop_is_abort_only_and_non_graceful() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &probe,
                TestBehavior::Normal,
            )))
            .await
            .expect("writer should start");

            drop(runtime);
            wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 1).await;
            assert_eq!(probe.normal_exits.load(Ordering::SeqCst), 0);
            assert_eq!(probe.state_dropped.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn premature_writer_exits_are_distinguished() {
        harness().block_on(async {
            let before_ready_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(options(
                &before_ready_probe,
                TestBehavior::ExitBeforeReadiness,
            )))
            .await
            .expect_err("premature startup exit must fail");
            assert_eq!(start_error, StartError::WriterExitedBeforeReadiness);

            let before_shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &before_shutdown_probe,
                TestBehavior::ExitBeforeShutdown,
            )))
            .await
            .expect("readiness should precede the injected exit");
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("premature writer exit must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterExitedBeforeShutdown);
        });
    }

    #[test]
    fn task_cancellation_maps_to_closed_errors() {
        harness().block_on(async {
            let start_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(options(
                &start_probe,
                TestBehavior::CancelBeforeReadiness,
            )))
            .await
            .expect_err("aborted startup writer should be cancelled");
            assert_eq!(start_error, StartError::WriterTaskCancelled);

            let shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &shutdown_probe,
                TestBehavior::Normal,
            )))
            .await
            .expect("writer should start");
            runtime
                .writer
                .as_ref()
                .expect("runtime should retain writer")
                .abort();
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("aborted writer must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterTaskCancelled);
        });
    }

    #[test]
    fn task_panics_map_without_exposing_the_payload() {
        harness().block_on(async {
            let start_probe = Arc::new(LifecycleProbe::default());
            let start_error = complete_bounded(HistorianRuntime::start_with_options(options(
                &start_probe,
                TestBehavior::PanicBeforeReadiness,
            )))
            .await
            .expect_err("writer panic must fail startup");
            assert_eq!(start_error, StartError::WriterTaskPanicked);
            assert!(!start_error.to_string().contains("hostile"));

            let shutdown_probe = Arc::new(LifecycleProbe::default());
            let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &shutdown_probe,
                TestBehavior::PanicBeforeShutdown,
            )))
            .await
            .expect("readiness should precede the injected panic");
            let shutdown_error = complete_bounded(runtime.shutdown())
                .await
                .expect_err("writer panic must fail shutdown");
            assert_eq!(shutdown_error, ShutdownError::WriterTaskPanicked);
            assert!(!shutdown_error.to_string().contains("hostile"));
        });
    }

    #[test]
    fn two_instances_own_isolated_single_writers() {
        harness().block_on(async {
            let first_probe = Arc::new(LifecycleProbe::default());
            let second_probe = Arc::new(LifecycleProbe::default());
            let first = complete_bounded(HistorianRuntime::start_with_options(options(
                &first_probe,
                TestBehavior::Normal,
            )))
            .await
            .expect("first writer should start");
            let second = complete_bounded(HistorianRuntime::start_with_options(options(
                &second_probe,
                TestBehavior::Normal,
            )))
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
                let start_error = complete_bounded(HistorianRuntime::start_with_options(options(
                    &probe,
                    TestBehavior::ExitBeforeReadiness,
                )))
                .await
                .expect_err("injected early exit must remain bounded");
                assert_eq!(start_error, StartError::WriterExitedBeforeReadiness);

                let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                    &probe,
                    TestBehavior::ExitBeforeShutdown,
                )))
                .await
                .expect("injected post-readiness exit should start");
                assert_eq!(
                    complete_bounded(runtime.shutdown()).await,
                    Err(ShutdownError::WriterExitedBeforeShutdown)
                );

                let runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                    &probe,
                    TestBehavior::Normal,
                )))
                .await
                .expect("normal writer should start");
                drop(runtime);
                wait_until(|| probe.task_dropped.load(Ordering::SeqCst) == 3).await;
            }
        });
    }

    #[test]
    fn scope_mismatch_is_sanitized_recoverable_and_uses_no_slot() {
        let ingress = HistorianIngress::new();
        let (envelope, _) = model_parts(series_id(1), producer_id(2), "HOSTILE-RETRY-KEY", 3, 0);
        let (_, retry) = model_parts(series_id(9), producer_id(2), "HOSTILE-RETRY-KEY", 3, 0);
        let expected_envelope = envelope.clone();
        let expected_retry = retry.clone();

        let Err(error) = IngressCommand::new(envelope, retry) else {
            panic!("mismatched series scope must be rejected");
        };
        assert!(!format!("{error:?}").contains("HOSTILE"));
        assert!(!error.to_string().contains("HOSTILE"));
        let (recovered_envelope, recovered_retry) = error.into_parts();
        assert_eq!(recovered_envelope, expected_envelope);
        assert_eq!(recovered_retry, expected_retry);
        assert_eq!(ingress.test_counts(), (0, 0, 0));
    }

    #[test]
    fn receipt_stays_pending_until_the_gated_writer_handles_work() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let (_, recovered_retry) = full.into_command().into_parts();
            assert_eq!(recovered_retry.key().as_str(), "seventeenth-hostile");

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
            let (_, recovered_conflict) = conflict.into_command().into_parts();
            assert_eq!(recovered_conflict.content().sha256(), &[11; 32]);
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
                .expect("qualification after terminal completion should be new work");
            assert_eq!(after_terminal.disposition(), SubmissionDisposition::Queued);
            assert_eq!(
                complete_bounded(after_terminal.into_receipt().wait()).await,
                ReceiptOutcome::WriterHandled
            );
            complete_bounded(runtime.shutdown())
                .await
                .expect("retry-window runtime should shut down");
        });
    }

    #[test]
    fn sequential_distinct_admissions_are_handled_fifo() {
        harness().block_on(async {
            let probe = Arc::new(LifecycleProbe::default());
            let (open_command, command_gate) = oneshot::channel();
            let mut writer_options = options(&probe, TestBehavior::Normal);
            writer_options.command_gate = Some(command_gate);
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
            let runtime = complete_bounded(HistorianRuntime::start_with_options(writer_options))
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
                closed.into_command().into_parts().1.key().as_str(),
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
            let handled_runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &handled_probe,
                TestBehavior::Normal,
            )))
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
            let drop_runtime = complete_bounded(HistorianRuntime::start_with_options(drop_options))
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
            let cancel_runtime =
                complete_bounded(HistorianRuntime::start_with_options(cancel_options))
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
            let exit_runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &exit_probe,
                TestBehavior::ExitWhileHandling,
            )))
            .await
            .expect("early-exit writer should start");
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

            let panic_probe = Arc::new(LifecycleProbe::default());
            let panic_runtime = complete_bounded(HistorianRuntime::start_with_options(options(
                &panic_probe,
                TestBehavior::PanicWhileHandling,
            )))
            .await
            .expect("panic writer should report readiness first");
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
        });
    }

    #[test]
    fn task_abort_stops_in_flight_receipt_and_maps_shutdown() {
        harness().block_on(async {
            let cancel_probe = Arc::new(LifecycleProbe::default());
            let (_keep_cancel_gate_closed, cancel_gate) = oneshot::channel();
            let mut cancel_options = options(&cancel_probe, TestBehavior::Normal);
            cancel_options.command_gate = Some(cancel_gate);
            let cancel_runtime =
                complete_bounded(HistorianRuntime::start_with_options(cancel_options))
                    .await
                    .expect("cancel writer should start");
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
        });
    }

    #[test]
    fn poisoned_admission_stops_receipts_and_fails_closed() {
        harness().block_on(async {
            let poison_probe = Arc::new(LifecycleProbe::default());
            let (open_poison_gate, poison_gate) = oneshot::channel();
            let mut poison_options = options(&poison_probe, TestBehavior::Normal);
            poison_options.command_gate = Some(poison_gate);
            let poison_runtime =
                complete_bounded(HistorianRuntime::start_with_options(poison_options))
                    .await
                    .expect("poison writer should start");
            let poison_ingress = poison_runtime.ingress();
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
            let first = complete_bounded(HistorianRuntime::start_with_options(first_options))
                .await
                .expect("first writer should start");
            let second = complete_bounded(HistorianRuntime::start_with_options(second_options))
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
                let ingress = HistorianIngress::new();
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
}
