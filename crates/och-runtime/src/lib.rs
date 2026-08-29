#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Caller-executor lifecycle for the `OpenControl` Historian writer.
//!
//! This crate owns only task startup and termination. It deliberately exposes no
//! command ingress, state observation, persistence, or restart mechanism. Each
//! [`HistorianRuntime`] owns exactly one private writer task on the caller's
//! active Tokio executor.

use std::error::Error;
use std::fmt;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

/// A running private Historian writer task.
///
/// Each handle owns one writer task and its sole graceful-shutdown signal.
/// Independent handles are isolated and valid concurrently; there is no global
/// runtime registry. A stopped or failed instance cannot be restarted—call
/// [`HistorianRuntime::start`] to construct a new one.
///
/// Dropping this handle requests task cancellation without blocking. Call
/// [`HistorianRuntime::shutdown`] and await its result when joined normal
/// termination is required.
pub struct HistorianRuntime {
    shutdown: Option<oneshot::Sender<()>>,
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
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        #[cfg(test)]
        let cancel_before_readiness = options.behavior == TestBehavior::CancelBeforeReadiness;
        let writer = executor.spawn(run_writer(options, readiness_tx, shutdown_rx));
        let mut startup = StartupGuard::new(writer);

        #[cfg(test)]
        if cancel_before_readiness {
            startup.abort();
        }

        if readiness_rx.await.is_ok() {
            Ok(Self {
                shutdown: Some(shutdown_tx),
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

    /// Gracefully stops and joins this instance's private writer task.
    ///
    /// `Ok(())` means the shutdown signal was accepted and the writer terminated
    /// normally before its retained task handle was joined. This lifecycle-only
    /// slice has no data commands, so draining is vacuous; success is not
    /// persistence, durability, or ingress-acceptance evidence.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ShutdownError`] when the writer had already exited,
    /// was cancelled, or panicked. If this future is cancelled, its owned handle
    /// is dropped and requests nonblocking writer abortion rather than detaching
    /// the retained task.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        let shutdown_was_sent = self
            .shutdown
            .take()
            .is_some_and(|shutdown| shutdown.send(()).is_ok());
        let Some(writer) = self.writer.as_mut() else {
            return Err(ShutdownError::WriterExitedBeforeShutdown);
        };
        let result = writer.await;
        self.writer = None;

        match result {
            Ok(WriterExit::Shutdown) if shutdown_was_sent => Ok(()),
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
        if let Some(writer) = self.writer.take() {
            writer.abort();
        }
        drop(self.shutdown.take());
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
    ShutdownSignalClosed,
    StartupReceiverClosed,
    #[cfg(test)]
    BeforeReadiness,
    #[cfg(test)]
    BeforeShutdown,
}

struct WriterState {
    #[cfg(test)]
    probe: Option<std::sync::Arc<LifecycleProbe>>,
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
    behavior: TestBehavior,
    #[cfg(test)]
    probe: Option<std::sync::Arc<LifecycleProbe>>,
}

impl WriterOptions {
    const fn production() -> Self {
        Self {
            #[cfg(test)]
            initialization_gate: None,
            #[cfg(test)]
            shutdown_gate: None,
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
    shutdown: oneshot::Receiver<()>,
) -> WriterExit {
    #[cfg(test)]
    let mut options = options;
    #[cfg(test)]
    let _task_guard = TaskGuard::new(options.probe.clone());

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
        | TestBehavior::PanicBeforeShutdown => {}
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
        | TestBehavior::PanicBeforeReadiness => {}
    }

    if shutdown.await.is_err() {
        return WriterExit::ShutdownSignalClosed;
    }
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
    WriterExit::Shutdown
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
}

#[cfg(test)]
struct TaskGuard(Option<std::sync::Arc<LifecycleProbe>>);

#[cfg(test)]
impl TaskGuard {
    fn new(probe: Option<std::sync::Arc<LifecycleProbe>>) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::{
        HistorianRuntime, LifecycleProbe, ShutdownError, StartError, TestBehavior, WriterOptions,
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
            behavior,
            probe: Some(Arc::clone(probe)),
        }
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
}
