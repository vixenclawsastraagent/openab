//! One-shot supervision for one acknowledged worker lane and ACP process tree.

#[cfg(target_os = "linux")]
use super::process::spawn_child;
#[cfg(any(target_os = "linux", all(test, unix)))]
use super::process::{WorkerProcess, WorkerProcessError};
use super::registration::RegisteredWorker;
#[cfg(target_os = "linux")]
use super::relay::relay_child;
use super::relay::WorkerRelayError;
use super::workspace::{PreparedWorkspace, WorkspacePreparationError};
use super::TerminationSignalError;
#[cfg(any(target_os = "linux", all(test, unix)))]
use async_trait::async_trait;
#[cfg(any(target_os = "linux", all(test, unix)))]
use futures_util::FutureExt;
#[cfg(any(target_os = "linux", all(test, unix)))]
use std::future;
use std::future::Future;
#[cfg(any(target_os = "linux", all(test, unix)))]
use std::panic::AssertUnwindSafe;
#[cfg(any(target_os = "linux", all(test, unix)))]
use std::pin::Pin;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerSupervisionError {
    #[error("the Kubernetes session worker requires Linux")]
    UnsupportedRuntime,
    #[error(transparent)]
    Workspace(#[from] WorkspacePreparationError),
    #[error("the ACP child could not be started")]
    Spawn,
    #[error("the ACP child standard streams are unavailable")]
    ChildStdio,
    #[error("the ACP child could not be reaped")]
    ChildWait,
    #[error("the ACP child exited unsuccessfully")]
    ChildFailed,
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
    #[error(transparent)]
    Signal(#[from] TerminationSignalError),
    #[error("the ACP child process group could not be contained")]
    Containment,
    #[error("worker supervision terminated unexpectedly")]
    Unexpected,
}

/// Consume one acknowledged registration capability and supervise exactly one
/// ACP process tree on Linux. No terminal path reconnects, restarts, or
/// replays. Cleanup failure overrides the initiating cause; simultaneous
/// events prefer shutdown, then relay, then child exit. A clean relay EOF is
/// successful only when the final child status is also successful.
///
/// This future must be awaited directly. Dropping it closes the registered
/// lane and issues emergency process-group KILL, but a destructor cannot
/// asynchronously reap or prove group absence. Graceful shutdown must use the
/// supplied `shutdown` future instead of aborting this supervisor.
pub async fn supervise_registered_worker<S, Shutdown>(
    registered: RegisteredWorker<S>,
    workspace: PreparedWorkspace,
    shutdown: Shutdown,
) -> Result<(), WorkerSupervisionError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send,
{
    #[cfg(target_os = "linux")]
    {
        supervise_registered_worker_linux(registered, workspace, shutdown).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (registered, workspace, shutdown);
        Err(WorkerSupervisionError::UnsupportedRuntime)
    }
}

#[cfg(target_os = "linux")]
async fn supervise_registered_worker_linux<S, Shutdown>(
    registered: RegisteredWorker<S>,
    workspace: PreparedWorkspace,
    shutdown: Shutdown,
) -> Result<(), WorkerSupervisionError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send,
{
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        signal = shutdown.as_mut() => return signal.map_err(WorkerSupervisionError::Signal),
        _ = future::ready(()) => {}
    }
    let mut process = spawn_child(registered.command(), &workspace).map_err(map_spawn_error)?;
    let (child_stdout, child_stdin) = match process.take_stdio() {
        Ok(stdio) => stdio,
        Err(error) => {
            drop(registered);
            return cleanup_after_setup_failure(&mut process, error).await;
        }
    };
    let relay = relay_child(registered, child_stdout, child_stdin);
    supervise_started(&mut process, relay, shutdown.as_mut()).await
}

#[cfg(target_os = "linux")]
async fn cleanup_after_setup_failure(
    process: &mut WorkerProcess,
    setup_error: WorkerProcessError,
) -> Result<(), WorkerSupervisionError> {
    process
        .terminate_and_reap()
        .await
        .map_err(|_| WorkerSupervisionError::Containment)?;
    Err(map_process_error(setup_error))
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[derive(Debug)]
enum Terminal {
    Shutdown(Result<(), TerminationSignalError>),
    Relay(Result<(), WorkerRelayError>),
    Child(Result<bool, WorkerProcessError>),
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[async_trait]
trait ProcessControl: Send {
    async fn wait_for_exit(&mut self) -> Result<bool, WorkerProcessError>;
    async fn terminate_and_reap(&mut self) -> Result<(), WorkerProcessError>;
    fn exit_success(&self) -> Option<bool>;
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[async_trait]
impl ProcessControl for WorkerProcess {
    async fn wait_for_exit(&mut self) -> Result<bool, WorkerProcessError> {
        WorkerProcess::wait_for_exit(self).await
    }

    async fn terminate_and_reap(&mut self) -> Result<(), WorkerProcessError> {
        WorkerProcess::terminate_and_reap(self).await
    }

    fn exit_success(&self) -> Option<bool> {
        WorkerProcess::exit_success(self)
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
async fn supervise_started<P, Relay, Shutdown>(
    process: &mut P,
    relay: Relay,
    shutdown: Pin<&mut Shutdown>,
) -> Result<(), WorkerSupervisionError>
where
    P: ProcessControl,
    Relay: Future<Output = Result<(), WorkerRelayError>> + Send,
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send + ?Sized,
{
    // `select_terminal` owns the relay future. Its completion or unwind drops
    // that future—and therefore both socket halves—before cleanup can signal
    // the process group below.
    let terminal = AssertUnwindSafe(select_terminal(process, relay, shutdown))
        .catch_unwind()
        .await;
    process
        .terminate_and_reap()
        .await
        .map_err(|_| WorkerSupervisionError::Containment)?;

    match terminal {
        Err(_) => Err(WorkerSupervisionError::Unexpected),
        Ok(Terminal::Shutdown(result)) => result.map_err(WorkerSupervisionError::Signal),
        Ok(Terminal::Relay(Ok(()))) if process.exit_success() == Some(false) => {
            Err(WorkerSupervisionError::ChildFailed)
        }
        Ok(Terminal::Relay(result)) => result.map_err(WorkerSupervisionError::Relay),
        Ok(Terminal::Child(Ok(true))) => Ok(()),
        Ok(Terminal::Child(Ok(false))) => Err(WorkerSupervisionError::ChildFailed),
        Ok(Terminal::Child(Err(error))) => Err(map_process_error(error)),
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
async fn select_terminal<P, Relay, Shutdown>(
    process: &mut P,
    relay: Relay,
    mut shutdown: Pin<&mut Shutdown>,
) -> Terminal
where
    P: ProcessControl,
    Relay: Future<Output = Result<(), WorkerRelayError>> + Send,
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send + ?Sized,
{
    tokio::pin!(relay);
    tokio::select! {
        biased;
        signal = shutdown.as_mut() => Terminal::Shutdown(signal),
        result = relay.as_mut() => Terminal::Relay(result),
        result = process.wait_for_exit() => Terminal::Child(result),
    }
}

#[cfg(target_os = "linux")]
fn map_spawn_error(error: WorkerProcessError) -> WorkerSupervisionError {
    match error {
        WorkerProcessError::Workspace(error) => WorkerSupervisionError::Workspace(error),
        other => map_process_error(other),
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn map_process_error(error: WorkerProcessError) -> WorkerSupervisionError {
    match error {
        WorkerProcessError::Workspace(error) => WorkerSupervisionError::Workspace(error),
        WorkerProcessError::Spawn => WorkerSupervisionError::Spawn,
        WorkerProcessError::ChildStdio => WorkerSupervisionError::ChildStdio,
        WorkerProcessError::ChildWait => WorkerSupervisionError::ChildWait,
        WorkerProcessError::TerminateGroup
        | WorkerProcessError::InspectGroup
        | WorkerProcessError::KillGroup
        | WorkerProcessError::KillLeader
        | WorkerProcessError::GroupSurvived => WorkerSupervisionError::Containment,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Event {
        WaitPolled,
        SocketDropped,
        TermGroup,
        Grace,
        KillGroup,
        Reap,
    }

    type Events = Arc<Mutex<Vec<Event>>>;

    struct FakeProcess {
        events: Events,
        wait: Option<Result<bool, WorkerProcessError>>,
        cleanup: Result<(), WorkerProcessError>,
        cleanup_exit: Option<bool>,
        exit_success: Option<bool>,
    }

    #[async_trait]
    impl ProcessControl for FakeProcess {
        async fn wait_for_exit(&mut self) -> Result<bool, WorkerProcessError> {
            self.events.lock().unwrap().push(Event::WaitPolled);
            let result = match self.wait {
                Some(result) => result,
                None => future::pending().await,
            };
            if let Ok(success) = result {
                self.exit_success = Some(success);
            }
            result
        }

        async fn terminate_and_reap(&mut self) -> Result<(), WorkerProcessError> {
            self.events.lock().unwrap().extend([
                Event::TermGroup,
                Event::Grace,
                Event::KillGroup,
                Event::Reap,
            ]);
            if let Some(success) = self.cleanup_exit {
                self.exit_success = Some(success);
            }
            self.cleanup
        }

        fn exit_success(&self) -> Option<bool> {
            self.exit_success
        }
    }

    enum RelayBehavior {
        Ready(Result<(), WorkerRelayError>),
        Pending,
        Panic,
    }

    struct ObservedRelay {
        events: Events,
        behavior: RelayBehavior,
    }

    impl Future for ObservedRelay {
        type Output = Result<(), WorkerRelayError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            match &self.get_mut().behavior {
                RelayBehavior::Ready(result) => Poll::Ready(*result),
                RelayBehavior::Pending => Poll::Pending,
                RelayBehavior::Panic => panic!("sensitive relay panic payload"),
            }
        }
    }

    impl Drop for ObservedRelay {
        fn drop(&mut self) {
            self.events.lock().unwrap().push(Event::SocketDropped);
        }
    }

    fn process(events: &Events, wait: Option<Result<bool, WorkerProcessError>>) -> FakeProcess {
        FakeProcess {
            events: Arc::clone(events),
            wait,
            cleanup: Ok(()),
            cleanup_exit: None,
            exit_success: None,
        }
    }

    fn relay(events: &Events, behavior: RelayBehavior) -> ObservedRelay {
        ObservedRelay {
            events: Arc::clone(events),
            behavior,
        }
    }

    fn assert_socket_first(events: &Events) {
        let events = events.lock().unwrap();
        let socket = events
            .iter()
            .position(|event| *event == Event::SocketDropped)
            .unwrap();
        let term = events
            .iter()
            .position(|event| *event == Event::TermGroup)
            .unwrap();
        assert!(socket < term, "events were {events:?}");
        assert_eq!(
            events[term..],
            [
                Event::TermGroup,
                Event::Grace,
                Event::KillGroup,
                Event::Reap,
            ]
        );
    }

    #[tokio::test]
    async fn simultaneous_events_prefer_shutdown_and_drop_socket_before_cleanup() {
        let events = Events::default();
        let mut process = process(&events, Some(Ok(false)));
        let mut shutdown = Box::pin(future::ready(Ok::<(), TerminationSignalError>(())));
        let result = supervise_started(
            &mut process,
            relay(
                &events,
                RelayBehavior::Ready(Err(WorkerRelayError::ControllerClosed)),
            ),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(result, Ok(()));
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn relay_error_is_preserved_after_socket_first_cleanup() {
        let events = Events::default();
        let mut process = process(&events, None);
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(
                &events,
                RelayBehavior::Ready(Err(WorkerRelayError::ChildWriteTimedOut)),
            ),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(
            result,
            Err(WorkerSupervisionError::Relay(
                WorkerRelayError::ChildWriteTimedOut
            ))
        );
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn simultaneous_relay_and_child_exit_prefer_the_relay_result() {
        let events = Events::default();
        let mut process = process(&events, Some(Ok(false)));
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(
                &events,
                RelayBehavior::Ready(Err(WorkerRelayError::ChildWrite)),
            ),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(
            result,
            Err(WorkerSupervisionError::Relay(WorkerRelayError::ChildWrite))
        );
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn clean_relay_eof_reports_the_final_nonzero_child_status() {
        let events = Events::default();
        let mut process = process(&events, None);
        process.cleanup_exit = Some(false);
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(&events, RelayBehavior::Ready(Ok(()))),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(result, Err(WorkerSupervisionError::ChildFailed));
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn nonzero_child_exit_is_terminal_without_restart() {
        let events = Events::default();
        let mut process = process(&events, Some(Ok(false)));
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(&events, RelayBehavior::Pending),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(result, Err(WorkerSupervisionError::ChildFailed));
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == Event::WaitPolled)
                .count(),
            1
        );
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn containment_failure_overrides_the_terminal_cause() {
        let events = Events::default();
        let mut process = process(&events, None);
        process.cleanup = Err(WorkerProcessError::KillGroup);
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(
                &events,
                RelayBehavior::Ready(Err(WorkerRelayError::WebSocketTransport)),
            ),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(result, Err(WorkerSupervisionError::Containment));
        assert_socket_first(&events);
    }

    #[tokio::test]
    async fn relay_panic_is_sanitized_and_still_cleans_up() {
        let events = Events::default();
        let mut process = process(&events, None);
        let mut shutdown = Box::pin(future::pending::<Result<(), TerminationSignalError>>());
        let result = supervise_started(
            &mut process,
            relay(&events, RelayBehavior::Panic),
            shutdown.as_mut(),
        )
        .await;

        assert_eq!(result, Err(WorkerSupervisionError::Unexpected));
        assert_socket_first(&events);
    }
}
