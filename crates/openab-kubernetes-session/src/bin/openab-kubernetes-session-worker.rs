use openab_kubernetes_session::worker::bootstrap::{
    WorkerBootstrap, WorkerBootstrapEnvironment, WorkerBootstrapError, WorkerCommand,
    WorkerCommandError, WorkerEnvironmentError,
};
#[cfg(target_os = "linux")]
use openab_kubernetes_session::worker::run_worker_once;
#[cfg(target_os = "linux")]
use openab_kubernetes_session::worker::workspace::prepare_workspace;
use openab_kubernetes_session::worker::workspace::WorkspacePreparationError;
use openab_kubernetes_session::worker::{
    TerminationSignalError, TerminationSignals, WorkerRuntimeError,
};
use std::ffi::OsString;
use std::process::ExitCode;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
enum ApplicationError {
    #[error(transparent)]
    Signal(#[from] TerminationSignalError),
    #[error(transparent)]
    Command(#[from] WorkerCommandError),
    #[error(transparent)]
    Environment(#[from] WorkerEnvironmentError),
    #[error(transparent)]
    Bootstrap(#[from] WorkerBootstrapError),
    #[error(transparent)]
    Workspace(#[from] WorkspacePreparationError),
    #[error(transparent)]
    Runtime(#[from] WorkerRuntimeError),
}

impl ApplicationError {
    const fn exit_code(&self) -> u8 {
        match self {
            Self::Signal(_) | Self::Command(_) | Self::Environment(_) | Self::Bootstrap(_) => 2,
            Self::Workspace(_) => 3,
            Self::Runtime(WorkerRuntimeError::Registration(_)) => 4,
            Self::Runtime(WorkerRuntimeError::Supervision(_)) => 5,
        }
    }
}

fn prepare_startup<I, S, Args, Install, Load>(
    args: Args,
    install_signals: Install,
    load_bootstrap: Load,
) -> Result<(S, WorkerBootstrap), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
    Args: FnOnce() -> I,
    Install: FnOnce() -> Result<S, ApplicationError>,
    Load: FnOnce(WorkerCommand) -> Result<WorkerBootstrap, ApplicationError>,
{
    let signals = install_signals()?;
    let command = WorkerCommand::parse(args())?;
    let bootstrap = load_bootstrap(command)?;
    Ok((signals, bootstrap))
}

#[cfg(target_os = "linux")]
async fn run<I, Args>(args: Args) -> Result<(), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
    Args: FnOnce() -> I,
{
    let (mut signals, bootstrap) = prepare_startup(
        args,
        || TerminationSignals::install().map_err(ApplicationError::from),
        |command| {
            let environment = WorkerBootstrapEnvironment::from_environment()?;
            WorkerBootstrap::load_from_files(command, environment).map_err(ApplicationError::from)
        },
    )?;
    let workspace = prepare_workspace()?;
    run_worker_once(bootstrap, workspace, signals.wait())
        .await
        .map_err(ApplicationError::from)
}

#[cfg(not(target_os = "linux"))]
fn run<I, Args>(args: Args) -> Result<(), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
    Args: FnOnce() -> I,
{
    let (_signals, _bootstrap) = prepare_startup(
        args,
        || TerminationSignals::install().map_err(ApplicationError::from),
        |command| {
            let environment = WorkerBootstrapEnvironment::from_environment()?;
            WorkerBootstrap::load_from_files(command, environment).map_err(ApplicationError::from)
        },
    )?;
    unreachable!("unsupported targets fail while installing termination signals")
}

fn report(result: Result<(), ApplicationError>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(error.exit_code())
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    install_sanitized_panic_hook();
    report(run(|| std::env::args_os().skip(1)).await)
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
    install_sanitized_panic_hook();
    report(run(|| std::env::args_os().skip(1)))
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        eprintln!("worker terminated unexpectedly");
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn signal_installation_precedes_parsing_and_loading() {
        let events = RefCell::new(Vec::new());
        let error = prepare_startup(
            || {
                events.borrow_mut().push("args");
                args(&["serve", "--", "/does/not/need/to/exist"])
            },
            || {
                events.borrow_mut().push("signal");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("load");
                Err(ApplicationError::Bootstrap(
                    WorkerBootstrapError::OpenRegistrationToken,
                ))
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            ApplicationError::Bootstrap(WorkerBootstrapError::OpenRegistrationToken)
        );
        assert_eq!(*events.borrow(), ["signal", "args", "load"]);

        events.borrow_mut().clear();
        let error = prepare_startup(
            || {
                events.borrow_mut().push("args");
                args(&["invalid-sensitive-argv"])
            },
            || {
                events.borrow_mut().push("signal");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("load");
                Err(ApplicationError::Bootstrap(
                    WorkerBootstrapError::OpenRegistrationToken,
                ))
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            ApplicationError::Command(WorkerCommandError::UnknownSubcommand)
        );
        assert_eq!(*events.borrow(), ["signal", "args"]);
        assert!(!format!("{error:?} {error}").contains("sensitive-argv"));
    }

    #[test]
    fn signal_failure_prevents_parsing_and_loading() {
        let args_loaded = RefCell::new(false);
        let loaded = RefCell::new(false);
        let error = prepare_startup(
            || {
                *args_loaded.borrow_mut() = true;
                args(&["invalid-sensitive-argv"])
            },
            || {
                Err::<(), _>(ApplicationError::Signal(
                    TerminationSignalError::Initialization,
                ))
            },
            |_| {
                *loaded.borrow_mut() = true;
                Err(ApplicationError::Bootstrap(
                    WorkerBootstrapError::OpenRegistrationToken,
                ))
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            ApplicationError::Signal(TerminationSignalError::Initialization)
        );
        assert!(!*args_loaded.borrow());
        assert!(!*loaded.borrow());
        assert!(!format!("{error:?} {error}").contains("sensitive-argv"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn unsupported_target_fails_before_argv_or_environment() {
        let args_loaded = RefCell::new(false);
        let error = run(|| {
            *args_loaded.borrow_mut() = true;
            args(&["sensitive-argv"])
        })
        .unwrap_err();
        assert_eq!(
            error,
            ApplicationError::Signal(TerminationSignalError::UnsupportedRuntime)
        );
        assert!(!*args_loaded.borrow());
        assert!(!format!("{error:?} {error}").contains("sensitive-argv"));
    }

    #[test]
    fn failures_have_stable_sanitized_phase_exit_codes() {
        use openab_kubernetes_session::worker::registration::WorkerRegistrationError;
        use openab_kubernetes_session::worker::supervisor::WorkerSupervisionError;
        use openab_kubernetes_session::worker::workspace::WorkspacePreparationError;

        let cases = [
            (
                ApplicationError::Signal(TerminationSignalError::Initialization),
                2,
            ),
            (
                ApplicationError::Command(WorkerCommandError::MissingSubcommand),
                2,
            ),
            (
                ApplicationError::Environment(WorkerEnvironmentError::Unavailable { name: "HOME" }),
                2,
            ),
            (
                ApplicationError::Bootstrap(WorkerBootstrapError::OpenRegistrationToken),
                2,
            ),
            (
                ApplicationError::Workspace(WorkspacePreparationError::RootUnavailable),
                3,
            ),
            (
                ApplicationError::Runtime(WorkerRuntimeError::Registration(
                    WorkerRegistrationError::Connect,
                )),
                4,
            ),
            (
                ApplicationError::Runtime(WorkerRuntimeError::Supervision(
                    WorkerSupervisionError::ChildFailed,
                )),
                5,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.exit_code(), expected);
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("sensitive"));
            assert!(!rendered.contains("Bearer"));
        }

        assert_eq!(report(Ok(())), ExitCode::SUCCESS);
    }
}
