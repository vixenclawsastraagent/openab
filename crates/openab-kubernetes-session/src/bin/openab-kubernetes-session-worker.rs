use openab_kubernetes_session::worker::bootstrap::{
    WorkerBootstrap, WorkerBootstrapEnvironment, WorkerBootstrapError, WorkerCommand,
    WorkerCommandError, WorkerEnvironmentError,
};
use openab_kubernetes_session::worker::{TerminationSignalError, TerminationSignals};
use std::ffi::OsString;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum ApplicationError {
    #[error(transparent)]
    Signal(#[from] TerminationSignalError),
    #[error(transparent)]
    Command(#[from] WorkerCommandError),
    #[error(transparent)]
    Environment(#[from] WorkerEnvironmentError),
    #[error(transparent)]
    Bootstrap(#[from] WorkerBootstrapError),
    #[error("worker activation is unavailable in this build stage")]
    ActivationUnavailable,
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
    Err(ApplicationError::ActivationUnavailable)
}

fn report(result: Result<(), ApplicationError>) {
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "linux")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    install_sanitized_panic_hook();
    report(run(|| std::env::args_os().skip(1)));
}

#[cfg(not(target_os = "linux"))]
fn main() {
    install_sanitized_panic_hook();
    report(run(|| std::env::args_os().skip(1)));
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
                Err(ApplicationError::ActivationUnavailable)
            },
        )
        .unwrap_err();
        assert_eq!(error, ApplicationError::ActivationUnavailable);
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
                Err(ApplicationError::ActivationUnavailable)
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
                Err(ApplicationError::ActivationUnavailable)
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
}
