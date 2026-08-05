//! One-shot Kubernetes session worker runtime.
//!
//! The worker is Linux-only at runtime. Parsing and bootstrap validation stay
//! platform-neutral so their closed contracts remain testable on development
//! hosts without touching an ACP executable or the network.

pub mod bootstrap;
#[cfg(any(target_os = "linux", all(test, unix)))]
mod process;
pub mod registration;
pub mod relay;
pub mod supervisor;
pub mod workspace;

use registration::{build_worker_request, register_worker_once, WorkerRegistrationError};
use std::future::Future;
use supervisor::{supervise_registered_worker, WorkerSupervisionError};
use thiserror::Error;
use workspace::PreparedWorkspace;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerRuntimeError {
    #[error(transparent)]
    Registration(#[from] WorkerRegistrationError),
    #[error(transparent)]
    Supervision(#[from] WorkerSupervisionError),
}

/// Consume one bootstrap and one private workspace to run exactly one
/// registration and one ACP process tree. A normal termination request before
/// acknowledgement is a successful shutdown; every other terminal path is
/// returned without reconnecting, replaying, or restarting.
pub async fn run_worker_once<Shutdown>(
    bootstrap: bootstrap::WorkerBootstrap,
    workspace: PreparedWorkspace,
    shutdown: Shutdown,
) -> Result<(), WorkerRuntimeError>
where
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send,
{
    let request = build_worker_request(bootstrap)?;
    tokio::pin!(shutdown);
    let registered = match register_worker_once(request, shutdown.as_mut()).await {
        Ok(registered) => registered,
        Err(WorkerRegistrationError::Terminated) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    supervise_registered_worker(registered, workspace, shutdown.as_mut()).await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TerminationSignalError {
    #[error("the Kubernetes session worker requires Linux")]
    UnsupportedRuntime,
    #[error("worker termination signal handling could not be initialized")]
    Initialization,
    #[error("worker termination signal stream failed")]
    StreamClosed,
}

#[cfg(target_os = "linux")]
pub struct TerminationSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(target_os = "linux")]
impl TerminationSignals {
    /// Install both signal receivers before any argv parsing or file access.
    pub fn install() -> Result<Self, TerminationSignalError> {
        use tokio::signal::unix::{signal, SignalKind};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())
                .map_err(|_| TerminationSignalError::Initialization)?,
            terminate: signal(SignalKind::terminate())
                .map_err(|_| TerminationSignalError::Initialization)?,
        })
    }

    /// Observe the first latched termination event, preferring shutdown when
    /// both signal streams become ready at the same boundary.
    pub async fn wait(&mut self) -> Result<(), TerminationSignalError> {
        tokio::select! {
            biased;
            received = self.interrupt.recv() => received
                .map(|_| ())
                .ok_or(TerminationSignalError::StreamClosed),
            received = self.terminate.recv() => received
                .map(|_| ())
                .ok_or(TerminationSignalError::StreamClosed),
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct TerminationSignals;

#[cfg(not(target_os = "linux"))]
impl TerminationSignals {
    /// Fail before inspecting argv, environment, or mounted files.
    pub fn install() -> Result<Self, TerminationSignalError> {
        Err(TerminationSignalError::UnsupportedRuntime)
    }

    pub async fn wait(&mut self) -> Result<(), TerminationSignalError> {
        Err(TerminationSignalError::UnsupportedRuntime)
    }
}
