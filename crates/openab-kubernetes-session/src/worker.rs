//! One-shot Kubernetes session worker runtime.
//!
//! The worker is Linux-only at runtime. Parsing and bootstrap validation stay
//! platform-neutral so their closed contracts remain testable on development
//! hosts without touching an ACP executable or the network.

pub mod bootstrap;
pub mod registration;
pub mod workspace;

use thiserror::Error;

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
