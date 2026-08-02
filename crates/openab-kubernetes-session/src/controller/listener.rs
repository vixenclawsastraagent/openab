//! Bounded TCP supervision for the trusted controller endpoint.

use super::{
    ControllerAdmissionError, ControllerConnectionOutcome, ControllerEndpoint,
    ControllerTlsAcceptor, ControllerTlsConnectionError,
};
use std::future::Future;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

type ConnectionResult = Result<ControllerConnectionOutcome, ControllerTlsConnectionError>;

/// Serves one already-bound, fixed-scope controller endpoint.
///
/// The caller owns address validation, binding, readiness, and the shutdown
/// signal. Admission happens before TLS so slow or malicious handshakes are
/// included in the same global connection bound as active relay connections.
///
/// This endpoint must be cluster-internal and protected by ingress policy.
/// Pre-authentication admission bounds resource use but does not identify the
/// peer, so exposing the listener to unrestricted sources would let them
/// consume the connection pool until the fixed TLS deadline expires.
pub struct ControllerListener {
    endpoint: ControllerEndpoint,
    tls: ControllerTlsAcceptor,
}

impl ControllerListener {
    pub fn new(endpoint: ControllerEndpoint, tls: ControllerTlsAcceptor) -> Self {
        Self { endpoint, tls }
    }

    /// Serve until the caller-owned shutdown future resolves.
    ///
    /// Peer-local TLS, authentication, and protocol failures close only that
    /// connection. Listener I/O failures and unexpectedly stopped connection
    /// tasks fail the listener closed. Shutdown stops admission, aborts every
    /// in-flight connection, and drains the task set before returning.
    /// Controller-owned containment tasks may outlive this transport drain;
    /// the process supervisor must perform its bounded containment pass or
    /// terminate so the next startup orphan scan runs before readiness.
    pub async fn serve_until<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), ControllerListenerError>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut connections = JoinSet::new();
        let exit_error = loop {
            tokio::select! {
                biased;

                _ = &mut shutdown => break None,
                completed = connections.join_next(), if !connections.is_empty() => {
                    let completed = completed.expect("guarded by a non-empty task set");
                    if let Err(error) = observe_connection(completed) {
                        break Some(error);
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(source) => break Some(ControllerListenerError::Accept(source)),
                    };
                    match self.endpoint.try_admit() {
                        Ok(connection) => {
                            let tls = self.tls.clone();
                            connections.spawn(async move { tls.serve(connection, stream).await });
                        }
                        Err(ControllerAdmissionError::AtCapacity) => {
                            drop(stream);
                        }
                    }
                }
            }
        };

        drop(listener);
        connections.abort_all();
        let mut drain_error = None;
        while let Some(completed) = connections.join_next().await {
            match completed {
                Err(error) if error.is_cancelled() => {}
                completed => {
                    if let Err(error) = observe_connection(completed) {
                        drain_error.get_or_insert(error);
                    }
                }
            }
        }

        match exit_error.or(drain_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn observe_connection(
    completed: Result<ConnectionResult, tokio::task::JoinError>,
) -> Result<(), ControllerListenerError> {
    match completed {
        Ok(Ok(_outcome)) => Ok(()),
        Ok(Err(error)) if error.is_process_fatal() => {
            tracing::error!("controller connection encountered an internal invariant failure");
            Err(ControllerListenerError::UnsafeConnectionState)
        }
        Ok(Err(error)) => {
            tracing::debug!(error = %error, "controller peer connection ended");
            Ok(())
        }
        Err(_) => Err(ControllerListenerError::ConnectionTaskStopped),
    }
}

#[derive(Debug, Error)]
pub enum ControllerListenerError {
    #[error("controller listener failed to accept a connection")]
    Accept(#[source] std::io::Error),
    #[error("controller connection task stopped unexpectedly")]
    ConnectionTaskStopped,
    #[error("controller connection encountered an unsafe internal state")]
    UnsafeConnectionState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::ControllerConnectionError;

    #[test]
    fn peer_errors_remain_local_but_internal_invariants_fail_closed() {
        assert!(observe_connection(Ok(Err(ControllerTlsConnectionError::TimedOut))).is_ok());
        assert!(matches!(
            observe_connection(Ok(Err(ControllerTlsConnectionError::Connection(Box::new(
                ControllerConnectionError::MissingUpgradeAuthority
            ))))),
            Err(ControllerListenerError::UnsafeConnectionState)
        ));
    }
}
