use super::bridge_websocket::{
    serve_bridge_websocket, BridgeWebSocketOutcome, ControllerBridgeWebSocketError,
    MIN_RELEASE_RETRY_INTERVAL,
};
use super::websocket::relay_websocket_config;
use super::worker_websocket::{serve_worker_websocket, WorkerWebSocketError};
use super::{RelayLossOutcome, RelayOrchestrator, WorkerBootstrapAuth};
use crate::bridge::is_valid_controller_bearer_credential;
use sha2::{Digest, Sha256};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request};
use tokio_tungstenite::tungstenite::http::header::{
    AUTHORIZATION, CACHE_CONTROL, ORIGIN, SEC_WEBSOCKET_PROTOCOL,
};
use tokio_tungstenite::tungstenite::http::{
    HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
};
use tokio_tungstenite::{tungstenite, WebSocketStream};

pub const BRIDGE_WEBSOCKET_PATH: &str = "/v1/bridge";
pub const WORKER_WEBSOCKET_PATH: &str = "/v1/worker";
pub const WORKER_POD_UID_HEADER: HeaderName = HeaderName::from_static("x-openab-pod-uid");

const BEARER_PREFIX: &[u8] = b"Bearer ";
const WORKER_TOKEN_HEX_BYTES: usize = 64;
const WORKER_TOKEN_BYTES: usize = WORKER_TOKEN_HEX_BYTES / 2;

/// Startup-only bridge credential verifier.
///
/// Only a digest is retained after trusted configuration is parsed. This
/// avoids keeping the raw shared bearer credential in the long-lived
/// controller process while still allowing constant-time comparisons.
#[derive(Clone)]
struct BridgeBearerVerifier {
    digest: [u8; 32],
}

impl BridgeBearerVerifier {
    fn new(credential: &[u8]) -> Result<Self, ControllerEndpointBuildError> {
        if !is_valid_controller_bearer_credential(credential) {
            return Err(ControllerEndpointBuildError::InvalidBridgeCredential);
        }
        Ok(Self {
            digest: Sha256::digest(credential).into(),
        })
    }

    fn accepts(&self, credential: &[u8]) -> bool {
        if !is_valid_controller_bearer_credential(credential) {
            return false;
        }
        let candidate: [u8; 32] = Sha256::digest(credential).into();
        bool::from(self.digest.ct_eq(&candidate))
    }
}

impl fmt::Debug for BridgeBearerVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeBearerVerifier")
            .field("digest", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControllerEndpointBuildError {
    #[error("bridge bearer credential is invalid")]
    InvalidBridgeCredential,
}

/// Transport authority produced only from one accepted HTTP upgrade.
#[derive(Debug)]
enum UpgradeAuthority {
    Bridge,
    Worker(WorkerBootstrapAuth),
}

#[derive(Debug, Error)]
enum AuthenticatedUpgradeError {
    #[error("WebSocket handshake timed out")]
    TimedOut,
    #[error("WebSocket handshake failed")]
    Handshake(#[source] Box<tungstenite::Error>),
    #[error("WebSocket handshake did not produce transport authority")]
    MissingAuthority,
}

/// Closed, detail-free HTTP rejections for the upgrade boundary.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum UpgradeRejection {
    #[error("WebSocket upgrade request is invalid")]
    BadRequest,
    #[error("WebSocket upgrade is unauthorized")]
    Unauthorized,
    #[error("browser-origin WebSocket upgrades are forbidden")]
    Forbidden,
    #[error("WebSocket endpoint was not found")]
    NotFound,
    #[error("WebSocket admission is unavailable")]
    Unavailable,
}

impl UpgradeRejection {
    fn status(self) -> StatusCode {
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// Build a body-free rejection suitable for tungstenite's server callback.
    fn into_response(self) -> ErrorResponse {
        let mut response = ErrorResponse::new(None);
        *response.status_mut() = self.status();
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

/// Process-owned limits for one fixed-scope controller endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerEndpointConfig {
    max_connections: NonZeroUsize,
    websocket_upgrade_timeout: Duration,
    activation_timeout: Duration,
    registration_timeout: Duration,
    write_timeout: Duration,
    release_retry_interval: Duration,
}

impl ControllerEndpointConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_connections: NonZeroUsize,
        websocket_upgrade_timeout: Duration,
        activation_timeout: Duration,
        registration_timeout: Duration,
        write_timeout: Duration,
        release_retry_interval: Duration,
    ) -> Result<Self, ControllerEndpointConfigError> {
        if [
            websocket_upgrade_timeout,
            activation_timeout,
            registration_timeout,
            write_timeout,
        ]
        .into_iter()
        .any(|duration| duration.is_zero())
        {
            return Err(ControllerEndpointConfigError::ZeroTimeout);
        }
        if release_retry_interval < MIN_RELEASE_RETRY_INTERVAL {
            return Err(ControllerEndpointConfigError::InvalidReleaseRetryInterval);
        }
        Ok(Self {
            max_connections,
            websocket_upgrade_timeout,
            activation_timeout,
            registration_timeout,
            write_timeout,
            release_retry_interval,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControllerEndpointConfigError {
    #[error("controller endpoint timeouts must be non-zero")]
    ZeroTimeout,
    #[error("controller release retry interval is below the safety floor")]
    InvalidReleaseRetryInterval,
}

/// Fixed-scope owner of relay authentication and global connection admission.
///
/// The verifier and the only reachable [`RelayOrchestrator`] are constructed
/// together and cannot be selected independently by a request or a caller.
/// A listener must acquire [`ControllerConnection`] before starting TLS so
/// slow TLS and HTTP handshakes consume the same bounded admission pool as
/// active bridge and worker drivers.
#[derive(Clone)]
pub struct ControllerEndpoint {
    relay: RelayOrchestrator,
    bridge_verifier: BridgeBearerVerifier,
    admission: ConnectionAdmission,
    config: ControllerEndpointConfig,
}

impl ControllerEndpoint {
    pub fn new(
        relay: RelayOrchestrator,
        bridge_credential: &[u8],
        config: ControllerEndpointConfig,
    ) -> Result<Self, ControllerEndpointBuildError> {
        Ok(Self {
            relay,
            bridge_verifier: BridgeBearerVerifier::new(bridge_credential)?,
            admission: ConnectionAdmission::new(config.max_connections),
            config,
        })
    }

    pub fn try_admit(&self) -> Result<ControllerConnection, ControllerAdmissionError> {
        let permit = self.admission.try_acquire()?;
        Ok(ControllerConnection {
            relay: self.relay.clone(),
            bridge_verifier: self.bridge_verifier.clone(),
            config: self.config,
            _permit: permit,
        })
    }
}

#[derive(Clone)]
struct ConnectionAdmission {
    permits: Arc<Semaphore>,
}

impl ConnectionAdmission {
    fn new(limit: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit.get())),
        }
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, ControllerAdmissionError> {
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| ControllerAdmissionError::AtCapacity)
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ControllerAdmissionError {
    #[error("controller connection admission is at capacity")]
    AtCapacity,
}

/// One admitted connection bound to its endpoint's exact verifier and relay.
pub struct ControllerConnection {
    relay: RelayOrchestrator,
    bridge_verifier: BridgeBearerVerifier,
    config: ControllerEndpointConfig,
    _permit: OwnedSemaphorePermit,
}

impl ControllerConnection {
    /// Upgrade and immediately dispatch one already-TLS-protected stream.
    pub(super) async fn serve<S>(
        self,
        stream: S,
    ) -> Result<ControllerConnectionOutcome, ControllerConnectionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let (socket, authority) = accept_authenticated_websocket(
            stream,
            self.bridge_verifier,
            self.config.websocket_upgrade_timeout,
        )
        .await
        .map_err(ControllerConnectionError::from)?;
        match authority {
            UpgradeAuthority::Bridge => serve_bridge_websocket(
                self.relay,
                socket,
                self.config.activation_timeout,
                self.config.write_timeout,
                self.config.release_retry_interval,
            )
            .await
            .map(ControllerConnectionOutcome::Bridge)
            .map_err(|source| ControllerConnectionError::Bridge(Box::new(source))),
            UpgradeAuthority::Worker(auth) => serve_worker_websocket(
                self.relay,
                socket,
                auth,
                self.config.registration_timeout,
                self.config.write_timeout,
            )
            .await
            .map(ControllerConnectionOutcome::Worker)
            .map_err(|source| ControllerConnectionError::Worker(Box::new(source))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControllerConnectionOutcome {
    Bridge(BridgeWebSocketOutcome),
    Worker(RelayLossOutcome),
}

#[derive(Debug, Error)]
pub enum ControllerConnectionError {
    #[error("controller WebSocket upgrade timed out")]
    UpgradeTimedOut,
    #[error("controller WebSocket upgrade failed")]
    UpgradeHandshake(#[source] Box<tungstenite::Error>),
    #[error("controller WebSocket upgrade did not produce authority")]
    MissingUpgradeAuthority,
    #[error("controller bridge connection failed")]
    Bridge(#[source] Box<ControllerBridgeWebSocketError>),
    #[error("controller worker connection failed")]
    Worker(#[source] Box<WorkerWebSocketError>),
}

impl ControllerConnectionError {
    pub(super) fn is_process_fatal(&self) -> bool {
        match self {
            Self::MissingUpgradeAuthority => true,
            Self::Bridge(source) => matches!(
                source.as_ref(),
                ControllerBridgeWebSocketError::UnsafeConfiguration
                    | ControllerBridgeWebSocketError::InvalidLifecycleRetryInterval
            ),
            Self::Worker(source) => {
                matches!(source.as_ref(), WorkerWebSocketError::UnsafeConfiguration)
            }
            Self::UpgradeTimedOut | Self::UpgradeHandshake(_) => false,
        }
    }
}

impl From<AuthenticatedUpgradeError> for ControllerConnectionError {
    fn from(error: AuthenticatedUpgradeError) -> Self {
        match error {
            AuthenticatedUpgradeError::TimedOut => Self::UpgradeTimedOut,
            AuthenticatedUpgradeError::Handshake(source) => Self::UpgradeHandshake(source),
            AuthenticatedUpgradeError::MissingAuthority => Self::MissingUpgradeAuthority,
        }
    }
}

/// Authenticate one HTTP request before returning an upgraded WebSocket.
///
/// The callback runs after tungstenite has validated the base WebSocket
/// handshake and before it writes HTTP 101. Worker authority returned here is
/// only bounded bootstrap material; [`super::serve_worker_websocket`] still
/// requires the first Registration frame and performs the authoritative
/// Kubernetes-backed one-time bootstrap verification.
async fn accept_authenticated_websocket<S>(
    stream: S,
    bridge_verifier: BridgeBearerVerifier,
    handshake_timeout: Duration,
) -> Result<(WebSocketStream<S>, UpgradeAuthority), AuthenticatedUpgradeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let authority = Arc::new(Mutex::new(None));
    let captured_authority = Arc::clone(&authority);
    // tungstenite's Callback contract fixes the error type as an unboxed
    // ErrorResponse; this boundary cannot make the Result variant smaller.
    #[allow(clippy::result_large_err)]
    let callback = move |request: &Request, response| {
        let accepted = authorize_websocket_upgrade(request, &bridge_verifier)
            .map_err(UpgradeRejection::into_response)?;
        let mut slot = captured_authority
            .lock()
            .map_err(|_| UpgradeRejection::Unavailable.into_response())?;
        if slot.is_some() {
            return Err(UpgradeRejection::Unavailable.into_response());
        }
        *slot = Some(accepted);
        Ok(response)
    };

    let socket = timeout(
        handshake_timeout,
        accept_hdr_async_with_config(stream, callback, Some(relay_websocket_config())),
    )
    .await
    .map_err(|_| AuthenticatedUpgradeError::TimedOut)?
    .map_err(|source| AuthenticatedUpgradeError::Handshake(Box::new(source)))?;
    let accepted = authority
        .lock()
        .ok()
        .and_then(|mut slot| slot.take())
        .ok_or(AuthenticatedUpgradeError::MissingAuthority)?;
    Ok((socket, accepted))
}

/// Authenticate and route one already-parsed WebSocket upgrade request.
///
/// This function is intentionally synchronous so it can run inside
/// tungstenite's HTTP handshake callback. It never consults peer JSON, the
/// network, or Kubernetes when selecting authority.
fn authorize_websocket_upgrade(
    request: &Request,
    bridge_verifier: &BridgeBearerVerifier,
) -> Result<UpgradeAuthority, UpgradeRejection> {
    let path = request.uri().path();
    if request.uri().query().is_some()
        || !matches!(path, BRIDGE_WEBSOCKET_PATH | WORKER_WEBSOCKET_PATH)
    {
        return Err(UpgradeRejection::NotFound);
    }
    if request.method() != Method::GET {
        return Err(UpgradeRejection::BadRequest);
    }
    if request.headers().contains_key(ORIGIN) {
        return Err(UpgradeRejection::Forbidden);
    }
    if request.headers().contains_key(SEC_WEBSOCKET_PROTOCOL) {
        return Err(UpgradeRejection::BadRequest);
    }

    match path {
        BRIDGE_WEBSOCKET_PATH => authorize_bridge(request.headers(), bridge_verifier),
        WORKER_WEBSOCKET_PATH => authorize_worker(request.headers()),
        _ => Err(UpgradeRejection::NotFound),
    }
}

fn authorize_bridge(
    headers: &HeaderMap,
    verifier: &BridgeBearerVerifier,
) -> Result<UpgradeAuthority, UpgradeRejection> {
    let credential = bearer_credential(headers).ok_or(UpgradeRejection::Unauthorized)?;
    if !verifier.accepts(credential) {
        return Err(UpgradeRejection::Unauthorized);
    }
    Ok(UpgradeAuthority::Bridge)
}

fn authorize_worker(headers: &HeaderMap) -> Result<UpgradeAuthority, UpgradeRejection> {
    let encoded_token = bearer_credential(headers).ok_or(UpgradeRejection::Unauthorized)?;
    if encoded_token.len() != WORKER_TOKEN_HEX_BYTES {
        return Err(UpgradeRejection::Unauthorized);
    }
    let mut token = [0_u8; WORKER_TOKEN_BYTES];
    if hex::decode_to_slice(encoded_token, &mut token).is_err() {
        token.fill(0);
        return Err(UpgradeRejection::Unauthorized);
    }

    let authority = single_header(headers, &WORKER_POD_UID_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(UpgradeRejection::Unauthorized)
        .and_then(|pod_uid| {
            WorkerBootstrapAuth::new(pod_uid, &token)
                .map(UpgradeAuthority::Worker)
                .map_err(|_| UpgradeRejection::Unauthorized)
        });
    token.fill(0);
    authority
}

fn bearer_credential(headers: &HeaderMap) -> Option<&[u8]> {
    single_header(headers, &AUTHORIZATION)?
        .as_bytes()
        .strip_prefix(BEARER_PREFIX)
}

fn single_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

#[cfg(test)]
mod tests;
