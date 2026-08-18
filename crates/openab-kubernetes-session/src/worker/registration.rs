//! Registration-first, one-shot worker activation.
//!
//! This module deliberately does not use tungstenite's client handshake
//! serializer. That serializer writes the complete HTTP request to TRACE and
//! stores it in an ordinary `Vec`, which is unsafe for the worker bootstrap
//! credential. The narrow handshake below owns one zeroizing plaintext
//! request buffer and hands the verified TLS stream to tungstenite only after
//! a strict, bounded HTTP 101 response has been accepted.

use super::bootstrap::{WorkerBootstrap, WorkerCommand};
use super::TerminationSignalError;
use crate::client_transport::{
    client_websocket_config, validate_worker_controller_url, worker_client_tls_config,
    ClientRequestError, PrivateCaError,
};
use crate::wire::{
    decode_frame, encode_frame, ControllerToWorkerV1, FatalCode, HandshakeOutcomeV1,
    WireProtocolError, WorkerRegistrationV1, WorkerToControllerV1, MAX_ACP_FRAME_BYTES,
    MAX_CONTROL_FRAME_BYTES,
};
use futures_util::{SinkExt, StreamExt};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep_until, Instant};
use tokio_tungstenite::tungstenite::handshake::{client::generate_key, derive_accept_key};
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use worker_tokio_rustls::client::TlsStream;
use worker_tokio_rustls::TlsConnector;
use zeroize::Zeroizing;

pub const REGISTRATION_ACK_TIMEOUT: Duration = Duration::from_secs(300);

const WORKER_TOKEN_HEX_BYTES: usize = 64;
const MAX_WORKER_UPGRADE_REQUEST_BYTES: usize = 4 * 1024;
const MAX_WORKER_UPGRADE_RESPONSE_BYTES: usize = 8 * 1024;
const MAX_WORKER_UPGRADE_RESPONSE_HEADERS: usize = 16;
const UPGRADE_READ_CHUNK_BYTES: usize = 1024;

pub type WorkerTlsStream = TlsStream<TcpStream>;

/// A non-reusable activation attempt. Its credential remains inside the
/// consumed bootstrap until verified TLS exists and the one HTTP request is
/// serialized.
pub struct WorkerRequest {
    bootstrap: WorkerBootstrap,
    connect_host: String,
    host_header: String,
    port: u16,
    server_name: ServerName<'static>,
    tls_config: Arc<ClientConfig>,
}

impl fmt::Debug for WorkerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerRequest")
            .field("bootstrap", &"<redacted>")
            .field("connect_host", &"<redacted>")
            .field("host_header", &"<redacted>")
            .field("port", &"<redacted>")
            .field("server_name", &"<redacted>")
            .field("tls_config", &"<redacted>")
            .finish()
    }
}

/// Capability returned only after a valid no-request-ID registration ACK.
/// Obtaining the child command and socket before that transition is
/// impossible through this API.
pub struct RegisteredWorker<S> {
    socket: WebSocketStream<S>,
    command: WorkerCommand,
}

impl<S> RegisteredWorker<S> {
    pub fn command(&self) -> &WorkerCommand {
        &self.command
    }

    pub(super) fn into_socket(self) -> WebSocketStream<S> {
        self.socket
    }

    #[cfg(test)]
    pub(super) fn for_test(socket: WebSocketStream<S>, command: WorkerCommand) -> Self {
        Self { socket, command }
    }
}

impl<S> fmt::Debug for RegisteredWorker<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredWorker")
            .field("socket", &"<redacted>")
            .field("command", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerRegistrationError {
    #[error(transparent)]
    Request(#[from] ClientRequestError),
    #[error(transparent)]
    Trust(#[from] PrivateCaError),
    #[error("worker TLS configuration is unsafe")]
    UnsafeTlsConfiguration,
    #[error("worker controller connection failed")]
    Connect,
    #[error("worker controller TLS verification failed")]
    Tls,
    #[error("worker upgrade request could not be constructed")]
    UpgradeRequest,
    #[error("worker upgrade request delivery was ambiguous")]
    UpgradeWrite,
    #[error("worker upgrade response could not be read")]
    UpgradeRead,
    #[error("worker upgrade response ended before its headers completed")]
    IncompleteUpgradeResponse,
    #[error("worker upgrade response exceeds its size limit")]
    UpgradeResponseTooLarge,
    #[error("worker upgrade response is invalid")]
    InvalidUpgradeResponse,
    #[error("worker WebSocket transport limits are unsafe")]
    UnsafeWebSocketConfiguration,
    #[error("worker registration could not be encoded")]
    OutboundFrame,
    #[error("worker registration JSON is not UTF-8")]
    OutboundUtf8,
    #[error("worker registration delivery was ambiguous")]
    RegistrationSend,
    #[error("worker registration timed out")]
    TimedOut,
    #[error("worker activation was cancelled by a termination signal")]
    Terminated,
    #[error("worker termination signal handling failed")]
    Signal(#[source] TerminationSignalError),
    #[error("worker WebSocket closed before registration acknowledgement")]
    ClosedBeforeAck,
    #[error("worker registration requires a text acknowledgement")]
    ExpectedTextAck,
    #[error("controller sent an invalid worker registration frame")]
    InvalidControllerFrame,
    #[error("controller sent ACP before worker registration completed")]
    AcpBeforeAck,
    #[error("controller rejected worker registration")]
    Rejected(FatalCode),
    #[error("worker WebSocket transport failed during registration")]
    Transport,
}

/// Consume one fully validated bootstrap into one non-cloneable request.
/// Credential bytes are intentionally not encoded until after TLS identity
/// verification succeeds.
pub fn build_worker_request(
    bootstrap: WorkerBootstrap,
) -> Result<WorkerRequest, WorkerRegistrationError> {
    validate_worker_controller_url(bootstrap.controller_url())?;
    let uri = bootstrap
        .controller_url()
        .parse::<Uri>()
        .map_err(|_| ClientRequestError::InvalidUrl)?;
    let authority = uri.authority().ok_or(ClientRequestError::InvalidUrl)?;
    let connect_host = worker_connect_host(authority.host())?;
    let host_header = authority.as_str().to_owned();
    let port = authority.port_u16().unwrap_or(443);
    let server_name =
        ServerName::try_from(connect_host.clone()).map_err(|_| ClientRequestError::InvalidUrl)?;

    let mut private_ca = Cursor::new(bootstrap.controller_ca_pem());
    let tls_config = worker_client_tls_config(&mut private_ca)?;
    if !tls_config.alpn_protocols.is_empty() {
        return Err(WorkerRegistrationError::UnsafeTlsConfiguration);
    }

    Ok(WorkerRequest {
        bootstrap,
        connect_host,
        host_header,
        port,
        server_name,
        tls_config,
    })
}

fn worker_connect_host(authority_host: &str) -> Result<String, ClientRequestError> {
    match authority_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        Some(host) if !host.is_empty() => Ok(host.to_owned()),
        Some(_) => Err(ClientRequestError::InvalidUrl),
        None => Ok(authority_host.to_owned()),
    }
}

/// Perform exactly one connection and registration attempt. Signal, timeout,
/// and every uncertain transport outcome consume the attempt and are
/// terminal; there is no reconnect or resend path.
pub async fn register_worker_once<Shutdown>(
    request: WorkerRequest,
    shutdown: Shutdown,
) -> Result<RegisteredWorker<WorkerTlsStream>, WorkerRegistrationError>
where
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + Send,
{
    let deadline = Instant::now() + REGISTRATION_ACK_TIMEOUT;
    tokio::pin!(shutdown);

    let pending = tokio::select! {
        biased;
        signal = shutdown.as_mut() => return Err(signal_error(signal)),
        _ = sleep_until(deadline) => return Err(WorkerRegistrationError::TimedOut),
        connected = connect_worker(request) => connected?,
    };

    await_registration_ack(
        pending.socket,
        pending.registration,
        pending.command,
        deadline,
        shutdown.as_mut(),
    )
    .await
}

struct PendingRegistration {
    socket: WebSocketStream<WorkerTlsStream>,
    registration: WorkerRegistrationV1,
    command: WorkerCommand,
}

async fn connect_worker(
    request: WorkerRequest,
) -> Result<PendingRegistration, WorkerRegistrationError> {
    let WorkerRequest {
        bootstrap,
        connect_host,
        host_header,
        port,
        server_name,
        tls_config,
    } = request;

    let tcp = TcpStream::connect((connect_host.as_str(), port))
        .await
        .map_err(|_| WorkerRegistrationError::Connect)?;
    let mut tls = TlsConnector::from(tls_config)
        .connect(server_name, tcp)
        .await
        .map_err(|_| WorkerRegistrationError::Tls)?;

    let key = generate_key();
    let expected_accept = derive_accept_key(key.as_bytes());
    let (plaintext_request, registration, command) =
        bootstrap.into_registration_request(|token, pod_uid| {
            encode_worker_upgrade_request(&host_header, &key, token, pod_uid)
        })?;

    tls.write_all(plaintext_request.as_slice())
        .await
        .map_err(|_| WorkerRegistrationError::UpgradeWrite)?;
    tls.flush()
        .await
        .map_err(|_| WorkerRegistrationError::UpgradeWrite)?;
    drop(plaintext_request);

    let tail = read_and_validate_upgrade_response(&mut tls, expected_accept.as_bytes()).await?;
    let socket = WebSocketStream::from_partially_read(
        tls,
        tail,
        Role::Client,
        Some(client_websocket_config()),
    )
    .await;

    Ok(PendingRegistration {
        socket,
        registration,
        command,
    })
}

fn encode_worker_upgrade_request(
    host_header: &str,
    key: &str,
    token: &[u8; 32],
    pod_uid: &str,
) -> Result<Zeroizing<Vec<u8>>, WorkerRegistrationError> {
    const PREFIX: &[u8] = b"GET /v1/worker HTTP/1.1\r\nHost: ";
    const WEBSOCKET_HEADERS: &[u8] = b"\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ";
    const AUTHORIZATION: &[u8] = b"\r\nAuthorization: Bearer ";
    const POD_UID: &[u8] = b"\r\nx-openab-pod-uid: ";
    const END: &[u8] = b"\r\n\r\n";

    if key.len() != 24 || !host_header.is_ascii() || !pod_uid.is_ascii() {
        return Err(WorkerRegistrationError::UpgradeRequest);
    }
    let length = PREFIX
        .len()
        .checked_add(host_header.len())
        .and_then(|length| length.checked_add(WEBSOCKET_HEADERS.len()))
        .and_then(|length| length.checked_add(key.len()))
        .and_then(|length| length.checked_add(AUTHORIZATION.len()))
        .and_then(|length| length.checked_add(WORKER_TOKEN_HEX_BYTES))
        .and_then(|length| length.checked_add(POD_UID.len()))
        .and_then(|length| length.checked_add(pod_uid.len()))
        .and_then(|length| length.checked_add(END.len()))
        .filter(|length| *length <= MAX_WORKER_UPGRADE_REQUEST_BYTES)
        .ok_or(WorkerRegistrationError::UpgradeRequest)?;

    let mut encoded_token = Zeroizing::new([0_u8; WORKER_TOKEN_HEX_BYTES]);
    hex::encode_to_slice(token, encoded_token.as_mut())
        .map_err(|_| WorkerRegistrationError::UpgradeRequest)?;
    let mut request = Zeroizing::new(Vec::with_capacity(length));
    let allocation = request.as_ptr();
    for bytes in [
        PREFIX,
        host_header.as_bytes(),
        WEBSOCKET_HEADERS,
        key.as_bytes(),
        AUTHORIZATION,
        encoded_token.as_slice(),
        POD_UID,
        pod_uid.as_bytes(),
        END,
    ] {
        request.extend_from_slice(bytes);
    }
    if request.len() != length || request.as_ptr() != allocation {
        return Err(WorkerRegistrationError::UpgradeRequest);
    }
    Ok(request)
}

async fn read_and_validate_upgrade_response<S>(
    stream: &mut S,
    expected_accept: &[u8],
) -> Result<Vec<u8>, WorkerRegistrationError>
where
    S: AsyncRead + Unpin,
{
    let mut response = Zeroizing::new(Vec::with_capacity(MAX_WORKER_UPGRADE_RESPONSE_BYTES));
    let mut chunk = Zeroizing::new([0_u8; UPGRADE_READ_CHUNK_BYTES]);

    loop {
        if let Some(header_end) = find_header_end(response.as_slice()) {
            validate_upgrade_response(&response[..header_end], expected_accept)?;
            return Ok(response[header_end..].to_vec());
        }
        let remaining = MAX_WORKER_UPGRADE_RESPONSE_BYTES.saturating_sub(response.len());
        if remaining == 0 {
            return Err(WorkerRegistrationError::UpgradeResponseTooLarge);
        }
        let read_limit = remaining.min(chunk.len());
        let read = stream
            .read(&mut chunk[..read_limit])
            .await
            .map_err(|_| WorkerRegistrationError::UpgradeRead)?;
        if read == 0 {
            return Err(WorkerRegistrationError::IncompleteUpgradeResponse);
        }
        response.extend_from_slice(&chunk[..read]);
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn validate_upgrade_response(
    bytes: &[u8],
    expected_accept: &[u8],
) -> Result<(), WorkerRegistrationError> {
    if !bytes.starts_with(b"HTTP/1.1 ") || !has_only_crlf_line_endings(bytes) {
        return Err(WorkerRegistrationError::InvalidUpgradeResponse);
    }

    let mut headers = [httparse::EMPTY_HEADER; MAX_WORKER_UPGRADE_RESPONSE_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let parsed = response
        .parse(bytes)
        .map_err(|_| WorkerRegistrationError::InvalidUpgradeResponse)?;
    if parsed != httparse::Status::Complete(bytes.len())
        || response.version != Some(1)
        || response.code != Some(101)
    {
        return Err(WorkerRegistrationError::InvalidUpgradeResponse);
    }

    let mut upgrade = 0_u8;
    let mut connection = 0_u8;
    let mut accept = 0_u8;
    for header in response.headers.iter() {
        if header.name.eq_ignore_ascii_case("upgrade") {
            upgrade = upgrade.saturating_add(1);
            if !header.value.eq_ignore_ascii_case(b"websocket") {
                return Err(WorkerRegistrationError::InvalidUpgradeResponse);
            }
        } else if header.name.eq_ignore_ascii_case("connection") {
            connection = connection.saturating_add(1);
            if !header.value.eq_ignore_ascii_case(b"upgrade") {
                return Err(WorkerRegistrationError::InvalidUpgradeResponse);
            }
        } else if header.name.eq_ignore_ascii_case("sec-websocket-accept") {
            accept = accept.saturating_add(1);
            if header.value != expected_accept {
                return Err(WorkerRegistrationError::InvalidUpgradeResponse);
            }
        } else {
            return Err(WorkerRegistrationError::InvalidUpgradeResponse);
        }
    }
    if (upgrade, connection, accept) != (1, 1, 1) {
        return Err(WorkerRegistrationError::InvalidUpgradeResponse);
    }
    Ok(())
}

fn has_only_crlf_line_endings(bytes: &[u8]) -> bool {
    bytes.iter().enumerate().all(|(index, byte)| match byte {
        b'\n' => index > 0 && bytes[index - 1] == b'\r',
        b'\r' => bytes.get(index + 1) == Some(&b'\n'),
        _ => true,
    })
}

async fn await_registration_ack<S, Shutdown>(
    mut socket: WebSocketStream<S>,
    registration: WorkerRegistrationV1,
    command: WorkerCommand,
    deadline: Instant,
    mut shutdown: Pin<&mut Shutdown>,
) -> Result<RegisteredWorker<S>, WorkerRegistrationError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Shutdown: Future<Output = Result<(), TerminationSignalError>> + ?Sized,
{
    if !safe_worker_websocket_config(socket.get_config()) {
        return Err(WorkerRegistrationError::UnsafeWebSocketConfiguration);
    }

    let frame = WorkerToControllerV1::Registration(registration);
    let bytes = encode_frame(&frame).map_err(map_outbound_frame)?;
    let text = String::from_utf8(bytes).map_err(map_outbound_utf8)?;
    tokio::select! {
        biased;
        signal = shutdown.as_mut() => return Err(signal_error(signal)),
        _ = sleep_until(deadline) => return Err(WorkerRegistrationError::TimedOut),
        sent = socket.send(Message::Text(text)) => {
            sent.map_err(|_| WorkerRegistrationError::RegistrationSend)?;
        }
    }

    loop {
        let frame = tokio::select! {
            biased;
            signal = shutdown.as_mut() => return Err(signal_error(signal)),
            _ = sleep_until(deadline) => return Err(WorkerRegistrationError::TimedOut),
            frame = socket.next() => frame,
        };
        let frame = frame
            .ok_or(WorkerRegistrationError::ClosedBeforeAck)?
            .map_err(|_| WorkerRegistrationError::Transport)?;
        match frame {
            Message::Text(text) => {
                let message = decode_frame::<ControllerToWorkerV1>(text.as_bytes())
                    .map_err(|_| WorkerRegistrationError::InvalidControllerFrame)?;
                match message {
                    ControllerToWorkerV1::ProtocolResult(result) => {
                        return match result
                            .into_handshake_outcome()
                            .map_err(|_| WorkerRegistrationError::InvalidControllerFrame)?
                        {
                            HandshakeOutcomeV1::Ack => Ok(RegisteredWorker { socket, command }),
                            HandshakeOutcomeV1::Fatal(code) => {
                                Err(WorkerRegistrationError::Rejected(code))
                            }
                        };
                    }
                    ControllerToWorkerV1::Acp(_) => {
                        return Err(WorkerRegistrationError::AcpBeforeAck)
                    }
                }
            }
            Message::Ping(_) => {
                tokio::select! {
                    biased;
                    signal = shutdown.as_mut() => return Err(signal_error(signal)),
                    _ = sleep_until(deadline) => return Err(WorkerRegistrationError::TimedOut),
                    flushed = socket.flush() => {
                        flushed.map_err(|_| WorkerRegistrationError::Transport)?;
                    }
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => return Err(WorkerRegistrationError::ClosedBeforeAck),
            Message::Binary(_) | Message::Frame(_) => {
                return Err(WorkerRegistrationError::ExpectedTextAck)
            }
        }
    }
}

fn safe_worker_websocket_config(config: &WebSocketConfig) -> bool {
    config.max_message_size == Some(MAX_ACP_FRAME_BYTES)
        && config.max_frame_size == Some(MAX_ACP_FRAME_BYTES)
        && !config.accept_unmasked_frames
        && config.write_buffer_size == 0
        && config.max_write_buffer_size == MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
}

fn signal_error(result: Result<(), TerminationSignalError>) -> WorkerRegistrationError {
    match result {
        Ok(()) => WorkerRegistrationError::Terminated,
        Err(error) => WorkerRegistrationError::Signal(error),
    }
}

fn map_outbound_frame(_error: WireProtocolError) -> WorkerRegistrationError {
    WorkerRegistrationError::OutboundFrame
}

fn map_outbound_utf8(_error: FromUtf8Error) -> WorkerRegistrationError {
    WorkerRegistrationError::OutboundUtf8
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::SessionBinding;
    use crate::identity::{ScopeId, SessionId};
    use crate::state::Fence;
    use crate::wire::{AcpMessageV1, ProtocolResultV1};
    use serde_json::json;
    use std::ffi::OsString;
    use std::future;
    use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
    use tokio::sync::oneshot;
    use tokio::time::advance;

    type TestSocket = WebSocketStream<DuplexStream>;

    fn registration() -> WorkerRegistrationV1 {
        let binding = SessionBinding::new(
            ScopeId::derive("team-a"),
            SessionId::derive("team-a", "discord:thread-a"),
            Fence::new(7, uuid::Uuid::from_u128(100)).unwrap(),
            uuid::Uuid::from_u128(200),
        )
        .unwrap();
        WorkerRegistrationV1::new(&binding)
    }

    fn command() -> WorkerCommand {
        WorkerCommand::parse([
            OsString::from("serve"),
            OsString::from("--"),
            OsString::from("/usr/local/bin/acp-test"),
        ])
        .unwrap()
    }

    async fn socket_pair() -> (TestSocket, TestSocket) {
        let (worker_io, controller_io) = duplex(256 * 1024);
        let worker = WebSocketStream::from_raw_socket(
            worker_io,
            Role::Client,
            Some(client_websocket_config()),
        )
        .await;
        let controller = WebSocketStream::from_raw_socket(
            controller_io,
            Role::Server,
            Some(client_websocket_config()),
        )
        .await;
        (worker, controller)
    }

    fn text<M: crate::wire::WireMessage>(message: &M) -> Message {
        Message::Text(String::from_utf8(encode_frame(message).unwrap()).unwrap())
    }

    async fn run_controller_reply(reply: Message) -> WorkerRegistrationError {
        let (worker, mut controller) = socket_pair().await;
        let worker_task = tokio::spawn(async move {
            let mut shutdown = future::pending::<Result<(), TerminationSignalError>>();
            await_registration_ack(
                worker,
                registration(),
                command(),
                Instant::now() + REGISTRATION_ACK_TIMEOUT,
                Pin::new(&mut shutdown),
            )
            .await
        });

        let outbound = controller.next().await.unwrap().unwrap();
        let Message::Text(outbound) = outbound else {
            panic!("registration must be text")
        };
        assert!(matches!(
            decode_frame::<WorkerToControllerV1>(outbound.as_bytes()).unwrap(),
            WorkerToControllerV1::Registration(_)
        ));
        controller.send(reply).await.unwrap();
        worker_task.await.unwrap().unwrap_err()
    }

    #[tokio::test]
    async fn only_a_no_request_id_ack_releases_the_registered_capability() {
        let (worker, mut controller) = socket_pair().await;
        let worker_task = tokio::spawn(async move {
            let mut shutdown = future::pending::<Result<(), TerminationSignalError>>();
            await_registration_ack(
                worker,
                registration(),
                command(),
                Instant::now() + REGISTRATION_ACK_TIMEOUT,
                Pin::new(&mut shutdown),
            )
            .await
        });
        let _registration = controller.next().await.unwrap().unwrap();
        controller
            .send(text(&ControllerToWorkerV1::ProtocolResult(
                ProtocolResultV1::ack(None).unwrap(),
            )))
            .await
            .unwrap();
        let registered = worker_task.await.unwrap().unwrap();
        assert_eq!(
            registered.command().executable().to_str(),
            Some("/usr/local/bin/acp-test")
        );
        assert!(!format!("{registered:?}").contains("acp-test"));
    }

    #[tokio::test]
    async fn every_sanitized_fatal_code_is_terminal() {
        for code in [
            FatalCode::InvalidMessage,
            FatalCode::Unauthorized,
            FatalCode::StaleBinding,
            FatalCode::Unavailable,
            FatalCode::Internal,
        ] {
            let error = run_controller_reply(text(&ControllerToWorkerV1::ProtocolResult(
                ProtocolResultV1::fatal(None, code).unwrap(),
            )))
            .await;
            assert_eq!(error, WorkerRegistrationError::Rejected(code));
        }
    }

    #[tokio::test]
    async fn invalid_application_frames_are_terminal() {
        let acp = ControllerToWorkerV1::Acp(
            AcpMessageV1::new(json!({"jsonrpc": "2.0", "method": "unsafe"})).unwrap(),
        );
        assert_eq!(
            run_controller_reply(text(&acp)).await,
            WorkerRegistrationError::AcpBeforeAck
        );

        let correlated = ControllerToWorkerV1::ProtocolResult(
            ProtocolResultV1::ack(Some(uuid::Uuid::from_u128(10))).unwrap(),
        );
        assert_eq!(
            run_controller_reply(text(&correlated)).await,
            WorkerRegistrationError::InvalidControllerFrame
        );
        assert_eq!(
            run_controller_reply(Message::Text("not-json".to_owned())).await,
            WorkerRegistrationError::InvalidControllerFrame
        );
        assert_eq!(
            run_controller_reply(Message::Binary(b"not-text".to_vec())).await,
            WorkerRegistrationError::ExpectedTextAck
        );

        let mut oversized = String::from_utf8(
            encode_frame(&ControllerToWorkerV1::ProtocolResult(
                ProtocolResultV1::ack(None).unwrap(),
            ))
            .unwrap(),
        )
        .unwrap();
        oversized.extend(std::iter::repeat_n(
            ' ',
            MAX_CONTROL_FRAME_BYTES + 1 - oversized.len(),
        ));
        assert_eq!(
            run_controller_reply(Message::Text(oversized)).await,
            WorkerRegistrationError::InvalidControllerFrame
        );
    }

    #[tokio::test]
    async fn close_and_ambiguous_send_are_terminal() {
        assert_eq!(
            run_controller_reply(Message::Close(None)).await,
            WorkerRegistrationError::ClosedBeforeAck
        );

        let (worker, controller) = socket_pair().await;
        drop(controller);
        let mut shutdown = future::pending::<Result<(), TerminationSignalError>>();
        let error = await_registration_ack(
            worker,
            registration(),
            command(),
            Instant::now() + REGISTRATION_ACK_TIMEOUT,
            Pin::new(&mut shutdown),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            WorkerRegistrationError::RegistrationSend
                | WorkerRegistrationError::ClosedBeforeAck
                | WorkerRegistrationError::Transport
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn ping_and_pong_do_not_extend_the_fixed_deadline() {
        let (worker, mut controller) = socket_pair().await;
        let worker_task = tokio::spawn(async move {
            let mut shutdown = future::pending::<Result<(), TerminationSignalError>>();
            await_registration_ack(
                worker,
                registration(),
                command(),
                Instant::now() + REGISTRATION_ACK_TIMEOUT,
                Pin::new(&mut shutdown),
            )
            .await
        });
        let _registration = controller.next().await.unwrap().unwrap();

        for _ in 0..3 {
            advance(Duration::from_secs(99)).await;
            controller.send(Message::Ping(vec![1])).await.unwrap();
            assert!(matches!(
                controller.next().await.unwrap().unwrap(),
                Message::Pong(_)
            ));
            controller.send(Message::Pong(vec![2])).await.unwrap();
            assert!(!worker_task.is_finished());
        }
        advance(Duration::from_secs(3)).await;
        assert_eq!(
            worker_task.await.unwrap().unwrap_err(),
            WorkerRegistrationError::TimedOut
        );
    }

    #[tokio::test]
    async fn signal_during_ack_wait_is_terminal() {
        let (worker, mut controller) = socket_pair().await;
        let (signal_tx, signal_rx) = oneshot::channel();
        let worker_task = tokio::spawn(async move {
            let mut shutdown = Box::pin(async move {
                signal_rx.await.unwrap();
                Ok(())
            });
            await_registration_ack(
                worker,
                registration(),
                command(),
                Instant::now() + REGISTRATION_ACK_TIMEOUT,
                shutdown.as_mut(),
            )
            .await
        });
        let _registration = controller.next().await.unwrap().unwrap();
        signal_tx.send(()).unwrap();
        assert_eq!(
            worker_task.await.unwrap().unwrap_err(),
            WorkerRegistrationError::Terminated
        );
    }

    fn valid_upgrade_response(key: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
            derive_accept_key(key.as_bytes())
        )
        .into_bytes()
    }

    #[test]
    fn plaintext_upgrade_request_is_exact_and_bounded() {
        let token = [0xab; 32];
        let request = encode_worker_upgrade_request(
            "controller.example.test:8443",
            "dGhlIHNhbXBsZSBub25jZQ==",
            &token,
            "pod-uid",
        )
        .unwrap();
        let expected = concat!(
            "GET /v1/worker HTTP/1.1\r\n",
            "Host: controller.example.test:8443\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "Authorization: Bearer abababababababababababababababababababababababababababababababab\r\n",
            "x-openab-pod-uid: pod-uid\r\n",
            "\r\n"
        );
        assert_eq!(request.as_slice(), expected.as_bytes());
        assert!(request.len() <= MAX_WORKER_UPGRADE_REQUEST_BYTES);
    }

    #[test]
    fn ipv6_authority_keeps_brackets_only_in_the_host_header() {
        let uri = "wss://[2001:db8::1]:8443/v1/worker".parse::<Uri>().unwrap();
        let authority = uri.authority().unwrap();
        let connect_host = worker_connect_host(authority.host()).unwrap();

        assert_eq!(connect_host, "2001:db8::1");
        assert_eq!(authority.as_str(), "[2001:db8::1]:8443");
        assert!(matches!(
            ServerName::try_from(connect_host).unwrap(),
            ServerName::IpAddress(_)
        ));
    }

    #[test]
    fn worker_build_compiles_dependency_payload_logs_out() {
        assert_eq!(log::STATIC_MAX_LEVEL, log::LevelFilter::Info);
    }

    #[test]
    fn strict_upgrade_response_rejects_ambiguity_and_extra_headers() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = derive_accept_key(key.as_bytes());
        assert!(validate_upgrade_response(&valid_upgrade_response(key), accept.as_bytes()).is_ok());

        for invalid in [
            format!(
                "HTTP/1.0 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            ),
            format!(
                "HTTP/1.1 200 OK\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            ),
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            ),
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Extensions: unsafe\r\n\r\n"
            ),
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\nContent-Length: 0\r\n\r\n"
            ),
            format!(
                "HTTP/1.1 101 Switching Protocols\nConnection: Upgrade\nUpgrade: websocket\nSec-WebSocket-Accept: {accept}\n\n"
            ),
        ] {
            assert_eq!(
                validate_upgrade_response(invalid.as_bytes(), accept.as_bytes()),
                Err(WorkerRegistrationError::InvalidUpgradeResponse)
            );
        }
    }

    #[tokio::test]
    async fn response_reader_preserves_websocket_tail_and_bounds_incomplete_headers() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = derive_accept_key(key.as_bytes());
        let mut input = valid_upgrade_response(key);
        let ping_frame = [0x89, 0x01, 0x07];
        input.extend_from_slice(&ping_frame);
        let mut reader = input.as_slice();
        let tail = read_and_validate_upgrade_response(&mut reader, accept.as_bytes())
            .await
            .unwrap();
        assert_eq!(tail, ping_frame);
        let (client_io, _server_io) = duplex(1024);
        let mut socket = WebSocketStream::from_partially_read(
            client_io,
            tail,
            Role::Client,
            Some(client_websocket_config()),
        )
        .await;
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Ping(vec![7])
        );

        let suffix = format!(
            "\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        let prefix = "HTTP/1.1 101 ";
        let padding = MAX_WORKER_UPGRADE_RESPONSE_BYTES - prefix.len() - suffix.len();
        let exact = format!("{prefix}{}{suffix}", "x".repeat(padding));
        assert_eq!(exact.len(), MAX_WORKER_UPGRADE_RESPONSE_BYTES);
        let mut exact_reader = exact.as_bytes();
        assert!(
            read_and_validate_upgrade_response(&mut exact_reader, accept.as_bytes())
                .await
                .unwrap()
                .is_empty()
        );

        let (mut writer, mut reader) = duplex(MAX_WORKER_UPGRADE_RESPONSE_BYTES + 1);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&vec![b'x'; MAX_WORKER_UPGRADE_RESPONSE_BYTES + 1])
                .await
                .unwrap();
        });
        assert_eq!(
            read_and_validate_upgrade_response(&mut reader, accept.as_bytes())
                .await
                .unwrap_err(),
            WorkerRegistrationError::UpgradeResponseTooLarge
        );
        writer_task.await.unwrap();
    }
}
