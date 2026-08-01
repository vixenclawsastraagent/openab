use super::{
    ControllerServiceError, RelayAcpDeliveryOutcome, RelayAttachment, RelayConnection,
    RelayDeliveryError, RelayLossOutcome, RelayOpenError, RelayOrchestrator, RelayOutboundItem,
    WorkerBootstrapAuth,
};
use crate::wire::{
    decode_frame, encode_frame, AcpMessageV1, ControllerToWorkerV1, FatalCode, ProtocolResultV1,
    WorkerRegistrationV1, WorkerToControllerV1, MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::string::FromUtf8Error;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::{self, protocol::WebSocketConfig};
use tokio_tungstenite::WebSocketStream;

/// Finite transport limits for an already-upgraded worker relay socket.
///
/// The HTTP/TLS endpoint must apply this configuration when it constructs the
/// [`WebSocketStream`]. Authentication remains outside WebSocket application
/// frames and is passed to [`serve_worker_websocket`] as trusted bootstrap
/// material.
pub fn worker_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: 0,
        max_write_buffer_size: MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES,
        max_message_size: Some(MAX_ACP_FRAME_BYTES),
        max_frame_size: Some(MAX_ACP_FRAME_BYTES),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

#[derive(Debug, Error)]
pub enum WorkerWebSocketError {
    #[error("worker WebSocket transport limits do not match the relay protocol")]
    UnsafeConfiguration,
    #[error("worker WebSocket registration timed out")]
    RegistrationTimedOut,
    #[error("worker WebSocket closed before registration")]
    ClosedBeforeRegistration,
    #[error("worker WebSocket transport failed")]
    Transport(#[source] tungstenite::Error),
    #[error("worker WebSocket requires a text registration as its first application frame")]
    ExpectedTextRegistration,
    #[error("worker WebSocket registration frame is invalid")]
    InvalidRegistrationFrame(#[source] crate::wire::WireProtocolError),
    #[error("worker WebSocket first application frame is not a registration")]
    ExpectedRegistration,
    #[error("worker relay registration failed")]
    Registration(#[source] RelayOpenError),
    #[error("worker WebSocket fatal result for {rejection:?} could not be delivered")]
    FatalDelivery {
        rejection: FatalCode,
        #[source]
        source: tungstenite::Error,
    },
    #[error("worker WebSocket fatal result for {rejection:?} timed out")]
    FatalDeliveryTimedOut { rejection: FatalCode },
    #[error("worker WebSocket could not close after rejecting {rejection:?}")]
    HandshakeClose {
        rejection: FatalCode,
        #[source]
        source: tungstenite::Error,
    },
    #[error("worker WebSocket close after rejecting {rejection:?} timed out")]
    HandshakeCloseTimedOut { rejection: FatalCode },
    #[error("worker WebSocket received a duplicate registration")]
    DuplicateRegistration,
    #[error("worker WebSocket requires text ACP frames after registration")]
    ExpectedTextAcp,
    #[error("worker WebSocket ACP frame is invalid")]
    InvalidAcpFrame(#[source] crate::wire::WireProtocolError),
    #[error("worker relay delivery failed")]
    Delivery(#[source] RelayDeliveryError),
    #[error("worker relay outbound frame is invalid")]
    OutboundFrame(#[source] crate::wire::WireProtocolError),
    #[error("worker relay outbound JSON is not UTF-8")]
    OutboundUtf8(#[source] FromUtf8Error),
    #[error("worker WebSocket writer stopped")]
    WriterStopped,
    #[error("worker WebSocket write timed out")]
    WriteTimedOut,
    #[error("worker relay containment failed")]
    Containment(#[source] ControllerServiceError),
}

/// Drive one already-upgraded, transport-authenticated worker connection.
///
/// The caller owns HTTP upgrade, TLS, endpoint authentication, and extraction
/// of the bounded bootstrap material. This function owns registration-first
/// application framing, exact ACP routing, backpressure, and containment. The
/// registration deadline spans all pre-registration control frames, while the
/// write deadline applies independently to each bounded transport write.
pub async fn serve_worker_websocket<S>(
    relay: RelayOrchestrator,
    mut socket: WebSocketStream<S>,
    auth: WorkerBootstrapAuth,
    registration_timeout: Duration,
    write_timeout: Duration,
) -> Result<RelayLossOutcome, WorkerWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    if !safe_worker_websocket_config(socket.get_config()) {
        return Err(reject_handshake(
            &mut socket,
            WorkerWebSocketError::UnsafeConfiguration,
            write_timeout,
        )
        .await);
    }

    let registration = match timeout(registration_timeout, read_registration(&mut socket)).await {
        Ok(Ok(registration)) => registration,
        Ok(Err(error @ WorkerWebSocketError::ClosedBeforeRegistration))
        | Ok(Err(error @ WorkerWebSocketError::Transport(_))) => return Err(error),
        Ok(Err(error)) => return Err(reject_handshake(&mut socket, error, write_timeout).await),
        Err(_) => {
            return Err(reject_handshake(
                &mut socket,
                WorkerWebSocketError::RegistrationTimedOut,
                write_timeout,
            )
            .await)
        }
    };

    let attachment = match relay.register_worker(registration, auth).await {
        Ok(attachment) => attachment,
        Err(error) => {
            return Err(reject_handshake(
                &mut socket,
                WorkerWebSocketError::Registration(error),
                write_timeout,
            )
            .await)
        }
    };
    serve_registered_worker(relay, socket, attachment, write_timeout).await
}

fn safe_worker_websocket_config(config: &WebSocketConfig) -> bool {
    config.max_message_size == Some(MAX_ACP_FRAME_BYTES)
        && config.max_frame_size == Some(MAX_ACP_FRAME_BYTES)
        && !config.accept_unmasked_frames
        && config.write_buffer_size == 0
        && config.max_write_buffer_size == MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
}

async fn read_registration<S>(
    socket: &mut WebSocketStream<S>,
) -> Result<WorkerRegistrationV1, WorkerWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let frame = socket
            .next()
            .await
            .ok_or(WorkerWebSocketError::ClosedBeforeRegistration)?
            .map_err(WorkerWebSocketError::Transport)?;
        match frame {
            Message::Text(text) => {
                return match decode_frame(text.as_bytes())
                    .map_err(WorkerWebSocketError::InvalidRegistrationFrame)?
                {
                    WorkerToControllerV1::Registration(registration) => Ok(registration),
                    WorkerToControllerV1::Acp(_) => Err(WorkerWebSocketError::ExpectedRegistration),
                }
            }
            Message::Ping(_) => socket
                .flush()
                .await
                .map_err(WorkerWebSocketError::Transport)?,
            Message::Pong(_) => {}
            Message::Close(_) => return Err(WorkerWebSocketError::ClosedBeforeRegistration),
            Message::Binary(_) | Message::Frame(_) => {
                return Err(WorkerWebSocketError::ExpectedTextRegistration)
            }
        }
    }
}

async fn reject_handshake<S>(
    socket: &mut WebSocketStream<S>,
    error: WorkerWebSocketError,
    write_timeout: Duration,
) -> WorkerWebSocketError
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(rejection) = handshake_fatal_code(&error) else {
        return error;
    };
    let result = match ProtocolResultV1::fatal(None, rejection) {
        Ok(result) => result,
        Err(source) => return WorkerWebSocketError::OutboundFrame(source),
    };
    let bytes = match encode_frame(&ControllerToWorkerV1::ProtocolResult(result)) {
        Ok(bytes) => bytes,
        Err(source) => return WorkerWebSocketError::OutboundFrame(source),
    };
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(source) => return WorkerWebSocketError::OutboundUtf8(source),
    };
    match timeout(write_timeout, socket.send(Message::Text(text))).await {
        Ok(Ok(())) => {}
        Ok(Err(source)) => return WorkerWebSocketError::FatalDelivery { rejection, source },
        Err(_) => return WorkerWebSocketError::FatalDeliveryTimedOut { rejection },
    }
    match timeout(write_timeout, socket.close(None)).await {
        Ok(Ok(())) => {}
        Ok(Err(source)) => return WorkerWebSocketError::HandshakeClose { rejection, source },
        Err(_) => return WorkerWebSocketError::HandshakeCloseTimedOut { rejection },
    }
    error
}

fn handshake_fatal_code(error: &WorkerWebSocketError) -> Option<FatalCode> {
    match error {
        WorkerWebSocketError::UnsafeConfiguration => Some(FatalCode::Internal),
        WorkerWebSocketError::RegistrationTimedOut => Some(FatalCode::Unavailable),
        WorkerWebSocketError::ExpectedTextRegistration
        | WorkerWebSocketError::InvalidRegistrationFrame(_)
        | WorkerWebSocketError::ExpectedRegistration => Some(FatalCode::InvalidMessage),
        WorkerWebSocketError::Registration(error) => Some(error.fatal_code()),
        WorkerWebSocketError::ClosedBeforeRegistration
        | WorkerWebSocketError::Transport(_)
        | WorkerWebSocketError::FatalDelivery { .. }
        | WorkerWebSocketError::FatalDeliveryTimedOut { .. }
        | WorkerWebSocketError::HandshakeClose { .. }
        | WorkerWebSocketError::HandshakeCloseTimedOut { .. }
        | WorkerWebSocketError::DuplicateRegistration
        | WorkerWebSocketError::ExpectedTextAcp
        | WorkerWebSocketError::InvalidAcpFrame(_)
        | WorkerWebSocketError::Delivery(_)
        | WorkerWebSocketError::OutboundFrame(_)
        | WorkerWebSocketError::OutboundUtf8(_)
        | WorkerWebSocketError::WriterStopped
        | WorkerWebSocketError::WriteTimedOut
        | WorkerWebSocketError::Containment(_) => None,
    }
}

async fn serve_registered_worker<S>(
    relay: RelayOrchestrator,
    socket: WebSocketStream<S>,
    mut attachment: RelayAttachment<ControllerToWorkerV1>,
    write_timeout: Duration,
) -> Result<RelayLossOutcome, WorkerWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let connection = attachment.connection().clone();
    let mut quiesced = attachment.quiesced();
    let connection_result = if *quiesced.borrow() {
        drop(socket);
        Ok(())
    } else {
        let (flush_requests, flush_request_receiver) = mpsc::channel(1);
        let (sink, stream) = socket.split();
        let reader = read_worker_frames(relay.clone(), connection, stream, flush_requests);
        let writer = write_worker_frames(
            sink,
            attachment.outbound(),
            flush_request_receiver,
            write_timeout,
        );
        tokio::pin!(reader, writer);
        tokio::select! {
            biased;
            _ = quiesced.changed() => Ok(()),
            result = &mut reader => result,
            result = &mut writer => result,
        }
    };

    let loss = relay
        .connection_lost(attachment)
        .await
        .map_err(WorkerWebSocketError::Containment)?;
    connection_result.map(|_| loss)
}

async fn read_worker_frames<R>(
    relay: RelayOrchestrator,
    connection: RelayConnection,
    mut stream: R,
    flush_requests: mpsc::Sender<()>,
) -> Result<(), WorkerWebSocketError>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    // After registration, a protocol violation exits immediately so the
    // common epilogue can fence the exact lane before any courtesy socket I/O.
    while let Some(frame) = stream.next().await {
        match frame.map_err(WorkerWebSocketError::Transport)? {
            Message::Text(text) => {
                let message =
                    decode_frame(text.as_bytes()).map_err(WorkerWebSocketError::InvalidAcpFrame)?;
                match message {
                    WorkerToControllerV1::Registration(_) => {
                        return Err(WorkerWebSocketError::DuplicateRegistration)
                    }
                    WorkerToControllerV1::Acp(message) => {
                        if route_worker_acp(&relay, &connection, message).await? {
                            return Ok(());
                        }
                    }
                }
            }
            Message::Ping(_) => match flush_requests.try_send(()) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(WorkerWebSocketError::WriterStopped)
                }
            },
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            Message::Binary(_) | Message::Frame(_) => {
                return Err(WorkerWebSocketError::ExpectedTextAcp)
            }
        }
    }
    Ok(())
}

async fn route_worker_acp(
    relay: &RelayOrchestrator,
    connection: &RelayConnection,
    mut message: AcpMessageV1,
) -> Result<bool, WorkerWebSocketError> {
    loop {
        match relay
            .route_acp(connection, message)
            .await
            .map_err(WorkerWebSocketError::Delivery)?
        {
            RelayAcpDeliveryOutcome::Delivered => return Ok(false),
            RelayAcpDeliveryOutcome::Backpressured {
                message: retained, ..
            } => {
                relay
                    .wait_for_route_capacity(connection, &retained)
                    .await
                    .map_err(WorkerWebSocketError::Delivery)?;
                message = retained;
            }
            RelayAcpDeliveryOutcome::PeerContained(_) => return Ok(true),
        }
    }
}

async fn write_worker_frames<W>(
    mut sink: W,
    outbound: &mut mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>,
    mut flush_requests: mpsc::Receiver<()>,
    write_timeout: Duration,
) -> Result<(), WorkerWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let mut flush_requests_open = true;
    loop {
        tokio::select! {
            biased;
            item = outbound.recv() => {
                let Some(item) = item else {
                    return Ok(());
                };
                let frame = item
                    .into_encoded_frame()
                    .map_err(WorkerWebSocketError::OutboundFrame)?;
                let (bytes, write_guard) = frame.into_write_parts();
                let text = String::from_utf8(bytes)
                    .map_err(WorkerWebSocketError::OutboundUtf8)?;
                match timeout(write_timeout, sink.send(Message::Text(text))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) => return Err(WorkerWebSocketError::Transport(source)),
                    Err(_) => return Err(WorkerWebSocketError::WriteTimedOut),
                }
                write_guard.mark_written();
            }
            request = flush_requests.recv(), if flush_requests_open => {
                match request {
                    Some(()) => {
                        match timeout(write_timeout, sink.flush()).await {
                            Ok(Ok(())) => {}
                            Ok(Err(source)) => {
                                return Err(WorkerWebSocketError::Transport(source));
                            }
                            Err(_) => return Err(WorkerWebSocketError::WriteTimedOut),
                        }
                    }
                    None => flush_requests_open = false,
                }
            }
        }
    }
}
