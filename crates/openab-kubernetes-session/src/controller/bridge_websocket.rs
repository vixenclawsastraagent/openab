use super::{
    ActivityEvent, ActivityOutcome, ActivityTurnId, BridgeOpenOutcome, ControllerServiceError,
    RelayAcpDeliveryOutcome, RelayActivityError, RelayAttachment, RelayConnection,
    RelayDeliveryError, RelayLifecycleError, RelayLifecycleOutcome, RelayLossOutcome,
    RelayOpenError, RelayOrchestrator, RelayOutboundItem,
};
use crate::wire::{
    decode_frame, encode_frame, AcpMessageV1, ActivationRequestV1, BridgeToControllerV1,
    ControllerToBridgeV1, FatalCode, LifecycleRequestV1, ProtocolResultV1, WireProtocolError,
    MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::Value;
use std::string::FromUtf8Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::WebSocketStream;

const MIN_RELEASE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Finite transport limits for an already-upgraded broker bridge socket.
pub fn controller_bridge_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: 0,
        max_write_buffer_size: MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES,
        max_message_size: Some(MAX_ACP_FRAME_BYTES),
        max_frame_size: Some(MAX_ACP_FRAME_BYTES),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BridgeWebSocketOutcome {
    MappingAbsent,
    ConnectionLost(RelayLossOutcome),
}

#[derive(Debug, Error)]
pub enum ControllerBridgeWebSocketError {
    #[error("controller bridge WebSocket transport limits do not match the relay protocol")]
    UnsafeConfiguration,
    #[error("controller bridge lifecycle retry interval is below the safe minimum")]
    InvalidLifecycleRetryInterval,
    #[error("controller bridge activation timed out")]
    ActivationTimedOut,
    #[error("controller bridge WebSocket closed before activation")]
    ClosedBeforeActivation,
    #[error("controller bridge WebSocket closed while activation was pending")]
    ClosedDuringActivation,
    #[error("controller bridge sent another application frame while activation was pending")]
    MessageDuringActivation,
    #[error("controller bridge WebSocket transport failed")]
    Transport(#[source] tungstenite::Error),
    #[error(
        "controller bridge WebSocket requires a text activation as its first application frame"
    )]
    ExpectedTextActivation,
    #[error("controller bridge activation frame is invalid")]
    InvalidActivationFrame(#[source] WireProtocolError),
    #[error("controller bridge first application frame is not an activation")]
    ExpectedActivation,
    #[error("controller bridge activation failed")]
    Activation(#[source] RelayOpenError),
    #[error("controller bridge received a duplicate activation")]
    DuplicateActivation,
    #[error("controller bridge WebSocket requires text application frames")]
    ExpectedTextMessage,
    #[error("controller bridge application frame is invalid")]
    InvalidMessageFrame(#[source] WireProtocolError),
    #[error("controller bridge sent another application message during lifecycle reconciliation")]
    MessageDuringLifecycle,
    #[error("controller bridge closed during lifecycle reconciliation")]
    ClosedDuringLifecycle,
    #[error("controller bridge tried to start a second concurrent prompt")]
    ConcurrentPrompt,
    #[error("controller bridge prompt request is not a valid JSON-RPC request")]
    InvalidPromptRequest,
    #[error("controller bridge requested lifecycle while a prompt is active")]
    BusyLifecycle,
    #[error("controller bridge durable activity result is stale")]
    StaleActivity,
    #[error("controller bridge activity persistence failed")]
    Activity(#[source] RelayActivityError),
    #[error("controller bridge ACP delivery failed")]
    Delivery(#[source] RelayDeliveryError),
    #[error("controller bridge lifecycle failed")]
    Lifecycle(#[source] RelayLifecycleError),
    #[error("controller bridge outbound frame is invalid")]
    OutboundFrame(#[source] WireProtocolError),
    #[error("controller bridge outbound JSON is not UTF-8")]
    OutboundUtf8(#[source] FromUtf8Error),
    #[error("controller bridge WebSocket writer stopped")]
    WriterStopped,
    #[error("controller bridge WebSocket write timed out")]
    WriteTimedOut,
    #[error("controller bridge WebSocket close timed out")]
    CloseTimedOut,
    #[error("controller bridge relay containment failed")]
    Containment(#[source] ControllerServiceError),
}

/// Drive one already-upgraded, transport-authenticated broker bridge.
///
/// Endpoint authentication must select the trusted scope and therefore the
/// [`RelayOrchestrator`] before this function sees any application frame. The
/// driver owns activation-first framing, prompt activity ordering, ACP
/// backpressure, exact lifecycle retries, write deadlines, and containment.
pub async fn serve_bridge_websocket<S>(
    relay: RelayOrchestrator,
    mut socket: WebSocketStream<S>,
    activation_timeout: Duration,
    write_timeout: Duration,
    release_retry_interval: Duration,
) -> Result<BridgeWebSocketOutcome, ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    if !safe_controller_bridge_websocket_config(socket.get_config()) {
        return Err(reject_handshake(
            &mut socket,
            ControllerBridgeWebSocketError::UnsafeConfiguration,
            write_timeout,
        )
        .await);
    }
    if release_retry_interval < MIN_RELEASE_RETRY_INTERVAL {
        return Err(reject_handshake(
            &mut socket,
            ControllerBridgeWebSocketError::InvalidLifecycleRetryInterval,
            write_timeout,
        )
        .await);
    }

    let activation = match timeout(
        activation_timeout,
        read_activation(&mut socket, write_timeout),
    )
    .await
    {
        Ok(Ok(activation)) => activation,
        Ok(Err(error @ ControllerBridgeWebSocketError::ClosedBeforeActivation))
        | Ok(Err(error @ ControllerBridgeWebSocketError::Transport(_))) => return Err(error),
        Ok(Err(error)) => return Err(reject_handshake(&mut socket, error, write_timeout).await),
        Err(_) => {
            return Err(reject_handshake(
                &mut socket,
                ControllerBridgeWebSocketError::ActivationTimedOut,
                write_timeout,
            )
            .await)
        }
    };

    let opened = match open_bridge(&relay, &mut socket, activation, write_timeout).await {
        Ok(opened) => opened,
        Err(error @ ControllerBridgeWebSocketError::ClosedDuringActivation)
        | Err(error @ ControllerBridgeWebSocketError::Transport(_))
        | Err(error @ ControllerBridgeWebSocketError::MessageDuringActivation)
        | Err(error @ ControllerBridgeWebSocketError::ExpectedTextMessage) => return Err(error),
        Err(error) => return Err(reject_handshake(&mut socket, error, write_timeout).await),
    };
    match opened {
        BridgeOpenOutcome::MappingAbsent(response) => {
            send_direct(&mut socket, &response, write_timeout).await?;
            close_direct(&mut socket, write_timeout).await?;
            Ok(BridgeWebSocketOutcome::MappingAbsent)
        }
        BridgeOpenOutcome::Attached(attachment) => {
            serve_attached_bridge(
                relay,
                socket,
                attachment,
                write_timeout,
                release_retry_interval,
            )
            .await
        }
    }
}

fn safe_controller_bridge_websocket_config(config: &WebSocketConfig) -> bool {
    config.max_message_size == Some(MAX_ACP_FRAME_BYTES)
        && config.max_frame_size == Some(MAX_ACP_FRAME_BYTES)
        && !config.accept_unmasked_frames
        && config.write_buffer_size == 0
        && config.max_write_buffer_size == MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
}

async fn read_activation<S>(
    socket: &mut WebSocketStream<S>,
    write_timeout: Duration,
) -> Result<ActivationRequestV1, ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let frame = socket
            .next()
            .await
            .ok_or(ControllerBridgeWebSocketError::ClosedBeforeActivation)?
            .map_err(ControllerBridgeWebSocketError::Transport)?;
        match frame {
            Message::Text(text) => {
                return match decode_frame(text.as_bytes())
                    .map_err(ControllerBridgeWebSocketError::InvalidActivationFrame)?
                {
                    BridgeToControllerV1::Activation(activation) => Ok(activation),
                    BridgeToControllerV1::Acp(_) | BridgeToControllerV1::Lifecycle(_) => {
                        Err(ControllerBridgeWebSocketError::ExpectedActivation)
                    }
                }
            }
            Message::Ping(_) => flush_sink(socket, write_timeout).await?,
            Message::Pong(_) => {}
            Message::Close(_) => {
                return Err(ControllerBridgeWebSocketError::ClosedBeforeActivation)
            }
            Message::Binary(_) | Message::Frame(_) => {
                return Err(ControllerBridgeWebSocketError::ExpectedTextActivation)
            }
        }
    }
}

async fn open_bridge<S>(
    relay: &RelayOrchestrator,
    socket: &mut WebSocketStream<S>,
    activation: ActivationRequestV1,
    write_timeout: Duration,
) -> Result<BridgeOpenOutcome, ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let opening = relay.activate_bridge(activation);
    tokio::pin!(opening);
    loop {
        tokio::select! {
            result = &mut opening => {
                return result.map_err(ControllerBridgeWebSocketError::Activation)
            }
            frame = socket.next() => {
                let frame = frame
                    .ok_or(ControllerBridgeWebSocketError::ClosedDuringActivation)?
                    .map_err(ControllerBridgeWebSocketError::Transport)?;
                match frame {
                    Message::Ping(_) => flush_sink(socket, write_timeout).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => {
                        return Err(ControllerBridgeWebSocketError::ClosedDuringActivation)
                    }
                    Message::Text(_) => {
                        return Err(ControllerBridgeWebSocketError::MessageDuringActivation)
                    }
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(ControllerBridgeWebSocketError::ExpectedTextMessage)
                    }
                }
            }
        }
    }
}

async fn reject_handshake<S>(
    socket: &mut WebSocketStream<S>,
    error: ControllerBridgeWebSocketError,
    write_timeout: Duration,
) -> ControllerBridgeWebSocketError
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(code) = handshake_fatal_code(&error) else {
        return error;
    };
    let result = match ProtocolResultV1::fatal(None, code) {
        Ok(result) => result,
        Err(source) => return ControllerBridgeWebSocketError::OutboundFrame(source),
    };
    if let Err(delivery) = send_direct(
        socket,
        &ControllerToBridgeV1::ProtocolResult(result),
        write_timeout,
    )
    .await
    {
        return delivery;
    }
    if let Err(close) = close_direct(socket, write_timeout).await {
        return close;
    }
    error
}

fn handshake_fatal_code(error: &ControllerBridgeWebSocketError) -> Option<FatalCode> {
    match error {
        ControllerBridgeWebSocketError::UnsafeConfiguration
        | ControllerBridgeWebSocketError::InvalidLifecycleRetryInterval => {
            Some(FatalCode::Internal)
        }
        ControllerBridgeWebSocketError::ActivationTimedOut => Some(FatalCode::Unavailable),
        ControllerBridgeWebSocketError::ExpectedTextActivation
        | ControllerBridgeWebSocketError::InvalidActivationFrame(_)
        | ControllerBridgeWebSocketError::ExpectedActivation => Some(FatalCode::InvalidMessage),
        ControllerBridgeWebSocketError::Activation(error) => Some(error.fatal_code()),
        ControllerBridgeWebSocketError::ClosedBeforeActivation
        | ControllerBridgeWebSocketError::ClosedDuringActivation
        | ControllerBridgeWebSocketError::MessageDuringActivation
        | ControllerBridgeWebSocketError::Transport(_)
        | ControllerBridgeWebSocketError::DuplicateActivation
        | ControllerBridgeWebSocketError::ExpectedTextMessage
        | ControllerBridgeWebSocketError::InvalidMessageFrame(_)
        | ControllerBridgeWebSocketError::MessageDuringLifecycle
        | ControllerBridgeWebSocketError::ClosedDuringLifecycle
        | ControllerBridgeWebSocketError::ConcurrentPrompt
        | ControllerBridgeWebSocketError::InvalidPromptRequest
        | ControllerBridgeWebSocketError::BusyLifecycle
        | ControllerBridgeWebSocketError::StaleActivity
        | ControllerBridgeWebSocketError::Activity(_)
        | ControllerBridgeWebSocketError::Delivery(_)
        | ControllerBridgeWebSocketError::Lifecycle(_)
        | ControllerBridgeWebSocketError::OutboundFrame(_)
        | ControllerBridgeWebSocketError::OutboundUtf8(_)
        | ControllerBridgeWebSocketError::WriterStopped
        | ControllerBridgeWebSocketError::WriteTimedOut
        | ControllerBridgeWebSocketError::CloseTimedOut
        | ControllerBridgeWebSocketError::Containment(_) => None,
    }
}

async fn serve_attached_bridge<S>(
    relay: RelayOrchestrator,
    socket: WebSocketStream<S>,
    mut attachment: RelayAttachment<ControllerToBridgeV1>,
    write_timeout: Duration,
    release_retry_interval: Duration,
) -> Result<BridgeWebSocketOutcome, ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let connection = attachment.connection().clone();
    let mut quiesced = attachment.quiesced();
    let tracker = PromptTracker::default();
    let connection_result = if *quiesced.borrow() {
        drop(socket);
        Ok(())
    } else {
        let (flush_requests, flush_request_receiver) = mpsc::channel(1);
        let (sink, stream) = socket.split();
        let reader = read_bridge_frames(
            relay.clone(),
            connection.clone(),
            stream,
            flush_requests,
            tracker.clone(),
            release_retry_interval,
        );
        let writer = write_bridge_frames(
            relay.clone(),
            connection,
            sink,
            attachment.outbound(),
            flush_request_receiver,
            tracker,
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
        .map_err(ControllerBridgeWebSocketError::Containment)?;
    connection_result.map(|_| BridgeWebSocketOutcome::ConnectionLost(loss))
}

async fn read_bridge_frames<R>(
    relay: RelayOrchestrator,
    connection: RelayConnection,
    mut stream: R,
    flush_requests: mpsc::Sender<()>,
    tracker: PromptTracker,
    release_retry_interval: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    while let Some(frame) = stream.next().await {
        match frame.map_err(ControllerBridgeWebSocketError::Transport)? {
            Message::Text(text) => {
                let message = decode_frame(text.as_bytes())
                    .map_err(ControllerBridgeWebSocketError::InvalidMessageFrame)?;
                match message {
                    BridgeToControllerV1::Activation(_) => {
                        return Err(ControllerBridgeWebSocketError::DuplicateActivation)
                    }
                    BridgeToControllerV1::Acp(message) => {
                        route_bridge_acp(&relay, &connection, &tracker, message).await?;
                    }
                    BridgeToControllerV1::Lifecycle(request) => {
                        if tracker.is_active() {
                            return Err(ControllerBridgeWebSocketError::BusyLifecycle);
                        }
                        drive_lifecycle(
                            &relay,
                            &connection,
                            request,
                            &mut stream,
                            &flush_requests,
                            release_retry_interval,
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }
            Message::Ping(_) => request_flush(&flush_requests)?,
            Message::Pong(_) => {}
            Message::Close(_) => return Ok(()),
            Message::Binary(_) | Message::Frame(_) => {
                return Err(ControllerBridgeWebSocketError::ExpectedTextMessage)
            }
        }
    }
    Ok(())
}

async fn route_bridge_acp(
    relay: &RelayOrchestrator,
    connection: &RelayConnection,
    tracker: &PromptTracker,
    mut message: AcpMessageV1,
) -> Result<(), ControllerBridgeWebSocketError> {
    if let Some(request_id) = prompt_request_id(&message)? {
        if tracker.is_active() {
            return Err(ControllerBridgeWebSocketError::ConcurrentPrompt);
        }
        let turn_id = ActivityTurnId::generate();
        persist_activity(relay, connection, turn_id, ActivityEvent::PromptStarted).await?;
        tracker.start(request_id, turn_id)?;
    }

    loop {
        match relay
            .route_acp(connection, message)
            .await
            .map_err(ControllerBridgeWebSocketError::Delivery)?
        {
            RelayAcpDeliveryOutcome::Delivered => return Ok(()),
            RelayAcpDeliveryOutcome::Backpressured {
                message: retained, ..
            } => {
                relay
                    .wait_for_route_capacity(connection, &retained)
                    .await
                    .map_err(ControllerBridgeWebSocketError::Delivery)?;
                message = retained;
            }
            RelayAcpDeliveryOutcome::PeerContained(_) => return Ok(()),
        }
    }
}

async fn drive_lifecycle<R>(
    relay: &RelayOrchestrator,
    connection: &RelayConnection,
    request: LifecycleRequestV1,
    stream: &mut R,
    flush_requests: &mpsc::Sender<()>,
    retry_interval: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    let mut release_accepted = false;
    loop {
        let pass = relay.request_lifecycle(connection, request.clone());
        tokio::pin!(pass);
        let outcome = loop {
            tokio::select! {
                result = &mut pass => break result,
                frame = stream.next() => {
                    handle_lifecycle_control_frame(frame, flush_requests)?;
                }
            }
        };
        match outcome {
            Ok(RelayLifecycleOutcome::Suspended | RelayLifecycleOutcome::Released) => return Ok(()),
            Ok(RelayLifecycleOutcome::ReleasePending) => release_accepted = true,
            Ok(RelayLifecycleOutcome::Coalesced) => {}
            Err(error @ RelayLifecycleError::Controller(_))
                if release_accepted && error.fatal_code() == FatalCode::Unavailable => {}
            Err(error) => return Err(ControllerBridgeWebSocketError::Lifecycle(error)),
        }
        wait_lifecycle_retry(stream, flush_requests, retry_interval).await?;
    }
}

async fn wait_lifecycle_retry<R>(
    stream: &mut R,
    flush_requests: &mpsc::Sender<()>,
    retry_interval: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    let delay = sleep(retry_interval);
    tokio::pin!(delay);
    loop {
        tokio::select! {
            _ = &mut delay => return Ok(()),
            frame = stream.next() => handle_lifecycle_control_frame(frame, flush_requests)?,
        }
    }
}

fn handle_lifecycle_control_frame(
    frame: Option<Result<Message, tungstenite::Error>>,
    flush_requests: &mpsc::Sender<()>,
) -> Result<(), ControllerBridgeWebSocketError> {
    let frame = frame
        .ok_or(ControllerBridgeWebSocketError::ClosedDuringLifecycle)?
        .map_err(ControllerBridgeWebSocketError::Transport)?;
    match frame {
        Message::Ping(_) => request_flush(flush_requests),
        Message::Pong(_) => Ok(()),
        Message::Close(_) => Err(ControllerBridgeWebSocketError::ClosedDuringLifecycle),
        Message::Text(_) => Err(ControllerBridgeWebSocketError::MessageDuringLifecycle),
        Message::Binary(_) | Message::Frame(_) => {
            Err(ControllerBridgeWebSocketError::ExpectedTextMessage)
        }
    }
}

fn request_flush(flush_requests: &mpsc::Sender<()>) -> Result<(), ControllerBridgeWebSocketError> {
    match flush_requests.try_send(()) {
        Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err(ControllerBridgeWebSocketError::WriterStopped)
        }
    }
}

async fn write_bridge_frames<W>(
    relay: RelayOrchestrator,
    connection: RelayConnection,
    mut sink: W,
    outbound: &mut mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
    mut flush_requests: mpsc::Receiver<()>,
    tracker: PromptTracker,
    write_timeout: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let mut flush_requests_open = true;
    loop {
        tokio::select! {
            biased;
            item = outbound.recv() => {
                let Some(item) = item else {
                    return Ok(())
                };
                if let Some(turn_id) = tracker.matching_response(item.message()) {
                    persist_activity(
                        &relay,
                        &connection,
                        turn_id,
                        ActivityEvent::PromptFinished,
                    ).await?;
                    tracker.finish(turn_id);
                }
                let frame = item
                    .into_encoded_frame()
                    .map_err(ControllerBridgeWebSocketError::OutboundFrame)?;
                let (bytes, write_guard) = frame.into_write_parts();
                let text = String::from_utf8(bytes)
                    .map_err(ControllerBridgeWebSocketError::OutboundUtf8)?;
                match timeout(write_timeout, sink.send(Message::Text(text))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) => {
                        return Err(ControllerBridgeWebSocketError::Transport(source))
                    }
                    Err(_) => return Err(ControllerBridgeWebSocketError::WriteTimedOut),
                }
                write_guard.mark_written();
            }
            request = flush_requests.recv(), if flush_requests_open => {
                match request {
                    Some(()) => flush_sink(&mut sink, write_timeout).await?,
                    None => flush_requests_open = false,
                }
            }
        }
    }
}

async fn persist_activity(
    relay: &RelayOrchestrator,
    connection: &RelayConnection,
    turn_id: ActivityTurnId,
    event: ActivityEvent,
) -> Result<(), ControllerBridgeWebSocketError> {
    match relay
        .record_activity(connection, turn_id, event)
        .await
        .map_err(ControllerBridgeWebSocketError::Activity)?
    {
        ActivityOutcome::Recorded | ActivityOutcome::AlreadyRecorded => Ok(()),
        ActivityOutcome::Stale => Err(ControllerBridgeWebSocketError::StaleActivity),
    }
}

fn prompt_request_id(
    message: &AcpMessageV1,
) -> Result<Option<Value>, ControllerBridgeWebSocketError> {
    let Some(object) = message.payload().as_object() else {
        return Ok(None);
    };
    if object.get("method").and_then(Value::as_str) != Some("session/prompt") {
        return Ok(None);
    }
    let id = object
        .get("id")
        .filter(|id| id.is_number() || id.is_string())
        .cloned()
        .ok_or(ControllerBridgeWebSocketError::InvalidPromptRequest)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !object.get("params").is_some_and(Value::is_object)
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return Err(ControllerBridgeWebSocketError::InvalidPromptRequest);
    }
    Ok(Some(id))
}

fn response_id(message: &ControllerToBridgeV1) -> Option<&Value> {
    let ControllerToBridgeV1::Acp(message) = message else {
        return None;
    };
    let object = message.payload().as_object()?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.contains_key("method")
        || object
            .get("id")
            .is_none_or(|id| !id.is_number() && !id.is_string())
        || object.contains_key("result") == object.contains_key("error")
    {
        return None;
    }
    object.get("id")
}

#[derive(Clone, Default)]
struct PromptTracker {
    active: Arc<Mutex<Option<PromptTurn>>>,
}

#[derive(Clone)]
struct PromptTurn {
    request_id: Value,
    turn_id: ActivityTurnId,
}

impl PromptTracker {
    fn is_active(&self) -> bool {
        self.active
            .lock()
            .expect("prompt tracker is not poisoned")
            .is_some()
    }

    fn start(
        &self,
        request_id: Value,
        turn_id: ActivityTurnId,
    ) -> Result<(), ControllerBridgeWebSocketError> {
        let mut active = self.active.lock().expect("prompt tracker is not poisoned");
        if active.is_some() {
            return Err(ControllerBridgeWebSocketError::ConcurrentPrompt);
        }
        *active = Some(PromptTurn {
            request_id,
            turn_id,
        });
        Ok(())
    }

    fn matching_response(&self, message: &ControllerToBridgeV1) -> Option<ActivityTurnId> {
        let response_id = response_id(message)?;
        self.active
            .lock()
            .expect("prompt tracker is not poisoned")
            .as_ref()
            .filter(|turn| turn.request_id == *response_id)
            .map(|turn| turn.turn_id)
    }

    fn finish(&self, turn_id: ActivityTurnId) {
        let mut active = self.active.lock().expect("prompt tracker is not poisoned");
        if active.as_ref().is_some_and(|turn| turn.turn_id == turn_id) {
            *active = None;
        }
    }
}

async fn send_direct<S>(
    socket: &mut WebSocketStream<S>,
    message: &ControllerToBridgeV1,
    write_timeout: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = encode_frame(message).map_err(ControllerBridgeWebSocketError::OutboundFrame)?;
    let text = String::from_utf8(bytes).map_err(ControllerBridgeWebSocketError::OutboundUtf8)?;
    match timeout(write_timeout, socket.send(Message::Text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(ControllerBridgeWebSocketError::Transport(source)),
        Err(_) => Err(ControllerBridgeWebSocketError::WriteTimedOut),
    }
}

async fn close_direct<S>(
    socket: &mut WebSocketStream<S>,
    write_timeout: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(write_timeout, socket.close(None)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(ControllerBridgeWebSocketError::Transport(source)),
        Err(_) => Err(ControllerBridgeWebSocketError::CloseTimedOut),
    }
}

async fn flush_sink<W>(
    sink: &mut W,
    write_timeout: Duration,
) -> Result<(), ControllerBridgeWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    match timeout(write_timeout, sink.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(ControllerBridgeWebSocketError::Transport(source)),
        Err(_) => Err(ControllerBridgeWebSocketError::WriteTimedOut),
    }
}
