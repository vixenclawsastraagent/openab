use super::runtime::MAPPING_ABSENT_INITIALIZATION_ERROR_CODE;
use super::{
    parse_logical_message, BridgeAction, BridgeIdentity, BridgeKernel, BridgeProtocolError,
    ControllerError, ControllerLifecycleAction, MAX_LOGICAL_MESSAGE_BYTES,
};
use crate::wire::{
    decode_frame, encode_frame, AcpMessageV1, ActivationRequestV1, BridgeToControllerV1,
    BrokerMappingExpectationV1, ControllerToBridgeV1, HandshakeOutcomeV1, LifecycleRequestV1,
    ValidatedActivationOutcomeV1, WireProtocolError, MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
};
use futures_util::{Sink, SinkExt, StreamExt};
use serde_json::{json, Value};
use std::io;
use std::string::FromUtf8Error;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::WebSocketStream;

const MAX_BROKER_LINE_BYTES: usize = MAX_LOGICAL_MESSAGE_BYTES + 2;

/// Finite transport limits for a connected broker-side relay socket.
pub fn bridge_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: 0,
        max_write_buffer_size: MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES,
        max_message_size: Some(MAX_ACP_FRAME_BYTES),
        max_frame_size: Some(MAX_ACP_FRAME_BYTES),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeWebSocketExit {
    /// The broker closed stdin. Dropping the socket lets the controller fence
    /// the exact attachment; the bridge never reconnects or replays ACP.
    BrokerEof,
    /// The controller authoritatively proved a retained broker mapping absent
    /// and the bridge emitted the correlated initialization sentinel.
    MappingAbsent,
    /// The correlated suspend acknowledgement reached the broker.
    Suspended,
    /// The correlated destructive release acknowledgement reached the broker.
    Released,
}

#[derive(Debug, Error)]
pub enum BridgeWebSocketError {
    #[error("bridge WebSocket transport limits do not match the relay protocol")]
    UnsafeConfiguration,
    #[error("bridge startup timed out")]
    StartupTimedOut,
    #[error("broker stdin closed before initialize")]
    BrokerClosedBeforeInitialize,
    #[error("broker stdin closed while bridge activation was pending")]
    BrokerClosedDuringActivation,
    #[error("broker sent another ACP message before bridge activation completed")]
    BrokerMessageDuringActivation,
    #[error("broker stdin could not be read")]
    BrokerRead(#[source] io::Error),
    #[error("broker ACP line exceeds the logical message limit")]
    BrokerLineTooLarge,
    #[error("the first broker message must be an initialize request with a scalar id")]
    ExpectedInitialize,
    #[error("bridge WebSocket closed before activation completed")]
    ClosedBeforeActivation,
    #[error("bridge WebSocket closed after activation")]
    ClosedAfterActivation,
    #[error("bridge WebSocket transport failed")]
    Transport(#[source] Box<tungstenite::Error>),
    #[error("bridge WebSocket writes timed out")]
    WebSocketWriteTimedOut,
    #[error("broker stdout writes timed out")]
    BrokerWriteTimedOut,
    #[error("broker stdout could not be written")]
    BrokerWrite(#[source] io::Error),
    #[error("bridge WebSocket requires text application frames")]
    ExpectedTextFrame,
    #[error("controller sent an application frame before bridge activation")]
    ControllerMessageBeforeActivation,
    #[error("controller sent an invalid relay frame")]
    InvalidControllerFrame(#[source] WireProtocolError),
    #[error("bridge could not encode a relay frame")]
    OutboundFrame(#[source] WireProtocolError),
    #[error("bridge relay JSON is not UTF-8")]
    OutboundUtf8(#[source] FromUtf8Error),
    #[error("controller sent ACP before activation completed")]
    AcpBeforeActivation,
    #[error("controller sent a duplicate activation result")]
    DuplicateActivation,
    #[error("controller sent an acknowledgement instead of an activation result")]
    UnexpectedActivationAck,
    #[error("controller rejected bridge activation")]
    ActivationRejected,
    #[error("controller sent a lifecycle result without a pending request")]
    UnexpectedLifecycleResult,
    #[error("bridge already has a pending lifecycle request")]
    LifecycleAlreadyPending,
    #[error("controller rejected the lifecycle request; the bridge is terminal")]
    LifecycleRejected,
    #[error("bridge protocol rejected an ACP message")]
    BridgeProtocol(#[source] BridgeProtocolError),
    #[error("bridge attempted an impossible relay direction")]
    InvalidBridgeDirection,
}

struct BrokerLines<R> {
    reader: BufReader<R>,
    pending: Vec<u8>,
}

enum BrokerRead {
    Progress,
    Line(Vec<u8>),
    Eof,
}

impl<R> BrokerLines<R>
where
    R: AsyncRead + Unpin,
{
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            pending: Vec::new(),
        }
    }

    /// Consume at most one bounded `BufReader` chunk. Keeping the accumulator
    /// in this struct makes this method safe to cancel inside `tokio::select!`.
    async fn read_step(&mut self) -> Result<BrokerRead, BridgeWebSocketError> {
        let available = self
            .reader
            .fill_buf()
            .await
            .map_err(BridgeWebSocketError::BrokerRead)?;
        if available.is_empty() {
            return if self.pending.is_empty() {
                Ok(BrokerRead::Eof)
            } else {
                Ok(BrokerRead::Line(std::mem::take(&mut self.pending)))
            };
        }

        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if self.pending.len().saturating_add(consumed) > MAX_BROKER_LINE_BYTES {
            return Err(BridgeWebSocketError::BrokerLineTooLarge);
        }
        let completes_line = available[consumed - 1] == b'\n';
        self.pending.extend_from_slice(&available[..consumed]);
        self.reader.consume(consumed);

        if completes_line {
            Ok(BrokerRead::Line(std::mem::take(&mut self.pending)))
        } else {
            Ok(BrokerRead::Progress)
        }
    }
}

enum ActivationOutcome {
    Activated {
        kernel: Box<BridgeKernel>,
        initialize: Vec<u8>,
    },
    MappingAbsent {
        initialize_id: Value,
    },
}

struct PendingLifecycle {
    action: ControllerLifecycleAction,
    request: LifecycleRequestV1,
}

/// Drive one connected, transport-authenticated bridge relay.
///
/// HTTP upgrade, TLS verification, credential handling, and connection
/// establishment stay outside this function. This driver owns the fixed
/// activation deadline, bounded newline framing, ACP policy, lifecycle
/// correlation, and write deadlines. It never reconnects.
pub async fn run_bridge_websocket<S, R, W>(
    mut socket: WebSocketStream<S>,
    broker_stdin: R,
    mut broker_stdout: W,
    identity: BridgeIdentity,
    broker_mapping_expectation: BrokerMappingExpectationV1,
    startup_timeout: Duration,
    write_timeout: Duration,
) -> Result<BridgeWebSocketExit, BridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if !safe_bridge_websocket_config(socket.get_config()) {
        return Err(BridgeWebSocketError::UnsafeConfiguration);
    }

    let mut broker_lines = BrokerLines::new(broker_stdin);
    let activation = timeout(
        startup_timeout,
        activate(
            &mut socket,
            &mut broker_lines,
            &identity,
            broker_mapping_expectation,
            write_timeout,
        ),
    )
    .await
    .map_err(|_| BridgeWebSocketError::StartupTimedOut)??;

    let (mut kernel, initialize) = match activation {
        ActivationOutcome::Activated { kernel, initialize } => (*kernel, initialize),
        ActivationOutcome::MappingAbsent { initialize_id } => {
            let response = mapping_absent_response(initialize_id, identity.broker_attempt_id());
            write_broker_value(&mut broker_stdout, &response, write_timeout).await?;
            return Ok(BridgeWebSocketExit::MappingAbsent);
        }
    };

    let (mut sink, mut stream) = socket.split();
    let mut pending_lifecycle = None;
    handle_broker_line(
        &mut kernel,
        &mut sink,
        &mut broker_stdout,
        &mut pending_lifecycle,
        &initialize,
        write_timeout,
    )
    .await?;

    loop {
        tokio::select! {
            broker = broker_lines.read_step() => {
                match broker? {
                    BrokerRead::Progress => {}
                    BrokerRead::Line(line) => {
                        handle_broker_line(
                            &mut kernel,
                            &mut sink,
                            &mut broker_stdout,
                            &mut pending_lifecycle,
                            &line,
                            write_timeout,
                        ).await?;
                    }
                    BrokerRead::Eof => return Ok(BridgeWebSocketExit::BrokerEof),
                }
            }
            frame = stream.next() => {
                let frame = frame
                    .ok_or(BridgeWebSocketError::ClosedAfterActivation)?
                    .map_err(|source| BridgeWebSocketError::Transport(Box::new(source)))?;
                match frame {
                    Message::Text(text) => {
                        let message = decode_frame::<ControllerToBridgeV1>(text.as_bytes())
                            .map_err(BridgeWebSocketError::InvalidControllerFrame)?;
                        if let Some(exit) = handle_controller_message(
                            &mut kernel,
                            &mut broker_stdout,
                            &mut pending_lifecycle,
                            message,
                            write_timeout,
                        ).await? {
                            return Ok(exit);
                        }
                    }
                    Message::Ping(_) => flush_websocket(&mut sink, write_timeout).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(BridgeWebSocketError::ClosedAfterActivation),
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BridgeWebSocketError::ExpectedTextFrame);
                    }
                }
            }
        }
    }
}

fn safe_bridge_websocket_config(config: &WebSocketConfig) -> bool {
    config.max_message_size == Some(MAX_ACP_FRAME_BYTES)
        && config.max_frame_size == Some(MAX_ACP_FRAME_BYTES)
        && !config.accept_unmasked_frames
        && config.write_buffer_size == 0
        && config.max_write_buffer_size == MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
}

async fn activate<S, R>(
    socket: &mut WebSocketStream<S>,
    broker_lines: &mut BrokerLines<R>,
    identity: &BridgeIdentity,
    broker_mapping_expectation: BrokerMappingExpectationV1,
    write_timeout: Duration,
) -> Result<ActivationOutcome, BridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let initialize = read_initial_broker_message(socket, broker_lines, write_timeout).await?;
    let initialize_id = initialize_request_id(&initialize)?;
    let request = ActivationRequestV1::from_identity(identity, broker_mapping_expectation);
    send_wire_message(
        socket,
        &BridgeToControllerV1::Activation(request.clone()),
        write_timeout,
    )
    .await?;

    loop {
        tokio::select! {
            broker = broker_lines.read_step() => {
                match broker? {
                    BrokerRead::Progress | BrokerRead::Line(_) => {
                        return Err(BridgeWebSocketError::BrokerMessageDuringActivation)
                    }
                    BrokerRead::Eof => {
                        return Err(BridgeWebSocketError::BrokerClosedDuringActivation)
                    }
                }
            }
            frame = socket.next() => {
                let frame = frame
                    .ok_or(BridgeWebSocketError::ClosedBeforeActivation)?
                    .map_err(|source| BridgeWebSocketError::Transport(Box::new(source)))?;
                match frame {
                    Message::Text(text) => {
                        let message = decode_frame::<ControllerToBridgeV1>(text.as_bytes())
                            .map_err(BridgeWebSocketError::InvalidControllerFrame)?;
                        match message {
                            ControllerToBridgeV1::Activation(response) => {
                                return match response
                                    .into_validated_outcome(&request)
                                    .map_err(BridgeWebSocketError::InvalidControllerFrame)?
                                {
                                    ValidatedActivationOutcomeV1::Activated {
                                        binding,
                                        worker_cwd,
                                        ..
                                    } => Ok(ActivationOutcome::Activated {
                                        kernel: Box::new(
                                            BridgeKernel::new(
                                                identity.clone(),
                                                binding,
                                                worker_cwd,
                                            )
                                            .map_err(|_| {
                                                BridgeWebSocketError::ActivationRejected
                                            })?,
                                        ),
                                        initialize,
                                    }),
                                    ValidatedActivationOutcomeV1::MappingAbsent => {
                                        Ok(ActivationOutcome::MappingAbsent { initialize_id })
                                    }
                                };
                            }
                            ControllerToBridgeV1::ProtocolResult(result) => {
                                return match result
                                    .into_handshake_outcome()
                                    .map_err(BridgeWebSocketError::InvalidControllerFrame)?
                                {
                                    HandshakeOutcomeV1::Ack => {
                                        Err(BridgeWebSocketError::UnexpectedActivationAck)
                                    }
                                    HandshakeOutcomeV1::Fatal(_) => {
                                        Err(BridgeWebSocketError::ActivationRejected)
                                    }
                                };
                            }
                            ControllerToBridgeV1::Acp(_) => {
                                return Err(BridgeWebSocketError::AcpBeforeActivation)
                            }
                        }
                    }
                    Message::Ping(_) => flush_websocket(socket, write_timeout).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => {
                        return Err(BridgeWebSocketError::ClosedBeforeActivation)
                    }
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BridgeWebSocketError::ExpectedTextFrame)
                    }
                }
            }
        }
    }
}

async fn read_initial_broker_message<S, R>(
    socket: &mut WebSocketStream<S>,
    broker_lines: &mut BrokerLines<R>,
    write_timeout: Duration,
) -> Result<Vec<u8>, BridgeWebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    loop {
        tokio::select! {
            broker = broker_lines.read_step() => {
                match broker? {
                    BrokerRead::Progress => {}
                    BrokerRead::Line(line) => return Ok(line),
                    BrokerRead::Eof => {
                        return Err(BridgeWebSocketError::BrokerClosedBeforeInitialize)
                    }
                }
            }
            frame = socket.next() => {
                let frame = frame
                    .ok_or(BridgeWebSocketError::ClosedBeforeActivation)?
                    .map_err(|source| BridgeWebSocketError::Transport(Box::new(source)))?;
                match frame {
                    Message::Ping(_) => flush_websocket(socket, write_timeout).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => {
                        return Err(BridgeWebSocketError::ClosedBeforeActivation)
                    }
                    Message::Text(_) => {
                        return Err(BridgeWebSocketError::ControllerMessageBeforeActivation)
                    }
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BridgeWebSocketError::ExpectedTextFrame)
                    }
                }
            }
        }
    }
}

fn initialize_request_id(bytes: &[u8]) -> Result<Value, BridgeWebSocketError> {
    let value = parse_logical_message(bytes).map_err(BridgeWebSocketError::BridgeProtocol)?;
    let object = value
        .as_object()
        .ok_or(BridgeWebSocketError::ExpectedInitialize)?;
    let id = object
        .get("id")
        .filter(|id| id.is_number() || id.is_string())
        .cloned()
        .ok_or(BridgeWebSocketError::ExpectedInitialize)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("method").and_then(Value::as_str) != Some("initialize")
        || !object.get("params").is_some_and(Value::is_object)
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return Err(BridgeWebSocketError::ExpectedInitialize);
    }
    Ok(id)
}

async fn handle_broker_line<W, O>(
    kernel: &mut BridgeKernel,
    sink: &mut W,
    broker_stdout: &mut O,
    pending_lifecycle: &mut Option<PendingLifecycle>,
    line: &[u8],
    write_timeout: Duration,
) -> Result<(), BridgeWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
    O: AsyncWrite + Unpin,
{
    let action = kernel
        .handle_broker_message(line)
        .map_err(BridgeWebSocketError::BridgeProtocol)?;
    match action {
        BridgeAction::ForwardToWorker(message) => {
            let message =
                AcpMessageV1::new(message).map_err(BridgeWebSocketError::OutboundFrame)?;
            send_wire_message(sink, &BridgeToControllerV1::Acp(message), write_timeout).await
        }
        BridgeAction::ForwardToBroker(message) => {
            write_broker_value(broker_stdout, &message, write_timeout).await
        }
        BridgeAction::Controller(action) => {
            if pending_lifecycle.is_some() {
                return Err(BridgeWebSocketError::LifecycleAlreadyPending);
            }
            let request = LifecycleRequestV1::from_bridge_action(&action)
                .map_err(BridgeWebSocketError::OutboundFrame)?;
            send_wire_message(
                sink,
                &BridgeToControllerV1::Lifecycle(request.clone()),
                write_timeout,
            )
            .await?;
            *pending_lifecycle = Some(PendingLifecycle { action, request });
            Ok(())
        }
    }
}

async fn handle_controller_message<W>(
    kernel: &mut BridgeKernel,
    broker_stdout: &mut W,
    pending_lifecycle: &mut Option<PendingLifecycle>,
    message: ControllerToBridgeV1,
    write_timeout: Duration,
) -> Result<Option<BridgeWebSocketExit>, BridgeWebSocketError>
where
    W: AsyncWrite + Unpin,
{
    match message {
        ControllerToBridgeV1::Activation(_) => Err(BridgeWebSocketError::DuplicateActivation),
        ControllerToBridgeV1::Acp(message) => {
            match kernel
                .handle_validated_worker_value(message.into_payload())
                .map_err(BridgeWebSocketError::BridgeProtocol)?
            {
                BridgeAction::ForwardToBroker(message) => {
                    write_broker_value(broker_stdout, &message, write_timeout).await?;
                    Ok(None)
                }
                BridgeAction::ForwardToWorker(_) | BridgeAction::Controller(_) => {
                    Err(BridgeWebSocketError::InvalidBridgeDirection)
                }
            }
        }
        ControllerToBridgeV1::ProtocolResult(result) => {
            let pending = pending_lifecycle
                .as_ref()
                .ok_or(BridgeWebSocketError::UnexpectedLifecycleResult)?;
            let fatal = result
                .into_lifecycle_outcome(&pending.request)
                .map_err(BridgeWebSocketError::InvalidControllerFrame)?;
            let pending = pending_lifecycle
                .take()
                .expect("pending lifecycle was checked above");
            let terminal_exit = match pending.action.kind() {
                super::LifecycleKind::Suspend => BridgeWebSocketExit::Suspended,
                super::LifecycleKind::Release => BridgeWebSocketExit::Released,
            };
            let rejected = fatal.is_some();
            let result = fatal.map_or_else(
                || Ok(()),
                |_| {
                    Err(ControllerError::new(
                        "controller rejected lifecycle request",
                    ))
                },
            );
            match kernel
                .finish_lifecycle(&pending.action, result)
                .map_err(BridgeWebSocketError::BridgeProtocol)?
            {
                BridgeAction::ForwardToBroker(message) => {
                    write_broker_value(broker_stdout, &message, write_timeout).await?
                }
                BridgeAction::ForwardToWorker(_) | BridgeAction::Controller(_) => {
                    return Err(BridgeWebSocketError::InvalidBridgeDirection)
                }
            }
            if rejected {
                Err(BridgeWebSocketError::LifecycleRejected)
            } else {
                Ok(Some(terminal_exit))
            }
        }
    }
}

async fn send_wire_message<W, M>(
    sink: &mut W,
    message: &M,
    write_timeout: Duration,
) -> Result<(), BridgeWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
    M: crate::wire::WireMessage,
{
    let bytes = encode_frame(message).map_err(BridgeWebSocketError::OutboundFrame)?;
    let text = String::from_utf8(bytes).map_err(BridgeWebSocketError::OutboundUtf8)?;
    match timeout(write_timeout, sink.send(Message::Text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BridgeWebSocketError::Transport(Box::new(source))),
        Err(_) => Err(BridgeWebSocketError::WebSocketWriteTimedOut),
    }
}

async fn flush_websocket<W>(
    sink: &mut W,
    write_timeout: Duration,
) -> Result<(), BridgeWebSocketError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    match timeout(write_timeout, sink.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BridgeWebSocketError::Transport(Box::new(source))),
        Err(_) => Err(BridgeWebSocketError::WebSocketWriteTimedOut),
    }
}

async fn write_broker_value<W>(
    writer: &mut W,
    value: &Value,
    write_timeout: Duration,
) -> Result<(), BridgeWebSocketError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(value).expect("a serde_json::Value always serializes as JSON");
    if bytes.len() > MAX_LOGICAL_MESSAGE_BYTES {
        return Err(BridgeWebSocketError::BrokerLineTooLarge);
    }
    match timeout(write_timeout, async {
        writer.write_all(&bytes).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BridgeWebSocketError::BrokerWrite(source)),
        Err(_) => Err(BridgeWebSocketError::BrokerWriteTimedOut),
    }
}

fn mapping_absent_response(initialize_id: Value, attempt_id: uuid::Uuid) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": initialize_id,
        "error": {
            "code": MAPPING_ABSENT_INITIALIZATION_ERROR_CODE,
            "message": "isolated session durable mapping is absent",
            "data": {
                "version": 1,
                "outcome": "mapping_absent",
                "attemptId": attempt_id,
            }
        }
    })
}
