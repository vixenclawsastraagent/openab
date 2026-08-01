#![cfg(feature = "bridge-runtime")]

use futures_util::{SinkExt, StreamExt};
use openab_kubernetes_session::bridge::runtime::{BridgeEnvironment, BridgeEnvironmentError};
use openab_kubernetes_session::bridge::websocket::{
    bridge_websocket_config, run_bridge_websocket, BridgeWebSocketError, BridgeWebSocketExit,
};
use openab_kubernetes_session::bridge::{BridgeIdentity, BridgeProtocolError, SessionBinding};
use openab_kubernetes_session::state::{Fence, ProfileRef};
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, AcpMessageV1, ActivatedSessionV1, ActivationResponseV1,
    BridgeToControllerV1, BrokerMappingExpectationV1, ControllerToBridgeV1, FatalCode,
    LifecycleRequestV1, ProtocolResultV1, WireMessage, MAX_ACP_FRAME_BYTES,
    MAX_CONTROL_FRAME_BYTES,
};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const ATTEMPT_ID: Uuid = Uuid::from_u128(100);
const INCARNATION_ID: Uuid = Uuid::from_u128(200);

type TestWebSocket = WebSocketStream<DuplexStream>;

#[test]
fn broker_environment_accepts_only_the_closed_mapping_expectation_values() {
    let absent = BridgeEnvironment::from_values(
        "team-a",
        "discord:thread-123",
        &ATTEMPT_ID.to_string(),
        "codex-strict",
        "absent",
    )
    .unwrap();
    assert_eq!(
        absent.broker_mapping_expectation(),
        BrokerMappingExpectationV1::Absent
    );

    let present = BridgeEnvironment::from_values(
        "team-a",
        "discord:thread-123",
        &ATTEMPT_ID.to_string(),
        "codex-strict",
        "present",
    )
    .unwrap();
    assert_eq!(
        present.broker_mapping_expectation(),
        BrokerMappingExpectationV1::Present
    );

    for invalid in ["", "Absent", "PRESENT", " present", "present\n", "unknown"] {
        assert_eq!(
            BridgeEnvironment::from_values(
                "team-a",
                "discord:thread-123",
                &ATTEMPT_ID.to_string(),
                "codex-strict",
                invalid,
            )
            .unwrap_err(),
            BridgeEnvironmentError::InvalidMappingExpectation
        );
    }
}

#[test]
fn broker_environment_does_not_retain_the_raw_session_key() {
    let raw_session_key = "discord:server-secret:thread-123";
    let environment = BridgeEnvironment::from_values(
        "team-a",
        raw_session_key,
        &ATTEMPT_ID.to_string(),
        "codex-strict",
        "present",
    )
    .unwrap();

    assert!(!format!("{environment:?}").contains(raw_session_key));
    assert_eq!(environment.identity().broker_attempt_id(), ATTEMPT_ID);
}

fn identity() -> BridgeIdentity {
    BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        &ATTEMPT_ID.to_string(),
        "codex-strict",
    )
    .unwrap()
}

fn binding() -> SessionBinding {
    let identity = identity();
    SessionBinding::new(
        identity.scope_id(),
        identity.session_id(),
        Fence::new(7, ATTEMPT_ID).unwrap(),
        INCARNATION_ID,
    )
    .unwrap()
}

fn request(id: Value, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

async fn websocket_pair() -> (TestWebSocket, TestWebSocket) {
    websocket_pair_with_capacity(256 * 1024).await
}

async fn websocket_pair_with_capacity(capacity: usize) -> (TestWebSocket, TestWebSocket) {
    let (bridge_io, controller_io) = duplex(capacity);
    let bridge =
        WebSocketStream::from_raw_socket(bridge_io, Role::Client, Some(bridge_websocket_config()))
            .await;
    let controller = WebSocketStream::from_raw_socket(
        controller_io,
        Role::Server,
        Some(bridge_websocket_config()),
    )
    .await;
    (bridge, controller)
}

fn websocket_text<M: WireMessage>(message: &M) -> Message {
    Message::Text(String::from_utf8(encode_frame(message).unwrap()).unwrap())
}

async fn receive_controller_message(socket: &mut TestWebSocket) -> BridgeToControllerV1 {
    let frame = timeout(Duration::from_secs(1), socket.next())
        .await
        .expect("bridge should send a WebSocket frame")
        .expect("bridge should keep the WebSocket open")
        .expect("bridge should send a valid WebSocket frame");
    let Message::Text(text) = frame else {
        panic!("bridge application frames must be text")
    };
    decode_frame(text.as_bytes()).unwrap()
}

async fn send_broker_line(writer: &mut DuplexStream, value: &Value) {
    writer
        .write_all(&serde_json::to_vec(value).unwrap())
        .await
        .unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn receive_broker_line(reader: &mut BufReader<DuplexStream>) -> Value {
    let mut line = String::new();
    timeout(Duration::from_secs(1), reader.read_line(&mut line))
        .await
        .expect("bridge should write one broker line")
        .expect("broker output should be readable");
    serde_json::from_str(&line).unwrap()
}

async fn send_activated(
    socket: &mut TestWebSocket,
    activation: &openab_kubernetes_session::wire::ActivationRequestV1,
) {
    let activated = ActivatedSessionV1::new(
        activation,
        ProfileRef::new("codex-strict", "2026-08-02").unwrap(),
        &binding(),
        "/workspace",
    )
    .unwrap();
    socket
        .send(websocket_text(&ControllerToBridgeV1::Activation(
            ActivationResponseV1::activated(activated),
        )))
        .await
        .unwrap();
}

struct ActiveBridge {
    controller: TestWebSocket,
    broker_input: DuplexStream,
    broker_output: BufReader<DuplexStream>,
    driver: tokio::task::JoinHandle<Result<BridgeWebSocketExit, BridgeWebSocketError>>,
}

async fn active_bridge(capacity: usize, write_timeout: Duration) -> ActiveBridge {
    let (bridge_socket, mut controller) = websocket_pair_with_capacity(capacity).await;
    let (mut broker_input, bridge_stdin) = duplex(capacity);
    let (bridge_stdout, broker_output) = duplex(capacity);
    let mut broker_output = BufReader::new(broker_output);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(30),
        write_timeout,
    ));

    send_broker_line(
        &mut broker_input,
        &request(json!(1), "initialize", json!({})),
    )
    .await;
    let BridgeToControllerV1::Activation(activation) =
        receive_controller_message(&mut controller).await
    else {
        panic!("activation must be first")
    };
    send_activated(&mut controller, &activation).await;
    assert!(matches!(
        receive_controller_message(&mut controller).await,
        BridgeToControllerV1::Acp(_)
    ));
    controller
        .send(websocket_text(&ControllerToBridgeV1::Acp(
            AcpMessageV1::new(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"agentCapabilities": {"loadSession": true}}
            }))
            .unwrap(),
        )))
        .await
        .unwrap();
    let _ = receive_broker_line(&mut broker_output).await;

    send_broker_line(
        &mut broker_input,
        &request(json!(2), "session/new", json!({})),
    )
    .await;
    assert!(matches!(
        receive_controller_message(&mut controller).await,
        BridgeToControllerV1::Acp(_)
    ));
    controller
        .send(websocket_text(&ControllerToBridgeV1::Acp(
            AcpMessageV1::new(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {"sessionId": "worker-1"}
            }))
            .unwrap(),
        )))
        .await
        .unwrap();
    let _ = receive_broker_line(&mut broker_output).await;

    ActiveBridge {
        controller,
        broker_input,
        broker_output,
        driver,
    }
}

#[test]
fn bridge_websocket_limits_match_the_wire_protocol() {
    let config = bridge_websocket_config();
    assert_eq!(config.max_message_size, Some(MAX_ACP_FRAME_BYTES));
    assert_eq!(config.max_frame_size, Some(MAX_ACP_FRAME_BYTES));
    assert!(!config.accept_unmasked_frames);
    assert_eq!(config.write_buffer_size, 0);
    assert_eq!(
        config.max_write_buffer_size,
        MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
    );
}

#[tokio::test]
async fn bridge_activates_first_and_relays_stdio_acp_in_both_directions() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, broker_output) = duplex(256 * 1024);
    let mut broker_output = BufReader::new(broker_output);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(1),
        Duration::from_secs(1),
    ));

    let initialize = request(
        json!(1),
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": true}}
        }),
    );
    send_broker_line(&mut broker_input, &initialize).await;

    let BridgeToControllerV1::Activation(activation) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("activation must be the first application frame")
    };
    assert_eq!(
        activation.broker_mapping_expectation(),
        BrokerMappingExpectationV1::Absent
    );
    send_activated(&mut controller_socket, &activation).await;

    let BridgeToControllerV1::Acp(forwarded_initialize) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("initialize should follow successful activation")
    };
    assert_eq!(
        forwarded_initialize.payload()["params"]["clientCapabilities"],
        json!({})
    );

    let initialized = AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {"loadSession": true}
        }
    }))
    .unwrap();
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Acp(initialized)))
        .await
        .unwrap();
    let initialized = receive_broker_line(&mut broker_output).await;
    assert_eq!(
        initialized["result"]["agentCapabilities"]["sessionCapabilities"]["close"],
        json!({})
    );
    assert_eq!(
        initialized["result"]["agentCapabilities"]["sessionCapabilities"]["_meta"]["openab.dev"]
            ["sessionRelease"]["version"],
        json!(1)
    );

    let new_session = request(
        json!(2),
        "session/new",
        json!({"cwd": "/broker", "mcpServers": [{"name": "broker-host"}]}),
    );
    send_broker_line(&mut broker_input, &new_session).await;
    let BridgeToControllerV1::Acp(new_session) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("session/new should be relayed as ACP")
    };
    assert_eq!(new_session.payload()["params"]["cwd"], json!("/workspace"));
    assert_eq!(new_session.payload()["params"]["mcpServers"], json!([]));

    let created = AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"sessionId": "worker-1"}
    }))
    .unwrap();
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Acp(created)))
        .await
        .unwrap();
    assert_eq!(
        receive_broker_line(&mut broker_output).await["result"]["sessionId"],
        json!("worker-1")
    );

    let update = AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": "worker-1", "update": {"sessionUpdate": "agent_message_chunk"}}
    }))
    .unwrap();
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Acp(update.clone())))
        .await
        .unwrap();
    assert_eq!(
        receive_broker_line(&mut broker_output).await,
        update.into_payload()
    );

    drop(broker_input);
    assert_eq!(
        timeout(Duration::from_secs(1), driver)
            .await
            .expect("broker EOF should stop the driver")
            .expect("driver should not panic")
            .expect("broker EOF should be clean"),
        BridgeWebSocketExit::BrokerEof
    );
}

#[tokio::test]
async fn present_mapping_absence_emits_the_exact_openab_initialization_sentinel() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, broker_output) = duplex(256 * 1024);
    let mut broker_output = BufReader::new(broker_output);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Present,
        Duration::from_secs(1),
        Duration::from_secs(1),
    ));

    send_broker_line(
        &mut broker_input,
        &request(json!(7), "initialize", json!({})),
    )
    .await;
    let BridgeToControllerV1::Activation(activation) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("activation must be first")
    };
    let absent = ActivationResponseV1::mapping_absent(&activation).unwrap();
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Activation(absent)))
        .await
        .unwrap();

    let response = receive_broker_line(&mut broker_output).await;
    assert_eq!(response["id"], json!(7));
    assert_eq!(response["error"]["code"], json!(-32041));
    assert_eq!(response["error"]["data"]["version"], json!(1));
    assert_eq!(
        response["error"]["data"]["outcome"],
        json!("mapping_absent")
    );
    assert_eq!(response["error"]["data"]["attemptId"], json!(ATTEMPT_ID));
    assert_eq!(
        timeout(Duration::from_secs(1), driver)
            .await
            .expect("mapping absence should stop the driver")
            .expect("driver should not panic")
            .expect("mapping absence is a typed exit"),
        BridgeWebSocketExit::MappingAbsent
    );
}

#[tokio::test]
async fn lifecycle_ack_is_emitted_only_after_the_correlated_controller_result() {
    let mut bridge = active_bridge(256 * 1024, Duration::from_secs(1)).await;

    send_broker_line(
        &mut bridge.broker_input,
        &request(json!(3), "session/close", json!({"sessionId": "worker-1"})),
    )
    .await;
    let BridgeToControllerV1::Lifecycle(lifecycle) =
        receive_controller_message(&mut bridge.controller).await
    else {
        panic!("close must become a controller lifecycle request")
    };
    assert!(
        timeout(Duration::from_millis(20), bridge.broker_output.fill_buf())
            .await
            .is_err()
    );

    acknowledge_lifecycle(&mut bridge.controller, &lifecycle).await;
    assert_eq!(
        receive_broker_line(&mut bridge.broker_output).await,
        json!({"jsonrpc": "2.0", "id": 3, "result": {}})
    );

    drop(bridge.broker_input);
    assert_eq!(
        timeout(Duration::from_secs(1), bridge.driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        BridgeWebSocketExit::BrokerEof
    );
}

async fn acknowledge_lifecycle(socket: &mut TestWebSocket, request: &LifecycleRequestV1) {
    let result = ProtocolResultV1::ack(Some(request.request_id())).unwrap();
    socket
        .send(websocket_text(&ControllerToBridgeV1::ProtocolResult(
            result,
        )))
        .await
        .unwrap();
}

#[tokio::test]
async fn lifecycle_fatal_is_written_to_the_broker_before_the_lane_terminates() {
    let mut bridge = active_bridge(256 * 1024, Duration::from_secs(1)).await;
    send_broker_line(
        &mut bridge.broker_input,
        &request(json!(3), "session/close", json!({"sessionId": "worker-1"})),
    )
    .await;
    let BridgeToControllerV1::Lifecycle(lifecycle) =
        receive_controller_message(&mut bridge.controller).await
    else {
        panic!("close must become a lifecycle request")
    };
    let fatal =
        ProtocolResultV1::fatal(Some(lifecycle.request_id()), FatalCode::Unavailable).unwrap();
    bridge
        .controller
        .send(websocket_text(&ControllerToBridgeV1::ProtocolResult(fatal)))
        .await
        .unwrap();

    let response = receive_broker_line(&mut bridge.broker_output).await;
    assert_eq!(response["id"], json!(3));
    assert_eq!(response["error"]["code"], json!(-32000));
    assert!(matches!(
        bridge.driver.await.unwrap(),
        Err(BridgeWebSocketError::LifecycleRejected)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_stalled_broker_stdout_write_is_bounded_by_the_write_deadline() {
    let bridge = active_bridge(8 * 1024, Duration::from_secs(5)).await;
    let message = ControllerToBridgeV1::Acp(
        AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "worker-1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "x".repeat(512 * 1024)}
                }
            }
        }))
        .unwrap(),
    );
    let mut controller = bridge.controller;
    let sender = tokio::spawn(async move { controller.send(websocket_text(&message)).await });

    assert!(matches!(
        bridge.driver.await.unwrap(),
        Err(BridgeWebSocketError::BrokerWriteTimedOut)
    ));
    let _ = sender.await;
}

#[tokio::test(start_paused = true)]
async fn transport_pings_do_not_extend_the_fixed_startup_deadline() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (_broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, _broker_output) = duplex(256 * 1024);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(30),
        Duration::from_secs(1),
    ));

    controller_socket
        .send(Message::Ping(vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(
        controller_socket.next().await.unwrap().unwrap(),
        Message::Pong(vec![1, 2, 3])
    );
    tokio::time::advance(Duration::from_secs(31)).await;

    assert!(matches!(
        driver.await.unwrap(),
        Err(BridgeWebSocketError::StartupTimedOut)
    ));
}

#[tokio::test]
async fn non_initialize_broker_input_fails_before_activation_is_sent() {
    for invalid in [
        request(json!(1), "session/new", json!({})),
        request(json!(1), "initialize", json!([])),
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}),
    ] {
        assert_broker_input_rejected_before_activation(&invalid).await;
    }
}

async fn assert_broker_input_rejected_before_activation(invalid: &Value) {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, _broker_output) = duplex(256 * 1024);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(1),
        Duration::from_secs(1),
    ));

    send_broker_line(&mut broker_input, invalid).await;
    assert!(matches!(
        driver.await.unwrap(),
        Err(BridgeWebSocketError::ExpectedInitialize)
    ));
    if let Ok(Some(Ok(Message::Text(text)))) =
        timeout(Duration::from_millis(20), controller_socket.next()).await
    {
        panic!("invalid broker input must not send an activation frame: {text}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_stalled_controller_write_is_bounded_by_the_write_deadline() {
    let (bridge_socket, mut controller_socket) = websocket_pair_with_capacity(8 * 1024).await;
    let (mut broker_input, bridge_stdin) = duplex(8 * 1024);
    let (bridge_stdout, _broker_output) = duplex(8 * 1024);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(30),
        Duration::from_secs(5),
    ));

    send_broker_line(
        &mut broker_input,
        &request(
            json!(1),
            "initialize",
            json!({
                "clientCapabilities": {},
                "clientInfo": {"padding": "x".repeat(512 * 1024)}
            }),
        ),
    )
    .await;
    let BridgeToControllerV1::Activation(activation) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("activation must be first")
    };
    send_activated(&mut controller_socket, &activation).await;

    assert!(matches!(
        driver.await.unwrap(),
        Err(BridgeWebSocketError::WebSocketWriteTimedOut)
    ));
}

#[tokio::test]
async fn broker_eof_while_activation_is_pending_closes_the_lane_promptly() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, _broker_output) = duplex(256 * 1024);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(60),
        Duration::from_secs(1),
    ));

    send_broker_line(
        &mut broker_input,
        &request(json!(1), "initialize", json!({})),
    )
    .await;
    assert!(matches!(
        receive_controller_message(&mut controller_socket).await,
        BridgeToControllerV1::Activation(_)
    ));
    drop(broker_input);

    assert!(matches!(
        timeout(Duration::from_secs(1), driver)
            .await
            .expect("broker EOF must not wait for the startup deadline")
            .expect("driver should not panic"),
        Err(BridgeWebSocketError::BrokerClosedDuringActivation)
    ));
}

#[tokio::test]
async fn partial_second_broker_message_is_rejected_while_activation_is_pending() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, _broker_output) = duplex(256 * 1024);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(60),
        Duration::from_secs(1),
    ));

    send_broker_line(
        &mut broker_input,
        &request(json!(1), "initialize", json!({})),
    )
    .await;
    assert!(matches!(
        receive_controller_message(&mut controller_socket).await,
        BridgeToControllerV1::Activation(_)
    ));
    broker_input.write_all(b"{").await.unwrap();
    broker_input.flush().await.unwrap();

    assert!(matches!(
        timeout(Duration::from_secs(1), driver)
            .await
            .expect("pipelined ACP must be rejected before activation completes")
            .expect("driver should not panic"),
        Err(BridgeWebSocketError::BrokerMessageDuringActivation)
    ));
}

#[tokio::test]
async fn oversized_worker_session_id_terminates_before_broker_mapping_is_emitted() {
    let (bridge_socket, mut controller_socket) = websocket_pair().await;
    let (mut broker_input, bridge_stdin) = duplex(256 * 1024);
    let (bridge_stdout, broker_output) = duplex(256 * 1024);
    let mut broker_output = BufReader::new(broker_output);
    let driver = tokio::spawn(run_bridge_websocket(
        bridge_socket,
        bridge_stdin,
        bridge_stdout,
        identity(),
        BrokerMappingExpectationV1::Absent,
        Duration::from_secs(1),
        Duration::from_secs(1),
    ));

    send_broker_line(
        &mut broker_input,
        &request(json!(1), "initialize", json!({})),
    )
    .await;
    let BridgeToControllerV1::Activation(activation) =
        receive_controller_message(&mut controller_socket).await
    else {
        panic!("activation must be first")
    };
    send_activated(&mut controller_socket, &activation).await;
    assert!(matches!(
        receive_controller_message(&mut controller_socket).await,
        BridgeToControllerV1::Acp(_)
    ));
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Acp(
            AcpMessageV1::new(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"agentCapabilities": {"loadSession": true}}
            }))
            .unwrap(),
        )))
        .await
        .unwrap();
    let _ = receive_broker_line(&mut broker_output).await;

    send_broker_line(
        &mut broker_input,
        &request(json!(2), "session/new", json!({})),
    )
    .await;
    assert!(matches!(
        receive_controller_message(&mut controller_socket).await,
        BridgeToControllerV1::Acp(_)
    ));
    controller_socket
        .send(websocket_text(&ControllerToBridgeV1::Acp(
            AcpMessageV1::new(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "sessionId": "s".repeat(
                        openab_kubernetes_session::wire::MAX_WORKER_SESSION_ID_BYTES + 1
                    )
                }
            }))
            .unwrap(),
        )))
        .await
        .unwrap();

    assert!(matches!(
        driver.await.unwrap(),
        Err(BridgeWebSocketError::BridgeProtocol(
            BridgeProtocolError::WorkerResponse(_)
        ))
    ));
    let mut line = String::new();
    assert_eq!(broker_output.read_line(&mut line).await.unwrap(), 0);
}
