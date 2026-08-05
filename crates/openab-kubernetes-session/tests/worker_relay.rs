#![cfg(feature = "worker-runtime")]

#[path = "support/registered_worker.rs"]
mod registered_worker;

use futures_util::{SinkExt, StreamExt};
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, AcpMessageV1, ControllerToWorkerV1, ProtocolResultV1,
    WorkerToControllerV1,
};
use openab_kubernetes_session::worker::relay::{relay_child, WorkerRelayError};
use registered_worker::{registered_worker_pair, ControllerSocket};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{duplex, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

fn controller_acp(value: Value) -> Message {
    let message = ControllerToWorkerV1::Acp(AcpMessageV1::new(value).unwrap());
    Message::Text(String::from_utf8(encode_frame(&message).unwrap()).unwrap())
}

async fn next_worker_acp(socket: &mut ControllerSocket) -> Value {
    let frame = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("worker ACP frame timed out")
        .expect("worker socket ended before ACP")
        .expect("worker socket failed before ACP");
    let Message::Text(text) = frame else {
        panic!("worker relay must emit a text application frame");
    };
    let WorkerToControllerV1::Acp(message) =
        decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap()
    else {
        panic!("a registered worker must emit only ACP frames");
    };
    message.into_payload()
}

#[tokio::test]
async fn real_registered_worker_relays_inner_acp_in_both_directions_fifo() {
    let (registered, mut controller) = registered_worker_pair().await;
    let (mut child_stdout, relay_stdout) = duplex(4096);
    let (relay_stdin, child_stdin) = duplex(4096);
    let mut child_stdin = BufReader::new(child_stdin);
    let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));

    let child_values = [
        json!({"jsonrpc": "2.0", "id": 1, "method": "session/prompt"}),
        json!({"kind": "protocol_result", "payload": {"still": "inner ACP"}}),
    ];
    for (index, value) in child_values.iter().enumerate() {
        child_stdout
            .write_all(&serde_json::to_vec(value).unwrap())
            .await
            .unwrap();
        child_stdout
            .write_all(if index == 0 { b"\r\n" } else { b"\n" })
            .await
            .unwrap();
    }
    for expected in &child_values {
        assert_eq!(next_worker_acp(&mut controller).await, *expected);
    }

    let controller_values = [
        json!({"kind": "registration", "payload": {"still": "inner ACP"}}),
        json!({"jsonrpc": "2.0", "id": 2, "result": {"ok": true}}),
    ];
    for value in &controller_values {
        timeout(TEST_TIMEOUT, controller.send(controller_acp(value.clone())))
            .await
            .expect("controller ACP send timed out")
            .unwrap();
    }
    let mut line = String::new();
    for expected in &controller_values {
        assert!(
            timeout(TEST_TIMEOUT, child_stdin.read_line(&mut line))
                .await
                .expect("child stdin line timed out")
                .unwrap()
                > 0
        );
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), *expected);
        assert!(line.ends_with('\n'));
        line.clear();
    }

    drop(child_stdout);
    assert_eq!(
        timeout(TEST_TIMEOUT, relay)
            .await
            .expect("clean child EOF did not stop the relay")
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        timeout(TEST_TIMEOUT, child_stdin.read_line(&mut line))
            .await
            .expect("relay did not close child stdin")
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn clean_child_eof_stops_the_real_registered_socket() {
    let (registered, mut controller) = registered_worker_pair().await;

    assert_eq!(
        timeout(
            TEST_TIMEOUT,
            relay_child(registered, tokio::io::empty(), tokio::io::sink()),
        )
        .await
        .expect("clean child EOF did not stop the relay"),
        Ok(())
    );
    let closed = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("registered socket stayed open after child EOF");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[tokio::test]
async fn post_ack_protocol_result_is_terminal_and_never_reaches_child() {
    let (registered, mut controller) = registered_worker_pair().await;
    let (mut child_stdout, relay_stdout) = duplex(64);
    let (relay_stdin, mut child_stdin) = duplex(64);
    let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));
    let duplicate_ack = ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());

    timeout(
        TEST_TIMEOUT,
        controller.send(Message::Text(
            String::from_utf8(encode_frame(&duplicate_ack).unwrap()).unwrap(),
        )),
    )
    .await
    .expect("post-ACK ProtocolResult send timed out")
    .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, relay)
            .await
            .expect("post-ACK ProtocolResult did not stop the relay")
            .unwrap()
            .unwrap_err(),
        WorkerRelayError::DuplicateProtocolResult
    );

    let mut child_bytes = Vec::new();
    timeout(TEST_TIMEOUT, child_stdin.read_to_end(&mut child_bytes))
        .await
        .expect("relay did not close child stdin")
        .unwrap();
    assert!(child_bytes.is_empty());
    assert!(child_stdout.write_all(b"{\"late\":true}\n").await.is_err());
}
