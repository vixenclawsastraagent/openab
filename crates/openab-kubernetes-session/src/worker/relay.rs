//! Bounded, one-shot ACP relay for a registered worker.

use super::registration::RegisteredWorker;
use crate::bridge::{parse_logical_message, BridgeProtocolError, MAX_LOGICAL_MESSAGE_BYTES};
use crate::wire::{
    decode_frame, encode_frame, AcpMessageV1, ControllerToWorkerV1, WireProtocolError,
    WorkerToControllerV1,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{self, Message};

pub const WORKER_RELAY_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerRelayError {
    #[error("worker child stdout could not be read")]
    ChildRead,
    #[error("worker child ACP line exceeds the logical message limit")]
    ChildLineTooLarge,
    #[error("worker child stdout ended with a truncated ACP line")]
    TruncatedChildLine,
    #[error("worker child emitted an invalid ACP message")]
    InvalidChildMessage,
    #[error("worker relay WebSocket closed")]
    ControllerClosed,
    #[error("worker relay WebSocket transport failed")]
    WebSocketTransport,
    #[error("worker relay requires text application frames")]
    ExpectedTextFrame,
    #[error("controller sent an invalid worker relay frame")]
    InvalidControllerFrame,
    #[error("controller sent a duplicate worker protocol result")]
    DuplicateProtocolResult,
    #[error("worker relay could not encode an outbound frame")]
    OutboundFrame,
    #[error("worker relay WebSocket write timed out")]
    WebSocketWriteTimedOut,
    #[error("worker child stdin could not be written")]
    ChildWrite,
    #[error("worker child stdin write timed out")]
    ChildWriteTimedOut,
}

struct ChildLines<R> {
    reader: BufReader<R>,
    pending: Vec<u8>,
    trailing_cr: bool,
}

#[derive(Debug)]
enum ChildRead {
    Progress,
    Line(Vec<u8>),
    Eof,
}

impl<R> ChildLines<R>
where
    R: AsyncRead + Unpin,
{
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            pending: Vec::new(),
            trailing_cr: false,
        }
    }

    /// Consume at most one bounded `BufReader` chunk. State changes only after
    /// `fill_buf` returns, so cancelling this future never loses child bytes.
    async fn read_step(&mut self) -> Result<ChildRead, WorkerRelayError> {
        let available = self
            .reader
            .fill_buf()
            .await
            .map_err(|_| WorkerRelayError::ChildRead)?;
        if available.is_empty() {
            return if self.pending.is_empty() && !self.trailing_cr {
                Ok(ChildRead::Eof)
            } else {
                Err(WorkerRelayError::TruncatedChildLine)
            };
        }

        if self.trailing_cr {
            if available[0] == b'\n' {
                self.trailing_cr = false;
                self.reader.consume(1);
                return Ok(ChildRead::Line(std::mem::take(&mut self.pending)));
            }
            let projected = self
                .pending
                .len()
                .checked_add(1)
                .ok_or(WorkerRelayError::ChildLineTooLarge)?;
            if projected > MAX_LOGICAL_MESSAGE_BYTES {
                return Err(WorkerRelayError::ChildLineTooLarge);
            }
            self.pending.push(b'\r');
            self.trailing_cr = false;
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let mut payload_end = newline.unwrap_or(available.len());
        let trailing_cr = newline.is_none() && available.last() == Some(&b'\r');
        if (newline.is_some() || trailing_cr)
            && payload_end > 0
            && available[payload_end - 1] == b'\r'
        {
            payload_end -= 1;
        }
        let projected = self
            .pending
            .len()
            .checked_add(payload_end)
            .ok_or(WorkerRelayError::ChildLineTooLarge)?;
        if projected > MAX_LOGICAL_MESSAGE_BYTES {
            return Err(WorkerRelayError::ChildLineTooLarge);
        }
        self.pending.extend_from_slice(&available[..payload_end]);
        self.trailing_cr = trailing_cr;
        self.reader.consume(consumed);
        if newline.is_some() {
            Ok(ChildRead::Line(std::mem::take(&mut self.pending)))
        } else {
            Ok(ChildRead::Progress)
        }
    }
}

fn parse_child_line(bytes: &[u8]) -> Result<AcpMessageV1, WorkerRelayError> {
    let payload = parse_logical_message(bytes).map_err(|error| match error {
        BridgeProtocolError::MessageTooLarge { .. } => WorkerRelayError::ChildLineTooLarge,
        _ => WorkerRelayError::InvalidChildMessage,
    })?;
    AcpMessageV1::new(payload).map_err(|error| match error {
        WireProtocolError::AcpPayloadTooLarge { .. } | WireProtocolError::FrameTooLarge { .. } => {
            WorkerRelayError::ChildLineTooLarge
        }
        _ => WorkerRelayError::InvalidChildMessage,
    })
}

/// Relay one registered worker socket to one already-spawned ACP child's
/// stdout/stdin. Each direction retains at most one logical ACP message and
/// completes its downstream write before polling the next message. Returning
/// cancels the other scoped direction and drops both socket halves; this
/// function never reconnects or replays.
pub async fn relay_child<S, R, W>(
    registered: RegisteredWorker<S>,
    child_stdout: R,
    child_stdin: W,
) -> Result<(), WorkerRelayError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let socket = registered.into_socket();
    let (flush_requests, flush_request_receiver) = mpsc::channel(1);
    let (sink, stream) = socket.split();
    let controller_to_child = read_controller_frames(stream, child_stdin, flush_requests);
    let child_to_controller =
        write_child_frames(sink, ChildLines::new(child_stdout), flush_request_receiver);
    tokio::pin!(controller_to_child, child_to_controller);
    tokio::select! {
        biased;
        result = &mut controller_to_child => result,
        result = &mut child_to_controller => result,
    }
}

async fn read_controller_frames<R, W>(
    mut stream: R,
    mut child_stdin: W,
    flush_requests: mpsc::Sender<()>,
) -> Result<(), WorkerRelayError>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let frame = stream
            .next()
            .await
            .ok_or(WorkerRelayError::ControllerClosed)?
            .map_err(|_| WorkerRelayError::WebSocketTransport)?;
        match frame {
            Message::Text(text) => {
                let message = decode_frame::<ControllerToWorkerV1>(text.as_bytes())
                    .map_err(|_| WorkerRelayError::InvalidControllerFrame)?;
                drop(text);
                match message {
                    ControllerToWorkerV1::Acp(message) => {
                        write_child_message(&mut child_stdin, message).await?
                    }
                    ControllerToWorkerV1::ProtocolResult(_) => {
                        return Err(WorkerRelayError::DuplicateProtocolResult)
                    }
                }
            }
            Message::Ping(_) => match flush_requests.try_send(()) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(WorkerRelayError::WebSocketTransport)
                }
            },
            Message::Pong(_) => {}
            Message::Close(_) => return Err(WorkerRelayError::ControllerClosed),
            Message::Binary(_) | Message::Frame(_) => {
                return Err(WorkerRelayError::ExpectedTextFrame)
            }
        }
    }
}

async fn write_child_message<W>(
    writer: &mut W,
    message: AcpMessageV1,
) -> Result<(), WorkerRelayError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(message.payload())
        .map_err(|_| WorkerRelayError::InvalidControllerFrame)?;
    if bytes.len() != message.encoded_payload_bytes() || bytes.len() > MAX_LOGICAL_MESSAGE_BYTES {
        return Err(WorkerRelayError::InvalidControllerFrame);
    }
    drop(message);
    match timeout(WORKER_RELAY_WRITE_TIMEOUT, async {
        writer.write_all(&bytes).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(WorkerRelayError::ChildWrite),
        Err(_) => Err(WorkerRelayError::ChildWriteTimedOut),
    }
}

async fn write_child_frames<W, R>(
    mut sink: W,
    mut child_lines: ChildLines<R>,
    mut flush_requests: mpsc::Receiver<()>,
) -> Result<(), WorkerRelayError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
    R: AsyncRead + Unpin,
{
    let mut flush_requests_open = true;
    loop {
        tokio::select! {
            biased;
            child = child_lines.read_step() => {
                match child? {
                    ChildRead::Progress => {}
                    ChildRead::Line(line) => {
                        let message = parse_child_line(&line)?;
                        drop(line);
                        send_worker_message(&mut sink, message).await?;
                    }
                    ChildRead::Eof => return Ok(()),
                }
            }
            request = flush_requests.recv(), if flush_requests_open => {
                match request {
                    Some(()) => flush_worker_socket(&mut sink).await?,
                    None => flush_requests_open = false,
                }
            }
        }
    }
}

async fn send_worker_message<W>(sink: &mut W, message: AcpMessageV1) -> Result<(), WorkerRelayError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let frame = WorkerToControllerV1::Acp(message);
    let bytes = encode_frame(&frame).map_err(|_| WorkerRelayError::OutboundFrame)?;
    drop(frame);
    let text = String::from_utf8(bytes).map_err(|_| WorkerRelayError::OutboundFrame)?;
    match timeout(WORKER_RELAY_WRITE_TIMEOUT, sink.send(Message::Text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(WorkerRelayError::WebSocketTransport),
        Err(_) => Err(WorkerRelayError::WebSocketWriteTimedOut),
    }
}

async fn flush_worker_socket<W>(sink: &mut W) -> Result<(), WorkerRelayError>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    match timeout(WORKER_RELAY_WRITE_TIMEOUT, sink.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(WorkerRelayError::WebSocketTransport),
        Err(_) => Err(WorkerRelayError::WebSocketWriteTimedOut),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MAX_LOGICAL_MESSAGE_BYTES;
    use crate::wire::{
        decode_frame, encode_frame, ControllerToWorkerV1, FatalCode, ProtocolResultV1,
        WorkerToControllerV1,
    };
    use crate::worker::bootstrap::WorkerCommand;
    use crate::worker::registration::RegisteredWorker;
    use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{
        duplex, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
        DuplexStream, ReadBuf,
    };
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    async fn next_event<R>(lines: &mut ChildLines<R>) -> Result<ChildRead, WorkerRelayError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        loop {
            match lines.read_step().await? {
                ChildRead::Progress => {}
                event => return Ok(event),
            }
        }
    }

    #[tokio::test]
    async fn child_lines_accept_lf_crlf_split_reads_and_fail_closed_on_truncation() {
        let (mut writer, reader) = duplex(16);
        let write = tokio::spawn(async move {
            for byte in b"{\"a\":1}\n[1]\r\ntrue\r \nfalse" {
                writer.write_all(&[*byte]).await.unwrap();
            }
        });
        let mut lines = ChildLines::new(reader);

        for (expected_line, expected_value) in [
            (&b"{\"a\":1}"[..], json!({"a": 1})),
            (&b"[1]"[..], json!([1])),
            (&b"true\r "[..], json!(true)),
        ] {
            let ChildRead::Line(line) = next_event(&mut lines).await.unwrap() else {
                panic!("expected one complete child line");
            };
            assert_eq!(line, expected_line);
            assert_eq!(parse_child_line(&line).unwrap().payload(), &expected_value);
        }
        assert_eq!(
            next_event(&mut lines).await.unwrap_err(),
            WorkerRelayError::TruncatedChildLine
        );
        write.await.unwrap();

        let mut empty = ChildLines::new(tokio::io::empty());
        assert!(matches!(
            next_event(&mut empty).await.unwrap(),
            ChildRead::Eof
        ));
        assert_eq!(
            parse_child_line(b"\n").unwrap_err(),
            WorkerRelayError::InvalidChildMessage
        );
    }

    fn object_with_encoded_len(length: usize) -> Vec<u8> {
        const PREFIX: &[u8] = b"{\"data\":\"";
        const SUFFIX: &[u8] = b"\"}";
        assert!(length >= PREFIX.len() + SUFFIX.len());
        let mut value = Vec::with_capacity(length);
        value.extend_from_slice(PREFIX);
        value.resize(length - SUFFIX.len(), b'a');
        value.extend_from_slice(SUFFIX);
        assert_eq!(value.len(), length);
        value
    }

    #[tokio::test]
    async fn exact_logical_limit_is_accepted_and_plus_one_is_rejected_before_extend() {
        for terminator in [&b"\n"[..], &b"\r\n"[..]] {
            let mut exact = object_with_encoded_len(MAX_LOGICAL_MESSAGE_BYTES);
            exact.extend_from_slice(terminator);
            let mut lines = ChildLines::new(exact.as_slice());
            let ChildRead::Line(line) = next_event(&mut lines).await.unwrap() else {
                panic!("exact-limit child record must complete");
            };
            assert_eq!(line.len(), MAX_LOGICAL_MESSAGE_BYTES);
            assert!(line.capacity() <= MAX_LOGICAL_MESSAGE_BYTES);
            let message = parse_child_line(&line).unwrap();
            assert_eq!(message.encoded_payload_bytes(), MAX_LOGICAL_MESSAGE_BYTES);
        }

        let mut oversized = object_with_encoded_len(MAX_LOGICAL_MESSAGE_BYTES + 1);
        oversized.push(b'\n');
        let mut lines = ChildLines::new(oversized.as_slice());
        let error = loop {
            match lines.read_step().await {
                Ok(ChildRead::Progress) => {}
                Ok(_) => panic!("oversized line must not be accepted"),
                Err(error) => break error,
            }
        };
        assert_eq!(error, WorkerRelayError::ChildLineTooLarge);
        assert!(lines.pending.len() <= MAX_LOGICAL_MESSAGE_BYTES);
    }

    fn command() -> WorkerCommand {
        WorkerCommand::parse([
            OsString::from("serve"),
            OsString::from("--"),
            OsString::from("/usr/local/bin/acp-test"),
        ])
        .unwrap()
    }

    async fn registered_pair() -> (
        RegisteredWorker<DuplexStream>,
        WebSocketStream<DuplexStream>,
    ) {
        let (worker_io, controller_io) = duplex(256 * 1024);
        let worker = WebSocketStream::from_raw_socket(
            worker_io,
            Role::Client,
            Some(crate::client_transport::client_websocket_config()),
        )
        .await;
        let controller = WebSocketStream::from_raw_socket(
            controller_io,
            Role::Server,
            Some(crate::client_transport::client_websocket_config()),
        )
        .await;
        (RegisteredWorker::for_test(worker, command()), controller)
    }

    async fn registered_pair_with_blocked_worker_writes() -> (
        RegisteredWorker<GateWrites<DuplexStream>>,
        WebSocketStream<DuplexStream>,
        Arc<AtomicUsize>,
    ) {
        let writes = Arc::new(AtomicUsize::new(0));
        let (worker_io, controller_io) = duplex(4096);
        let worker = WebSocketStream::from_raw_socket(
            GateWrites {
                inner: worker_io,
                writes: Arc::clone(&writes),
            },
            Role::Client,
            Some(crate::client_transport::client_websocket_config()),
        )
        .await;
        let controller = WebSocketStream::from_raw_socket(
            controller_io,
            Role::Server,
            Some(crate::client_transport::client_websocket_config()),
        )
        .await;
        (
            RegisteredWorker::for_test(worker, command()),
            controller,
            writes,
        )
    }

    async fn wait_for_io(counter: &AtomicUsize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the blocked I/O path must be polled");
    }

    fn controller_acp(value: serde_json::Value) -> Message {
        let message = ControllerToWorkerV1::Acp(AcpMessageV1::new(value).unwrap());
        Message::Text(String::from_utf8(encode_frame(&message).unwrap()).unwrap())
    }

    async fn next_worker_acp(socket: &mut WebSocketStream<DuplexStream>) -> serde_json::Value {
        let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
            panic!("worker relay must emit text frames");
        };
        let WorkerToControllerV1::Acp(message) =
            decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap()
        else {
            panic!("registered worker must emit only ACP frames");
        };
        message.into_payload()
    }

    #[tokio::test]
    async fn relay_preserves_fifo_and_only_transports_inner_acp_payloads() {
        let (registered, mut controller) = registered_pair().await;
        let (mut child_stdout, relay_stdout) = duplex(4096);
        let (relay_stdin, child_stdin) = duplex(4096);
        let mut child_stdin = BufReader::new(child_stdin);
        let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));

        let child_values = [
            json!({"kind": "registration", "payload": {"secret": "opaque"}}),
            json!({"jsonrpc": "2.0", "method": "second"}),
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
            json!({"kind": "protocol_result", "payload": {"still": "opaque"}}),
            json!({"jsonrpc": "2.0", "id": 2, "result": {"ok": true}}),
        ];
        for value in &controller_values {
            controller
                .send(controller_acp(value.clone()))
                .await
                .unwrap();
        }
        let mut line = String::new();
        for expected in &controller_values {
            assert!(child_stdin.read_line(&mut line).await.unwrap() > 0);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&line).unwrap(),
                *expected
            );
            assert!(line.ends_with('\n'));
            line.clear();
        }

        drop(child_stdout);
        relay.await.unwrap().unwrap();
        assert_eq!(child_stdin.read_line(&mut line).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn malformed_truncated_and_empty_child_lines_are_terminal() {
        for (bytes, expected) in [
            (&b"\n"[..], WorkerRelayError::InvalidChildMessage),
            (&b"{]\n"[..], WorkerRelayError::InvalidChildMessage),
            (
                &b"{\"valid\":true}"[..],
                WorkerRelayError::TruncatedChildLine,
            ),
        ] {
            let (registered, _controller) = registered_pair().await;
            let (mut child_stdout, relay_stdout) = duplex(64);
            let (relay_stdin, _child_stdin) = duplex(64);
            let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));
            child_stdout.write_all(bytes).await.unwrap();
            drop(child_stdout);
            assert_eq!(relay.await.unwrap().unwrap_err(), expected);
        }
    }

    #[tokio::test]
    async fn terminal_in_either_direction_cancels_the_blocked_peer() {
        let (registered, mut controller) = registered_pair().await;
        let (mut child_stdout, relay_stdout) = duplex(64);
        let writes = Arc::new(AtomicUsize::new(0));
        let child_stdin = PendingWriter {
            writes: Arc::clone(&writes),
        };
        let relay = tokio::spawn(relay_child(registered, relay_stdout, child_stdin));
        controller
            .send(controller_acp(json!({"blocked": true})))
            .await
            .unwrap();
        wait_for_io(&writes).await;
        child_stdout.write_all(b"{]\n").await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .expect("malformed child output must cancel the controller reader")
                .unwrap()
                .unwrap_err(),
            WorkerRelayError::InvalidChildMessage
        );

        let (registered, mut controller, writes) =
            registered_pair_with_blocked_worker_writes().await;
        let (mut child_stdout, relay_stdout) = duplex(64);
        let (relay_stdin, _child_stdin) = duplex(64);
        let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));
        child_stdout
            .write_all(b"{\"blocked\":true}\n")
            .await
            .unwrap();
        wait_for_io(&writes).await;
        let result = ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());
        controller
            .send(Message::Text(
                String::from_utf8(encode_frame(&result).unwrap()).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .expect("duplicate protocol result must cancel the child reader")
                .unwrap()
                .unwrap_err(),
            WorkerRelayError::DuplicateProtocolResult
        );
        assert!(child_stdout.write_all(b"{\"late\":true}\n").await.is_err());
    }

    #[tokio::test]
    async fn ping_is_flushed_as_transport_pong_without_becoming_acp() {
        let (registered, mut controller) = registered_pair().await;
        let (child_stdout, relay_stdout) = duplex(64);
        let relay = tokio::spawn(relay_child(registered, relay_stdout, tokio::io::sink()));

        controller
            .send(Message::Ping(b"transport-only".to_vec()))
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), controller.next())
                .await
                .expect("Ping must be flushed promptly"),
            Some(Ok(Message::Pong(payload))) if payload == b"transport-only"
        ));

        drop(child_stdout);
        relay.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_relay_drops_socket_and_child_pipe_owners() {
        let (registered, mut controller) = registered_pair().await;
        let (_child_stdout, relay_stdout) = duplex(64);
        let (relay_stdin, mut child_stdin) = duplex(64);
        let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));
        tokio::task::yield_now().await;
        relay.abort();
        assert!(relay.await.unwrap_err().is_cancelled());

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), controller.next())
                .await
                .expect("cancelled relay must drop the registered socket"),
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
        let mut child_input = Vec::new();
        child_stdin.read_to_end(&mut child_input).await.unwrap();
        assert!(child_input.is_empty());
    }

    #[tokio::test]
    async fn every_post_ack_protocol_result_is_terminal_before_child_write() {
        let results = [
            ProtocolResultV1::ack(None).unwrap(),
            ProtocolResultV1::fatal(None, FatalCode::Unavailable).unwrap(),
            ProtocolResultV1::ack(Some(uuid::Uuid::from_u128(7))).unwrap(),
        ];
        for result in results {
            let (registered, mut controller) = registered_pair().await;
            let (_child_stdout, relay_stdout) = duplex(64);
            let (relay_stdin, mut child_stdin) = duplex(64);
            let relay = tokio::spawn(relay_child(registered, relay_stdout, relay_stdin));
            let message = ControllerToWorkerV1::ProtocolResult(result);
            controller
                .send(Message::Text(
                    String::from_utf8(encode_frame(&message).unwrap()).unwrap(),
                ))
                .await
                .unwrap();
            assert_eq!(
                relay.await.unwrap().unwrap_err(),
                WorkerRelayError::DuplicateProtocolResult
            );
            let mut bytes = Vec::new();
            child_stdin.read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.is_empty());
        }
    }

    #[tokio::test]
    async fn clean_child_eof_stops_the_registered_socket_without_reconnect() {
        let (registered, mut controller) = registered_pair().await;
        relay_child(registered, tokio::io::empty(), tokio::io::sink())
            .await
            .unwrap();
        assert!(matches!(
            controller.next().await,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
    }

    struct ChunkReader {
        chunks: VecDeque<Vec<u8>>,
        polls: Arc<AtomicUsize>,
    }

    impl AsyncRead for ChunkReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, Ordering::SeqCst);
            if let Some(chunk) = this.chunks.pop_front() {
                buffer.put_slice(&chunk);
            }
            Poll::Ready(Ok(()))
        }
    }

    struct PendingFlushSink {
        sends: Arc<AtomicUsize>,
    }

    impl Sink<Message> for PendingFlushSink {
        type Error = tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    struct CountingStream {
        frames: VecDeque<Result<Message, tungstenite::Error>>,
        polls: Arc<AtomicUsize>,
    }

    impl Stream for CountingStream {
        type Item = Result<Message, tungstenite::Error>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(this.frames.pop_front())
        }
    }

    struct PendingWriter {
        writes: Arc<AtomicUsize>,
    }

    struct PartialThenPendingWriter {
        writes: Arc<AtomicUsize>,
        progressed: bool,
    }

    impl AsyncWrite for PartialThenPendingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if buffer.is_empty() {
                return Poll::Ready(Ok(0));
            }
            if self.progressed {
                Poll::Pending
            } else {
                self.progressed = true;
                Poll::Ready(Ok(1))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct GateWrites<S> {
        inner: S,
        writes: Arc<AtomicUsize>,
    }

    impl<S> AsyncRead for GateWrites<S>
    where
        S: AsyncRead + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(context, buffer)
        }
    }

    impl<S> AsyncWrite for GateWrites<S>
    where
        S: Unpin,
    {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn blocked_websocket_send_does_not_poll_the_second_child_line() {
        let polls = Arc::new(AtomicUsize::new(0));
        let sends = Arc::new(AtomicUsize::new(0));
        let reader = ChunkReader {
            chunks: VecDeque::from([
                b"{\"first\":true}\n".to_vec(),
                b"{\"second\":true}\n".to_vec(),
            ]),
            polls: Arc::clone(&polls),
        };
        let sink = PendingFlushSink {
            sends: Arc::clone(&sends),
        };
        let (_flush_sender, flush_receiver) = mpsc::channel(1);
        let pump = write_child_frames(sink, ChildLines::new(reader), flush_receiver);
        tokio::pin!(pump);
        tokio::select! {
            result = &mut pump => panic!("blocked first send completed unexpectedly: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }

        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(sends.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn blocked_child_write_does_not_poll_the_second_controller_frame() {
        let polls = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let stream = CountingStream {
            frames: VecDeque::from([
                Ok(controller_acp(json!({"first": true}))),
                Ok(controller_acp(json!({"second": true}))),
            ]),
            polls: Arc::clone(&polls),
        };
        let writer = PendingWriter {
            writes: Arc::clone(&writes),
        };
        let (flush_sender, _flush_receiver) = mpsc::channel(1);
        let pump = read_controller_frames(stream, writer, flush_sender);
        tokio::pin!(pump);
        tokio::select! {
            result = &mut pump => panic!("blocked first write completed unexpectedly: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }

        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(writes.load(Ordering::SeqCst) >= 1);
    }

    struct PayloadRejectingSink;

    impl Sink<Message> for PayloadRejectingSink {
        type Error = tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            Err(tungstenite::Error::WriteBufferFull(item))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn payload_owning_transport_errors_are_discarded_and_sanitized() {
        const SECRET: &str = "SECRET-MUST-NOT-APPEAR";
        let mut sink = PayloadRejectingSink;
        let message = AcpMessageV1::new(json!({"secret": SECRET})).unwrap();
        let error = send_worker_message(&mut sink, message).await.unwrap_err();

        assert_eq!(error, WorkerRelayError::WebSocketTransport);
        assert!(!format!("{error:?} {error}").contains(SECRET));

        let close = tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: SECRET.into(),
        };
        for frames in [
            vec![Err(tungstenite::Error::WriteBufferFull(Message::Text(
                SECRET.to_owned(),
            )))],
            vec![Err(tungstenite::Error::Io(io::Error::other(SECRET)))],
            vec![Ok(Message::Close(Some(close)))],
        ] {
            let (flush_sender, _flush_receiver) = mpsc::channel(1);
            let error = read_controller_frames(
                futures_util::stream::iter(frames),
                tokio::io::sink(),
                flush_sender,
            )
            .await
            .unwrap_err();
            assert!(!format!("{error:?} {error}").contains(SECRET));
        }
    }

    #[tokio::test]
    async fn non_text_invalid_and_closed_controller_frames_are_terminal() {
        for (frames, expected) in [
            (
                vec![Ok(Message::Binary(br#"{"valid":"json"}"#.to_vec()))],
                WorkerRelayError::ExpectedTextFrame,
            ),
            (
                vec![Ok(Message::Text("not-json".to_owned()))],
                WorkerRelayError::InvalidControllerFrame,
            ),
            (
                vec![Ok(Message::Close(None))],
                WorkerRelayError::ControllerClosed,
            ),
            (vec![], WorkerRelayError::ControllerClosed),
        ] {
            let (flush_sender, _flush_receiver) = mpsc::channel(1);
            assert_eq!(
                read_controller_frames(
                    futures_util::stream::iter(frames),
                    tokio::io::sink(),
                    flush_sender,
                )
                .await
                .unwrap_err(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn ping_flush_requests_are_coalesced_and_never_become_acp() {
        let frames = vec![Ok(Message::Ping(vec![1])), Ok(Message::Ping(vec![2]))];
        let (flush_sender, mut flush_receiver) = mpsc::channel(1);
        assert_eq!(
            read_controller_frames(
                futures_util::stream::iter(frames),
                tokio::io::sink(),
                flush_sender,
            )
            .await
            .unwrap_err(),
            WorkerRelayError::ControllerClosed
        );
        assert_eq!(flush_receiver.recv().await, Some(()));
        assert_eq!(flush_receiver.recv().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn one_fixed_deadline_bounds_each_uncertain_write() {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut writer = PartialThenPendingWriter {
            writes: Arc::clone(&writes),
            progressed: false,
        };
        let child_write = write_child_message(
            &mut writer,
            AcpMessageV1::new(json!({"slow": true})).unwrap(),
        );
        tokio::pin!(child_write);
        assert!(child_write.as_mut().now_or_never().is_none());
        tokio::time::advance(WORKER_RELAY_WRITE_TIMEOUT).await;
        assert_eq!(
            child_write.await.unwrap_err(),
            WorkerRelayError::ChildWriteTimedOut
        );
        assert!(writes.load(Ordering::SeqCst) >= 2);

        let sends = Arc::new(AtomicUsize::new(0));
        let mut sink = PendingFlushSink {
            sends: Arc::clone(&sends),
        };
        let websocket_write =
            send_worker_message(&mut sink, AcpMessageV1::new(json!({"slow": true})).unwrap());
        tokio::pin!(websocket_write);
        assert!(websocket_write.as_mut().now_or_never().is_none());
        tokio::time::advance(WORKER_RELAY_WRITE_TIMEOUT).await;
        assert_eq!(
            websocket_write.await.unwrap_err(),
            WorkerRelayError::WebSocketWriteTimedOut
        );
        assert_eq!(sends.load(Ordering::SeqCst), 1);
    }
}
