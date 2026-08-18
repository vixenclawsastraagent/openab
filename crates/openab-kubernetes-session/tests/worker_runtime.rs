#![cfg(all(feature = "worker-runtime", feature = "fake-acp", unix))]

#[cfg(target_os = "linux")]
use futures_util::SinkExt;
use futures_util::StreamExt;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::client_transport::client_websocket_config;
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::Fence;
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, WorkerRegistrationV1, WorkerToControllerV1,
};
#[cfg(target_os = "linux")]
use openab_kubernetes_session::wire::{AcpMessageV1, ControllerToWorkerV1, ProtocolResultV1};
use openab_kubernetes_session::worker::bootstrap::{
    WorkerBootstrap, WorkerBootstrapEnvironment, WorkerCommand,
};
use openab_kubernetes_session::worker::registration::WorkerRegistrationError;
#[cfg(target_os = "linux")]
use openab_kubernetes_session::worker::supervisor::WorkerSupervisionError;
use openab_kubernetes_session::worker::workspace::{
    prepare_workspace_beneath, PreparedWorkspace, WorkspaceIdentity,
};
use openab_kubernetes_session::worker::{
    run_worker_once, TerminationSignalError, WorkerRuntimeError,
};
use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
use rustix::fd::OwnedFd;
use rustix::fs::{Mode, OFlags};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
#[cfg(target_os = "linux")]
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::header::{
    AUTHORIZATION, CONNECTION, HOST, ORIGIN, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
    SEC_WEBSOCKET_VERSION, UPGRADE,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_hdr_async_with_config, WebSocketStream};
use uuid::Uuid;
use worker_tokio_rustls::server::TlsStream as ServerTlsStream;
use worker_tokio_rustls::TlsAcceptor;

const POD_UID: &str = "4db5a02c-74e2-4a27-838f-7f3483c541a9";
const TOKEN: &[u8; 32] = b"secret-token-must-never-appear!!";
const TOKEN_HEX: &str = "7365637265742d746f6b656e2d6d7573742d6e657665722d6170706561722121";
#[cfg(target_os = "linux")]
const SESSION_ID: &str = "openab-fake-session-v1";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(target_os = "linux")]
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);
const NO_RETRY_WINDOW: Duration = Duration::from_millis(150);

type ControllerSocket = WebSocketStream<ServerTlsStream<TcpStream>>;

struct TemporaryWorkspace {
    parent: PathBuf,
}

impl TemporaryWorkspace {
    fn prepare() -> (Self, PreparedWorkspace) {
        let parent = std::env::temp_dir().join(format!("openab-worker-runtime-{}", Uuid::new_v4()));
        fs::create_dir(&parent).unwrap();
        fs::create_dir(parent.join("session")).unwrap();
        let parent_fd = open_directory(&parent);
        let workspace = prepare_workspace_beneath(parent_fd, WorkspaceIdentity::current()).unwrap();
        (Self { parent }, workspace)
    }

    #[cfg(target_os = "linux")]
    fn workspace(&self) -> PathBuf {
        self.parent.join("session/workspace")
    }
}

impl Drop for TemporaryWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.parent);
    }
}

struct RuntimeFixture {
    temporary: TemporaryWorkspace,
    workspace: PreparedWorkspace,
    bootstrap: WorkerBootstrap,
    listener: TcpListener,
    identity: CertifiedKey<KeyPair>,
    port: u16,
}

impl RuntimeFixture {
    async fn new(command: WorkerCommand) -> Self {
        let identity = tls_identity();
        let ca_pem = identity.cert.pem();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let bootstrap =
            bootstrap_with_command(command, format!("wss://localhost:{port}/v1/worker"), ca_pem);
        let (temporary, workspace) = TemporaryWorkspace::prepare();
        Self {
            temporary,
            workspace,
            bootstrap,
            listener,
            identity,
            port,
        }
    }
}

fn open_directory(path: &Path) -> OwnedFd {
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap()
}

fn command(executable: impl Into<OsString>) -> WorkerCommand {
    WorkerCommand::parse([
        OsString::from("serve"),
        OsString::from("--"),
        executable.into(),
    ])
    .unwrap()
}

#[cfg(target_os = "linux")]
fn fake_command() -> WorkerCommand {
    command(env!("CARGO_BIN_EXE_openab-kubernetes-session-fake-acp"))
}

#[cfg(target_os = "linux")]
fn failing_fake_command() -> WorkerCommand {
    WorkerCommand::parse([
        OsString::from("serve"),
        OsString::from("--"),
        OsString::from(env!("CARGO_BIN_EXE_openab-kubernetes-session-fake-acp")),
        OsString::from("unexpected-argument"),
    ])
    .unwrap()
}

fn registration() -> WorkerRegistrationV1 {
    let binding = SessionBinding::new(
        ScopeId::derive("team-sensitive"),
        SessionId::derive("team-sensitive", "discord:thread-sensitive"),
        Fence::new(7, Uuid::from_u128(100)).unwrap(),
        Uuid::from_u128(200),
    )
    .unwrap();
    WorkerRegistrationV1::new(&binding)
}

fn bootstrap_with_command(command: WorkerCommand, url: String, ca_pem: String) -> WorkerBootstrap {
    let values = BTreeMap::from([
        ("OPENAB_SESSION_CONTROLLER_URL", OsString::from(url)),
        (
            "OPENAB_SESSION_CONTROLLER_CA_FILE",
            OsString::from("/var/run/openab-controller-ca/ca.crt"),
        ),
        (
            "OPENAB_REGISTRATION_TOKEN_FILE",
            OsString::from("/var/run/openab-registration/token"),
        ),
        (
            "OPENAB_REGISTRATION_BINDING_FILE",
            OsString::from("/var/run/openab-registration/binding.json"),
        ),
        ("OPENAB_WORKER_POD_UID", OsString::from(POD_UID)),
        ("OPENAB_SESSION_ROOT", OsString::from("/session")),
        ("OPENAB_WORKSPACE", OsString::from("/session/workspace")),
        ("HOME", OsString::from("/session/home")),
    ]);
    let environment =
        WorkerBootstrapEnvironment::from_lookup(|name| values.get(name).cloned()).unwrap();
    WorkerBootstrap::load_from_readers(
        command,
        environment,
        Cursor::new(TOKEN),
        Cursor::new(encode_frame(&registration()).unwrap()),
        Cursor::new(ca_pem),
    )
    .unwrap()
}

fn tls_identity() -> CertifiedKey<KeyPair> {
    generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap()
}

fn tls_server_config(identity: &CertifiedKey<KeyPair>) -> Arc<ServerConfig> {
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![identity.cert.der().clone()], key)
            .unwrap(),
    )
}

struct ExactRequestCallback {
    port: u16,
}

impl Callback for ExactRequestCallback {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(request.method(), "GET");
        assert_eq!(request.uri(), "/v1/worker");
        assert_eq!(request.headers().len(), 7);
        assert_eq!(request.headers()[HOST], format!("localhost:{}", self.port));
        assert_eq!(request.headers()[CONNECTION], "Upgrade");
        assert_eq!(request.headers()[UPGRADE], "websocket");
        assert_eq!(request.headers()[SEC_WEBSOCKET_VERSION], "13");
        assert_eq!(request.headers()[SEC_WEBSOCKET_KEY].as_bytes().len(), 24);
        assert_eq!(
            request.headers()[AUTHORIZATION],
            format!("Bearer {TOKEN_HEX}")
        );
        assert_eq!(request.headers()["x-openab-pod-uid"], POD_UID);
        assert!(!request.headers().contains_key(ORIGIN));
        assert!(!request.headers().contains_key(SEC_WEBSOCKET_PROTOCOL));
        Ok(response)
    }
}

async fn accept_registration(
    listener: &TcpListener,
    identity: &CertifiedKey<KeyPair>,
    port: u16,
) -> ControllerSocket {
    timeout(TEST_TIMEOUT, async {
        let (stream, _) = listener.accept().await.unwrap();
        let tls = TlsAcceptor::from(tls_server_config(identity))
            .accept(stream)
            .await
            .unwrap();
        assert_eq!(tls.get_ref().1.server_name(), Some("localhost"));
        let mut socket = accept_hdr_async_with_config(
            tls,
            ExactRequestCallback { port },
            Some(client_websocket_config()),
        )
        .await
        .unwrap();
        let first = socket
            .next()
            .await
            .expect("worker disconnected before registration")
            .unwrap();
        let Message::Text(text) = first else {
            panic!("Registration must be the first text application frame");
        };
        assert_eq!(
            decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap(),
            WorkerToControllerV1::Registration(registration())
        );
        socket
    })
    .await
    .expect("worker TLS upgrade and registration timed out")
}

#[cfg(target_os = "linux")]
async fn send_ack(socket: &mut ControllerSocket) {
    let ack = ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());
    timeout(
        TEST_TIMEOUT,
        socket.send(Message::Text(
            String::from_utf8(encode_frame(&ack).unwrap()).unwrap(),
        )),
    )
    .await
    .expect("worker acknowledgement write timed out")
    .unwrap();
}

#[cfg(target_os = "linux")]
async fn send_acp(socket: &mut ControllerSocket, value: Value) {
    let frame = ControllerToWorkerV1::Acp(AcpMessageV1::new(value).unwrap());
    timeout(
        TEST_TIMEOUT,
        socket.send(Message::Text(
            String::from_utf8(encode_frame(&frame).unwrap()).unwrap(),
        )),
    )
    .await
    .expect("controller ACP write timed out")
    .unwrap();
}

#[cfg(target_os = "linux")]
async fn next_acp(socket: &mut ControllerSocket) -> Value {
    let frame = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("worker ACP response timed out")
        .expect("worker socket ended before ACP response")
        .expect("worker socket failed before ACP response");
    let Message::Text(text) = frame else {
        panic!("worker must emit ACP as a text application frame");
    };
    let WorkerToControllerV1::Acp(message) =
        decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap()
    else {
        panic!("acknowledged worker emitted a control frame");
    };
    message.into_payload()
}

#[cfg(target_os = "linux")]
async fn expect_socket_closed(socket: &mut ControllerSocket) {
    let closed = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("worker lane remained open after its terminal event");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

async fn assert_no_retry(listener: &TcpListener) {
    assert!(timeout(NO_RETRY_WINDOW, listener.accept()).await.is_err());
}

#[cfg(target_os = "linux")]
fn initialize(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "openab-test", "version": "1"}
        }
    })
}

#[cfg(target_os = "linux")]
fn new_session(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/new",
        "params": {
            "cwd": "/session/workspace",
            "mcpServers": [],
            "additionalDirectories": []
        }
    })
}

#[cfg(target_os = "linux")]
fn prompt(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/prompt",
        "params": {"sessionId": SESSION_ID, "prompt": []}
    })
}

#[cfg(target_os = "linux")]
fn cancel() -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": SESSION_ID}
    })
}

#[cfg(target_os = "linux")]
fn close_session(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/close",
        "params": {"sessionId": SESSION_ID}
    })
}

#[tokio::test]
async fn shutdown_before_connect_is_clean_and_consumes_the_attempt() {
    let RuntimeFixture {
        temporary: _temporary,
        workspace,
        bootstrap,
        listener,
        identity: _identity,
        port: _port,
    } = RuntimeFixture::new(command("/must/not/start")).await;

    assert_eq!(
        timeout(
            TEST_TIMEOUT,
            run_worker_once(bootstrap, workspace, future::ready(Ok(())))
        )
        .await
        .expect("latched worker shutdown timed out"),
        Ok(())
    );
    assert_no_retry(&listener).await;
}

#[tokio::test]
async fn connection_loss_before_ack_is_terminal_without_retry() {
    let RuntimeFixture {
        temporary: _temporary,
        workspace,
        bootstrap,
        listener,
        identity,
        port,
    } = RuntimeFixture::new(command("/must/not/start")).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_registration(&listener, &identity, port).await;
        timeout(TEST_TIMEOUT, socket.close(None))
            .await
            .expect("pre-ACK close timed out")
            .unwrap();
        drop(socket);
        assert_no_retry(&listener).await;
    });

    assert_eq!(
        timeout(
            TEST_TIMEOUT,
            run_worker_once(
                bootstrap,
                workspace,
                future::pending::<Result<(), TerminationSignalError>>()
            )
        )
        .await
        .expect("pre-ACK connection loss did not terminate"),
        Err(WorkerRuntimeError::Registration(
            WorkerRegistrationError::ClosedBeforeAck
        ))
    );
    timeout(TEST_TIMEOUT, server)
        .await
        .expect("pre-ACK server did not finish")
        .unwrap();
}

#[cfg(target_os = "linux")]
fn fake_processes(workspace: &Path) -> Vec<u32> {
    let expected_workspace = fs::canonicalize(workspace).unwrap();
    let expected_executable =
        fs::canonicalize(env!("CARGO_BIN_EXE_openab-kubernetes-session-fake-acp")).unwrap();
    let mut processes = fs::read_dir("/proc")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            let root = PathBuf::from(format!("/proc/{pid}"));
            fs::read_link(root.join("cwd")).ok().as_deref() == Some(expected_workspace.as_path())
                && fs::read_link(root.join("exe")).ok().as_deref()
                    == Some(expected_executable.as_path())
        })
        .collect::<Vec<_>>();
    processes.sort_unstable();
    processes
}

#[cfg(target_os = "linux")]
async fn wait_for_one_fake_process(workspace: &Path) -> u32 {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        let processes = fake_processes(workspace);
        if let [pid] = processes.as_slice() {
            return *pid;
        }
        assert!(
            processes.len() <= 1,
            "one worker activation started multiple ACP children"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "acknowledged worker did not start its ACP child"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(target_os = "linux")]
async fn wait_for_no_fake_process(workspace: &Path) {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        if fake_processes(workspace).is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker left its ACP child alive"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(target_os = "linux")]
fn assert_child_environment_is_closed(pid: u32) {
    let bytes = fs::read(format!("/proc/{pid}/environ")).unwrap();
    let actual = bytes
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| std::str::from_utf8(entry).unwrap().split_once('=').unwrap())
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual,
        BTreeMap::from([
            ("HOME".to_owned(), "/session/home".to_owned()),
            ("OPENAB_SESSION_ROOT".to_owned(), "/session".to_owned()),
            (
                "OPENAB_WORKSPACE".to_owned(),
                "/session/workspace".to_owned()
            ),
            (
                "PATH".to_owned(),
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned()
            ),
            ("USER".to_owned(), "agent".to_owned()),
        ])
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn delayed_ack_runs_one_scrubbed_fake_acp_and_relays_a_clean_lifecycle() {
    let RuntimeFixture {
        temporary,
        workspace,
        bootstrap,
        listener,
        identity,
        port,
    } = RuntimeFixture::new(fake_command()).await;
    let workspace_path = temporary.workspace();
    let (registration_seen_tx, registration_seen_rx) = oneshot::channel();
    let (allow_ack_tx, allow_ack_rx) = oneshot::channel();
    let (child_checked_tx, child_checked_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_registration(&listener, &identity, port).await;
        registration_seen_tx.send(()).unwrap();
        timeout(TEST_TIMEOUT, allow_ack_rx)
            .await
            .expect("test did not release the delayed acknowledgement")
            .unwrap();
        send_ack(&mut socket).await;
        timeout(CLEANUP_TIMEOUT, child_checked_rx)
            .await
            .expect("test did not inspect the acknowledged child")
            .unwrap();

        send_acp(&mut socket, initialize(1)).await;
        let initialized = next_acp(&mut socket).await;
        assert_eq!(initialized["id"], 1);
        assert_eq!(initialized["result"]["protocolVersion"], 1);

        send_acp(&mut socket, new_session(2)).await;
        assert_eq!(
            next_acp(&mut socket).await,
            json!({"jsonrpc": "2.0", "id": 2, "result": {"sessionId": SESSION_ID}})
        );

        send_acp(&mut socket, prompt(3)).await;
        let update = next_acp(&mut socket).await;
        assert_eq!(update["method"], "session/update");
        assert_eq!(update["params"]["sessionId"], SESSION_ID);
        assert_eq!(
            next_acp(&mut socket).await,
            json!({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}})
        );

        send_acp(&mut socket, cancel()).await;
        send_acp(&mut socket, close_session(4)).await;
        assert_eq!(
            next_acp(&mut socket).await,
            json!({"jsonrpc": "2.0", "id": 4, "result": {}})
        );
        expect_socket_closed(&mut socket).await;
        assert_no_retry(&listener).await;
    });
    let worker = tokio::spawn(run_worker_once(
        bootstrap,
        workspace,
        future::pending::<Result<(), TerminationSignalError>>(),
    ));

    timeout(TEST_TIMEOUT, registration_seen_rx)
        .await
        .expect("worker registration was not observed")
        .unwrap();
    assert!(fake_processes(&workspace_path).is_empty());
    allow_ack_tx.send(()).unwrap();
    let child_pid = wait_for_one_fake_process(&workspace_path).await;
    assert_child_environment_is_closed(child_pid);
    child_checked_tx.send(()).unwrap();

    assert_eq!(
        timeout(CLEANUP_TIMEOUT, worker)
            .await
            .expect("clean worker runtime did not finish")
            .unwrap(),
        Ok(())
    );
    timeout(CLEANUP_TIMEOUT, server)
        .await
        .expect("clean lifecycle server did not finish")
        .unwrap();
    wait_for_no_fake_process(&workspace_path).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connection_loss_after_ack_cleans_the_child_without_retry() {
    let RuntimeFixture {
        temporary,
        workspace,
        bootstrap,
        listener,
        identity,
        port,
    } = RuntimeFixture::new(fake_command()).await;
    let workspace_path = temporary.workspace();
    let (ack_sent_tx, ack_sent_rx) = oneshot::channel();
    let (disconnect_tx, disconnect_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_registration(&listener, &identity, port).await;
        send_ack(&mut socket).await;
        ack_sent_tx.send(()).unwrap();
        timeout(CLEANUP_TIMEOUT, disconnect_rx)
            .await
            .expect("test did not request post-ACK disconnect")
            .unwrap();
        timeout(TEST_TIMEOUT, socket.close(None))
            .await
            .expect("post-ACK close timed out")
            .unwrap();
        drop(socket);
        assert_no_retry(&listener).await;
    });
    let worker = tokio::spawn(run_worker_once(
        bootstrap,
        workspace,
        future::pending::<Result<(), TerminationSignalError>>(),
    ));

    timeout(TEST_TIMEOUT, ack_sent_rx)
        .await
        .expect("worker acknowledgement was not sent")
        .unwrap();
    wait_for_one_fake_process(&workspace_path).await;
    disconnect_tx.send(()).unwrap();
    let result = timeout(CLEANUP_TIMEOUT, worker)
        .await
        .expect("worker cleanup exceeded its fixed bound")
        .unwrap();
    assert!(matches!(
        result,
        Err(WorkerRuntimeError::Supervision(
            WorkerSupervisionError::Relay(
                openab_kubernetes_session::worker::relay::WorkerRelayError::ControllerClosed
                    | openab_kubernetes_session::worker::relay::WorkerRelayError::WebSocketTransport
            )
        ))
    ));
    timeout(CLEANUP_TIMEOUT, server)
        .await
        .expect("post-ACK loss server did not finish")
        .unwrap();
    wait_for_no_fake_process(&workspace_path).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn child_failure_closes_the_lane_without_retry() {
    let RuntimeFixture {
        temporary: _temporary,
        workspace,
        bootstrap,
        listener,
        identity,
        port,
    } = RuntimeFixture::new(failing_fake_command()).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_registration(&listener, &identity, port).await;
        send_ack(&mut socket).await;
        expect_socket_closed(&mut socket).await;
        assert_no_retry(&listener).await;
    });

    assert_eq!(
        timeout(
            CLEANUP_TIMEOUT,
            run_worker_once(
                bootstrap,
                workspace,
                future::pending::<Result<(), TerminationSignalError>>()
            )
        )
        .await
        .expect("failing ACP child did not terminate"),
        Err(WorkerRuntimeError::Supervision(
            WorkerSupervisionError::ChildFailed
        ))
    );
    timeout(CLEANUP_TIMEOUT, server)
        .await
        .expect("child-failure server did not finish")
        .unwrap();
}
