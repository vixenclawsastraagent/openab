use futures_util::{SinkExt, StreamExt};
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::client_transport::client_websocket_config;
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::Fence;
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, ControllerToWorkerV1, ProtocolResultV1, WorkerRegistrationV1,
    WorkerToControllerV1,
};
use openab_kubernetes_session::worker::bootstrap::{
    WorkerBootstrap, WorkerBootstrapEnvironment, WorkerCommand,
};
use openab_kubernetes_session::worker::registration::{
    build_worker_request, register_worker_once, RegisteredWorker, WorkerTlsStream,
};
use openab_kubernetes_session::worker::TerminationSignalError;
use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::future;
use std::io::Cursor;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
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

pub type ControllerSocket = WebSocketStream<ServerTlsStream<TcpStream>>;

#[allow(dead_code)]
fn command() -> WorkerCommand {
    WorkerCommand::parse([
        OsString::from("serve"),
        OsString::from("--"),
        OsString::from("/usr/local/bin/acp-sensitive"),
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

/// Return the public post-ACK capability and its peer only after a real local
/// TLS connection, exact HTTP upgrade, Registration frame, and ACK complete.
#[allow(dead_code)]
pub async fn registered_worker_pair() -> (RegisteredWorker<WorkerTlsStream>, ControllerSocket) {
    registered_worker_pair_with_command(command()).await
}

/// Complete the real registration handshake while retaining a caller-chosen
/// literal ACP command for post-ACK supervision tests.
#[allow(dead_code)]
pub async fn registered_worker_pair_with_command(
    command: WorkerCommand,
) -> (RegisteredWorker<WorkerTlsStream>, ControllerSocket) {
    let identity = tls_identity();
    let ca_pem = identity.cert.pem();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let expected_registration = registration();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let tls = TlsAcceptor::from(tls_server_config(&identity))
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

        let first = socket.next().await.unwrap().unwrap();
        let Message::Text(text) = first else {
            panic!("Registration must be the first text application frame");
        };
        assert_eq!(
            decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap(),
            WorkerToControllerV1::Registration(expected_registration)
        );
        let ack = ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());
        socket
            .send(Message::Text(
                String::from_utf8(encode_frame(&ack).unwrap()).unwrap(),
            ))
            .await
            .unwrap();
        socket
    });

    let request = build_worker_request(bootstrap_with_command(
        command,
        format!("wss://localhost:{port}/v1/worker"),
        ca_pem,
    ))
    .unwrap();
    let registered = register_worker_once(
        request,
        future::pending::<Result<(), TerminationSignalError>>(),
    )
    .await
    .unwrap();
    let controller = server.await.unwrap();
    (registered, controller)
}
