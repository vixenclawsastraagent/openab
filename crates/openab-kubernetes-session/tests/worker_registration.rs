#![cfg(feature = "worker-runtime")]

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
    build_worker_request, register_worker_once, WorkerRegistrationError, WorkerRequest,
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::header::{
    AUTHORIZATION, CONNECTION, HOST, ORIGIN, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
    SEC_WEBSOCKET_VERSION, UPGRADE,
};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use worker_tokio_rustls::TlsAcceptor;

const POD_UID: &str = "4db5a02c-74e2-4a27-838f-7f3483c541a9";
const TOKEN: &[u8; 32] = b"secret-token-must-never-appear!!";
const TOKEN_HEX: &str = "7365637265742d746f6b656e2d6d7573742d6e657665722d6170706561722121";

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

fn bootstrap(url: String, ca_pem: String) -> WorkerBootstrap {
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
        command(),
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

fn worker_request(bootstrap: WorkerBootstrap) -> WorkerRequest {
    build_worker_request(bootstrap).unwrap()
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

#[tokio::test]
async fn one_exact_request_and_registration_release_the_child_only_after_ack() {
    let identity = tls_identity();
    let ca_pem = identity.cert.pem();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (registration_seen_tx, registration_seen_rx) = oneshot::channel();
    let (allow_ack_tx, allow_ack_rx) = oneshot::channel();
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
        assert!(matches!(
            decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap(),
            WorkerToControllerV1::Registration(_)
        ));
        registration_seen_tx.send(()).unwrap();
        allow_ack_rx.await.unwrap();
        let ack = ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());
        socket
            .send(Message::Text(
                String::from_utf8(encode_frame(&ack).unwrap()).unwrap(),
            ))
            .await
            .unwrap();
    });

    let request = worker_request(bootstrap(
        format!("wss://localhost:{port}/v1/worker"),
        ca_pem,
    ));
    let mut worker = tokio::spawn(register_worker_once(
        request,
        future::pending::<Result<(), TerminationSignalError>>(),
    ));

    registration_seen_rx.await.unwrap();
    assert!(!worker.is_finished());
    allow_ack_tx.send(()).unwrap();
    let registered = (&mut worker).await.unwrap().unwrap();
    assert_eq!(
        registered.command().executable().to_str(),
        Some("/usr/local/bin/acp-sensitive")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn biased_signal_before_connect_consumes_the_only_attempt() {
    let identity = tls_identity();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let request = worker_request(bootstrap(
        format!("wss://localhost:{port}/v1/worker"),
        identity.cert.pem(),
    ));

    assert_eq!(
        register_worker_once(request, future::ready(Ok(())))
            .await
            .unwrap_err(),
        WorkerRegistrationError::Terminated
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hostname_and_private_ca_failures_happen_before_http_upgrade() {
    let server_identity = tls_identity();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_config = tls_server_config(&server_identity);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        assert!(TlsAcceptor::from(server_config)
            .accept(stream)
            .await
            .is_err());
    });

    let request = worker_request(bootstrap(
        format!("wss://127.0.0.1:{port}/v1/worker"),
        server_identity.cert.pem(),
    ));
    assert_eq!(
        register_worker_once(
            request,
            future::pending::<Result<(), TerminationSignalError>>()
        )
        .await
        .unwrap_err(),
        WorkerRegistrationError::Tls
    );
    server.await.unwrap();

    let server_identity = tls_identity();
    let untrusted_identity = tls_identity();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_config = tls_server_config(&server_identity);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        assert!(TlsAcceptor::from(server_config)
            .accept(stream)
            .await
            .is_err());
    });
    let request = worker_request(bootstrap(
        format!("wss://localhost:{port}/v1/worker"),
        untrusted_identity.cert.pem(),
    ));
    assert_eq!(
        register_worker_once(
            request,
            future::pending::<Result<(), TerminationSignalError>>()
        )
        .await
        .unwrap_err(),
        WorkerRegistrationError::Tls
    );
    server.await.unwrap();
}

#[tokio::test]
async fn reflected_non_101_response_is_terminal_and_redacted() {
    let identity = tls_identity();
    let ca_pem = identity.cert.pem();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut tls = TlsAcceptor::from(tls_server_config(&identity))
            .accept(stream)
            .await
            .unwrap();
        let mut request = vec![0_u8; 4096];
        let mut length = 0;
        while !request[..length]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            length += tls.read(&mut request[length..]).await.unwrap();
        }
        assert!(request[..length]
            .windows(TOKEN_HEX.len())
            .any(|window| window == TOKEN_HEX.as_bytes()));
        tls.write_all(
            format!(
                "HTTP/1.1 401 {TOKEN_HEX}\r\nX-Reflected-Authorization: Bearer {TOKEN_HEX}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        tls.flush().await.unwrap();
    });

    let request = worker_request(bootstrap(
        format!("wss://localhost:{port}/v1/worker"),
        ca_pem,
    ));
    let error = register_worker_once(
        request,
        future::pending::<Result<(), TerminationSignalError>>(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, WorkerRegistrationError::InvalidUpgradeResponse);
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains(TOKEN_HEX));
    assert!(!rendered.contains("Authorization"));
    server.await.unwrap();
}
