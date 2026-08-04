#![cfg(feature = "client-transport")]

use openab_kubernetes_session::bridge::{
    MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES, MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES,
};
use openab_kubernetes_session::client_transport::{
    build_client_request, client_connector, client_websocket_config, ClientEndpoint,
    ClientRequestError, PrivateCaError, MAX_CLIENT_CA_PEM_BYTES, MAX_CLIENT_URL_BYTES,
};
use openab_kubernetes_session::wire::{MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES};
use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
use std::io::{self, Cursor, Read};
use tokio_tungstenite::tungstenite::http::header::{
    AUTHORIZATION, CONNECTION, HOST, ORIGIN, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
    SEC_WEBSOCKET_VERSION, UPGRADE,
};
use tokio_tungstenite::Connector;

const TEST_CREDENTIAL: &[u8] = b"abc_DEF-123.~+/abc_DEF-123.~+/==";
const WORKER_POD_UID: &str = "4db5a02c-74e2-4a27-838f-7f3483c541a9";
const WORKER_TOKEN: [u8; 32] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
];

fn bridge_endpoint() -> ClientEndpoint<'static> {
    ClientEndpoint::Bridge {
        bearer: TEST_CREDENTIAL,
    }
}

fn worker_endpoint() -> ClientEndpoint<'static> {
    ClientEndpoint::Worker {
        token: &WORKER_TOKEN,
        pod_uid: WORKER_POD_UID,
    }
}

fn test_identity() -> CertifiedKey<KeyPair> {
    generate_simple_self_signed(vec!["controller.example.test".to_owned()])
        .expect("test TLS identity")
}

#[test]
fn closed_endpoints_build_exact_sensitive_upgrade_requests() {
    let bridge_url = "wss://controller.example.test/v1/bridge";
    let bridge = build_client_request(bridge_endpoint(), bridge_url).unwrap();
    assert_upgrade_request(&bridge, bridge_url, "controller.example.test");
    assert_eq!(
        bridge.headers()[AUTHORIZATION],
        "Bearer abc_DEF-123.~+/abc_DEF-123.~+/=="
    );
    assert_eq!(bridge.headers().get_all(AUTHORIZATION).iter().count(), 1);
    assert!(bridge.headers()[AUTHORIZATION].is_sensitive());
    assert!(!bridge.headers().contains_key("x-openab-pod-uid"));
    assert!(!format!("{bridge:?}").contains("abc_DEF-123"));

    let worker_url = "wss://controller.example.test:8443/v1/worker";
    let worker = build_client_request(worker_endpoint(), worker_url).unwrap();
    assert_upgrade_request(&worker, worker_url, "controller.example.test:8443");
    let authorization = worker.headers()[AUTHORIZATION]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    assert_eq!(
        authorization,
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    );
    assert_eq!(authorization.len(), 64);
    assert!(authorization
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(worker.headers().get_all(AUTHORIZATION).iter().count(), 1);
    assert!(worker.headers()[AUTHORIZATION].is_sensitive());
    assert_eq!(worker.headers()["x-openab-pod-uid"], WORKER_POD_UID);
    assert_eq!(
        worker.headers().get_all("x-openab-pod-uid").iter().count(),
        1
    );
    assert!(worker.headers()["x-openab-pod-uid"].is_sensitive());
    let debug = format!("{worker:?} {:?}", worker_endpoint());
    assert!(!debug.contains(authorization));
    assert!(!debug.contains(WORKER_POD_UID));
}

#[test]
fn request_rejects_open_or_ambiguous_url_shapes() {
    for (endpoint, url) in [
        (bridge_endpoint(), "ws://controller.example.test/v1/bridge"),
        (
            bridge_endpoint(),
            "https://controller.example.test/v1/bridge",
        ),
        (bridge_endpoint(), "/v1/bridge"),
        (
            bridge_endpoint(),
            "wss://user@controller.example.test/v1/bridge",
        ),
        (bridge_endpoint(), "wss://controller.example.test/v1/worker"),
        (worker_endpoint(), "wss://controller.example.test/v1/bridge"),
        (worker_endpoint(), "wss://controller.example.test/arbitrary"),
        (
            bridge_endpoint(),
            "wss://controller.example.test/v1/bridge/",
        ),
        (
            bridge_endpoint(),
            "wss://controller.example.test/v1/%62ridge",
        ),
        (
            bridge_endpoint(),
            "wss://controller.example.test/v1/bridge?unsafe=true",
        ),
        (
            bridge_endpoint(),
            "wss://controller.example.test/v1/bridge?",
        ),
        (
            bridge_endpoint(),
            "wss://controller.example.test/v1/bridge#unsafe",
        ),
        (
            bridge_endpoint(),
            "wss://controller.example.test:not-a-port/v1/bridge",
        ),
        (bridge_endpoint(), "wss://:443/v1/bridge"),
    ] {
        assert_eq!(
            request_error(build_client_request(endpoint, url)),
            ClientRequestError::InvalidUrl
        );
    }
}

#[test]
fn request_bounds_urls_and_authorization_values() {
    let exact_host = "a".repeat(MAX_CLIENT_URL_BYTES - "wss:///v1/bridge".len());
    let exact_url = format!("wss://{exact_host}/v1/bridge");
    assert_eq!(exact_url.len(), MAX_CLIENT_URL_BYTES);
    assert!(build_client_request(bridge_endpoint(), &exact_url).is_ok());

    let oversized_host = format!("{exact_host}a");
    let oversized_url = format!("wss://{oversized_host}/v1/bridge");
    assert_eq!(
        request_error(build_client_request(bridge_endpoint(), &oversized_url)),
        ClientRequestError::UrlTooLarge
    );

    for credential in [
        Vec::new(),
        vec![b'a'; MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES - 1],
        vec![b'a'; MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES + 1],
        b"unsafe\ncredential".to_vec(),
    ] {
        assert_eq!(
            request_error(build_client_request(
                ClientEndpoint::Bridge {
                    bearer: &credential,
                },
                "wss://controller.example.test/v1/bridge",
            )),
            ClientRequestError::InvalidAuthorization
        );
    }

    for pod_uid in [
        String::new(),
        "pod uid".to_owned(),
        "pod/uid".to_owned(),
        "pod\\uid".to_owned(),
        "pod\nuid".to_owned(),
        "x".repeat(257),
    ] {
        assert_eq!(
            request_error(build_client_request(
                ClientEndpoint::Worker {
                    token: &WORKER_TOKEN,
                    pod_uid: &pod_uid,
                },
                "wss://controller.example.test/v1/worker",
            )),
            ClientRequestError::InvalidWorkerPodUid
        );
    }
}

#[test]
fn private_ca_reader_rejects_unsafe_inputs_before_native_root_loading() {
    let identity = test_identity();
    let mut oversized = identity.cert.pem().into_bytes();
    oversized.resize(MAX_CLIENT_CA_PEM_BYTES + 1, b' ');
    assert_eq!(
        connector_error(client_connector(Some(&mut Cursor::new(oversized)))),
        PrivateCaError::TooLarge
    );

    let CertifiedKey { signing_key, .. } = test_identity();
    for (pem, expected) in [
        (Vec::new(), PrivateCaError::MissingCertificate),
        (b"not a PEM bundle".to_vec(), PrivateCaError::InvalidPem),
        (
            signing_key.serialize_pem().into_bytes(),
            PrivateCaError::NonCertificateBlock,
        ),
        (
            b"-----BEGIN CERTIFICATE-----\nAQID\n".to_vec(),
            PrivateCaError::InvalidPem,
        ),
        (
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n".to_vec(),
            PrivateCaError::InvalidCertificate,
        ),
    ] {
        assert_eq!(
            connector_error(client_connector(Some(&mut Cursor::new(pem)))),
            expected
        );
    }

    let error = connector_error(client_connector(Some(&mut FailingReader)));
    assert_eq!(error, PrivateCaError::Read);
    assert!(!format!("{error:?} {error}").contains("sensitive source"));
}

#[test]
fn request_errors_do_not_echo_urls_or_credentials() {
    let sentinel = "do-not-echo-this-value";
    let url = format!("wss://{sentinel}@controller.example.test/v1/bridge");
    let error = request_error(build_client_request(bridge_endpoint(), &url));
    assert!(!format!("{error:?} {error}").contains(sentinel));

    let mut credential = vec![b'a'; MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES];
    credential[4..4 + sentinel.len()].copy_from_slice(sentinel.as_bytes());
    credential[0] = b'!';
    let error = request_error(build_client_request(
        ClientEndpoint::Bridge {
            bearer: &credential,
        },
        "wss://controller.example.test/v1/bridge",
    ));
    assert!(!format!("{error:?} {error}").contains(sentinel));

    let pod_uid = format!("{sentinel}/bad");
    let error = request_error(build_client_request(
        ClientEndpoint::Worker {
            token: &WORKER_TOKEN,
            pod_uid: &pod_uid,
        },
        "wss://controller.example.test/v1/worker",
    ));
    assert!(!format!("{error:?} {error}").contains(sentinel));
}

#[test]
fn default_uses_the_native_transport_without_loading_a_private_bundle() {
    assert!(client_connector(None).unwrap().is_none());
}

#[test]
fn shared_websocket_limits_match_the_wire_protocol() {
    let config = client_websocket_config();
    assert_eq!(config.max_message_size, Some(MAX_ACP_FRAME_BYTES));
    assert_eq!(config.max_frame_size, Some(MAX_ACP_FRAME_BYTES));
    assert!(!config.accept_unmasked_frames);
    assert_eq!(config.write_buffer_size, 0);
    assert_eq!(
        config.max_write_buffer_size,
        MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
    );
}

struct FailingReader;

impl Read for FailingReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("sensitive source"))
    }
}

fn request_error(
    result: Result<tokio_tungstenite::tungstenite::handshake::client::Request, ClientRequestError>,
) -> ClientRequestError {
    match result {
        Ok(_) => panic!("request unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn assert_upgrade_request(
    request: &tokio_tungstenite::tungstenite::handshake::client::Request,
    expected_url: &str,
    expected_host: &str,
) {
    assert_eq!(request.method(), "GET");
    assert_eq!(request.uri(), expected_url);
    assert_eq!(request.headers()[HOST], expected_host);
    assert_eq!(request.headers()[CONNECTION], "Upgrade");
    assert_eq!(request.headers()[UPGRADE], "websocket");
    assert_eq!(request.headers()[SEC_WEBSOCKET_VERSION], "13");
    assert!(!request.headers()[SEC_WEBSOCKET_KEY].is_empty());
    assert!(!request.headers().contains_key(ORIGIN));
    assert!(!request.headers().contains_key(SEC_WEBSOCKET_PROTOCOL));
}

fn connector_error(result: Result<Option<Connector>, PrivateCaError>) -> PrivateCaError {
    match result {
        Ok(_) => panic!("connector unexpectedly succeeded"),
        Err(error) => error,
    }
}
