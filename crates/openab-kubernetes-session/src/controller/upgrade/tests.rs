use super::*;
use http::header::{AUTHORIZATION, CACHE_CONTROL, ORIGIN, SEC_WEBSOCKET_PROTOCOL};
use http::{HeaderValue, Method, Request, StatusCode};
use tokio::io::{duplex, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const BRIDGE_CREDENTIAL: &str = "bridge-secret-0123456789abcdef-0123456789abcdef";
const BRIDGE_AUTHORIZATION: &str = "Bearer bridge-secret-0123456789abcdef-0123456789abcdef";
const WRONG_BRIDGE_AUTHORIZATION: &str = "Bearer wrong--secret-0123456789abcdef-0123456789abcdef";

fn request(path: &str, authorization: &str) -> Request<()> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(AUTHORIZATION, authorization)
        .body(())
        .expect("request")
}

fn endpoint_config() -> ControllerEndpointConfig {
    ControllerEndpointConfig::new(
        NonZeroUsize::new(8).expect("non-zero"),
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::from_millis(250),
    )
    .expect("endpoint config")
}

#[test]
fn endpoint_config_rejects_zero_timeouts_and_unsafe_release_retry() {
    let valid = endpoint_config();
    assert_eq!(valid.max_connections.get(), 8);

    for zero_index in 0..4 {
        let mut durations = [
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ];
        durations[zero_index] = Duration::ZERO;
        assert_eq!(
            ControllerEndpointConfig::new(
                NonZeroUsize::new(1).expect("non-zero"),
                durations[0],
                durations[1],
                durations[2],
                durations[3],
                MIN_RELEASE_RETRY_INTERVAL,
            ),
            Err(ControllerEndpointConfigError::ZeroTimeout)
        );
    }
    assert_eq!(
        ControllerEndpointConfig::new(
            NonZeroUsize::new(1).expect("non-zero"),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            MIN_RELEASE_RETRY_INTERVAL - Duration::from_millis(1),
        ),
        Err(ControllerEndpointConfigError::InvalidReleaseRetryInterval)
    );
}

#[test]
fn only_process_invariants_are_fatal_to_the_listener() {
    for error in [
        ControllerConnectionError::MissingUpgradeAuthority,
        ControllerConnectionError::Bridge(Box::new(
            ControllerBridgeWebSocketError::UnsafeConfiguration,
        )),
        ControllerConnectionError::Bridge(Box::new(
            ControllerBridgeWebSocketError::InvalidLifecycleRetryInterval,
        )),
        ControllerConnectionError::Worker(Box::new(WorkerWebSocketError::UnsafeConfiguration)),
    ] {
        assert!(error.is_process_fatal());
    }

    for error in [
        ControllerConnectionError::UpgradeTimedOut,
        ControllerConnectionError::Bridge(Box::new(
            ControllerBridgeWebSocketError::ActivationTimedOut,
        )),
        ControllerConnectionError::Worker(Box::new(WorkerWebSocketError::RegistrationTimedOut)),
    ] {
        assert!(!error.is_process_fatal());
    }
}

#[test]
fn global_connection_admission_is_bounded_and_raii() {
    let admission = ConnectionAdmission::new(NonZeroUsize::new(1).expect("non-zero"));
    let permit = admission.try_acquire().expect("first connection");
    assert_eq!(
        admission.try_acquire().expect_err("at capacity"),
        ControllerAdmissionError::AtCapacity
    );

    drop(permit);
    let _returned = admission.try_acquire().expect("permit returned on drop");
}

#[test]
fn bridge_upgrade_requires_the_exact_path_and_bearer() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let request = request(BRIDGE_WEBSOCKET_PATH, BRIDGE_AUTHORIZATION);

    assert!(matches!(
        authorize_websocket_upgrade(&request, &verifier),
        Ok(UpgradeAuthority::Bridge)
    ));
    assert!(!format!("{verifier:?}").contains(BRIDGE_CREDENTIAL));
}

#[test]
fn bridge_bearer_mismatch_and_header_ambiguity_are_unauthorized() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    for credential in [
        "bridge-secret-0123456789abcdef-0123456789abcdee",
        "aridge-secret-0123456789abcdef-0123456789abcdef",
    ] {
        let request = request(BRIDGE_WEBSOCKET_PATH, &format!("Bearer {credential}"));
        assert_eq!(
            authorize_websocket_upgrade(&request, &verifier).expect_err("wrong credential"),
            UpgradeRejection::Unauthorized
        );
    }

    let mut missing = Request::builder()
        .method(Method::GET)
        .uri(BRIDGE_WEBSOCKET_PATH)
        .body(())
        .expect("request");
    assert_eq!(
        authorize_websocket_upgrade(&missing, &verifier).expect_err("missing credential"),
        UpgradeRejection::Unauthorized
    );
    missing.headers_mut().append(
        AUTHORIZATION,
        HeaderValue::from_static(BRIDGE_AUTHORIZATION),
    );
    missing.headers_mut().append(
        AUTHORIZATION,
        HeaderValue::from_static(BRIDGE_AUTHORIZATION),
    );
    assert_eq!(
        authorize_websocket_upgrade(&missing, &verifier).expect_err("duplicate credential"),
        UpgradeRejection::Unauthorized
    );
}

#[test]
fn bridge_verifier_rejects_unsafe_configuration_without_echoing_it() {
    for credential in [
        b"".as_slice(),
        b"secret\n".as_slice(),
        b"=secret".as_slice(),
    ] {
        let error = BridgeBearerVerifier::new(credential).expect_err("invalid credential");
        assert_eq!(error, ControllerEndpointBuildError::InvalidBridgeCredential);
        assert!(!error.to_string().contains("secret"));
    }
    assert_eq!(
        BridgeBearerVerifier::new(&[b'a'; 31]).expect_err("short credential"),
        ControllerEndpointBuildError::InvalidBridgeCredential
    );
    assert_eq!(
        BridgeBearerVerifier::new(&vec![b'a'; 4 * 1024 + 1]).expect_err("oversized credential"),
        ControllerEndpointBuildError::InvalidBridgeCredential
    );
}

#[test]
fn worker_upgrade_builds_only_the_existing_bootstrap_authority() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let token = [0xa5_u8; 32];
    let encoded = hex::encode(token);
    let mut request = request(WORKER_WEBSOCKET_PATH, &format!("Bearer {encoded}"));
    request.headers_mut().insert(
        WORKER_POD_UID_HEADER,
        HeaderValue::from_static("4db5a02c-74e2-4a27-838f-7f3483c541a9"),
    );

    let authority = authorize_websocket_upgrade(&request, &verifier).expect("worker authority");
    let UpgradeAuthority::Worker(auth) = authority else {
        panic!("expected worker authority");
    };
    assert_eq!(auth.pod_uid(), "4db5a02c-74e2-4a27-838f-7f3483c541a9");
    let debug = format!("{auth:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(&encoded));
}

#[test]
fn worker_upgrade_rejects_malformed_or_ambiguous_transport_authority() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    for token in ["00".to_owned(), "z".repeat(64), "0".repeat(66)] {
        let mut request = request(WORKER_WEBSOCKET_PATH, &format!("Bearer {token}"));
        request
            .headers_mut()
            .insert(WORKER_POD_UID_HEADER, HeaderValue::from_static("pod-uid"));
        assert_eq!(
            authorize_websocket_upgrade(&request, &verifier).expect_err("invalid worker token"),
            UpgradeRejection::Unauthorized
        );
    }

    let mut duplicate_uid = request(
        WORKER_WEBSOCKET_PATH,
        &format!("Bearer {}", hex::encode([7_u8; 32])),
    );
    duplicate_uid
        .headers_mut()
        .append(WORKER_POD_UID_HEADER, HeaderValue::from_static("pod-a"));
    duplicate_uid
        .headers_mut()
        .append(WORKER_POD_UID_HEADER, HeaderValue::from_static("pod-b"));
    assert_eq!(
        authorize_websocket_upgrade(&duplicate_uid, &verifier).expect_err("ambiguous Pod UID"),
        UpgradeRejection::Unauthorized
    );

    for pod_uid in ["pod/uid".to_owned(), "pod\\uid".to_owned(), "x".repeat(257)] {
        let mut request = request(
            WORKER_WEBSOCKET_PATH,
            &format!("Bearer {}", hex::encode([7_u8; 32])),
        );
        request.headers_mut().insert(
            WORKER_POD_UID_HEADER,
            HeaderValue::from_str(&pod_uid).expect("HTTP-safe test UID"),
        );
        assert_eq!(
            authorize_websocket_upgrade(&request, &verifier).expect_err("invalid Pod UID"),
            UpgradeRejection::Unauthorized
        );
    }
}

#[test]
fn endpoint_routing_is_exact_and_non_browser() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    for path in [
        "/",
        "/v1/bridge/",
        "/v1/bridge?scope=other",
        "/v1/worker/extra",
    ] {
        let request = request(path, BRIDGE_AUTHORIZATION);
        assert_eq!(
            authorize_websocket_upgrade(&request, &verifier).expect_err("unknown endpoint"),
            UpgradeRejection::NotFound
        );
    }

    let mut origin = request(BRIDGE_WEBSOCKET_PATH, BRIDGE_AUTHORIZATION);
    origin
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_static("https://example.invalid"));
    assert_eq!(
        authorize_websocket_upgrade(&origin, &verifier).expect_err("browser origin"),
        UpgradeRejection::Forbidden
    );

    let mut subprotocol = request(BRIDGE_WEBSOCKET_PATH, BRIDGE_AUTHORIZATION);
    subprotocol.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("unexpected-protocol"),
    );
    assert_eq!(
        authorize_websocket_upgrade(&subprotocol, &verifier).expect_err("unexpected subprotocol"),
        UpgradeRejection::BadRequest
    );

    let post = Request::builder()
        .method(Method::POST)
        .uri(BRIDGE_WEBSOCKET_PATH)
        .header(AUTHORIZATION, BRIDGE_AUTHORIZATION)
        .body(())
        .expect("request");
    assert_eq!(
        authorize_websocket_upgrade(&post, &verifier).expect_err("wrong method"),
        UpgradeRejection::BadRequest
    );
}

#[test]
fn rejection_response_is_static_and_non_cacheable() {
    for (rejection, status) in [
        (UpgradeRejection::BadRequest, StatusCode::BAD_REQUEST),
        (UpgradeRejection::Unauthorized, StatusCode::UNAUTHORIZED),
        (UpgradeRejection::Forbidden, StatusCode::FORBIDDEN),
        (UpgradeRejection::NotFound, StatusCode::NOT_FOUND),
        (
            UpgradeRejection::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let response = rejection.into_response();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert!(response.body().is_none());
    }
}

#[tokio::test]
async fn authenticated_handshake_returns_the_callback_authority() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let (client_io, server_io) = duplex(8 * 1024);
    let mut request = "ws://controller.test/v1/bridge"
        .into_client_request()
        .expect("client request");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static(BRIDGE_AUTHORIZATION),
    );

    let server = accept_authenticated_websocket(server_io, verifier, Duration::from_secs(1));
    let client = tokio_tungstenite::client_async(request, client_io);
    let (server, client) = tokio::join!(server, client);

    let (_, authority) = server.expect("accepted server socket");
    assert!(matches!(authority, UpgradeAuthority::Bridge));
    client.expect("accepted client socket");
}

#[tokio::test]
async fn handshake_rejects_bad_bearer_before_http_101() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let (client_io, server_io) = duplex(8 * 1024);
    let mut request = "ws://controller.test/v1/bridge"
        .into_client_request()
        .expect("client request");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static(WRONG_BRIDGE_AUTHORIZATION),
    );

    let server = accept_authenticated_websocket(server_io, verifier, Duration::from_secs(1));
    let client = tokio_tungstenite::client_async(request, client_io);
    let (server, client) = tokio::join!(server, client);

    assert!(matches!(
        server.expect_err("server must reject"),
        AuthenticatedUpgradeError::Handshake(_)
    ));
    let error = client.expect_err("client must not receive HTTP 101");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected HTTP rejection");
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response
        .body()
        .as_deref()
        .is_none_or(|body| body.is_empty()));
}

#[tokio::test(start_paused = true)]
async fn handshake_has_one_fixed_deadline() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let (_client_io, server_io) = duplex(8 * 1024);
    let server = tokio::spawn(accept_authenticated_websocket(
        server_io,
        verifier,
        Duration::from_secs(1),
    ));

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        server.await.expect("server task"),
        Err(AuthenticatedUpgradeError::TimedOut)
    ));
}

#[tokio::test]
async fn oversized_http_handshake_is_rejected_by_the_parser_bound() {
    let verifier = BridgeBearerVerifier::new(BRIDGE_CREDENTIAL.as_bytes()).expect("verifier");
    let (mut client_io, server_io) = duplex(128 * 1024);
    let mut request = b"GET /v1/bridge HTTP/1.1\r\nHost: controller.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 66 * 1024));
    request.extend_from_slice(b"\r\n\r\n");

    let server = accept_authenticated_websocket(server_io, verifier, Duration::from_secs(1));
    let client = async move {
        let _ = client_io.write_all(&request).await;
    };
    let (server, ()) = tokio::join!(server, client);

    let AuthenticatedUpgradeError::Handshake(source) =
        server.expect_err("oversized handshake must fail")
    else {
        panic!("expected parser rejection");
    };
    assert!(matches!(
        *source,
        tokio_tungstenite::tungstenite::Error::AttackAttempt
    ));
}
