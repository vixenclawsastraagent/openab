#![cfg(feature = "controller-runtime")]

use chrono::{Duration as ChronoDuration, Utc};
use http::{Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::controller::{
    ControllerEndpointConfig, ControllerRuntimeBuildError, ControllerStartup,
    ControllerStartupError, ControllerTlsAcceptor, RelayByteBudget,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::ControllerPolicy;
use openab_kubernetes_session::resources::{
    EgressPort, EgressProtocol, MvpWorkerProfile, PersistentWorkspace, PvcAccessMode,
    RunAsIdentity, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use serde_json::{json, Value};
use std::error::Error as _;
use std::future::ready;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::net::TcpListener;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BRIDGE_CREDENTIAL: &str = "bridge-secret-0123456789abcdef-0123456789abcdef";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn session_id() -> SessionId {
    SessionId::derive(RAW_SCOPE, "discord:runtime-startup")
}

fn profile_ref(version: &str) -> ProfileRef {
    ProfileRef::new(PROFILE_NAME, version).unwrap()
}

fn profile() -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        profile_ref(PROFILE_VERSION),
        IMAGE,
        ["/usr/local/bin/openab-session-supervisor"],
        ["serve"],
        PersistentWorkspace::new("20Gi", "encrypted-rwo", PvcAccessMode::default()).unwrap(),
        WorkerResources::new("250m", "1", "256Mi", "2Gi", "1Gi", "8Gi").unwrap(),
        [TrustedEgressRule::for_cidr(
            "10.96.0.10/32",
            [EgressPort::new(EgressProtocol::Udp, 53).unwrap()],
        )
        .unwrap()],
        RunAsIdentity::new(10001, 10001).unwrap(),
        None,
        None,
    )
    .unwrap()
}

fn ready_anchor() -> SessionAnchorV1 {
    let now = Utc::now();
    let mut anchor = SessionAnchorV1::new(
        session_id(),
        scope_id(),
        profile_ref(PROFILE_VERSION),
        Uuid::from_u128(0x100),
        Uuid::from_u128(0x200),
        now,
        now + ChronoDuration::minutes(15),
        now + ChronoDuration::hours(72),
    )
    .unwrap();
    let fence = anchor.fence().clone();
    anchor.observe_pod(&fence, "worker-pod-uid").unwrap();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    anchor
}

fn config_map(anchor: &SessionAnchorV1, resource_version: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": ResourceNames::new(anchor.session_id()).anchor(),
            "namespace": NAMESPACE,
            "uid": format!("uid-{}", anchor.session_id().as_hex()),
            "resourceVersion": resource_version,
            "labels": {
                "app.kubernetes.io/managed-by": "openab-session-controller",
                "openab.dev/resource": "session-anchor"
            }
        },
        "data": { "anchor.json": serde_json::to_string(anchor).unwrap() }
    })
}

fn config_map_list(anchors: &[SessionAnchorV1]) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMapList",
        "metadata": { "resourceVersion": "inventory-rv" },
        "items": anchors
            .iter()
            .enumerate()
            .map(|(index, anchor)| config_map(anchor, &format!("rv-{index}")))
            .collect::<Vec<_>>()
    })
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn api_failure() -> Response<Body> {
    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "InternalError",
            "code": 500
        }),
    )
}

fn tls_acceptor() -> ControllerTlsAcceptor {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test TLS identity");
    ControllerTlsAcceptor::from_pem(
        Cursor::new(cert.pem().into_bytes()),
        Cursor::new(signing_key.serialize_pem().into_bytes()),
        Duration::from_secs(1),
    )
    .unwrap()
}

fn endpoint_config() -> ControllerEndpointConfig {
    ControllerEndpointConfig::new(
        NonZeroUsize::new(4).unwrap(),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_millis(100),
    )
    .unwrap()
}

fn client_and_handle() -> (Client, mock::Handle<Request<Body>, Response<Body>>) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    (Client::new(service, "default"), handle)
}

fn configured_with(
    client: Client,
    profiles: impl IntoIterator<Item = MvpWorkerProfile>,
    current_profiles: impl IntoIterator<Item = ProfileRef>,
    credential: &[u8],
) -> Result<ControllerStartup, ControllerRuntimeBuildError> {
    ControllerStartup::from_resolved_profiles(
        client,
        NAMESPACE,
        scope_id(),
        profiles,
        current_profiles,
        ControllerPolicy::new(15 * 60, 72 * 60 * 60, 20).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap(),
        credential,
        endpoint_config(),
        tls_acceptor(),
    )
}

fn configured(client: Client) -> ControllerStartup {
    configured_with(
        client,
        [profile()],
        [profile_ref(PROFILE_VERSION)],
        BRIDGE_CREDENTIAL.as_bytes(),
    )
    .unwrap()
}

async fn prepared_empty() -> openab_kubernetes_session::controller::PreparedController {
    let (client, handle) = client_and_handle();
    let task = tokio::spawn(configured(client).prepare());
    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(StatusCode::OK, config_map_list(&[])));
    task.await.unwrap().unwrap()
}

#[tokio::test]
async fn static_wiring_rejects_invalid_profiles_and_bridge_credentials() {
    let (client, _handle) = client_and_handle();
    assert!(matches!(
        configured_with(client.clone(), [], [], BRIDGE_CREDENTIAL.as_bytes()),
        Err(ControllerRuntimeBuildError::Coordinator(_))
    ));
    assert!(matches!(
        configured_with(
            client.clone(),
            [profile()],
            [profile_ref("2026-08-02")],
            BRIDGE_CREDENTIAL.as_bytes()
        ),
        Err(ControllerRuntimeBuildError::Service(_))
    ));
    let sentinel_credential = b"sentinel-secret";
    let error = match configured_with(
        client,
        [profile()],
        [profile_ref(PROFILE_VERSION)],
        sentinel_credential,
    ) {
        Err(error @ ControllerRuntimeBuildError::Endpoint(_)) => error,
        _ => panic!("invalid bridge credential was not rejected by the endpoint"),
    };
    let mut exposed = format!("{error:?}\n{error}");
    let mut source = error.source();
    while let Some(error) = source {
        exposed.push_str(&format!("\n{error}"));
        source = error.source();
    }
    assert!(!exposed.contains(std::str::from_utf8(sentinel_credential).unwrap()));
    assert!(!exposed.contains(RAW_SCOPE));
}

#[tokio::test]
async fn inventory_failure_never_produces_a_prepared_controller() {
    let (client, handle) = client_and_handle();
    let task = tokio::spawn(configured(client).prepare());
    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.unwrap();
    send.send_response(api_failure());

    assert!(matches!(
        task.await.unwrap(),
        Err(ControllerStartupError::StartupContainment(_))
    ));
}

#[tokio::test]
async fn one_orphan_failure_keeps_startup_fail_closed() {
    let anchor = ready_anchor();
    let (client, handle) = client_and_handle();
    let task = tokio::spawn(configured(client).prepare());
    let mut handle = std::pin::pin!(handle);
    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(api_failure());

    match task.await.unwrap() {
        Err(ControllerStartupError::StartupContainmentIncomplete { report }) => {
            assert_eq!(report.results().len(), 1);
            assert!(report.results()[0].error().is_some());
        }
        _ => panic!("orphan failure did not retain the incomplete startup report"),
    }
}

#[tokio::test]
async fn cancelling_prepare_cannot_yield_a_serving_capability() {
    let anchor = ready_anchor();
    let (client, handle) = client_and_handle();
    let task = tokio::spawn(configured(client).prepare());
    let mut handle = std::pin::pin!(handle);
    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (_get, _send) = handle.next_request().await.unwrap();

    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("cancelled preparation unexpectedly completed"),
    }
}

#[tokio::test]
async fn empty_inventory_prepares_and_external_shutdown_stops_listener() {
    let prepared = prepared_empty().await;
    assert!(prepared.startup_orphans().results().is_empty());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    prepared.serve_until(listener, ready(())).await.unwrap();
}
