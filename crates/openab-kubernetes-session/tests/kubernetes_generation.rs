#![cfg(feature = "controller")]

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::node::v1::RuntimeClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::controller::{
    CleanupProgress, GenerationProvisioner, GenerationProvisionerError, GenerationResource,
    KubernetesGenerationProvisioner, LifecycleProvisioner, ProvisionerOperation,
    ReleaseCoordinator, ReleaseError, ReleaseOutcome, SessionLocks,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::resources::{
    AllowedRuntimeClass, DesiredGeneration, EgressPort, EgressProtocol, GenerationContext,
    MvpWorkerProfile, PersistentWorkspace, PinnedSkillsConfigMap, PvcAccessMode, RunAsIdentity,
    RuntimeClassSelection, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::ConfigMapAnchorStore;
use openab_kubernetes_session::wire::LifecycleRequestV1;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ANCHOR_UID: &str = "f6d6f3dd-1274-4a17-83dd-d14be72edb86";

fn session_id(label: &str) -> SessionId {
    SessionId::derive(RAW_SCOPE, label)
}

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn anchor(session_id: SessionId) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        session_id,
        scope_id(),
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
        Uuid::from_u128(0x10),
        Uuid::from_u128(0x20),
        now,
        now + ChronoDuration::minutes(15),
        now + ChronoDuration::hours(72),
    )
    .unwrap()
}

fn replacement_anchor(session_id: SessionId) -> SessionAnchorV1 {
    let mut anchor = anchor(session_id);
    let first_fence = anchor.fence().clone();
    anchor.observe_pod(&first_fence, "first-pod-uid").unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Ready)
        .unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Suspending)
        .unwrap();
    anchor
        .confirm_pod_deleted(&first_fence, "first-pod-uid")
        .unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Suspended)
        .unwrap();
    let resumed_at = anchor.last_activity_at() + ChronoDuration::minutes(20);
    anchor
        .advance_generation(
            &first_fence,
            Uuid::from_u128(0x30),
            resumed_at,
            resumed_at + ChronoDuration::minutes(15),
            resumed_at + ChronoDuration::hours(72),
        )
        .unwrap();
    anchor
}

fn anchor_with_observed_pod(session_id: SessionId, pod_uid: &str) -> SessionAnchorV1 {
    let mut anchor = anchor(session_id);
    let fence = anchor.fence().clone();
    anchor.observe_pod(&fence, pod_uid).unwrap();
    anchor
}

fn suspending_anchor(session_id: SessionId, recorded_pod_uid: Option<&str>) -> SessionAnchorV1 {
    let observed_uid = recorded_pod_uid.unwrap_or("already-absent-pod-uid");
    let mut anchor = anchor_with_observed_pod(session_id, observed_uid);
    let fence = anchor.fence().clone();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    anchor.transition(&fence, SessionPhase::Suspending).unwrap();
    if recorded_pod_uid.is_none() {
        anchor.confirm_pod_deleted(&fence, observed_uid).unwrap();
    }
    anchor
}

fn suspended_anchor(session_id: SessionId) -> SessionAnchorV1 {
    let mut anchor = suspending_anchor(session_id, None);
    let fence = anchor.fence().clone();
    anchor.transition(&fence, SessionPhase::Suspended).unwrap();
    anchor
}

fn deleting_anchor(session_id: SessionId) -> SessionAnchorV1 {
    let mut anchor = anchor(session_id);
    let fence = anchor.fence().clone();
    anchor.transition(&fence, SessionPhase::Deleting).unwrap();
    anchor
}

fn release_request(anchor: &SessionAnchorV1) -> LifecycleRequestV1 {
    serde_json::from_value(json!({
        "version": 1,
        "requestId": Uuid::from_u128(0x40),
        "kind": "release",
        "binding": {
            "version": 1,
            "scopeId": anchor.scope_id(),
            "sessionId": anchor.session_id(),
            "generation": anchor.fence().generation(),
            "attemptId": anchor.fence().attempt_id(),
            "incarnationId": anchor.incarnation_id(),
        },
        "workerSessionId": "opaque-worker-session"
    }))
    .unwrap()
}

fn profile() -> MvpWorkerProfile {
    profile_with_pins(None, None)
}

fn selected_skills(name: &str, uid: &str, resource_version: &str) -> PinnedSkillsConfigMap {
    let observed = ConfigMap {
        immutable: Some(true),
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(NAMESPACE.into()),
            uid: Some(uid.into()),
            resource_version: Some(resource_version.into()),
            ..ObjectMeta::default()
        },
        ..ConfigMap::default()
    };
    PinnedSkillsConfigMap::from_observed(NAMESPACE, &observed).unwrap()
}

fn profile_with_pins(
    runtime_class: Option<RuntimeClassSelection>,
    skills: Option<PinnedSkillsConfigMap>,
) -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
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
        runtime_class,
        skills,
    )
    .unwrap()
}

fn observed_runtime_class() -> RuntimeClass {
    RuntimeClass {
        handler: "kata-qemu".into(),
        metadata: ObjectMeta {
            name: Some("kata".into()),
            uid: Some("runtime-uid".into()),
            resource_version: Some("runtime-rv-1".into()),
            ..ObjectMeta::default()
        },
        overhead: None,
        scheduling: None,
    }
}

fn selected_runtime_class() -> RuntimeClassSelection {
    RuntimeClassSelection::from_observed(
        &observed_runtime_class(),
        [AllowedRuntimeClass::new("kata", "kata-qemu").unwrap()],
    )
    .unwrap()
}

fn observed_skills_config_map() -> ConfigMap {
    ConfigMap {
        immutable: Some(true),
        metadata: ObjectMeta {
            name: Some("team-skills-v1".into()),
            namespace: Some(NAMESPACE.into()),
            uid: Some("skills-uid".into()),
            resource_version: Some("skills-rv-1".into()),
            ..ObjectMeta::default()
        },
        ..ConfigMap::default()
    }
}

fn anchor_config_map(anchor: &SessionAnchorV1) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": ResourceNames::new(anchor.session_id()).anchor(),
            "namespace": NAMESPACE,
            "uid": ANCHOR_UID,
            "resourceVersion": "anchor-rv-1",
            "labels": {
                "app.kubernetes.io/managed-by": "openab-session-controller",
                "openab.dev/resource": "session-anchor"
            }
        },
        "data": {"anchor.json": serde_json::to_string(anchor).unwrap()}
    })
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn missing_response() -> Response<Body> {
    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "NotFound",
            "code": 404
        }),
    )
}

fn conflict_response() -> Response<Body> {
    json_response(
        StatusCode::CONFLICT,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "AlreadyExists",
            "code": 409
        }),
    )
}

fn delete_accepted_response() -> Response<Body> {
    json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    )
}

fn internal_error_response() -> Response<Body> {
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

async fn request_body(request: Request<Body>) -> Value {
    let bytes = request.into_body().collect_bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn respond_created(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    expected_path: &str,
    uid: &str,
) -> Value {
    let (request, send) = handle.next_request().await.expect("create request");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.uri().path(), expected_path);
    let mut body = request_body(request).await;
    body["metadata"]["uid"] = json!(uid);
    body["metadata"]["resourceVersion"] = json!(format!("rv-{uid}"));
    send.send_response(json_response(StatusCode::CREATED, body.clone()));
    body
}

async fn respond_get(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    expected_path: &str,
    body: Value,
) {
    let (request, send) = handle.next_request().await.expect("get request");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), expected_path);
    send.send_response(json_response(StatusCode::OK, body));
}

async fn respond_initial_pod_absent(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    generation: u64,
) {
    let (request, send) = handle.next_request().await.expect("initial worker Pod GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            ResourceNames::new(session_id).pod(generation).unwrap()
        )
    );
    send.send_response(missing_response());
}

async fn respond_list(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    expected_path: &str,
    api_version: &str,
    kind: &str,
    items: Vec<Value>,
) {
    let (request, send) = handle.next_request().await.expect("list request");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), expected_path);
    assert!(request
        .uri()
        .query()
        .is_some_and(|query| query.contains("labelSelector=")));
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {},
            "items": items
        }),
    ));
}

async fn respond_delete(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    expected_path: &str,
    expected_uid: &str,
) {
    let (request, send) = handle.next_request().await.expect("delete request");
    assert_eq!(request.method(), Method::DELETE);
    assert_eq!(request.uri().path(), expected_path);
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], expected_uid);
    assert_eq!(
        body["preconditions"]["resourceVersion"],
        format!("rv-{expected_uid}")
    );
    send.send_response(delete_accepted_response());
}

async fn respond_compute_absence_proof(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    generation: u64,
) {
    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(generation).unwrap();
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");

    respond_list(handle, &pod_path, "v1", "PodList", vec![]).await;
    respond_list(handle, &secret_path, "v1", "SecretList", vec![]).await;
    respond_list(
        handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![],
    )
    .await;
    respond_list(handle, &account_path, "v1", "ServiceAccountList", vec![]).await;

    for path in [
        format!("{pod_path}/{pod_name}"),
        format!(
            "{secret_path}/{}",
            names.registration_secret(generation).unwrap()
        ),
        format!("{policy_path}/{pod_name}-net"),
        format!(
            "{account_path}/{}",
            names.service_account(generation).unwrap()
        ),
    ] {
        let (request, send) = handle.next_request().await.expect("absence proof GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
}

#[allow(clippy::too_many_arguments)]
async fn respond_generation_revalidation(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    pvc: &Value,
    policy: &Value,
    account: &Value,
    secret: &Value,
    pod: &Value,
) {
    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");

    respond_get(handle, &format!("{pvc_path}/{}", names.pvc()), pvc.clone()).await;
    respond_get(
        handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy.clone(),
    )
    .await;
    respond_get(
        handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account.clone(),
    )
    .await;
    respond_get(
        handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        secret.clone(),
    )
    .await;
    respond_get(handle, &format!("{pod_path}/{pod_name}"), pod.clone()).await;

    respond_list(
        handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![pvc.clone()],
    )
    .await;
    respond_list(
        handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![policy.clone()],
    )
    .await;
    respond_list(
        handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        vec![account.clone()],
    )
    .await;
    respond_list(
        handle,
        &secret_path,
        "v1",
        "SecretList",
        vec![secret.clone()],
    )
    .await;
    respond_list(handle, &pod_path, "v1", "PodList", vec![pod.clone()]).await;
}

async fn load_stored_anchor(
    client: Client,
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    anchor: SessionAnchorV1,
) -> openab_kubernetes_session::store::StoredAnchor {
    let session_id = anchor.session_id();
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { store.get(session_id).await });
    let (request, send) = handle.next_request().await.expect("anchor GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
            ResourceNames::new(session_id).anchor()
        )
    );
    send.send_response(json_response(StatusCode::OK, anchor_config_map(&anchor)));
    task.await.unwrap().unwrap().unwrap()
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected = tokio::time::timeout(Duration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn absence_proof_lists_every_child_kind_then_reads_deterministic_names() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let session_id = session_id("discord:thread-a");
    let names = ResourceNames::new(session_id);
    let expected_lists = [
        format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims"),
        format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies"),
        format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts"),
        format!("/api/v1/namespaces/{NAMESPACE}/secrets"),
        format!("/api/v1/namespaces/{NAMESPACE}/pods"),
    ];
    let expected_gets = [
        format!(
            "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
            names.pvc()
        ),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
            names.service_account(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(1).unwrap()
        ),
    ];

    let task = tokio::spawn(async move { provisioner.prove_v1_children_absent(session_id).await });
    let mut handle = std::pin::pin!(handle);
    for (path, (kind, api_version)) in expected_lists.into_iter().zip([
        ("PersistentVolumeClaimList", "v1"),
        ("NetworkPolicyList", "networking.k8s.io/v1"),
        ("ServiceAccountList", "v1"),
        ("SecretList", "v1"),
        ("PodList", "v1"),
    ]) {
        let (request, send) = handle.next_request().await.expect("absence LIST");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        let query = request.uri().query().expect("label selector");
        assert!(query.contains("labelSelector="));
        assert!(query.contains("app.kubernetes.io"));
        assert!(query.contains("openab.dev"));
        send.send_response(json_response(
            StatusCode::OK,
            json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": {},
                "items": []
            }),
        ));
    }
    for path in expected_gets {
        let (request, send) = handle.next_request().await.expect("deterministic GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }

    task.await.unwrap().unwrap();
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn absence_proof_stops_at_the_first_present_or_ambiguous_child() {
    for (status, expected) in [
        (
            StatusCode::OK,
            GenerationProvisionerError::ChildrenAmbiguous,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            GenerationProvisionerError::KubernetesApi {
                operation: ProvisionerOperation::ProveChildrenAbsent,
            },
        ),
    ] {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let provisioner =
            KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
        let session_id = session_id(&format!("discord:{status}"));
        let task =
            tokio::spawn(async move { provisioner.prove_v1_children_absent(session_id).await });
        let mut handle = std::pin::pin!(handle);
        let (_request, send) = handle.next_request().await.expect("first absence LIST");
        let body = if status == StatusCode::OK {
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaimList",
                "metadata": {},
                "items": [{"metadata": {"name": "unexpected"}}]
            })
        } else {
            json!({
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Failure",
                "reason": "InternalError",
                "code": status.as_u16()
            })
        };
        send.send_response(json_response(status, body));

        assert_eq!(task.await.unwrap().unwrap_err(), expected);
        assert_no_request(&mut handle).await;
    }
}

#[tokio::test]
async fn absence_proof_detects_a_well_owned_later_generation_orphan() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:later-orphan");
    let anchor = anchor(session_id);
    let mut orphan = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).pod(),
        "orphan-pod-uid",
    );
    let names = ResourceNames::new(session_id);
    orphan["metadata"]["name"] = json!(names.pod(2).unwrap());
    orphan["metadata"]["labels"]["openab.dev/generation"] = json!("2");
    orphan["metadata"]["annotations"]["openab.dev/generation"] = json!("2");

    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.prove_v1_children_absent(session_id).await });
    let mut handle = std::pin::pin!(handle);
    for (path, kind, api_version) in [
        (
            format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims"),
            "PersistentVolumeClaimList",
            "v1",
        ),
        (
            format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies"),
            "NetworkPolicyList",
            "networking.k8s.io/v1",
        ),
        (
            format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts"),
            "ServiceAccountList",
            "v1",
        ),
        (
            format!("/api/v1/namespaces/{NAMESPACE}/secrets"),
            "SecretList",
            "v1",
        ),
    ] {
        respond_list(&mut handle, &path, api_version, kind, vec![]).await;
    }
    respond_list(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/pods"),
        "v1",
        "PodList",
        vec![orphan],
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ChildrenPresent
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn generation_is_created_in_order_and_revalidated_before_the_pod() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:happy-path");
    let anchor = anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor.clone()).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let profile = profile();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile)
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");

    let pvc = respond_created(&mut handle, &pvc_path, "pvc-uid").await;
    assert_eq!(pvc["metadata"]["name"], names.pvc());
    let policy = respond_created(&mut handle, &policy_path, "policy-uid").await;
    assert_eq!(policy["metadata"]["name"], format!("{pod_name}-net"));
    let account = respond_created(&mut handle, &account_path, "account-uid").await;
    assert_eq!(
        account["metadata"]["name"],
        names.service_account(1).unwrap()
    );
    let secret = respond_created(&mut handle, &secret_path, "secret-uid").await;
    assert_eq!(
        secret["metadata"]["name"],
        names.registration_secret(1).unwrap()
    );
    assert_eq!(secret["immutable"], true);
    let token = secret["data"]["token"].as_str().expect("base64 token");
    assert!(!token.is_empty());

    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", names.pvc()),
        pvc.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        secret.clone(),
    )
    .await;
    let (request, send) = handle.next_request().await.expect("Pod preflight GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), format!("{pod_path}/{pod_name}"));
    send.send_response(missing_response());

    respond_list(
        &mut handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![pvc.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![policy.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        vec![account.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &secret_path,
        "v1",
        "SecretList",
        vec![secret.clone()],
    )
    .await;
    respond_list(&mut handle, &pod_path, "v1", "PodList", vec![]).await;

    let pod = respond_created(&mut handle, &pod_path, "pod-uid").await;
    assert_eq!(pod["metadata"]["name"], pod_name);
    respond_generation_revalidation(
        &mut handle,
        session_id,
        &pvc,
        &policy,
        &account,
        &secret,
        &pod,
    )
    .await;
    let observed = task.await.unwrap().unwrap();
    assert_eq!(observed.pod_uid(), "pod-uid");
    assert_no_request(&mut handle).await;
}

fn observed_value<T: serde::Serialize>(object: &T, uid: &str) -> Value {
    let mut value = serde_json::to_value(object).unwrap();
    value["metadata"]["uid"] = json!(uid);
    value["metadata"]["resourceVersion"] = json!(format!("rv-{uid}"));
    value
}

fn desired_for(
    anchor: &SessionAnchorV1,
    profile: MvpWorkerProfile,
    token: [u8; 32],
) -> DesiredGeneration {
    let names = ResourceNames::new(anchor.session_id());
    let context =
        GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, anchor, names)
            .unwrap();
    DesiredGeneration::build(context, profile, token).unwrap()
}

#[tokio::test]
async fn existing_secret_and_raced_pod_are_adopted_without_rotation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:secret-recovery");
    let anchor = anchor(session_id);
    let existing_desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let existing_secret = observed_value(existing_desired.registration_secret(), "secret-uid");
    let existing_pod = observed_value(existing_desired.pod(), "recovered-pod-uid");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    let pvc = respond_created(&mut handle, &pvc_path, "pvc-uid").await;
    let policy = respond_created(&mut handle, &policy_path, "policy-uid").await;
    let account = respond_created(&mut handle, &account_path, "account-uid").await;

    let (request, send) = handle.next_request().await.expect("Secret create");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.uri().path(), secret_path);
    let proposed = request_body(request).await;
    assert!(proposed["data"]["token"]
        .as_str()
        .is_some_and(|token| !token.is_empty()));
    send.send_response(conflict_response());
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        existing_secret.clone(),
    )
    .await;

    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", names.pvc()),
        pvc.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        existing_secret.clone(),
    )
    .await;
    let (_request, send) = handle.next_request().await.expect("Pod preflight GET");
    send.send_response(missing_response());

    respond_list(
        &mut handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![pvc.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![policy.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        vec![account.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &secret_path,
        "v1",
        "SecretList",
        vec![existing_secret.clone()],
    )
    .await;
    respond_list(&mut handle, &pod_path, "v1", "PodList", vec![]).await;

    let (request, send) = handle.next_request().await.expect("Pod create");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.uri().path(), pod_path);
    let proposed_pod = request_body(request).await;
    assert_eq!(proposed_pod["metadata"]["name"], pod_name);
    send.send_response(conflict_response());
    respond_get(
        &mut handle,
        &format!("{pod_path}/{pod_name}"),
        existing_pod.clone(),
    )
    .await;
    respond_generation_revalidation(
        &mut handle,
        session_id,
        &pvc,
        &policy,
        &account,
        &existing_secret,
        &existing_pod,
    )
    .await;

    assert_eq!(task.await.unwrap().unwrap().pod_uid(), "recovered-pod-uid");
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn pod_already_exists_race_rejects_child_uid_drift_after_preflight() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:pod-race-child-drift");
    let anchor = anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    let pvc = respond_created(&mut handle, &pvc_path, "pvc-uid").await;
    let policy = respond_created(&mut handle, &policy_path, "policy-uid").await;
    let account = respond_created(&mut handle, &account_path, "account-uid").await;
    let secret = respond_created(&mut handle, &secret_path, "secret-uid").await;

    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", names.pvc()),
        pvc.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        secret.clone(),
    )
    .await;
    let (request, send) = handle.next_request().await.expect("Pod preflight GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), format!("{pod_path}/{pod_name}"));
    send.send_response(missing_response());

    respond_list(
        &mut handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![pvc.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![policy.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        vec![account.clone()],
    )
    .await;
    respond_list(
        &mut handle,
        &secret_path,
        "v1",
        "SecretList",
        vec![secret.clone()],
    )
    .await;
    respond_list(&mut handle, &pod_path, "v1", "PodList", vec![]).await;

    let (request, send) = handle.next_request().await.expect("raced Pod create");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.uri().path(), pod_path);
    let mut raced_pod = request_body(request).await;
    raced_pod["metadata"]["uid"] = json!("race-pod-uid");
    raced_pod["metadata"]["resourceVersion"] = json!("rv-race-pod-uid");
    send.send_response(conflict_response());
    respond_get(&mut handle, &format!("{pod_path}/{pod_name}"), raced_pod).await;

    let mut replacement_pvc = pvc;
    replacement_pvc["metadata"]["uid"] = json!("replacement-pvc-uid");
    replacement_pvc["metadata"]["resourceVersion"] = json!("rv-replacement-pvc-uid");
    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", names.pvc()),
        replacement_pvc,
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy,
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account,
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        secret,
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::PersistentVolumeClaim,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn malformed_existing_bootstrap_token_fails_closed_before_pod_creation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:bad-token");
    let anchor = anchor(session_id);
    let mut existing_secret = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).registration_secret(),
        "secret-uid",
    );
    existing_secret["data"]["token"] = json!("dG9vLXNob3J0");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    respond_created(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims"),
        "pvc-uid",
    )
    .await;
    respond_created(
        &mut handle,
        &format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies"),
        "policy-uid",
    )
    .await;
    respond_created(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts"),
        "account-uid",
    )
    .await;
    let (request, send) = handle.next_request().await.expect("Secret create");
    assert_eq!(request.method(), Method::POST);
    send.send_response(conflict_response());
    let (request, send) = handle.next_request().await.expect("Secret recovery GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, existing_secret));

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::InvalidBootstrapToken
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn recorded_pod_uid_with_a_missing_pod_rejects_before_any_mutation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:recorded-pod-missing");
    let anchor = anchor_with_observed_pod(session_id, "recorded-pod-uid");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });

    respond_initial_pod_absent(&mut handle, session_id, 1).await;
    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::Pod,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn existing_pod_with_a_consumed_secret_never_recreates_the_credential() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:consumed-secret");
    let anchor = anchor(session_id);
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let pod = observed_value(desired.pod(), "existing-pod-uid");
    let secret_name = ResourceNames::new(session_id)
        .registration_secret(1)
        .unwrap();
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });

    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            ResourceNames::new(session_id).pod(1).unwrap()
        ),
        pod,
    )
    .await;
    let (request, send) = handle.next_request().await.expect("adopt-only Secret GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!("/api/v1/namespaces/{NAMESPACE}/secrets/{secret_name}")
    );
    send.send_response(missing_response());

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::BootstrapCredentialConsumedOrMissing
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn existing_pod_with_a_missing_network_policy_never_recreates_children() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:missing-policy-after-pod");
    let anchor = anchor(session_id);
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let pod = observed_value(desired.pod(), "existing-pod-uid");
    let secret = observed_value(desired.registration_secret(), "secret-uid");
    let pvc = observed_value(desired.persistent_volume_claim(), "pvc-uid");
    let names = ResourceNames::new(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });

    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(1).unwrap()
        ),
        pod,
    )
    .await;
    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        ),
        secret,
    )
    .await;
    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
            names.pvc()
        ),
        pvc,
    )
    .await;
    let (request, send) = handle
        .next_request()
        .await
        .expect("adopt-only NetworkPolicy GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(1).unwrap()
        )
    );
    send.send_response(missing_response());

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::NetworkPolicy,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn already_existing_child_with_a_different_spec_is_not_adopted() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:foreign-pvc");
    let anchor = anchor(session_id);
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let mut foreign = observed_value(desired.persistent_volume_claim(), "foreign-pvc-uid");
    foreign["spec"]["storageClassName"] = json!("different-storage-class");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let (request, send) = handle.next_request().await.expect("PVC create");
    assert_eq!(request.method(), Method::POST);
    send.send_response(conflict_response());
    let (request, send) = handle.next_request().await.expect("PVC adoption GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, foreign));

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource:
                openab_kubernetes_session::controller::GenerationResource::PersistentVolumeClaim,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn replacement_generation_adopts_the_session_lifetime_workspace_claim() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:replacement-pvc");
    let first_anchor = anchor(session_id);
    let first_desired = desired_for(&first_anchor, profile(), [0x11; 32]);
    let expected_metadata =
        serde_json::to_value(&first_desired.persistent_volume_claim().metadata).unwrap();
    let observed_pvc = observed_value(first_desired.persistent_volume_claim(), "workspace-uid");
    let second_anchor = replacement_anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, second_anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 2).await;

    let (request, send) = handle.next_request().await.expect("replacement PVC create");
    assert_eq!(request.method(), Method::POST);
    let requested_pvc = request_body(request).await;
    assert_eq!(requested_pvc["metadata"], expected_metadata);
    send.send_response(conflict_response());
    let (request, send) = handle.next_request().await.expect("replacement PVC GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, observed_pvc));

    let (request, send) = handle
        .next_request()
        .await
        .expect("NetworkPolicy create proves PVC adoption continued");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(
        request.uri().path(),
        format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies")
    );
    send.send_response(json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "InternalError",
            "code": 500
        }),
    ));

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::KubernetesApi {
            operation: ProvisionerOperation::EnsureGeneration,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn child_terminating_during_preflight_stops_before_pod_creation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:terminating-pvc");
    let anchor = anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &profile())
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let mut pvc = respond_created(&mut handle, &pvc_path, "pvc-uid").await;
    respond_created(&mut handle, &policy_path, "policy-uid").await;
    respond_created(&mut handle, &account_path, "account-uid").await;
    respond_created(&mut handle, &secret_path, "secret-uid").await;
    pvc["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", ResourceNames::new(session_id).pvc()),
        pvc,
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource:
                openab_kubernetes_session::controller::GenerationResource::PersistentVolumeClaim,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn pinned_dependencies_are_revalidated_and_drift_fails_before_pod_creation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:pinned-drift");
    let anchor = anchor(session_id);
    let skills_pin = selected_skills("team-skills-v1", "skills-uid", "skills-rv-1");
    let pinned_profile = profile_with_pins(Some(selected_runtime_class()), Some(skills_pin));
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .ensure_provisioning_generation(&stored, &pinned_profile)
            .await
    });
    respond_initial_pod_absent(&mut handle, session_id, 1).await;

    let pvc = respond_created(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims"),
        "pvc-uid",
    )
    .await;
    let policy = respond_created(
        &mut handle,
        &format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies"),
        "policy-uid",
    )
    .await;
    let account = respond_created(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts"),
        "account-uid",
    )
    .await;
    let secret = respond_created(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/secrets"),
        "secret-uid",
    )
    .await;
    let names = ResourceNames::new(session_id);
    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    respond_get(
        &mut handle,
        &format!("{pvc_path}/{}", names.pvc()),
        pvc.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{policy_path}/{pod_name}-net"),
        policy.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{account_path}/{}", names.service_account(1).unwrap()),
        account.clone(),
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{secret_path}/{}", names.registration_secret(1).unwrap()),
        secret.clone(),
    )
    .await;
    let (request, send) = handle.next_request().await.expect("Pod preflight GET");
    assert_eq!(request.uri().path(), format!("{pod_path}/{pod_name}"));
    send.send_response(missing_response());
    respond_list(
        &mut handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![pvc],
    )
    .await;
    respond_list(
        &mut handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![policy],
    )
    .await;
    respond_list(
        &mut handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        vec![account],
    )
    .await;
    respond_list(&mut handle, &secret_path, "v1", "SecretList", vec![secret]).await;
    respond_list(&mut handle, &pod_path, "v1", "PodList", vec![]).await;
    respond_get(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/configmaps/team-skills-v1"),
        serde_json::to_value(observed_skills_config_map()).unwrap(),
    )
    .await;
    let mut drifted_runtime = serde_json::to_value(observed_runtime_class()).unwrap();
    drifted_runtime["metadata"]["resourceVersion"] = json!("runtime-rv-2");
    respond_get(
        &mut handle,
        "/apis/node.k8s.io/v1/runtimeclasses/kata",
        drifted_runtime,
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: openab_kubernetes_session::controller::GenerationResource::RuntimeClass,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_rejects_the_wrong_recorded_pod_uid_before_mutation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-wrong-pod-uid");
    let anchor = suspending_anchor(session_id, Some("recorded-pod-uid"));
    let pod = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).pod(),
        "replacement-pod-uid",
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            ResourceNames::new(session_id).pod(1).unwrap()
        ),
        pod,
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::Pod,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_rejects_tampered_durable_identity_before_delete() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-tampered-attempt");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let mut pod = observed_value(desired_for(&anchor, profile(), [0x5a; 32]).pod(), "pod-uid");
    pod["metadata"]["annotations"]["openab.dev/attempt-id"] =
        json!(Uuid::from_u128(0xdead).to_string());
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        ResourceNames::new(session_id).pod(1).unwrap()
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(&mut handle, &pod_path, pod).await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::Pod,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_observes_pod_absence_after_a_preconditioned_delete_returns_not_found() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-pod-preconditions");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let pod = observed_value(desired_for(&anchor, profile(), [0x5a; 32]).pod(), "pod-uid");
    let names = ResourceNames::new(session_id);
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        names.pod(1).unwrap()
    );
    let secret_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
        names.registration_secret(1).unwrap()
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(&mut handle, &pod_path, pod).await;
    let (request, send) = handle.next_request().await.expect("Pod delete request");
    assert_eq!(request.method(), Method::DELETE);
    assert_eq!(request.uri().path(), pod_path);
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], "pod-uid");
    assert_eq!(body["preconditions"]["resourceVersion"], "rv-pod-uid");
    send.send_response(missing_response());
    let (request, send) = handle
        .next_request()
        .await
        .expect("post-delete Pod observation");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), pod_path);
    send.send_response(missing_response());
    let (request, send) = handle
        .next_request()
        .await
        .expect("Secret GET only after Pod absence");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), secret_path);
    send.send_response(internal_error_response());

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::KubernetesApi {
            operation: ProvisionerOperation::ReconcileComputeAbsence,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_does_not_touch_children_until_pod_absence_is_observed() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-pod-terminating-after-delete");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let mut pod = observed_value(desired_for(&anchor, profile(), [0x5a; 32]).pod(), "pod-uid");
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        ResourceNames::new(session_id).pod(1).unwrap()
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(&mut handle, &pod_path, pod.clone()).await;
    respond_delete(&mut handle, &pod_path, "pod-uid").await;
    pod["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
    respond_get(&mut handle, &pod_path, pod).await;

    assert_eq!(task.await.unwrap().unwrap(), CleanupProgress::Pending);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_deletes_children_in_order_and_retains_the_pvc() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-ordered");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let expected_fence = anchor.fence().clone();
    let expected_incarnation = anchor.incarnation_id();
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let names = ResourceNames::new(session_id);
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        names.pod(1).unwrap()
    );
    let secret_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
        names.registration_secret(1).unwrap()
    );
    let policy_path = format!(
        "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
        names.pod(1).unwrap()
    );
    let account_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
        names.service_account(1).unwrap()
    );
    let resources = [
        (
            pod_path,
            observed_value(desired.pod(), "pod-uid"),
            "pod-uid",
        ),
        (
            secret_path,
            observed_value(desired.registration_secret(), "secret-uid"),
            "secret-uid",
        ),
        (
            policy_path,
            observed_value(desired.network_policy(), "policy-uid"),
            "policy-uid",
        ),
        (
            account_path,
            observed_value(desired.service_account(), "account-uid"),
            "account-uid",
        ),
    ];
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    for (path, object, uid) in resources {
        respond_get(&mut handle, &path, object).await;
        respond_delete(&mut handle, &path, uid).await;
        let (request, send) = handle
            .next_request()
            .await
            .expect("post-delete absence observation");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_compute_absence_proof(&mut handle, session_id, 1).await;

    let CleanupProgress::Absent(proof) = task.await.unwrap().unwrap() else {
        panic!("all compute children should be absent");
    };
    assert_eq!(proof.session_id(), session_id);
    assert_eq!(proof.incarnation_id(), expected_incarnation);
    assert_eq!(proof.fence(), &expected_fence);
    assert_eq!(proof.anchor_uid(), ANCHOR_UID);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_returns_pending_for_an_exact_terminating_pod() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-already-terminating");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let mut pod = observed_value(desired_for(&anchor, profile(), [0x5a; 32]).pod(), "pod-uid");
    pod["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            ResourceNames::new(session_id).pod(1).unwrap()
        ),
        pod,
    )
    .await;

    assert_eq!(task.await.unwrap().unwrap(), CleanupProgress::Pending);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_rejects_a_same_name_replacement_after_delete() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-replacement-after-delete");
    let anchor = suspending_anchor(session_id, Some("pod-uid"));
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let pod = observed_value(desired.pod(), "pod-uid");
    let replacement = observed_value(desired.pod(), "replacement-pod-uid");
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        ResourceNames::new(session_id).pod(1).unwrap()
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    respond_get(&mut handle, &pod_path, pod).await;
    respond_delete(&mut handle, &pod_path, "pod-uid").await;
    respond_get(&mut handle, &pod_path, replacement).await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::Pod,
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_proves_absence_with_lists_and_deterministic_reads() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-absence-proof");
    let anchor = suspending_anchor(session_id, None);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });
    let names = ResourceNames::new(session_id);
    for path in [
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        ),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
            names.service_account(1).unwrap()
        ),
    ] {
        let (request, send) = handle.next_request().await.expect("cleanup GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_compute_absence_proof(&mut handle, session_id, 1).await;

    assert!(matches!(
        task.await.unwrap().unwrap(),
        CleanupProgress::Absent(_)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_rejects_a_selector_evading_deterministic_child() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-selector-evasion");
    let anchor = suspending_anchor(session_id, None);
    let mut escaped_pod = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).pod(),
        "selector-evading-pod-uid",
    );
    escaped_pod["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove("openab.dev/session");
    let names = ResourceNames::new(session_id);
    let pod_collection = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    let secret_collection = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let policy_collection =
        format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_collection = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let initial_paths = [
        format!("{pod_collection}/{}", names.pod(1).unwrap()),
        format!(
            "{secret_collection}/{}",
            names.registration_secret(1).unwrap()
        ),
        format!("{policy_collection}/{}-net", names.pod(1).unwrap()),
        format!("{account_collection}/{}", names.service_account(1).unwrap()),
    ];
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });

    for path in initial_paths {
        let (request, send) = handle.next_request().await.expect("initial cleanup GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_list(&mut handle, &pod_collection, "v1", "PodList", vec![]).await;
    respond_list(&mut handle, &secret_collection, "v1", "SecretList", vec![]).await;
    respond_list(
        &mut handle,
        &policy_collection,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        vec![],
    )
    .await;
    respond_list(
        &mut handle,
        &account_collection,
        "v1",
        "ServiceAccountList",
        vec![],
    )
    .await;
    respond_get(
        &mut handle,
        &format!("{pod_collection}/{}", names.pod(1).unwrap()),
        escaped_pod,
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ChildrenAmbiguous
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_absence_proof_rejects_wrong_generation_inventory() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-orphan-inventory");
    let anchor = suspending_anchor(session_id, None);
    let mut orphan = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).pod(),
        "orphan-pod-uid",
    );
    let names = ResourceNames::new(session_id);
    orphan["metadata"]["name"] = json!(names.pod(2).unwrap());
    orphan["metadata"]["labels"]["openab.dev/generation"] = json!("2");
    orphan["metadata"]["annotations"]["openab.dev/generation"] = json!("2");
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });
    for path in [
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        ),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
            names.service_account(1).unwrap()
        ),
    ] {
        let (request, send) = handle.next_request().await.expect("cleanup GET");
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_list(
        &mut handle,
        &format!("/api/v1/namespaces/{NAMESPACE}/pods"),
        "v1",
        "PodList",
        vec![orphan],
    )
    .await;

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        GenerationProvisionerError::ChildrenAmbiguous
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_accepts_a_blocked_partial_generation_without_a_recorded_pod() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-blocked-partial");
    let mut anchor = anchor(session_id);
    let fence = anchor.fence().clone();
    anchor.transition(&fence, SessionPhase::Blocked).unwrap();
    let secret = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).registration_secret(),
        "blocked-secret-uid",
    );
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });
    let names = ResourceNames::new(session_id);
    let pod_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/pods/{}",
        names.pod(1).unwrap()
    );
    let secret_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
        names.registration_secret(1).unwrap()
    );
    let policy_path = format!(
        "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
        names.pod(1).unwrap()
    );
    let account_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
        names.service_account(1).unwrap()
    );
    let (request, send) = handle.next_request().await.expect("blocked Pod GET");
    assert_eq!(request.uri().path(), pod_path);
    send.send_response(missing_response());
    respond_get(&mut handle, &secret_path, secret).await;
    respond_delete(&mut handle, &secret_path, "blocked-secret-uid").await;
    let (request, send) = handle
        .next_request()
        .await
        .expect("blocked Secret absence observation");
    assert_eq!(request.uri().path(), secret_path);
    send.send_response(missing_response());
    for path in [policy_path, account_path] {
        let (request, send) = handle.next_request().await.expect("cleanup GET");
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_compute_absence_proof(&mut handle, session_id, 1).await;

    assert!(matches!(
        task.await.unwrap().unwrap(),
        CleanupProgress::Absent(_)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_proves_a_suspended_generation_is_still_absent() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-suspended-proof");
    let anchor = suspended_anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.reconcile_compute_absent(&stored).await });
    let names = ResourceNames::new(session_id);

    for path in [
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        ),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(1).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
            names.service_account(1).unwrap()
        ),
    ] {
        let (request, send) = handle.next_request().await.expect("cleanup absence GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_compute_absence_proof(&mut handle, session_id, 1).await;

    assert!(matches!(
        task.await.unwrap().unwrap(),
        CleanupProgress::Absent(_)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn compute_cleanup_rejects_a_wrong_phase_without_a_kubernetes_api_call() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:cleanup-wrong-phase");
    let anchor = anchor(session_id);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();

    assert_eq!(
        provisioner
            .reconcile_compute_absent(&stored)
            .await
            .unwrap_err(),
        GenerationProvisionerError::InvalidCleanupPhase {
            phase: SessionPhase::Provisioning,
        }
    );
    assert_no_request(&mut handle).await;
}

async fn respond_initial_release_compute_absence(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    generation: u64,
) {
    let names = ResourceNames::new(session_id);
    for path in [
        format!(
            "/api/v1/namespaces/{NAMESPACE}/pods/{}",
            names.pod(generation).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(generation).unwrap()
        ),
        format!(
            "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{}-net",
            names.pod(generation).unwrap()
        ),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
            names.service_account(generation).unwrap()
        ),
    ] {
        let (request, send) = handle.next_request().await.expect("release cleanup GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    respond_compute_absence_proof(handle, session_id, generation).await;
}

async fn respond_release_children_absence_after_pvc(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    generation: u64,
) {
    respond_compute_absence_proof(handle, session_id, generation).await;
    let claims_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    respond_list(
        handle,
        &claims_path,
        "v1",
        "PersistentVolumeClaimList",
        vec![],
    )
    .await;
    let (request, send) = handle
        .next_request()
        .await
        .expect("workspace PVC proof GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!("{claims_path}/{}", ResourceNames::new(session_id).pvc())
    );
    send.send_response(missing_response());
}

async fn respond_post_anchor_absence(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    session_id: SessionId,
    generation: u64,
) {
    let names = ResourceNames::new(session_id);
    let claims_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    for (path, api_version, kind) in [
        (claims_path.as_str(), "v1", "PersistentVolumeClaimList"),
        (
            policy_path.as_str(),
            "networking.k8s.io/v1",
            "NetworkPolicyList",
        ),
        (account_path.as_str(), "v1", "ServiceAccountList"),
        (secret_path.as_str(), "v1", "SecretList"),
        (pod_path.as_str(), "v1", "PodList"),
    ] {
        respond_list(handle, path, api_version, kind, vec![]).await;
    }
    for path in [
        format!("{claims_path}/{}", names.pvc()),
        format!("{policy_path}/{}-net", names.pod(generation).unwrap()),
        format!(
            "{account_path}/{}",
            names.service_account(generation).unwrap()
        ),
        format!(
            "{secret_path}/{}",
            names.registration_secret(generation).unwrap()
        ),
        format!("{pod_path}/{}", names.pod(generation).unwrap()),
    ] {
        let (request, send) = handle
            .next_request()
            .await
            .expect("post-anchor deterministic GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
}

fn real_release_coordinator(
    client: Client,
) -> Result<ReleaseCoordinator, GenerationProvisionerError> {
    let store = ConfigMapAnchorStore::new(client.clone(), NAMESPACE, scope_id()).unwrap();
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id())?;
    Ok(ReleaseCoordinator::new(
        store,
        SessionLocks::new(),
        Arc::new(provisioner),
    ))
}

async fn respond_release_anchor_get(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    anchor: &SessionAnchorV1,
) {
    let (request, send) = handle.next_request().await.expect("release anchor GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
            ResourceNames::new(anchor.session_id()).anchor()
        )
    );
    send.send_response(json_response(StatusCode::OK, anchor_config_map(anchor)));
}

async fn respond_release_anchor_delete(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    anchor: &SessionAnchorV1,
) {
    let (request, send) = handle.next_request().await.expect("release anchor DELETE");
    assert_eq!(request.method(), Method::DELETE);
    assert_eq!(
        request.uri().path(),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
            ResourceNames::new(anchor.session_id()).anchor()
        )
    );
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], ANCHOR_UID);
    assert_eq!(body["preconditions"]["resourceVersion"], "anchor-rv-1");
    send.send_response(delete_accepted_response());
}

#[tokio::test]
async fn release_deletes_exact_pvc_then_proves_every_child_and_anchor_absent() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:release-happy");
    let anchor = deleting_anchor(session_id);
    let request = release_request(&anchor);
    let pvc = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).persistent_volume_claim(),
        "pvc-uid",
    );
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    respond_release_anchor_get(&mut handle, &anchor).await;
    respond_initial_release_compute_absence(&mut handle, session_id, 1).await;
    let pvc_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
        ResourceNames::new(session_id).pvc()
    );
    respond_get(&mut handle, &pvc_path, pvc).await;
    respond_delete(&mut handle, &pvc_path, "pvc-uid").await;
    let (request, send) = handle.next_request().await.expect("post-delete PVC GET");
    assert_eq!(request.uri().path(), pvc_path);
    send.send_response(missing_response());
    respond_release_children_absence_after_pvc(&mut handle, session_id, 1).await;
    respond_release_anchor_delete(&mut handle, &anchor).await;
    let (request, send) = handle
        .next_request()
        .await
        .expect("anchor deletion observation");
    assert_eq!(request.method(), Method::GET);
    send.send_response(missing_response());
    respond_post_anchor_absence(&mut handle, session_id, 1).await;

    assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Released);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn release_returns_pending_for_an_exact_terminating_pvc() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:release-pvc-terminating");
    let anchor = deleting_anchor(session_id);
    let request = release_request(&anchor);
    let mut pvc = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).persistent_volume_claim(),
        "pvc-uid",
    );
    pvc["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
    pvc["metadata"]["finalizers"] = json!(["kubernetes.io/pvc-protection"]);
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    respond_release_anchor_get(&mut handle, &anchor).await;
    respond_initial_release_compute_absence(&mut handle, session_id, 1).await;
    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
            ResourceNames::new(session_id).pvc()
        ),
        pvc,
    )
    .await;

    assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Pending);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn release_rejects_a_same_name_pvc_replacement_after_exact_delete() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:release-pvc-replacement");
    let anchor = deleting_anchor(session_id);
    let request = release_request(&anchor);
    let desired = desired_for(&anchor, profile(), [0x5a; 32]);
    let pvc = observed_value(desired.persistent_volume_claim(), "pvc-uid");
    let replacement = observed_value(desired.persistent_volume_claim(), "replacement-pvc-uid");
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    respond_release_anchor_get(&mut handle, &anchor).await;
    respond_initial_release_compute_absence(&mut handle, session_id, 1).await;
    let pvc_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
        ResourceNames::new(session_id).pvc()
    );
    respond_get(&mut handle, &pvc_path, pvc).await;
    respond_delete(&mut handle, &pvc_path, "pvc-uid").await;
    respond_get(&mut handle, &pvc_path, replacement).await;

    assert!(matches!(
        task.await.unwrap(),
        Err(ReleaseError::Provisioner(
            GenerationProvisionerError::ResourceRejected {
                resource: GenerationResource::PersistentVolumeClaim
            }
        ))
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn release_rejects_pvc_ownership_metadata_tampering_before_delete() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:release-pvc-tamper");
    let anchor = deleting_anchor(session_id);
    let request = release_request(&anchor);
    let mut pvc = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).persistent_volume_claim(),
        "pvc-uid",
    );
    pvc["metadata"]["annotations"]["openab.dev/anchor-uid"] = json!("replacement-anchor-uid");
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    respond_release_anchor_get(&mut handle, &anchor).await;
    respond_initial_release_compute_absence(&mut handle, session_id, 1).await;
    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
            ResourceNames::new(session_id).pvc()
        ),
        pvc,
    )
    .await;

    assert!(matches!(
        task.await.unwrap(),
        Err(ReleaseError::Provisioner(
            GenerationProvisionerError::ResourceRejected {
                resource: GenerationResource::PersistentVolumeClaim
            }
        ))
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn release_does_not_acknowledge_a_pvc_delete_precondition_conflict() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let session_id = session_id("discord:release-pvc-conflict");
    let anchor = deleting_anchor(session_id);
    let request = release_request(&anchor);
    let pvc = observed_value(
        desired_for(&anchor, profile(), [0x5a; 32]).persistent_volume_claim(),
        "pvc-uid",
    );
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    respond_release_anchor_get(&mut handle, &anchor).await;
    respond_initial_release_compute_absence(&mut handle, session_id, 1).await;
    let pvc_path = format!(
        "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
        ResourceNames::new(session_id).pvc()
    );
    respond_get(&mut handle, &pvc_path, pvc).await;
    let (request, send) = handle.next_request().await.expect("PVC DELETE");
    assert_eq!(request.method(), Method::DELETE);
    assert_eq!(request.uri().path(), pvc_path);
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], "pvc-uid");
    assert_eq!(body["preconditions"]["resourceVersion"], "rv-pvc-uid");
    send.send_response(conflict_response());

    assert!(matches!(
        task.await.unwrap(),
        Err(ReleaseError::Provisioner(
            GenerationProvisionerError::KubernetesApi {
                operation: ProvisionerOperation::ReconcileStorageAbsence
            }
        ))
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn absent_anchor_post_proof_uses_request_generation_and_blocks_any_child() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let anchor = replacement_anchor(session_id("discord:release-post-anchor"));
    let session_id = anchor.session_id();
    assert_eq!(anchor.fence().generation(), 2);
    let request = release_request(&anchor);
    let coordinator = real_release_coordinator(client).unwrap();
    let task = tokio::spawn(async move { coordinator.release(&request).await });
    let mut handle = std::pin::pin!(handle);

    let (request, send) = handle.next_request().await.expect("absent anchor GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(missing_response());
    let claims_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    for (path, api_version, kind) in [
        (claims_path.as_str(), "v1", "PersistentVolumeClaimList"),
        (
            policy_path.as_str(),
            "networking.k8s.io/v1",
            "NetworkPolicyList",
        ),
        (account_path.as_str(), "v1", "ServiceAccountList"),
        (secret_path.as_str(), "v1", "SecretList"),
        (pod_path.as_str(), "v1", "PodList"),
    ] {
        respond_list(&mut handle, path, api_version, kind, vec![]).await;
    }
    let names = ResourceNames::new(session_id);
    for path in [
        format!("{claims_path}/{}", names.pvc()),
        format!("{policy_path}/{}-net", names.pod(2).unwrap()),
    ] {
        let (request, send) = handle.next_request().await.expect("post-anchor GET");
        assert_eq!(request.uri().path(), path);
        send.send_response(missing_response());
    }
    let (request, send) = handle.next_request().await.expect("generation-two SA GET");
    assert_eq!(
        request.uri().path(),
        format!("{account_path}/{}", names.service_account(2).unwrap())
    );
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": names.service_account(2).unwrap(),
                "namespace": NAMESPACE,
                "uid": "orphan-uid",
                "resourceVersion": "orphan-rv"
            }
        }),
    ));

    assert!(matches!(
        task.await.unwrap(),
        Err(ReleaseError::Provisioner(
            GenerationProvisionerError::ChildrenPresent
        ))
    ));
    assert_no_request(&mut handle).await;
}
