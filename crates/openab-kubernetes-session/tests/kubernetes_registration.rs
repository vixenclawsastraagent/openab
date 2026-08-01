#![cfg(feature = "controller")]

use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    KubernetesGenerationProvisioner, RegistrationCoordinator, RegistrationProvisioner,
    RegistrationProvisionerError, RegistrationRecovery, SessionLocks, VerifiedBootstrap,
    WorkerBootstrapAuth,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::resources::{
    DesiredGeneration, EgressPort, EgressProtocol, GenerationContext, MvpWorkerProfile,
    PersistentWorkspace, PvcAccessMode, RunAsIdentity, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::{ConfigMapAnchorStore, StoredAnchor};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const SECRET_NAME: &str = "oab-register-test-g1";
const SECRET_UID: &str = "secret-uid-a";
const SECRET_RESOURCE_VERSION: &str = "secret-rv-a";
const ANCHOR_UID: &str = "anchor-uid-a";
const POD_UID: &str = "pod-uid-a";
const TOKEN: [u8; 32] = [0x5a; 32];
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn anchor(label: &str) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    let mut anchor = SessionAnchorV1::new(
        SessionId::derive(RAW_SCOPE, label),
        scope_id(),
        ProfileRef::new("codex-strict", "2026-08-01").unwrap(),
        Uuid::from_u128(0x100),
        Uuid::from_u128(0x200),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap();
    let fence = anchor.fence().clone();
    anchor.observe_pod(&fence, POD_UID).unwrap();
    anchor
}

fn binding_for(anchor: &SessionAnchorV1) -> SessionBinding {
    SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap()
}

fn binding() -> SessionBinding {
    binding_for(&anchor("discord:registration-consume"))
}

fn profile() -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        ProfileRef::new("codex-strict", "2026-08-01").unwrap(),
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

fn proof() -> VerifiedBootstrap {
    VerifiedBootstrap::new(
        binding(),
        "anchor-uid-a",
        "pod-uid-a",
        SECRET_NAME,
        SECRET_UID,
        SECRET_RESOURCE_VERSION,
    )
    .unwrap()
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

async fn request_body(request: Request<Body>) -> Value {
    let bytes = request.into_body().collect_bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
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

fn observed_value<T: serde::Serialize>(object: &T, uid: &str) -> Value {
    let mut value = serde_json::to_value(object).unwrap();
    value["metadata"]["uid"] = json!(uid);
    value["metadata"]["resourceVersion"] = json!(format!("rv-{uid}"));
    value
}

async fn load_stored_anchor(
    client: Client,
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    anchor: &SessionAnchorV1,
) -> StoredAnchor {
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let session_id = anchor.session_id();
    let task = tokio::spawn(async move { store.get(session_id).await });
    let (request, send) = handle.next_request().await.expect("anchor GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, anchor_config_map(anchor)));
    task.await.unwrap().unwrap().unwrap()
}

async fn respond_get(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    path: &str,
    body: Value,
) {
    let (request, send) = handle.next_request().await.expect("resource GET");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), path);
    send.send_response(json_response(StatusCode::OK, body));
}

async fn respond_list(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    path: &str,
    api_version: &str,
    kind: &str,
    item: Value,
) {
    let (request, send) = handle.next_request().await.expect("resource LIST");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.uri().path(), path);
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
            "items": [item]
        }),
    ));
}

async fn run_exact_verification(
    presented_token: [u8; 32],
) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
    run_verification_scenario(
        presented_token,
        POD_UID,
        SECRET_UID,
        &format!("rv-{SECRET_UID}"),
    )
    .await
}

async fn run_verification_scenario(
    presented_token: [u8; 32],
    observed_pod_uid: &str,
    final_secret_uid: &str,
    final_secret_resource_version: &str,
) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let anchor = anchor("discord:registration-verify");
    let expected_binding = binding_for(&anchor);
    let names = ResourceNames::new(anchor.session_id());
    let context =
        GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, &anchor, names)
            .unwrap();
    let desired = DesiredGeneration::build(context, profile(), TOKEN).unwrap();
    let pvc = observed_value(desired.persistent_volume_claim(), "pvc-uid-a");
    let policy = observed_value(desired.network_policy(), "policy-uid-a");
    let account = observed_value(desired.service_account(), "account-uid-a");
    let secret = observed_value(desired.registration_secret(), SECRET_UID);
    let pod = observed_value(desired.pod(), observed_pod_uid);
    let mut final_secret = secret.clone();
    final_secret["metadata"]["uid"] = json!(final_secret_uid);
    final_secret["metadata"]["resourceVersion"] = json!(final_secret_resource_version);
    let mut handle = std::pin::pin!(handle);
    let stored = load_stored_anchor(client.clone(), &mut handle, &anchor).await;
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move {
        provisioner
            .verify_bootstrap(
                &stored,
                &profile(),
                &expected_binding,
                &WorkerBootstrapAuth::new(POD_UID, &presented_token).unwrap(),
            )
            .await
    });

    let pod_name = names.pod(1).unwrap();
    let pvc_path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let policy_path = format!("/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies");
    let account_path = format!("/api/v1/namespaces/{NAMESPACE}/serviceaccounts");
    let secret_path = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let pod_path = format!("/api/v1/namespaces/{NAMESPACE}/pods");
    let exact_secret_path = format!("{secret_path}/{}", names.registration_secret(1).unwrap());
    respond_get(&mut handle, &exact_secret_path, secret.clone()).await;
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
    respond_get(&mut handle, &exact_secret_path, secret.clone()).await;
    respond_get(&mut handle, &format!("{pod_path}/{pod_name}"), pod.clone()).await;
    if observed_pod_uid != POD_UID {
        let result = task.await.unwrap();
        assert_no_request(&mut handle).await;
        return result;
    }
    respond_list(
        &mut handle,
        &pvc_path,
        "v1",
        "PersistentVolumeClaimList",
        pvc,
    )
    .await;
    respond_list(
        &mut handle,
        &policy_path,
        "networking.k8s.io/v1",
        "NetworkPolicyList",
        policy,
    )
    .await;
    respond_list(
        &mut handle,
        &account_path,
        "v1",
        "ServiceAccountList",
        account,
    )
    .await;
    respond_list(
        &mut handle,
        &secret_path,
        "v1",
        "SecretList",
        secret.clone(),
    )
    .await;
    respond_list(&mut handle, &pod_path, "v1", "PodList", pod).await;
    respond_get(&mut handle, &exact_secret_path, final_secret).await;

    let result = task.await.unwrap();
    assert_no_request(&mut handle).await;
    result
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn registration_verifies_the_exact_generation_and_pod_uid() {
    let verified = run_exact_verification(TOKEN).await.unwrap();
    assert_eq!(verified.anchor_uid(), ANCHOR_UID);
    assert_eq!(verified.pod_uid(), POD_UID);
}

#[tokio::test]
async fn first_and_last_byte_token_mismatches_are_both_unauthorized() {
    for index in [0, TOKEN.len() - 1] {
        let mut presented = TOKEN;
        presented[index] ^= 0xff;
        assert_eq!(
            run_exact_verification(presented).await.unwrap_err(),
            RegistrationProvisionerError::Unauthorized
        );
    }
}

#[tokio::test]
async fn pod_uid_drift_fails_closed_before_inventory_or_secret_deletion() {
    assert_eq!(
        run_verification_scenario(
            TOKEN,
            "replacement-pod-uid",
            SECRET_UID,
            &format!("rv-{SECRET_UID}"),
        )
        .await
        .unwrap_err(),
        RegistrationProvisionerError::ResourceRejected
    );
}

#[tokio::test]
async fn final_secret_uid_or_resource_version_drift_fails_closed() {
    for (uid, resource_version) in [
        ("replacement-secret-uid", format!("rv-{SECRET_UID}")),
        (SECRET_UID, "replacement-secret-rv".to_owned()),
    ] {
        assert_eq!(
            run_verification_scenario(TOKEN, POD_UID, uid, &resource_version)
                .await
                .unwrap_err(),
            RegistrationProvisionerError::ResourceRejected
        );
    }
}

#[tokio::test]
async fn restart_recovery_recycles_a_missing_or_replaced_recorded_pod() {
    for observed_pod_uid in [None, Some("replacement-pod-uid")] {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let label = match observed_pod_uid {
            None => "discord:recover-missing-pod",
            Some(_) => "discord:recover-replaced-pod",
        };
        let anchor = anchor(label);
        let names = ResourceNames::new(anchor.session_id());
        let context =
            GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, &anchor, names)
                .unwrap();
        let desired = DesiredGeneration::build(context, profile(), TOKEN).unwrap();
        let pvc = observed_value(desired.persistent_volume_claim(), "pvc-uid-a");
        let policy = observed_value(desired.network_policy(), "policy-uid-a");
        let account = observed_value(desired.service_account(), "account-uid-a");
        let secret = observed_value(desired.registration_secret(), SECRET_UID);
        let replacement_pod = observed_pod_uid.map(|uid| observed_value(desired.pod(), uid));
        let store = ConfigMapAnchorStore::new(client.clone(), NAMESPACE, scope_id()).unwrap();
        let provisioner =
            KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
        let coordinator = Arc::new(RegistrationCoordinator::new(
            store,
            SessionLocks::new(),
            profile(),
            Arc::new(provisioner),
        ));
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.recover_incomplete(target).await });
        let mut handle = std::pin::pin!(handle);

        let (_anchor_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(StatusCode::OK, anchor_config_map(&anchor)));
        let pod_name = names.pod(1).unwrap();
        let secret_path = format!(
            "/api/v1/namespaces/{NAMESPACE}/secrets/{}",
            names.registration_secret(1).unwrap()
        );
        respond_get(&mut handle, &secret_path, secret.clone()).await;
        respond_get(
            &mut handle,
            &format!(
                "/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{}",
                names.pvc()
            ),
            pvc,
        )
        .await;
        respond_get(
            &mut handle,
            &format!(
                "/apis/networking.k8s.io/v1/namespaces/{NAMESPACE}/networkpolicies/{pod_name}-net"
            ),
            policy,
        )
        .await;
        respond_get(
            &mut handle,
            &format!(
                "/api/v1/namespaces/{NAMESPACE}/serviceaccounts/{}",
                names.service_account(1).unwrap()
            ),
            account,
        )
        .await;
        respond_get(&mut handle, &secret_path, secret).await;
        let (request, send) = handle.next_request().await.expect("recorded Pod GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(
            request.uri().path(),
            format!("/api/v1/namespaces/{NAMESPACE}/pods/{pod_name}")
        );
        match replacement_pod {
            Some(pod) => send.send_response(json_response(StatusCode::OK, pod)),
            None => send.send_response(missing_response()),
        }

        let (request, send) = handle.next_request().await.expect("Blocked anchor PUT");
        assert_eq!(request.method(), Method::PUT);
        let mut body = request_body(request).await;
        let blocked: SessionAnchorV1 =
            serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        assert_eq!(blocked.phase(), SessionPhase::Blocked);
        body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
        send.send_response(json_response(StatusCode::OK, body));

        assert!(matches!(
            task.await.unwrap().unwrap(),
            RegistrationRecovery::RecycleRequired { .. }
        ));
        assert_no_request(&mut handle).await;
    }
}

#[tokio::test]
async fn bootstrap_is_consumed_with_exact_preconditions_and_confirmed_absent() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.consume_bootstrap(proof()).await });
    let mut handle = std::pin::pin!(handle);

    let (request, send) = handle.next_request().await.expect("Secret DELETE");
    assert_eq!(request.method(), Method::DELETE);
    assert_eq!(
        request.uri().path(),
        format!("/api/v1/namespaces/{NAMESPACE}/secrets/{SECRET_NAME}")
    );
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], SECRET_UID);
    assert_eq!(
        body["preconditions"]["resourceVersion"],
        SECRET_RESOURCE_VERSION
    );
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    ));

    let (request, send) = handle
        .next_request()
        .await
        .expect("authoritative Secret GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(missing_response());

    let consumed = task.await.unwrap().unwrap();
    assert_eq!(consumed.binding(), &binding());
    assert_eq!(consumed.pod_uid(), "pod-uid-a");
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn retry_after_an_already_absent_secret_still_requires_a_confirming_get() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.consume_bootstrap(proof()).await });
    let mut handle = std::pin::pin!(handle);

    let (request, send) = handle.next_request().await.expect("retry Secret DELETE");
    assert_eq!(request.method(), Method::DELETE);
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], SECRET_UID);
    assert_eq!(
        body["preconditions"]["resourceVersion"],
        SECRET_RESOURCE_VERSION
    );
    send.send_response(missing_response());
    let (request, send) = handle
        .next_request()
        .await
        .expect("authoritative absence GET");
    assert_eq!(request.method(), Method::GET);
    send.send_response(missing_response());

    assert_eq!(task.await.unwrap().unwrap().pod_uid(), "pod-uid-a");
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn replacement_secret_uid_after_delete_fails_closed() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.consume_bootstrap(proof()).await });
    let mut handle = std::pin::pin!(handle);

    let (_delete, send) = handle.next_request().await.expect("Secret DELETE");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    ));
    let (_get, send) = handle
        .next_request()
        .await
        .expect("authoritative Secret GET");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": SECRET_NAME,
                "namespace": NAMESPACE,
                "uid": "replacement-secret-uid",
                "resourceVersion": "replacement-secret-rv"
            }
        }),
    ));

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        RegistrationProvisionerError::ResourceRejected
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn a_secret_still_present_after_delete_never_produces_a_consumed_proof() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task = tokio::spawn(async move { provisioner.consume_bootstrap(proof()).await });
    let mut handle = std::pin::pin!(handle);

    let (_delete, send) = handle.next_request().await.expect("Secret DELETE");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    ));
    let (_get, send) = handle
        .next_request()
        .await
        .expect("authoritative Secret GET");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": SECRET_NAME,
                "namespace": NAMESPACE,
                "uid": SECRET_UID,
                "resourceVersion": SECRET_RESOURCE_VERSION
            }
        }),
    ));

    assert_eq!(
        task.await.unwrap().unwrap_err(),
        RegistrationProvisionerError::BootstrapDeletionNotObserved
    );
    assert_no_request(&mut handle).await;
}
