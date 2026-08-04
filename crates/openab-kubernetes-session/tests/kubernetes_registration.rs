#![cfg(feature = "controller")]

#[path = "support/worker_transport.rs"]
mod worker_transport;

use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::node::v1::RuntimeClass;
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
    AllowedRuntimeClass, DesiredGeneration, EgressPort, EgressProtocol, GenerationContext,
    MvpWorkerProfile, PersistentWorkspace, PinnedSkillsConfigMap, PvcAccessMode, RunAsIdentity,
    RuntimeClassSelection, TrustedEgressRule, WorkerResources,
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
const SKILLS_NAME: &str = "team-skills-v1";
const SKILLS_UID: &str = "skills-uid-a";
const SKILLS_RESOURCE_VERSION: &str = "skills-rv-a";
const RUNTIME_CLASS_NAME: &str = "kata";
const RUNTIME_CLASS_HANDLER: &str = "kata-qemu";
const RUNTIME_CLASS_UID: &str = "runtime-class-uid-a";
const RUNTIME_CLASS_RESOURCE_VERSION: &str = "runtime-class-rv-a";

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

fn profile_with_pins(
    runtime_class: Option<RuntimeClassSelection>,
    skills: Option<PinnedSkillsConfigMap>,
) -> MvpWorkerProfile {
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
        runtime_class,
        skills,
    )
    .unwrap()
}

fn profile() -> MvpWorkerProfile {
    profile_with_pins(None, None)
}

fn observed_skills_config_map() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": SKILLS_NAME,
            "namespace": NAMESPACE,
            "uid": SKILLS_UID,
            "resourceVersion": SKILLS_RESOURCE_VERSION
        },
        "immutable": true,
        "data": {"SKILL.md": "trusted versioned skills"}
    })
}

fn observed_runtime_class() -> Value {
    json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {
            "name": RUNTIME_CLASS_NAME,
            "uid": RUNTIME_CLASS_UID,
            "resourceVersion": RUNTIME_CLASS_RESOURCE_VERSION
        },
        "handler": RUNTIME_CLASS_HANDLER
    })
}

fn profile_with_external_pins() -> MvpWorkerProfile {
    let observed_skills: ConfigMap = serde_json::from_value(observed_skills_config_map()).unwrap();
    let skills = PinnedSkillsConfigMap::from_observed(NAMESPACE, &observed_skills).unwrap();
    let observed_runtime: RuntimeClass = serde_json::from_value(observed_runtime_class()).unwrap();
    let runtime_class = RuntimeClassSelection::from_observed(
        &observed_runtime,
        [AllowedRuntimeClass::new(RUNTIME_CLASS_NAME, RUNTIME_CLASS_HANDLER).unwrap()],
    )
    .unwrap();
    profile_with_pins(Some(runtime_class), Some(skills))
}

#[derive(Clone, Copy)]
enum ExternalPinDrift {
    SkillsUid,
    SkillsResourceVersion,
    SkillsMutable,
    SkillsDeleting,
    RuntimeClassResourceVersion,
    RuntimeClassHandler,
    RelayCaUid,
    RelayCaResourceVersion,
    RelayCaDeleting,
    RelayCaInvalidPem,
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
    run_verification_scenario_with_profile(
        presented_token,
        observed_pod_uid,
        final_secret_uid,
        final_secret_resource_version,
        profile(),
        None,
    )
    .await
}

async fn run_verification_scenario_with_profile(
    presented_token: [u8; 32],
    observed_pod_uid: &str,
    final_secret_uid: &str,
    final_secret_resource_version: &str,
    worker_profile: MvpWorkerProfile,
    external_pin_drift: Option<ExternalPinDrift>,
) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let anchor = anchor("discord:registration-verify");
    let expected_binding = binding_for(&anchor);
    let names = ResourceNames::new(anchor.session_id());
    let context =
        GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, &anchor, names)
            .unwrap();
    let desired = DesiredGeneration::build(context, worker_profile.clone(), TOKEN).unwrap();
    let has_relay_ca = worker_profile.relay_ca_config_map().is_some();
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
                &worker_profile,
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

    if let Some(drift) = external_pin_drift {
        let mut skills = observed_skills_config_map();
        let mut runtime_class = observed_runtime_class();
        let mut relay_ca =
            serde_json::to_value(worker_transport::observed_relay_ca(NAMESPACE)).unwrap();
        match drift {
            ExternalPinDrift::SkillsUid => {
                skills["metadata"]["uid"] = json!("replacement-skills-uid");
            }
            ExternalPinDrift::SkillsResourceVersion => {
                skills["metadata"]["resourceVersion"] =
                    json!("replacement-skills-resource-version");
            }
            ExternalPinDrift::SkillsMutable => skills["immutable"] = json!(false),
            ExternalPinDrift::SkillsDeleting => {
                skills["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
            }
            ExternalPinDrift::RuntimeClassResourceVersion => {
                runtime_class["metadata"]["resourceVersion"] =
                    json!("replacement-runtime-class-resource-version");
            }
            ExternalPinDrift::RuntimeClassHandler => {
                runtime_class["handler"] = json!("replacement-handler");
            }
            ExternalPinDrift::RelayCaUid => {
                relay_ca["metadata"]["uid"] = json!("replacement-relay-ca-uid");
            }
            ExternalPinDrift::RelayCaResourceVersion => {
                relay_ca["metadata"]["resourceVersion"] =
                    json!("replacement-relay-ca-resource-version");
            }
            ExternalPinDrift::RelayCaDeleting => {
                relay_ca["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
            }
            ExternalPinDrift::RelayCaInvalidPem => {
                relay_ca["data"]["ca.crt"] = json!("not a certificate");
            }
        }

        if matches!(
            drift,
            ExternalPinDrift::SkillsUid
                | ExternalPinDrift::SkillsResourceVersion
                | ExternalPinDrift::SkillsMutable
                | ExternalPinDrift::SkillsDeleting
        ) {
            respond_get(
                &mut handle,
                &format!("/api/v1/namespaces/{NAMESPACE}/configmaps/{SKILLS_NAME}"),
                skills,
            )
            .await;
            let result = task.await.unwrap();
            assert_no_request(&mut handle).await;
            return result;
        }

        if matches!(
            drift,
            ExternalPinDrift::RuntimeClassResourceVersion | ExternalPinDrift::RuntimeClassHandler
        ) {
            respond_get(
                &mut handle,
                &format!("/api/v1/namespaces/{NAMESPACE}/configmaps/{SKILLS_NAME}"),
                skills,
            )
            .await;
            respond_get(
                &mut handle,
                &format!("/apis/node.k8s.io/v1/runtimeclasses/{RUNTIME_CLASS_NAME}"),
                runtime_class,
            )
            .await;
        } else {
            respond_get(
                &mut handle,
                &format!(
                    "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
                    worker_transport::RELAY_CA_NAME
                ),
                relay_ca,
            )
            .await;
        }
        let result = task.await.unwrap();
        assert_no_request(&mut handle).await;
        return result;
    }

    if has_relay_ca {
        respond_get(
            &mut handle,
            &format!(
                "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
                worker_transport::RELAY_CA_NAME
            ),
            serde_json::to_value(worker_transport::observed_relay_ca(NAMESPACE)).unwrap(),
        )
        .await;
    }

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
async fn external_pin_drift_fails_registration_before_bootstrap_secret_deletion() {
    for drift in [
        ExternalPinDrift::SkillsUid,
        ExternalPinDrift::SkillsResourceVersion,
        ExternalPinDrift::SkillsMutable,
        ExternalPinDrift::SkillsDeleting,
        ExternalPinDrift::RuntimeClassResourceVersion,
        ExternalPinDrift::RuntimeClassHandler,
    ] {
        assert_eq!(
            run_verification_scenario_with_profile(
                TOKEN,
                POD_UID,
                SECRET_UID,
                &format!("rv-{SECRET_UID}"),
                profile_with_external_pins(),
                Some(drift),
            )
            .await
            .unwrap_err(),
            RegistrationProvisionerError::ResourceRejected
        );
    }
}

#[tokio::test]
async fn relay_ca_drift_or_invalidity_fails_before_bootstrap_secret_deletion() {
    for drift in [
        ExternalPinDrift::RelayCaUid,
        ExternalPinDrift::RelayCaResourceVersion,
        ExternalPinDrift::RelayCaDeleting,
        ExternalPinDrift::RelayCaInvalidPem,
    ] {
        assert_eq!(
            run_verification_scenario_with_profile(
                TOKEN,
                POD_UID,
                SECRET_UID,
                &format!("rv-{SECRET_UID}"),
                worker_transport::transport_profile(NAMESPACE),
                Some(drift),
            )
            .await
            .unwrap_err(),
            RegistrationProvisionerError::ResourceRejected
        );
    }
}

#[tokio::test]
async fn exact_relay_ca_allows_bootstrap_verification() {
    assert!(run_verification_scenario_with_profile(
        TOKEN,
        POD_UID,
        SECRET_UID,
        &format!("rv-{SECRET_UID}"),
        worker_transport::transport_profile(NAMESPACE),
        None,
    )
    .await
    .is_ok());
}

#[tokio::test]
async fn relay_ca_drift_at_consumption_fails_before_bootstrap_secret_deletion() {
    for drift in [
        ExternalPinDrift::RelayCaUid,
        ExternalPinDrift::RelayCaResourceVersion,
        ExternalPinDrift::RelayCaDeleting,
        ExternalPinDrift::RelayCaInvalidPem,
    ] {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let provisioner =
            KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
        let worker_profile = worker_transport::transport_profile(NAMESPACE);
        let task = tokio::spawn(async move {
            provisioner
                .consume_bootstrap(&worker_profile, proof())
                .await
        });
        let mut handle = std::pin::pin!(handle);

        let (request, send) = handle.next_request().await.expect("relay CA GET");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(
            request.uri().path(),
            format!(
                "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
                worker_transport::RELAY_CA_NAME
            )
        );
        let mut observed_ca =
            serde_json::to_value(worker_transport::observed_relay_ca(NAMESPACE)).unwrap();
        match drift {
            ExternalPinDrift::RelayCaUid => {
                observed_ca["metadata"]["uid"] = json!("replacement-relay-ca-uid");
            }
            ExternalPinDrift::RelayCaResourceVersion => {
                observed_ca["metadata"]["resourceVersion"] =
                    json!("replacement-relay-ca-resource-version");
            }
            ExternalPinDrift::RelayCaDeleting => {
                observed_ca["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
            }
            ExternalPinDrift::RelayCaInvalidPem => {
                observed_ca["data"]["ca.crt"] = json!("not a certificate");
            }
            _ => unreachable!(),
        }
        send.send_response(json_response(StatusCode::OK, observed_ca));

        assert_eq!(
            task.await.unwrap().unwrap_err(),
            RegistrationProvisionerError::ResourceRejected
        );
        assert_no_request(&mut handle).await;
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
    let task =
        tokio::spawn(async move { provisioner.consume_bootstrap(&profile(), proof()).await });
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
async fn exact_relay_ca_is_revalidated_immediately_before_bootstrap_consumption() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let worker_profile = worker_transport::transport_profile(NAMESPACE);
    let task = tokio::spawn(async move {
        provisioner
            .consume_bootstrap(&worker_profile, proof())
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(
        &mut handle,
        &format!(
            "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
            worker_transport::RELAY_CA_NAME
        ),
        serde_json::to_value(worker_transport::observed_relay_ca(NAMESPACE)).unwrap(),
    )
    .await;
    let (request, send) = handle.next_request().await.expect("Secret DELETE");
    assert_eq!(request.method(), Method::DELETE);
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

    assert_eq!(task.await.unwrap().unwrap().pod_uid(), "pod-uid-a");
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn retry_after_an_already_absent_secret_still_requires_a_confirming_get() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let provisioner = KubernetesGenerationProvisioner::new(client, NAMESPACE, scope_id()).unwrap();
    let task =
        tokio::spawn(async move { provisioner.consume_bootstrap(&profile(), proof()).await });
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
    let task =
        tokio::spawn(async move { provisioner.consume_bootstrap(&profile(), proof()).await });
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
    let task =
        tokio::spawn(async move { provisioner.consume_bootstrap(&profile(), proof()).await });
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
