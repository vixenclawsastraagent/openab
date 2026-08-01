#![cfg(feature = "controller")]

use async_trait::async_trait;
use chrono::{Duration, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    ActivationError, ActivityError, BootstrapPresence, CleanupProgress, ConsumedBootstrap,
    ControllerCoordinators, ControllerService, ControllerServiceConfigError,
    ControllerServiceError, GenerationProvisioner, GenerationProvisionerError, LifecycleError,
    LifecycleProvisioner, LifecycleServiceOutcome, ObservedWorker, RegistrationError,
    RegistrationProvisioner, RegistrationProvisionerError, ReleaseCleanupProgress, ReleaseError,
    ReleaseProvisioner, ReleasedChildrenAbsentProof, VerifiedBootstrap, WorkerBootstrapAuth,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::ControllerPolicy;
use openab_kubernetes_session::resources::{
    EgressPort, EgressProtocol, MvpWorkerProfile, PersistentWorkspace, PvcAccessMode,
    RunAsIdentity, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::{ConfigMapAnchorStore, StoredAnchor};
use openab_kubernetes_session::wire::{
    ActivationRequestV1, BrokerMappingExpectationV1, FatalCode, LifecycleRequestV1,
    WorkerRegistrationV1,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ANCHOR_UID: &str = "anchor-uid-service";
const POD_UID: &str = "worker-pod-uid-service";
const TOKEN: [u8; 32] = [0x5a; 32];

#[derive(Default)]
struct FakeProvisioner {
    registration_profiles: Mutex<Vec<ProfileRef>>,
}

impl FakeProvisioner {
    fn registration_profiles(&self) -> Vec<ProfileRef> {
        self.registration_profiles.lock().unwrap().clone()
    }
}

#[async_trait]
impl GenerationProvisioner for FakeProvisioner {
    async fn prove_v1_children_absent(
        &self,
        _session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        Ok(())
    }

    async fn ensure_provisioning_generation(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        ObservedWorker::new(POD_UID)
    }
}

#[async_trait]
impl LifecycleProvisioner for FakeProvisioner {
    async fn reconcile_compute_absent(
        &self,
        _anchor: &StoredAnchor,
    ) -> Result<CleanupProgress, GenerationProvisionerError> {
        Ok(CleanupProgress::Pending)
    }
}

#[async_trait]
impl RegistrationProvisioner for FakeProvisioner {
    async fn verify_bootstrap(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
        expected_binding: &SessionBinding,
        auth: &WorkerBootstrapAuth,
    ) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
        assert_eq!(auth.pod_uid(), POD_UID);
        self.registration_profiles
            .lock()
            .unwrap()
            .push(profile.profile().clone());
        VerifiedBootstrap::new(
            expected_binding.clone(),
            anchor.uid(),
            POD_UID,
            "oab-register-service-g1",
            "secret-uid-service",
            "secret-rv-service",
        )
    }

    async fn consume_bootstrap(
        &self,
        verified: VerifiedBootstrap,
    ) -> Result<ConsumedBootstrap, RegistrationProvisionerError> {
        Ok(verified.into_consumed())
    }

    async fn observe_bootstrap_presence(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<BootstrapPresence, RegistrationProvisionerError> {
        Ok(BootstrapPresence::Present)
    }
}

#[async_trait]
impl ReleaseProvisioner for FakeProvisioner {
    async fn reconcile_all_children_absent(
        &self,
        _anchor: &StoredAnchor,
        _compute_proof: &openab_kubernetes_session::controller::ComputeAbsentProof,
    ) -> Result<ReleaseCleanupProgress, GenerationProvisionerError> {
        Ok(ReleaseCleanupProgress::Pending)
    }

    async fn prove_released_children_absent(
        &self,
        _binding: &SessionBinding,
    ) -> Result<ReleasedChildrenAbsentProof, GenerationProvisionerError> {
        Err(GenerationProvisionerError::ChildrenAmbiguous)
    }
}

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn session_id(label: &str) -> SessionId {
    SessionId::derive(RAW_SCOPE, label)
}

fn profile_ref(version: &str) -> ProfileRef {
    ProfileRef::new(PROFILE_NAME, version).unwrap()
}

fn profile(version: &str) -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        profile_ref(version),
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

fn policy() -> ControllerPolicy {
    ControllerPolicy::new(15 * 60, 72 * 60 * 60, 20).unwrap()
}

fn store_and_handle() -> (
    ConfigMapAnchorStore,
    mock::Handle<Request<Body>, Response<Body>>,
) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let store =
        ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id()).unwrap();
    (store, handle)
}

fn coordinators(
    store: ConfigMapAnchorStore,
    profiles: impl IntoIterator<Item = MvpWorkerProfile>,
    fake: Arc<FakeProvisioner>,
) -> ControllerCoordinators {
    let generation: Arc<dyn GenerationProvisioner> = fake.clone();
    let lifecycle: Arc<dyn LifecycleProvisioner> = fake.clone();
    let registration: Arc<dyn RegistrationProvisioner> = fake.clone();
    let release: Arc<dyn ReleaseProvisioner> = fake;
    ControllerCoordinators::new(
        store,
        profiles,
        policy(),
        generation,
        lifecycle,
        registration,
        release,
    )
    .unwrap()
}

fn activation_request(profile_name: &str, label: &str) -> ActivationRequestV1 {
    ActivationRequestV1::new(
        scope_id(),
        session_id(label),
        Uuid::from_u128(0x100),
        profile_name,
        BrokerMappingExpectationV1::Absent,
    )
    .unwrap()
}

fn provisioning_anchor(label: &str, version: &str) -> SessionAnchorV1 {
    let now = Utc::now();
    let mut anchor = SessionAnchorV1::new(
        session_id(label),
        scope_id(),
        profile_ref(version),
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

fn ready_anchor(label: &str, version: &str) -> SessionAnchorV1 {
    let mut anchor = provisioning_anchor(label, version);
    let fence = anchor.fence().clone();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    anchor
}

fn binding(anchor: &SessionAnchorV1) -> SessionBinding {
    SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap()
}

fn lifecycle_request(anchor: &SessionAnchorV1, kind: &str) -> LifecycleRequestV1 {
    serde_json::from_value(json!({
        "version": 1,
        "requestId": Uuid::new_v4(),
        "kind": kind,
        "binding": {
            "version": 1,
            "scopeId": anchor.scope_id(),
            "sessionId": anchor.session_id(),
            "generation": anchor.fence().generation(),
            "attemptId": anchor.fence().attempt_id(),
            "incarnationId": anchor.incarnation_id(),
        },
        "workerSessionId": "opaque-worker-session",
    }))
    .unwrap()
}

fn config_map(anchor: &SessionAnchorV1, resource_version: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": ResourceNames::new(anchor.session_id()).anchor(),
            "namespace": NAMESPACE,
            "uid": ANCHOR_UID,
            "resourceVersion": resource_version,
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

fn empty_inventory_response() -> Response<Body> {
    json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": {"resourceVersion": "inventory-rv"},
            "items": []
        }),
    )
}

async fn request_body(request: Request<Body>) -> Value {
    let bytes = request.into_body().collect_bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn service_configuration_rejects_ambiguous_current_profiles() {
    let (store, _handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-01")],
        Arc::new(FakeProvisioner::default()),
    );
    assert!(matches!(
        ControllerService::new(root, []),
        Err(ControllerServiceConfigError::EmptyCurrentProfiles)
    ));

    let (store, _handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-01"), profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    assert!(matches!(
        ControllerService::new(root, [profile_ref("2026-08-01"), profile_ref("2026-08-02")]),
        Err(ControllerServiceConfigError::DuplicateCurrentProfileName { .. })
    ));

    let (store, _handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-01")],
        Arc::new(FakeProvisioner::default()),
    );
    assert!(matches!(
        ControllerService::new(root, [profile_ref("2026-08-03")]),
        Err(ControllerServiceConfigError::MissingCoordinator { profile })
            if profile == profile_ref("2026-08-03")
    ));
}

#[tokio::test]
async fn unknown_activation_profile_fails_before_kubernetes() {
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();

    let error = service
        .activate(&activation_request(
            "unconfigured-profile",
            "discord:unknown-profile",
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, ControllerServiceError::ProfileUnavailable));
    assert_eq!(error.fatal_code(), FatalCode::Unavailable);
    assert_eq!(
        error.to_string(),
        "the requested worker profile is unavailable"
    );

    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn wrong_scope_fails_before_profile_lookup_or_kubernetes() {
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let other_scope = ScopeId::derive("another-private-scope");
    let other_session = SessionId::derive("another-private-scope", "discord:foreign");
    let activation = ActivationRequestV1::new(
        other_scope,
        other_session,
        Uuid::from_u128(0x300),
        "unconfigured-profile",
        BrokerMappingExpectationV1::Absent,
    )
    .unwrap();

    let error = service.activate(&activation).await.unwrap_err();
    assert!(matches!(error, ControllerServiceError::ScopeMismatch));
    assert_eq!(error.fatal_code(), FatalCode::Unauthorized);

    let foreign_anchor = SessionAnchorV1::new(
        other_session,
        other_scope,
        profile_ref("2026-08-02"),
        Uuid::from_u128(0x300),
        Uuid::from_u128(0x400),
        Utc::now(),
        Utc::now() + Duration::minutes(15),
        Utc::now() + Duration::hours(72),
    )
    .unwrap();
    let registration = WorkerRegistrationV1::new(&binding(&foreign_anchor));
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let error = service.register(registration, auth).await.unwrap_err();
    assert!(matches!(error, ControllerServiceError::ScopeMismatch));
    assert_eq!(error.fatal_code(), FatalCode::Unauthorized);

    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn activation_routes_current_revision_and_owns_deadlines() {
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-01"), profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let request = activation_request(PROFILE_NAME, "discord:activation");
    let expected_session_id = request.session_id();
    let expected_attempt_id = request.attempt_id();
    let before = Utc::now();
    let task = tokio::spawn(async move { service.activate(&request).await });
    let mut handle = std::pin::pin!(handle);

    for _ in 0..3 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(missing_response());
    }

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    assert_eq!(
        list.uri().path(),
        format!("/api/v1/namespaces/{NAMESPACE}/configmaps")
    );
    send.send_response(empty_inventory_response());

    let (create, send) = handle.next_request().await.unwrap();
    assert_eq!(create.method(), Method::POST);
    let mut create_body = request_body(create).await;
    let created_anchor: SessionAnchorV1 =
        serde_json::from_str(create_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(created_anchor.session_id(), expected_session_id);
    assert_eq!(created_anchor.profile(), &profile_ref("2026-08-02"));
    assert_eq!(created_anchor.fence().attempt_id(), expected_attempt_id);
    assert_eq!(created_anchor.phase(), SessionPhase::Provisioning);
    assert_eq!(created_anchor.fence().generation(), 1);
    assert!(created_anchor.last_activity_at() >= before);
    assert_eq!(
        created_anchor.compute_deadline_at() - created_anchor.last_activity_at(),
        Duration::minutes(15)
    );
    assert_eq!(
        created_anchor.storage_deadline_at() - created_anchor.last_activity_at(),
        Duration::hours(72)
    );
    create_body["metadata"]["uid"] = json!(ANCHOR_UID);
    create_body["metadata"]["resourceVersion"] = json!("anchor-rv-1");
    send.send_response(json_response(StatusCode::CREATED, create_body));

    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut replace_body = request_body(replace).await;
    let observed_anchor: SessionAnchorV1 =
        serde_json::from_str(replace_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(observed_anchor.pod_uid(), Some(POD_UID));
    replace_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, replace_body));

    let preparation = task.await.unwrap().unwrap();
    assert!(matches!(
        preparation,
        openab_kubernetes_session::controller::ActivationPreparation::AwaitingRegistration {
            profile,
            pod_uid,
            ..
        } if profile == profile_ref("2026-08-02") && pod_uid == POD_UID
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn activation_routes_existing_durable_profile_revision() {
    let durable = provisioning_anchor("discord:historical-activation", "2026-08-01");
    let request = activation_request(PROFILE_NAME, "discord:historical-activation");
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-01"), profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let task = tokio::spawn(async move { service.activate(&request).await });
    let mut handle = std::pin::pin!(handle);

    for _ in 0..2 {
        let next = tokio::time::timeout(StdDuration::from_millis(100), handle.next_request())
            .await
            .expect("durable activation routing must perform a fresh authoritative read")
            .unwrap();
        let (get, send) = next;
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&durable, "anchor-rv-1"),
        ));
    }

    let preparation = task.await.unwrap().unwrap();
    assert!(matches!(
        preparation,
        openab_kubernetes_session::controller::ActivationPreparation::AwaitingRegistration {
            profile,
            pod_uid,
            ..
        } if profile == profile_ref("2026-08-01") && pod_uid == POD_UID
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn registration_routes_using_durable_profile_revision() {
    let durable = provisioning_anchor("discord:registration", "2026-08-01");
    let expected_binding = binding(&durable);
    let registration = WorkerRegistrationV1::new(&expected_binding);
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(
        store,
        [profile("2026-08-01"), profile("2026-08-02")],
        fake.clone(),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let task = tokio::spawn(async move { service.register(registration, auth).await });
    let mut handle = std::pin::pin!(handle);

    for _ in 0..3 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&durable, "anchor-rv-1"),
        ));
    }

    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut replace_body = request_body(replace).await;
    let ready: SessionAnchorV1 =
        serde_json::from_str(replace_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(ready.phase(), SessionPhase::Ready);
    replace_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, replace_body));

    let registered = task.await.unwrap().unwrap();
    assert_eq!(registered.binding(), &expected_binding);
    assert_eq!(registered.profile(), &profile_ref("2026-08-01"));
    assert_eq!(fake.registration_profiles(), [profile_ref("2026-08-01")]);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn lifecycle_maps_suspend_and_pending_release() {
    let ready = ready_anchor("discord:suspend", "2026-08-02");
    let suspend = lifecycle_request(&ready, "suspend");
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let task = tokio::spawn(async move { service.lifecycle(&suspend).await });
    let mut handle = std::pin::pin!(handle);

    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&ready, "anchor-rv-1"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut replace_body = request_body(replace).await;
    let suspending: SessionAnchorV1 =
        serde_json::from_str(replace_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(suspending.phase(), SessionPhase::Suspending);
    replace_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, replace_body));
    assert_eq!(
        task.await.unwrap().unwrap(),
        LifecycleServiceOutcome::SuspendAccepted
    );
    assert_no_request(&mut handle).await;

    let ready = ready_anchor("discord:release", "2026-08-02");
    let release = lifecycle_request(&ready, "release");
    let (store, handle) = store_and_handle();
    let root = coordinators(
        store,
        [profile("2026-08-02")],
        Arc::new(FakeProvisioner::default()),
    );
    let service = ControllerService::new(root, [profile_ref("2026-08-02")]).unwrap();
    let task = tokio::spawn(async move { service.lifecycle(&release).await });
    let mut handle = std::pin::pin!(handle);

    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&ready, "anchor-rv-1"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut replace_body = request_body(replace).await;
    let deleting: SessionAnchorV1 =
        serde_json::from_str(replace_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(deleting.phase(), SessionPhase::Deleting);
    replace_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, replace_body));
    assert_eq!(
        task.await.unwrap().unwrap(),
        LifecycleServiceOutcome::ReleasePending
    );
    assert_no_request(&mut handle).await;
}

#[test]
fn service_errors_have_stable_fatal_codes_and_sanitized_display() {
    let cases = [
        (
            ControllerServiceError::Activation(ActivationError::ScopeMismatch),
            FatalCode::Unauthorized,
        ),
        (
            ControllerServiceError::Registration(RegistrationError::AnchorNotFound),
            FatalCode::StaleBinding,
        ),
        (
            ControllerServiceError::Lifecycle(LifecycleError::Provisioner(
                GenerationProvisionerError::ChildrenAmbiguous,
            )),
            FatalCode::Unavailable,
        ),
        (
            ControllerServiceError::Release(ReleaseError::InvalidAnchor),
            FatalCode::Internal,
        ),
        (
            ControllerServiceError::Activity(ActivityError::TurnMismatch),
            FatalCode::StaleBinding,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.fatal_code(), expected);
        assert!(!error.to_string().contains("team-a-workers"));
    }

    let store_error = ControllerServiceError::Store(
        openab_kubernetes_session::store::AnchorStoreError::InvalidNamespace {
            namespace: "sentinel-private-namespace".to_owned(),
        },
    );
    assert_eq!(store_error.fatal_code(), FatalCode::Unavailable);
    assert_eq!(
        store_error.to_string(),
        "session inventory persistence failed"
    );
    assert!(!store_error
        .to_string()
        .contains("sentinel-private-namespace"));
}
