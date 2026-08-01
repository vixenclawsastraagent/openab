#![cfg(feature = "controller")]

use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    ActivationCoordinator, ActivationError, ActivationTiming, BootstrapPresence, CleanupProgress,
    ConsumedBootstrap, GenerationProvisioner, GenerationProvisionerError, LifecycleProvisioner,
    ObservedWorker, RegistrationCoordinator, RegistrationError, RegistrationProvisioner,
    RegistrationProvisionerError, RegistrationRecovery, ScopeCapacityAdmission, SessionLocks,
    VerifiedBootstrap, WorkerBootstrapAuth,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::ControllerPolicy;
use openab_kubernetes_session::resources::{
    EgressPort, EgressProtocol, MvpWorkerProfile, PersistentWorkspace, PvcAccessMode,
    RunAsIdentity, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::AnchorStoreError;
use openab_kubernetes_session::store::{ConfigMapAnchorStore, StoredAnchor};
use openab_kubernetes_session::wire::{
    ActivationRequestV1, BrokerMappingExpectationV1, FatalCode, WireProtocolError,
    WorkerRegistrationV1,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::Notify;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ANCHOR_UID: &str = "anchor-uid-a";
const POD_UID: &str = "pod-uid-a";
const TOKEN: [u8; 32] = [0x5a; 32];

#[derive(Clone)]
struct FakeProvisioner {
    verify_calls: Arc<AtomicUsize>,
    consume_calls: Arc<AtomicUsize>,
    missing: bool,
    recovery_error: Option<RegistrationProvisionerError>,
    consume_entered: Arc<Notify>,
    consume_release: Arc<Notify>,
    gate_consume: bool,
}

impl FakeProvisioner {
    fn successful() -> Self {
        Self {
            verify_calls: Arc::new(AtomicUsize::new(0)),
            consume_calls: Arc::new(AtomicUsize::new(0)),
            missing: false,
            recovery_error: None,
            consume_entered: Arc::new(Notify::new()),
            consume_release: Arc::new(Notify::new()),
            gate_consume: false,
        }
    }

    fn missing() -> Self {
        Self {
            missing: true,
            ..Self::successful()
        }
    }

    fn gated() -> Self {
        Self {
            gate_consume: true,
            ..Self::successful()
        }
    }

    fn invalid_recovery_resources() -> Self {
        Self::with_recovery_error(RegistrationProvisionerError::ResourceRejected)
    }

    fn with_recovery_error(error: RegistrationProvisionerError) -> Self {
        Self {
            recovery_error: Some(error),
            ..Self::successful()
        }
    }
}

#[async_trait]
impl RegistrationProvisioner for FakeProvisioner {
    async fn verify_bootstrap(
        &self,
        anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
        expected_binding: &SessionBinding,
        auth: &WorkerBootstrapAuth,
    ) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
        self.verify_calls.fetch_add(1, Ordering::SeqCst);
        if self.missing {
            return Err(RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing);
        }
        assert_eq!(auth.pod_uid(), POD_UID);
        VerifiedBootstrap::new(
            expected_binding.clone(),
            anchor.uid(),
            POD_UID,
            "oab-register-test-g1",
            "secret-uid-a",
            "secret-rv-a",
        )
    }

    async fn consume_bootstrap(
        &self,
        verified: VerifiedBootstrap,
    ) -> Result<ConsumedBootstrap, RegistrationProvisionerError> {
        self.consume_calls.fetch_add(1, Ordering::SeqCst);
        if self.gate_consume {
            self.consume_entered.notify_one();
            self.consume_release.notified().await;
        }
        Ok(verified.into_consumed())
    }

    async fn observe_bootstrap_presence(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<BootstrapPresence, RegistrationProvisionerError> {
        if let Some(error) = self.recovery_error.clone() {
            return Err(error);
        }
        Ok(if self.missing {
            BootstrapPresence::Absent
        } else {
            BootstrapPresence::Present
        })
    }
}

struct UnusedGenerationProvisioner;

#[async_trait]
impl GenerationProvisioner for UnusedGenerationProvisioner {
    async fn prove_v1_children_absent(
        &self,
        _session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        panic!("an existing session must not run the absence proof")
    }

    async fn ensure_provisioning_generation(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        panic!("an already Ready session must not provision a generation")
    }
}

#[async_trait]
impl LifecycleProvisioner for UnusedGenerationProvisioner {
    async fn reconcile_compute_absent(
        &self,
        _anchor: &StoredAnchor,
    ) -> Result<CleanupProgress, GenerationProvisionerError> {
        Ok(CleanupProgress::Pending)
    }
}

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn session_id(label: &str) -> SessionId {
    SessionId::derive(RAW_SCOPE, label)
}

fn profile_ref() -> ProfileRef {
    ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap()
}

fn profile() -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        profile_ref(),
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

fn capacity() -> ScopeCapacityAdmission {
    ScopeCapacityAdmission::from_policy(&ControllerPolicy::new(900, 259_200, 20).unwrap())
}

fn provisioning_anchor(session_id: SessionId) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    let mut anchor = SessionAnchorV1::new(
        session_id,
        scope_id(),
        profile_ref(),
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

fn binding(anchor: &SessionAnchorV1) -> SessionBinding {
    SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
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

fn api_failure_response() -> Response<Body> {
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

fn coordinator(
    fake: FakeProvisioner,
) -> (
    Arc<RegistrationCoordinator>,
    mock::Handle<Request<Body>, Response<Body>>,
) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    (
        Arc::new(RegistrationCoordinator::new(
            store,
            SessionLocks::new(),
            profile(),
            Arc::new(fake),
        )),
        handle,
    )
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn ready_is_persisted_only_after_bootstrap_consumption_finishes() {
    let fake = FakeProvisioner::gated();
    let entered = Arc::clone(&fake.consume_entered);
    let release = Arc::clone(&fake.consume_release);
    let (coordinator, handle) = coordinator(fake.clone());
    let anchor = provisioning_anchor(session_id("discord:thread-a"));
    let expected_binding = binding(&anchor);
    let registration = WorkerRegistrationV1::new(&expected_binding);
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    for _ in 0..2 {
        let (request, send) = handle.next_request().await.unwrap();
        assert_eq!(request.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "anchor-rv-1"),
        ));
    }
    entered.notified().await;
    assert_no_request(&mut handle).await;
    release.notify_one();

    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut body = request_body(request).await;
    let ready: SessionAnchorV1 =
        serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(ready.phase(), SessionPhase::Ready);
    assert_eq!(ready.pod_uid(), Some(POD_UID));
    body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, body));

    let registered = task.await.unwrap().unwrap();
    assert_eq!(registered.binding(), &expected_binding);
    assert_eq!(registered.pod_uid(), POD_UID);
    assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.consume_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_session_locks_serialize_activation_and_registration_coordinators() {
    let fake = FakeProvisioner::gated();
    let entered = Arc::clone(&fake.consume_entered);
    let release = Arc::clone(&fake.consume_release);
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let locks = SessionLocks::new();
    let registration_coordinator = Arc::new(RegistrationCoordinator::new(
        ConfigMapAnchorStore::new(client.clone(), NAMESPACE, scope_id()).unwrap(),
        locks.clone(),
        profile(),
        Arc::new(fake),
    ));
    let activation_provisioner = Arc::new(UnusedGenerationProvisioner);
    let activation_coordinator = Arc::new(ActivationCoordinator::new(
        ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap(),
        locks,
        capacity(),
        profile(),
        activation_provisioner.clone(),
        activation_provisioner,
    ));
    let anchor = provisioning_anchor(session_id("discord:shared-controller-lock"));
    let expected_binding = binding(&anchor);
    let registration = WorkerRegistrationV1::new(&expected_binding);
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let registration_task = tokio::spawn(async move {
        registration_coordinator
            .register(target, registration, auth)
            .await
    });

    let mut handle = std::pin::pin!(handle);
    for _ in 0..2 {
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "anchor-rv-1"),
        ));
    }
    entered.notified().await;

    let activation_request = ActivationRequestV1::new(
        scope_id(),
        target,
        anchor.fence().attempt_id(),
        PROFILE_NAME,
        BrokerMappingExpectationV1::Present,
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 1, 0).unwrap();
    let timing = ActivationTiming::new(now, now + Duration::minutes(15), now + Duration::hours(72));
    let activation_task = tokio::spawn(async move {
        activation_coordinator
            .prepare(&activation_request, timing)
            .await
    });
    assert_no_request(&mut handle).await;

    release.notify_one();
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut ready_body = request_body(request).await;
    let ready: SessionAnchorV1 =
        serde_json::from_str(ready_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(ready.phase(), SessionPhase::Ready);
    ready_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, ready_body));
    registration_task.await.unwrap().unwrap();

    let (_activation_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&ready, "anchor-rv-2"),
    ));
    assert!(matches!(
        activation_task.await.unwrap(),
        Err(ActivationError::AlreadyActive {
            phase: SessionPhase::Ready
        })
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn lost_ready_response_is_recovered_only_from_an_exact_ready_anchor() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:ready-response-lost"));
    let expected_binding = binding(&anchor);
    let registration = WorkerRegistrationV1::new(&expected_binding);
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    for _ in 0..2 {
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "anchor-rv-1"),
        ));
    }
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let body = request_body(request).await;
    let ready: SessionAnchorV1 =
        serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(ready.phase(), SessionPhase::Ready);
    send.send_response(api_failure_response());

    let (_recovery_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&ready, "anchor-rv-2"),
    ));

    let registered = task.await.unwrap().unwrap();
    assert_eq!(registered.binding(), &expected_binding);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn failed_ready_persistence_blocks_the_consumed_generation() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:ready-not-persisted"));
    let registration = WorkerRegistrationV1::new(&binding(&anchor));
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    for _ in 0..2 {
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "anchor-rv-1"),
        ));
    }
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let ready_body = request_body(request).await;
    let ready: SessionAnchorV1 =
        serde_json::from_str(ready_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(ready.phase(), SessionPhase::Ready);
    send.send_response(api_failure_response());

    let (_recovery_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut blocked_body = request_body(request).await;
    let blocked: SessionAnchorV1 =
        serde_json::from_str(blocked_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(blocked.phase(), SessionPhase::Blocked);
    blocked_body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, blocked_body));

    assert!(matches!(
        task.await.unwrap(),
        Err(RegistrationError::ReadyPersistenceFailed)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn stale_registration_is_rejected_before_bootstrap_mutation() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let anchor = provisioning_anchor(session_id("discord:current"));
    let stale_anchor = provisioning_anchor(session_id("discord:stale"));
    let registration = WorkerRegistrationV1::new(&binding(&stale_anchor));
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));

    assert!(matches!(
        task.await.unwrap(),
        Err(RegistrationError::Binding(_))
    ));
    assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.consume_calls.load(Ordering::SeqCst), 0);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn anchor_resource_version_drift_aborts_before_bootstrap_consumption() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let anchor = provisioning_anchor(session_id("discord:anchor-rv-drift"));
    let registration = WorkerRegistrationV1::new(&binding(&anchor));
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    for resource_version in ["anchor-rv-1", "anchor-rv-2"] {
        let (request, send) = handle.next_request().await.unwrap();
        assert_eq!(request.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, resource_version),
        ));
    }

    assert!(matches!(
        task.await.unwrap(),
        Err(RegistrationError::AnchorChanged)
    ));
    assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.consume_calls.load(Ordering::SeqCst), 0);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn missing_consumed_secret_blocks_generation_instead_of_inferring_ready() {
    let fake = FakeProvisioner::missing();
    let (coordinator, handle) = coordinator(fake.clone());
    let anchor = provisioning_anchor(session_id("discord:crash-after-secret-delete"));
    let registration = WorkerRegistrationV1::new(&binding(&anchor));
    let auth = WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap();
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.register(target, registration, auth).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));

    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut body = request_body(request).await;
    let blocked: SessionAnchorV1 =
        serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(blocked.phase(), SessionPhase::Blocked);
    assert_eq!(blocked.pod_uid(), Some(POD_UID));
    body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, body));

    assert!(matches!(
        task.await.unwrap(),
        Err(RegistrationError::RecycleRequired)
    ));
    assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.consume_calls.load(Ordering::SeqCst), 0);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn restart_recovery_waits_while_the_exact_bootstrap_secret_is_present() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:recover-present"));
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.recover_incomplete(target).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));

    assert_eq!(
        task.await.unwrap().unwrap(),
        RegistrationRecovery::AwaitingRegistration
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn restart_recovery_blocks_when_the_bootstrap_secret_is_absent() {
    let fake = FakeProvisioner::missing();
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:recover-absent"));
    let expected_binding = binding(&anchor);
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.recover_incomplete(target).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut body = request_body(request).await;
    let blocked: SessionAnchorV1 =
        serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(blocked.phase(), SessionPhase::Blocked);
    body["metadata"]["resourceVersion"] = json!("anchor-rv-2");
    send.send_response(json_response(StatusCode::OK, body));

    assert_eq!(
        task.await.unwrap().unwrap(),
        RegistrationRecovery::RecycleRequired {
            binding: expected_binding,
            pod_uid: POD_UID.to_owned(),
        }
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn restart_recovery_blocks_a_deterministically_invalid_generation() {
    let fake = FakeProvisioner::invalid_recovery_resources();
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:recover-invalid-generation"));
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.recover_incomplete(target).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));
    let (request, send) = handle.next_request().await.unwrap();
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

#[tokio::test]
async fn restart_recovery_does_not_destroy_state_on_kubernetes_api_ambiguity() {
    let fake = FakeProvisioner::with_recovery_error(
        RegistrationProvisionerError::KubernetesApi {
            operation: openab_kubernetes_session::controller::RegistrationOperation::ObserveBootstrapSecretPresence,
        },
    );
    let (coordinator, handle) = coordinator(fake);
    let anchor = provisioning_anchor(session_id("discord:recover-api-ambiguity"));
    let target = anchor.session_id();
    let task = tokio::spawn(async move { coordinator.recover_incomplete(target).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "anchor-rv-1"),
    ));

    assert!(matches!(
        task.await.unwrap(),
        Err(RegistrationError::Provisioner(
            RegistrationProvisionerError::KubernetesApi { .. }
        ))
    ));
    assert_no_request(&mut handle).await;
}

#[test]
fn bootstrap_auth_rejects_malformed_inputs_and_redacts_the_token() {
    assert!(WorkerBootstrapAuth::new("", &TOKEN).is_err());
    assert!(WorkerBootstrapAuth::new(POD_UID, &[0_u8; 31]).is_err());
    let auth = WorkerBootstrapAuth::new(POD_UID, b"secret-token-must-never-appear!!").unwrap();
    assert!(!format!("{auth:?}").contains("secret-token-must-never-appear"));
}

#[test]
fn registration_errors_have_a_closed_sanitized_fatal_code_mapping() {
    assert_eq!(
        RegistrationError::PodUidMismatch.fatal_code(),
        FatalCode::Unauthorized
    );
    assert_eq!(
        RegistrationError::Provisioner(RegistrationProvisionerError::Unauthorized).fatal_code(),
        FatalCode::Unauthorized
    );
    assert_eq!(
        RegistrationError::Binding(WireProtocolError::BindingMismatch("binding")).fatal_code(),
        FatalCode::StaleBinding
    );
    assert_eq!(
        RegistrationError::Provisioner(RegistrationProvisionerError::KubernetesApi {
            operation:
                openab_kubernetes_session::controller::RegistrationOperation::VerifyResources,
        })
        .fatal_code(),
        FatalCode::Unavailable
    );
    assert_eq!(
        RegistrationError::Store(AnchorStoreError::InvalidExpectedUid).fatal_code(),
        FatalCode::Unavailable
    );
    assert_eq!(
        RegistrationError::ProofMismatch.fatal_code(),
        FatalCode::Internal
    );
}
