#![cfg(feature = "controller")]

use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::controller::{
    ActivationCoordinator, ActivationError, ActivationPreparation, ActivationTiming,
    CleanupProgress, GenerationProvisioner, GenerationProvisionerError, LifecycleProvisioner,
    ObservedWorker, ProvisionerOperation, ScopeCapacityAdmission, SessionLocks,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::ControllerPolicy;
use openab_kubernetes_session::resources::{
    EgressPort, EgressProtocol, MvpWorkerProfile, PersistentWorkspace, PvcAccessMode,
    RunAsIdentity, TrustedEgressRule, WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::{
    AnchorStoreError, ConfigMapAnchorStore, StoreOperation, StoredAnchor,
};
use openab_kubernetes_session::wire::{
    ActivationRequestV1, BrokerMappingExpectationV1, ValidatedActivationOutcomeV1,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;
use tokio::sync::Notify;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const POD_UID: &str = "a8f21326-39ef-4016-9b2a-fb31359062be";

#[derive(Clone)]
struct FakeProvisioner {
    state: Arc<Mutex<FakeState>>,
}

struct FakeState {
    proof_result: Result<(), GenerationProvisionerError>,
    worker_result: Result<ObservedWorker, GenerationProvisionerError>,
    proof_calls: Vec<SessionId>,
    ensure_calls: Vec<(SessionId, ProfileRef)>,
}

impl FakeProvisioner {
    fn successful() -> Self {
        Self::new(Ok(()), Ok(ObservedWorker::new(POD_UID).unwrap()))
    }

    fn with_proof_error(error: GenerationProvisionerError) -> Self {
        Self::new(Err(error), Ok(ObservedWorker::new(POD_UID).unwrap()))
    }

    fn with_worker_uid(pod_uid: &str) -> Self {
        Self::new(Ok(()), Ok(ObservedWorker::new(pod_uid).unwrap()))
    }

    fn new(
        proof_result: Result<(), GenerationProvisionerError>,
        worker_result: Result<ObservedWorker, GenerationProvisionerError>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState {
                proof_result,
                worker_result,
                proof_calls: Vec::new(),
                ensure_calls: Vec::new(),
            })),
        }
    }

    fn proof_calls(&self) -> Vec<SessionId> {
        self.state.lock().unwrap().proof_calls.clone()
    }

    fn ensure_calls(&self) -> Vec<(SessionId, ProfileRef)> {
        self.state.lock().unwrap().ensure_calls.clone()
    }
}

#[async_trait]
impl GenerationProvisioner for FakeProvisioner {
    async fn prove_v1_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        let mut state = self.state.lock().unwrap();
        state.proof_calls.push(session_id);
        state.proof_result.clone()
    }

    async fn ensure_provisioning_generation(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_calls
            .push((anchor.state().session_id(), profile.profile().clone()));
        state.worker_result.clone()
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

struct GateProvisioner {
    blocked_session: SessionId,
    block_once: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl GateProvisioner {
    fn new(blocked_session: SessionId) -> Self {
        Self {
            blocked_session,
            block_once: AtomicBool::new(true),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

#[async_trait]
impl GenerationProvisioner for GateProvisioner {
    async fn prove_v1_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        if session_id == self.blocked_session && self.block_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }

    async fn ensure_provisioning_generation(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        Ok(ObservedWorker::new(POD_UID).unwrap())
    }
}

#[async_trait]
impl LifecycleProvisioner for GateProvisioner {
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

fn attempt_id() -> Uuid {
    Uuid::from_u128(0x100)
}

fn timing() -> ActivationTiming {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    ActivationTiming::new(now, now + Duration::minutes(15), now + Duration::hours(72))
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

fn capacity() -> ScopeCapacityAdmission {
    capacity_with_limit(20)
}

fn capacity_with_limit(max_active_workers: usize) -> ScopeCapacityAdmission {
    ScopeCapacityAdmission::from_policy(
        &ControllerPolicy::new(900, 259_200, max_active_workers).unwrap(),
    )
}

fn request(session_id: SessionId, expectation: BrokerMappingExpectationV1) -> ActivationRequestV1 {
    ActivationRequestV1::new(
        scope_id(),
        session_id,
        attempt_id(),
        PROFILE_NAME,
        expectation,
    )
    .unwrap()
}

fn anchor(session_id: SessionId, attempt_id: Uuid, version: &str) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        session_id,
        scope_id(),
        profile_ref(version),
        attempt_id,
        Uuid::from_u128(0x200),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap()
}

fn anchor_in_phase(session_id: SessionId, phase: SessionPhase) -> SessionAnchorV1 {
    let mut value =
        serde_json::to_value(anchor(session_id, Uuid::from_u128(0x999), PROFILE_VERSION)).unwrap();
    value["phase"] = serde_json::to_value(phase).unwrap();
    value["podUid"] = if matches!(phase, SessionPhase::Ready | SessionPhase::Busy) {
        json!("existing-pod-uid")
    } else {
        Value::Null
    };
    serde_json::from_value(value).unwrap()
}

fn config_map(anchor: &SessionAnchorV1, uid: &str, resource_version: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": ResourceNames::new(anchor.session_id()).anchor(),
            "namespace": NAMESPACE,
            "uid": uid,
            "resourceVersion": resource_version,
            "labels": {
                "app.kubernetes.io/managed-by": "openab-session-controller",
                "openab.dev/resource": "session-anchor"
            }
        },
        "data": {
            "anchor.json": serde_json::to_string(anchor).unwrap()
        }
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
            .map(|(index, anchor)| config_map(anchor, &format!("uid-{index}"), &format!("rv-{index}")))
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

fn missing_response() -> Response<Body> {
    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": "not found",
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
            "message": "resource version changed",
            "reason": "Conflict",
            "code": 409
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
    Arc<ActivationCoordinator>,
    mock::Handle<Request<Body>, Response<Body>>,
) {
    coordinator_with(Arc::new(fake))
}

fn coordinator_with<P>(
    provisioner: Arc<P>,
) -> (
    Arc<ActivationCoordinator>,
    mock::Handle<Request<Body>, Response<Body>>,
)
where
    P: GenerationProvisioner + LifecycleProvisioner + 'static,
{
    coordinator_with_capacity(provisioner, capacity())
}

fn coordinator_with_capacity<P>(
    provisioner: Arc<P>,
    capacity: ScopeCapacityAdmission,
) -> (
    Arc<ActivationCoordinator>,
    mock::Handle<Request<Body>, Response<Body>>,
)
where
    P: GenerationProvisioner + LifecycleProvisioner + 'static,
{
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    (
        Arc::new(ActivationCoordinator::new(
            store,
            SessionLocks::new(),
            capacity,
            profile(),
            provisioner.clone(),
            provisioner,
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
async fn wrong_scope_and_requested_profile_fail_before_kubernetes() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let wrong_scope = ActivationRequestV1::new(
        ScopeId::derive("another-scope"),
        session_id("discord:thread-a"),
        attempt_id(),
        PROFILE_NAME,
        BrokerMappingExpectationV1::Present,
    )
    .unwrap();

    assert!(matches!(
        coordinator.prepare(&wrong_scope, timing()).await,
        Err(ActivationError::ScopeMismatch)
    ));

    let wrong_profile = ActivationRequestV1::new(
        scope_id(),
        session_id("discord:thread-a"),
        attempt_id(),
        "another-profile",
        BrokerMappingExpectationV1::Present,
    )
    .unwrap();
    assert!(matches!(
        coordinator.prepare(&wrong_profile, timing()).await,
        Err(ActivationError::RequestedProfileMismatch)
    ));

    assert!(fake.proof_calls().is_empty());
    assert!(fake.ensure_calls().is_empty());
    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn present_mapping_gets_proof_then_second_get_and_returns_correlated_absence() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let activation = request(
        session_id("discord:thread-present"),
        BrokerMappingExpectationV1::Present,
    );
    let expected = activation.clone();
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (first, send) = handle.next_request().await.unwrap();
    assert_eq!(first.method(), Method::GET);
    send.send_response(missing_response());

    let (second, send) = handle.next_request().await.unwrap();
    assert_eq!(second.method(), Method::GET);
    assert_eq!(fake.proof_calls(), vec![expected.session_id()]);
    send.send_response(missing_response());

    let preparation = task.await.unwrap().unwrap();
    let ActivationPreparation::MappingAbsent(response) = preparation else {
        panic!("absence proof must not fabricate an activated worker")
    };
    assert_eq!(
        response.into_validated_outcome(&expected).unwrap(),
        ValidatedActivationOutcomeV1::MappingAbsent
    );
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn every_failed_absence_proof_fails_closed_without_a_second_get() {
    for expected_error in [
        GenerationProvisionerError::ChildrenPresent,
        GenerationProvisionerError::ChildrenAmbiguous,
        GenerationProvisionerError::KubernetesApi {
            operation: ProvisionerOperation::ProveChildrenAbsent,
        },
    ] {
        let fake = FakeProvisioner::with_proof_error(expected_error.clone());
        let (coordinator, handle) = coordinator(fake.clone());
        let activation = request(
            session_id(&format!("discord:proof-{expected_error:?}")),
            BrokerMappingExpectationV1::Present,
        );
        let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

        let mut handle = std::pin::pin!(handle);
        let (_first, send) = handle.next_request().await.unwrap();
        send.send_response(missing_response());

        assert!(matches!(
            task.await.unwrap(),
            Err(ActivationError::Provisioner(error)) if error == expected_error
        ));
        assert_eq!(fake.proof_calls().len(), 1);
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }
}

#[tokio::test]
async fn absent_mapping_creates_generation_one_then_records_observed_pod() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let activation = request(
        session_id("discord:new-thread"),
        BrokerMappingExpectationV1::Absent,
    );
    let expected_session = activation.session_id();
    let expected_attempt = activation.attempt_id();
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&[])));

    let (create, send) = handle.next_request().await.unwrap();
    assert_eq!(create.method(), Method::POST);
    let mut create_body = request_body(create).await;
    let created_anchor: SessionAnchorV1 =
        serde_json::from_str(create_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(created_anchor.session_id(), expected_session);
    assert_eq!(created_anchor.scope_id(), scope_id());
    assert_eq!(created_anchor.profile(), &profile_ref(PROFILE_VERSION));
    assert_eq!(created_anchor.fence().generation(), 1);
    assert_eq!(created_anchor.fence().attempt_id(), expected_attempt);
    assert!(!created_anchor.incarnation_id().is_nil());
    assert_eq!(created_anchor.phase(), SessionPhase::Provisioning);
    assert_eq!(created_anchor.pod_uid(), None);
    create_body["metadata"]["uid"] = json!("anchor-uid-new");
    create_body["metadata"]["resourceVersion"] = json!("rv-1");
    send.send_response(json_response(StatusCode::CREATED, create_body));

    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut replace_body = request_body(replace).await;
    let replaced_anchor: SessionAnchorV1 =
        serde_json::from_str(replace_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(replaced_anchor.pod_uid(), Some(POD_UID));
    assert_eq!(
        replaced_anchor.incarnation_id(),
        created_anchor.incarnation_id()
    );
    replace_body["metadata"]["resourceVersion"] = json!("rv-2");
    send.send_response(json_response(StatusCode::OK, replace_body));

    let preparation = task.await.unwrap().unwrap();
    let ActivationPreparation::AwaitingRegistration {
        binding,
        profile,
        pod_uid,
    } = preparation
    else {
        panic!("new worker must still await authenticated registration")
    };
    assert_eq!(binding.session_id(), expected_session);
    assert_eq!(binding.fence().generation(), 1);
    assert_eq!(binding.fence().attempt_id(), expected_attempt);
    assert_eq!(binding.incarnation_id(), created_anchor.incarnation_id());
    assert_eq!(profile, profile_ref(PROFILE_VERSION));
    assert_eq!(pod_uid, POD_UID);
    assert_eq!(fake.proof_calls(), vec![expected_session]);
    assert_eq!(
        fake.ensure_calls(),
        vec![(expected_session, profile_ref(PROFILE_VERSION))]
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn absent_mapping_fails_closed_when_active_worker_capacity_is_full() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) =
        coordinator_with_capacity(Arc::new(fake.clone()), capacity_with_limit(1));
    let activation = request(
        session_id("discord:capacity-rejected"),
        BrokerMappingExpectationV1::Absent,
    );
    let expected_session = activation.session_id();
    let active = anchor_in_phase(session_id("discord:capacity-existing"), SessionPhase::Ready);
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    assert_eq!(
        list.uri().path(),
        format!("/api/v1/namespaces/{NAMESPACE}/configmaps")
    );
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&active)),
    ));

    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, ActivationError::CapacityExhausted));
    assert_eq!(
        error.to_string(),
        "the configured active-worker capacity is exhausted"
    );
    assert_eq!(fake.proof_calls(), vec![expected_session]);
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn capacity_inventory_failure_never_creates_or_provisions() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let activation = request(
        session_id("discord:capacity-inventory-failure"),
        BrokerMappingExpectationV1::Absent,
    );
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
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

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivationError::Store(AnchorStoreError::Kubernetes {
            operation: StoreOperation::List,
            ..
        }))
    ));
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn matching_provisioning_attempt_idempotently_continues() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) =
        coordinator_with_capacity(Arc::new(fake.clone()), capacity_with_limit(1));
    let session_id = session_id("discord:retry");
    let activation = request(session_id, BrokerMappingExpectationV1::Present);
    let existing = anchor(session_id, attempt_id(), PROFILE_VERSION);
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut body = request_body(replace).await;
    body["metadata"]["resourceVersion"] = json!("rv-2");
    send.send_response(json_response(StatusCode::OK, body));

    assert!(matches!(
        task.await.unwrap().unwrap(),
        ActivationPreparation::AwaitingRegistration { .. }
    ));
    assert_eq!(fake.ensure_calls().len(), 1);
}

#[tokio::test]
async fn absent_mapping_can_retry_the_exact_created_provisioning_attempt() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let session_id = session_id("discord:lost-activation-response");
    let activation = request(session_id, BrokerMappingExpectationV1::Absent);
    let existing = anchor(session_id, attempt_id(), PROFILE_VERSION);
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut body = request_body(replace).await;
    body["metadata"]["resourceVersion"] = json!("rv-2");
    send.send_response(json_response(StatusCode::OK, body));

    assert!(matches!(
        task.await.unwrap().unwrap(),
        ActivationPreparation::AwaitingRegistration { .. }
    ));
    assert_eq!(fake.ensure_calls().len(), 1);
}

#[tokio::test]
async fn different_attempt_and_different_pinned_profile_fail_before_mutation() {
    for existing in [
        anchor(
            session_id("discord:wrong-attempt"),
            Uuid::from_u128(0x999),
            PROFILE_VERSION,
        ),
        anchor(
            session_id("discord:wrong-version"),
            attempt_id(),
            "2026-07-31",
        ),
    ] {
        let fake = FakeProvisioner::successful();
        let (coordinator, handle) = coordinator(fake.clone());
        let activation = request(existing.session_id(), BrokerMappingExpectationV1::Present);
        let expect_profile_error = existing.profile().version() != PROFILE_VERSION;
        let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

        let mut handle = std::pin::pin!(handle);
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&existing, "anchor-uid", "rv-1"),
        ));

        let result = task.await.unwrap();
        if expect_profile_error {
            assert!(matches!(
                result,
                Err(ActivationError::AnchorProfileMismatch)
            ));
        } else {
            assert!(matches!(
                result,
                Err(ActivationError::ProvisioningAttemptMismatch)
            ));
        }
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }
}

#[tokio::test]
async fn matching_recorded_pod_uid_is_idempotent_without_anchor_rewrite() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let session_id = session_id("discord:recorded-pod");
    let activation = request(session_id, BrokerMappingExpectationV1::Present);
    let mut existing = anchor(session_id, attempt_id(), PROFILE_VERSION);
    let fence = existing.fence().clone();
    existing.observe_pod(&fence, POD_UID).unwrap();
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));

    assert!(matches!(
        task.await.unwrap().unwrap(),
        ActivationPreparation::AwaitingRegistration { pod_uid, .. } if pod_uid == POD_UID
    ));
    assert_eq!(fake.ensure_calls().len(), 1);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn different_observed_pod_uid_fails_closed_without_anchor_rewrite() {
    let fake = FakeProvisioner::with_worker_uid("replacement-pod-uid");
    let (coordinator, handle) = coordinator(fake.clone());
    let session_id = session_id("discord:pod-mismatch");
    let activation = request(session_id, BrokerMappingExpectationV1::Present);
    let mut existing = anchor(session_id, attempt_id(), PROFILE_VERSION);
    let fence = existing.fence().clone();
    existing.observe_pod(&fence, POD_UID).unwrap();
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivationError::State(_))
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn anchor_cas_failure_never_returns_success() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake);
    let session_id = session_id("discord:cas-conflict");
    let activation = request(session_id, BrokerMappingExpectationV1::Present);
    let existing = anchor(session_id, attempt_id(), PROFILE_VERSION);
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    send.send_response(conflict_response());

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivationError::Store(
            openab_kubernetes_session::store::AnchorStoreError::Conflict {
                operation: StoreOperation::Replace,
                ..
            }
        ))
    ));
}

#[tokio::test]
async fn anchor_create_conflict_never_provisions_or_returns_success() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let activation = request(
        session_id("discord:create-conflict"),
        BrokerMappingExpectationV1::Absent,
    );
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&[])));
    let (create, send) = handle.next_request().await.unwrap();
    assert_eq!(create.method(), Method::POST);
    send.send_response(conflict_response());

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivationError::Store(AnchorStoreError::Conflict {
            operation: StoreOperation::Create,
            ..
        }))
    ));
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn malformed_anchor_create_response_never_provisions_or_returns_success() {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let activation = request(
        session_id("discord:malformed-create"),
        BrokerMappingExpectationV1::Absent,
    );
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&[])));
    let (create, send) = handle.next_request().await.unwrap();
    assert_eq!(create.method(), Method::POST);
    let mut malformed = request_body(create).await;
    // Without a UID, the store cannot establish object identity or safely
    // issue its guarded recovery DELETE.
    malformed["metadata"]["resourceVersion"] = json!("rv-1");
    send.send_response(json_response(StatusCode::CREATED, malformed));

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivationError::Store(
            AnchorStoreError::WriteRecoveryFailed {
                operation: StoreOperation::Create,
                ..
            }
        ))
    ));
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
}

async fn classify_existing(
    phase: SessionPhase,
    expectation: BrokerMappingExpectationV1,
) -> ActivationError {
    let fake = FakeProvisioner::successful();
    let (coordinator, handle) = coordinator(fake.clone());
    let session_id = session_id(&format!("discord:{phase:?}-{expectation:?}"));
    let activation = request(session_id, expectation);
    let existing = anchor_in_phase(session_id, phase);
    let task = tokio::spawn(async move { coordinator.prepare(&activation, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&existing, "anchor-uid", "rv-1"),
    ));
    let error = task.await.unwrap().unwrap_err();
    assert!(fake.ensure_calls().is_empty());
    assert_no_request(&mut handle).await;
    error
}

#[tokio::test]
async fn existing_phase_and_mapping_expectation_table_is_fail_closed() {
    // The exact same Provisioning attempt is tested above as the sole
    // Absent + existing idempotency exception. Every different attempt and
    // every other phase conflicts.
    for phase in [
        SessionPhase::Provisioning,
        SessionPhase::Ready,
        SessionPhase::Busy,
        SessionPhase::Suspending,
        SessionPhase::Suspended,
        SessionPhase::Deleting,
        SessionPhase::Blocked,
    ] {
        assert!(matches!(
            classify_existing(phase, BrokerMappingExpectationV1::Absent).await,
            ActivationError::UnexpectedExistingAnchor
        ));
    }

    assert!(matches!(
        classify_existing(
            SessionPhase::Provisioning,
            BrokerMappingExpectationV1::Present
        )
        .await,
        ActivationError::ProvisioningAttemptMismatch
    ));
    for phase in [SessionPhase::Ready, SessionPhase::Busy] {
        assert!(matches!(
            classify_existing(phase, BrokerMappingExpectationV1::Present).await,
            ActivationError::AlreadyActive { phase: actual } if actual == phase
        ));
    }
    for phase in [SessionPhase::Suspending, SessionPhase::Deleting] {
        let error = classify_existing(phase, BrokerMappingExpectationV1::Present).await;
        assert!(matches!(
            error,
            ActivationError::LifecycleInProgress { phase: actual } if actual == phase
        ));
    }
    for phase in [SessionPhase::Suspended, SessionPhase::Blocked] {
        assert!(matches!(
            classify_existing(phase, BrokerMappingExpectationV1::Present).await,
            ActivationError::ResumeCleanupPending
        ));
    }
}

#[tokio::test]
async fn activation_serializes_the_full_absence_proof_for_one_session() {
    let session_id = session_id("discord:serialized");
    let gate = Arc::new(GateProvisioner::new(session_id));
    let (coordinator, handle) = coordinator_with(gate.clone());
    let first_request = request(session_id, BrokerMappingExpectationV1::Present);
    let first = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.prepare(&first_request, timing()).await }
    });

    let mut handle = std::pin::pin!(handle);
    let (_first_get, send) = handle.next_request().await.unwrap();
    let entered = gate.entered.notified();
    send.send_response(missing_response());
    tokio::time::timeout(StdDuration::from_secs(1), entered)
        .await
        .expect("first absence proof should enter the gate");

    let second_request = request(session_id, BrokerMappingExpectationV1::Present);
    let second = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.prepare(&second_request, timing()).await }
    });
    assert_no_request(&mut handle).await;

    gate.release.notify_one();
    let (_first_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    assert!(matches!(
        first.await.unwrap().unwrap(),
        ActivationPreparation::MappingAbsent(_)
    ));

    let (_second_first_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    assert!(matches!(
        second.await.unwrap().unwrap(),
        ActivationPreparation::MappingAbsent(_)
    ));
}

#[tokio::test]
async fn shared_scope_capacity_gate_serializes_inventory_and_durable_reservation() {
    let fake = Arc::new(FakeProvisioner::successful());
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let store =
        ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id()).unwrap();
    let locks = SessionLocks::new();
    let capacity = capacity_with_limit(1);
    let first_coordinator = Arc::new(ActivationCoordinator::new(
        store.clone(),
        locks.clone(),
        capacity.clone(),
        profile(),
        fake.clone(),
        fake.clone(),
    ));
    let second_coordinator = Arc::new(ActivationCoordinator::new(
        store,
        locks,
        capacity,
        profile(),
        fake.clone(),
        fake.clone(),
    ));
    let first_session = session_id("discord:capacity-race-a");
    let second_session = session_id("discord:capacity-race-b");
    let first_request = request(first_session, BrokerMappingExpectationV1::Absent);
    let first =
        tokio::spawn(async move { first_coordinator.prepare(&first_request, timing()).await });

    let mut handle = std::pin::pin!(handle);
    let (_first_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_first_recheck, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (first_list, first_list_response) = handle.next_request().await.unwrap();
    assert_eq!(first_list.method(), Method::GET);

    let second_request = request(second_session, BrokerMappingExpectationV1::Absent);
    let second =
        tokio::spawn(async move { second_coordinator.prepare(&second_request, timing()).await });
    let (_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    let (_second_recheck, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());

    // The second session has finished its absence proof, but cannot perform
    // its inventory LIST while the first admission owns the scope gate.
    assert_no_request(&mut handle).await;
    first_list_response.send_response(json_response(StatusCode::OK, config_map_list(&[])));

    let (first_create, first_create_response) = handle.next_request().await.unwrap();
    assert_eq!(first_create.method(), Method::POST);
    let mut created_object = request_body(first_create).await;
    let created_anchor: SessionAnchorV1 =
        serde_json::from_str(created_object["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(created_anchor.session_id(), first_session);

    // The gate covers the durable CREATE response, not just the preceding
    // inventory snapshot.
    assert_no_request(&mut handle).await;
    created_object["metadata"]["uid"] = json!("capacity-race-anchor-uid");
    created_object["metadata"]["resourceVersion"] = json!("rv-1");
    first_create_response.send_response(json_response(StatusCode::CREATED, created_object));

    let mut saw_second_inventory = false;
    let mut saw_first_observation = false;
    while !saw_second_inventory || !saw_first_observation {
        let (request, send) =
            tokio::time::timeout(StdDuration::from_secs(1), handle.next_request())
                .await
                .expect("both post-reservation operations should reach Kubernetes")
                .unwrap();
        match *request.method() {
            Method::GET => {
                assert!(!saw_second_inventory);
                assert_eq!(
                    request.uri().path(),
                    format!("/api/v1/namespaces/{NAMESPACE}/configmaps")
                );
                saw_second_inventory = true;
                send.send_response(json_response(
                    StatusCode::OK,
                    config_map_list(std::slice::from_ref(&created_anchor)),
                ));
            }
            Method::PUT => {
                assert!(!saw_first_observation);
                saw_first_observation = true;
                let mut body = request_body(request).await;
                body["metadata"]["resourceVersion"] = json!("rv-2");
                send.send_response(json_response(StatusCode::OK, body));
            }
            ref method => panic!("unexpected Kubernetes method {method}"),
        }
    }

    assert!(matches!(
        first.await.unwrap().unwrap(),
        ActivationPreparation::AwaitingRegistration { .. }
    ));
    assert!(matches!(
        second.await.unwrap(),
        Err(ActivationError::CapacityExhausted)
    ));
    assert_eq!(
        fake.ensure_calls(),
        vec![(first_session, profile_ref(PROFILE_VERSION))]
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn a_blocked_session_does_not_block_another_session() {
    let blocked_session = session_id("discord:blocked-a");
    let independent_session = session_id("discord:independent-b");
    let gate = Arc::new(GateProvisioner::new(blocked_session));
    let (coordinator, handle) = coordinator_with(gate.clone());
    let blocked_request = request(blocked_session, BrokerMappingExpectationV1::Present);
    let blocked = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.prepare(&blocked_request, timing()).await }
    });

    let mut handle = std::pin::pin!(handle);
    let (_blocked_get, send) = handle.next_request().await.unwrap();
    let entered = gate.entered.notified();
    send.send_response(missing_response());
    tokio::time::timeout(StdDuration::from_secs(1), entered)
        .await
        .expect("blocked session should enter the proof gate");

    let independent_request = request(independent_session, BrokerMappingExpectationV1::Present);
    let independent = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.prepare(&independent_request, timing()).await }
    });
    let (independent_get, send) =
        tokio::time::timeout(StdDuration::from_secs(1), handle.next_request())
            .await
            .expect("independent session should not wait")
            .unwrap();
    assert!(independent_get
        .uri()
        .path()
        .ends_with(&ResourceNames::new(independent_session).anchor()));
    send.send_response(missing_response());
    let (_independent_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    assert!(matches!(
        independent.await.unwrap().unwrap(),
        ActivationPreparation::MappingAbsent(_)
    ));

    gate.release.notify_one();
    let (_blocked_second_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());
    assert!(matches!(
        blocked.await.unwrap().unwrap(),
        ActivationPreparation::MappingAbsent(_)
    ));
}

#[test]
fn worker_observation_uses_the_exact_relay_uid_invariant() {
    for uid in ["", "   ", "pod\nuid", "pod\0uid", "pod/uid", "pod\\uid"] {
        assert_eq!(
            ObservedWorker::new(uid),
            Err(GenerationProvisionerError::InvalidPodUid)
        );
    }
    assert_eq!(
        ObservedWorker::new("p".repeat(257)),
        Err(GenerationProvisionerError::InvalidPodUid)
    );
}
