#![cfg(feature = "controller")]

use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    ActivityEvent, ActivityOutcome, ActivityTurnId, BootstrapPresence, CleanupProgress,
    ConsumedBootstrap, ControllerCoordinatorConfigError, ControllerCoordinators,
    DurableIntentError, DurableIntentOutcome, DurableIntentReport, GenerationProvisioner,
    GenerationProvisionerError, LifecycleDeadlineOutcome, LifecycleDeadlineReport, LifecycleError,
    LifecycleProvisioner, LifecycleReconcileOutcome, ObservedWorker, RegistrationProvisioner,
    RegistrationProvisionerError, ReleaseCleanupProgress, ReleaseOutcome, ReleaseProvisioner,
    ReleasedChildrenAbsentProof, StartupOrphanOutcome, StartupOrphanReport, VerifiedBootstrap,
    WorkerBootstrapAuth,
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
use openab_kubernetes_session::wire::LifecycleRequestV1;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const POD_UID: &str = "worker-pod-uid";

#[derive(Default)]
struct FakeProvisioner {
    generation_calls: AtomicUsize,
    lifecycle_calls: AtomicUsize,
    registration_calls: AtomicUsize,
    release_calls: AtomicUsize,
    failing_lifecycle_session: Option<SessionId>,
}

impl FakeProvisioner {
    fn failing_lifecycle_for(session_id: SessionId) -> Self {
        Self {
            failing_lifecycle_session: Some(session_id),
            ..Self::default()
        }
    }

    fn calls(&self) -> (usize, usize, usize, usize) {
        (
            self.generation_calls.load(Ordering::SeqCst),
            self.lifecycle_calls.load(Ordering::SeqCst),
            self.registration_calls.load(Ordering::SeqCst),
            self.release_calls.load(Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl GenerationProvisioner for FakeProvisioner {
    async fn prove_v1_children_absent(
        &self,
        _session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        self.generation_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn ensure_provisioning_generation(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        self.generation_calls.fetch_add(1, Ordering::SeqCst);
        ObservedWorker::new(POD_UID)
    }
}

#[async_trait]
impl LifecycleProvisioner for FakeProvisioner {
    async fn reconcile_compute_absent(
        &self,
        anchor: &StoredAnchor,
    ) -> Result<CleanupProgress, GenerationProvisionerError> {
        self.lifecycle_calls.fetch_add(1, Ordering::SeqCst);
        if self.failing_lifecycle_session == Some(anchor.state().session_id()) {
            return Err(GenerationProvisionerError::ChildrenAmbiguous);
        }
        Ok(CleanupProgress::Pending)
    }
}

#[async_trait]
impl RegistrationProvisioner for FakeProvisioner {
    async fn verify_bootstrap(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
        _expected_binding: &SessionBinding,
        _auth: &WorkerBootstrapAuth,
    ) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
        self.registration_calls.fetch_add(1, Ordering::SeqCst);
        Err(RegistrationProvisionerError::Unauthorized)
    }

    async fn consume_bootstrap(
        &self,
        _verified: VerifiedBootstrap,
    ) -> Result<ConsumedBootstrap, RegistrationProvisionerError> {
        self.registration_calls.fetch_add(1, Ordering::SeqCst);
        Err(RegistrationProvisionerError::Unauthorized)
    }

    async fn observe_bootstrap_presence(
        &self,
        _anchor: &StoredAnchor,
        _profile: &MvpWorkerProfile,
    ) -> Result<BootstrapPresence, RegistrationProvisionerError> {
        self.registration_calls.fetch_add(1, Ordering::SeqCst);
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
        self.release_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReleaseCleanupProgress::Pending)
    }

    async fn prove_released_children_absent(
        &self,
        _binding: &SessionBinding,
    ) -> Result<ReleasedChildrenAbsentProof, GenerationProvisionerError> {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
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

fn base_anchor(session_id: SessionId, version: &str) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        session_id,
        scope_id(),
        profile_ref(version),
        Uuid::from_u128(0x100),
        Uuid::from_u128(0x200),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap()
}

fn anchor_in_phase(session_id: SessionId, phase: SessionPhase, version: &str) -> SessionAnchorV1 {
    let mut anchor = base_anchor(session_id, version);
    let fence = anchor.fence().clone();
    match phase {
        SessionPhase::Provisioning => {}
        SessionPhase::Ready => {
            anchor.observe_pod(&fence, POD_UID).unwrap();
            anchor.transition(&fence, SessionPhase::Ready).unwrap();
        }
        SessionPhase::Busy => {
            anchor.observe_pod(&fence, POD_UID).unwrap();
            anchor.transition(&fence, SessionPhase::Ready).unwrap();
            anchor.transition(&fence, SessionPhase::Busy).unwrap();
        }
        SessionPhase::Suspending => {
            anchor.observe_pod(&fence, POD_UID).unwrap();
            anchor.transition(&fence, SessionPhase::Ready).unwrap();
            anchor.transition(&fence, SessionPhase::Suspending).unwrap();
        }
        SessionPhase::Suspended => {
            anchor.observe_pod(&fence, POD_UID).unwrap();
            anchor.transition(&fence, SessionPhase::Ready).unwrap();
            anchor.transition(&fence, SessionPhase::Suspending).unwrap();
            anchor.confirm_pod_deleted(&fence, POD_UID).unwrap();
            anchor.transition(&fence, SessionPhase::Suspended).unwrap();
        }
        SessionPhase::Blocked | SessionPhase::Deleting => {
            let mut value = serde_json::to_value(anchor).unwrap();
            value["phase"] = serde_json::to_value(phase).unwrap();
            anchor = serde_json::from_value(value).unwrap();
        }
    }
    anchor
}

fn anchor_with_deadlines(
    session_id: SessionId,
    phase: SessionPhase,
    last_activity_at: chrono::DateTime<Utc>,
    compute_deadline_at: chrono::DateTime<Utc>,
    storage_deadline_at: chrono::DateTime<Utc>,
) -> SessionAnchorV1 {
    let anchor = anchor_in_phase(session_id, phase, "2026-08-01");
    let mut value = serde_json::to_value(anchor).unwrap();
    value["lastActivityAt"] = json!(last_activity_at);
    value["computeDeadlineAt"] = json!(compute_deadline_at);
    value["storageDeadlineAt"] = json!(storage_deadline_at);
    serde_json::from_value(value).unwrap()
}

fn provisioning_with_pod(session_id: SessionId, version: &str) -> SessionAnchorV1 {
    let mut anchor = base_anchor(session_id, version);
    let fence = anchor.fence().clone();
    anchor.observe_pod(&fence, POD_UID).unwrap();
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
) -> Result<ControllerCoordinators, ControllerCoordinatorConfigError> {
    let generation: Arc<dyn GenerationProvisioner> = fake.clone();
    let lifecycle: Arc<dyn LifecycleProvisioner> = fake.clone();
    let registration: Arc<dyn RegistrationProvisioner> = fake.clone();
    let release: Arc<dyn ReleaseProvisioner> = fake;
    ControllerCoordinators::new(
        store,
        profiles,
        ControllerPolicy::new(15 * 60, 72 * 60 * 60, 20).unwrap(),
        generation,
        lifecycle,
        registration,
        release,
    )
}

fn suspend_request(anchor: &SessionAnchorV1) -> LifecycleRequestV1 {
    serde_json::from_value(json!({
        "version": 1,
        "requestId": Uuid::from_u128(0x300),
        "kind": "suspend",
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

fn assert_public_report_type(_report: &DurableIntentReport) {}

fn assert_public_deadline_report_type(_report: &LifecycleDeadlineReport) {}

fn assert_public_startup_orphan_report_type(_report: &StartupOrphanReport) {}

#[tokio::test]
async fn composition_root_indexes_exact_profile_revisions_and_rejects_duplicates() {
    let (store, _handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01"), profile("2026-08-02")], fake).unwrap();

    assert!(root.activation(&profile_ref("2026-08-01")).is_some());
    assert!(root.activation(&profile_ref("2026-08-02")).is_some());
    assert!(root.activation(&profile_ref("2026-08-03")).is_none());
    assert!(root.registration(&profile_ref("2026-08-01")).is_some());
    let _ = root.lifecycle();
    let _ = root.activity();
    let _ = root.release();

    let (store, _handle) = store_and_handle();
    let duplicate = coordinators(
        store,
        [profile("2026-08-01"), profile("2026-08-01")],
        Arc::new(FakeProvisioner::default()),
    );
    assert!(matches!(
        duplicate,
        Err(ControllerCoordinatorConfigError::DuplicateProfile { profile })
            if profile == profile_ref("2026-08-01")
    ));
}

#[tokio::test]
async fn stable_live_phases_are_noops_without_fresh_gets_or_provisioner_calls() {
    let anchors = vec![
        anchor_in_phase(
            session_id("discord:ready"),
            SessionPhase::Ready,
            "2026-08-01",
        ),
        anchor_in_phase(session_id("discord:busy"), SessionPhase::Busy, "2026-08-01"),
        anchor_in_phase(
            session_id("discord:suspended"),
            SessionPhase::Suspended,
            "2026-08-01",
        ),
    ];
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&anchors)));

    let report = task.await.unwrap().unwrap();
    assert_public_report_type(&report);
    assert_eq!(report.results().len(), 3);
    assert!(report
        .results()
        .iter()
        .all(|result| matches!(result.outcome(), Some(DurableIntentOutcome::Noop))));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn startup_contains_every_unrecoverable_live_generation_without_touching_retained_state() {
    let ready = anchor_in_phase(
        session_id("discord:startup-ready-orphan"),
        SessionPhase::Ready,
        "2026-08-01",
    );
    let mut busy = anchor_in_phase(
        session_id("discord:startup-busy-orphan"),
        SessionPhase::Busy,
        "2026-08-01",
    );
    let mut busy_value = serde_json::to_value(&busy).unwrap();
    busy_value["lastPromptTurnId"] = json!(Uuid::from_u128(0x500));
    busy = serde_json::from_value(busy_value).unwrap();
    let suspended = anchor_in_phase(
        session_id("discord:startup-suspended"),
        SessionPhase::Suspended,
        "2026-08-01",
    );
    let provisioning = base_anchor(
        session_id("discord:startup-provisioning-orphan"),
        "2026-08-01",
    );
    let anchors = vec![ready, busy, provisioning, suspended];
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.quiesce_startup_orphans().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&anchors)));

    for _ in 0..3 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        let requested_name = get.uri().path().rsplit('/').next().unwrap();
        let anchor = anchors
            .iter()
            .find(|anchor| ResourceNames::new(anchor.session_id()).anchor() == requested_name)
            .unwrap();
        assert!(matches!(
            anchor.phase(),
            SessionPhase::Provisioning | SessionPhase::Ready | SessionPhase::Busy
        ));
        send.send_response(json_response(
            StatusCode::OK,
            config_map(anchor, "rv-fresh"),
        ));

        let (replace, send) = handle.next_request().await.unwrap();
        assert_eq!(replace.method(), Method::PUT);
        let mut body = request_body(replace).await;
        let next: SessionAnchorV1 =
            serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        assert_eq!(next.phase(), SessionPhase::Blocked);
        assert_eq!(next.pod_uid(), anchor.pod_uid());
        assert_eq!(next.last_prompt_turn_id(), None);
        body["metadata"]["resourceVersion"] = json!("rv-suspending");
        send.send_response(json_response(StatusCode::OK, body));
    }

    let report = task.await.unwrap().unwrap();
    assert_public_startup_orphan_report_type(&report);
    assert_eq!(report.results().len(), 4);
    assert!(report.containment_complete());
    for result in report.results() {
        match result.scheduled_phase() {
            SessionPhase::Provisioning | SessionPhase::Ready | SessionPhase::Busy => {
                assert!(matches!(
                    result.outcome(),
                    Some(StartupOrphanOutcome::ContainmentAccepted)
                ))
            }
            SessionPhase::Suspended => {
                assert!(matches!(result.outcome(), Some(StartupOrphanOutcome::Noop)))
            }
            phase => panic!("unexpected scheduled phase: {phase:?}"),
        }
    }
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn startup_orphan_inventory_is_only_a_hint_and_fresh_containment_is_required() {
    let scheduled = anchor_in_phase(
        session_id("discord:startup-phase-drift"),
        SessionPhase::Ready,
        "2026-08-01",
    );
    let fresh = anchor_in_phase(
        scheduled.session_id(),
        SessionPhase::Suspended,
        "2026-08-01",
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.quiesce_startup_orphans().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&scheduled)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&fresh, "rv-fresh"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(report.containment_complete());
    assert!(matches!(
        report.results()[0].outcome(),
        Some(StartupOrphanOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn startup_orphan_cas_failure_keeps_containment_incomplete() {
    let active = anchor_in_phase(
        session_id("discord:startup-cas-failure"),
        SessionPhase::Busy,
        "2026-08-01",
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.quiesce_startup_orphans().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&active)),
    ));
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&active, "rv-fresh"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    send.send_response(json_response(
        StatusCode::CONFLICT,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "Conflict",
            "code": 409
        }),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(!report.containment_complete());
    assert!(matches!(
        report.results()[0].error(),
        Some(LifecycleError::Store(_))
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn startup_orphan_inventory_failure_never_claims_containment() {
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.quiesce_startup_orphans().await });
    let mut handle = std::pin::pin!(handle);

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
        Err(AnchorStoreError::Kubernetes {
            operation: StoreOperation::List,
            ..
        })
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn one_session_failure_does_not_stop_the_next_durable_intent() {
    let failing = session_id("discord:lifecycle-failure");
    let succeeding = session_id("discord:lifecycle-pending");
    let anchors = vec![
        anchor_in_phase(failing, SessionPhase::Suspending, "2026-08-01"),
        anchor_in_phase(succeeding, SessionPhase::Suspending, "2026-08-01"),
    ];
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::failing_lifecycle_for(failing));
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(StatusCode::OK, config_map_list(&anchors)));
    for _ in 0..2 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        let requested_name = get.uri().path().rsplit('/').next().unwrap();
        let anchor = anchors
            .iter()
            .find(|anchor| ResourceNames::new(anchor.session_id()).anchor() == requested_name)
            .unwrap();
        send.send_response(json_response(
            StatusCode::OK,
            config_map(anchor, "rv-fresh"),
        ));
    }

    let report = task.await.unwrap().unwrap();
    assert_eq!(report.results().len(), 2);
    let failed = report
        .results()
        .iter()
        .find(|result| result.session_id() == failing)
        .unwrap();
    assert!(matches!(
        failed.error(),
        Some(DurableIntentError::Lifecycle(_))
    ));
    let pending = report
        .results()
        .iter()
        .find(|result| result.session_id() == succeeding)
        .unwrap();
    assert!(matches!(
        pending.outcome(),
        Some(DurableIntentOutcome::Compute(
            LifecycleReconcileOutcome::Pending
        ))
    ));
    assert_eq!(fake.calls(), (0, 2, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn missing_profile_is_checked_against_fresh_provisioning_state() {
    let scheduled = base_anchor(session_id("discord:retired-profile"), "2026-07-31");
    let fresh_ready = anchor_in_phase(scheduled.session_id(), SessionPhase::Ready, "2026-07-31");
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&scheduled)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&fresh_ready, "rv-fresh"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(DurableIntentOutcome::Noop)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn unavailable_exact_profile_is_a_per_session_error_without_mutation() {
    let anchor = base_anchor(session_id("discord:retired-profile"), "2026-07-31");
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (_get, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-fresh"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].error(),
        Some(DurableIntentError::ProfileUnavailable { profile })
            if profile == &profile_ref("2026-07-31")
    ));
    assert!(!report.results()[0]
        .error()
        .unwrap()
        .to_string()
        .contains("2026-07-31"));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn provisioning_anchor_disappearing_after_preflight_is_a_stale_noop() {
    let anchor = provisioning_with_pod(
        session_id("discord:startup-registration-race"),
        "2026-08-01",
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (_preflight, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-fresh"),
    ));
    let (_registration_get, send) = handle.next_request().await.unwrap();
    send.send_response(missing_response());

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(DurableIntentOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn provisioning_without_a_pod_ensures_once_then_waits_for_registration() {
    let anchor = base_anchor(session_id("discord:startup-ensure"), "2026-08-01");
    let mut observed = anchor.clone();
    let fence = observed.fence().clone();
    observed.observe_pod(&fence, POD_UID).unwrap();
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    for _ in 0..2 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "rv-fresh"),
        ));
    }
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&observed, "rv-pod"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(DurableIntentOutcome::AwaitingRegistration)
    ));
    assert_eq!(fake.calls(), (1, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn provisioning_with_a_pod_recovers_registration_without_ensuring_again() {
    let anchor = provisioning_with_pod(session_id("discord:startup-registration"), "2026-08-01");
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    for _ in 0..2 {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&anchor, "rv-fresh"),
        ));
    }

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(DurableIntentOutcome::AwaitingRegistration)
    ));
    assert_eq!(fake.calls(), (0, 0, 1, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn deleting_intent_continues_without_profile_resolution_or_new_authority() {
    let anchor = anchor_in_phase(
        session_id("discord:startup-deleting"),
        SessionPhase::Deleting,
        "retired-profile-version",
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-fresh"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(DurableIntentOutcome::Release(ReleaseOutcome::Pending))
    ));
    assert_eq!(fake.calls(), (0, 1, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn inventory_failure_aborts_before_any_session_operation() {
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.reconcile_durable_intents().await });
    let mut handle = std::pin::pin!(handle);

    let (_list, send) = handle.next_request().await.unwrap();
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
        Err(AnchorStoreError::Kubernetes {
            operation: StoreOperation::List,
            ..
        })
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn composition_root_serializes_live_activity_and_suspend_for_one_session() {
    let anchor = anchor_in_phase(
        session_id("discord:shared-root-lock"),
        SessionPhase::Ready,
        "2026-08-01",
    );
    let expected_binding = SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap();
    let suspend = suspend_request(&anchor);
    let turn_id = ActivityTurnId::from_uuid(Uuid::from_u128(0x400)).unwrap();
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = Arc::new(coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap());

    let activity_root = Arc::clone(&root);
    let activity = tokio::spawn(async move {
        activity_root
            .activity()
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-ready"),
    ));
    let (replace, activity_send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut activity_body = request_body(replace).await;
    let busy: SessionAnchorV1 =
        serde_json::from_str(activity_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(busy.phase(), SessionPhase::Busy);

    let lifecycle_root = Arc::clone(&root);
    let lifecycle =
        tokio::spawn(async move { lifecycle_root.lifecycle().accept_suspend(&suspend).await });
    assert_no_request(&mut handle).await;

    activity_body["metadata"]["resourceVersion"] = json!("rv-activity");
    activity_send.send_response(json_response(StatusCode::OK, activity_body));
    assert_eq!(activity.await.unwrap().unwrap(), ActivityOutcome::Recorded);

    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&busy, "rv-activity"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut lifecycle_body = request_body(replace).await;
    let suspending: SessionAnchorV1 =
        serde_json::from_str(lifecycle_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(suspending.phase(), SessionPhase::Suspending);
    assert_eq!(suspending.last_prompt_turn_id(), None);
    lifecycle_body["metadata"]["resourceVersion"] = json!("rv-suspending");
    send.send_response(json_response(StatusCode::OK, lifecycle_body));

    lifecycle.await.unwrap().unwrap();
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn expired_ready_deadline_durably_accepts_compute_suspension_only() {
    let now = Utc::now();
    let anchor = anchor_with_deadlines(
        session_id("discord:expired-ready"),
        SessionPhase::Ready,
        now - Duration::hours(2),
        now - Duration::hours(1),
        now + Duration::hours(70),
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-fresh"),
    ));
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut body = request_body(replace).await;
    let written: SessionAnchorV1 =
        serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(written.phase(), SessionPhase::Suspending);
    assert_eq!(written.last_prompt_turn_id(), None);
    assert_eq!(written.last_activity_at(), anchor.last_activity_at());
    assert_eq!(written.compute_deadline_at(), anchor.compute_deadline_at());
    assert_eq!(written.storage_deadline_at(), anchor.storage_deadline_at());
    body["metadata"]["resourceVersion"] = json!("rv-suspending");
    send.send_response(json_response(StatusCode::OK, body));

    let report = task.await.unwrap().unwrap();
    assert_public_deadline_report_type(&report);
    assert_eq!(report.results().len(), 1);
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::ComputeSuspensionAccepted)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn deadline_scan_never_suspends_busy_or_not_due_compute() {
    let now = Utc::now();
    let anchors = vec![
        anchor_with_deadlines(
            session_id("discord:expired-busy"),
            SessionPhase::Busy,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:not-due"),
            SessionPhase::Ready,
            now,
            now + Duration::hours(1),
            now + Duration::hours(72),
        ),
        anchor_with_deadlines(
            session_id("discord:provisioning"),
            SessionPhase::Provisioning,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:suspending"),
            SessionPhase::Suspending,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:blocked"),
            SessionPhase::Blocked,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:deleting"),
            SessionPhase::Deleting,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:suspended-retained"),
            SessionPhase::Suspended,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
    ];
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&anchors)));

    let report = task.await.unwrap().unwrap();
    assert_eq!(report.results().len(), 7);
    assert!(report
        .results()
        .iter()
        .all(|result| matches!(result.outcome(), Some(LifecycleDeadlineOutcome::Noop))));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn expired_suspended_storage_is_advisory_without_any_mutation() {
    let now = Utc::now();
    let anchor = anchor_with_deadlines(
        session_id("discord:release-eligible"),
        SessionPhase::Suspended,
        now - Duration::hours(4),
        now - Duration::hours(3),
        now - Duration::hours(2),
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::StorageDeadlineExpiredObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn fresh_activity_or_phase_drift_invalidates_an_expired_compute_hint() {
    let now = Utc::now();
    let scheduled = anchor_with_deadlines(
        session_id("discord:deadline-drift"),
        SessionPhase::Ready,
        now - Duration::hours(2),
        now - Duration::hours(1),
        now + Duration::hours(70),
    );
    let fresh = anchor_with_deadlines(
        scheduled.session_id(),
        SessionPhase::Busy,
        now,
        now + Duration::hours(1),
        now + Duration::hours(72),
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&scheduled)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&fresh, "rv-fresh"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;

    let refreshed = anchor_with_deadlines(
        scheduled.session_id(),
        SessionPhase::Ready,
        now,
        now + Duration::hours(1),
        now + Duration::hours(72),
    );
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&scheduled)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&refreshed, "rv-refreshed"),
    ));

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;

    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&scheduled)),
    ));
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(missing_response());

    let report = task.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn deadline_cas_failure_is_isolated_and_does_not_acknowledge_suspend() {
    let now = Utc::now();
    let mut anchors = vec![
        anchor_with_deadlines(
            session_id("discord:deadline-a"),
            SessionPhase::Ready,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
        anchor_with_deadlines(
            session_id("discord:deadline-b"),
            SessionPhase::Ready,
            now - Duration::hours(2),
            now - Duration::hours(1),
            now + Duration::hours(70),
        ),
    ];
    anchors.sort_unstable_by_key(|anchor| ResourceNames::new(anchor.session_id()).anchor());
    let failed_session = anchors[0].session_id();
    let accepted_session = anchors[1].session_id();
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(StatusCode::OK, config_map_list(&anchors)));
    for (index, anchor) in anchors.iter().enumerate() {
        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(
            StatusCode::OK,
            config_map(anchor, "rv-fresh"),
        ));
        let (replace, send) = handle.next_request().await.unwrap();
        assert_eq!(replace.method(), Method::PUT);
        if index == 0 {
            send.send_response(json_response(
                StatusCode::CONFLICT,
                json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": "Conflict",
                    "code": 409
                }),
            ));
        } else {
            let mut body = request_body(replace).await;
            body["metadata"]["resourceVersion"] = json!("rv-suspending");
            send.send_response(json_response(StatusCode::OK, body));
        }
    }

    let report = task.await.unwrap().unwrap();
    let failed = report
        .results()
        .iter()
        .find(|result| result.session_id() == failed_session)
        .unwrap();
    assert!(matches!(
        failed.error(),
        Some(LifecycleError::Store(AnchorStoreError::Conflict {
            operation: StoreOperation::Replace,
            ..
        }))
    ));
    let accepted = report
        .results()
        .iter()
        .find(|result| result.session_id() == accepted_session)
        .unwrap();
    assert!(matches!(
        accepted.outcome(),
        Some(LifecycleDeadlineOutcome::ComputeSuspensionAccepted)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn deadline_inventory_failure_aborts_before_any_session_operation() {
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap();
    let task = tokio::spawn(async move { root.scan_lifecycle_deadlines().await });
    let mut handle = std::pin::pin!(handle);

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
        Err(AnchorStoreError::Kubernetes {
            operation: StoreOperation::List,
            ..
        })
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn deadline_scan_rechecks_after_live_activity_releases_the_shared_lock() {
    let now = Utc::now();
    let anchor = anchor_with_deadlines(
        session_id("discord:deadline-activity-race"),
        SessionPhase::Ready,
        now - Duration::hours(2),
        now - Duration::hours(1),
        now + Duration::hours(70),
    );
    let binding = SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap();
    let turn_id = ActivityTurnId::from_uuid(Uuid::from_u128(0x500)).unwrap();
    let (store, handle) = store_and_handle();
    let fake = Arc::new(FakeProvisioner::default());
    let root = Arc::new(coordinators(store, [profile("2026-08-01")], fake.clone()).unwrap());

    let activity_root = Arc::clone(&root);
    let activity = tokio::spawn(async move {
        activity_root
            .activity()
            .record(&binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, "rv-ready"),
    ));
    let (replace, activity_send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    let mut activity_body = request_body(replace).await;
    let busy: SessionAnchorV1 =
        serde_json::from_str(activity_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(busy.phase(), SessionPhase::Busy);

    let scan_root = Arc::clone(&root);
    let scan = tokio::spawn(async move { scan_root.scan_lifecycle_deadlines().await });
    let (list, send) = handle.next_request().await.unwrap();
    assert_eq!(list.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map_list(std::slice::from_ref(&anchor)),
    ));
    assert_no_request(&mut handle).await;

    activity_body["metadata"]["resourceVersion"] = json!("rv-activity");
    activity_send.send_response(json_response(StatusCode::OK, activity_body));
    assert_eq!(activity.await.unwrap().unwrap(), ActivityOutcome::Recorded);

    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&busy, "rv-activity"),
    ));

    let report = scan.await.unwrap().unwrap();
    assert!(matches!(
        report.results()[0].outcome(),
        Some(LifecycleDeadlineOutcome::StaleObservation)
    ));
    assert_eq!(fake.calls(), (0, 0, 0, 0));
    assert_no_request(&mut handle).await;
}
