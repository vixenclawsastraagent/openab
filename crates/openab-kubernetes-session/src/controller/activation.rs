use super::{CleanupProgress, ComputeAbsentProof, LifecycleProvisioner, SessionLocks};
use crate::bridge::{SessionBinding, SessionBindingError};
use crate::identity::SessionId;
use crate::resources::MvpWorkerProfile;
use crate::state::{ProfileRef, SessionAnchorV1, SessionPhase, StateError};
use crate::store::{AnchorStoreError, ConfigMapAnchorStore, StoredAnchor};
use crate::wire::{
    ActivationRequestV1, ActivationResponseV1, BrokerMappingExpectationV1, WireProtocolError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// Deterministic lifecycle timestamps supplied by the trusted controller.
///
/// Validation stays centralized in [`SessionAnchorV1`]. These values are used
/// when activation creates an absent anchor or resumes a stopped generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivationTiming {
    last_activity_at: DateTime<Utc>,
    compute_deadline_at: DateTime<Utc>,
    storage_deadline_at: DateTime<Utc>,
}

impl ActivationTiming {
    pub fn new(
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Self {
        Self {
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        }
    }
}

/// A Pod observation returned by the narrow generation provisioner seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedWorker {
    pod_uid: String,
}

impl ObservedWorker {
    pub fn new(pod_uid: impl Into<String>) -> Result<Self, GenerationProvisionerError> {
        let pod_uid = pod_uid.into();
        if pod_uid.trim().is_empty() || pod_uid.chars().any(char::is_control) {
            return Err(GenerationProvisionerError::InvalidPodUid);
        }
        Ok(Self { pod_uid })
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvisionerOperation {
    ProveChildrenAbsent,
    EnsureGeneration,
    ReconcileComputeAbsence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationResource {
    PersistentVolumeClaim,
    NetworkPolicy,
    ServiceAccount,
    RegistrationSecret,
    Pod,
    SkillsConfigMap,
    RuntimeClass,
}

/// Closed error contract between activation arbitration and the concrete
/// Kubernetes resource driver.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GenerationProvisionerError {
    #[error("one or more V1 child resources still exist")]
    ChildrenPresent,
    #[error("V1 child-resource absence could not be proven")]
    ChildrenAmbiguous,
    #[error("Kubernetes API failed during {operation:?}")]
    KubernetesApi { operation: ProvisionerOperation },
    #[error("the trusted worker profile or lifecycle anchor could not build a generation")]
    InvalidGeneration,
    #[error("the observed {resource:?} is missing or does not exactly match this generation")]
    ResourceRejected { resource: GenerationResource },
    #[error("the immutable bootstrap Secret does not contain one 32-byte token")]
    InvalidBootstrapToken,
    #[error("the bootstrap credential was consumed or is missing after worker creation")]
    BootstrapCredentialConsumedOrMissing,
    #[error("the controller could not obtain cryptographically secure bootstrap randomness")]
    RandomnessUnavailable,
    #[error("the observed worker Pod UID is invalid")]
    InvalidPodUid,
    #[error("compute cleanup is not allowed while the session is {phase:?}")]
    InvalidCleanupPhase { phase: SessionPhase },
}

/// Minimal Kubernetes operations needed to arbitrate an activation.
///
/// `prove_v1_children_absent` is a read-only, closed-set absence proof.
/// `ensure_provisioning_generation` may create or observe only the generation
/// selected by the supplied durable anchor and must return its exact Pod UID.
#[async_trait]
pub trait GenerationProvisioner: Send + Sync {
    async fn prove_v1_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError>;

    async fn ensure_provisioning_generation(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError>;
}

/// Result of activation arbitration before worker registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationPreparation {
    AwaitingRegistration {
        binding: SessionBinding,
        profile: ProfileRef,
        pod_uid: String,
    },
    MappingAbsent(ActivationResponseV1),
}

#[derive(Debug, Error)]
pub enum ActivationError {
    #[error("activation scope does not match this controller")]
    ScopeMismatch,
    #[error("requested profile does not match trusted controller configuration")]
    RequestedProfileMismatch,
    #[error("durable anchor profile revision does not match trusted controller configuration")]
    AnchorProfileMismatch,
    #[error("the broker expected no mapping, but a durable session anchor exists")]
    UnexpectedExistingAnchor,
    #[error("a provisioning anchor belongs to a different activation attempt")]
    ProvisioningAttemptMismatch,
    #[error("session phase {phase:?} is already active; registration must establish readiness")]
    AlreadyActive { phase: SessionPhase },
    #[error("session phase {phase:?} is in progress; retry activation later")]
    LifecycleInProgress { phase: SessionPhase },
    #[error("worker compute cleanup is still in progress; retry activation later")]
    ResumeCleanupPending,
    #[error("worker compute cleanup must finish before activation can resume")]
    ResumeCleanupRequired,
    #[error("the compute-absence proof does not match the durable session anchor")]
    ResumeProofMismatch,
    #[error("anchor state is invalid")]
    State(#[from] StateError),
    #[error("anchor persistence failed")]
    Store(#[from] AnchorStoreError),
    #[error("worker generation reconciliation failed")]
    Provisioner(#[from] GenerationProvisionerError),
    #[error("worker binding is invalid")]
    Binding(#[from] SessionBindingError),
    #[error("mapping-absence response could not be constructed")]
    Wire(#[from] WireProtocolError),
}

/// Serializes and fences activation preparation for one deployment scope.
pub struct ActivationCoordinator {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    profile: MvpWorkerProfile,
    provisioner: Arc<dyn GenerationProvisioner>,
    lifecycle_provisioner: Arc<dyn LifecycleProvisioner>,
}

impl ActivationCoordinator {
    pub fn new(
        store: ConfigMapAnchorStore,
        locks: SessionLocks,
        profile: MvpWorkerProfile,
        provisioner: Arc<dyn GenerationProvisioner>,
        lifecycle_provisioner: Arc<dyn LifecycleProvisioner>,
    ) -> Self {
        Self {
            store,
            locks,
            profile,
            provisioner,
            lifecycle_provisioner,
        }
    }

    /// Prepare exactly one activation without waiting for worker registration.
    pub async fn prepare(
        &self,
        request: &ActivationRequestV1,
        timing: ActivationTiming,
    ) -> Result<ActivationPreparation, ActivationError> {
        self.validate_request(request)?;

        let session_id = request.session_id();
        let _guard = self.locks.lock(session_id).await;
        match self.store.get(session_id).await? {
            Some(anchor) => self.prepare_existing(request, anchor, timing).await,
            None => self.prepare_absent(request, timing).await,
        }
    }

    fn validate_request(&self, request: &ActivationRequestV1) -> Result<(), ActivationError> {
        if request.scope_id() != self.store.scope_id() {
            return Err(ActivationError::ScopeMismatch);
        }
        if request.requested_profile_name() != self.profile.profile().name() {
            return Err(ActivationError::RequestedProfileMismatch);
        }
        Ok(())
    }

    async fn prepare_absent(
        &self,
        request: &ActivationRequestV1,
        timing: ActivationTiming,
    ) -> Result<ActivationPreparation, ActivationError> {
        self.provisioner
            .prove_v1_children_absent(request.session_id())
            .await?;

        if let Some(anchor) = self.store.get(request.session_id()).await? {
            return self.prepare_existing(request, anchor, timing).await;
        }

        if request.broker_mapping_expectation() == BrokerMappingExpectationV1::Present {
            return Ok(ActivationPreparation::MappingAbsent(
                ActivationResponseV1::mapping_absent(request)?,
            ));
        }

        let anchor = SessionAnchorV1::new(
            request.session_id(),
            self.store.scope_id(),
            self.profile.profile().clone(),
            request.attempt_id(),
            Uuid::new_v4(),
            timing.last_activity_at,
            timing.compute_deadline_at,
            timing.storage_deadline_at,
        )?;
        let stored = self.store.create(&anchor).await?;
        self.ensure_generation(stored).await
    }

    async fn prepare_existing(
        &self,
        request: &ActivationRequestV1,
        anchor: StoredAnchor,
        timing: ActivationTiming,
    ) -> Result<ActivationPreparation, ActivationError> {
        if anchor.state().profile() != self.profile.profile() {
            return Err(ActivationError::AnchorProfileMismatch);
        }
        if anchor.state().phase() == SessionPhase::Provisioning
            && anchor.state().fence().attempt_id() == request.attempt_id()
        {
            // This is the sole exception to Absent + existing: the same bridge
            // may be retrying after the anchor was durably created but before
            // it received the activation result.
            return self.ensure_generation(anchor).await;
        }
        if request.broker_mapping_expectation() == BrokerMappingExpectationV1::Absent {
            return Err(ActivationError::UnexpectedExistingAnchor);
        }

        match anchor.state().phase() {
            SessionPhase::Provisioning => Err(ActivationError::ProvisioningAttemptMismatch),
            phase @ (SessionPhase::Ready | SessionPhase::Busy) => {
                Err(ActivationError::AlreadyActive { phase })
            }
            phase @ (SessionPhase::Suspending | SessionPhase::Deleting) => {
                Err(ActivationError::LifecycleInProgress { phase })
            }
            SessionPhase::Suspended => self.resume(request, anchor, timing).await,
            SessionPhase::Blocked if anchor.state().pod_uid().is_none() => {
                self.resume(request, anchor, timing).await
            }
            SessionPhase::Blocked => Err(ActivationError::ResumeCleanupRequired),
        }
    }

    async fn resume(
        &self,
        request: &ActivationRequestV1,
        anchor: StoredAnchor,
        timing: ActivationTiming,
    ) -> Result<ActivationPreparation, ActivationError> {
        // Validate the complete successor in memory before cleanup performs
        // any Kubernetes reads or deletes. The eventual absence proof remains
        // bound to `anchor`, and only that exact old observation may authorize
        // the CAS to this precomputed successor.
        let mut next = anchor.state().clone();
        let fence = next.fence().clone();
        next.advance_generation(
            &fence,
            request.attempt_id(),
            timing.last_activity_at,
            timing.compute_deadline_at,
            timing.storage_deadline_at,
        )?;

        let progress = self
            .lifecycle_provisioner
            .reconcile_compute_absent(&anchor)
            .await?;
        let CleanupProgress::Absent(proof) = progress else {
            return Err(ActivationError::ResumeCleanupPending);
        };
        validate_resume_proof(&anchor, &proof)?;
        let advanced = self.store.replace(&anchor, &next).await?;
        self.ensure_generation(advanced).await
    }

    async fn ensure_generation(
        &self,
        anchor: StoredAnchor,
    ) -> Result<ActivationPreparation, ActivationError> {
        let observed = self
            .provisioner
            .ensure_provisioning_generation(&anchor, &self.profile)
            .await?;
        let pod_uid = observed.pod_uid().to_string();

        let state = match anchor.state().pod_uid() {
            Some(recorded) if recorded == pod_uid => anchor.state().clone(),
            Some(recorded) => {
                return Err(ActivationError::State(StateError::PodUidMismatch {
                    expected: recorded.to_string(),
                    actual: pod_uid,
                }))
            }
            None => {
                let mut next = anchor.state().clone();
                let fence = next.fence().clone();
                next.observe_pod(&fence, &pod_uid)?;
                self.store.replace(&anchor, &next).await?.state().clone()
            }
        };

        let binding = SessionBinding::new(
            state.scope_id(),
            state.session_id(),
            state.fence().clone(),
            state.incarnation_id(),
        )?;
        Ok(ActivationPreparation::AwaitingRegistration {
            binding,
            profile: state.profile().clone(),
            pod_uid,
        })
    }
}

fn validate_resume_proof(
    anchor: &StoredAnchor,
    proof: &ComputeAbsentProof,
) -> Result<(), ActivationError> {
    if !proof.matches_anchor(anchor) {
        return Err(ActivationError::ResumeProofMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{CleanupProgress, ComputeAbsentProof, LifecycleProvisioner};
    use crate::identity::{ResourceNames, ScopeId};
    use crate::resources::{
        EgressPort, EgressProtocol, PersistentWorkspace, PvcAccessMode, RunAsIdentity,
        TrustedEgressRule, WorkerResources,
    };
    use crate::state::Fence;
    use async_trait::async_trait;
    use chrono::{Duration, TimeZone};
    use http::{Method, Request, Response, StatusCode};
    use kube::client::Body;
    use kube::Client;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration as StdDuration;
    use tower_test::mock;

    const NAMESPACE: &str = "team-a-workers";
    const RAW_SCOPE: &str = "organization-secret-team-a";
    const PROFILE_NAME: &str = "codex-strict";
    const PROFILE_VERSION: &str = "2026-08-01";
    const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const ANCHOR_UID: &str = "anchor-uid-resume";
    const POD_UID: &str = "worker-pod-uid-resume";

    type ResumeTask = tokio::task::JoinHandle<Result<ActivationPreparation, ActivationError>>;
    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    #[derive(Clone, Copy)]
    enum CleanupBehavior {
        Matching,
        Pending,
        WrongSession,
        WrongIncarnation,
        WrongFence,
        WrongAnchorUid,
    }

    #[derive(Clone)]
    struct FakeProvisioner {
        behavior: CleanupBehavior,
        lifecycle_calls: Arc<AtomicUsize>,
        ensure_calls: Arc<Mutex<Vec<Fence>>>,
    }

    impl FakeProvisioner {
        fn new(behavior: CleanupBehavior) -> Self {
            Self {
                behavior,
                lifecycle_calls: Arc::new(AtomicUsize::new(0)),
                ensure_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn lifecycle_calls(&self) -> usize {
            self.lifecycle_calls.load(Ordering::SeqCst)
        }

        fn ensure_calls(&self) -> Vec<Fence> {
            self.ensure_calls.lock().unwrap().clone()
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
            anchor: &StoredAnchor,
            _profile: &MvpWorkerProfile,
        ) -> Result<ObservedWorker, GenerationProvisionerError> {
            self.ensure_calls
                .lock()
                .unwrap()
                .push(anchor.state().fence().clone());
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
            let state = anchor.state();
            let mut session_id = state.session_id();
            let mut incarnation_id = state.incarnation_id();
            let mut fence = state.fence().clone();
            let mut anchor_uid = anchor.uid().to_owned();
            match self.behavior {
                CleanupBehavior::Matching => {}
                CleanupBehavior::Pending => return Ok(CleanupProgress::Pending),
                CleanupBehavior::WrongSession => {
                    session_id = SessionId::derive(RAW_SCOPE, "discord:another-thread")
                }
                CleanupBehavior::WrongIncarnation => incarnation_id = Uuid::from_u128(0xdead),
                CleanupBehavior::WrongFence => {
                    fence = Fence::new(fence.generation() + 1, Uuid::from_u128(0xbeef)).unwrap()
                }
                CleanupBehavior::WrongAnchorUid => anchor_uid = "replacement-anchor-uid".into(),
            }
            Ok(CleanupProgress::Absent(ComputeAbsentProof::for_test(
                session_id,
                incarnation_id,
                fence,
                anchor_uid,
            )))
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

    fn initial_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap()
    }

    fn resume_timing() -> ActivationTiming {
        let now = initial_time() + Duration::hours(1);
        ActivationTiming::new(now, now + Duration::minutes(15), now + Duration::hours(72))
    }

    fn anchor_in_phase(
        session_id: SessionId,
        phase: SessionPhase,
        pod_uid: Option<&str>,
    ) -> SessionAnchorV1 {
        let now = initial_time();
        let anchor = SessionAnchorV1::new(
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
        let mut value = serde_json::to_value(anchor).unwrap();
        value["phase"] = serde_json::to_value(phase).unwrap();
        value["podUid"] = pod_uid.map_or(Value::Null, |uid| json!(uid));
        serde_json::from_value(value).unwrap()
    }

    fn activation_request(session_id: SessionId, attempt_id: Uuid) -> ActivationRequestV1 {
        ActivationRequestV1::new(
            scope_id(),
            session_id,
            attempt_id,
            PROFILE_NAME,
            BrokerMappingExpectationV1::Present,
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
            "data": { "anchor.json": serde_json::to_string(anchor).unwrap() }
        })
    }

    fn json_response(status: StatusCode, body: Value) -> Response<Body> {
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
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

    fn start_resume(
        existing: SessionAnchorV1,
        attempt_id: Uuid,
        behavior: CleanupBehavior,
    ) -> (ResumeTask, MockHandle, FakeProvisioner) {
        let fake = FakeProvisioner::new(behavior);
        let shared = Arc::new(fake.clone());
        let generation: Arc<dyn GenerationProvisioner> = shared.clone();
        let lifecycle: Arc<dyn LifecycleProvisioner> = shared;
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let store =
            ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id())
                .unwrap();
        let coordinator = ActivationCoordinator::new(
            store,
            SessionLocks::new(),
            profile(),
            generation,
            lifecycle,
        );
        let activation = activation_request(existing.session_id(), attempt_id);
        let task =
            tokio::spawn(async move { coordinator.prepare(&activation, resume_timing()).await });
        (task, handle, fake)
    }

    async fn assert_no_request(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    ) {
        let unexpected =
            tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
        assert!(!matches!(unexpected, Ok(Some(_))));
    }

    async fn assert_happy_resume(phase: SessionPhase, label: &str) {
        let existing = anchor_in_phase(session_id(label), phase, None);
        let original_incarnation = existing.incarnation_id();
        let new_attempt = Uuid::from_u128(0x300);
        let (task, handle, fake) =
            start_resume(existing.clone(), new_attempt, CleanupBehavior::Matching);
        let mut handle = std::pin::pin!(handle);

        let (get, send) = handle.next_request().await.unwrap();
        assert_eq!(get.method(), Method::GET);
        send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));

        let (advance, send) = handle.next_request().await.unwrap();
        assert_eq!(advance.method(), Method::PUT);
        let mut advance_body = request_body(advance).await;
        let advanced: SessionAnchorV1 =
            serde_json::from_str(advance_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        assert_eq!(advanced.phase(), SessionPhase::Provisioning);
        assert_eq!(advanced.fence().generation(), 2);
        assert_eq!(advanced.fence().attempt_id(), new_attempt);
        assert_eq!(advanced.incarnation_id(), original_incarnation);
        assert_eq!(advanced.pod_uid(), None);
        assert_eq!(
            advanced.last_activity_at(),
            initial_time() + Duration::hours(1)
        );
        advance_body["metadata"]["resourceVersion"] = json!("rv-2");
        send.send_response(json_response(StatusCode::OK, advance_body));

        let (observe, send) = handle.next_request().await.unwrap();
        assert_eq!(observe.method(), Method::PUT);
        let mut observe_body = request_body(observe).await;
        let observed: SessionAnchorV1 =
            serde_json::from_str(observe_body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        assert_eq!(observed.fence(), advanced.fence());
        assert_eq!(observed.pod_uid(), Some(POD_UID));
        observe_body["metadata"]["resourceVersion"] = json!("rv-3");
        send.send_response(json_response(StatusCode::OK, observe_body));

        let result = task.await.unwrap().unwrap();
        assert!(matches!(
            result,
            ActivationPreparation::AwaitingRegistration { binding, pod_uid, .. }
                if binding.fence().generation() == 2
                    && binding.fence().attempt_id() == new_attempt
                    && pod_uid == POD_UID
        ));
        assert_eq!(fake.lifecycle_calls(), 1);
        assert_eq!(fake.ensure_calls(), vec![advanced.fence().clone()]);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn suspended_anchor_resumes_with_exactly_one_fresh_generation() {
        assert_happy_resume(SessionPhase::Suspended, "discord:suspended-resume").await;
    }

    #[tokio::test]
    async fn clean_blocked_anchor_resumes_with_exactly_one_fresh_generation() {
        assert_happy_resume(SessionPhase::Blocked, "discord:blocked-resume").await;
    }

    #[tokio::test]
    async fn pending_cleanup_never_advances_or_ensures_a_generation() {
        let existing = anchor_in_phase(
            session_id("discord:cleanup-pending"),
            SessionPhase::Suspended,
            None,
        );
        let (task, handle, fake) = start_resume(
            existing.clone(),
            Uuid::from_u128(0x300),
            CleanupBehavior::Pending,
        );
        let mut handle = std::pin::pin!(handle);
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));

        assert!(matches!(
            task.await.unwrap(),
            Err(ActivationError::ResumeCleanupPending)
        ));
        assert_eq!(fake.lifecycle_calls(), 1);
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn every_mismatched_absence_proof_fails_before_cas_or_generation_ensure() {
        for (index, behavior) in [
            CleanupBehavior::WrongSession,
            CleanupBehavior::WrongIncarnation,
            CleanupBehavior::WrongFence,
            CleanupBehavior::WrongAnchorUid,
        ]
        .into_iter()
        .enumerate()
        {
            let existing = anchor_in_phase(
                session_id(&format!("discord:proof-mismatch-{index}")),
                SessionPhase::Suspended,
                None,
            );
            let (task, handle, fake) =
                start_resume(existing.clone(), Uuid::from_u128(0x300), behavior);
            let mut handle = std::pin::pin!(handle);
            let (_get, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));

            assert!(matches!(
                task.await.unwrap(),
                Err(ActivationError::ResumeProofMismatch)
            ));
            assert_eq!(fake.lifecycle_calls(), 1);
            assert!(fake.ensure_calls().is_empty());
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn blocked_anchor_with_recorded_pod_requires_cleanup_before_resume() {
        let existing = anchor_in_phase(
            session_id("discord:blocked-with-pod"),
            SessionPhase::Blocked,
            Some("recorded-pod-uid"),
        );
        let (task, handle, fake) = start_resume(
            existing.clone(),
            Uuid::from_u128(0x300),
            CleanupBehavior::Matching,
        );
        let mut handle = std::pin::pin!(handle);
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));

        assert!(matches!(
            task.await.unwrap(),
            Err(ActivationError::ResumeCleanupRequired)
        ));
        assert_eq!(fake.lifecycle_calls(), 0);
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn resume_rejects_the_previous_attempt_before_cas_or_generation_ensure() {
        let existing = anchor_in_phase(
            session_id("discord:reused-attempt"),
            SessionPhase::Suspended,
            None,
        );
        let previous_attempt = existing.fence().attempt_id();
        let (task, handle, fake) = start_resume(
            existing.clone(),
            previous_attempt,
            CleanupBehavior::Matching,
        );
        let mut handle = std::pin::pin!(handle);
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));

        assert!(matches!(
            task.await.unwrap(),
            Err(ActivationError::State(StateError::ReusedAttemptIdentifier))
        ));
        assert_eq!(fake.lifecycle_calls(), 0);
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn resume_cas_conflict_never_ensures_a_generation() {
        let existing = anchor_in_phase(
            session_id("discord:resume-cas-conflict"),
            SessionPhase::Suspended,
            None,
        );
        let (task, handle, fake) = start_resume(
            existing.clone(),
            Uuid::from_u128(0x300),
            CleanupBehavior::Matching,
        );
        let mut handle = std::pin::pin!(handle);
        let (_get, send) = handle.next_request().await.unwrap();
        send.send_response(json_response(StatusCode::OK, config_map(&existing, "rv-1")));
        let (replace, send) = handle.next_request().await.unwrap();
        assert_eq!(replace.method(), Method::PUT);
        send.send_response(conflict_response());

        assert!(matches!(
            task.await.unwrap(),
            Err(ActivationError::Store(AnchorStoreError::Conflict {
                operation: crate::store::StoreOperation::Replace,
                ..
            }))
        ));
        assert_eq!(fake.lifecycle_calls(), 1);
        assert!(fake.ensure_calls().is_empty());
        assert_no_request(&mut handle).await;
    }
}
