use super::{
    AllChildrenAbsentProof, CleanupProgress, ComputeAbsentProof, ReleaseCleanupProgress,
    ReleaseProvisioner, SessionLocks,
};
use crate::bridge::{LifecycleKind, SessionBinding};
use crate::state::SessionPhase;
use crate::store::{
    AnchorDeletionObservation, AnchorStoreError, ConfigMapAnchorStore, StoredAnchor,
};
use crate::wire::{LifecycleRequestV1, WireProtocolError};
use std::sync::Arc;
use thiserror::Error;

/// Stable result of one request-driven destructive release pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// Kubernetes accepted deletion or is still completing finalization.
    Pending,
    /// The anchor and every selector/deterministic child are absent.
    Released,
}

/// Sanitized destructive-release failures. Sources remain available to
/// trusted controller logs while transport-facing messages stay static.
#[derive(Debug, Error)]
pub enum ReleaseError {
    #[error("only destructive release is supported by this coordinator")]
    UnsupportedKind,
    #[error("the release request does not belong to this controller scope")]
    ScopeMismatch,
    #[error("the release binding is stale or invalid")]
    StaleBinding,
    #[error("the durable release anchor is invalid")]
    InvalidAnchor,
    #[error("a release absence proof does not match its exact authority")]
    ProofMismatch,
    #[error("release anchor persistence failed")]
    Store(#[source] AnchorStoreError),
    #[error("worker release reconciliation failed")]
    Provisioner(#[source] super::GenerationProvisionerError),
}

impl From<AnchorStoreError> for ReleaseError {
    fn from(error: AnchorStoreError) -> Self {
        Self::Store(error)
    }
}

/// Coordinates explicit, fenced, destructive release.
///
/// Unlike suspension, release is acknowledged only after the lifecycle
/// anchor and every managed child are authoritatively absent. Every step is
/// retry-safe from the durable `Deleting` phase. `worker_session_id` remains
/// opaque ACP data and never participates in authorization or selection.
pub struct ReleaseCoordinator {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    provisioner: Arc<dyn ReleaseProvisioner>,
}

impl ReleaseCoordinator {
    pub fn new(
        store: ConfigMapAnchorStore,
        locks: SessionLocks,
        provisioner: Arc<dyn ReleaseProvisioner>,
    ) -> Self {
        Self {
            store,
            locks,
            provisioner,
        }
    }

    /// Reconcile one exact release request to either pending or fully absent.
    pub async fn release(
        &self,
        request: &LifecycleRequestV1,
    ) -> Result<ReleaseOutcome, ReleaseError> {
        if request.kind() != LifecycleKind::Release {
            return Err(ReleaseError::UnsupportedKind);
        }
        let binding = request
            .to_binding()
            .map_err(|_error: WireProtocolError| ReleaseError::StaleBinding)?;
        if binding.scope_id() != self.store.scope_id() {
            return Err(ReleaseError::ScopeMismatch);
        }

        let _guard = self.locks.lock(binding.session_id()).await;
        let Some(mut observed) = self.store.get_for_deletion(binding.session_id()).await? else {
            return self.prove_post_anchor_absence(&binding).await;
        };
        validate_binding(&observed, &binding, self.store.scope_id())?;

        if observed.is_terminating() {
            return self.observe_anchor_absence(&binding, &observed).await;
        }

        if observed.state().phase() != SessionPhase::Deleting {
            let mut next = observed.state().clone();
            next.transition(binding.fence(), SessionPhase::Deleting)
                .map_err(|_| ReleaseError::InvalidAnchor)?;
            observed = self.store.replace(&observed, &next).await?;
        }

        self.reconcile_observed_deleting(&binding, observed).await
    }

    /// Continue only an already-durable destructive release intent.
    ///
    /// An absent or non-`Deleting` fresh anchor is a no-op, so controller
    /// startup can never manufacture release authority from a stale LIST
    /// snapshot. A terminating anchor is observed only; child cleanup is not
    /// restarted after Kubernetes has accepted anchor deletion.
    pub(crate) async fn reconcile_deleting(
        &self,
        session_id: crate::identity::SessionId,
    ) -> Result<Option<ReleaseOutcome>, ReleaseError> {
        let _guard = self.locks.lock(session_id).await;
        let Some(observed) = self.store.get_for_deletion(session_id).await? else {
            return Ok(None);
        };
        if observed.state().phase() != SessionPhase::Deleting {
            return Ok(None);
        }
        let binding = binding_from_anchor(&observed)?;
        if observed.is_terminating() {
            return self
                .observe_anchor_absence(&binding, &observed)
                .await
                .map(Some);
        }

        self.reconcile_observed_deleting(&binding, observed)
            .await
            .map(Some)
    }

    async fn reconcile_observed_deleting(
        &self,
        binding: &SessionBinding,
        mut observed: StoredAnchor,
    ) -> Result<ReleaseOutcome, ReleaseError> {
        let compute_proof = match self
            .provisioner
            .reconcile_compute_absent(&observed)
            .await
            .map_err(ReleaseError::Provisioner)?
        {
            CleanupProgress::Pending => return Ok(ReleaseOutcome::Pending),
            CleanupProgress::Absent(proof) => proof,
        };
        validate_compute_proof(&observed, &compute_proof)?;

        if let Some(pod_uid) = observed.state().pod_uid() {
            let mut next = observed.state().clone();
            next.confirm_pod_deleted(observed.state().fence(), pod_uid)
                .map_err(|_| ReleaseError::InvalidAnchor)?;
            observed = self.store.replace(&observed, &next).await?;
        }

        let all_children_proof = match self
            .provisioner
            .reconcile_all_children_absent(&observed, &compute_proof)
            .await
            .map_err(ReleaseError::Provisioner)?
        {
            ReleaseCleanupProgress::Pending => return Ok(ReleaseOutcome::Pending),
            ReleaseCleanupProgress::Absent(proof) => proof,
        };
        validate_all_children_proof(&observed, &all_children_proof)?;

        self.store.delete(&observed, &all_children_proof).await?;
        self.observe_anchor_absence(binding, &observed).await
    }

    async fn observe_anchor_absence(
        &self,
        binding: &SessionBinding,
        observed: &StoredAnchor,
    ) -> Result<ReleaseOutcome, ReleaseError> {
        match self
            .store
            .observe_deletion(binding.session_id(), observed.uid())
            .await?
        {
            AnchorDeletionObservation::Absent => self.prove_post_anchor_absence(binding).await,
            AnchorDeletionObservation::Present | AnchorDeletionObservation::Terminating => {
                Ok(ReleaseOutcome::Pending)
            }
        }
    }

    async fn prove_post_anchor_absence(
        &self,
        binding: &SessionBinding,
    ) -> Result<ReleaseOutcome, ReleaseError> {
        let proof = self
            .provisioner
            .prove_released_children_absent(binding)
            .await
            .map_err(ReleaseError::Provisioner)?;
        if !proof.matches_binding(binding) {
            return Err(ReleaseError::ProofMismatch);
        }
        Ok(ReleaseOutcome::Released)
    }
}

fn binding_from_anchor(observed: &StoredAnchor) -> Result<SessionBinding, ReleaseError> {
    let state = observed.state();
    SessionBinding::new(
        state.scope_id(),
        state.session_id(),
        state.fence().clone(),
        state.incarnation_id(),
    )
    .map_err(|_| ReleaseError::InvalidAnchor)
}

fn validate_binding(
    observed: &StoredAnchor,
    binding: &SessionBinding,
    store_scope: crate::identity::ScopeId,
) -> Result<(), ReleaseError> {
    let state = observed.state();
    if state.scope_id() != store_scope
        || binding.scope_id() != store_scope
        || state.session_id() != binding.session_id()
        || state.fence() != binding.fence()
        || state.incarnation_id() != binding.incarnation_id()
    {
        return Err(ReleaseError::StaleBinding);
    }
    Ok(())
}

fn validate_compute_proof(
    observed: &StoredAnchor,
    proof: &ComputeAbsentProof,
) -> Result<(), ReleaseError> {
    if !proof.matches_anchor(observed) {
        return Err(ReleaseError::ProofMismatch);
    }
    Ok(())
}

fn validate_all_children_proof(
    observed: &StoredAnchor,
    proof: &AllChildrenAbsentProof,
) -> Result<(), ReleaseError> {
    if !proof.matches_anchor(observed) {
        return Err(ReleaseError::ProofMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{
        AllChildrenAbsentProof, GenerationProvisionerError, ProvisionerOperation,
        ReleasedChildrenAbsentProof,
    };
    use crate::identity::{ResourceNames, ScopeId, SessionId};
    use crate::state::{Fence, ProfileRef, SessionAnchorV1};
    use async_trait::async_trait;
    use chrono::{Duration, TimeZone, Utc};
    use http::{Method, Request, Response, StatusCode};
    use kube::client::Body;
    use kube::Client;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration as StdDuration;
    use tower_test::mock;
    use uuid::Uuid;

    const NAMESPACE: &str = "team-a-workers";
    const RAW_SCOPE: &str = "organization-secret-team-a";
    const POD_UID: &str = "worker-pod-uid";
    const ANCHOR_UID: &str = "anchor-uid";

    #[derive(Clone, Copy)]
    enum ProofBehavior {
        Pending,
        Matching,
        WrongSession,
        WrongResourceVersion,
    }

    #[derive(Clone, Copy)]
    enum PostProofBehavior {
        Matching,
        WrongFence,
    }

    #[derive(Clone)]
    struct FakeProvisioner {
        compute: ProofBehavior,
        storage: ProofBehavior,
        post: PostProofBehavior,
        compute_calls: Arc<AtomicUsize>,
        storage_calls: Arc<AtomicUsize>,
        post_calls: Arc<AtomicUsize>,
        observed_anchor_versions: Arc<Mutex<Vec<String>>>,
    }

    impl FakeProvisioner {
        fn new(compute: ProofBehavior, storage: ProofBehavior, post: PostProofBehavior) -> Self {
            Self {
                compute,
                storage,
                post,
                compute_calls: Arc::new(AtomicUsize::new(0)),
                storage_calls: Arc::new(AtomicUsize::new(0)),
                post_calls: Arc::new(AtomicUsize::new(0)),
                observed_anchor_versions: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn all_matching() -> Self {
            Self::new(
                ProofBehavior::Matching,
                ProofBehavior::Matching,
                PostProofBehavior::Matching,
            )
        }

        fn calls(&self) -> (usize, usize, usize) {
            (
                self.compute_calls.load(Ordering::SeqCst),
                self.storage_calls.load(Ordering::SeqCst),
                self.post_calls.load(Ordering::SeqCst),
            )
        }

        fn versions(&self) -> Vec<String> {
            self.observed_anchor_versions.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl super::super::LifecycleProvisioner for FakeProvisioner {
        async fn reconcile_compute_absent(
            &self,
            anchor: &StoredAnchor,
        ) -> Result<CleanupProgress, GenerationProvisionerError> {
            self.compute_calls.fetch_add(1, Ordering::SeqCst);
            self.observed_anchor_versions
                .lock()
                .unwrap()
                .push(anchor.resource_version().to_string());
            Ok(match self.compute {
                ProofBehavior::Pending => CleanupProgress::Pending,
                ProofBehavior::Matching | ProofBehavior::WrongResourceVersion => {
                    CleanupProgress::Absent(ComputeAbsentProof::for_test(
                        anchor.state().session_id(),
                        anchor.state().incarnation_id(),
                        anchor.state().fence().clone(),
                        anchor.uid(),
                    ))
                }
                ProofBehavior::WrongSession => {
                    CleanupProgress::Absent(ComputeAbsentProof::for_test(
                        session_id("discord:foreign"),
                        anchor.state().incarnation_id(),
                        anchor.state().fence().clone(),
                        anchor.uid(),
                    ))
                }
            })
        }
    }

    #[async_trait]
    impl ReleaseProvisioner for FakeProvisioner {
        async fn reconcile_all_children_absent(
            &self,
            anchor: &StoredAnchor,
            _compute_proof: &ComputeAbsentProof,
        ) -> Result<ReleaseCleanupProgress, GenerationProvisionerError> {
            self.storage_calls.fetch_add(1, Ordering::SeqCst);
            self.observed_anchor_versions
                .lock()
                .unwrap()
                .push(anchor.resource_version().to_string());
            Ok(match self.storage {
                ProofBehavior::Pending => ReleaseCleanupProgress::Pending,
                ProofBehavior::Matching => {
                    ReleaseCleanupProgress::Absent(AllChildrenAbsentProof::for_test(
                        anchor.state().session_id(),
                        anchor.state().incarnation_id(),
                        anchor.state().fence().clone(),
                        anchor.uid(),
                        anchor.resource_version(),
                    ))
                }
                ProofBehavior::WrongSession => {
                    ReleaseCleanupProgress::Absent(AllChildrenAbsentProof::for_test(
                        session_id("discord:foreign"),
                        anchor.state().incarnation_id(),
                        anchor.state().fence().clone(),
                        anchor.uid(),
                        anchor.resource_version(),
                    ))
                }
                ProofBehavior::WrongResourceVersion => {
                    ReleaseCleanupProgress::Absent(AllChildrenAbsentProof::for_test(
                        anchor.state().session_id(),
                        anchor.state().incarnation_id(),
                        anchor.state().fence().clone(),
                        anchor.uid(),
                        "stale-resource-version",
                    ))
                }
            })
        }

        async fn prove_released_children_absent(
            &self,
            binding: &SessionBinding,
        ) -> Result<ReleasedChildrenAbsentProof, GenerationProvisionerError> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            let fence = match self.post {
                PostProofBehavior::Matching => binding.fence().clone(),
                PostProofBehavior::WrongFence => {
                    Fence::new(binding.fence().generation() + 1, Uuid::from_u128(0x999)).unwrap()
                }
            };
            Ok(ReleasedChildrenAbsentProof::for_test(
                binding.scope_id(),
                binding.session_id(),
                binding.incarnation_id(),
                fence,
            ))
        }
    }

    fn scope_id() -> ScopeId {
        ScopeId::derive(RAW_SCOPE)
    }

    fn session_id(label: &str) -> SessionId {
        SessionId::derive(RAW_SCOPE, label)
    }

    fn base_anchor(session_id: SessionId) -> SessionAnchorV1 {
        let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
        SessionAnchorV1::new(
            session_id,
            scope_id(),
            ProfileRef::new("codex-strict", "2026-08-01").unwrap(),
            Uuid::from_u128(0x100),
            Uuid::from_u128(0x200),
            now,
            now + Duration::minutes(15),
            now + Duration::hours(72),
        )
        .unwrap()
    }

    fn ready_anchor(session_id: SessionId) -> SessionAnchorV1 {
        let mut anchor = base_anchor(session_id);
        let fence = anchor.fence().clone();
        anchor.observe_pod(&fence, POD_UID).unwrap();
        anchor.transition(&fence, SessionPhase::Ready).unwrap();
        anchor
    }

    fn deleting_anchor(session_id: SessionId, with_pod: bool) -> SessionAnchorV1 {
        let mut anchor = base_anchor(session_id);
        let fence = anchor.fence().clone();
        if with_pod {
            anchor.observe_pod(&fence, POD_UID).unwrap();
        }
        anchor.transition(&fence, SessionPhase::Deleting).unwrap();
        anchor
    }

    fn lifecycle_request(
        kind: LifecycleKind,
        anchor: &SessionAnchorV1,
        scope_id: ScopeId,
        fence: &Fence,
        incarnation_id: Uuid,
        worker_session_id: &str,
    ) -> LifecycleRequestV1 {
        serde_json::from_value(json!({
            "version": 1,
            "requestId": Uuid::from_u128(0x300),
            "kind": match kind {
                LifecycleKind::Suspend => "suspend",
                LifecycleKind::Release => "release",
            },
            "binding": {
                "version": 1,
                "scopeId": scope_id,
                "sessionId": anchor.session_id(),
                "generation": fence.generation(),
                "attemptId": fence.attempt_id(),
                "incarnationId": incarnation_id,
            },
            "workerSessionId": worker_session_id,
        }))
        .unwrap()
    }

    fn request_for(anchor: &SessionAnchorV1) -> LifecycleRequestV1 {
        lifecycle_request(
            LifecycleKind::Release,
            anchor,
            anchor.scope_id(),
            anchor.fence(),
            anchor.incarnation_id(),
            "opaque-worker-session",
        )
    }

    fn config_map(anchor: &SessionAnchorV1, resource_version: &str, terminating: bool) -> Value {
        let mut object = json!({
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
            "data": {
                "anchor.json": serde_json::to_string(anchor).unwrap()
            }
        });
        if terminating {
            object["metadata"]["deletionTimestamp"] = json!("2026-08-01T08:01:00Z");
            object["metadata"]["finalizers"] = json!(["admission.example.test/hold"]);
        }
        object
    }

    fn json_response(status: StatusCode, body: Value) -> Response<Body> {
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn failure_response(status: StatusCode, reason: &str) -> Response<Body> {
        json_response(
            status,
            json!({
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Failure",
                "reason": reason,
                "code": status.as_u16()
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
        Arc<ReleaseCoordinator>,
        mock::Handle<Request<Body>, Response<Body>>,
    ) {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let store =
            ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id())
                .unwrap();
        (
            Arc::new(ReleaseCoordinator::new(
                store,
                SessionLocks::new(),
                Arc::new(fake),
            )),
            handle,
        )
    }

    async fn respond_get_anchor(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
        anchor: &SessionAnchorV1,
        resource_version: &str,
        terminating: bool,
    ) {
        let (request, send) = handle.next_request().await.expect("anchor GET");
        assert_eq!(request.method(), Method::GET);
        assert!(request
            .uri()
            .path()
            .ends_with(&ResourceNames::new(anchor.session_id()).anchor()));
        send.send_response(json_response(
            StatusCode::OK,
            config_map(anchor, resource_version, terminating),
        ));
    }

    async fn respond_replace(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
        next_resource_version: &str,
    ) -> SessionAnchorV1 {
        let (request, send) = handle.next_request().await.expect("anchor replace");
        assert_eq!(request.method(), Method::PUT);
        let mut body = request_body(request).await;
        let state: SessionAnchorV1 =
            serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        body["metadata"]["resourceVersion"] = json!(next_resource_version);
        send.send_response(json_response(StatusCode::OK, body));
        state
    }

    async fn respond_anchor_delete(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
        expected_resource_version: &str,
        status: StatusCode,
    ) {
        let (request, send) = handle.next_request().await.expect("anchor DELETE");
        assert_eq!(request.method(), Method::DELETE);
        let body = request_body(request).await;
        assert_eq!(body["preconditions"]["uid"], ANCHOR_UID);
        assert_eq!(
            body["preconditions"]["resourceVersion"],
            expected_resource_version
        );
        if status == StatusCode::CONFLICT {
            send.send_response(failure_response(status, "Conflict"));
        } else if status == StatusCode::NOT_FOUND {
            send.send_response(failure_response(status, "NotFound"));
        } else {
            send.send_response(json_response(
                status,
                json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Success",
                    "code": status.as_u16()
                }),
            ));
        }
    }

    async fn respond_anchor_absent(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    ) {
        let (request, send) = handle.next_request().await.expect("anchor observation");
        assert_eq!(request.method(), Method::GET);
        send.send_response(failure_response(StatusCode::NOT_FOUND, "NotFound"));
    }

    async fn assert_no_request(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    ) {
        let unexpected =
            tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
        assert!(!matches!(unexpected, Ok(Some(_))));
    }

    #[tokio::test]
    async fn ready_release_persists_intent_then_proves_everything_before_ack() {
        let anchor = ready_anchor(session_id("discord:happy"));
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed", false).await;
        let deleting = respond_replace(&mut handle, "rv-deleting").await;
        assert_eq!(deleting.phase(), SessionPhase::Deleting);
        assert_eq!(deleting.pod_uid(), Some(POD_UID));
        let pod_cleared = respond_replace(&mut handle, "rv-cleared").await;
        assert_eq!(pod_cleared.phase(), SessionPhase::Deleting);
        assert_eq!(pod_cleared.pod_uid(), None);
        respond_anchor_delete(&mut handle, "rv-cleared", StatusCode::OK).await;
        respond_anchor_absent(&mut handle).await;

        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Released);
        assert_eq!(fake.calls(), (1, 1, 1));
        assert_eq!(fake.versions(), ["rv-deleting", "rv-cleared"]);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn compute_pending_never_clears_pod_or_deletes_anchor() {
        let anchor = ready_anchor(session_id("discord:compute-pending"));
        let request = request_for(&anchor);
        let fake = FakeProvisioner::new(
            ProofBehavior::Pending,
            ProofBehavior::Matching,
            PostProofBehavior::Matching,
        );
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed", false).await;
        let deleting = respond_replace(&mut handle, "rv-deleting").await;
        assert_eq!(deleting.phase(), SessionPhase::Deleting);
        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Pending);
        assert_eq!(fake.calls(), (1, 0, 0));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn initial_deleting_cas_conflict_never_starts_cleanup() {
        let anchor = ready_anchor(session_id("discord:intent-conflict"));
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed", false).await;
        let (request, send) = handle.next_request().await.expect("anchor replace");
        assert_eq!(request.method(), Method::PUT);
        send.send_response(failure_response(StatusCode::CONFLICT, "Conflict"));

        assert!(matches!(
            task.await.unwrap(),
            Err(ReleaseError::Store(AnchorStoreError::Conflict { .. }))
        ));
        assert_eq!(fake.calls(), (0, 0, 0));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn storage_pending_never_deletes_anchor_or_acknowledges_release() {
        let anchor = deleting_anchor(session_id("discord:storage-pending"), false);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::new(
            ProofBehavior::Matching,
            ProofBehavior::Pending,
            PostProofBehavior::Matching,
        );
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;
        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Pending);
        assert_eq!(fake.calls(), (1, 1, 0));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn mismatched_compute_or_all_children_proof_fails_before_anchor_delete() {
        for (compute, storage) in [
            (ProofBehavior::WrongSession, ProofBehavior::Matching),
            (ProofBehavior::Matching, ProofBehavior::WrongSession),
            (ProofBehavior::Matching, ProofBehavior::WrongResourceVersion),
        ] {
            let anchor = deleting_anchor(session_id("discord:proof-mismatch"), false);
            let request = request_for(&anchor);
            let fake = FakeProvisioner::new(compute, storage, PostProofBehavior::Matching);
            let (coordinator, handle) = coordinator(fake);
            let task = tokio::spawn(async move { coordinator.release(&request).await });
            let mut handle = std::pin::pin!(handle);
            respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;

            assert!(matches!(
                task.await.unwrap(),
                Err(ReleaseError::ProofMismatch)
            ));
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn deleting_crash_retry_completes_without_reaccepting_intent() {
        let anchor = deleting_anchor(session_id("discord:retry"), false);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;
        respond_anchor_delete(&mut handle, "rv-deleting", StatusCode::OK).await;
        respond_anchor_absent(&mut handle).await;

        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Released);
        assert_eq!(fake.calls(), (1, 1, 1));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn anchor_delete_not_found_still_requires_observation_and_post_proof() {
        let anchor = deleting_anchor(session_id("discord:delete-not-found"), false);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;
        respond_anchor_delete(&mut handle, "rv-deleting", StatusCode::NOT_FOUND).await;
        respond_anchor_absent(&mut handle).await;

        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Released);
        assert_eq!(fake.calls(), (1, 1, 1));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn terminating_anchor_retry_waits_without_reconciling_children() {
        let anchor = deleting_anchor(session_id("discord:terminating"), false);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-terminating", true).await;
        respond_get_anchor(&mut handle, &anchor, "rv-terminating", true).await;

        assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Pending);
        assert_eq!(fake.calls(), (0, 0, 0));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn absent_anchor_requires_post_anchor_child_proof_before_ack() {
        for post in [PostProofBehavior::Matching, PostProofBehavior::WrongFence] {
            let anchor = deleting_anchor(session_id("discord:anchor-absent"), false);
            let request = request_for(&anchor);
            let fake = FakeProvisioner::new(ProofBehavior::Matching, ProofBehavior::Matching, post);
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.release(&request).await });
            let mut handle = std::pin::pin!(handle);
            respond_anchor_absent(&mut handle).await;

            match post {
                PostProofBehavior::Matching => {
                    assert_eq!(task.await.unwrap().unwrap(), ReleaseOutcome::Released)
                }
                PostProofBehavior::WrongFence => assert!(matches!(
                    task.await.unwrap(),
                    Err(ReleaseError::ProofMismatch)
                )),
            }
            assert_eq!(fake.calls(), (0, 0, 1));
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn anchor_delete_conflict_is_not_acknowledged() {
        let anchor = deleting_anchor(session_id("discord:anchor-conflict"), false);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;
        respond_anchor_delete(&mut handle, "rv-deleting", StatusCode::CONFLICT).await;

        assert!(matches!(
            task.await.unwrap(),
            Err(ReleaseError::Store(AnchorStoreError::Conflict { .. }))
        ));
        assert_eq!(fake.calls(), (1, 1, 0));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn wrong_kind_or_scope_fails_without_kubernetes_or_provisioner_calls() {
        let anchor = ready_anchor(session_id("discord:wrong-authority"));
        let requests = [
            lifecycle_request(
                LifecycleKind::Suspend,
                &anchor,
                anchor.scope_id(),
                anchor.fence(),
                anchor.incarnation_id(),
                "opaque-suspend-worker",
            ),
            lifecycle_request(
                LifecycleKind::Release,
                &anchor,
                ScopeId::derive("wrong-scope"),
                anchor.fence(),
                anchor.incarnation_id(),
                "opaque-wrong-scope-worker",
            ),
        ];

        for request in requests {
            let fake = FakeProvisioner::all_matching();
            let (coordinator, handle) = coordinator(fake.clone());
            let error = coordinator.release(&request).await.unwrap_err();
            assert!(matches!(
                error,
                ReleaseError::UnsupportedKind | ReleaseError::ScopeMismatch
            ));
            assert!(!error.to_string().contains(request.worker_session_id()));
            assert_eq!(fake.calls(), (0, 0, 0));
            let mut handle = std::pin::pin!(handle);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn stale_fence_or_incarnation_never_persists_or_reconciles() {
        let anchor = ready_anchor(session_id("discord:stale"));
        let stale_fence = Fence::new(2, Uuid::from_u128(0x999)).unwrap();
        let requests = [
            lifecycle_request(
                LifecycleKind::Release,
                &anchor,
                anchor.scope_id(),
                &stale_fence,
                anchor.incarnation_id(),
                "opaque-stale-fence",
            ),
            lifecycle_request(
                LifecycleKind::Release,
                &anchor,
                anchor.scope_id(),
                anchor.fence(),
                Uuid::from_u128(0x998),
                "opaque-stale-incarnation",
            ),
        ];

        for request in requests {
            let fake = FakeProvisioner::all_matching();
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.release(&request).await });
            let mut handle = std::pin::pin!(handle);
            respond_get_anchor(&mut handle, &anchor, "rv-observed", false).await;

            assert!(matches!(
                task.await.unwrap(),
                Err(ReleaseError::StaleBinding)
            ));
            assert_eq!(fake.calls(), (0, 0, 0));
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn provisioner_errors_are_sanitized_and_never_acknowledged() {
        #[derive(Clone)]
        struct FailingProvisioner;

        #[async_trait]
        impl super::super::LifecycleProvisioner for FailingProvisioner {
            async fn reconcile_compute_absent(
                &self,
                _anchor: &StoredAnchor,
            ) -> Result<CleanupProgress, GenerationProvisionerError> {
                Err(GenerationProvisionerError::KubernetesApi {
                    operation: ProvisionerOperation::ReconcileComputeAbsence,
                })
            }
        }

        #[async_trait]
        impl ReleaseProvisioner for FailingProvisioner {
            async fn reconcile_all_children_absent(
                &self,
                _anchor: &StoredAnchor,
                _compute_proof: &ComputeAbsentProof,
            ) -> Result<ReleaseCleanupProgress, GenerationProvisionerError> {
                Err(GenerationProvisionerError::InvalidGeneration)
            }

            async fn prove_released_children_absent(
                &self,
                _binding: &SessionBinding,
            ) -> Result<ReleasedChildrenAbsentProof, GenerationProvisionerError> {
                Err(GenerationProvisionerError::InvalidGeneration)
            }
        }

        let anchor = deleting_anchor(session_id("discord:api-error"), false);
        let request = request_for(&anchor);
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let store =
            ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id())
                .unwrap();
        let coordinator = Arc::new(ReleaseCoordinator::new(
            store,
            SessionLocks::new(),
            Arc::new(FailingProvisioner),
        ));
        let task = tokio::spawn(async move { coordinator.release(&request).await });
        let mut handle = std::pin::pin!(handle);
        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;

        let error = task.await.unwrap().unwrap_err();
        assert!(matches!(error, ReleaseError::Provisioner(_)));
        assert_eq!(error.to_string(), "worker release reconciliation failed");
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn durable_release_recovery_never_starts_a_new_deleting_intent() {
        for anchor in [
            None,
            Some(ready_anchor(session_id("discord:startup-ready"))),
        ] {
            let fake = FakeProvisioner::all_matching();
            let (coordinator, handle) = coordinator(fake.clone());
            let target = anchor.as_ref().map_or_else(
                || session_id("discord:startup-absent"),
                SessionAnchorV1::session_id,
            );
            let task = tokio::spawn(async move { coordinator.reconcile_deleting(target).await });
            let mut handle = std::pin::pin!(handle);
            match anchor {
                Some(anchor) => {
                    respond_get_anchor(&mut handle, &anchor, "rv-observed", false).await
                }
                None => respond_anchor_absent(&mut handle).await,
            }

            assert_eq!(task.await.unwrap().unwrap(), None);
            assert_eq!(fake.calls(), (0, 0, 0));
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn durable_release_recovery_continues_only_an_existing_deleting_intent() {
        let anchor = deleting_anchor(session_id("discord:startup-deleting"), false);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_deleting(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-deleting", false).await;
        respond_anchor_delete(&mut handle, "rv-deleting", StatusCode::OK).await;
        respond_anchor_absent(&mut handle).await;

        assert_eq!(task.await.unwrap().unwrap(), Some(ReleaseOutcome::Released));
        assert_eq!(fake.calls(), (1, 1, 1));
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn durable_release_recovery_only_observes_a_terminating_anchor() {
        let anchor = deleting_anchor(session_id("discord:startup-terminating"), false);
        let fake = FakeProvisioner::all_matching();
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_deleting(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-terminating", true).await;
        respond_get_anchor(&mut handle, &anchor, "rv-terminating", true).await;

        assert_eq!(task.await.unwrap().unwrap(), Some(ReleaseOutcome::Pending));
        assert_eq!(fake.calls(), (0, 0, 0));
        assert_no_request(&mut handle).await;
    }
}
