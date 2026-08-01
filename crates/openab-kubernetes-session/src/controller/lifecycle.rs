use super::{CleanupProgress, ComputeAbsentProof, LifecycleProvisioner, SessionLocks};
use crate::bridge::{LifecycleKind, SessionBinding};
use crate::identity::SessionId;
use crate::state::SessionPhase;
use crate::store::{AnchorStoreError, ConfigMapAnchorStore, StoredAnchor};
use crate::wire::{LifecycleRequestV1, WireProtocolError};
use std::sync::Arc;
use thiserror::Error;

/// Stable outcomes from one non-destructive compute reconciliation pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleReconcileOutcome {
    /// Kubernetes has accepted or is still completing a child deletion.
    Pending,
    /// Compute is absent and the durable anchor is now `Suspended`.
    Suspended,
    /// Compute is absent and a recovery-only `Blocked` anchor remains blocked.
    Blocked,
}

/// Sanitized lifecycle failures. Nested sources are retained for trusted
/// controller logs, while each transport-facing display string is static.
#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("only non-destructive suspension is supported by this coordinator")]
    UnsupportedKind,
    #[error("the lifecycle request does not belong to this controller scope")]
    ScopeMismatch,
    #[error("the target lifecycle anchor was not found")]
    AnchorNotFound,
    #[error("the lifecycle binding is stale or invalid")]
    StaleBinding,
    #[error("the session phase does not accept this lifecycle operation")]
    PhaseRejected,
    #[error("the durable lifecycle anchor is invalid")]
    InvalidAnchor,
    #[error("the compute-absence proof does not match the durable lifecycle anchor")]
    ProofMismatch,
    #[error("lifecycle anchor persistence failed")]
    Store(#[source] AnchorStoreError),
    #[error("worker compute reconciliation failed")]
    Provisioner(#[source] super::GenerationProvisionerError),
}

impl From<AnchorStoreError> for LifecycleError {
    fn from(error: AnchorStoreError) -> Self {
        Self::Store(error)
    }
}

/// Coordinates durable suspend acceptance and non-destructive worker cleanup.
///
/// The broker ACKs `accept_suspend` only after the `Suspending` intent has
/// been persisted. Cleanup is a separate, retry-safe reconciliation pass and
/// deliberately retains the workspace PVC and lifecycle ConfigMap.
pub struct LifecycleCoordinator {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    provisioner: Arc<dyn LifecycleProvisioner>,
}

impl LifecycleCoordinator {
    pub fn new(
        store: ConfigMapAnchorStore,
        locks: SessionLocks,
        provisioner: Arc<dyn LifecycleProvisioner>,
    ) -> Self {
        Self {
            store,
            locks,
            provisioner,
        }
    }

    /// Durably accept one exact, fenced suspend request without waiting for
    /// Kubernetes deletion to finish. `worker_session_id` remains opaque ACP
    /// data and never participates in anchor selection or authorization.
    pub async fn accept_suspend(&self, request: &LifecycleRequestV1) -> Result<(), LifecycleError> {
        if request.kind() != LifecycleKind::Suspend {
            return Err(LifecycleError::UnsupportedKind);
        }
        let binding = request
            .to_binding()
            .map_err(|_error: WireProtocolError| LifecycleError::StaleBinding)?;
        if binding.scope_id() != self.store.scope_id() {
            return Err(LifecycleError::ScopeMismatch);
        }

        let _guard = self.locks.lock(binding.session_id()).await;
        let observed = self
            .store
            .get(binding.session_id())
            .await?
            .ok_or(LifecycleError::AnchorNotFound)?;
        validate_binding(&observed, &binding, self.store.scope_id())?;

        match observed.state().phase() {
            SessionPhase::Ready | SessionPhase::Busy => {
                let mut next = observed.state().clone();
                next.transition(binding.fence(), SessionPhase::Suspending)
                    .map_err(|_| LifecycleError::InvalidAnchor)?;
                self.store.replace(&observed, &next).await?;
                Ok(())
            }
            SessionPhase::Suspending | SessionPhase::Suspended => Ok(()),
            SessionPhase::Provisioning | SessionPhase::Deleting | SessionPhase::Blocked => {
                Err(LifecycleError::PhaseRejected)
            }
        }
    }

    /// Reconcile generation-scoped compute to absence for a previously
    /// accepted suspend or a fail-closed `Blocked` recovery anchor.
    pub async fn reconcile_compute(
        &self,
        session_id: SessionId,
    ) -> Result<LifecycleReconcileOutcome, LifecycleError> {
        let _guard = self.locks.lock(session_id).await;
        let observed = self
            .store
            .get(session_id)
            .await?
            .ok_or(LifecycleError::AnchorNotFound)?;
        if observed.state().scope_id() != self.store.scope_id()
            || observed.state().session_id() != session_id
        {
            return Err(LifecycleError::StaleBinding);
        }
        let phase = observed.state().phase();
        if !matches!(phase, SessionPhase::Suspending | SessionPhase::Blocked) {
            return Err(LifecycleError::PhaseRejected);
        }

        self.reconcile_observed_compute(observed, phase).await
    }

    /// Continue only an already-durable compute-removal intent.
    ///
    /// Unlike the request-facing strict API, startup races that leave the
    /// anchor absent or in another phase are harmless no-ops. The inventory
    /// snapshot is never trusted as mutation authority: this method takes the
    /// session lock and performs a fresh read first.
    pub(crate) async fn reconcile_durable_compute(
        &self,
        session_id: SessionId,
    ) -> Result<Option<LifecycleReconcileOutcome>, LifecycleError> {
        let _guard = self.locks.lock(session_id).await;
        let Some(observed) = self.store.get(session_id).await? else {
            return Ok(None);
        };
        if observed.state().scope_id() != self.store.scope_id()
            || observed.state().session_id() != session_id
        {
            return Err(LifecycleError::StaleBinding);
        }
        let phase = observed.state().phase();
        if !matches!(phase, SessionPhase::Suspending | SessionPhase::Blocked) {
            return Ok(None);
        }

        self.reconcile_observed_compute(observed, phase)
            .await
            .map(Some)
    }

    async fn reconcile_observed_compute(
        &self,
        observed: StoredAnchor,
        phase: SessionPhase,
    ) -> Result<LifecycleReconcileOutcome, LifecycleError> {
        let progress = self
            .provisioner
            .reconcile_compute_absent(&observed)
            .await
            .map_err(LifecycleError::Provisioner)?;
        let CleanupProgress::Absent(proof) = progress else {
            return Ok(LifecycleReconcileOutcome::Pending);
        };
        validate_absence_proof(&observed, &proof)?;

        let mut next = observed.state().clone();
        if let Some(pod_uid) = observed.state().pod_uid() {
            next.confirm_pod_deleted(observed.state().fence(), pod_uid)
                .map_err(|_| LifecycleError::InvalidAnchor)?;
        }

        match phase {
            SessionPhase::Suspending => {
                next.transition(observed.state().fence(), SessionPhase::Suspended)
                    .map_err(|_| LifecycleError::InvalidAnchor)?;
                self.store.replace(&observed, &next).await?;
                Ok(LifecycleReconcileOutcome::Suspended)
            }
            SessionPhase::Blocked => {
                if next != *observed.state() {
                    self.store.replace(&observed, &next).await?;
                }
                Ok(LifecycleReconcileOutcome::Blocked)
            }
            _ => Err(LifecycleError::PhaseRejected),
        }
    }
}

fn validate_binding(
    observed: &StoredAnchor,
    binding: &SessionBinding,
    store_scope: crate::identity::ScopeId,
) -> Result<(), LifecycleError> {
    let state = observed.state();
    if state.scope_id() != store_scope
        || binding.scope_id() != store_scope
        || state.session_id() != binding.session_id()
        || state.fence() != binding.fence()
        || state.incarnation_id() != binding.incarnation_id()
    {
        return Err(LifecycleError::StaleBinding);
    }
    Ok(())
}

fn validate_absence_proof(
    observed: &StoredAnchor,
    proof: &ComputeAbsentProof,
) -> Result<(), LifecycleError> {
    if !proof.matches_anchor(observed) {
        return Err(LifecycleError::ProofMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{GenerationProvisionerError, ProvisionerOperation};
    use crate::identity::{ResourceNames, ScopeId};
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
    use tokio::sync::Notify;
    use tower_test::mock;
    use uuid::Uuid;

    const NAMESPACE: &str = "team-a-workers";
    const RAW_SCOPE: &str = "organization-secret-team-a";
    const POD_UID: &str = "worker-pod-uid";
    const ANCHOR_UID: &str = "anchor-uid";

    #[derive(Clone)]
    struct FakeProvisioner {
        progress: Arc<Mutex<Result<CleanupProgress, GenerationProvisionerError>>>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeProvisioner {
        fn returning(progress: CleanupProgress) -> Self {
            Self {
                progress: Arc::new(Mutex::new(Ok(progress))),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl LifecycleProvisioner for FakeProvisioner {
        async fn reconcile_compute_absent(
            &self,
            _anchor: &StoredAnchor,
        ) -> Result<CleanupProgress, GenerationProvisionerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.progress.lock().unwrap().clone()
        }
    }

    struct GateProvisioner {
        entered: Notify,
        release: Notify,
    }

    #[async_trait]
    impl LifecycleProvisioner for GateProvisioner {
        async fn reconcile_compute_absent(
            &self,
            _anchor: &StoredAnchor,
        ) -> Result<CleanupProgress, GenerationProvisionerError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(CleanupProgress::Pending)
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

    fn anchor_in_phase(session_id: SessionId, phase: SessionPhase) -> SessionAnchorV1 {
        let mut anchor = base_anchor(session_id);
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
            SessionPhase::Blocked => {
                anchor.transition(&fence, SessionPhase::Blocked).unwrap();
            }
            SessionPhase::Deleting => {
                anchor.transition(&fence, SessionPhase::Deleting).unwrap();
            }
        }
        anchor
    }

    fn blocked_anchor_with_pod(session_id: SessionId) -> SessionAnchorV1 {
        let mut anchor = base_anchor(session_id);
        let fence = anchor.fence().clone();
        anchor.observe_pod(&fence, POD_UID).unwrap();
        anchor.transition(&fence, SessionPhase::Blocked).unwrap();
        anchor
    }

    fn lifecycle_request(
        kind: LifecycleKind,
        session_id: SessionId,
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
                "sessionId": session_id,
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
            LifecycleKind::Suspend,
            anchor.session_id(),
            anchor.scope_id(),
            anchor.fence(),
            anchor.incarnation_id(),
            "opaque-worker-session",
        )
    }

    fn proof_for(anchor: &SessionAnchorV1) -> ComputeAbsentProof {
        ComputeAbsentProof::for_test(
            anchor.session_id(),
            anchor.incarnation_id(),
            anchor.fence().clone(),
            ANCHOR_UID,
        )
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
            "data": {
                "anchor.json": serde_json::to_string(anchor).unwrap()
            }
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
                "reason": "Conflict",
                "code": 409
            }),
        )
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

    fn coordinator_with(
        provisioner: Arc<dyn LifecycleProvisioner>,
        locks: SessionLocks,
    ) -> (
        Arc<LifecycleCoordinator>,
        mock::Handle<Request<Body>, Response<Body>>,
    ) {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
        (
            Arc::new(LifecycleCoordinator::new(store, locks, provisioner)),
            handle,
        )
    }

    fn coordinator(
        fake: FakeProvisioner,
    ) -> (
        Arc<LifecycleCoordinator>,
        mock::Handle<Request<Body>, Response<Body>>,
    ) {
        coordinator_with(Arc::new(fake), SessionLocks::new())
    }

    async fn respond_get_anchor(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
        anchor: &SessionAnchorV1,
        resource_version: &str,
    ) {
        let (request, send) = handle.next_request().await.expect("anchor GET");
        assert_eq!(request.method(), Method::GET);
        assert!(request
            .uri()
            .path()
            .ends_with(&ResourceNames::new(anchor.session_id()).anchor()));
        send.send_response(json_response(
            StatusCode::OK,
            config_map(anchor, resource_version),
        ));
    }

    async fn respond_replace(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    ) -> SessionAnchorV1 {
        let (request, send) = handle.next_request().await.expect("anchor replace");
        assert_eq!(request.method(), Method::PUT);
        let mut body = request_body(request).await;
        let state: SessionAnchorV1 =
            serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
        body["metadata"]["resourceVersion"] = json!("rv-next");
        send.send_response(json_response(StatusCode::OK, body));
        state
    }

    async fn assert_no_request(
        handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    ) {
        let unexpected =
            tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
        assert!(!matches!(unexpected, Ok(Some(_))));
    }

    #[tokio::test]
    async fn ready_and_busy_suspend_are_durably_accepted_before_ack() {
        for phase in [SessionPhase::Ready, SessionPhase::Busy] {
            let session_id = session_id(&format!("discord:{phase:?}"));
            let anchor = anchor_in_phase(session_id, phase);
            let request = request_for(&anchor);
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.accept_suspend(&request).await });
            let mut handle = std::pin::pin!(handle);

            respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
            let persisted = respond_replace(&mut handle).await;
            assert_eq!(persisted.phase(), SessionPhase::Suspending);
            assert_eq!(persisted.pod_uid(), Some(POD_UID));
            assert!(task.await.unwrap().is_ok());
            assert_eq!(fake.calls(), 0);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn exact_suspending_and_suspended_retries_are_idempotent() {
        for phase in [SessionPhase::Suspending, SessionPhase::Suspended] {
            let anchor = anchor_in_phase(session_id(&format!("discord:retry-{phase:?}")), phase);
            let request = request_for(&anchor);
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.accept_suspend(&request).await });
            let mut handle = std::pin::pin!(handle);

            respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
            assert!(task.await.unwrap().is_ok());
            assert_eq!(fake.calls(), 0);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn release_and_wrong_scope_fail_without_kubernetes_or_cleanup() {
        let anchor = anchor_in_phase(session_id("discord:rejected"), SessionPhase::Ready);
        for request in [
            lifecycle_request(
                LifecycleKind::Release,
                anchor.session_id(),
                anchor.scope_id(),
                anchor.fence(),
                anchor.incarnation_id(),
                "release-worker-secret-name",
            ),
            lifecycle_request(
                LifecycleKind::Suspend,
                anchor.session_id(),
                ScopeId::derive("wrong-scope"),
                anchor.fence(),
                anchor.incarnation_id(),
                "wrong-scope-worker-secret-name",
            ),
        ] {
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let error = coordinator.accept_suspend(&request).await.unwrap_err();
            assert!(matches!(
                error,
                LifecycleError::UnsupportedKind | LifecycleError::ScopeMismatch
            ));
            assert!(!error.to_string().contains(request.worker_session_id()));
            assert_eq!(fake.calls(), 0);
            let mut handle = std::pin::pin!(handle);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn stale_fence_and_incarnation_never_write_or_reconcile() {
        let anchor = anchor_in_phase(session_id("discord:stale"), SessionPhase::Ready);
        let stale_fence = Fence::new(2, Uuid::from_u128(0x999)).unwrap();
        let requests = [
            lifecycle_request(
                LifecycleKind::Suspend,
                anchor.session_id(),
                anchor.scope_id(),
                &stale_fence,
                anchor.incarnation_id(),
                "opaque-stale-fence",
            ),
            lifecycle_request(
                LifecycleKind::Suspend,
                anchor.session_id(),
                anchor.scope_id(),
                anchor.fence(),
                Uuid::from_u128(0x998),
                "opaque-stale-incarnation",
            ),
        ];

        for request in requests {
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.accept_suspend(&request).await });
            let mut handle = std::pin::pin!(handle);
            respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

            assert!(matches!(
                task.await.unwrap(),
                Err(LifecycleError::StaleBinding)
            ));
            assert_eq!(fake.calls(), 0);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn non_active_phases_reject_suspend_without_write_or_cleanup() {
        for phase in [
            SessionPhase::Provisioning,
            SessionPhase::Blocked,
            SessionPhase::Deleting,
        ] {
            let anchor = anchor_in_phase(session_id(&format!("discord:reject-{phase:?}")), phase);
            let request = request_for(&anchor);
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let task = tokio::spawn(async move { coordinator.accept_suspend(&request).await });
            let mut handle = std::pin::pin!(handle);
            respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

            assert!(matches!(
                task.await.unwrap(),
                Err(LifecycleError::PhaseRejected)
            ));
            assert_eq!(fake.calls(), 0);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn suspend_anchor_cas_failure_is_not_acknowledged() {
        let anchor = anchor_in_phase(session_id("discord:cas"), SessionPhase::Ready);
        let request = request_for(&anchor);
        let fake = FakeProvisioner::returning(CleanupProgress::Pending);
        let (coordinator, handle) = coordinator(fake.clone());
        let task = tokio::spawn(async move { coordinator.accept_suspend(&request).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        let (replace, send) = handle.next_request().await.expect("anchor replace");
        assert_eq!(replace.method(), Method::PUT);
        send.send_response(conflict_response());

        assert!(matches!(
            task.await.unwrap(),
            Err(LifecycleError::Store(AnchorStoreError::Conflict { .. }))
        ));
        assert_eq!(fake.calls(), 0);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn pending_cleanup_does_not_write_anchor_state() {
        let anchor = anchor_in_phase(session_id("discord:pending"), SessionPhase::Suspending);
        let fake = FakeProvisioner::returning(CleanupProgress::Pending);
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        assert_eq!(
            task.await.unwrap().unwrap(),
            LifecycleReconcileOutcome::Pending
        );
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn valid_absence_proof_clears_pod_and_persists_suspended() {
        let anchor = anchor_in_phase(session_id("discord:complete"), SessionPhase::Suspending);
        let fake = FakeProvisioner::returning(CleanupProgress::Absent(proof_for(&anchor)));
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        let persisted = respond_replace(&mut handle).await;
        assert_eq!(persisted.phase(), SessionPhase::Suspended);
        assert_eq!(persisted.pod_uid(), None);
        assert_eq!(
            task.await.unwrap().unwrap(),
            LifecycleReconcileOutcome::Suspended
        );
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn cleanup_anchor_cas_failure_never_reports_suspended() {
        let anchor = anchor_in_phase(session_id("discord:cleanup-cas"), SessionPhase::Suspending);
        let fake = FakeProvisioner::returning(CleanupProgress::Absent(proof_for(&anchor)));
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        let (replace, send) = handle.next_request().await.expect("anchor replace");
        assert_eq!(replace.method(), Method::PUT);
        send.send_response(conflict_response());

        assert!(matches!(
            task.await.unwrap(),
            Err(LifecycleError::Store(AnchorStoreError::Conflict { .. }))
        ));
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn every_mismatched_absence_proof_is_rejected_without_state_write() {
        let anchor = anchor_in_phase(session_id("discord:proof"), SessionPhase::Suspending);
        let mismatches = [
            ComputeAbsentProof::for_test(
                session_id("discord:other"),
                anchor.incarnation_id(),
                anchor.fence().clone(),
                ANCHOR_UID,
            ),
            ComputeAbsentProof::for_test(
                anchor.session_id(),
                Uuid::from_u128(0x998),
                anchor.fence().clone(),
                ANCHOR_UID,
            ),
            ComputeAbsentProof::for_test(
                anchor.session_id(),
                anchor.incarnation_id(),
                Fence::new(2, Uuid::from_u128(0x999)).unwrap(),
                ANCHOR_UID,
            ),
            ComputeAbsentProof::for_test(
                anchor.session_id(),
                anchor.incarnation_id(),
                anchor.fence().clone(),
                "replacement-anchor-uid",
            ),
        ];

        for proof in mismatches {
            let fake = FakeProvisioner::returning(CleanupProgress::Absent(proof));
            let (coordinator, handle) = coordinator(fake.clone());
            let target = anchor.session_id();
            let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
            let mut handle = std::pin::pin!(handle);
            respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

            assert!(matches!(
                task.await.unwrap(),
                Err(LifecycleError::ProofMismatch)
            ));
            assert_eq!(fake.calls(), 1);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn blocked_partial_generation_stays_blocked_while_recorded_pod_is_cleared() {
        let anchor = blocked_anchor_with_pod(session_id("discord:blocked-with-pod"));
        let fake = FakeProvisioner::returning(CleanupProgress::Absent(proof_for(&anchor)));
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        let persisted = respond_replace(&mut handle).await;
        assert_eq!(persisted.phase(), SessionPhase::Blocked);
        assert_eq!(persisted.pod_uid(), None);
        assert_eq!(
            task.await.unwrap().unwrap(),
            LifecycleReconcileOutcome::Blocked
        );
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn blocked_anchor_without_recorded_pod_is_not_rewritten() {
        let anchor = anchor_in_phase(session_id("discord:blocked-no-pod"), SessionPhase::Blocked);
        let fake = FakeProvisioner::returning(CleanupProgress::Absent(proof_for(&anchor)));
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);

        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;
        assert_eq!(
            task.await.unwrap().unwrap(),
            LifecycleReconcileOutcome::Blocked
        );
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn invalid_reconcile_phase_never_calls_the_provisioner() {
        let anchor = anchor_in_phase(session_id("discord:ready-reconcile"), SessionPhase::Ready);
        let fake = FakeProvisioner::returning(CleanupProgress::Pending);
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);
        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

        assert!(matches!(
            task.await.unwrap(),
            Err(LifecycleError::PhaseRejected)
        ));
        assert_eq!(fake.calls(), 0);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn shared_session_locks_serialize_lifecycle_operations() {
        let anchor = anchor_in_phase(session_id("discord:serialized"), SessionPhase::Suspending);
        let request = request_for(&anchor);
        let gate = Arc::new(GateProvisioner {
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (coordinator, handle) = coordinator_with(gate.clone(), SessionLocks::new());
        let first = tokio::spawn({
            let coordinator = coordinator.clone();
            let target = anchor.session_id();
            async move { coordinator.reconcile_compute(target).await }
        });
        let mut handle = std::pin::pin!(handle);
        respond_get_anchor(&mut handle, &anchor, "rv-first").await;
        tokio::time::timeout(StdDuration::from_secs(1), gate.entered.notified())
            .await
            .expect("first lifecycle operation should enter provisioner");

        let second = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.accept_suspend(&request).await }
        });
        assert_no_request(&mut handle).await;

        gate.release.notify_one();
        assert_eq!(
            first.await.unwrap().unwrap(),
            LifecycleReconcileOutcome::Pending
        );
        respond_get_anchor(&mut handle, &anchor, "rv-second").await;
        assert!(second.await.unwrap().is_ok());
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn provisioner_failure_is_sanitized_and_never_writes_state() {
        let anchor = anchor_in_phase(session_id("discord:api-error"), SessionPhase::Suspending);
        let fake = FakeProvisioner {
            progress: Arc::new(Mutex::new(Err(GenerationProvisionerError::KubernetesApi {
                operation: ProvisionerOperation::ReconcileComputeAbsence,
            }))),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_compute(target).await });
        let mut handle = std::pin::pin!(handle);
        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

        let error = task.await.unwrap().unwrap_err();
        assert!(matches!(error, LifecycleError::Provisioner(_)));
        assert_eq!(error.to_string(), "worker compute reconciliation failed");
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }

    #[tokio::test]
    async fn durable_compute_recovery_tolerates_absent_and_phase_drift() {
        for anchor in [
            None,
            Some(anchor_in_phase(
                session_id("discord:startup-ready"),
                SessionPhase::Ready,
            )),
            Some(anchor_in_phase(
                session_id("discord:startup-suspended"),
                SessionPhase::Suspended,
            )),
        ] {
            let fake = FakeProvisioner::returning(CleanupProgress::Pending);
            let (coordinator, handle) = coordinator(fake.clone());
            let target = anchor.as_ref().map_or_else(
                || session_id("discord:startup-absent"),
                SessionAnchorV1::session_id,
            );
            let task =
                tokio::spawn(async move { coordinator.reconcile_durable_compute(target).await });
            let mut handle = std::pin::pin!(handle);
            let (_get, send) = handle.next_request().await.unwrap();
            match anchor {
                Some(anchor) => send.send_response(json_response(
                    StatusCode::OK,
                    config_map(&anchor, "rv-observed"),
                )),
                None => send.send_response(missing_response()),
            }

            assert_eq!(task.await.unwrap().unwrap(), None);
            assert_eq!(fake.calls(), 0);
            assert_no_request(&mut handle).await;
        }
    }

    #[tokio::test]
    async fn durable_compute_recovery_continues_suspending_intent() {
        let anchor = anchor_in_phase(
            session_id("discord:startup-suspending"),
            SessionPhase::Suspending,
        );
        let fake = FakeProvisioner::returning(CleanupProgress::Pending);
        let (coordinator, handle) = coordinator(fake.clone());
        let target = anchor.session_id();
        let task = tokio::spawn(async move { coordinator.reconcile_durable_compute(target).await });
        let mut handle = std::pin::pin!(handle);
        respond_get_anchor(&mut handle, &anchor, "rv-observed").await;

        assert_eq!(
            task.await.unwrap().unwrap(),
            Some(LifecycleReconcileOutcome::Pending)
        );
        assert_eq!(fake.calls(), 1);
        assert_no_request(&mut handle).await;
    }
}
