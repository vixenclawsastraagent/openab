use super::SessionLocks;
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
/// only when an activation creates a previously absent lifecycle anchor.
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
    #[error("resuming session phase {phase:?} is deferred to a later controller slice")]
    ResumeDeferred { phase: SessionPhase },
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
}

impl ActivationCoordinator {
    pub fn new(
        store: ConfigMapAnchorStore,
        locks: SessionLocks,
        profile: MvpWorkerProfile,
        provisioner: Arc<dyn GenerationProvisioner>,
    ) -> Self {
        Self {
            store,
            locks,
            profile,
            provisioner,
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
            Some(anchor) => self.prepare_existing(request, anchor).await,
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
            return self.prepare_existing(request, anchor).await;
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
            phase @ (SessionPhase::Suspended | SessionPhase::Blocked) => {
                Err(ActivationError::ResumeDeferred { phase })
            }
        }
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
