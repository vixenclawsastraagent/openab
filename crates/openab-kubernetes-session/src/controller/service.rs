use super::composition::{ActivationDispatchError, RegistrationDispatchError};
use super::{
    ActivationError, ActivationPreparation, ActivationTiming, ActivityError, ActivityEvent,
    ActivityOutcome, ActivityTurnId, ControllerCoordinators, DurableIntentReport,
    GenerationProvisionerError, LifecycleDeadlineReport, LifecycleError, RegisteredWorker,
    RegistrationError, ReleaseError, ReleaseOutcome, WorkerBootstrapAuth,
};
use crate::bridge::{LifecycleKind, SessionBinding};
use crate::profile_config::ControllerPolicy;
use crate::state::ProfileRef;
use crate::store::AnchorStoreError;
use crate::wire::{ActivationRequestV1, FatalCode, LifecycleRequestV1, WorkerRegistrationV1};
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;
use thiserror::Error;

/// Invalid trusted-controller wiring detected before serving traffic.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControllerServiceConfigError {
    #[error("at least one current worker profile is required")]
    EmptyCurrentProfiles,
    #[error("a current worker profile name is duplicated")]
    DuplicateCurrentProfileName { profile_name: String },
    #[error("a current worker profile has no exact coordinator")]
    MissingCoordinator { profile: ProfileRef },
}

/// Stable result of one request-facing lifecycle operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleServiceOutcome {
    /// The non-destructive suspend intent is durable.
    SuspendAccepted,
    /// Destructive release is durable but cleanup or finalization continues.
    ReleasePending,
    /// The anchor and every managed child are authoritatively absent.
    Released,
}

/// Sanitized failures exposed to a controller transport.
///
/// Nested sources remain available to trusted logs, while each Display string
/// is static and [`Self::fatal_code`] is the only wire-level classification.
#[derive(Debug, Error)]
pub enum ControllerServiceError {
    #[error("the request does not belong to this controller scope")]
    ScopeMismatch,
    #[error("the requested worker profile is unavailable")]
    ProfileUnavailable,
    #[error("controller-owned activation timing cannot be represented")]
    TimingOverflow,
    #[error("session activation failed")]
    Activation(#[source] ActivationError),
    #[error("worker registration failed")]
    Registration(#[source] RegistrationError),
    #[error("session lifecycle operation failed")]
    Lifecycle(#[source] LifecycleError),
    #[error("session release failed")]
    Release(#[source] ReleaseError),
    #[error("trusted activity persistence failed")]
    Activity(#[source] ActivityError),
    #[error("session inventory persistence failed")]
    Store(#[source] AnchorStoreError),
}

impl ControllerServiceError {
    /// Map typed controller failures to the closed, detail-free wire contract.
    pub fn fatal_code(&self) -> FatalCode {
        match self {
            Self::ScopeMismatch => FatalCode::Unauthorized,
            Self::ProfileUnavailable => FatalCode::Unavailable,
            Self::TimingOverflow => FatalCode::Internal,
            Self::Activation(error) => activation_fatal_code(error),
            Self::Registration(error) => error.fatal_code(),
            Self::Lifecycle(error) => lifecycle_fatal_code(error),
            Self::Release(error) => release_fatal_code(error),
            Self::Activity(error) => activity_fatal_code(error),
            Self::Store(_) => FatalCode::Unavailable,
        }
    }
}

/// Transport-neutral request facade for the single-controller MVP.
///
/// It owns current-profile selection and controller time. Socket framing,
/// authentication headers, rendezvous, and process I/O remain outside this
/// layer. Historical exact profile revisions may remain in the composition
/// root for durable sessions, while activation exposes one current revision
/// per profile name.
pub struct ControllerService {
    coordinators: ControllerCoordinators,
    current_profiles: BTreeMap<String, ProfileRef>,
}

impl ControllerService {
    pub fn new(
        coordinators: ControllerCoordinators,
        current_profiles: impl IntoIterator<Item = ProfileRef>,
    ) -> Result<Self, ControllerServiceConfigError> {
        let mut indexed = BTreeMap::new();
        for profile in current_profiles {
            if coordinators.activation(&profile).is_none() {
                return Err(ControllerServiceConfigError::MissingCoordinator { profile });
            }
            let profile_name = profile.name().to_owned();
            if indexed.insert(profile_name.clone(), profile).is_some() {
                return Err(ControllerServiceConfigError::DuplicateCurrentProfileName {
                    profile_name,
                });
            }
        }
        if indexed.is_empty() {
            return Err(ControllerServiceConfigError::EmptyCurrentProfiles);
        }
        Ok(Self {
            coordinators,
            current_profiles: indexed,
        })
    }

    /// Prepare an activation using the current trusted profile for new state,
    /// the durable pinned revision for existing state, and policy-derived
    /// controller time.
    pub async fn activate(
        &self,
        request: &ActivationRequestV1,
    ) -> Result<ActivationPreparation, ControllerServiceError> {
        if request.scope_id() != self.coordinators.scope_id() {
            return Err(ControllerServiceError::ScopeMismatch);
        }
        let profile = self
            .current_profiles
            .get(request.requested_profile_name())
            .ok_or(ControllerServiceError::ProfileUnavailable)?;
        let timing = activation_timing(self.coordinators.policy())?;
        match self
            .coordinators
            .prepare_activation(request, timing, profile)
            .await
        {
            Ok(preparation) => Ok(preparation),
            Err(ActivationDispatchError::ProfileUnavailable) => {
                Err(ControllerServiceError::ProfileUnavailable)
            }
            Err(ActivationDispatchError::Activation(error)) => {
                Err(ControllerServiceError::Activation(error))
            }
        }
    }

    /// Authenticate and accept a worker using the durable anchor's exact
    /// profile revision. The registration session ID is only a routing hint.
    pub async fn register(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RegisteredWorker, ControllerServiceError> {
        if registration.scope_id() != self.coordinators.scope_id() {
            return Err(ControllerServiceError::ScopeMismatch);
        }
        match self.coordinators.register_worker(registration, auth).await {
            Ok(worker) => Ok(worker),
            Err(RegistrationDispatchError::ProfileUnavailable) => {
                Err(ControllerServiceError::ProfileUnavailable)
            }
            Err(RegistrationDispatchError::Registration(error)) => {
                Err(ControllerServiceError::Registration(error))
            }
        }
    }

    /// Persist a non-destructive suspend or drive one explicit release pass.
    pub async fn lifecycle(
        &self,
        request: &LifecycleRequestV1,
    ) -> Result<LifecycleServiceOutcome, ControllerServiceError> {
        match request.kind() {
            LifecycleKind::Suspend => {
                self.coordinators
                    .lifecycle()
                    .accept_suspend(request)
                    .await
                    .map_err(ControllerServiceError::Lifecycle)?;
                Ok(LifecycleServiceOutcome::SuspendAccepted)
            }
            LifecycleKind::Release => self
                .coordinators
                .release()
                .release(request)
                .await
                .map(|outcome| match outcome {
                    ReleaseOutcome::Pending => LifecycleServiceOutcome::ReleasePending,
                    ReleaseOutcome::Released => LifecycleServiceOutcome::Released,
                })
                .map_err(ControllerServiceError::Release),
        }
    }

    /// Record one relay-authenticated prompt event.
    pub async fn record_activity(
        &self,
        binding: &SessionBinding,
        turn_id: ActivityTurnId,
        event: ActivityEvent,
    ) -> Result<ActivityOutcome, ControllerServiceError> {
        self.coordinators
            .activity()
            .record(binding, turn_id, event)
            .await
            .map_err(ControllerServiceError::Activity)
    }

    /// Continue only intents already made durable before this pass.
    pub async fn reconcile_durable_intents(
        &self,
    ) -> Result<DurableIntentReport, ControllerServiceError> {
        self.coordinators
            .reconcile_durable_intents()
            .await
            .map_err(ControllerServiceError::Store)
    }

    /// Scan controller-owned compute and advisory storage deadlines.
    pub async fn scan_lifecycle_deadlines(
        &self,
    ) -> Result<LifecycleDeadlineReport, ControllerServiceError> {
        self.coordinators
            .scan_lifecycle_deadlines()
            .await
            .map_err(ControllerServiceError::Store)
    }
}

fn activation_timing(policy: ControllerPolicy) -> Result<ActivationTiming, ControllerServiceError> {
    let now = Utc::now();
    Ok(ActivationTiming::new(
        now,
        checked_deadline(now, policy.compute_idle_ttl())?,
        checked_deadline(now, policy.storage_retention_ttl())?,
    ))
}

fn checked_deadline(
    observed_at: DateTime<Utc>,
    ttl: std::time::Duration,
) -> Result<DateTime<Utc>, ControllerServiceError> {
    let ttl = Duration::from_std(ttl).map_err(|_| ControllerServiceError::TimingOverflow)?;
    observed_at
        .checked_add_signed(ttl)
        .ok_or(ControllerServiceError::TimingOverflow)
}

fn activation_fatal_code(error: &ActivationError) -> FatalCode {
    match error {
        ActivationError::ScopeMismatch => FatalCode::Unauthorized,
        ActivationError::UnexpectedExistingAnchor
        | ActivationError::ProvisioningAttemptMismatch
        | ActivationError::AlreadyActive { .. } => FatalCode::StaleBinding,
        ActivationError::AnchorProfileMismatch
        | ActivationError::LifecycleInProgress { .. }
        | ActivationError::ResumeCleanupPending
        | ActivationError::ResumeCleanupRequired
        | ActivationError::CapacityExhausted
        | ActivationError::Store(_) => FatalCode::Unavailable,
        ActivationError::RequestedProfileMismatch
        | ActivationError::ResumeProofMismatch
        | ActivationError::State(_)
        | ActivationError::Binding(_)
        | ActivationError::Wire(_) => FatalCode::Internal,
        ActivationError::Provisioner(error) => activation_generation_fatal_code(error),
    }
}

fn activation_generation_fatal_code(error: &GenerationProvisionerError) -> FatalCode {
    match error {
        GenerationProvisionerError::BootstrapCredentialConsumedOrMissing => FatalCode::StaleBinding,
        GenerationProvisionerError::ChildrenPresent
        | GenerationProvisionerError::ChildrenAmbiguous
        | GenerationProvisionerError::KubernetesApi { .. } => FatalCode::Unavailable,
        GenerationProvisionerError::InvalidGeneration
        | GenerationProvisionerError::ResourceRejected { .. }
        | GenerationProvisionerError::InvalidBootstrapToken
        | GenerationProvisionerError::RandomnessUnavailable
        | GenerationProvisionerError::InvalidPodUid
        | GenerationProvisionerError::InvalidCleanupPhase { .. } => FatalCode::Internal,
    }
}

fn lifecycle_fatal_code(error: &LifecycleError) -> FatalCode {
    match error {
        LifecycleError::ScopeMismatch => FatalCode::Unauthorized,
        LifecycleError::AnchorNotFound
        | LifecycleError::StaleBinding
        | LifecycleError::PhaseRejected => FatalCode::StaleBinding,
        LifecycleError::Store(_) => FatalCode::Unavailable,
        LifecycleError::Provisioner(error) => generation_fatal_code(error),
        LifecycleError::UnsupportedKind
        | LifecycleError::InvalidAnchor
        | LifecycleError::ProofMismatch => FatalCode::Internal,
    }
}

fn release_fatal_code(error: &ReleaseError) -> FatalCode {
    match error {
        ReleaseError::ScopeMismatch => FatalCode::Unauthorized,
        ReleaseError::StaleBinding => FatalCode::StaleBinding,
        ReleaseError::Store(_) => FatalCode::Unavailable,
        ReleaseError::Provisioner(error) => generation_fatal_code(error),
        ReleaseError::UnsupportedKind
        | ReleaseError::InvalidAnchor
        | ReleaseError::ProofMismatch => FatalCode::Internal,
    }
}

fn activity_fatal_code(error: &ActivityError) -> FatalCode {
    match error {
        ActivityError::ScopeMismatch => FatalCode::Unauthorized,
        ActivityError::AnchorNotFound
        | ActivityError::StaleBinding
        | ActivityError::PhaseRejected
        | ActivityError::TurnMismatch => FatalCode::StaleBinding,
        ActivityError::Store(_) => FatalCode::Unavailable,
        ActivityError::TimingOverflow | ActivityError::InvalidAnchor => FatalCode::Internal,
    }
}

fn generation_fatal_code(error: &GenerationProvisionerError) -> FatalCode {
    match error {
        GenerationProvisionerError::ChildrenPresent
        | GenerationProvisionerError::ChildrenAmbiguous
        | GenerationProvisionerError::KubernetesApi { .. } => FatalCode::Unavailable,
        GenerationProvisionerError::InvalidGeneration
        | GenerationProvisionerError::ResourceRejected { .. }
        | GenerationProvisionerError::InvalidBootstrapToken
        | GenerationProvisionerError::BootstrapCredentialConsumedOrMissing
        | GenerationProvisionerError::RandomnessUnavailable
        | GenerationProvisionerError::InvalidPodUid
        | GenerationProvisionerError::InvalidCleanupPhase { .. } => FatalCode::Internal,
    }
}
