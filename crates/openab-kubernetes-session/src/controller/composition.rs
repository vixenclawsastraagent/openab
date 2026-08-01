use super::{
    ActivationCoordinator, ActivationError, ActivationPreparation, ActivityCoordinator,
    GenerationProvisioner, LifecycleCoordinator, LifecycleError, LifecycleProvisioner,
    LifecycleReconcileOutcome, RegistrationCoordinator, RegistrationError, RegistrationProvisioner,
    RegistrationRecovery, ReleaseCoordinator, ReleaseError, ReleaseOutcome, ReleaseProvisioner,
    SessionLocks,
};
use crate::identity::SessionId;
use crate::profile_config::ControllerPolicy;
use crate::resources::MvpWorkerProfile;
use crate::state::{ProfileRef, SessionPhase};
use crate::store::{AnchorStoreError, ConfigMapAnchorStore};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProfileKey {
    name: String,
    version: String,
}

impl From<&ProfileRef> for ProfileKey {
    fn from(profile: &ProfileRef) -> Self {
        Self {
            name: profile.name().to_owned(),
            version: profile.version().to_owned(),
        }
    }
}

struct ProfileCoordinators {
    activation: ActivationCoordinator,
    registration: RegistrationCoordinator,
}

/// Construction failures for the single-controller MVP composition root.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControllerCoordinatorConfigError {
    #[error("at least one resolved worker profile is required")]
    EmptyProfiles,
    #[error("a resolved worker profile revision is duplicated")]
    DuplicateProfile { profile: ProfileRef },
}

/// Per-session failures retained in a reconciliation report.
///
/// Display strings on the nested controller errors are sanitized. Keeping
/// the sources lets trusted controller logs classify the failed subsystem
/// without exposing raw chat identity or worker credentials.
#[derive(Debug, Error)]
pub enum DurableIntentError {
    #[error("the durable session profile revision is unavailable")]
    ProfileUnavailable { profile: ProfileRef },
    #[error("durable provisioning reconciliation failed")]
    Activation(#[source] ActivationError),
    #[error("durable registration reconciliation failed")]
    Registration(#[source] RegistrationError),
    #[error("durable compute reconciliation failed")]
    Lifecycle(#[source] LifecycleError),
    #[error("durable release reconciliation failed")]
    Release(#[source] ReleaseError),
}

/// Stable outcome from one durable-intent reconciliation candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurableIntentOutcome {
    /// The scheduled phase does not require startup work.
    Noop,
    /// A generation exists and must complete authenticated registration.
    AwaitingRegistration,
    /// The current generation must be recycled before it can resume.
    RecycleRequired,
    /// Non-destructive compute cleanup made one reconciliation pass.
    Compute(LifecycleReconcileOutcome),
    /// Destructive cleanup continued an already-durable `Deleting` intent.
    Release(ReleaseOutcome),
    /// A fresh read no longer matched the inventory scheduling hint.
    StaleObservation,
}

/// One deterministic per-session entry in a startup reconciliation report.
#[derive(Debug)]
pub struct DurableIntentResult {
    session_id: SessionId,
    scheduled_phase: SessionPhase,
    result: Result<DurableIntentOutcome, DurableIntentError>,
}

impl DurableIntentResult {
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn scheduled_phase(&self) -> SessionPhase {
        self.scheduled_phase
    }

    pub fn outcome(&self) -> Option<&DurableIntentOutcome> {
        self.result.as_ref().ok()
    }

    pub fn error(&self) -> Option<&DurableIntentError> {
        self.result.as_ref().err()
    }
}

/// Complete result of one sequential, bounded startup reconciliation pass.
#[derive(Debug, Default)]
pub struct DurableIntentReport {
    results: Vec<DurableIntentResult>,
}

impl DurableIntentReport {
    pub fn results(&self) -> &[DurableIntentResult] {
        &self.results
    }
}

/// Composition root for every controller operation in one deployment scope.
///
/// It creates exactly one process-local [`SessionLocks`] registry and clones
/// that registry into live request coordinators and startup reconciliation.
/// This is intentionally a single-controller MVP; Kubernetes CAS fences still
/// protect durable writes, but this lock is not a multi-replica leader lease.
pub struct ControllerCoordinators {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    profiles: BTreeMap<ProfileKey, ProfileCoordinators>,
    activity: ActivityCoordinator,
    lifecycle: LifecycleCoordinator,
    release: ReleaseCoordinator,
}

impl ControllerCoordinators {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: ConfigMapAnchorStore,
        profiles: impl IntoIterator<Item = MvpWorkerProfile>,
        policy: ControllerPolicy,
        generation_provisioner: Arc<dyn GenerationProvisioner>,
        lifecycle_provisioner: Arc<dyn LifecycleProvisioner>,
        registration_provisioner: Arc<dyn RegistrationProvisioner>,
        release_provisioner: Arc<dyn ReleaseProvisioner>,
    ) -> Result<Self, ControllerCoordinatorConfigError> {
        let locks = SessionLocks::new();
        let mut profile_coordinators = BTreeMap::new();
        for profile in profiles {
            let profile_ref = profile.profile().clone();
            let key = ProfileKey::from(&profile_ref);
            let coordinators = ProfileCoordinators {
                activation: ActivationCoordinator::new(
                    store.clone(),
                    locks.clone(),
                    profile.clone(),
                    Arc::clone(&generation_provisioner),
                    Arc::clone(&lifecycle_provisioner),
                ),
                registration: RegistrationCoordinator::new(
                    store.clone(),
                    locks.clone(),
                    profile,
                    Arc::clone(&registration_provisioner),
                ),
            };
            if profile_coordinators.insert(key, coordinators).is_some() {
                return Err(ControllerCoordinatorConfigError::DuplicateProfile {
                    profile: profile_ref,
                });
            }
        }
        if profile_coordinators.is_empty() {
            return Err(ControllerCoordinatorConfigError::EmptyProfiles);
        }

        let activity = ActivityCoordinator::new(store.clone(), locks.clone(), policy);
        let lifecycle = LifecycleCoordinator::new(
            store.clone(),
            locks.clone(),
            Arc::clone(&lifecycle_provisioner),
        );
        let release = ReleaseCoordinator::new(store.clone(), locks.clone(), release_provisioner);
        Ok(Self {
            store,
            locks,
            profiles: profile_coordinators,
            activity,
            lifecycle,
            release,
        })
    }

    pub fn activation(&self, profile: &ProfileRef) -> Option<&ActivationCoordinator> {
        self.profiles
            .get(&ProfileKey::from(profile))
            .map(|coordinators| &coordinators.activation)
    }

    pub fn registration(&self, profile: &ProfileRef) -> Option<&RegistrationCoordinator> {
        self.profiles
            .get(&ProfileKey::from(profile))
            .map(|coordinators| &coordinators.registration)
    }

    pub fn lifecycle(&self) -> &LifecycleCoordinator {
        &self.lifecycle
    }

    pub fn activity(&self) -> &ActivityCoordinator {
        &self.activity
    }

    pub fn release(&self) -> &ReleaseCoordinator {
        &self.release
    }

    /// Continue only intents that were already durable before this pass.
    ///
    /// The inventory is completed before any session is processed. Each
    /// candidate then enters a coordinator that owns the shared session lock
    /// and performs a fresh GET. Per-session failures are recorded and do not
    /// cancel later candidates; sequential execution is the MVP's explicit
    /// concurrency bound of one.
    pub async fn reconcile_durable_intents(&self) -> Result<DurableIntentReport, AnchorStoreError> {
        let inventory = self.store.list_inventory().await?;
        let mut results = Vec::with_capacity(inventory.len());
        for scheduled in inventory {
            let session_id = scheduled.state().session_id();
            let scheduled_phase = scheduled.state().phase();
            let result = self.reconcile_candidate(session_id, scheduled_phase).await;
            results.push(DurableIntentResult {
                session_id,
                scheduled_phase,
                result,
            });
        }
        Ok(DurableIntentReport { results })
    }

    async fn reconcile_candidate(
        &self,
        session_id: SessionId,
        scheduled_phase: SessionPhase,
    ) -> Result<DurableIntentOutcome, DurableIntentError> {
        match scheduled_phase {
            SessionPhase::Provisioning => self.reconcile_provisioning(session_id).await,
            SessionPhase::Suspending | SessionPhase::Blocked => self
                .lifecycle
                .reconcile_durable_compute(session_id)
                .await
                .map(|outcome| {
                    outcome.map_or(
                        DurableIntentOutcome::StaleObservation,
                        DurableIntentOutcome::Compute,
                    )
                })
                .map_err(DurableIntentError::Lifecycle),
            SessionPhase::Deleting => self
                .release
                .reconcile_deleting(session_id)
                .await
                .map(|outcome| {
                    outcome.map_or(
                        DurableIntentOutcome::StaleObservation,
                        DurableIntentOutcome::Release,
                    )
                })
                .map_err(DurableIntentError::Release),
            SessionPhase::Ready | SessionPhase::Busy | SessionPhase::Suspended => {
                Ok(DurableIntentOutcome::Noop)
            }
        }
    }

    async fn reconcile_provisioning(
        &self,
        session_id: SessionId,
    ) -> Result<DurableIntentOutcome, DurableIntentError> {
        // Resolve the profile and pod-presence route from a fresh observation
        // under the same registry used by every live coordinator. The lock is
        // released before invoking the selected self-locking entrypoint.
        let (profile, pod_observed) = {
            let _guard = self.locks.lock(session_id).await;
            let Some(observed) = self
                .store
                .get(session_id)
                .await
                .map_err(ActivationError::Store)
                .map_err(DurableIntentError::Activation)?
            else {
                return Ok(DurableIntentOutcome::StaleObservation);
            };
            if observed.state().phase() != SessionPhase::Provisioning {
                return Ok(DurableIntentOutcome::Noop);
            }
            (
                observed.state().profile().clone(),
                observed.state().pod_uid().is_some(),
            )
        };

        let coordinators = self
            .profiles
            .get(&ProfileKey::from(&profile))
            .ok_or(DurableIntentError::ProfileUnavailable { profile })?;
        if pod_observed {
            return match coordinators
                .registration
                .recover_incomplete(session_id)
                .await
            {
                Ok(outcome) => Ok(match outcome {
                    RegistrationRecovery::AwaitingRegistration => {
                        DurableIntentOutcome::AwaitingRegistration
                    }
                    RegistrationRecovery::RecycleRequired { .. } => {
                        DurableIntentOutcome::RecycleRequired
                    }
                    RegistrationRecovery::NotApplicable { .. } => {
                        DurableIntentOutcome::StaleObservation
                    }
                }),
                Err(RegistrationError::AnchorNotFound) => {
                    Ok(DurableIntentOutcome::StaleObservation)
                }
                Err(error) => Err(DurableIntentError::Registration(error)),
            };
        }

        coordinators
            .activation
            .reconcile_provisioning(session_id)
            .await
            .map(|preparation| match preparation {
                Some(ActivationPreparation::AwaitingRegistration { .. }) => {
                    DurableIntentOutcome::AwaitingRegistration
                }
                Some(ActivationPreparation::MappingAbsent(_)) | None => {
                    DurableIntentOutcome::StaleObservation
                }
            })
            .map_err(DurableIntentError::Activation)
    }
}
