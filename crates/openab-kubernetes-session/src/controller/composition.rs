use super::{
    ActivationCoordinator, ActivationError, ActivationPreparation, ActivityCoordinator,
    GenerationProvisioner, LifecycleCoordinator, LifecycleError, LifecycleProvisioner,
    LifecycleReconcileOutcome, RegistrationCoordinator, RegistrationError, RegistrationProvisioner,
    RegistrationRecovery, ReleaseCoordinator, ReleaseError, ReleaseOutcome, ReleaseProvisioner,
    ScopeCapacityAdmission, SessionLocks,
};
use crate::identity::SessionId;
use crate::profile_config::ControllerPolicy;
use crate::resources::MvpWorkerProfile;
use crate::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use crate::store::{AnchorStoreError, ConfigMapAnchorStore};
use chrono::{DateTime, Utc};
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

/// Stable outcome from one lifecycle-deadline scan candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleDeadlineOutcome {
    /// The scheduled anchor does not require deadline maintenance.
    Noop,
    /// A fresh expired `Ready` anchor is durably `Suspending`.
    ComputeSuspensionAccepted,
    /// A validated inventory snapshot observed an expired storage deadline.
    /// This stale-tolerant telemetry never authorizes deletion or release.
    StorageDeadlineExpiredObservation,
    /// A fresh compute observation no longer matched the scheduling hint.
    StaleObservation,
}

/// One deterministic per-session entry in a lifecycle-deadline report.
#[derive(Debug)]
pub struct LifecycleDeadlineResult {
    session_id: SessionId,
    scheduled_phase: SessionPhase,
    result: Result<LifecycleDeadlineOutcome, LifecycleError>,
}

impl LifecycleDeadlineResult {
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn scheduled_phase(&self) -> SessionPhase {
        self.scheduled_phase
    }

    pub fn outcome(&self) -> Option<&LifecycleDeadlineOutcome> {
        self.result.as_ref().ok()
    }

    pub fn error(&self) -> Option<&LifecycleError> {
        self.result.as_ref().err()
    }
}

/// Complete result of one sequential, bounded lifecycle-deadline scan.
#[derive(Debug, Default)]
pub struct LifecycleDeadlineReport {
    results: Vec<LifecycleDeadlineResult>,
}

/// Stable outcome from one startup orphan-containment candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupOrphanOutcome {
    /// The scheduled phase cannot own a registered live relay lane.
    Noop,
    /// A fresh active anchor is durably `Blocked` for safe recycling.
    ContainmentAccepted,
    /// The active inventory observation became stale before containment.
    StaleObservation,
}

/// Profile status observed for one anchor in the startup inventory snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupProfileRevisionStatus {
    /// The exact `(name, version)` coordinator is loaded.
    Loaded,
    /// Activation and registration fail closed until restore or release.
    Unavailable,
    /// Terminal deletion is profile-independent and remains reclaimable.
    NotRequiredForTerminalCleanup,
}

/// One deterministic per-session entry in a startup orphan report.
#[derive(Debug)]
pub struct StartupOrphanResult {
    session_id: SessionId,
    scheduled_phase: SessionPhase,
    scheduled_profile_revision_status: StartupProfileRevisionStatus,
    result: Result<StartupOrphanOutcome, LifecycleError>,
}

impl StartupOrphanResult {
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn scheduled_phase(&self) -> SessionPhase {
        self.scheduled_phase
    }

    /// Return profile status from the same LIST snapshot that scheduled this entry.
    ///
    /// `NotRequiredForTerminalCleanup` means deletion remains reclaimable
    /// without restoring a retired profile revision.
    pub fn scheduled_profile_revision_status(&self) -> StartupProfileRevisionStatus {
        self.scheduled_profile_revision_status
    }

    pub fn outcome(&self) -> Option<&StartupOrphanOutcome> {
        self.result.as_ref().ok()
    }

    pub fn error(&self) -> Option<&LifecycleError> {
        self.result.as_ref().err()
    }
}

/// Complete result of one sequential startup orphan-containment pass.
#[derive(Debug, Default)]
pub struct StartupOrphanReport {
    results: Vec<StartupOrphanResult>,
}

impl StartupOrphanReport {
    pub fn results(&self) -> &[StartupOrphanResult] {
        &self.results
    }

    /// Whether every scheduled active candidate was either durably contained
    /// or proven stale by a fresh locked observation.
    pub fn containment_complete(&self) -> bool {
        self.results.iter().all(|result| result.result.is_ok())
    }

    /// Count non-deleting sessions observed with an unavailable exact revision.
    ///
    /// This advisory count must not become a readiness gate: cleanup must
    /// continue while an operator restores a revision or releases the session.
    pub fn unavailable_profile_session_count(&self) -> usize {
        self.results
            .iter()
            .filter(|result| {
                result.scheduled_profile_revision_status
                    == StartupProfileRevisionStatus::Unavailable
            })
            .count()
    }
}

impl LifecycleDeadlineReport {
    pub fn results(&self) -> &[LifecycleDeadlineResult] {
        &self.results
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScheduledDeadlineAction {
    Noop,
    SuspendCompute,
    ReportStorageDeadlineExpiry,
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
    policy: ControllerPolicy,
    activity: ActivityCoordinator,
    lifecycle: LifecycleCoordinator,
    release: ReleaseCoordinator,
}

pub(super) enum RegistrationDispatchError {
    ProfileUnavailable,
    Registration(RegistrationError),
}

pub(super) enum ActivationDispatchError {
    ProfileUnavailable,
    Activation(ActivationError),
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
        let capacity = ScopeCapacityAdmission::from_policy(&policy);
        let mut profile_coordinators = BTreeMap::new();
        for profile in profiles {
            let profile_ref = profile.profile().clone();
            let key = ProfileKey::from(&profile_ref);
            let coordinators = ProfileCoordinators {
                activation: ActivationCoordinator::new(
                    store.clone(),
                    locks.clone(),
                    capacity.clone(),
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
            policy,
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

    pub(super) fn policy(&self) -> ControllerPolicy {
        self.policy
    }

    pub(super) fn scope_id(&self) -> crate::identity::ScopeId {
        self.store.scope_id()
    }

    /// Route a new session to its configured current profile and an existing
    /// session to the exact revision pinned in its durable anchor.
    ///
    /// As with registration routing, the preflight lock is released before
    /// entering the selected self-locking coordinator. Its fresh read is the
    /// mutation authority and safely rejects any intervening replacement.
    pub(super) async fn prepare_activation(
        &self,
        request: &crate::wire::ActivationRequestV1,
        timing: super::ActivationTiming,
        current_profile: &ProfileRef,
    ) -> Result<ActivationPreparation, ActivationDispatchError> {
        let routed_profile = {
            let _guard = self.locks.lock(request.session_id()).await;
            self.store
                .get(request.session_id())
                .await
                .map_err(ActivationError::Store)
                .map_err(ActivationDispatchError::Activation)?
                .map_or_else(
                    || current_profile.clone(),
                    |anchor| anchor.state().profile().clone(),
                )
        };
        let coordinator = self
            .activation(&routed_profile)
            .ok_or(ActivationDispatchError::ProfileUnavailable)?;
        coordinator
            .prepare(request, timing)
            .await
            .map_err(ActivationDispatchError::Activation)
    }

    /// Route a registration by the exact profile revision retained in the
    /// durable anchor, never by current configuration or worker input.
    ///
    /// The routing read and the selected registration coordinator share this
    /// root's lock registry. The first lock must be released before calling
    /// the self-locking coordinator; its fresh read then authoritatively
    /// revalidates the complete binding and bootstrap credential.
    pub(super) async fn register_worker(
        &self,
        registration: crate::wire::WorkerRegistrationV1,
        auth: super::WorkerBootstrapAuth,
    ) -> Result<super::RegisteredWorker, RegistrationDispatchError> {
        let session_id = registration.session_id();
        let profile = {
            let _guard = self.locks.lock(session_id).await;
            let observed = self
                .store
                .get(session_id)
                .await
                .map_err(RegistrationError::Store)
                .map_err(RegistrationDispatchError::Registration)?
                .ok_or(RegistrationError::AnchorNotFound)
                .map_err(RegistrationDispatchError::Registration)?;
            observed.state().profile().clone()
        };
        let coordinator = self
            .registration(&profile)
            .ok_or(RegistrationDispatchError::ProfileUnavailable)?;
        coordinator
            .register(session_id, registration, auth)
            .await
            .map_err(RegistrationDispatchError::Registration)
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

    /// Scan absolute lifecycle deadlines using one controller-owned cutoff.
    ///
    /// Inventory is only a scheduling hint for compute mutation. Every
    /// expired `Ready` candidate takes the shared session lock, performs a
    /// fresh GET, rechecks its deadline, and persists one CAS transition to
    /// `Suspending`. Expired storage is reported only for an already
    /// `Suspended` snapshot as stale-tolerant telemetry. The observation is
    /// not deletion or release authority, and this method never invokes
    /// release or a Kubernetes provisioner.
    pub async fn scan_lifecycle_deadlines(
        &self,
    ) -> Result<LifecycleDeadlineReport, AnchorStoreError> {
        let cutoff = Utc::now();
        let inventory = self.store.list_inventory().await?;
        let mut results = Vec::with_capacity(inventory.len());
        for scheduled in inventory {
            let session_id = scheduled.state().session_id();
            let scheduled_phase = scheduled.state().phase();
            let result = match scheduled_deadline_action(scheduled.state(), cutoff) {
                ScheduledDeadlineAction::Noop => Ok(LifecycleDeadlineOutcome::Noop),
                ScheduledDeadlineAction::ReportStorageDeadlineExpiry => {
                    Ok(LifecycleDeadlineOutcome::StorageDeadlineExpiredObservation)
                }
                ScheduledDeadlineAction::SuspendCompute => self
                    .lifecycle
                    .accept_expired_ready(session_id, cutoff)
                    .await
                    .map(|accepted| {
                        if accepted {
                            LifecycleDeadlineOutcome::ComputeSuspensionAccepted
                        } else {
                            LifecycleDeadlineOutcome::StaleObservation
                        }
                    }),
            };
            results.push(LifecycleDeadlineResult {
                session_id,
                scheduled_phase,
                result,
            });
        }
        Ok(LifecycleDeadlineReport { results })
    }

    /// Contain every worker that could have been registered before this
    /// controller process started.
    ///
    /// This must run while relay traffic admission and readiness are closed:
    /// process restart discards all authenticated in-memory lanes, and a
    /// consumed bootstrap credential cannot authorize transparent worker
    /// reconnection. Each active inventory candidate is re-read under the
    /// shared session lock before its non-destructive intent is persisted.
    /// Per-session failures remain visible in the report and must keep the
    /// executable from declaring startup recovery complete.
    pub async fn quiesce_startup_orphans(&self) -> Result<StartupOrphanReport, AnchorStoreError> {
        let inventory = self.store.list_inventory().await?;
        let mut results = Vec::with_capacity(inventory.len());
        for scheduled in inventory {
            let session_id = scheduled.state().session_id();
            let scheduled_phase = scheduled.state().phase();
            let scheduled_profile_revision_status = if scheduled_phase == SessionPhase::Deleting {
                StartupProfileRevisionStatus::NotRequiredForTerminalCleanup
            } else if self
                .profiles
                .contains_key(&ProfileKey::from(scheduled.state().profile()))
            {
                StartupProfileRevisionStatus::Loaded
            } else {
                StartupProfileRevisionStatus::Unavailable
            };
            let result = if matches!(
                scheduled_phase,
                SessionPhase::Provisioning | SessionPhase::Ready | SessionPhase::Busy
            ) {
                self.lifecycle
                    .accept_startup_orphan(session_id)
                    .await
                    .map(|accepted| {
                        if accepted {
                            StartupOrphanOutcome::ContainmentAccepted
                        } else {
                            StartupOrphanOutcome::StaleObservation
                        }
                    })
            } else {
                Ok(StartupOrphanOutcome::Noop)
            };
            results.push(StartupOrphanResult {
                session_id,
                scheduled_phase,
                scheduled_profile_revision_status,
                result,
            });
        }
        Ok(StartupOrphanReport { results })
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

fn scheduled_deadline_action(
    anchor: &SessionAnchorV1,
    cutoff: DateTime<Utc>,
) -> ScheduledDeadlineAction {
    match anchor.phase() {
        SessionPhase::Ready if anchor.compute_deadline_at() <= cutoff => {
            ScheduledDeadlineAction::SuspendCompute
        }
        SessionPhase::Suspended if anchor.storage_deadline_at() <= cutoff => {
            ScheduledDeadlineAction::ReportStorageDeadlineExpiry
        }
        SessionPhase::Provisioning
        | SessionPhase::Ready
        | SessionPhase::Busy
        | SessionPhase::Suspending
        | SessionPhase::Suspended
        | SessionPhase::Deleting
        | SessionPhase::Blocked => ScheduledDeadlineAction::Noop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{ScopeId, SessionId};
    use chrono::{Duration, TimeZone};
    use uuid::Uuid;

    fn anchor(
        phase: SessionPhase,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> SessionAnchorV1 {
        let mut anchor = SessionAnchorV1::new(
            SessionId::derive("scope", "discord:deadline-boundary"),
            ScopeId::derive("scope"),
            ProfileRef::new("codex-strict", "2026-08-01").unwrap(),
            Uuid::from_u128(0x100),
            Uuid::from_u128(0x200),
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        )
        .unwrap();
        let fence = anchor.fence().clone();
        anchor.observe_pod(&fence, "pod-uid").unwrap();
        anchor.transition(&fence, SessionPhase::Ready).unwrap();
        match phase {
            SessionPhase::Ready => {}
            SessionPhase::Busy => anchor.transition(&fence, SessionPhase::Busy).unwrap(),
            SessionPhase::Suspended => {
                anchor.transition(&fence, SessionPhase::Suspending).unwrap();
                anchor.confirm_pod_deleted(&fence, "pod-uid").unwrap();
                anchor.transition(&fence, SessionPhase::Suspended).unwrap();
            }
            other => panic!("unsupported test phase: {other:?}"),
        }
        anchor
    }

    #[test]
    fn deadline_classifier_includes_the_boundary_and_never_treats_busy_as_idle() {
        let cutoff = Utc.with_ymd_and_hms(2026, 8, 1, 12, 0, 0).unwrap();
        let last_activity_at = cutoff - Duration::hours(2);

        for compute_deadline_at in [cutoff - Duration::nanoseconds(1), cutoff] {
            let ready = anchor(
                SessionPhase::Ready,
                last_activity_at,
                compute_deadline_at,
                cutoff + Duration::hours(72),
            );
            assert_eq!(
                scheduled_deadline_action(&ready, cutoff),
                ScheduledDeadlineAction::SuspendCompute
            );
        }

        let future = anchor(
            SessionPhase::Ready,
            last_activity_at,
            cutoff + Duration::nanoseconds(1),
            cutoff + Duration::hours(72),
        );
        assert_eq!(
            scheduled_deadline_action(&future, cutoff),
            ScheduledDeadlineAction::Noop
        );

        let busy = anchor(
            SessionPhase::Busy,
            last_activity_at,
            cutoff - Duration::nanoseconds(1),
            cutoff + Duration::hours(72),
        );
        assert_eq!(
            scheduled_deadline_action(&busy, cutoff),
            ScheduledDeadlineAction::Noop
        );

        let suspended = anchor(
            SessionPhase::Suspended,
            last_activity_at,
            cutoff - Duration::hours(1),
            cutoff,
        );
        assert_eq!(
            scheduled_deadline_action(&suspended, cutoff),
            ScheduledDeadlineAction::ReportStorageDeadlineExpiry
        );
    }
}
