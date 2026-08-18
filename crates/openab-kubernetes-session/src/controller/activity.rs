use super::SessionLocks;
use crate::bridge::SessionBinding;
use crate::profile_config::ControllerPolicy;
use crate::state::SessionPhase;
use crate::store::{AnchorStoreError, ConfigMapAnchorStore, StoredAnchor};
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use uuid::Uuid;

/// Controller-generated durable correlation for exactly one prompt turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivityTurnId(Uuid);

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ActivityTurnIdError {
    #[error("activity turn identifier must not be nil")]
    Nil,
}

impl ActivityTurnId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Reconstruct an ID retained by the trusted relay for an exact retry.
    /// Worker or ACP payload values must never be promoted through this API.
    pub fn from_uuid(value: Uuid) -> Result<Self, ActivityTurnIdError> {
        if value.is_nil() {
            return Err(ActivityTurnIdError::Nil);
        }
        Ok(Self(value))
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Trusted broker activity that changes the durable compute-idle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityEvent {
    PromptStarted,
    PromptFinished,
}

/// Result of recording one trusted activity event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityOutcome {
    /// The event and its policy-derived deadlines were durably recorded.
    Recorded,
    /// An exact retry was already durably recorded.
    AlreadyRecorded,
    /// This action is older than the durable session activity.
    Stale,
}

/// Sanitized failures from trusted activity persistence.
#[derive(Debug, Error)]
pub enum ActivityError {
    #[error("the activity request does not belong to this controller scope")]
    ScopeMismatch,
    #[error("the target activity anchor was not found")]
    AnchorNotFound,
    #[error("the activity binding is stale or invalid")]
    StaleBinding,
    #[error("the session phase does not accept this activity event")]
    PhaseRejected,
    #[error("the activity turn does not match the durable prompt")]
    TurnMismatch,
    #[error("the activity deadline cannot be represented")]
    TimingOverflow,
    #[error("the durable activity anchor is invalid")]
    InvalidAnchor,
    #[error("activity anchor persistence failed")]
    Store(#[source] AnchorStoreError),
}

impl From<AnchorStoreError> for ActivityError {
    fn from(error: AnchorStoreError) -> Self {
        Self::Store(error)
    }
}

/// Persists broker-observed prompt activity for one controller scope.
///
/// Callers must retain the controller-generated turn ID across delivery
/// retries. The full generation binding and durable turn correlation prevent
/// another worker, an older generation, or a stale terminal event from
/// changing the current prompt lease. Deadline values are derived here from
/// the trusted controller policy and cannot be supplied by the worker.
///
/// The anchor retains only the latest turn. The trusted relay must serialize
/// one turn per session, persist `PromptStarted` before forwarding the prompt,
/// persist `PromptFinished` before accepting the next turn, and never redeliver
/// an acknowledged start. A transport that cannot preserve those guarantees
/// requires a durable sequence or event history instead.
pub struct ActivityCoordinator {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    policy: ControllerPolicy,
}

impl ActivityCoordinator {
    pub fn new(store: ConfigMapAnchorStore, locks: SessionLocks, policy: ControllerPolicy) -> Self {
        Self {
            store,
            locks,
            policy,
        }
    }

    /// Record one event received on the authenticated broker/controller lane.
    /// The untrusted worker data lane must never call this entrypoint directly.
    pub async fn record(
        &self,
        binding: &SessionBinding,
        turn_id: ActivityTurnId,
        event: ActivityEvent,
    ) -> Result<ActivityOutcome, ActivityError> {
        if binding.scope_id() != self.store.scope_id() {
            return Err(ActivityError::ScopeMismatch);
        }

        let _guard = self.locks.lock(binding.session_id()).await;
        let observed = self
            .store
            .get(binding.session_id())
            .await?
            .ok_or(ActivityError::AnchorNotFound)?;
        validate_binding(&observed, binding, self.store.scope_id())?;

        let current = observed.state();
        let durable_turn = current.last_prompt_turn_id();
        match event {
            ActivityEvent::PromptStarted => match current.phase() {
                SessionPhase::Busy if durable_turn == Some(turn_id.as_uuid()) => {
                    return Ok(ActivityOutcome::AlreadyRecorded);
                }
                SessionPhase::Busy => return Err(ActivityError::TurnMismatch),
                SessionPhase::Ready if durable_turn == Some(turn_id.as_uuid()) => {
                    return Ok(ActivityOutcome::Stale);
                }
                SessionPhase::Ready => {}
                _ => return Err(ActivityError::PhaseRejected),
            },
            ActivityEvent::PromptFinished => match current.phase() {
                SessionPhase::Ready if durable_turn == Some(turn_id.as_uuid()) => {
                    return Ok(ActivityOutcome::AlreadyRecorded);
                }
                SessionPhase::Ready => return Err(ActivityError::TurnMismatch),
                SessionPhase::Busy if durable_turn == Some(turn_id.as_uuid()) => {}
                SessionPhase::Busy => return Err(ActivityError::TurnMismatch),
                _ => return Err(ActivityError::PhaseRejected),
            },
        }
        let observed_at = next_controller_time(current.last_activity_at())?;
        let compute_deadline_at = checked_deadline(observed_at, self.policy.compute_idle_ttl())?;
        let storage_deadline_at =
            checked_deadline(observed_at, self.policy.storage_retention_ttl())?;

        let mut next = current.clone();
        match event {
            ActivityEvent::PromptStarted => next.record_prompt_started(
                binding.fence(),
                turn_id.as_uuid(),
                observed_at,
                compute_deadline_at,
                storage_deadline_at,
            ),
            ActivityEvent::PromptFinished => next.record_prompt_finished(
                binding.fence(),
                turn_id.as_uuid(),
                observed_at,
                compute_deadline_at,
                storage_deadline_at,
            ),
        }
        .map_err(|_| ActivityError::InvalidAnchor)?;
        self.store.replace(&observed, &next).await?;
        Ok(ActivityOutcome::Recorded)
    }
}

fn next_controller_time(after: DateTime<Utc>) -> Result<DateTime<Utc>, ActivityError> {
    let now = Utc::now();
    if now > after {
        return Ok(now);
    }
    after
        .checked_add_signed(Duration::nanoseconds(1))
        .ok_or(ActivityError::TimingOverflow)
}

fn checked_deadline(
    observed_at: DateTime<Utc>,
    ttl: std::time::Duration,
) -> Result<DateTime<Utc>, ActivityError> {
    let ttl = Duration::from_std(ttl).map_err(|_| ActivityError::TimingOverflow)?;
    observed_at
        .checked_add_signed(ttl)
        .ok_or(ActivityError::TimingOverflow)
}

fn validate_binding(
    observed: &StoredAnchor,
    binding: &SessionBinding,
    store_scope: crate::identity::ScopeId,
) -> Result<(), ActivityError> {
    let state = observed.state();
    if state.scope_id() != store_scope
        || binding.scope_id() != store_scope
        || state.session_id() != binding.session_id()
        || state.fence() != binding.fence()
        || state.incarnation_id() != binding.incarnation_id()
    {
        return Err(ActivityError::StaleBinding);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_time_is_strictly_monotonic_and_checked() {
        let durable_future = Utc::now() + Duration::hours(1);
        assert_eq!(
            next_controller_time(durable_future).unwrap(),
            durable_future + Duration::nanoseconds(1)
        );
        assert!(matches!(
            next_controller_time(DateTime::<Utc>::MAX_UTC),
            Err(ActivityError::TimingOverflow)
        ));
    }

    #[test]
    fn activity_turn_ids_are_non_nil() {
        assert!(!ActivityTurnId::generate().as_uuid().is_nil());
        assert!(ActivityTurnId::from_uuid(Uuid::nil()).is_err());
    }
}
