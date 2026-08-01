use crate::identity::{ScopeId, SessionId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::num::NonZeroU64;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StateError {
    #[error("generation must be greater than zero")]
    InvalidGeneration,
    #[error("profile name must be a lowercase Kubernetes DNS label")]
    InvalidProfileName,
    #[error("profile version must not be empty")]
    InvalidProfileVersion,
    #[error("attempt and incarnation identifiers must not be nil")]
    InvalidRuntimeIdentifier,
    #[error("a new generation must use a fresh attempt identifier")]
    ReusedAttemptIdentifier,
    #[error("compute and storage deadlines must follow last activity")]
    InvalidDeadlines,
    #[error("last activity cannot move backwards from {current} to {proposed}")]
    ActivityTimeRegression {
        current: DateTime<Utc>,
        proposed: DateTime<Utc>,
    },
    #[error("activity cannot be refreshed while the session is {phase:?}")]
    ActivityNotAllowed { phase: SessionPhase },
    #[error("prompt turn identifier must not be nil")]
    InvalidPromptTurnIdentifier,
    #[error("prompt turn does not match the durable prompt")]
    PromptTurnMismatch,
    #[error("prompt turn changed outside a prompt transition")]
    InvalidPromptTurnSuccessor,
    #[error("prompt turn is not allowed while the session is {phase:?}")]
    PromptTurnNotAllowed { phase: SessionPhase },
    #[error("immutable anchor field {field} changed")]
    ImmutableAnchorFieldChanged { field: &'static str },
    #[error(
        "invalid fence successor from generation {current_generation} attempt {current_attempt} \
         to generation {next_generation} attempt {next_attempt}"
    )]
    InvalidFenceSuccessor {
        current_generation: u64,
        current_attempt: Uuid,
        next_generation: u64,
        next_attempt: Uuid,
    },
    #[error("worker Pod UID changed outside a create/delete observation")]
    InvalidPodSuccessor,
    #[error(
        "fence mismatch: expected generation {expected_generation} attempt {expected_attempt}, \
         got generation {actual_generation} attempt {actual_attempt}"
    )]
    FenceMismatch {
        expected_generation: u64,
        expected_attempt: Uuid,
        actual_generation: u64,
        actual_attempt: Uuid,
    },
    #[error("invalid phase transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: SessionPhase,
        to: SessionPhase,
    },
    #[error("phase requires an observed worker Pod UID")]
    MissingPodUid,
    #[error("Pod UID must not be empty")]
    InvalidPodUid,
    #[error("worker Pod {pod_uid} is still present")]
    PodStillPresent { pod_uid: String },
    #[error("expected Pod UID {expected}, got {actual}")]
    PodUidMismatch { expected: String, actual: String },
    #[error("session generation overflow")]
    GenerationOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchemaVersion;

impl Serialize for SchemaVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(1)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let version = u8::deserialize(deserializer)?;
        if version == 1 {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported session anchor schema version {version}"
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "ProfileRefWire")]
pub struct ProfileRef {
    name: String,
    version: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileRefWire {
    name: String,
    version: String,
}

impl TryFrom<ProfileRefWire> for ProfileRef {
    type Error = StateError;

    fn try_from(wire: ProfileRefWire) -> Result<Self, Self::Error> {
        Self::new(wire.name, wire.version)
    }
}

impl ProfileRef {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Result<Self, StateError> {
        let profile = Self {
            name: name.into(),
            version: version.into(),
        };
        profile.validate()?;
        Ok(profile)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    fn validate(&self) -> Result<(), StateError> {
        validate_profile_name(&self.name)?;
        if self.version.trim().is_empty() {
            return Err(StateError::InvalidProfileVersion);
        }
        Ok(())
    }
}

pub(crate) fn validate_profile_name(value: &str) -> Result<(), StateError> {
    if !is_dns_label(value) {
        return Err(StateError::InvalidProfileName);
    }
    Ok(())
}

fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "FenceWire")]
pub struct Fence {
    generation: NonZeroU64,
    attempt_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FenceWire {
    generation: u64,
    attempt_id: Uuid,
}

impl TryFrom<FenceWire> for Fence {
    type Error = StateError;

    fn try_from(wire: FenceWire) -> Result<Self, Self::Error> {
        Self::new(wire.generation, wire.attempt_id)
    }
}

impl Fence {
    pub fn new(generation: u64, attempt_id: Uuid) -> Result<Self, StateError> {
        let generation = NonZeroU64::new(generation).ok_or(StateError::InvalidGeneration)?;
        if attempt_id.is_nil() {
            return Err(StateError::InvalidRuntimeIdentifier);
        }
        Ok(Self {
            generation,
            attempt_id,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub fn attempt_id(&self) -> Uuid {
        self.attempt_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    Provisioning,
    Ready,
    Busy,
    Suspending,
    Suspended,
    Deleting,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(try_from = "SessionAnchorWire")]
pub struct SessionAnchorV1 {
    schema_version: SchemaVersion,
    session_id: SessionId,
    scope_id: ScopeId,
    profile: ProfileRef,
    incarnation_id: Uuid,
    fence: Fence,
    phase: SessionPhase,
    pod_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_prompt_turn_id: Option<Uuid>,
    last_activity_at: DateTime<Utc>,
    compute_deadline_at: DateTime<Utc>,
    storage_deadline_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionAnchorWire {
    schema_version: SchemaVersion,
    session_id: SessionId,
    scope_id: ScopeId,
    profile: ProfileRef,
    incarnation_id: Uuid,
    fence: Fence,
    phase: SessionPhase,
    pod_uid: Option<String>,
    #[serde(default)]
    last_prompt_turn_id: Option<Uuid>,
    last_activity_at: DateTime<Utc>,
    compute_deadline_at: DateTime<Utc>,
    storage_deadline_at: DateTime<Utc>,
}

impl TryFrom<SessionAnchorWire> for SessionAnchorV1 {
    type Error = StateError;

    fn try_from(wire: SessionAnchorWire) -> Result<Self, Self::Error> {
        let anchor = Self {
            schema_version: wire.schema_version,
            session_id: wire.session_id,
            scope_id: wire.scope_id,
            profile: wire.profile,
            incarnation_id: wire.incarnation_id,
            fence: wire.fence,
            phase: wire.phase,
            pod_uid: wire.pod_uid,
            last_prompt_turn_id: wire.last_prompt_turn_id,
            last_activity_at: wire.last_activity_at,
            compute_deadline_at: wire.compute_deadline_at,
            storage_deadline_at: wire.storage_deadline_at,
        };
        anchor.validate()?;
        Ok(anchor)
    }
}

impl SessionAnchorV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: SessionId,
        scope_id: ScopeId,
        profile: ProfileRef,
        attempt_id: Uuid,
        incarnation_id: Uuid,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Result<Self, StateError> {
        let anchor = Self {
            schema_version: SchemaVersion,
            session_id,
            scope_id,
            profile,
            incarnation_id,
            fence: Fence::new(1, attempt_id)?,
            phase: SessionPhase::Provisioning,
            pod_uid: None,
            last_prompt_turn_id: None,
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        };
        anchor.validate()?;
        Ok(anchor)
    }

    pub fn fence(&self) -> &Fence {
        &self.fence
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }

    pub fn incarnation_id(&self) -> Uuid {
        self.incarnation_id
    }

    pub fn phase(&self) -> SessionPhase {
        self.phase
    }

    pub fn pod_uid(&self) -> Option<&str> {
        self.pod_uid.as_deref()
    }

    pub fn last_prompt_turn_id(&self) -> Option<Uuid> {
        self.last_prompt_turn_id
    }

    pub fn last_activity_at(&self) -> DateTime<Utc> {
        self.last_activity_at
    }

    pub fn compute_deadline_at(&self) -> DateTime<Utc> {
        self.compute_deadline_at
    }

    pub fn storage_deadline_at(&self) -> DateTime<Utc> {
        self.storage_deadline_at
    }

    pub fn validate_successor(&self, next: &Self) -> Result<(), StateError> {
        self.validate()?;
        next.validate()?;
        self.validate_immutable_fields(next)?;

        let current_generation = self.fence.generation();
        let next_generation = next.fence.generation();
        if next_generation == current_generation {
            if next.fence.attempt_id() != self.fence.attempt_id() {
                return Err(self.invalid_fence_successor(next));
            }
            if self.phase != next.phase && !valid_transition(self.phase, next.phase) {
                return Err(StateError::InvalidTransition {
                    from: self.phase,
                    to: next.phase,
                });
            }
            self.validate_pod_successor(next)?;
            self.validate_activity_successor(next)?;
            return Ok(());
        }

        let expected_generation = current_generation
            .checked_add(1)
            .ok_or_else(|| self.invalid_fence_successor(next))?;
        if next_generation != expected_generation
            || next.fence.attempt_id() == self.fence.attempt_id()
        {
            return Err(self.invalid_fence_successor(next));
        }
        if !matches!(self.phase, SessionPhase::Suspended | SessionPhase::Blocked)
            || next.phase != SessionPhase::Provisioning
        {
            return Err(StateError::InvalidTransition {
                from: self.phase,
                to: next.phase,
            });
        }
        if let Some(pod_uid) = &self.pod_uid {
            return Err(StateError::PodStillPresent {
                pod_uid: pod_uid.clone(),
            });
        }
        if next.pod_uid.is_some() {
            return Err(StateError::InvalidPodSuccessor);
        }
        if next.last_prompt_turn_id.is_some() {
            return Err(StateError::InvalidPromptTurnSuccessor);
        }
        validate_deadlines(
            self.last_activity_at,
            next.last_activity_at,
            next.compute_deadline_at,
            next.storage_deadline_at,
        )
    }

    /// Refresh deadlines while a generation is still provisioning.
    /// Ready/Busy prompt activity must use the turn-fenced methods below.
    pub fn refresh_activity(
        &mut self,
        expected: &Fence,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if self.phase != SessionPhase::Provisioning {
            return Err(StateError::ActivityNotAllowed { phase: self.phase });
        }
        validate_deadlines(
            self.last_activity_at,
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        )?;
        self.last_activity_at = last_activity_at;
        self.compute_deadline_at = compute_deadline_at;
        self.storage_deadline_at = storage_deadline_at;
        Ok(())
    }

    pub fn record_prompt_started(
        &mut self,
        expected: &Fence,
        turn_id: Uuid,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if turn_id.is_nil() {
            return Err(StateError::InvalidPromptTurnIdentifier);
        }
        if self.phase != SessionPhase::Ready {
            return Err(StateError::ActivityNotAllowed { phase: self.phase });
        }
        if self.last_prompt_turn_id == Some(turn_id) {
            return Err(StateError::PromptTurnMismatch);
        }
        validate_deadlines(
            self.last_activity_at,
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        )?;
        self.last_activity_at = last_activity_at;
        self.compute_deadline_at = compute_deadline_at;
        self.storage_deadline_at = storage_deadline_at;
        self.last_prompt_turn_id = Some(turn_id);
        self.phase = SessionPhase::Busy;
        Ok(())
    }

    pub fn record_prompt_finished(
        &mut self,
        expected: &Fence,
        turn_id: Uuid,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if turn_id.is_nil() {
            return Err(StateError::InvalidPromptTurnIdentifier);
        }
        if self.phase != SessionPhase::Busy {
            return Err(StateError::ActivityNotAllowed { phase: self.phase });
        }
        if self.last_prompt_turn_id != Some(turn_id) {
            return Err(StateError::PromptTurnMismatch);
        }
        validate_deadlines(
            self.last_activity_at,
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        )?;
        self.last_activity_at = last_activity_at;
        self.compute_deadline_at = compute_deadline_at;
        self.storage_deadline_at = storage_deadline_at;
        self.phase = SessionPhase::Ready;
        Ok(())
    }

    pub fn observe_pod(&mut self, expected: &Fence, pod_uid: &str) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if self.phase != SessionPhase::Provisioning {
            return Err(StateError::InvalidTransition {
                from: self.phase,
                to: SessionPhase::Provisioning,
            });
        }
        if pod_uid.trim().is_empty() {
            return Err(StateError::InvalidPodUid);
        }
        match self.pod_uid.as_deref() {
            Some(current) if current == pod_uid => Ok(()),
            Some(current) => Err(StateError::PodStillPresent {
                pod_uid: current.to_string(),
            }),
            None => {
                self.pod_uid = Some(pod_uid.to_string());
                Ok(())
            }
        }
    }

    pub fn confirm_pod_deleted(
        &mut self,
        expected: &Fence,
        observed_pod_uid: &str,
    ) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if matches!(self.phase, SessionPhase::Ready | SessionPhase::Busy) {
            return Err(StateError::InvalidTransition {
                from: self.phase,
                to: SessionPhase::Suspending,
            });
        }
        match self.pod_uid.as_deref() {
            Some(current) if current == observed_pod_uid => {
                self.pod_uid = None;
                Ok(())
            }
            Some(current) => Err(StateError::PodUidMismatch {
                expected: current.to_string(),
                actual: observed_pod_uid.to_string(),
            }),
            None => Err(StateError::MissingPodUid),
        }
    }

    pub fn transition(&mut self, expected: &Fence, next: SessionPhase) -> Result<(), StateError> {
        self.check_fence(expected)?;
        if self.phase != next && !valid_transition(self.phase, next) {
            return Err(StateError::InvalidTransition {
                from: self.phase,
                to: next,
            });
        }
        if matches!(next, SessionPhase::Ready | SessionPhase::Busy) && self.pod_uid.is_none() {
            return Err(StateError::MissingPodUid);
        }
        if next == SessionPhase::Suspended {
            if let Some(pod_uid) = &self.pod_uid {
                return Err(StateError::PodStillPresent {
                    pod_uid: pod_uid.clone(),
                });
            }
        }
        if matches!(
            next,
            SessionPhase::Suspending | SessionPhase::Deleting | SessionPhase::Blocked
        ) {
            self.last_prompt_turn_id = None;
        }
        self.phase = next;
        Ok(())
    }

    pub fn advance_generation(
        &mut self,
        expected: &Fence,
        new_attempt_id: Uuid,
        last_activity_at: DateTime<Utc>,
        compute_deadline_at: DateTime<Utc>,
        storage_deadline_at: DateTime<Utc>,
    ) -> Result<Fence, StateError> {
        self.check_fence(expected)?;
        if let Some(pod_uid) = &self.pod_uid {
            return Err(StateError::PodStillPresent {
                pod_uid: pod_uid.clone(),
            });
        }
        if !matches!(self.phase, SessionPhase::Suspended | SessionPhase::Blocked) {
            return Err(StateError::InvalidTransition {
                from: self.phase,
                to: SessionPhase::Provisioning,
            });
        }
        let generation = self
            .fence
            .generation()
            .checked_add(1)
            .ok_or(StateError::GenerationOverflow)?;
        if new_attempt_id == self.fence.attempt_id() {
            return Err(StateError::ReusedAttemptIdentifier);
        }
        let next_fence = Fence::new(generation, new_attempt_id)?;
        validate_deadlines(
            self.last_activity_at,
            last_activity_at,
            compute_deadline_at,
            storage_deadline_at,
        )?;
        self.fence = next_fence;
        self.phase = SessionPhase::Provisioning;
        self.last_prompt_turn_id = None;
        self.last_activity_at = last_activity_at;
        self.compute_deadline_at = compute_deadline_at;
        self.storage_deadline_at = storage_deadline_at;
        Ok(self.fence.clone())
    }

    fn check_fence(&self, expected: &Fence) -> Result<(), StateError> {
        if self.fence == *expected {
            return Ok(());
        }
        Err(StateError::FenceMismatch {
            expected_generation: expected.generation(),
            expected_attempt: expected.attempt_id(),
            actual_generation: self.fence.generation(),
            actual_attempt: self.fence.attempt_id(),
        })
    }

    fn validate_immutable_fields(&self, next: &Self) -> Result<(), StateError> {
        if next.session_id != self.session_id {
            return Err(StateError::ImmutableAnchorFieldChanged { field: "sessionId" });
        }
        if next.scope_id != self.scope_id {
            return Err(StateError::ImmutableAnchorFieldChanged { field: "scopeId" });
        }
        if next.incarnation_id != self.incarnation_id {
            return Err(StateError::ImmutableAnchorFieldChanged {
                field: "incarnationId",
            });
        }
        if next.profile != self.profile {
            return Err(StateError::ImmutableAnchorFieldChanged { field: "profile" });
        }
        Ok(())
    }

    fn validate_pod_successor(&self, next: &Self) -> Result<(), StateError> {
        match (self.pod_uid.as_deref(), next.pod_uid.as_deref()) {
            (current, successor) if current == successor => Ok(()),
            (None, Some(_)) if self.phase == SessionPhase::Provisioning => Ok(()),
            (Some(_), None) if !matches!(self.phase, SessionPhase::Ready | SessionPhase::Busy) => {
                Ok(())
            }
            _ => Err(StateError::InvalidPodSuccessor),
        }
    }

    fn validate_activity_successor(&self, next: &Self) -> Result<(), StateError> {
        let timing_changed = self.last_activity_at != next.last_activity_at
            || self.compute_deadline_at != next.compute_deadline_at
            || self.storage_deadline_at != next.storage_deadline_at;
        let turn_changed = self.last_prompt_turn_id != next.last_prompt_turn_id;

        let prompt_transition = match (self.phase, next.phase) {
            (SessionPhase::Ready, SessionPhase::Busy) => {
                if !timing_changed || !turn_changed || next.last_prompt_turn_id.is_none() {
                    return Err(StateError::InvalidPromptTurnSuccessor);
                }
                true
            }
            (SessionPhase::Busy, SessionPhase::Ready) => {
                if !timing_changed || turn_changed || self.last_prompt_turn_id.is_none() {
                    return Err(StateError::InvalidPromptTurnSuccessor);
                }
                true
            }
            (
                SessionPhase::Ready | SessionPhase::Busy,
                SessionPhase::Suspending | SessionPhase::Deleting | SessionPhase::Blocked,
            ) => {
                if timing_changed || next.last_prompt_turn_id.is_some() {
                    return Err(StateError::InvalidPromptTurnSuccessor);
                }
                false
            }
            _ if turn_changed => {
                return Err(StateError::InvalidPromptTurnSuccessor);
            }
            _ => false,
        };

        if !timing_changed {
            return Ok(());
        }
        if !prompt_transition && self.phase != SessionPhase::Provisioning {
            return Err(StateError::ActivityNotAllowed { phase: self.phase });
        }
        validate_deadlines(
            self.last_activity_at,
            next.last_activity_at,
            next.compute_deadline_at,
            next.storage_deadline_at,
        )
    }

    fn invalid_fence_successor(&self, next: &Self) -> StateError {
        StateError::InvalidFenceSuccessor {
            current_generation: self.fence.generation(),
            current_attempt: self.fence.attempt_id(),
            next_generation: next.fence.generation(),
            next_attempt: next.fence.attempt_id(),
        }
    }

    fn validate(&self) -> Result<(), StateError> {
        self.profile.validate()?;
        if self.incarnation_id.is_nil() || self.fence.attempt_id().is_nil() {
            return Err(StateError::InvalidRuntimeIdentifier);
        }
        if self.compute_deadline_at <= self.last_activity_at
            || self.storage_deadline_at < self.compute_deadline_at
        {
            return Err(StateError::InvalidDeadlines);
        }
        if self
            .pod_uid
            .as_deref()
            .is_some_and(|uid| uid.trim().is_empty())
        {
            return Err(StateError::InvalidPodUid);
        }
        if self.last_prompt_turn_id.is_some_and(|id| id.is_nil()) {
            return Err(StateError::InvalidPromptTurnIdentifier);
        }
        if self.last_prompt_turn_id.is_some()
            && !matches!(self.phase, SessionPhase::Ready | SessionPhase::Busy)
        {
            return Err(StateError::PromptTurnNotAllowed { phase: self.phase });
        }
        if matches!(self.phase, SessionPhase::Ready | SessionPhase::Busy) && self.pod_uid.is_none()
        {
            return Err(StateError::MissingPodUid);
        }
        if self.phase == SessionPhase::Suspended {
            if let Some(pod_uid) = &self.pod_uid {
                return Err(StateError::PodStillPresent {
                    pod_uid: pod_uid.clone(),
                });
            }
        }
        Ok(())
    }
}

fn validate_deadlines(
    current_activity_at: DateTime<Utc>,
    last_activity_at: DateTime<Utc>,
    compute_deadline_at: DateTime<Utc>,
    storage_deadline_at: DateTime<Utc>,
) -> Result<(), StateError> {
    if last_activity_at < current_activity_at {
        return Err(StateError::ActivityTimeRegression {
            current: current_activity_at,
            proposed: last_activity_at,
        });
    }
    if compute_deadline_at <= last_activity_at || storage_deadline_at < compute_deadline_at {
        return Err(StateError::InvalidDeadlines);
    }
    Ok(())
}

fn valid_transition(from: SessionPhase, to: SessionPhase) -> bool {
    matches!(
        (from, to),
        (
            SessionPhase::Provisioning,
            SessionPhase::Ready | SessionPhase::Blocked | SessionPhase::Deleting
        ) | (
            SessionPhase::Ready,
            SessionPhase::Busy
                | SessionPhase::Suspending
                | SessionPhase::Deleting
                | SessionPhase::Blocked
        ) | (
            SessionPhase::Busy,
            SessionPhase::Ready
                | SessionPhase::Suspending
                | SessionPhase::Deleting
                | SessionPhase::Blocked
        ) | (
            SessionPhase::Suspending,
            SessionPhase::Suspended | SessionPhase::Deleting | SessionPhase::Blocked
        ) | (SessionPhase::Suspended, SessionPhase::Deleting)
            | (SessionPhase::Blocked, SessionPhase::Deleting)
    )
}
