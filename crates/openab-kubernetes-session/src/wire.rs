//! Versioned, data-only messages for the add-on relay transport.
//!
//! Authentication credentials belong to the transport handshake, never these
//! JSON messages. Every decoded object rejects unknown fields and version
//! values other than `1`.
//!
//! A structurally valid worker registration is not authorization. The future
//! controller transport must bind a single-use bootstrap credential to the
//! expected binding, compare that binding with the current durable anchor,
//! and consume the credential before accepting any worker ACP traffic.

use crate::bridge::{BridgeIdentity, ControllerLifecycleAction, LifecycleKind, SessionBinding};
use crate::identity::{ScopeId, SessionId};
use crate::state::{validate_profile_name, Fence, ProfileRef};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::num::NonZeroU64;
use thiserror::Error;
use uuid::Uuid;

mod envelope;

pub use envelope::{
    BridgeToControllerV1, ControllerToBridgeV1, ControllerToWorkerV1, WorkerToControllerV1,
};

pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_ACP_FRAME_BYTES: usize =
    crate::bridge::MAX_LOGICAL_MESSAGE_BYTES + MAX_CONTROL_FRAME_BYTES;
pub const MAX_WORKER_CWD_BYTES: usize = 4 * 1024;
pub const MAX_WORKER_SESSION_ID_BYTES: usize = 4 * 1024;
pub const MAX_PROFILE_VERSION_BYTES: usize = 1024;

mod sealed {
    pub trait Sealed {}
}

/// Closed set of messages accepted by [`encode_frame`] and [`decode_frame`].
///
/// A WebSocket adapter can inspect `MAX_FRAME_BYTES` before allocating or
/// accumulating a complete message. Standalone control-plane types are
/// limited to 64 KiB. Mixed direction envelopes use the ACP ceiling until the
/// variant is known, then [`decode_frame`] applies the 64 KiB limit to their
/// control variants using the complete outer-frame length.
pub trait WireMessage: sealed::Sealed + Serialize + DeserializeOwned {
    /// Pre-allocation ceiling for this message type. Mixed relay envelopes use
    /// the ACP ceiling here, then apply a smaller variant-specific limit after
    /// decoding identifies a control-plane variant.
    const MAX_FRAME_BYTES: usize;

    /// Exact encoded-frame limit for this concrete value.
    fn encoded_frame_limit(&self) -> usize {
        Self::MAX_FRAME_BYTES
    }
}

#[derive(Debug, Error)]
pub enum WireProtocolError {
    #[error("wire frame is {bytes} bytes; maximum is {maximum}")]
    FrameTooLarge { bytes: usize, maximum: usize },
    #[error("ACP payload is {bytes} bytes; maximum is {maximum}")]
    AcpPayloadTooLarge { bytes: usize, maximum: usize },
    #[error("wire message is not valid JSON")]
    InvalidJson(#[source] serde_json::Error),
    #[error("attempt UUID must not be nil")]
    NilAttemptId,
    #[error("request UUID must not be nil")]
    NilRequestId,
    #[error("requested profile name must be a lowercase Kubernetes DNS label")]
    InvalidProfileName,
    #[error("worker cwd must be bounded, absolute, canonical, and free of control characters")]
    InvalidWorkerCwd,
    #[error("worker session ID must be non-empty and within its byte limit")]
    InvalidWorkerSessionId,
    #[error("selected profile version exceeds its byte limit")]
    ProfileVersionTooLong,
    #[error("session binding is invalid")]
    InvalidBinding,
    #[error("activated binding does not match activation field {0}")]
    BindingMismatch(&'static str),
    #[error("activated profile does not match the requested profile name")]
    ProfileMismatch,
    #[error("mapping-absent response does not match activation field {0}")]
    MappingAbsentMismatch(&'static str),
    #[error("mapping-absent response requires a present broker mapping expectation")]
    UnexpectedMappingAbsent,
    #[error("protocol result requestId does not match the lifecycle request")]
    ResultRequestIdMismatch,
    #[error("handshake protocol results must not contain a requestId")]
    UnexpectedHandshakeRequestId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Version1;

impl Serialize for Version1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(1)
    }
}

impl<'de> Deserialize<'de> for Version1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let version = u8::deserialize(deserializer)?;
        if version == 1 {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom(format!(
                "unsupported wire protocol version {version}"
            )))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NonNilUuid(Uuid);

impl NonNilUuid {
    fn attempt(value: Uuid) -> Result<Self, WireProtocolError> {
        if value.is_nil() {
            return Err(WireProtocolError::NilAttemptId);
        }
        Ok(Self(value))
    }

    fn request(value: Uuid) -> Result<Self, WireProtocolError> {
        if value.is_nil() {
            return Err(WireProtocolError::NilRequestId);
        }
        Ok(Self(value))
    }
}

impl Serialize for NonNilUuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NonNilUuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Uuid::deserialize(deserializer)?;
        if value.is_nil() {
            return Err(serde::de::Error::custom("UUID must not be nil"));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestedProfileName(String);

impl RequestedProfileName {
    fn new(value: impl Into<String>) -> Result<Self, WireProtocolError> {
        let value = value.into();
        validate_profile_name(&value).map_err(|_| WireProtocolError::InvalidProfileName)?;
        Ok(Self(value))
    }
}

impl Serialize for RequestedProfileName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RequestedProfileName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AbsoluteWorkerCwd(String);

impl AbsoluteWorkerCwd {
    fn new(value: impl Into<String>) -> Result<Self, WireProtocolError> {
        let value = value.into();
        if !value.starts_with('/')
            || value.len() > MAX_WORKER_CWD_BYTES
            || value.chars().any(char::is_control)
            || value
                .split('/')
                .any(|component| matches!(component, "." | ".."))
        {
            return Err(WireProtocolError::InvalidWorkerCwd);
        }
        Ok(Self(value))
    }
}

impl Serialize for AbsoluteWorkerCwd {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AbsoluteWorkerCwd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerSessionId(String);

impl WorkerSessionId {
    fn new(value: impl Into<String>) -> Result<Self, WireProtocolError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_WORKER_SESSION_ID_BYTES {
            return Err(WireProtocolError::InvalidWorkerSessionId);
        }
        Ok(Self(value))
    }
}

impl Serialize for WorkerSessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkerSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectedProfile(ProfileRef);

impl SelectedProfile {
    fn new(value: ProfileRef) -> Result<Self, WireProtocolError> {
        if value.version().len() > MAX_PROFILE_VERSION_BYTES {
            return Err(WireProtocolError::ProfileVersionTooLong);
        }
        Ok(Self(value))
    }
}

impl Serialize for SelectedProfile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SelectedProfile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = ProfileRef::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcpPayload {
    value: Value,
    encoded_len: usize,
}

impl AcpPayload {
    fn new(value: Value) -> Result<Self, WireProtocolError> {
        let encoded_len = validate_acp_payload(&value)?;
        Ok(Self { value, encoded_len })
    }
}

impl Serialize for AcpPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AcpPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Whether the broker has a durable opaque ACP session mapping for this
/// logical session. This is reconciliation input, never controller authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerMappingExpectationV1 {
    Absent,
    Present,
}

/// First bridge message used to activate or resume one broker-derived
/// session. Transport credentials are deliberately absent.
///
/// This V1 contract is still pre-release. The required broker-mapping field
/// and the corresponding response envelope must be deployed to bridge and
/// controller peers together; they are not compatible with the earlier draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActivationRequestV1 {
    version: Version1,
    scope_id: ScopeId,
    session_id: SessionId,
    attempt_id: NonNilUuid,
    broker_mapping_expectation: BrokerMappingExpectationV1,
    requested_profile_name: RequestedProfileName,
}

impl ActivationRequestV1 {
    pub fn new(
        scope_id: ScopeId,
        session_id: SessionId,
        attempt_id: Uuid,
        requested_profile_name: impl Into<String>,
        broker_mapping_expectation: BrokerMappingExpectationV1,
    ) -> Result<Self, WireProtocolError> {
        Ok(Self {
            version: Version1,
            scope_id,
            session_id,
            attempt_id: NonNilUuid::attempt(attempt_id)?,
            broker_mapping_expectation,
            requested_profile_name: RequestedProfileName::new(requested_profile_name)?,
        })
    }

    pub fn from_identity(
        identity: &BridgeIdentity,
        broker_mapping_expectation: BrokerMappingExpectationV1,
    ) -> Self {
        Self::new(
            identity.scope_id(),
            identity.session_id(),
            identity.broker_attempt_id(),
            identity.requested_profile_name(),
            broker_mapping_expectation,
        )
        .expect("BridgeIdentity already contains validated activation values")
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn attempt_id(&self) -> Uuid {
        self.attempt_id.0
    }

    pub fn broker_mapping_expectation(&self) -> BrokerMappingExpectationV1 {
        self.broker_mapping_expectation
    }

    pub fn requested_profile_name(&self) -> &str {
        &self.requested_profile_name.0
    }
}

/// Serializable form of the controller-issued worker authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionBindingV1 {
    version: Version1,
    scope_id: ScopeId,
    session_id: SessionId,
    generation: NonZeroU64,
    attempt_id: NonNilUuid,
    incarnation_id: NonNilUuid,
}

impl SessionBindingV1 {
    /// Rebuild the authority type and rerun all structural binding checks.
    /// The controller must additionally compare it with the current durable
    /// anchor before using it to select or mutate resources.
    pub fn to_binding(&self) -> Result<SessionBinding, WireProtocolError> {
        let fence = Fence::new(self.generation.get(), self.attempt_id.0)
            .map_err(|_| WireProtocolError::InvalidBinding)?;
        SessionBinding::new(self.scope_id, self.session_id, fence, self.incarnation_id.0)
            .map_err(|_| WireProtocolError::InvalidBinding)
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub fn attempt_id(&self) -> Uuid {
        self.attempt_id.0
    }

    pub fn incarnation_id(&self) -> Uuid {
        self.incarnation_id.0
    }

    fn validate_for(&self, activation: &ActivationRequestV1) -> Result<(), WireProtocolError> {
        if self.scope_id != activation.scope_id {
            return Err(WireProtocolError::BindingMismatch("scopeId"));
        }
        if self.session_id != activation.session_id {
            return Err(WireProtocolError::BindingMismatch("sessionId"));
        }
        if self.attempt_id.0 != activation.attempt_id.0 {
            return Err(WireProtocolError::BindingMismatch("attemptId"));
        }
        Ok(())
    }
}

impl From<&SessionBinding> for SessionBindingV1 {
    fn from(binding: &SessionBinding) -> Self {
        Self {
            version: Version1,
            scope_id: binding.scope_id(),
            session_id: binding.session_id(),
            generation: NonZeroU64::new(binding.fence().generation())
                .expect("SessionBinding generations are non-zero"),
            attempt_id: NonNilUuid(binding.fence().attempt_id()),
            incarnation_id: NonNilUuid(binding.incarnation_id()),
        }
    }
}

impl TryFrom<&SessionBindingV1> for SessionBinding {
    type Error = WireProtocolError;

    fn try_from(binding: &SessionBindingV1) -> Result<Self, Self::Error> {
        binding.to_binding()
    }
}

/// Controller response that pins both the selected profile version and the
/// worker-owned filesystem root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActivatedSessionV1 {
    version: Version1,
    profile: SelectedProfile,
    binding: SessionBindingV1,
    worker_cwd: AbsoluteWorkerCwd,
}

impl ActivatedSessionV1 {
    pub fn new(
        activation: &ActivationRequestV1,
        profile: ProfileRef,
        binding: &SessionBinding,
        worker_cwd: impl Into<String>,
    ) -> Result<Self, WireProtocolError> {
        let response = Self {
            version: Version1,
            profile: SelectedProfile::new(profile)?,
            binding: SessionBindingV1::from(binding),
            worker_cwd: AbsoluteWorkerCwd::new(worker_cwd)?,
        };
        response.validate_for(activation)?;
        Ok(response)
    }

    /// Revalidate an untrusted controller response against the exact request
    /// that caused it. Call this before constructing a `BridgeKernel`.
    fn validate_for(&self, activation: &ActivationRequestV1) -> Result<(), WireProtocolError> {
        self.binding.validate_for(activation)?;
        if self.profile.0.name() != activation.requested_profile_name() {
            return Err(WireProtocolError::ProfileMismatch);
        }
        Ok(())
    }

    /// Consume an untrusted activation response and expose its contents only
    /// after exact request correlation succeeds.
    pub fn into_validated_parts(
        self,
        activation: &ActivationRequestV1,
    ) -> Result<(ProfileRef, SessionBinding, String), WireProtocolError> {
        self.validate_for(activation)?;
        let binding = self.binding.to_binding()?;
        Ok((self.profile.0, binding, self.worker_cwd.0))
    }
}

/// Correlated controller assertion that no durable worker generation remains
/// for a broker mapping that the activation request expected to exist.
///
/// Correlation is not the absence proof itself. The controller may construct
/// this payload only after an authenticated, serialized reconciliation has
/// authoritatively observed the anchor and every owned resource as absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MappingAbsentV1 {
    version: Version1,
    scope_id: ScopeId,
    session_id: SessionId,
    attempt_id: NonNilUuid,
}

impl MappingAbsentV1 {
    fn for_request(request: &ActivationRequestV1) -> Self {
        Self {
            version: Version1,
            scope_id: request.scope_id,
            session_id: request.session_id,
            attempt_id: request.attempt_id,
        }
    }

    fn validate_for(&self, request: &ActivationRequestV1) -> Result<(), WireProtocolError> {
        if request.broker_mapping_expectation != BrokerMappingExpectationV1::Present {
            return Err(WireProtocolError::UnexpectedMappingAbsent);
        }
        if self.scope_id != request.scope_id {
            return Err(WireProtocolError::MappingAbsentMismatch("scopeId"));
        }
        if self.session_id != request.session_id {
            return Err(WireProtocolError::MappingAbsentMismatch("sessionId"));
        }
        if self.attempt_id != request.attempt_id {
            return Err(WireProtocolError::MappingAbsentMismatch("attemptId"));
        }
        Ok(())
    }
}

/// Validated activation data exposed to the broker. Raw response fields are
/// not available until the response has been correlated with its request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedActivationOutcomeV1 {
    Activated {
        profile: ProfileRef,
        binding: SessionBinding,
        worker_cwd: String,
    },
    MappingAbsent,
}

/// Closed activation response union. The adjacent `outcome` tag keeps the
/// response kind explicit while each versioned payload rejects unknown data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "outcome",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ActivationResponseV1 {
    Activated(ActivatedSessionV1),
    MappingAbsent(MappingAbsentV1),
}

impl ActivationResponseV1 {
    pub fn activated(session: ActivatedSessionV1) -> Self {
        Self::Activated(session)
    }

    pub fn mapping_absent(request: &ActivationRequestV1) -> Result<Self, WireProtocolError> {
        let absence = MappingAbsentV1::for_request(request);
        absence.validate_for(request)?;
        Ok(Self::MappingAbsent(absence))
    }

    /// Consume an untrusted controller response and expose one typed outcome
    /// only after exact correlation with the originating request.
    pub fn into_validated_outcome(
        self,
        request: &ActivationRequestV1,
    ) -> Result<ValidatedActivationOutcomeV1, WireProtocolError> {
        match self {
            Self::Activated(session) => {
                let (profile, binding, worker_cwd) = session.into_validated_parts(request)?;
                Ok(ValidatedActivationOutcomeV1::Activated {
                    profile,
                    binding,
                    worker_cwd,
                })
            }
            Self::MappingAbsent(absence) => {
                absence.validate_for(request)?;
                Ok(ValidatedActivationOutcomeV1::MappingAbsent)
            }
        }
    }
}

/// Opaque ACP JSON carried after bridge activation or worker registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcpMessageV1 {
    version: Version1,
    payload: AcpPayload,
}

impl AcpMessageV1 {
    pub fn new(payload: Value) -> Result<Self, WireProtocolError> {
        Ok(Self {
            version: Version1,
            payload: AcpPayload::new(payload)?,
        })
    }

    pub fn payload(&self) -> &Value {
        &self.payload.value
    }

    pub fn into_payload(self) -> Value {
        self.payload.value
    }

    /// Exact encoded JSON size of the opaque ACP payload, excluding this
    /// protocol's outer envelope.
    pub fn encoded_payload_bytes(&self) -> usize {
        self.payload.encoded_len
    }
}

/// Fenced bridge request for non-destructive suspension or destructive
/// release. `worker_session_id` is opaque ACP data, never controller authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LifecycleRequestV1 {
    version: Version1,
    request_id: NonNilUuid,
    #[serde(with = "lifecycle_kind_serde")]
    kind: LifecycleKind,
    binding: SessionBindingV1,
    worker_session_id: WorkerSessionId,
}

impl LifecycleRequestV1 {
    fn new(
        request_id: Uuid,
        kind: LifecycleKind,
        binding: &SessionBinding,
        worker_session_id: impl Into<String>,
    ) -> Result<Self, WireProtocolError> {
        Ok(Self {
            version: Version1,
            request_id: NonNilUuid::request(request_id)?,
            kind,
            binding: SessionBindingV1::from(binding),
            worker_session_id: WorkerSessionId::new(worker_session_id)?,
        })
    }

    /// Build one controller request using the bridge action's stable
    /// idempotency key. Retain the resulting request across delivery retries.
    pub fn from_bridge_action(
        action: &ControllerLifecycleAction,
    ) -> Result<Self, WireProtocolError> {
        Self::new(
            action.action_id(),
            action.kind(),
            action.binding(),
            action.worker_session_id(),
        )
    }

    pub fn request_id(&self) -> Uuid {
        self.request_id.0
    }

    pub fn kind(&self) -> LifecycleKind {
        self.kind
    }

    pub fn binding_wire(&self) -> &SessionBindingV1 {
        &self.binding
    }

    pub fn to_binding(&self) -> Result<SessionBinding, WireProtocolError> {
        self.binding.to_binding()
    }

    pub fn worker_session_id(&self) -> &str {
        &self.worker_session_id.0
    }
}

/// First outbound worker message. Bootstrap credentials remain in the
/// authenticated transport and are not represented in JSON.
///
/// Deserialization proves only that the binding is structurally valid. Before
/// accepting ACP traffic, the controller must authenticate and consume the
/// binding's single-use bootstrap credential, compare the expected binding
/// with the current durable anchor, then call [`Self::into_validated_binding`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerRegistrationV1 {
    version: Version1,
    binding: SessionBindingV1,
}

impl WorkerRegistrationV1 {
    pub fn new(binding: &SessionBinding) -> Self {
        Self {
            version: Version1,
            binding: SessionBindingV1::from(binding),
        }
    }

    /// Return the structurally validated session identifier for routing only.
    ///
    /// This value is not registration authority. The trusted controller must
    /// use it to re-read the durable anchor, route by that anchor's exact
    /// profile revision, and then validate the complete binding and bootstrap
    /// credential before accepting the worker.
    pub fn session_id(&self) -> SessionId {
        self.binding.session_id()
    }

    /// Return the structurally validated scope identifier for early routing
    /// rejection only. The complete binding and bootstrap credential remain
    /// mandatory registration authority.
    pub fn scope_id(&self) -> ScopeId {
        self.binding.scope_id()
    }

    /// Consume a registration and expose its binding only after an exact
    /// comparison with transport-authenticated, anchor-checked authority.
    pub fn into_validated_binding(
        self,
        expected_binding: &SessionBinding,
    ) -> Result<SessionBinding, WireProtocolError> {
        let actual = self.binding.to_binding()?;
        if actual != *expected_binding {
            return Err(WireProtocolError::BindingMismatch("binding"));
        }
        Ok(actual)
    }
}

/// Stable public failure classes. There is intentionally no free-form error
/// detail field that could expose controller, Kubernetes, or storage internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FatalCode {
    InvalidMessage,
    Unauthorized,
    StaleBinding,
    Unavailable,
    Internal,
}

/// Typed result accepted only by activation and registration handshakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeOutcomeV1 {
    Ack,
    Fatal(FatalCode),
}

/// ACK when `fatal_code` is absent; sanitized fatal result when it is present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolResultV1 {
    version: Version1,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<NonNilUuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fatal_code: Option<FatalCode>,
}

impl ProtocolResultV1 {
    pub fn ack(request_id: Option<Uuid>) -> Result<Self, WireProtocolError> {
        Ok(Self {
            version: Version1,
            request_id: request_id.map(NonNilUuid::request).transpose()?,
            fatal_code: None,
        })
    }

    pub fn fatal(request_id: Option<Uuid>, code: FatalCode) -> Result<Self, WireProtocolError> {
        Ok(Self {
            version: Version1,
            request_id: request_id.map(NonNilUuid::request).transpose()?,
            fatal_code: Some(code),
        })
    }

    fn request_id(&self) -> Option<Uuid> {
        self.request_id.map(|request_id| request_id.0)
    }

    /// Correlate a controller result with one exact lifecycle request.
    /// Missing IDs and IDs belonging to another request fail closed.
    pub fn into_lifecycle_outcome(
        self,
        request: &LifecycleRequestV1,
    ) -> Result<Option<FatalCode>, WireProtocolError> {
        if self.request_id() != Some(request.request_id()) {
            return Err(WireProtocolError::ResultRequestIdMismatch);
        }
        Ok(self.fatal_code)
    }

    /// Consume a handshake result only when it has no lifecycle correlation
    /// identifier. Connection state must still decide whether an ACK is valid
    /// for the pending activation or registration step.
    pub fn into_handshake_outcome(self) -> Result<HandshakeOutcomeV1, WireProtocolError> {
        if self.request_id().is_some() {
            return Err(WireProtocolError::UnexpectedHandshakeRequestId);
        }
        Ok(self
            .fatal_code
            .map_or(HandshakeOutcomeV1::Ack, HandshakeOutcomeV1::Fatal))
    }
}

macro_rules! control_wire_messages {
    ($($message:ty),+ $(,)?) => {
        $(
            impl sealed::Sealed for $message {}

            impl WireMessage for $message {
                const MAX_FRAME_BYTES: usize = MAX_CONTROL_FRAME_BYTES;
            }
        )+
    };
}

control_wire_messages!(
    ActivationRequestV1,
    SessionBindingV1,
    ActivationResponseV1,
    LifecycleRequestV1,
    WorkerRegistrationV1,
    ProtocolResultV1,
);

impl sealed::Sealed for AcpMessageV1 {}

impl WireMessage for AcpMessageV1 {
    const MAX_FRAME_BYTES: usize = MAX_ACP_FRAME_BYTES;
}

/// Return the pre-allocation ceiling for a concrete, sealed wire message type.
/// Mixed relay envelopes apply a smaller control-plane limit after decoding.
pub const fn max_frame_len<T: WireMessage>() -> usize {
    T::MAX_FRAME_BYTES
}

/// Validate a declared or accumulated frame length before allocating more
/// relay buffer space.
pub fn validate_frame_len<T: WireMessage>(bytes: usize) -> Result<(), WireProtocolError> {
    validate_frame_len_against(bytes, T::MAX_FRAME_BYTES)
}

fn validate_frame_len_against(bytes: usize, maximum: usize) -> Result<(), WireProtocolError> {
    if bytes > maximum {
        return Err(WireProtocolError::FrameTooLarge { bytes, maximum });
    }
    Ok(())
}

pub fn encode_frame<T: WireMessage>(message: &T) -> Result<Vec<u8>, WireProtocolError> {
    let encoded = serde_json::to_vec(message).map_err(WireProtocolError::InvalidJson)?;
    validate_frame_len::<T>(encoded.len())?;
    validate_frame_len_against(encoded.len(), message.encoded_frame_limit())?;
    Ok(encoded)
}

pub fn decode_frame<T: WireMessage>(bytes: &[u8]) -> Result<T, WireProtocolError> {
    validate_frame_len::<T>(bytes.len())?;
    let message: T = serde_json::from_slice(bytes).map_err(WireProtocolError::InvalidJson)?;
    validate_frame_len_against(bytes.len(), message.encoded_frame_limit())?;
    Ok(message)
}

fn validate_acp_payload(payload: &Value) -> Result<usize, WireProtocolError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(WireProtocolError::InvalidJson)?
        .len();
    validate_acp_payload_len(bytes)?;
    Ok(bytes)
}

fn validate_acp_payload_len(bytes: usize) -> Result<(), WireProtocolError> {
    let maximum = crate::bridge::MAX_LOGICAL_MESSAGE_BYTES;
    if bytes > maximum {
        return Err(WireProtocolError::AcpPayloadTooLarge { bytes, maximum });
    }
    Ok(())
}

mod lifecycle_kind_serde {
    use super::*;

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum WireKind {
        Suspend,
        Release,
    }

    pub(super) fn serialize<S>(kind: &LifecycleKind, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match kind {
            LifecycleKind::Suspend => WireKind::Suspend,
            LifecycleKind::Release => WireKind::Release,
        }
        .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<LifecycleKind, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match WireKind::deserialize(deserializer)? {
            WireKind::Suspend => LifecycleKind::Suspend,
            WireKind::Release => LifecycleKind::Release,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_and_frame_length_checks_include_the_boundary() {
        assert!(validate_acp_payload_len(crate::bridge::MAX_LOGICAL_MESSAGE_BYTES).is_ok());
        assert!(matches!(
            validate_acp_payload_len(crate::bridge::MAX_LOGICAL_MESSAGE_BYTES + 1),
            Err(WireProtocolError::AcpPayloadTooLarge { .. })
        ));
        assert!(validate_frame_len::<AcpMessageV1>(MAX_ACP_FRAME_BYTES).is_ok());
        assert!(matches!(
            validate_frame_len::<AcpMessageV1>(MAX_ACP_FRAME_BYTES + 1),
            Err(WireProtocolError::FrameTooLarge { .. })
        ));
        assert!(validate_frame_len::<ProtocolResultV1>(MAX_CONTROL_FRAME_BYTES).is_ok());
        assert!(matches!(
            validate_frame_len::<ProtocolResultV1>(MAX_CONTROL_FRAME_BYTES + 1),
            Err(WireProtocolError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn acp_message_exposes_its_validated_payload_size() {
        let payload = serde_json::json!({"jsonrpc": "2.0", "id": 7});
        let expected = serde_json::to_vec(&payload).unwrap().len();
        let message = AcpMessageV1::new(payload).unwrap();

        assert_eq!(message.encoded_payload_bytes(), expected);
    }
}
