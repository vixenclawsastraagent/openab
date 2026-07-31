use crate::identity::{ScopeId, SessionId};
use crate::state::{Fence, ProfileRef};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use std::env;
use thiserror::Error;
use uuid::Uuid;

pub const SESSION_KEY_ENV: &str = "OPENAB_SESSION_KEY";
pub const SESSION_ATTEMPT_ID_ENV: &str = "OPENAB_SESSION_ATTEMPT_ID";

/// Maximum size of one complete ACP JSON-RPC message.
///
/// ACP content blocks can contain several encoded image attachments, so this
/// logical-message bound is deliberately larger than the future relay's
/// transport-frame bound. A relay may split one message into many small frames.
pub const MAX_LOGICAL_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

const INVALID_STATE_CODE: i64 = -32001;
const CONTROLLER_ERROR_CODE: i64 = -32000;
const INVALID_BINDING_CODE: i64 = -32003;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BridgeIdentityError {
    #[error("trusted scope must not be empty")]
    EmptyScope,
    #[error("{SESSION_KEY_ENV} must not be empty")]
    EmptySessionKey,
    #[error("required broker-owned environment variable {name} is unavailable")]
    EnvironmentVariable { name: &'static str },
    #[error("{SESSION_ATTEMPT_ID_ENV} must be a non-nil UUID")]
    InvalidAttemptId,
}

/// Opaque identity derived from the two broker-owned environment values and
/// trusted bridge configuration. The raw chat-thread key is not retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeIdentity {
    scope_id: ScopeId,
    session_id: SessionId,
    broker_attempt_id: Uuid,
    profile: ProfileRef,
}

impl BridgeIdentity {
    pub fn from_environment(scope: &str, profile: ProfileRef) -> Result<Self, BridgeIdentityError> {
        let session_key =
            env::var(SESSION_KEY_ENV).map_err(|_| BridgeIdentityError::EnvironmentVariable {
                name: SESSION_KEY_ENV,
            })?;
        let attempt_id = env::var(SESSION_ATTEMPT_ID_ENV).map_err(|_| {
            BridgeIdentityError::EnvironmentVariable {
                name: SESSION_ATTEMPT_ID_ENV,
            }
        })?;
        Self::from_values(scope, &session_key, &attempt_id, profile)
    }

    /// Construct from already captured broker values.
    ///
    /// This is useful for embedders that read the process environment before
    /// installing a restricted runtime and for deterministic tests.
    pub fn from_values(
        scope: &str,
        logical_session_key: &str,
        broker_attempt_id: &str,
        profile: ProfileRef,
    ) -> Result<Self, BridgeIdentityError> {
        if scope.trim().is_empty() {
            return Err(BridgeIdentityError::EmptyScope);
        }
        if logical_session_key.is_empty() {
            return Err(BridgeIdentityError::EmptySessionKey);
        }
        let broker_attempt_id = Uuid::parse_str(broker_attempt_id)
            .ok()
            .filter(|value| !value.is_nil())
            .ok_or(BridgeIdentityError::InvalidAttemptId)?;
        Ok(Self {
            scope_id: ScopeId::derive(scope),
            session_id: SessionId::derive(scope, logical_session_key),
            broker_attempt_id,
            profile,
        })
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn broker_attempt_id(&self) -> Uuid {
        self.broker_attempt_id
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }

    fn activation_request(&self, intent: ActivationIntent) -> ActivateRequest {
        ActivateRequest {
            scope_id: self.scope_id,
            session_id: self.session_id,
            broker_attempt_id: self.broker_attempt_id,
            profile: self.profile.clone(),
            intent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationIntent {
    New,
    Load,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateRequest {
    scope_id: ScopeId,
    session_id: SessionId,
    broker_attempt_id: Uuid,
    profile: ProfileRef,
    intent: ActivationIntent,
}

impl ActivateRequest {
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn broker_attempt_id(&self) -> Uuid {
        self.broker_attempt_id
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }

    pub fn intent(&self) -> ActivationIntent {
        self.intent
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionBindingError {
    #[error("controller incarnation identifier must not be nil")]
    NilIncarnationId,
}

/// Controller-issued binding for one active worker generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBinding {
    scope_id: ScopeId,
    session_id: SessionId,
    fence: Fence,
    incarnation_id: Uuid,
}

impl SessionBinding {
    pub fn new(
        scope_id: ScopeId,
        session_id: SessionId,
        fence: Fence,
        incarnation_id: Uuid,
    ) -> Result<Self, SessionBindingError> {
        if incarnation_id.is_nil() {
            return Err(SessionBindingError::NilIncarnationId);
        }
        Ok(Self {
            scope_id,
            session_id,
            fence,
            incarnation_id,
        })
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn fence(&self) -> &Fence {
        &self.fence
    }

    pub fn incarnation_id(&self) -> Uuid {
        self.incarnation_id
    }

    fn validate_for(&self, request: &ActivateRequest) -> Result<(), BindingMismatch> {
        if self.scope_id != request.scope_id {
            return Err(BindingMismatch("scopeId"));
        }
        if self.session_id != request.session_id {
            return Err(BindingMismatch("sessionId"));
        }
        if self.fence.attempt_id() != request.broker_attempt_id {
            return Err(BindingMismatch("attemptId"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct ControllerError {
    message: String,
}

impl ControllerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Minimal controller boundary needed by the protocol kernel.
///
/// Implementations own transport timeouts, authentication, durable cleanup
/// intent, and retries. An `Ok(())` release means the destructive intent is
/// durable, not that Kubernetes storage has already disappeared.
#[async_trait]
pub trait SessionControllerClient: Send + Sync {
    async fn activate(
        &mut self,
        request: ActivateRequest,
    ) -> Result<SessionBinding, ControllerError>;

    async fn suspend(&mut self, binding: &SessionBinding) -> Result<(), ControllerError>;

    async fn cancel_turn(&mut self, binding: &SessionBinding) -> Result<(), ControllerError>;

    async fn release(&mut self, binding: &SessionBinding) -> Result<(), ControllerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeState {
    Uninitialized,
    Initialized,
    Active,
    /// Terminal for this broker-owned attempt. Resume uses a new bridge
    /// process and a fresh attempt through `Initialized -> session/load`.
    Closed,
    Released,
}

#[derive(Debug, Error)]
pub enum BridgeProtocolError {
    #[error("logical ACP message is {bytes} bytes; maximum is {maximum}")]
    MessageTooLarge { bytes: usize, maximum: usize },
    #[error("logical ACP message is not valid JSON")]
    InvalidJson(#[source] serde_json::Error),
    #[error("invalid JSON-RPC envelope: {0}")]
    InvalidEnvelope(&'static str),
    #[error("session/cancel must be a notification without an id")]
    CancelMustBeNotification,
    #[error("controller rejected session/cancel")]
    CancelFailed(#[source] ControllerError),
}

#[derive(Debug, Clone, Copy, Error)]
#[error("controller binding does not match activation field {0}")]
struct BindingMismatch(&'static str);

#[derive(Debug)]
struct RpcMessage {
    id: Option<Value>,
    method: String,
    params: Value,
}

#[derive(Debug)]
struct RpcFailure {
    code: i64,
    message: String,
}

impl RpcFailure {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn invalid_state(expected: &'static str) -> Self {
        Self {
            code: INVALID_STATE_CODE,
            message: format!("session lifecycle is not ready; expected {expected}"),
        }
    }

    fn method_not_found() -> Self {
        Self {
            code: -32601,
            message: "method not implemented by the bridge kernel".to_string(),
        }
    }

    fn controller(error: ControllerError) -> Self {
        Self {
            code: CONTROLLER_ERROR_CODE,
            message: error.to_string(),
        }
    }

    fn binding(error: BindingMismatch) -> Self {
        Self {
            code: INVALID_BINDING_CODE,
            message: error.to_string(),
        }
    }
}

pub struct BridgeKernel<C> {
    controller: C,
    identity: BridgeIdentity,
    state: BridgeState,
    binding: Option<SessionBinding>,
}

impl<C> BridgeKernel<C>
where
    C: SessionControllerClient,
{
    pub fn new(controller: C, identity: BridgeIdentity) -> Self {
        Self {
            controller,
            identity,
            state: BridgeState::Uninitialized,
            binding: None,
        }
    }

    pub fn state(&self) -> BridgeState {
        self.state
    }

    pub fn binding(&self) -> Option<&SessionBinding> {
        self.binding.as_ref()
    }

    pub fn controller(&self) -> &C {
        &self.controller
    }

    pub fn controller_mut(&mut self) -> &mut C {
        &mut self.controller
    }

    /// Parse and handle one complete newline-delimited ACP JSON-RPC message.
    ///
    /// The returned value is a response object for the caller to serialize.
    /// Notifications return `None`. This kernel deliberately does not implement
    /// prompt relay; `session/prompt` receives a method-not-found response.
    pub async fn handle_logical_message(
        &mut self,
        bytes: &[u8],
    ) -> Result<Option<Value>, BridgeProtocolError> {
        let value = parse_logical_message(bytes)?;
        let message = parse_rpc_message(value)?;

        if message.method == "session/cancel" {
            if message.id.is_some() {
                return Err(BridgeProtocolError::CancelMustBeNotification);
            }
            return self.handle_cancel(&message.params).await.map(|()| None);
        }

        let Some(id) = message.id else {
            if is_request_method(&message.method) {
                return Err(BridgeProtocolError::InvalidEnvelope(
                    "request method requires an id",
                ));
            }
            return Ok(None);
        };

        let result = self
            .dispatch_request(&message.method, &message.params)
            .await;
        Ok(Some(match result {
            Ok(result) => rpc_result(id, result),
            Err(error) => rpc_error(id, error),
        }))
    }

    async fn dispatch_request(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<Value, RpcFailure> {
        match method {
            "initialize" => self.initialize(params),
            "session/new" => self.activate(params, ActivationIntent::New).await,
            "session/load" => self.activate(params, ActivationIntent::Load).await,
            "session/close" => self.close(params).await,
            "_openab/session/release" => self.release(params).await,
            _ => Err(RpcFailure::method_not_found()),
        }
    }

    fn initialize(&mut self, params: &Value) -> Result<Value, RpcFailure> {
        if self.state != BridgeState::Uninitialized {
            return Err(RpcFailure::invalid_state("uninitialized bridge"));
        }
        validate_initialize_params(params)?;
        self.state = BridgeState::Initialized;
        Ok(json!({
            "protocolVersion": 1,
            "agentCapabilities": {
                "loadSession": true,
                "sessionCapabilities": {
                    "close": {},
                    "_meta": {
                        "openab.dev": {
                            "sessionRelease": {"version": 1}
                        }
                    }
                }
            },
            "agentInfo": {
                "name": "openab-kubernetes-session",
                "version": env!("CARGO_PKG_VERSION")
            },
            "authMethods": []
        }))
    }

    async fn activate(
        &mut self,
        params: &Value,
        intent: ActivationIntent,
    ) -> Result<Value, RpcFailure> {
        let allowed = match intent {
            ActivationIntent::New => self.state == BridgeState::Initialized,
            ActivationIntent::Load => self.state == BridgeState::Initialized,
        };
        if !allowed {
            return Err(RpcFailure::invalid_state(match intent {
                ActivationIntent::New => "initialized bridge",
                ActivationIntent::Load => "newly initialized bridge",
            }));
        }
        validate_setup_params(params, intent, &self.identity.session_id.as_hex())?;

        let request = self.identity.activation_request(intent);
        let binding = self
            .controller
            .activate(request.clone())
            .await
            .map_err(RpcFailure::controller)?;
        binding
            .validate_for(&request)
            .map_err(RpcFailure::binding)?;

        self.binding = Some(binding);
        self.state = BridgeState::Active;
        Ok(match intent {
            ActivationIntent::New => json!({"sessionId": self.identity.session_id.as_hex()}),
            ActivationIntent::Load => json!({}),
        })
    }

    async fn close(&mut self, params: &Value) -> Result<Value, RpcFailure> {
        validate_session_params(params, &self.identity.session_id.as_hex())?;
        if self.state != BridgeState::Active {
            return Err(RpcFailure::invalid_state("active session"));
        }
        let binding = self
            .binding
            .as_ref()
            .expect("active state always has a binding")
            .clone();
        self.controller
            .suspend(&binding)
            .await
            .map_err(RpcFailure::controller)?;
        self.binding = None;
        self.state = BridgeState::Closed;
        Ok(json!({}))
    }

    async fn release(&mut self, params: &Value) -> Result<Value, RpcFailure> {
        validate_session_params(params, &self.identity.session_id.as_hex())?;
        if self.state == BridgeState::Released {
            return Ok(json!({}));
        }
        if self.state != BridgeState::Active {
            return Err(RpcFailure::invalid_state("active session"));
        }
        let binding = self
            .binding
            .as_ref()
            .expect("active state always has a binding")
            .clone();
        self.controller
            .release(&binding)
            .await
            .map_err(RpcFailure::controller)?;
        self.binding = None;
        self.state = BridgeState::Released;
        Ok(json!({}))
    }

    async fn handle_cancel(&mut self, params: &Value) -> Result<(), BridgeProtocolError> {
        validate_session_params(params, &self.identity.session_id.as_hex())
            .map_err(|_| BridgeProtocolError::InvalidEnvelope("invalid session/cancel params"))?;
        if self.state != BridgeState::Active {
            return Err(BridgeProtocolError::InvalidEnvelope(
                "session/cancel requires an active session",
            ));
        }
        let binding = self
            .binding
            .as_ref()
            .expect("active state always has a binding");
        self.controller
            .cancel_turn(binding)
            .await
            .map_err(BridgeProtocolError::CancelFailed)
    }
}

fn validate_initialize_params(params: &Value) -> Result<(), RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| RpcFailure::invalid_params("initialize params must be an object"))?;
    ensure_keys(
        object,
        &[
            "protocolVersion",
            "clientCapabilities",
            "clientInfo",
            "_meta",
        ],
    )?;
    if object.get("protocolVersion").and_then(Value::as_u64) != Some(1) {
        return Err(RpcFailure::invalid_params(
            "protocolVersion must be integer 1",
        ));
    }
    if object
        .get("clientCapabilities")
        .is_some_and(|value| !value.is_object())
    {
        return Err(RpcFailure::invalid_params(
            "clientCapabilities must be an object",
        ));
    }
    if object
        .get("clientInfo")
        .is_some_and(|value| !value.is_object())
    {
        return Err(RpcFailure::invalid_params("clientInfo must be an object"));
    }
    validate_meta(object)
}

fn validate_setup_params(
    params: &Value,
    intent: ActivationIntent,
    expected_session_id: &str,
) -> Result<(), RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| RpcFailure::invalid_params("session params must be an object"))?;
    let allowed: &[&str] = match intent {
        ActivationIntent::New => &["cwd", "mcpServers", "additionalDirectories", "_meta"],
        ActivationIntent::Load => &[
            "sessionId",
            "cwd",
            "mcpServers",
            "additionalDirectories",
            "_meta",
        ],
    };
    ensure_keys(object, allowed)?;
    if !object.get("cwd").is_some_and(Value::is_string) {
        return Err(RpcFailure::invalid_params("cwd must be a string"));
    }
    if !object
        .get("mcpServers")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        return Err(RpcFailure::invalid_params(
            "mcpServers must be an empty array",
        ));
    }
    if object
        .get("additionalDirectories")
        .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
    {
        return Err(RpcFailure::invalid_params(
            "additionalDirectories must be absent or empty",
        ));
    }
    if intent == ActivationIntent::Load {
        validate_session_id(object, expected_session_id)?;
    }
    validate_meta(object)
}

fn validate_session_params(params: &Value, expected_session_id: &str) -> Result<(), RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| RpcFailure::invalid_params("session params must be an object"))?;
    ensure_keys(object, &["sessionId", "_meta"])?;
    validate_session_id(object, expected_session_id)?;
    validate_meta(object)
}

fn validate_session_id(
    object: &Map<String, Value>,
    expected_session_id: &str,
) -> Result<(), RpcFailure> {
    if object.get("sessionId").and_then(Value::as_str) != Some(expected_session_id) {
        return Err(RpcFailure::invalid_params(
            "sessionId does not match this bridge",
        ));
    }
    Ok(())
}

fn validate_meta(object: &Map<String, Value>) -> Result<(), RpcFailure> {
    if object
        .get("_meta")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(RpcFailure::invalid_params(
            "_meta must be an object or null",
        ));
    }
    Ok(())
}

fn ensure_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), RpcFailure> {
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(RpcFailure::invalid_params(
            "params contain an unsupported field",
        ));
    }
    Ok(())
}

fn is_request_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "session/new"
            | "session/load"
            | "session/close"
            | "session/prompt"
            | "_openab/session/release"
    )
}

fn parse_rpc_message(value: Value) -> Result<RpcMessage, BridgeProtocolError> {
    let Value::Object(mut object) = value else {
        return Err(BridgeProtocolError::InvalidEnvelope(
            "message must be an object",
        ));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(BridgeProtocolError::InvalidEnvelope(
            "jsonrpc must equal 2.0",
        ));
    }
    let method = object
        .remove("method")
        .and_then(|method| method.as_str().map(str::to_owned))
        .filter(|method| !method.is_empty())
        .ok_or(BridgeProtocolError::InvalidEnvelope(
            "method must be a non-empty string",
        ))?;
    let id = object.remove("id");
    if id
        .as_ref()
        .is_some_and(|id| !id.is_number() && !id.is_string())
    {
        return Err(BridgeProtocolError::InvalidEnvelope(
            "id must be a number or string",
        ));
    }
    Ok(RpcMessage {
        id,
        method,
        params: object.remove("params").unwrap_or(Value::Null),
    })
}

/// Parse one complete logical message after bounded accumulation.
///
/// A stream adapter must stop accumulating after
/// `MAX_LOGICAL_MESSAGE_BYTES + 2` bytes (allowing CRLF); this function then
/// enforces the payload bound before JSON decoding.
pub fn parse_logical_message(bytes: &[u8]) -> Result<Value, BridgeProtocolError> {
    parse_logical_message_with_limit(bytes, MAX_LOGICAL_MESSAGE_BYTES)
}

fn parse_logical_message_with_limit(
    bytes: &[u8],
    maximum: usize,
) -> Result<Value, BridgeProtocolError> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);
    if bytes.len() > maximum {
        return Err(BridgeProtocolError::MessageTooLarge {
            bytes: bytes.len(),
            maximum,
        });
    }
    serde_json::from_slice(bytes).map_err(BridgeProtocolError::InvalidJson)
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: Value, error: RpcFailure) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": error.code, "message": error.message},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_message_limit_excludes_one_line_terminator() {
        assert_eq!(
            parse_logical_message_with_limit(b"{}\r\n", 2).unwrap(),
            json!({})
        );
        assert!(matches!(
            parse_logical_message_with_limit(b"[123]", 4),
            Err(BridgeProtocolError::MessageTooLarge {
                bytes: 5,
                maximum: 4
            })
        ));
    }

    #[test]
    fn logical_message_parser_accepts_an_image_payload_over_thirteen_megabytes() {
        let image = "A".repeat(14 * 1024 * 1024);
        let encoded = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "session/prompt",
            "params": {
                "prompt": [{"type": "image", "data": image, "mimeType": "image/png"}]
            }
        }))
        .unwrap();

        let decoded = parse_logical_message(&encoded).unwrap();

        assert_eq!(
            decoded["params"]["prompt"][0]["data"]
                .as_str()
                .unwrap()
                .len(),
            14 * 1024 * 1024
        );
    }
}
