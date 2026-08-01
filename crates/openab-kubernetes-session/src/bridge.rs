use crate::identity::{ScopeId, SessionId};
use crate::state::{Fence, ProfileRef};
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

const CONTROLLER_ERROR_CODE: i64 = -32000;

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

/// Opaque identity derived from broker-owned values and trusted bridge
/// configuration. The raw chat-thread key is not retained.
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
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionBindingError {
    #[error("controller incarnation identifier must not be nil")]
    NilIncarnationId,
}

/// Controller-issued authority for exactly one worker generation.
///
/// The controller must select and mutate Kubernetes resources with this
/// broker-derived binding. `worker_session_id` values are ACP data only and
/// must never be used as an authorization or resource-selection key.
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

    fn validate_for(&self, identity: &BridgeIdentity) -> Result<(), &'static str> {
        if self.scope_id != identity.scope_id {
            return Err("scopeId");
        }
        if self.session_id != identity.session_id {
            return Err("sessionId");
        }
        if self.fence.attempt_id() != identity.broker_attempt_id {
            return Err("attemptId");
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BridgeConfigError {
    #[error("worker cwd must be a non-empty absolute Linux path without NUL bytes")]
    InvalidWorkerCwd,
    #[error("controller binding does not match broker identity field {0}")]
    BindingMismatch(&'static str),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleKind {
    Suspend,
    Release,
}

/// A lifecycle request that the relay transport must send to the trusted
/// controller. The bridge does not acknowledge the ACP request until the
/// transport returns this action to [`BridgeKernel::finish_lifecycle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerLifecycleAction {
    action_id: Uuid,
    kind: LifecycleKind,
    request_id: Value,
    binding: SessionBinding,
    worker_session_id: String,
}

impl ControllerLifecycleAction {
    fn new(
        kind: LifecycleKind,
        request_id: Value,
        binding: SessionBinding,
        worker_session_id: String,
    ) -> Self {
        Self {
            action_id: Uuid::new_v4(),
            kind,
            request_id,
            binding,
            worker_session_id,
        }
    }

    /// Stable transport correlation ID for this controller action.
    /// Retries of the same action must reuse this value.
    pub fn action_id(&self) -> Uuid {
        self.action_id
    }

    pub fn kind(&self) -> LifecycleKind {
        self.kind
    }

    pub fn request_id(&self) -> &Value {
        &self.request_id
    }

    pub fn binding(&self) -> &SessionBinding {
        &self.binding
    }

    pub fn worker_session_id(&self) -> &str {
        &self.worker_session_id
    }
}

/// One transport decision for a complete ACP logical message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeAction {
    ForwardToWorker(Value),
    ForwardToBroker(Value),
    Controller(ControllerLifecycleAction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeState {
    Uninitialized,
    Initializing,
    Initialized,
    StartingSession,
    Active,
    LifecyclePending,
    Closed,
    Released,
    Failed,
}

#[derive(Debug, Error)]
pub enum BridgeProtocolError {
    #[error("logical ACP message is {bytes} bytes; maximum is {maximum}")]
    MessageTooLarge { bytes: usize, maximum: usize },
    #[error("logical ACP message is not valid JSON")]
    InvalidJson(#[source] serde_json::Error),
    #[error("invalid JSON-RPC envelope: {0}")]
    InvalidEnvelope(&'static str),
    #[error("ACP lifecycle is invalid in the current state: {0}")]
    InvalidState(&'static str),
    #[error("invalid ACP lifecycle params: {0}")]
    InvalidParams(&'static str),
    #[error("worker cannot satisfy the isolation relay contract: {0}")]
    WorkerCapability(&'static str),
    #[error("worker requested a broker-host capability that isolation mode does not expose: {0}")]
    BrokerCapability(&'static str),
    #[error("worker returned an invalid lifecycle response: {0}")]
    WorkerResponse(&'static str),
    #[error("controller lifecycle completion does not match the pending action")]
    LifecycleMismatch,
    #[error("worker attempted to spoof a controller-owned lifecycle response")]
    LifecycleResponseSpoof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingWorkerRequest {
    Initialize {
        request_id: Value,
    },
    New {
        request_id: Value,
    },
    Load {
        request_id: Value,
        session_id: String,
    },
}

impl PendingWorkerRequest {
    fn request_id(&self) -> &Value {
        match self {
            Self::Initialize { request_id }
            | Self::New { request_id }
            | Self::Load { request_id, .. } => request_id,
        }
    }
}

/// Envelope-aware policy for a single broker-to-worker ACP relay.
///
/// The kernel owns no sockets and performs no controller I/O. Its caller owns
/// bounded framing, queues, authentication, timeouts, and delivery. Messages
/// are pass-through by default; only setup paths, advertised lifecycle
/// capabilities, and bridge-owned lifecycle requests are intercepted.
pub struct BridgeKernel {
    identity: BridgeIdentity,
    binding: SessionBinding,
    worker_cwd: String,
    state: BridgeState,
    worker_session_id: Option<String>,
    pending_worker: Option<PendingWorkerRequest>,
    pending_lifecycle: Option<ControllerLifecycleAction>,
}

impl BridgeKernel {
    pub fn new(
        identity: BridgeIdentity,
        binding: SessionBinding,
        worker_cwd: impl Into<String>,
    ) -> Result<Self, BridgeConfigError> {
        binding
            .validate_for(&identity)
            .map_err(BridgeConfigError::BindingMismatch)?;
        let worker_cwd = worker_cwd.into();
        if !worker_cwd.starts_with('/') || worker_cwd.contains('\0') {
            return Err(BridgeConfigError::InvalidWorkerCwd);
        }
        Ok(Self {
            identity,
            binding,
            worker_cwd,
            state: BridgeState::Uninitialized,
            worker_session_id: None,
            pending_worker: None,
            pending_lifecycle: None,
        })
    }

    pub fn identity(&self) -> &BridgeIdentity {
        &self.identity
    }

    pub fn worker_cwd(&self) -> &str {
        &self.worker_cwd
    }

    pub fn state(&self) -> BridgeState {
        self.state
    }

    pub fn worker_session_id(&self) -> Option<&str> {
        self.worker_session_id.as_deref()
    }

    /// Inspect one complete message received from OAB and decide where the
    /// transport must deliver it.
    pub fn handle_broker_message(
        &mut self,
        bytes: &[u8],
    ) -> Result<BridgeAction, BridgeProtocolError> {
        self.ensure_not_failed()?;
        let parsed = parse_rpc_envelope(bytes)?;
        self.ensure_broker_message_allowed(&parsed.kind)?;

        match &parsed.kind {
            EnvelopeKind::Request { id, method, .. } if method == "initialize" => {
                self.begin_initialize(id.clone(), parsed.value)
            }
            EnvelopeKind::Request { id, method, params } if method == "session/new" => {
                self.begin_session(id.clone(), method, params, parsed.value)
            }
            EnvelopeKind::Request { id, method, params } if method == "session/load" => {
                self.begin_session(id.clone(), method, params, parsed.value)
            }
            EnvelopeKind::Request { id, method, params } if method == "session/close" => {
                self.begin_lifecycle(LifecycleKind::Suspend, id.clone(), params)
            }
            EnvelopeKind::Request { id, method, params } if method == "_openab/session/release" => {
                self.begin_lifecycle(LifecycleKind::Release, id.clone(), params)
            }
            EnvelopeKind::Notification { method } if is_bridge_lifecycle_method(method) => {
                Err(BridgeProtocolError::InvalidEnvelope(
                    "bridge lifecycle methods must be requests with an id",
                ))
            }
            _ => Ok(BridgeAction::ForwardToWorker(parsed.value)),
        }
    }

    /// Inspect one complete message received from the worker and decide where
    /// the transport must deliver it.
    pub fn handle_worker_message(
        &mut self,
        bytes: &[u8],
    ) -> Result<BridgeAction, BridgeProtocolError> {
        self.ensure_not_failed()?;
        if matches!(self.state, BridgeState::Closed | BridgeState::Released) {
            return Err(BridgeProtocolError::InvalidState(
                "terminal bridge cannot accept worker messages",
            ));
        }
        let mut parsed = parse_rpc_envelope(bytes)?;
        if matches!(
            &parsed.kind,
            EnvelopeKind::Request { method, .. } | EnvelopeKind::Notification { method }
                if is_broker_host_method(method)
        ) {
            return Err(BridgeProtocolError::BrokerCapability(
                "filesystem and terminal methods must execute inside the worker Pod",
            ));
        }
        let EnvelopeKind::Response { id, success } = &parsed.kind else {
            return Ok(BridgeAction::ForwardToBroker(parsed.value));
        };
        if self
            .pending_lifecycle
            .as_ref()
            .is_some_and(|action| action.request_id() == id)
        {
            return Err(BridgeProtocolError::LifecycleResponseSpoof);
        }
        let Some(pending) = self.pending_worker.as_ref() else {
            return Ok(BridgeAction::ForwardToBroker(parsed.value));
        };
        if pending.request_id() != id {
            return Ok(BridgeAction::ForwardToBroker(parsed.value));
        }

        let pending = self
            .pending_worker
            .take()
            .expect("pending worker request was checked above");
        if !success {
            self.state = match pending {
                PendingWorkerRequest::Initialize { .. } => BridgeState::Uninitialized,
                PendingWorkerRequest::New { .. } | PendingWorkerRequest::Load { .. } => {
                    BridgeState::Initialized
                }
            };
            return Ok(BridgeAction::ForwardToBroker(parsed.value));
        }

        match pending {
            PendingWorkerRequest::Initialize { .. } => {
                if let Err(error) = augment_initialize_response(&mut parsed.value)
                    .and_then(|()| ensure_logical_value_size(&parsed.value))
                {
                    self.state = BridgeState::Failed;
                    return Err(error);
                }
                self.state = BridgeState::Initialized;
            }
            PendingWorkerRequest::New { .. } => {
                let Some(session_id) = response_session_id(&parsed.value) else {
                    self.state = BridgeState::Failed;
                    return Err(BridgeProtocolError::WorkerResponse(
                        "session/new result must contain a non-empty sessionId",
                    ));
                };
                self.worker_session_id = Some(session_id.to_string());
                self.state = BridgeState::Active;
            }
            PendingWorkerRequest::Load { session_id, .. } => {
                if !parsed.value.get("result").is_some_and(Value::is_object) {
                    self.state = BridgeState::Failed;
                    return Err(BridgeProtocolError::WorkerResponse(
                        "session/load result must be an object",
                    ));
                }
                self.worker_session_id = Some(session_id);
                self.state = BridgeState::Active;
            }
        }
        Ok(BridgeAction::ForwardToBroker(parsed.value))
    }

    /// Resolve the controller action emitted by [`Self::handle_broker_message`].
    ///
    /// Only a successful controller transport result produces an ACP `{}`
    /// acknowledgement. A controller failure is returned to OAB as JSON-RPC
    /// error and leaves the worker session active so the operation can retry.
    pub fn finish_lifecycle(
        &mut self,
        action: &ControllerLifecycleAction,
        result: Result<(), ControllerError>,
    ) -> Result<BridgeAction, BridgeProtocolError> {
        if self.pending_lifecycle.as_ref() != Some(action) {
            return Err(BridgeProtocolError::LifecycleMismatch);
        }
        self.pending_lifecycle = None;

        let response = match result {
            Ok(()) => {
                self.state = match action.kind {
                    LifecycleKind::Suspend => BridgeState::Closed,
                    LifecycleKind::Release => BridgeState::Released,
                };
                rpc_result(action.request_id.clone(), json!({}))
            }
            Err(_error) => {
                self.state = BridgeState::Active;
                rpc_error(
                    action.request_id.clone(),
                    CONTROLLER_ERROR_CODE,
                    "session lifecycle controller rejected the operation".to_string(),
                )
            }
        };
        Ok(BridgeAction::ForwardToBroker(response))
    }

    fn ensure_not_failed(&self) -> Result<(), BridgeProtocolError> {
        if self.state == BridgeState::Failed {
            return Err(BridgeProtocolError::InvalidState(
                "bridge is terminal after a fail-closed worker response",
            ));
        }
        Ok(())
    }

    fn ensure_broker_message_allowed(
        &self,
        envelope: &EnvelopeKind,
    ) -> Result<(), BridgeProtocolError> {
        let allowed = match self.state {
            BridgeState::Uninitialized => matches!(
                envelope,
                EnvelopeKind::Request { method, .. } if method == "initialize"
            ),
            BridgeState::Initialized => {
                matches!(
                    envelope,
                    EnvelopeKind::Request { method, .. }
                        if method == "session/new" || method == "session/load"
                ) || matches!(envelope, EnvelopeKind::Response { .. })
            }
            BridgeState::Active => true,
            BridgeState::Initializing
            | BridgeState::StartingSession
            | BridgeState::LifecyclePending => {
                matches!(envelope, EnvelopeKind::Response { .. })
            }
            BridgeState::Closed | BridgeState::Released | BridgeState::Failed => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(BridgeProtocolError::InvalidState(
                "message is not allowed in the current bridge lifecycle state",
            ))
        }
    }

    fn begin_initialize(
        &mut self,
        request_id: Value,
        mut message: Value,
    ) -> Result<BridgeAction, BridgeProtocolError> {
        if self.state != BridgeState::Uninitialized || self.pending_worker.is_some() {
            return Err(BridgeProtocolError::InvalidState(
                "initialize requires an uninitialized bridge",
            ));
        }
        rewrite_initialize_params(&mut message)?;
        ensure_logical_value_size(&message)?;
        self.pending_worker = Some(PendingWorkerRequest::Initialize { request_id });
        self.state = BridgeState::Initializing;
        Ok(BridgeAction::ForwardToWorker(message))
    }

    fn begin_session(
        &mut self,
        request_id: Value,
        method: &str,
        params: &Value,
        mut message: Value,
    ) -> Result<BridgeAction, BridgeProtocolError> {
        if self.state != BridgeState::Initialized || self.pending_worker.is_some() {
            return Err(BridgeProtocolError::InvalidState(
                "session/new or session/load requires an initialized bridge",
            ));
        }
        let load_session_id = if method == "session/load" {
            Some(required_session_id(params)?.to_string())
        } else {
            None
        };
        rewrite_setup_params(&mut message, &self.worker_cwd, load_session_id.is_some())?;
        ensure_logical_value_size(&message)?;
        self.pending_worker = Some(match load_session_id {
            Some(session_id) => PendingWorkerRequest::Load {
                request_id,
                session_id,
            },
            None => PendingWorkerRequest::New { request_id },
        });
        self.state = BridgeState::StartingSession;
        Ok(BridgeAction::ForwardToWorker(message))
    }

    fn begin_lifecycle(
        &mut self,
        kind: LifecycleKind,
        request_id: Value,
        params: &Value,
    ) -> Result<BridgeAction, BridgeProtocolError> {
        if self.state != BridgeState::Active || self.pending_lifecycle.is_some() {
            return Err(BridgeProtocolError::InvalidState(
                "session close or release requires an active session",
            ));
        }
        let session_id = required_session_id(params)?;
        if self.worker_session_id.as_deref() != Some(session_id) {
            return Err(BridgeProtocolError::InvalidParams(
                "sessionId does not match the active worker session",
            ));
        }
        let action = ControllerLifecycleAction::new(
            kind,
            request_id,
            self.binding.clone(),
            session_id.to_string(),
        );
        self.pending_lifecycle = Some(action.clone());
        self.state = BridgeState::LifecyclePending;
        Ok(BridgeAction::Controller(action))
    }
}

fn rewrite_initialize_params(message: &mut Value) -> Result<(), BridgeProtocolError> {
    let params = message
        .get_mut("params")
        .and_then(Value::as_object_mut)
        .ok_or(BridgeProtocolError::InvalidParams(
            "initialize params must be an object",
        ))?;

    // The worker must execute filesystem and terminal operations inside its
    // own Pod. Never advertise broker-host capabilities that could turn OAB
    // into a confused deputy if its ACP client grows those capabilities.
    params.insert("clientCapabilities".to_string(), json!({}));
    Ok(())
}

fn rewrite_setup_params(
    message: &mut Value,
    worker_cwd: &str,
    is_load: bool,
) -> Result<(), BridgeProtocolError> {
    let root = message
        .as_object_mut()
        .expect("validated JSON-RPC envelope is an object");
    let params = root
        .entry("params")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or(BridgeProtocolError::InvalidParams(
            "session setup params must be an object",
        ))?;
    if params
        .get("additionalDirectories")
        .is_some_and(|value| !value.is_array())
    {
        return Err(BridgeProtocolError::InvalidParams(
            "additionalDirectories must be an array when present",
        ));
    }
    params.retain(|key, _| key == "_meta" || (is_load && key == "sessionId"));
    params.insert("cwd".to_string(), Value::String(worker_cwd.to_string()));
    params.insert("mcpServers".to_string(), json!([]));
    params.insert("additionalDirectories".to_string(), json!([]));
    Ok(())
}

fn required_session_id(params: &Value) -> Result<&str, BridgeProtocolError> {
    params
        .as_object()
        .and_then(|params| params.get("sessionId"))
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
        .ok_or(BridgeProtocolError::InvalidParams(
            "sessionId must be a non-empty string",
        ))
}

fn response_session_id(message: &Value) -> Option<&str> {
    message
        .get("result")
        .and_then(Value::as_object)
        .and_then(|result| result.get("sessionId"))
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
}

fn augment_initialize_response(message: &mut Value) -> Result<(), BridgeProtocolError> {
    let result = message
        .get_mut("result")
        .and_then(Value::as_object_mut)
        .ok_or(BridgeProtocolError::WorkerCapability(
            "initialize result must be an object",
        ))?;
    let capabilities = result
        .get_mut("agentCapabilities")
        .and_then(Value::as_object_mut)
        .ok_or(BridgeProtocolError::WorkerCapability(
            "agentCapabilities must be an object",
        ))?;
    if capabilities.get("loadSession") != Some(&Value::Bool(true)) {
        return Err(BridgeProtocolError::WorkerCapability(
            "loadSession=true is required",
        ));
    }

    let session_capabilities = object_entry(capabilities, "sessionCapabilities").ok_or(
        BridgeProtocolError::WorkerCapability("sessionCapabilities must be an object"),
    )?;
    session_capabilities.insert("close".to_string(), json!({}));
    let metadata = object_entry(session_capabilities, "_meta").ok_or(
        BridgeProtocolError::WorkerCapability("sessionCapabilities._meta must be an object"),
    )?;
    let openab =
        object_entry(metadata, "openab.dev").ok_or(BridgeProtocolError::WorkerCapability(
            "sessionCapabilities._meta.openab.dev must be an object",
        ))?;
    openab.insert("sessionRelease".to_string(), json!({"version": 1}));
    Ok(())
}

fn object_entry<'a>(
    object: &'a mut Map<String, Value>,
    key: &str,
) -> Option<&'a mut Map<String, Value>> {
    object
        .entry(key.to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
}

fn is_bridge_lifecycle_method(method: &str) -> bool {
    matches!(
        method,
        "initialize" | "session/new" | "session/load" | "session/close" | "_openab/session/release"
    )
}

fn is_broker_host_method(method: &str) -> bool {
    method.starts_with("fs/") || method.starts_with("terminal/")
}

#[derive(Debug)]
struct ParsedEnvelope {
    value: Value,
    kind: EnvelopeKind,
}

#[derive(Debug)]
enum EnvelopeKind {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
    },
    Response {
        id: Value,
        success: bool,
    },
}

fn parse_rpc_envelope(bytes: &[u8]) -> Result<ParsedEnvelope, BridgeProtocolError> {
    let value = parse_logical_message(bytes)?;
    let object = value
        .as_object()
        .ok_or(BridgeProtocolError::InvalidEnvelope(
            "message must be an object",
        ))?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(BridgeProtocolError::InvalidEnvelope(
            "jsonrpc must equal 2.0",
        ));
    }

    let id = object.get("id").cloned();
    if id
        .as_ref()
        .is_some_and(|id| !id.is_number() && !id.is_string())
    {
        return Err(BridgeProtocolError::InvalidEnvelope(
            "id must be a number or string",
        ));
    }
    let method = object.get("method");
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");

    let kind = if let Some(method) = method {
        if has_result || has_error {
            return Err(BridgeProtocolError::InvalidEnvelope(
                "request or notification cannot contain result or error",
            ));
        }
        let method = method
            .as_str()
            .filter(|method| !method.is_empty())
            .ok_or(BridgeProtocolError::InvalidEnvelope(
                "method must be a non-empty string",
            ))?
            .to_string();
        if object
            .get("params")
            .is_some_and(|params| !params.is_object() && !params.is_array())
        {
            return Err(BridgeProtocolError::InvalidEnvelope(
                "params must be an object or array when present",
            ));
        }
        let params = object.get("params").cloned().unwrap_or(Value::Null);
        match id {
            Some(id) => EnvelopeKind::Request { id, method, params },
            None => EnvelopeKind::Notification { method },
        }
    } else {
        let id = id.ok_or(BridgeProtocolError::InvalidEnvelope(
            "response must contain an id",
        ))?;
        if has_result == has_error {
            return Err(BridgeProtocolError::InvalidEnvelope(
                "response must contain exactly one of result or error",
            ));
        }
        if has_error {
            let error = object.get("error").and_then(Value::as_object).ok_or(
                BridgeProtocolError::InvalidEnvelope("response error must be an object"),
            )?;
            if error.get("code").is_none_or(|code| code.as_i64().is_none())
                || !error.get("message").is_some_and(Value::is_string)
            {
                return Err(BridgeProtocolError::InvalidEnvelope(
                    "response error must contain an integer code and string message",
                ));
            }
        }
        EnvelopeKind::Response {
            id,
            success: has_result,
        }
    };
    Ok(ParsedEnvelope { value, kind })
}

/// Parse one complete logical message after bounded accumulation.
///
/// A stream adapter must stop accumulating after
/// `MAX_LOGICAL_MESSAGE_BYTES + 2` bytes (allowing CRLF); this function then
/// enforces the payload bound before JSON decoding.
pub fn parse_logical_message(bytes: &[u8]) -> Result<Value, BridgeProtocolError> {
    parse_logical_message_with_limit(bytes, MAX_LOGICAL_MESSAGE_BYTES)
}

fn ensure_logical_value_size(value: &Value) -> Result<(), BridgeProtocolError> {
    ensure_logical_value_size_with_limit(value, MAX_LOGICAL_MESSAGE_BYTES)
}

fn ensure_logical_value_size_with_limit(
    value: &Value,
    maximum: usize,
) -> Result<(), BridgeProtocolError> {
    let bytes = serde_json::to_vec(value).expect("a serde_json::Value always serializes as JSON");
    if bytes.len() > maximum {
        return Err(BridgeProtocolError::MessageTooLarge {
            bytes: bytes.len(),
            maximum,
        });
    }
    Ok(())
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

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
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
    fn rewritten_logical_message_must_still_fit_the_outbound_limit() {
        let value = json!({"value": "1234"});
        let encoded_len = serde_json::to_vec(&value).unwrap().len();

        assert!(ensure_logical_value_size_with_limit(&value, encoded_len).is_ok());
        assert!(matches!(
            ensure_logical_value_size_with_limit(&value, encoded_len - 1),
            Err(BridgeProtocolError::MessageTooLarge { .. })
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
