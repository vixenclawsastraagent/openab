use async_trait::async_trait;
use openab_kubernetes_session::bridge::{
    ActivateRequest, ActivationIntent, BridgeIdentity, BridgeKernel, BridgeState, ControllerError,
    SessionBinding, SessionControllerClient,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::{Fence, ProfileRef};
use serde_json::{json, Value};
use std::collections::VecDeque;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ControllerCall {
    Activate(ActivateRequest),
    Suspend(SessionBinding),
    Cancel(SessionBinding),
    Release(SessionBinding),
}

#[derive(Default)]
struct FakeController {
    calls: Vec<ControllerCall>,
    activate_results: VecDeque<Result<SessionBinding, ControllerError>>,
    suspend_error: Option<ControllerError>,
    cancel_error: Option<ControllerError>,
    release_error: Option<ControllerError>,
}

#[async_trait]
impl SessionControllerClient for FakeController {
    async fn activate(
        &mut self,
        request: ActivateRequest,
    ) -> Result<SessionBinding, ControllerError> {
        self.calls.push(ControllerCall::Activate(request.clone()));
        self.activate_results.pop_front().unwrap_or_else(|| {
            Ok(SessionBinding::new(
                request.scope_id(),
                request.session_id(),
                Fence::new(1, request.broker_attempt_id()).unwrap(),
                Uuid::from_u128(200),
            )
            .unwrap())
        })
    }

    async fn suspend(&mut self, binding: &SessionBinding) -> Result<(), ControllerError> {
        self.calls.push(ControllerCall::Suspend(binding.clone()));
        self.suspend_error.clone().map_or(Ok(()), Err)
    }

    async fn cancel_turn(&mut self, binding: &SessionBinding) -> Result<(), ControllerError> {
        self.calls.push(ControllerCall::Cancel(binding.clone()));
        self.cancel_error.clone().map_or(Ok(()), Err)
    }

    async fn release(&mut self, binding: &SessionBinding) -> Result<(), ControllerError> {
        self.calls.push(ControllerCall::Release(binding.clone()));
        self.release_error.clone().map_or(Ok(()), Err)
    }
}

fn identity() -> BridgeIdentity {
    BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        "00000000-0000-0000-0000-000000000064",
        ProfileRef::new("codex-strict", "sha256-abc123").unwrap(),
    )
    .unwrap()
}

fn request(id: u64, method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    }))
    .unwrap()
}

async fn initialize(kernel: &mut BridgeKernel<FakeController>) -> Value {
    kernel
        .handle_logical_message(&request(
            1,
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {"name": "openab", "version": "0.1.0"},
            }),
        ))
        .await
        .unwrap()
        .unwrap()
}

async fn activate_new(kernel: &mut BridgeKernel<FakeController>) -> Value {
    kernel
        .handle_logical_message(&request(
            2,
            "session/new",
            json!({"cwd": "/workspace", "mcpServers": []}),
        ))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn initialize_advertises_only_the_implemented_lifecycle_contract() {
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());

    let response = initialize(&mut kernel).await;

    assert_eq!(response["result"]["protocolVersion"], 1);
    assert_eq!(response["result"]["agentCapabilities"]["loadSession"], true);
    assert!(response["result"]["agentCapabilities"]["sessionCapabilities"]["close"].is_object());
    assert_eq!(
        response["result"]["agentCapabilities"]["sessionCapabilities"]["_meta"]["openab.dev"]
            ["sessionRelease"]["version"],
        1
    );
    assert_eq!(kernel.state(), BridgeState::Initialized);
}

#[tokio::test]
async fn new_activates_the_exact_broker_identity_and_returns_opaque_outer_id() {
    let expected = identity();
    let expected_outer_id = expected.session_id().as_hex();
    let mut kernel = BridgeKernel::new(FakeController::default(), expected.clone());
    initialize(&mut kernel).await;

    let response = activate_new(&mut kernel).await;

    assert_eq!(response["result"], json!({"sessionId": expected_outer_id}));
    assert_eq!(kernel.state(), BridgeState::Active);
    assert_eq!(kernel.controller().calls.len(), 1);
    let ControllerCall::Activate(activation) = &kernel.controller().calls[0] else {
        panic!("expected activate call");
    };
    assert_eq!(activation.intent(), ActivationIntent::New);
    assert_eq!(activation.scope_id(), expected.scope_id());
    assert_eq!(activation.session_id(), expected.session_id());
    assert_eq!(activation.broker_attempt_id(), expected.broker_attempt_id());
    assert_eq!(activation.profile(), expected.profile());
}

#[tokio::test]
async fn load_from_a_new_bridge_requires_the_exact_outer_session() {
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());
    initialize(&mut kernel).await;

    let wrong = kernel
        .handle_logical_message(&request(
            2,
            "session/load",
            json!({
                "sessionId": "wrong",
                "cwd": "/workspace",
                "mcpServers": [],
            }),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(wrong["error"]["code"], -32602);
    assert_eq!(kernel.state(), BridgeState::Initialized);

    let loaded = kernel
        .handle_logical_message(&request(
            3,
            "session/load",
            json!({
                "sessionId": identity().session_id().as_hex(),
                "cwd": "/workspace",
                "mcpServers": [],
            }),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded["result"], json!({}));
    assert_eq!(kernel.state(), BridgeState::Active);

    let ControllerCall::Activate(activation) = &kernel.controller().calls[0] else {
        panic!("expected load activation call");
    };
    assert_eq!(activation.intent(), ActivationIntent::Load);
}

#[tokio::test]
async fn close_is_terminal_for_one_broker_attempt() {
    let outer_id = identity().session_id().as_hex();
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());
    initialize(&mut kernel).await;
    activate_new(&mut kernel).await;

    let close = kernel
        .handle_logical_message(&request(3, "session/close", json!({"sessionId": outer_id})))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(close["result"], json!({}));
    assert_eq!(kernel.state(), BridgeState::Closed);

    let load = kernel
        .handle_logical_message(&request(
            4,
            "session/load",
            json!({
                "sessionId": identity().session_id().as_hex(),
                "cwd": "/workspace",
                "mcpServers": [],
            }),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(load["error"]["code"], -32001);
    assert_eq!(kernel.state(), BridgeState::Closed);
    assert_eq!(kernel.controller().calls.len(), 2);
}

#[tokio::test]
async fn setup_params_cannot_expand_the_controller_owned_workspace() {
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());
    initialize(&mut kernel).await;

    for params in [
        json!({"cwd": 42, "mcpServers": []}),
        json!({"cwd": "/workspace", "mcpServers": [{}]}),
        json!({
            "cwd": "/workspace",
            "mcpServers": [],
            "additionalDirectories": ["/shared"],
        }),
    ] {
        let response = kernel
            .handle_logical_message(&request(2, "session/new", params))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(kernel.state(), BridgeState::Initialized);
    }

    assert!(kernel.controller().calls.is_empty());
}

#[tokio::test]
async fn mismatched_controller_binding_is_rejected_without_advancing_state() {
    let expected = identity();
    let mismatches = [
        SessionBinding::new(
            ScopeId::derive("other-team"),
            expected.session_id(),
            Fence::new(1, expected.broker_attempt_id()).unwrap(),
            Uuid::from_u128(200),
        )
        .unwrap(),
        SessionBinding::new(
            expected.scope_id(),
            SessionId::derive("team-a", "discord:other-thread"),
            Fence::new(1, expected.broker_attempt_id()).unwrap(),
            Uuid::from_u128(200),
        )
        .unwrap(),
        SessionBinding::new(
            expected.scope_id(),
            expected.session_id(),
            Fence::new(1, Uuid::from_u128(999)).unwrap(),
            Uuid::from_u128(200),
        )
        .unwrap(),
    ];

    for mismatched in mismatches {
        let mut controller = FakeController::default();
        controller.activate_results.push_back(Ok(mismatched));
        let mut kernel = BridgeKernel::new(controller, expected.clone());
        initialize(&mut kernel).await;

        let response = activate_new(&mut kernel).await;

        assert_eq!(response["error"]["code"], -32003);
        assert_eq!(kernel.state(), BridgeState::Initialized);
        assert!(kernel.binding().is_none());

        let retry = activate_new(&mut kernel).await;
        assert!(retry["result"].is_object());
        assert_eq!(kernel.state(), BridgeState::Active);
    }
}

#[tokio::test]
async fn controller_failures_leave_lifecycle_state_unchanged() {
    let mut controller = FakeController::default();
    controller
        .activate_results
        .push_back(Err(ControllerError::new("activation unavailable")));
    controller.release_error = Some(ControllerError::new("release unavailable"));
    let mut kernel = BridgeKernel::new(controller, identity());
    initialize(&mut kernel).await;

    let activation_failed = activate_new(&mut kernel).await;
    assert_eq!(activation_failed["error"]["code"], -32000);
    assert_eq!(kernel.state(), BridgeState::Initialized);

    activate_new(&mut kernel).await;
    let outer_id = identity().session_id().as_hex();

    kernel.controller_mut().suspend_error = Some(ControllerError::new("suspend unavailable"));
    let close_failed = kernel
        .handle_logical_message(&request(3, "session/close", json!({"sessionId": outer_id})))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(close_failed["error"]["code"], -32000);
    assert_eq!(kernel.state(), BridgeState::Active);
    kernel.controller_mut().suspend_error = None;

    kernel.controller_mut().cancel_error = Some(ControllerError::new("cancel unavailable"));
    let cancel = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": identity().session_id().as_hex()},
    }))
    .unwrap();
    assert!(kernel.handle_logical_message(&cancel).await.is_err());
    assert_eq!(kernel.state(), BridgeState::Active);
    kernel.controller_mut().cancel_error = None;

    let failed = kernel
        .handle_logical_message(&request(
            4,
            "_openab/session/release",
            json!({"sessionId": outer_id}),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed["error"]["code"], -32000);
    assert_eq!(kernel.state(), BridgeState::Active);
    assert!(kernel.binding().is_some());

    kernel.controller_mut().release_error = None;
    let released = kernel
        .handle_logical_message(&request(
            5,
            "_openab/session/release",
            json!({"sessionId": identity().session_id().as_hex()}),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released["result"], json!({}));
    assert_eq!(kernel.state(), BridgeState::Released);
    assert!(kernel.binding().is_none());

    let call_count = kernel.controller().calls.len();
    let repeated = kernel
        .handle_logical_message(&request(
            6,
            "_openab/session/release",
            json!({"sessionId": identity().session_id().as_hex()}),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated["result"], json!({}));
    assert_eq!(kernel.controller().calls.len(), call_count);
}

#[tokio::test]
async fn cancel_is_notification_only_and_never_writes_a_response() {
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());
    initialize(&mut kernel).await;
    activate_new(&mut kernel).await;
    let notification = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": identity().session_id().as_hex()},
    }))
    .unwrap();

    assert!(kernel
        .handle_logical_message(&notification)
        .await
        .unwrap()
        .is_none());
    assert_eq!(kernel.state(), BridgeState::Active);
    assert!(matches!(
        kernel.controller().calls.last(),
        Some(ControllerCall::Cancel(_))
    ));

    let before = kernel.controller().calls.len();
    assert!(kernel
        .handle_logical_message(&request(
            4,
            "session/cancel",
            json!({"sessionId": identity().session_id().as_hex()}),
        ))
        .await
        .is_err());
    assert_eq!(kernel.controller().calls.len(), before);
}

#[tokio::test]
async fn prompt_is_never_acknowledged_before_the_relay_exists() {
    let mut kernel = BridgeKernel::new(FakeController::default(), identity());
    initialize(&mut kernel).await;
    activate_new(&mut kernel).await;

    let response = kernel
        .handle_logical_message(&request(
            3,
            "session/prompt",
            json!({
                "sessionId": identity().session_id().as_hex(),
                "prompt": [{"type": "text", "text": "hello"}],
            }),
        ))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response["error"]["code"], -32601);
    assert!(response.get("result").is_none());
}

#[test]
fn bridge_identity_derives_only_from_the_exact_broker_values() {
    let expected = identity();
    assert_eq!(expected.scope_id(), ScopeId::derive("team-a"));
    assert_eq!(
        expected.session_id(),
        SessionId::derive("team-a", "discord:thread-123")
    );
    assert_eq!(expected.broker_attempt_id(), Uuid::from_u128(100));

    assert!(BridgeIdentity::from_values(
        "team-a",
        "",
        "00000000-0000-0000-0000-000000000064",
        ProfileRef::new("codex-strict", "v1").unwrap(),
    )
    .is_err());
    assert!(BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        "not-a-uuid",
        ProfileRef::new("codex-strict", "v1").unwrap(),
    )
    .is_err());
    assert!(SessionBinding::new(
        expected.scope_id(),
        expected.session_id(),
        Fence::new(1, expected.broker_attempt_id()).unwrap(),
        Uuid::nil(),
    )
    .is_err());
}
