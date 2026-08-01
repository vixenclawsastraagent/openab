use openab_kubernetes_session::bridge::{
    BridgeAction, BridgeIdentity, BridgeIdentityError, BridgeKernel, BridgeProtocolError,
    BridgeState, ControllerError, ControllerLifecycleAction, LifecycleKind, SessionBinding,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::Fence;
use serde_json::{json, Value};
use uuid::Uuid;

fn identity() -> BridgeIdentity {
    BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        "00000000-0000-0000-0000-000000000064",
        "codex-strict",
    )
    .unwrap()
}

fn kernel() -> BridgeKernel {
    let identity = identity();
    let binding = SessionBinding::new(
        identity.scope_id(),
        identity.session_id(),
        Fence::new(7, identity.broker_attempt_id()).unwrap(),
        Uuid::from_u128(200),
    )
    .unwrap();
    BridgeKernel::new(identity, binding, "/workspace").unwrap()
}

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn forwarded_to_worker(action: BridgeAction) -> Value {
    let BridgeAction::ForwardToWorker(message) = action else {
        panic!("expected message to be forwarded to the worker");
    };
    message
}

fn forwarded_to_broker(action: BridgeAction) -> Value {
    let BridgeAction::ForwardToBroker(message) = action else {
        panic!("expected message to be forwarded to the broker");
    };
    message
}

fn controller_action(action: BridgeAction) -> ControllerLifecycleAction {
    let BridgeAction::Controller(action) = action else {
        panic!("expected a controller lifecycle action");
    };
    action
}

fn initialize_request() -> Value {
    request(
        1,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": true}},
            "clientInfo": {"name": "openab", "version": "0.1.0"},
        }),
    )
}

fn worker_initialize_response(load_session: bool) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {
                "loadSession": load_session,
                "promptCapabilities": {"image": true},
                "sessionCapabilities": {
                    "fork": {},
                    "_meta": {"worker.example": {"extension": true}}
                }
            },
            "agentInfo": {"name": "real-worker", "version": "9.4.0"},
            "authMethods": [{"id": "worker-auth", "name": "Worker auth"}]
        }
    })
}

fn initialize(kernel: &mut BridgeKernel) {
    let request = initialize_request();
    let forwarded = forwarded_to_worker(kernel.handle_broker_message(&bytes(&request)).unwrap());
    assert_eq!(forwarded["params"]["clientCapabilities"], json!({}));
    forwarded_to_broker(
        kernel
            .handle_worker_message(&bytes(&worker_initialize_response(true)))
            .unwrap(),
    );
    assert_eq!(kernel.state(), BridgeState::Initialized);
}

fn activate_new(kernel: &mut BridgeKernel, worker_session_id: &str) {
    let setup = request(
        2,
        "session/new",
        json!({"cwd": "/broker/private", "mcpServers": []}),
    );
    forwarded_to_worker(kernel.handle_broker_message(&bytes(&setup)).unwrap());
    let response = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"sessionId": worker_session_id}
    });
    assert_eq!(
        forwarded_to_broker(kernel.handle_worker_message(&bytes(&response)).unwrap()),
        response
    );
    assert_eq!(kernel.state(), BridgeState::Active);
}

#[test]
fn initialize_is_relayed_and_worker_capabilities_are_augmented() {
    let mut kernel = kernel();
    let request = initialize_request();

    let forwarded = forwarded_to_worker(kernel.handle_broker_message(&bytes(&request)).unwrap());
    assert_eq!(forwarded["params"]["clientCapabilities"], json!({}));
    assert_eq!(
        forwarded["params"]["clientInfo"],
        request["params"]["clientInfo"]
    );
    assert_eq!(forwarded["params"]["protocolVersion"], 1);
    assert_eq!(kernel.state(), BridgeState::Initializing);

    let response = forwarded_to_broker(
        kernel
            .handle_worker_message(&bytes(&worker_initialize_response(true)))
            .unwrap(),
    );

    assert_eq!(response["result"]["protocolVersion"], 1);
    assert_eq!(
        response["result"]["agentInfo"],
        json!({"name": "real-worker", "version": "9.4.0"})
    );
    assert_eq!(
        response["result"]["agentCapabilities"]["promptCapabilities"],
        json!({"image": true})
    );
    assert!(response["result"]["agentCapabilities"]["sessionCapabilities"]["fork"].is_object());
    assert_eq!(
        response["result"]["agentCapabilities"]["sessionCapabilities"]["_meta"]["worker.example"]
            ["extension"],
        true
    );
    assert!(response["result"]["agentCapabilities"]["sessionCapabilities"]["close"].is_object());
    assert_eq!(
        response["result"]["agentCapabilities"]["sessionCapabilities"]["_meta"]["openab.dev"]
            ["sessionRelease"]["version"],
        1
    );
    assert_eq!(kernel.state(), BridgeState::Initialized);
}

#[test]
fn initialize_fails_closed_when_worker_cannot_load_sessions() {
    let mut kernel = kernel();
    forwarded_to_worker(
        kernel
            .handle_broker_message(&bytes(&initialize_request()))
            .unwrap(),
    );

    assert!(matches!(
        kernel.handle_worker_message(&bytes(&worker_initialize_response(false))),
        Err(BridgeProtocolError::WorkerCapability(_))
    ));
    assert_eq!(kernel.state(), BridgeState::Failed);
}

#[test]
fn session_new_rewrites_only_worker_owned_filesystem_fields() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    let original = request(
        2,
        "session/new",
        json!({
            "cwd": "/Users/broker/secret-repo",
            "mcpServers": [{"name": "broker-mcp", "command": "/bin/leak"}],
            "additionalDirectories": ["/Users/broker/other-worktree"],
            "workspaceMount": "/Users/broker/future-extension",
            "_meta": {"extension.example": {"keep": true}}
        }),
    );

    let forwarded = forwarded_to_worker(kernel.handle_broker_message(&bytes(&original)).unwrap());

    assert_eq!(forwarded["params"]["cwd"], "/workspace");
    assert_eq!(forwarded["params"]["mcpServers"], json!([]));
    assert_eq!(forwarded["params"]["additionalDirectories"], json!([]));
    assert!(forwarded["params"].get("workspaceMount").is_none());
    assert_eq!(
        forwarded["params"]["_meta"],
        json!({"extension.example": {"keep": true}})
    );
    let encoded = serde_json::to_string(&forwarded).unwrap();
    assert!(!encoded.contains("/Users/broker"));
    assert_eq!(kernel.state(), BridgeState::StartingSession);

    let worker_response = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"sessionId": "worker-real-session-7", "modes": {}}
    });
    assert_eq!(
        forwarded_to_broker(
            kernel
                .handle_worker_message(&bytes(&worker_response))
                .unwrap()
        ),
        worker_response
    );
    assert_eq!(kernel.worker_session_id(), Some("worker-real-session-7"));
    assert_eq!(kernel.state(), BridgeState::Active);
}

#[test]
fn session_load_uses_the_real_worker_session_id_and_safe_workspace() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    let load = request(
        2,
        "session/load",
        json!({
            "sessionId": "worker-session-from-oab-mapping",
            "cwd": "/broker/repo",
            "mcpServers": [{"name": "unsafe"}],
            "additionalDirectories": ["/broker/other"]
        }),
    );

    let forwarded = forwarded_to_worker(kernel.handle_broker_message(&bytes(&load)).unwrap());
    assert_eq!(
        forwarded["params"]["sessionId"],
        "worker-session-from-oab-mapping"
    );
    assert_eq!(forwarded["params"]["cwd"], "/workspace");
    assert_eq!(forwarded["params"]["mcpServers"], json!([]));
    assert_eq!(forwarded["params"]["additionalDirectories"], json!([]));

    let response = json!({"jsonrpc": "2.0", "id": 2, "result": {}});
    assert_eq!(
        forwarded_to_broker(kernel.handle_worker_message(&bytes(&response)).unwrap()),
        response
    );
    assert_eq!(
        kernel.worker_session_id(),
        Some("worker-session-from-oab-mapping")
    );
    assert_eq!(kernel.state(), BridgeState::Active);
}

#[test]
fn malformed_additional_directories_are_rejected_instead_of_forwarded() {
    let mut kernel = kernel();
    initialize(&mut kernel);

    let setup = request(
        2,
        "session/new",
        json!({"additionalDirectories": "/broker/other"}),
    );
    assert!(matches!(
        kernel.handle_broker_message(&bytes(&setup)),
        Err(BridgeProtocolError::InvalidParams(_))
    ));
    assert_eq!(kernel.state(), BridgeState::Initialized);
}

#[test]
fn prompt_unknown_notifications_and_duplex_agent_requests_are_passthrough() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");

    let prompt = request(
        3,
        "session/prompt",
        json!({
            "sessionId": "worker-1",
            "prompt": [{"type": "text", "text": "hello"}]
        }),
    );
    assert_eq!(
        forwarded_to_worker(kernel.handle_broker_message(&bytes(&prompt)).unwrap()),
        prompt
    );

    let broker_notification = json!({
        "jsonrpc": "2.0",
        "method": "extension/changed",
        "params": {"value": 1}
    });
    assert_eq!(
        forwarded_to_worker(
            kernel
                .handle_broker_message(&bytes(&broker_notification))
                .unwrap()
        ),
        broker_notification
    );

    let agent_request = request(
        90,
        "session/request_permission",
        json!({
            "sessionId": "worker-1",
            "toolCall": {"toolCallId": "call-1", "title": "Run tests"},
            "options": []
        }),
    );
    assert_eq!(
        forwarded_to_broker(
            kernel
                .handle_worker_message(&bytes(&agent_request))
                .unwrap()
        ),
        agent_request
    );

    let broker_response = json!({
        "jsonrpc": "2.0",
        "id": 90,
        "result": {"content": "read me"}
    });
    assert_eq!(
        forwarded_to_worker(
            kernel
                .handle_broker_message(&bytes(&broker_response))
                .unwrap()
        ),
        broker_response
    );

    let worker_notification = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": "worker-1", "update": {"sessionUpdate": "available_commands_update", "availableCommands": []}}
    });
    assert_eq!(
        forwarded_to_broker(
            kernel
                .handle_worker_message(&bytes(&worker_notification))
                .unwrap()
        ),
        worker_notification
    );
}

#[test]
fn worker_cannot_delegate_filesystem_or_terminal_access_to_the_broker() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");

    for method in ["fs/read_text_file", "terminal/create"] {
        let request = request(90, method, json!({"path": "/broker/private"}));
        assert!(matches!(
            kernel.handle_worker_message(&bytes(&request)),
            Err(BridgeProtocolError::BrokerCapability(_))
        ));
    }
}

#[test]
fn close_requires_controller_success_before_acknowledgement() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");
    let close = request(3, "session/close", json!({"sessionId": "worker-1"}));

    let action = controller_action(kernel.handle_broker_message(&bytes(&close)).unwrap());

    assert_eq!(action.kind(), LifecycleKind::Suspend);
    assert_eq!(action.request_id(), &json!(3));
    assert_eq!(action.worker_session_id(), "worker-1");
    assert_eq!(action.binding().scope_id(), ScopeId::derive("team-a"));
    assert_eq!(
        action.binding().session_id(),
        SessionId::derive("team-a", "discord:thread-123")
    );
    assert_eq!(action.binding().fence().generation(), 7);
    assert_eq!(kernel.state(), BridgeState::LifecyclePending);
    assert!(kernel.handle_broker_message(&bytes(&close)).is_err());

    let ack = forwarded_to_broker(kernel.finish_lifecycle(&action, Ok(())).unwrap());
    assert_eq!(ack, json!({"jsonrpc": "2.0", "id": 3, "result": {}}));
    assert_eq!(kernel.state(), BridgeState::Closed);
    assert!(kernel.finish_lifecycle(&action, Ok(())).is_err());
}

#[test]
fn worker_cannot_spoof_a_pending_lifecycle_acknowledgement() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");
    let close = request(3, "session/close", json!({"sessionId": "worker-1"}));
    let action = controller_action(kernel.handle_broker_message(&bytes(&close)).unwrap());

    let spoofed = json!({"jsonrpc": "2.0", "id": 3, "result": {}});
    assert!(matches!(
        kernel.handle_worker_message(&bytes(&spoofed)),
        Err(BridgeProtocolError::LifecycleResponseSpoof)
    ));
    assert_eq!(kernel.state(), BridgeState::LifecyclePending);

    let ack = forwarded_to_broker(kernel.finish_lifecycle(&action, Ok(())).unwrap());
    assert_eq!(ack, spoofed);
}

#[test]
fn pending_and_terminal_lifecycle_states_do_not_accept_new_broker_work() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");
    let close = request(3, "session/close", json!({"sessionId": "worker-1"}));
    let action = controller_action(kernel.handle_broker_message(&bytes(&close)).unwrap());
    let prompt = request(
        4,
        "session/prompt",
        json!({"sessionId": "worker-1", "prompt": []}),
    );
    let notification = json!({
        "jsonrpc": "2.0",
        "method": "session/cancel",
        "params": {"sessionId": "worker-1"}
    });
    for message in [&prompt, &notification] {
        assert!(matches!(
            kernel.handle_broker_message(&bytes(message)),
            Err(BridgeProtocolError::InvalidState(_))
        ));
    }

    let agent_response = json!({"jsonrpc": "2.0", "id": 90, "result": {}});
    assert_eq!(
        forwarded_to_worker(
            kernel
                .handle_broker_message(&bytes(&agent_response))
                .unwrap()
        ),
        agent_response
    );

    forwarded_to_broker(kernel.finish_lifecycle(&action, Ok(())).unwrap());
    assert!(matches!(
        kernel.handle_broker_message(&bytes(&prompt)),
        Err(BridgeProtocolError::InvalidState(_))
    ));
}

#[test]
fn release_failure_is_reported_and_can_be_retried() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    activate_new(&mut kernel, "worker-1");

    let release = request(
        3,
        "_openab/session/release",
        json!({"sessionId": "worker-1"}),
    );
    let failed_action = controller_action(kernel.handle_broker_message(&bytes(&release)).unwrap());
    let failure = forwarded_to_broker(
        kernel
            .finish_lifecycle(
                &failed_action,
                Err(ControllerError::new("controller unavailable")),
            )
            .unwrap(),
    );
    assert_eq!(failure["id"], 3);
    assert_eq!(failure["error"]["code"], -32000);
    assert_eq!(kernel.state(), BridgeState::Active);

    let retry = request(
        4,
        "_openab/session/release",
        json!({"sessionId": "worker-1"}),
    );
    let retry_action = controller_action(kernel.handle_broker_message(&bytes(&retry)).unwrap());
    let ack = forwarded_to_broker(kernel.finish_lifecycle(&retry_action, Ok(())).unwrap());
    assert_eq!(ack, json!({"jsonrpc": "2.0", "id": 4, "result": {}}));
    assert_eq!(kernel.state(), BridgeState::Released);
    assert!(kernel.handle_broker_message(&bytes(&retry)).is_err());
}

#[test]
fn lifecycle_rejects_the_wrong_worker_session_and_mismatched_completion() {
    let mut first_kernel = kernel();
    initialize(&mut first_kernel);
    activate_new(&mut first_kernel, "worker-1");

    let wrong = request(3, "session/close", json!({"sessionId": "worker-2"}));
    assert!(matches!(
        first_kernel.handle_broker_message(&bytes(&wrong)),
        Err(BridgeProtocolError::InvalidParams(_))
    ));

    let close = request(4, "session/close", json!({"sessionId": "worker-1"}));
    let action = controller_action(first_kernel.handle_broker_message(&bytes(&close)).unwrap());

    let mut other_kernel = kernel();
    initialize(&mut other_kernel);
    activate_new(&mut other_kernel, "worker-1");
    let other_close = request(4, "session/close", json!({"sessionId": "worker-1"}));
    let mismatched = controller_action(
        other_kernel
            .handle_broker_message(&bytes(&other_close))
            .unwrap(),
    );
    assert!(matches!(
        first_kernel.finish_lifecycle(&mismatched, Ok(())),
        Err(BridgeProtocolError::LifecycleMismatch)
    ));
    assert_eq!(first_kernel.state(), BridgeState::LifecyclePending);
    forwarded_to_broker(first_kernel.finish_lifecycle(&action, Ok(())).unwrap());
}

#[test]
fn duplicate_initialize_and_session_start_are_rejected() {
    let mut kernel = kernel();
    let initialize = initialize_request();
    forwarded_to_worker(kernel.handle_broker_message(&bytes(&initialize)).unwrap());
    assert!(matches!(
        kernel.handle_broker_message(&bytes(&initialize)),
        Err(BridgeProtocolError::InvalidState(_))
    ));
    forwarded_to_broker(
        kernel
            .handle_worker_message(&bytes(&worker_initialize_response(true)))
            .unwrap(),
    );

    let setup = request(2, "session/new", json!({}));
    forwarded_to_worker(kernel.handle_broker_message(&bytes(&setup)).unwrap());
    assert!(matches!(
        kernel.handle_broker_message(&bytes(&setup)),
        Err(BridgeProtocolError::InvalidState(_))
    ));
}

#[test]
fn malformed_json_rpc_envelopes_are_rejected_in_both_directions() {
    let mut kernel = kernel();
    for invalid in [
        json!([]),
        json!({"jsonrpc": "1.0", "method": "initialize", "id": 1}),
        json!({"jsonrpc": "2.0", "method": "extension/event", "params": 1}),
        json!({"jsonrpc": "2.0", "id": true, "result": {}}),
        json!({"jsonrpc": "2.0", "id": 1, "result": {}, "error": {}}),
        json!({"jsonrpc": "2.0", "id": 1, "error": {}}),
        json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000}}),
        json!({"jsonrpc": "2.0", "id": 1}),
    ] {
        assert!(matches!(
            kernel.handle_broker_message(&bytes(&invalid)),
            Err(BridgeProtocolError::InvalidEnvelope(_))
        ));
        assert!(matches!(
            kernel.handle_worker_message(&bytes(&invalid)),
            Err(BridgeProtocolError::InvalidEnvelope(_))
        ));
    }
}

#[test]
fn session_load_rejects_a_non_object_success_response() {
    let mut kernel = kernel();
    initialize(&mut kernel);
    let load = request(
        2,
        "session/load",
        json!({"sessionId": "worker-1", "cwd": "/broker", "mcpServers": []}),
    );
    forwarded_to_worker(kernel.handle_broker_message(&bytes(&load)).unwrap());

    let response = json!({"jsonrpc": "2.0", "id": 2, "result": true});
    assert!(matches!(
        kernel.handle_worker_message(&bytes(&response)),
        Err(BridgeProtocolError::WorkerResponse(_))
    ));
    assert_eq!(kernel.state(), BridgeState::Failed);
}

#[test]
fn bridge_rejects_a_controller_binding_for_another_logical_session() {
    let identity = identity();
    let binding = SessionBinding::new(
        ScopeId::derive("another-team"),
        identity.session_id(),
        Fence::new(7, identity.broker_attempt_id()).unwrap(),
        Uuid::from_u128(200),
    )
    .unwrap();

    assert!(BridgeKernel::new(identity, binding, "/workspace").is_err());
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
    assert_eq!(expected.requested_profile_name(), "codex-strict");
    let debug = format!("{expected:?}");
    assert!(debug.contains("codex-strict"));
    assert!(!debug.contains("version"));
    assert!(!debug.contains("ProfileRef"));
    assert!(!debug.contains("sha256-abc123"));

    assert!(BridgeIdentity::from_values(
        "team-a",
        "",
        "00000000-0000-0000-0000-000000000064",
        "codex-strict",
    )
    .is_err());
    assert!(BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        "not-a-uuid",
        "codex-strict",
    )
    .is_err());
}

#[test]
fn bridge_identity_rejects_invalid_profile_names_without_echoing_them() {
    const SENTINEL: &str = "forged-bridge-log-line";
    for invalid in [
        String::new(),
        "Not-A-DNS-Label".into(),
        "profile/with/slash".into(),
        format!("bad\n{SENTINEL}"),
        "a".repeat(64),
    ] {
        let error = BridgeIdentity::from_values(
            "team-a",
            "discord:thread-123",
            "00000000-0000-0000-0000-000000000064",
            &invalid,
        )
        .unwrap_err();
        assert_eq!(error, BridgeIdentityError::InvalidRequestedProfileName);
        assert_eq!(
            error.to_string(),
            "requested profile name must be a lowercase Kubernetes DNS label"
        );
        assert_eq!(format!("{error:?}"), "InvalidRequestedProfileName");
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
    }
}
