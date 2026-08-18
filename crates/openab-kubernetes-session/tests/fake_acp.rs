#![cfg(feature = "fake-acp")]

use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const SESSION_ID: &str = "openab-fake-session-v1";

struct TemporaryWorkspace {
    path: PathBuf,
}

impl TemporaryWorkspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("openab-fake-acp-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryWorkspace {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

fn request(id: Value, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn input(messages: &[Value]) -> Vec<u8> {
    let mut input = Vec::new();
    for message in messages {
        serde_json::to_writer(&mut input, message).unwrap();
        input.push(b'\n');
    }
    input
}

fn run_fake(workspace: &Path, messages: &[Value], args: &[&str]) -> Output {
    run_fake_bytes(workspace, &input(messages), args)
}

fn run_fake_bytes(workspace: &Path, input: &[u8], args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_openab-kubernetes-session-fake-acp"))
        .args(args)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

fn output_messages(output: &Output) -> Vec<Value> {
    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    assert!(output.stderr.is_empty());
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn send_live(writer: &mut impl Write, message: &Value) {
    serde_json::to_writer(&mut *writer, message).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
}

fn receive_live(receiver: &Receiver<String>, child: &mut Child) -> Value {
    match receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(line) => serde_json::from_str(&line).unwrap(),
        Err(error) => {
            let _ = child.kill();
            panic!("fake ACP did not flush its response: {error}");
        }
    }
}

fn initialize(id: Value) -> Value {
    request(
        id,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "openab-test", "version": "1"}
        }),
    )
}

fn new_session(id: Value) -> Value {
    request(
        id,
        "session/new",
        json!({
            "cwd": "/session/workspace",
            "mcpServers": [],
            "additionalDirectories": []
        }),
    )
}

fn load_session(id: Value) -> Value {
    request(
        id,
        "session/load",
        json!({
            "sessionId": SESSION_ID,
            "cwd": "/session/workspace",
            "mcpServers": [],
            "additionalDirectories": []
        }),
    )
}

fn initialize_result(id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {
                "loadSession": true,
                "promptCapabilities": {},
                "sessionCapabilities": {
                    "close": {},
                    "_meta": {
                        "openab.dev": {"sessionRelease": {"version": 1}}
                    }
                }
            },
            "agentInfo": {
                "name": "openab-kubernetes-session-fake-acp",
                "version": "1"
            },
            "authMethods": []
        }
    })
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}

#[test]
fn lifecycle_is_exact_deterministic_and_preserves_request_ids() {
    let workspace = TemporaryWorkspace::new();
    let messages = [
        initialize(json!("initialize-string-id")),
        new_session(json!(2)),
        request(
            json!("prompt-string-id"),
            "session/prompt",
            json!({
                "sessionId": SESSION_ID,
                "prompt": [{"type": "text", "text": "ignored deterministic input"}]
            }),
        ),
        notification("session/cancel", json!({"sessionId": SESSION_ID})),
        request(json!(4), "session/close", json!({"sessionId": SESSION_ID})),
    ];

    let first = run_fake(workspace.path(), &messages, &[]);
    let repeated_workspace = TemporaryWorkspace::new();
    let repeated = run_fake(repeated_workspace.path(), &messages, &[]);
    assert_eq!(first.stdout, repeated.stdout);
    assert_eq!(
        output_messages(&first),
        [
            initialize_result(json!("initialize-string-id")),
            json!({"jsonrpc": "2.0", "id": 2, "result": {"sessionId": SESSION_ID}}),
            json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": SESSION_ID,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "openab fake ACP response"}
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "prompt-string-id",
                "result": {"stopReason": "end_turn"}
            }),
            json!({"jsonrpc": "2.0", "id": 4, "result": {}}),
        ]
    );
}

#[test]
fn load_and_release_use_the_fixed_resumable_session() {
    let workspace = TemporaryWorkspace::new();
    let messages = [
        initialize(json!(10)),
        load_session(json!("load-string-id")),
        request(
            json!(12),
            "_openab/session/release",
            json!({"sessionId": SESSION_ID}),
        ),
    ];

    assert_eq!(
        output_messages(&run_fake(workspace.path(), &messages, &[])),
        [
            initialize_result(json!(10)),
            json!({"jsonrpc": "2.0", "id": "load-string-id", "result": {}}),
            json!({"jsonrpc": "2.0", "id": 12, "result": {}}),
        ]
    );
}

#[test]
fn fixed_workspace_probe_persists_but_caller_paths_are_rejected() {
    let workspace = TemporaryWorkspace::new();
    let first = [
        initialize(json!(1)),
        new_session(json!(2)),
        request(
            json!(3),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": "private generation state"}),
        ),
    ];
    let first_output = output_messages(&run_fake(workspace.path(), &first, &[]));
    assert_eq!(
        first_output.last(),
        Some(&json!({"jsonrpc": "2.0", "id": 3, "result": {}}))
    );

    let second = [
        initialize(json!(4)),
        load_session(json!(5)),
        request(
            json!(6),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
        request(
            json!(7),
            "_openab/test/workspace/write",
            json!({
                "sessionId": SESSION_ID,
                "path": "../outside",
                "content": "must not escape"
            }),
        ),
        request(
            json!(8),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID, "path": "/etc/passwd"}),
        ),
    ];
    let second_output = output_messages(&run_fake(workspace.path(), &second, &[]));
    assert_eq!(
        &second_output[2..],
        [
            json!({
                "jsonrpc": "2.0",
                "id": 6,
                "result": {"content": "private generation state"}
            }),
            error(json!(7), -32602, "Invalid params"),
            error(json!(8), -32602, "Invalid params"),
        ]
    );
    assert!(!workspace.path().parent().unwrap().join("outside").exists());
}

#[test]
fn dangerous_and_unimplemented_capabilities_are_closed() {
    let workspace = TemporaryWorkspace::new();
    let mut messages = vec![initialize(json!(1)), new_session(json!(2))];
    for (id, method) in [
        (10, "terminal/create"),
        (11, "_openab/test/shell"),
        (12, "_openab/test/exec"),
        (13, "session/set_config_option"),
        (14, "_openab/test/network"),
    ] {
        messages.push(request(json!(id), method, json!({})));
    }

    let output = output_messages(&run_fake(workspace.path(), &messages, &[]));
    assert_eq!(
        &output[2..],
        [
            error(json!(10), -32601, "Method not found"),
            error(json!(11), -32601, "Method not found"),
            error(json!(12), -32601, "Method not found"),
            error(json!(13), -32601, "Method not found"),
            error(json!(14), -32601, "Method not found"),
        ]
    );
}

#[test]
fn arguments_and_oversized_probe_content_fail_without_echoing_input() {
    let workspace = TemporaryWorkspace::new();
    let argument_failure = run_fake(workspace.path(), &[], &["--secret-path"]);
    assert!(!argument_failure.status.success());
    assert!(argument_failure.stdout.is_empty());
    assert_eq!(
        String::from_utf8(argument_failure.stderr).unwrap(),
        "fake ACP does not accept arguments\n"
    );

    let sensitive = "sensitive-probe-content".repeat(200);
    let messages = [
        initialize(json!(1)),
        new_session(json!(2)),
        request(
            json!(3),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": sensitive}),
        ),
    ];
    let output = run_fake(workspace.path(), &messages, &[]);
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(!stderr.contains("sensitive-probe-content"));
    assert_eq!(
        output_messages(&output).last(),
        Some(&error(json!(3), -32602, "Invalid params"))
    );
}

#[cfg(unix)]
#[test]
fn workspace_probe_never_follows_a_preexisting_symlink() {
    let workspace = TemporaryWorkspace::new();
    let outside = TemporaryWorkspace::new();
    let sentinel = outside.path().join("sentinel");
    fs::write(&sentinel, "outside must remain unchanged").unwrap();
    std::os::unix::fs::symlink(&sentinel, workspace.path().join(".openab-fake-acp-probe")).unwrap();
    let messages = [
        initialize(json!(1)),
        new_session(json!(2)),
        request(
            json!(3),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
        request(
            json!(4),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": "overwrite attempt"}),
        ),
        request(
            json!(5),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
    ];

    let output = output_messages(&run_fake(workspace.path(), &messages, &[]));
    assert_eq!(
        &output[2..],
        [
            error(json!(3), -32001, "Workspace probe failed"),
            json!({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "result": {"content": "overwrite attempt"}
            }),
        ]
    );
    assert_eq!(
        fs::read_to_string(sentinel).unwrap(),
        "outside must remain unchanged"
    );
}

#[test]
fn malformed_and_oversized_input_fail_with_fixed_sanitized_errors() {
    let workspace = TemporaryWorkspace::new();
    let malformed = run_fake_bytes(workspace.path(), b"sensitive malformed input\n", &[]);
    assert!(!malformed.status.success());
    assert!(malformed.stdout.is_empty());
    assert_eq!(
        String::from_utf8(malformed.stderr).unwrap(),
        "fake ACP input is invalid\n"
    );

    let oversized = vec![b'x'; 64 * 1024 + 1];
    let oversized = run_fake_bytes(workspace.path(), &oversized, &[]);
    assert!(!oversized.status.success());
    assert!(oversized.stdout.is_empty());
    assert_eq!(
        String::from_utf8(oversized.stderr).unwrap(),
        "fake ACP input exceeds its limit\n"
    );
}

#[test]
fn eof_without_a_line_terminator_never_commits_a_workspace_write() {
    let workspace = TemporaryWorkspace::new();
    let initial = [
        initialize(json!(1)),
        new_session(json!(2)),
        request(
            json!(3),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": "committed"}),
        ),
    ];
    assert!(run_fake(workspace.path(), &initial, &[]).status.success());

    let mut truncated = input(&[initialize(json!(4)), load_session(json!(5))]);
    serde_json::to_writer(
        &mut truncated,
        &request(
            json!(6),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": "must not commit"}),
        ),
    )
    .unwrap();
    let failure = run_fake_bytes(workspace.path(), &truncated, &[]);
    assert!(!failure.status.success());
    assert_eq!(
        String::from_utf8(failure.stderr).unwrap(),
        "fake ACP input is invalid\n"
    );

    let read = [
        initialize(json!(7)),
        load_session(json!(8)),
        request(
            json!(9),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
    ];
    assert_eq!(
        output_messages(&run_fake(workspace.path(), &read, &[])).last(),
        Some(&json!({"jsonrpc": "2.0", "id": 9, "result": {"content": "committed"}}))
    );
}

#[cfg(unix)]
#[test]
fn workspace_probe_rejects_hardlink_reads_and_replaces_writes_atomically() {
    let workspace = TemporaryWorkspace::new();
    let outside = TemporaryWorkspace::new();
    let sentinel = outside.path().join("sentinel");
    fs::write(&sentinel, "outside hardlink content").unwrap();
    fs::hard_link(&sentinel, workspace.path().join(".openab-fake-acp-probe")).unwrap();
    let messages = [
        initialize(json!(1)),
        new_session(json!(2)),
        request(
            json!(3),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
        request(
            json!(4),
            "_openab/test/workspace/write",
            json!({"sessionId": SESSION_ID, "content": "private replacement"}),
        ),
        request(
            json!(5),
            "_openab/test/workspace/read",
            json!({"sessionId": SESSION_ID}),
        ),
    ];

    let output = output_messages(&run_fake(workspace.path(), &messages, &[]));
    assert_eq!(
        &output[2..],
        [
            error(json!(3), -32001, "Workspace probe failed"),
            json!({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "result": {"content": "private replacement"}
            }),
        ]
    );
    assert_eq!(
        fs::read_to_string(sentinel).unwrap(),
        "outside hardlink content"
    );
}

#[test]
fn responses_are_flushed_before_stdin_eof_and_cancel_has_no_output() {
    let workspace = TemporaryWorkspace::new();
    let mut child = Command::new(env!("CARGO_BIN_EXE_openab-kubernetes-session-fake-acp"))
        .current_dir(workspace.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line.unwrap()).is_err() {
                break;
            }
        }
    });

    send_live(&mut stdin, &initialize(json!("live-init")));
    assert_eq!(
        receive_live(&receiver, &mut child),
        initialize_result(json!("live-init"))
    );
    send_live(&mut stdin, &new_session(json!(20)));
    assert_eq!(
        receive_live(&receiver, &mut child),
        json!({"jsonrpc": "2.0", "id": 20, "result": {"sessionId": SESSION_ID}})
    );
    send_live(
        &mut stdin,
        &request(
            json!(21),
            "session/prompt",
            json!({"sessionId": SESSION_ID, "prompt": []}),
        ),
    );
    assert_eq!(
        receive_live(&receiver, &mut child)["method"],
        "session/update"
    );
    assert_eq!(
        receive_live(&receiver, &mut child),
        json!({
            "jsonrpc": "2.0",
            "id": 21,
            "result": {"stopReason": "end_turn"}
        })
    );
    send_live(
        &mut stdin,
        &notification("session/cancel", json!({"sessionId": SESSION_ID})),
    );
    send_live(
        &mut stdin,
        &request(json!(22), "session/close", json!({"sessionId": SESSION_ID})),
    );
    assert_eq!(
        receive_live(&receiver, &mut child),
        json!({"jsonrpc": "2.0", "id": 22, "result": {}})
    );

    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("terminal fake ACP session did not exit while stdin remained open");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success());
    drop(stdin);
    reader.join().unwrap();
    let mut errors = String::new();
    stderr.read_to_string(&mut errors).unwrap();
    assert!(errors.is_empty());
}

#[cfg(feature = "controller")]
#[test]
fn composed_bridge_rewrite_matches_the_fake_contract() {
    use openab_kubernetes_session::bridge::{
        BridgeAction, BridgeIdentity, BridgeKernel, SessionBinding,
    };
    use openab_kubernetes_session::resources::SESSION_WORKSPACE_V1;
    use openab_kubernetes_session::state::Fence;

    let workspace = TemporaryWorkspace::new();
    let identity = BridgeIdentity::from_values(
        "team-a",
        "discord:thread-fake",
        "00000000-0000-0000-0000-000000000064",
        "fake-profile",
    )
    .unwrap();
    let binding = SessionBinding::new(
        identity.scope_id(),
        identity.session_id(),
        Fence::new(1, identity.broker_attempt_id()).unwrap(),
        Uuid::from_u128(200),
    )
    .unwrap();
    let mut bridge = BridgeKernel::new(identity, binding, SESSION_WORKSPACE_V1).unwrap();
    let BridgeAction::ForwardToWorker(forwarded_initialize) = bridge
        .handle_broker_message(&serde_json::to_vec(&initialize(json!(1))).unwrap())
        .unwrap()
    else {
        panic!("initialize must be forwarded");
    };
    bridge
        .handle_worker_message(&serde_json::to_vec(&initialize_result(json!(1))).unwrap())
        .unwrap();
    let start = request(
        json!(2),
        "session/new",
        json!({
            "cwd": "/broker/private",
            "mcpServers": [{"command": "/bin/unsafe"}],
            "additionalDirectories": ["/broker/peer"],
            "_meta": {"test.example": {"preserved": true}}
        }),
    );
    let BridgeAction::ForwardToWorker(forwarded_start) = bridge
        .handle_broker_message(&serde_json::to_vec(&start).unwrap())
        .unwrap()
    else {
        panic!("session/new must be forwarded");
    };
    assert_eq!(forwarded_start["params"]["cwd"], SESSION_WORKSPACE_V1);
    assert_eq!(
        forwarded_start["params"]["_meta"],
        json!({"test.example": {"preserved": true}})
    );

    let output = output_messages(&run_fake(
        workspace.path(),
        &[forwarded_initialize, forwarded_start],
        &[],
    ));
    assert_eq!(
        output.last(),
        Some(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": {"sessionId": SESSION_ID}
        }))
    );
}
