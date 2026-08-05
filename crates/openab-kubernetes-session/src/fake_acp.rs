//! Deterministic, test-only ACP child for isolation verification.

use serde_json::{json, Map, Value};
use std::io::{BufRead, Write};
use thiserror::Error;

#[cfg(unix)]
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use uuid::Uuid;

const SESSION_ID: &str = "openab-fake-session-v1";
#[cfg(unix)]
const PROBE_FILE: &str = ".openab-fake-acp-probe";
const MAX_INPUT_LINE_BYTES: usize = 64 * 1024;
const MAX_PROBE_CONTENT_BYTES: usize = 4 * 1024;

#[derive(Debug, Error)]
pub(crate) enum FakeAcpError {
    #[error("fake ACP input is invalid")]
    InvalidInput,
    #[error("fake ACP input exceeds its limit")]
    InputTooLarge,
    #[error("fake ACP input/output failed")]
    Io,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    AwaitingInitialize,
    Initialized,
    Active,
    Terminal,
}

struct FakeAcp {
    state: State,
}

impl FakeAcp {
    fn new() -> Self {
        Self {
            state: State::AwaitingInitialize,
        }
    }

    fn handle(&mut self, message: Value) -> Result<Vec<Value>, FakeAcpError> {
        let object = message.as_object().ok_or(FakeAcpError::InvalidInput)?;
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(FakeAcpError::InvalidInput);
        }
        let method = object
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| !method.is_empty())
            .ok_or(FakeAcpError::InvalidInput)?;
        let params = object.get("params").cloned().unwrap_or(Value::Null);

        match object.get("id") {
            Some(id) if id.is_number() || id.is_string() => {
                Ok(self.handle_request(id.clone(), method, params))
            }
            Some(_) => Err(FakeAcpError::InvalidInput),
            None => self.handle_notification(method, &params),
        }
    }

    fn handle_request(&mut self, id: Value, method: &str, params: Value) -> Vec<Value> {
        if !is_supported_request(method) {
            return vec![rpc_error(id, -32601, "Method not found")];
        }
        if !self.method_is_valid_in_state(method) {
            return vec![rpc_error(id, -32000, "Invalid fake ACP state")];
        }

        match method {
            "initialize" if valid_initialize_params(&params) => {
                self.state = State::Initialized;
                vec![initialize_response(id)]
            }
            "session/new" if valid_session_start_params(&params, false) => {
                self.state = State::Active;
                vec![rpc_result(id, json!({"sessionId": SESSION_ID}))]
            }
            "session/load" if valid_session_start_params(&params, true) => {
                self.state = State::Active;
                vec![rpc_result(id, json!({}))]
            }
            "session/prompt" if valid_prompt_params(&params) => vec![
                json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": SESSION_ID,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {
                                "type": "text",
                                "text": "openab fake ACP response"
                            }
                        }
                    }
                }),
                rpc_result(id, json!({"stopReason": "end_turn"})),
            ],
            "session/close" | "_openab/session/release" if valid_session_params(&params) => {
                self.state = State::Terminal;
                vec![rpc_result(id, json!({}))]
            }
            "_openab/test/workspace/write" => {
                let Some(content) = valid_write_probe_params(&params) else {
                    return vec![rpc_error(id, -32602, "Invalid params")];
                };
                match write_probe(content) {
                    Ok(()) => vec![rpc_result(id, json!({}))],
                    Err(()) => vec![rpc_error(id, -32001, "Workspace probe failed")],
                }
            }
            "_openab/test/workspace/read" if valid_session_params(&params) => match read_probe() {
                Ok(content) => vec![rpc_result(id, json!({"content": content}))],
                Err(()) => vec![rpc_error(id, -32001, "Workspace probe failed")],
            },
            _ => vec![rpc_error(id, -32602, "Invalid params")],
        }
    }

    fn handle_notification(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<Vec<Value>, FakeAcpError> {
        if method == "session/cancel"
            && (self.state != State::Active || !valid_session_params(params))
        {
            return Err(FakeAcpError::InvalidInput);
        }
        Ok(Vec::new())
    }

    fn method_is_valid_in_state(&self, method: &str) -> bool {
        match method {
            "initialize" => self.state == State::AwaitingInitialize,
            "session/new" | "session/load" => self.state == State::Initialized,
            "session/prompt"
            | "session/close"
            | "_openab/session/release"
            | "_openab/test/workspace/write"
            | "_openab/test/workspace/read" => self.state == State::Active,
            _ => false,
        }
    }
}

pub(crate) fn run<R, W>(mut reader: R, mut writer: W) -> Result<(), FakeAcpError>
where
    R: BufRead,
    W: Write,
{
    let mut fake = FakeAcp::new();
    let mut line = Vec::new();
    while read_bounded_line(&mut reader, &mut line)? {
        let message = serde_json::from_slice(&line).map_err(|_| FakeAcpError::InvalidInput)?;
        for response in fake.handle(message)? {
            serde_json::to_writer(&mut writer, &response).map_err(|_| FakeAcpError::Io)?;
            writer.write_all(b"\n").map_err(|_| FakeAcpError::Io)?;
        }
        writer.flush().map_err(|_| FakeAcpError::Io)?;
    }
    Ok(())
}

fn read_bounded_line<R>(reader: &mut R, line: &mut Vec<u8>) -> Result<bool, FakeAcpError>
where
    R: BufRead,
{
    line.clear();
    loop {
        let available = reader.fill_buf().map_err(|_| FakeAcpError::Io)?;
        if available.is_empty() {
            return line
                .is_empty()
                .then_some(false)
                .ok_or(FakeAcpError::InvalidInput);
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(newline) > MAX_INPUT_LINE_BYTES {
                return Err(FakeAcpError::InputTooLarge);
            }
            line.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return if line.is_empty() {
                Err(FakeAcpError::InvalidInput)
            } else {
                Ok(true)
            };
        }
        if line.len().saturating_add(available.len()) > MAX_INPUT_LINE_BYTES {
            return Err(FakeAcpError::InputTooLarge);
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
}

fn is_supported_request(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "session/new"
            | "session/load"
            | "session/prompt"
            | "session/close"
            | "_openab/session/release"
            | "_openab/test/workspace/write"
            | "_openab/test/workspace/read"
    )
}

fn valid_initialize_params(params: &Value) -> bool {
    let Some(params) = exact_object(
        params,
        &["clientCapabilities", "clientInfo", "protocolVersion"],
    ) else {
        return false;
    };
    params.get("protocolVersion").and_then(Value::as_u64) == Some(1)
        && params
            .get("clientCapabilities")
            .is_some_and(Value::is_object)
        && params.get("clientInfo").is_some_and(Value::is_object)
}

fn valid_session_start_params(params: &Value, load: bool) -> bool {
    let expected = if load {
        &["additionalDirectories", "cwd", "mcpServers", "sessionId"][..]
    } else {
        &["additionalDirectories", "cwd", "mcpServers"][..]
    };
    let Some(params) = object_with_optional_meta(params, expected) else {
        return false;
    };
    params.get("cwd").and_then(Value::as_str) == Some("/session/workspace")
        && params
            .get("mcpServers")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        && params
            .get("additionalDirectories")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        && (!load || params.get("sessionId").and_then(Value::as_str) == Some(SESSION_ID))
}

fn valid_prompt_params(params: &Value) -> bool {
    let Some(params) = exact_object(params, &["prompt", "sessionId"]) else {
        return false;
    };
    valid_session_id(params) && params.get("prompt").is_some_and(Value::is_array)
}

fn valid_session_params(params: &Value) -> bool {
    exact_object(params, &["sessionId"]).is_some_and(valid_session_id)
}

fn valid_write_probe_params(params: &Value) -> Option<&str> {
    let params = exact_object(params, &["content", "sessionId"])?;
    if !valid_session_id(params) {
        return None;
    }
    params
        .get("content")
        .and_then(Value::as_str)
        .filter(|content| content.len() <= MAX_PROBE_CONTENT_BYTES)
}

fn valid_session_id(params: &Map<String, Value>) -> bool {
    params.get("sessionId").and_then(Value::as_str) == Some(SESSION_ID)
}

fn exact_object<'a>(value: &'a Value, expected: &[&str]) -> Option<&'a Map<String, Value>> {
    let object = value.as_object()?;
    (object.len() == expected.len() && expected.iter().all(|field| object.contains_key(*field)))
        .then_some(object)
}

fn object_with_optional_meta<'a>(
    value: &'a Value,
    required: &[&str],
) -> Option<&'a Map<String, Value>> {
    let object = value.as_object()?;
    if !required.iter().all(|field| object.contains_key(*field))
        || object
            .keys()
            .any(|field| field != "_meta" && !required.contains(&field.as_str()))
        || object.get("_meta").is_some_and(|meta| !meta.is_object())
    {
        return None;
    }
    Some(object)
}

#[cfg(unix)]
fn write_probe(content: &str) -> Result<(), ()> {
    let directory = open_workspace_directory()?;
    let temporary = format!(".openab-fake-acp-probe-{}", Uuid::new_v4());
    let descriptor = rustix::fs::openat(
        &directory,
        &temporary,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ())?;
    let mut file = File::from(descriptor);
    let write_result = (|| {
        file.write_all(content.as_bytes()).map_err(|_| ())?;
        file.sync_all().map_err(|_| ())?;
        require_private_regular_file(&file)
    })();
    drop(file);
    if write_result.is_err() {
        let _ = rustix::fs::unlinkat(&directory, &temporary, AtFlags::empty());
        return Err(());
    }
    if rustix::fs::renameat(&directory, &temporary, &directory, PROBE_FILE).is_err() {
        let _ = rustix::fs::unlinkat(&directory, &temporary, AtFlags::empty());
        return Err(());
    }
    rustix::fs::fsync(&directory).map_err(|_| ())
}

#[cfg(not(unix))]
fn write_probe(_content: &str) -> Result<(), ()> {
    Err(())
}

#[cfg(unix)]
fn read_probe() -> Result<String, ()> {
    let directory = open_workspace_directory()?;
    let descriptor = rustix::fs::openat(
        directory,
        PROBE_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())?;
    require_private_regular_file(&descriptor)?;
    let file = File::from(descriptor);
    let mut bytes = Vec::new();
    file.take((MAX_PROBE_CONTENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > MAX_PROBE_CONTENT_BYTES {
        return Err(());
    }
    String::from_utf8(bytes).map_err(|_| ())
}

#[cfg(not(unix))]
fn read_probe() -> Result<String, ()> {
    Err(())
}

#[cfg(unix)]
fn open_workspace_directory() -> Result<rustix::fd::OwnedFd, ()> {
    rustix::fs::open(
        ".",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())
}

#[cfg(unix)]
fn require_private_regular_file(file: impl std::os::fd::AsFd) -> Result<(), ()> {
    let stat = rustix::fs::fstat(file).map_err(|_| ())?;
    if FileType::from_raw_mode(stat.st_mode).is_file() && stat.st_nlink == 1 {
        Ok(())
    } else {
        Err(())
    }
}

fn initialize_response(id: Value) -> Value {
    rpc_result(
        id,
        json!({
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
        }),
    )
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}
