use crate::acp::protocol::{
    parse_config_options, parse_usage_report, ConfigOption, JsonRpcMessage, JsonRpcRequest,
    JsonRpcResponse, UsageReport,
};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, trace};

/// Pick the most permissive selectable permission option from ACP options.
fn pick_best_option(options: &[Value]) -> Option<String> {
    let mut fallback: Option<&Value> = None;

    for kind in ["allow_always", "allow_once"] {
        if let Some(option) = options
            .iter()
            .find(|option| option.get("kind").and_then(|k| k.as_str()) == Some(kind))
        {
            return option
                .get("optionId")
                .and_then(|id| id.as_str())
                .map(str::to_owned);
        }
    }

    for option in options {
        let kind = option.get("kind").and_then(|k| k.as_str());
        if kind == Some("reject_once") || kind == Some("reject_always") {
            continue;
        }
        fallback = Some(option);
        break;
    }

    fallback
        .and_then(|option| option.get("optionId"))
        .and_then(|id| id.as_str())
        .map(str::to_owned)
}

/// Build a spec-compliant permission response with backward-compatible fallback.
fn build_permission_response(params: Option<&Value>) -> Value {
    match params
        .and_then(|p| p.get("options"))
        .and_then(|options| options.as_array())
    {
        None => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": "allow_always"
            }
        }),
        Some(options) => {
            if let Some(option_id) = pick_best_option(options) {
                json!({
                    "outcome": {
                        "outcome": "selected",
                        "optionId": option_id
                    }
                })
            } else {
                json!({
                    "outcome": {
                        "outcome": "cancelled"
                    }
                })
            }
        }
    }
}

fn expand_env(val: &str) -> String {
    if val.starts_with("${") && val.ends_with('}') {
        let key = &val[2..val.len() - 1];
        std::env::var(key).unwrap_or_default()
    } else {
        val.to_string()
    }
}

const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const LIFECYCLE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const OPENAB_META_NAMESPACE: &str = "openab.dev";

pub(crate) type PendingRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LifecycleCapabilities {
    pub(crate) close: bool,
    pub(crate) release_v1: bool,
}

pub(crate) fn parse_lifecycle_capabilities(
    initialize_result: Option<&Value>,
) -> LifecycleCapabilities {
    let session_capabilities = initialize_result
        .and_then(|result| result.get("agentCapabilities"))
        .and_then(|capabilities| capabilities.get("sessionCapabilities"));

    LifecycleCapabilities {
        close: session_capabilities
            .and_then(|capabilities| capabilities.get("close"))
            .is_some_and(Value::is_object),
        release_v1: session_capabilities
            .and_then(|capabilities| capabilities.get("_meta"))
            .and_then(|meta| meta.get(OPENAB_META_NAMESPACE))
            .and_then(|openab| openab.get("sessionRelease"))
            .and_then(|release| release.get("version"))
            .and_then(Value::as_u64)
            == Some(1),
    }
}

async fn write_bounded_line<W>(
    writer: &Arc<Mutex<W>>,
    data: &str,
    write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    tokio::time::timeout(write_timeout, async {
        let mut writer = writer.lock().await;
        writer.write_all(data.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("stdin write timeout"))??;
    Ok(())
}

pub(crate) async fn send_bounded_request<W>(
    writer: &Arc<Mutex<W>>,
    next_id: &AtomicU64,
    pending: &PendingRequests,
    method: &str,
    params: Option<Value>,
    write_timeout: Duration,
    response_timeout: Duration,
) -> Result<JsonRpcMessage>
where
    W: AsyncWrite + Unpin + Send,
{
    let id = next_id.fetch_add(1, Ordering::Relaxed);
    let request = JsonRpcRequest::new(id, method, params);
    let data = serde_json::to_string(&request)?;
    let (response_tx, response_rx) = oneshot::channel();
    pending.lock().await.insert(id, response_tx);

    debug!(data = data.trim(), "acp_send");
    if let Err(error) = write_bounded_line(writer, &data, write_timeout).await {
        pending.lock().await.remove(&id);
        return Err(error);
    }

    let response = match tokio::time::timeout(response_timeout, response_rx).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            pending.lock().await.remove(&id);
            return Err(anyhow!("channel closed waiting for {method}"));
        }
        Err(_) => {
            pending.lock().await.remove(&id);
            return Err(anyhow!("timeout waiting for {method} response"));
        }
    };

    if let Some(error) = &response.error {
        return Err(anyhow!("{error}"));
    }
    Ok(response)
}

async fn send_bounded_notification<W>(
    writer: &Arc<Mutex<W>>,
    method: &str,
    params: Value,
    write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let data = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    }))?;
    debug!(data = data.trim(), "acp_send");
    write_bounded_line(writer, &data, write_timeout).await
}

struct AcpLifecycleHandle<W> {
    writer: Arc<Mutex<W>>,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    session_id: String,
    capabilities: LifecycleCapabilities,
}

impl<W> AcpLifecycleHandle<W>
where
    W: AsyncWrite + Unpin + Send,
{
    fn new(
        writer: Arc<Mutex<W>>,
        next_id: Arc<AtomicU64>,
        pending: PendingRequests,
        session_id: String,
        capabilities: LifecycleCapabilities,
    ) -> Self {
        Self {
            writer,
            next_id,
            pending,
            session_id,
            capabilities,
        }
    }

    async fn cancel_inner(&self) -> Result<()> {
        send_bounded_notification(
            &self.writer,
            "session/cancel",
            json!({"sessionId": self.session_id}),
            CONTROL_WRITE_TIMEOUT,
        )
        .await
    }

    async fn close_inner(&self) -> Result<()> {
        if !self.capabilities.close {
            return Err(anyhow!("session/close capability was not advertised"));
        }
        let response = send_bounded_request(
            &self.writer,
            &self.next_id,
            &self.pending,
            "session/close",
            Some(json!({"sessionId": self.session_id})),
            CONTROL_WRITE_TIMEOUT,
            LIFECYCLE_RESPONSE_TIMEOUT,
        )
        .await?;
        if !response.result.as_ref().is_some_and(Value::is_object) {
            return Err(anyhow!("session/close returned an invalid acknowledgement"));
        }
        Ok(())
    }

    async fn release_inner(&self) -> Result<()> {
        if !self.capabilities.release_v1 {
            return Err(anyhow!(
                "_openab/session/release capability was not advertised"
            ));
        }
        let response = send_bounded_request(
            &self.writer,
            &self.next_id,
            &self.pending,
            "_openab/session/release",
            Some(json!({"sessionId": self.session_id})),
            CONTROL_WRITE_TIMEOUT,
            LIFECYCLE_RESPONSE_TIMEOUT,
        )
        .await?;
        if !response.result.as_ref().is_some_and(Value::is_object) {
            return Err(anyhow!(
                "_openab/session/release returned an invalid acknowledgement"
            ));
        }
        Ok(())
    }
}

/// Lock-free lifecycle controls for an initialized isolated-session bridge.
///
/// Implementations must bound their own writes and request waits so pool
/// lifecycle paths never hold state indefinitely.
#[async_trait::async_trait]
pub(crate) trait SessionLifecycleControl: Send + Sync {
    fn capabilities(&self) -> LifecycleCapabilities;
    async fn cancel(&self) -> Result<()>;
    // The pool integration calls this through LifecycleHandle. Keep a default
    // so existing local/test controls remain inert unless they opt in.
    #[allow(dead_code)]
    async fn close(&self) -> Result<()> {
        Err(anyhow!("session/close lifecycle control is not available"))
    }
    async fn release(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl SessionLifecycleControl for AcpLifecycleHandle<ChildStdin> {
    fn capabilities(&self) -> LifecycleCapabilities {
        self.capabilities
    }

    async fn cancel(&self) -> Result<()> {
        self.cancel_inner().await
    }

    async fn close(&self) -> Result<()> {
        self.close_inner().await
    }

    async fn release(&self) -> Result<()> {
        self.release_inner().await
    }
}

pub(crate) type LifecycleHandle = Arc<dyn SessionLifecycleControl>;

/// A content block for the ACP prompt — either text or image.
#[derive(Debug, Clone)]
pub enum ContentBlock {
    Text { text: String },
    Image { media_type: String, data: String },
}

impl ContentBlock {
    pub fn to_json(&self) -> Value {
        match self {
            ContentBlock::Text { text } => json!({
                "type": "text",
                "text": text
            }),
            ContentBlock::Image { media_type, data } => json!({
                "type": "image",
                "data": data,
                "mimeType": media_type
            }),
        }
    }
}

/// Lock-free view of session activity, readable without the connection mutex.
pub struct SessionActivity {
    /// Milliseconds since process boot (monotonic) of the last observed activity.
    last_active_ms: AtomicU64,
    /// True while a prompt turn is in flight (mutex likely held).
    prompt_in_flight: AtomicBool,
}

impl Default for SessionActivity {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionActivity {
    pub fn new() -> Self {
        Self {
            last_active_ms: AtomicU64::new(Self::now_ms()),
            prompt_in_flight: AtomicBool::new(false),
        }
    }

    /// Monotonic milliseconds since first use (process boot). SystemTime is
    /// unsuitable here: a wall-clock step (NTP, manual change) could make an
    /// active session look hours stale and trigger a false hung eviction.
    fn now_ms() -> u64 {
        use std::sync::OnceLock;
        static BOOT: OnceLock<std::time::Instant> = OnceLock::new();
        BOOT.get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis() as u64
    }

    pub fn touch(&self) {
        self.last_active_ms.store(Self::now_ms(), Ordering::Release);
    }

    pub fn set_in_flight(&self, in_flight: bool) {
        self.prompt_in_flight.store(in_flight, Ordering::Release);
    }

    /// Milliseconds since process boot of the last observed activity.
    pub fn last_active_ms(&self) -> u64 {
        self.last_active_ms.load(Ordering::Acquire)
    }

    /// Elapsed time since the last observed activity (saturating at zero).
    pub fn age(&self) -> std::time::Duration {
        let last = self.last_active_ms.load(Ordering::Acquire);
        std::time::Duration::from_millis(Self::now_ms().saturating_sub(last))
    }

    pub fn in_flight(&self) -> bool {
        self.prompt_in_flight.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_last_active_ms(&self, ms: u64) {
        self.last_active_ms.store(ms, Ordering::Release);
    }
}

pub struct AcpConnection {
    _proc: Child,
    /// PID of the direct child, used as the process group ID for cleanup.
    child_pgid: Option<i32>,
    stdin: Arc<Mutex<ChildStdin>>,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>>,
    pub acp_session_id: Option<String>,
    pub supports_load_session: bool,
    lifecycle_capabilities: LifecycleCapabilities,
    requires_openab_lifecycle: bool,
    /// Agent name from `initialize` (`agentInfo.name`), e.g. "Kiro CLI Agent".
    /// Used to gate agent-specific extension methods.
    pub agent_name: String,
    pub config_options: Vec<ConfigOption>,
    pub last_active: Instant,
    pub activity: Arc<SessionActivity>,
    pub session_reset: bool,
    _reader_handle: JoinHandle<()>,
    _stderr_handle: Option<JoinHandle<()>>,
}

/// Broker-owned context injected when spawning an ACP process for a logical
/// session. This is deliberately separate from operator-controlled `[agent]`
/// environment configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionSpawnContext {
    logical_session_key: String,
    attempt_id: String,
}

impl SessionSpawnContext {
    pub(crate) fn new(logical_session_key: impl Into<String>) -> Self {
        Self::from_parts(logical_session_key, uuid::Uuid::new_v4().to_string())
    }

    fn from_parts(logical_session_key: impl Into<String>, attempt_id: impl Into<String>) -> Self {
        Self {
            logical_session_key: logical_session_key.into(),
            attempt_id: attempt_id.into(),
        }
    }

    pub(crate) fn logical_session_key(&self) -> &str {
        &self.logical_session_key
    }

    pub(crate) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
}

fn session_spawn_env(context: Option<&SessionSpawnContext>) -> Vec<(&'static str, &str)> {
    context
        .map(|context| {
            vec![
                (super::SESSION_KEY_ENV, context.logical_session_key()),
                (super::SESSION_ATTEMPT_ID_ENV, context.attempt_id()),
            ]
        })
        .unwrap_or_default()
}

/// Build the final set of env vars for the agent subprocess.
/// `explicit` ([agent].env) takes precedence over `inherit` ([agent].inherit_env).
/// Returns (merged env map, list of keys that were inherited from the process).
fn build_agent_env(
    explicit: &std::collections::HashMap<String, String>,
    inherit_keys: &[String],
) -> (std::collections::HashMap<String, String>, Vec<String>) {
    let mut result: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut inherited: Vec<String> = Vec::new();

    for (k, v) in explicit {
        result.insert(k.clone(), expand_env(v));
    }

    for key in inherit_keys {
        if !result.contains_key(key) {
            if let Ok(v) = std::env::var(key) {
                result.insert(key.clone(), v);
                inherited.push(key.clone());
            }
        }
    }

    (result, inherited)
}

/// Reader loop body: reads JSON-RPC messages from `reader`, auto-replies
/// `session/request_permission` via `writer`, resolves pending responses,
/// and forwards notifications + stale id-bearing messages to the active
/// subscriber. Extracted as a free generic function so unit tests can drive
/// it with `tokio::io::duplex()` halves instead of a real child process.
pub(crate) async fn run_reader_loop<R, W>(
    reader: R,
    writer: Arc<Mutex<W>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>>,
    notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                error!("reader error: {e}");
                break;
            }
        }
        let msg: JsonRpcMessage = match serde_json::from_str(line.trim()) {
            Ok(m) => m,
            Err(_) => continue,
        };
        debug!(line = line.trim(), "acp_recv");

        // Auto-reply session/request_permission
        if msg.method.as_deref() == Some("session/request_permission") {
            if let Some(id) = msg.id {
                let title = msg
                    .params
                    .as_ref()
                    .and_then(|p| p.get("toolCall"))
                    .and_then(|t| t.get("title"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("?");

                let outcome = build_permission_response(msg.params.as_ref());
                info!(title, %outcome, "auto-respond permission");
                let reply = JsonRpcResponse::new(id, outcome);
                if let Ok(data) = serde_json::to_string(&reply) {
                    let mut w = writer.lock().await;
                    let _ = w.write_all(format!("{data}\n").as_bytes()).await;
                    let _ = w.flush().await;
                }
            }
            continue;
        }

        // Response (has id) → resolve pending AND forward to subscriber
        if let Some(id) = msg.id {
            let mut map = pending.lock().await;
            if let Some(tx) = map.remove(&id) {
                // Forward to subscriber so they see the completion
                let sub = notify_tx.lock().await;
                if let Some(ntx) = sub.as_ref() {
                    // Clone the essential fields for the subscriber
                    let _ = ntx.send(JsonRpcMessage {
                        id: Some(id),
                        method: None,
                        result: msg.result.clone(),
                        error: msg.error.clone(),
                        params: None,
                    });
                }
                let _ = tx.send(msg);
                continue;
            }
            // Stale id (#732): pending was already abandoned. Falls through
            // to subscriber forwarding; the adapter recv loop filters by
            // request_id so it can't leak into the next prompt.
            trace!(request_id = id, "stale id-bearing message after abandon");
        }

        // Notification → forward to subscriber
        let sub = notify_tx.lock().await;
        if let Some(tx) = sub.as_ref() {
            let _ = tx.send(msg);
        }
    }

    // Connection closed — resolve all pending with error
    let mut map = pending.lock().await;
    for (_, tx) in map.drain() {
        let _ = tx.send(JsonRpcMessage {
            id: None,
            method: None,
            result: None,
            error: Some(crate::acp::protocol::JsonRpcError {
                code: -1,
                message: "connection closed".into(),
                data: None,
            }),
            params: None,
        });
    }
    // Close the notify channel so rx.recv() returns None
    let mut sub = notify_tx.lock().await;
    *sub = None;
}

impl AcpConnection {
    /// Spawn an ordinary ACP child process with no broker-owned session
    /// context. This preserves the public API and existing local-agent
    /// behavior.
    pub async fn spawn(
        command: &str,
        args: &[String],
        working_dir: &str,
        env: &std::collections::HashMap<String, String>,
        inherit_env: &[String],
    ) -> Result<Self> {
        Self::spawn_with_context(command, args, working_dir, env, inherit_env, None).await
    }

    /// Spawn an ACP child with optional broker-owned session context.
    ///
    /// Only an explicitly configured session-runtime bridge should use this
    /// entry point. Ordinary local and AgentCore agents use [`Self::spawn`].
    pub(crate) async fn spawn_with_context(
        command: &str,
        args: &[String],
        working_dir: &str,
        env: &std::collections::HashMap<String, String>,
        inherit_env: &[String],
        session_context: Option<&SessionSpawnContext>,
    ) -> Result<Self> {
        info!(cmd = command, ?args, cwd = working_dir, "spawning agent");
        let requires_openab_lifecycle = session_context.is_some();

        let mut cmd = tokio::process::Command::new(command);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .current_dir(working_dir);
        // Create a new process group so we can kill the entire tree.
        // SAFETY: setpgid is async-signal-safe (POSIX.1-2008) and called
        // before exec. Return value checked — failure means the child won't
        // have its own process group, so kill(-pgid) would be unsafe.
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        #[cfg(windows)]
        {
            cmd.creation_flags(0x00000200); // CREATE_NEW_PROCESS_GROUP
        }
        // Clear inherited env to prevent credential leakage (e.g. DISCORD_BOT_TOKEN).
        // Only [agent].env values + essential baseline vars are passed through.
        cmd.env_clear();
        // Preserve the real HOME so agents can find OAuth/auth files (~/.codex,
        // ~/.claude, ~/.config/gh, etc.). working_dir is already set via
        // current_dir() above and is not necessarily the user's home directory.
        cmd.env(
            "HOME",
            std::env::var("HOME").unwrap_or_else(|_| working_dir.into()),
        );
        cmd.env(
            "PATH",
            std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".into()),
        );
        #[cfg(unix)]
        {
            cmd.env(
                "USER",
                std::env::var("USER").unwrap_or_else(|_| "agent".into()),
            );
        }
        #[cfg(windows)]
        {
            // Windows requires SystemRoot for DLL loading and basic OS functionality.
            // USERPROFILE is the Windows equivalent of HOME.
            cmd.env(
                "USERPROFILE",
                std::env::var("USERPROFILE").unwrap_or_else(|_| working_dir.into()),
            );
            cmd.env(
                "USERNAME",
                std::env::var("USERNAME").unwrap_or_else(|_| "agent".into()),
            );
            if let Ok(v) = std::env::var("SystemRoot") {
                cmd.env("SystemRoot", v);
            }
            if let Ok(v) = std::env::var("SystemDrive") {
                cmd.env("SystemDrive", v);
            }
        }
        for (k, v) in env {
            cmd.env(k, expand_env(v));
        }
        // Inherit selected env vars from the OAB process (e.g. vars injected
        // via Kubernetes envFrom).  Keys already in [agent].env are skipped —
        // explicit values take precedence.
        let (agent_env, inherited_keys) = build_agent_env(env, inherit_env);
        for (k, v) in &agent_env {
            cmd.env(k, v);
        }
        for (key, value) in session_spawn_env(session_context) {
            // Broker-owned context is applied after all operator-controlled
            // environment sources so reserved values cannot be spoofed.
            cmd.env(key, value);
        }
        if !agent_env.is_empty() {
            let explicit_keys: Vec<&String> = env.keys().collect();
            tracing::warn!(
                ?explicit_keys,
                ?inherited_keys,
                "[agent].env/inherit_env is set -- these values are accessible to the agent and could be exfiltrated via prompt injection"
            );
        }
        let mut proc = cmd
            .spawn()
            .map_err(|e| anyhow!("failed to spawn {command}: {e}"))?;
        let child_pgid = proc.id().and_then(|pid| i32::try_from(pid).ok());

        let stdout = proc.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
        let stdin = proc.stdin.take().ok_or_else(|| anyhow!("no stdin"))?;
        let stdin = Arc::new(Mutex::new(stdin));

        // Capture agent stderr and log it (ACP spec: agents MAY write to stderr
        // for logging; clients MAY capture or ignore it).
        let stderr_handle = if let Some(stderr) = proc.stderr.take() {
            let cmd_name = command.to_string();
            Some(tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim();
                            if !trimmed.is_empty() {
                                let sanitized: String = trimmed
                                    .chars()
                                    .filter(|c| !c.is_control() || *c == '\t')
                                    .collect();
                                if !sanitized.is_empty() {
                                    tracing::warn!(agent = %cmd_name, "{sanitized}");
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }))
        } else {
            None
        };

        let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let reader_handle = tokio::spawn(run_reader_loop(
            stdout,
            stdin.clone(),
            pending.clone(),
            notify_tx.clone(),
        ));

        let activity = Arc::new(SessionActivity::new());

        Ok(Self {
            _proc: proc,
            child_pgid,
            stdin,
            next_id: Arc::new(AtomicU64::new(1)),
            pending,
            notify_tx,
            acp_session_id: None,
            supports_load_session: false,
            lifecycle_capabilities: LifecycleCapabilities::default(),
            requires_openab_lifecycle,
            agent_name: String::new(),
            config_options: Vec::new(),
            last_active: Instant::now(),
            activity,
            session_reset: false,
            _reader_handle: reader_handle,
            _stderr_handle: stderr_handle,
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) async fn send_raw(&self, data: &str) -> Result<()> {
        debug!(data = data.trim(), "acp_send");
        write_bounded_line(&self.stdin, data, CONTROL_WRITE_TIMEOUT).await
    }

    async fn send_request(&self, method: &str, params: Option<Value>) -> Result<JsonRpcMessage> {
        let response_timeout = if method == "session/new" {
            Duration::from_secs(120)
        } else {
            Duration::from_secs(30)
        };
        send_bounded_request(
            &self.stdin,
            &self.next_id,
            &self.pending,
            method,
            params,
            CONTROL_WRITE_TIMEOUT,
            response_timeout,
        )
        .await
    }

    pub async fn initialize(&mut self) -> Result<()> {
        let resp = self
            .send_request(
                "initialize",
                Some(json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": {"name": "openab", "version": "0.1.0"},
                })),
            )
            .await?;

        let result = resp.result.as_ref();
        let agent_name = result
            .and_then(|r| r.get("agentInfo"))
            .and_then(|a| a.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown");
        self.agent_name = agent_name.to_string();
        self.supports_load_session = result
            .and_then(|r| r.get("agentCapabilities"))
            .and_then(|c| c.get("loadSession"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.lifecycle_capabilities = parse_lifecycle_capabilities(result);
        if self.requires_openab_lifecycle {
            if !self.supports_load_session {
                return Err(anyhow!(
                    "isolated session bridge did not advertise loadSession"
                ));
            }
            if !self.lifecycle_capabilities.close {
                return Err(anyhow!(
                    "isolated session bridge did not advertise session/close"
                ));
            }
            if !self.lifecycle_capabilities.release_v1 {
                return Err(anyhow!(
                    "isolated session bridge did not advertise {OPENAB_META_NAMESPACE} sessionRelease v1"
                ));
            }
        }
        info!(
            agent = agent_name,
            load_session = self.supports_load_session,
            close_session = self.lifecycle_capabilities.close,
            openab_release_v1 = self.lifecycle_capabilities.release_v1,
            "initialized"
        );
        Ok(())
    }

    pub async fn session_new(&mut self, cwd: &str) -> Result<String> {
        let resp = self
            .send_request("session/new", Some(json!({"cwd": cwd, "mcpServers": []})))
            .await?;

        let session_id = resp
            .result
            .as_ref()
            .and_then(|r| r.get("sessionId"))
            .and_then(|s| s.as_str())
            .ok_or_else(|| anyhow!("no sessionId in session/new response"))?
            .to_string();

        info!(session_id = %session_id, "session created");
        self.acp_session_id = Some(session_id.clone());
        if let Some(result) = resp.result.as_ref() {
            self.config_options = parse_config_options(result);
            if !self.config_options.is_empty() {
                info!(count = self.config_options.len(), "parsed configOptions");
            }
        }
        Ok(session_id)
    }

    /// Set a config option (e.g. model, mode) via ACP session/set_config_option.
    /// Returns the updated list of all config options.
    pub async fn set_config_option(
        &mut self,
        config_id: &str,
        value: &str,
    ) -> Result<Vec<ConfigOption>> {
        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?
            .clone();

        let resp = self
            .send_request(
                "session/set_config_option",
                Some(json!({
                    "sessionId": session_id,
                    "configId": config_id,
                    "value": value,
                })),
            )
            .await;

        match resp {
            Ok(r) => {
                if let Some(result) = r.result.as_ref() {
                    self.config_options = parse_config_options(result);
                }
                info!(config_id, value, "config option set");
            }
            Err(_) => {
                // Fall back: send as a slash command (e.g. "/model claude-sonnet-4")
                let cmd = format!("/{config_id} {value}");
                info!(
                    cmd,
                    "set_config_option not supported, falling back to prompt"
                );
                let _resp = self
                    .send_request(
                        "session/prompt",
                        Some(json!({
                            "sessionId": session_id,
                            "prompt": [{"type": "text", "text": cmd}],
                        })),
                    )
                    .await?;
                for opt in &mut self.config_options {
                    if opt.id == config_id {
                        opt.current_value = value.to_string();
                    }
                }
            }
        }

        Ok(self.config_options.clone())
    }

    /// Query account-level usage/billing via kiro-cli's
    /// `_kiro.dev/commands/execute` extension (the `/usage` slash command).
    ///
    /// This is a Kiro-specific extension, not part of the ACP spec, and the
    /// request shape is strict: a malformed `command` value is a
    /// deserialization error that kills the whole ACP connection (no JSON-RPC
    /// error is returned). We therefore gate on the agent name advertised in
    /// `initialize` and never retry on failure.
    pub async fn get_usage(&mut self) -> Result<UsageReport> {
        if !self.agent_name.to_ascii_lowercase().contains("kiro") {
            return Err(anyhow!(
                "usage query is not supported by this backend ({})",
                if self.agent_name.is_empty() {
                    "unknown agent"
                } else {
                    &self.agent_name
                }
            ));
        }
        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?
            .clone();

        let resp = self
            .send_request(
                "_kiro.dev/commands/execute",
                Some(json!({
                    "sessionId": session_id,
                    // Adjacently-tagged TuiCommand enum: tag = "command", content = "args".
                    "command": {"command": "usage", "args": {}},
                })),
            )
            .await?;

        let result = resp
            .result
            .as_ref()
            .ok_or_else(|| anyhow!("empty usage response"))?;
        parse_usage_report(result)
            .ok_or_else(|| anyhow!("could not parse usage response from agent"))
    }

    /// Send a prompt with content blocks (text and/or images) and return a receiver
    /// for streaming notifications. The final message on the channel will have id set
    /// (the prompt response).
    pub async fn session_prompt(
        &mut self,
        content_blocks: Vec<ContentBlock>,
    ) -> Result<(mpsc::UnboundedReceiver<JsonRpcMessage>, u64)> {
        self.last_active = Instant::now();
        self.activity.touch();
        self.activity.set_in_flight(true);

        let session_id = self
            .acp_session_id
            .as_ref()
            .ok_or_else(|| anyhow!("no session"))?;

        let (tx, rx) = mpsc::unbounded_channel();
        *self.notify_tx.lock().await = Some(tx);

        let id = self.next_id();

        // Convert content blocks to JSON
        let prompt_json: Vec<Value> = content_blocks.iter().map(|b| b.to_json()).collect();

        let req = JsonRpcRequest::new(
            id,
            "session/prompt",
            Some(json!({
                "sessionId": session_id,
                "prompt": prompt_json,
            })),
        );
        let data = serde_json::to_string(&req)?;

        let (resp_tx, _resp_rx) = oneshot::channel();
        self.pending.lock().await.insert(id, resp_tx);

        self.send_raw(&data).await?;
        Ok((rx, id))
    }

    /// Call after prompt streaming is done to clean up subscriber.
    pub async fn prompt_done(&mut self) {
        *self.notify_tx.lock().await = None;
        self.activity.touch();
        self.activity.set_in_flight(false);
        self.last_active = Instant::now();
    }

    /// Drop the pending entry for `request_id` and best-effort send
    /// `session/cancel` as a JSON-RPC notification (no id; per ACP spec the
    /// agent does not reply). Errors are swallowed: the agent process may
    /// already be dead, in which case the stdin write fails harmlessly.
    /// See #732.
    pub async fn abandon_request(&self, request_id: u64) {
        self.pending.lock().await.remove(&request_id);
        let Some(session_id) = self.acp_session_id.as_deref() else {
            return;
        };
        let req = json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id},
        });
        if let Ok(data) = serde_json::to_string(&req) {
            let _ = self.send_raw(&data).await;
        }
    }

    /// Return a clone of the stdin handle for lock-free cancel.
    pub fn cancel_handle(&self) -> Arc<Mutex<ChildStdin>> {
        Arc::clone(&self.stdin)
    }

    pub(crate) fn lifecycle_handle(&self) -> Option<LifecycleHandle> {
        if !self.requires_openab_lifecycle {
            return None;
        }
        let session_id = self.acp_session_id.clone()?;
        Some(Arc::new(AcpLifecycleHandle::new(
            Arc::clone(&self.stdin),
            Arc::clone(&self.next_id),
            Arc::clone(&self.pending),
            session_id,
            self.lifecycle_capabilities,
        )))
    }

    pub fn activity_handle(&self) -> Arc<SessionActivity> {
        Arc::clone(&self.activity)
    }

    /// Process-group id of the agent child, readable without any lock state.
    pub fn child_pgid(&self) -> Option<i32> {
        self.child_pgid
    }

    pub fn alive(&self) -> bool {
        !self._reader_handle.is_finished()
    }

    /// Resume a previous session by ID. Returns Ok(()) if the agent accepted
    /// the load, or an error if it failed (caller should fall back to session/new).
    pub async fn session_load(&mut self, session_id: &str, cwd: &str) -> Result<()> {
        let resp = self
            .send_request(
                "session/load",
                Some(json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []})),
            )
            .await?;
        // Accept any non-error response as success
        if resp.error.is_some() {
            return Err(anyhow!("session/load rejected"));
        }
        info!(session_id, "session loaded");
        self.acp_session_id = Some(session_id.to_string());
        if let Some(result) = resp.result.as_ref() {
            self.config_options = parse_config_options(result);
        }
        Ok(())
    }

    /// Kill the entire process group: SIGTERM → SIGKILL.
    /// Uses std::thread (not tokio::spawn) so SIGKILL fires even during
    /// runtime shutdown or panic unwinding.
    fn kill_process_group(&mut self) {
        let pgid = match self.child_pgid {
            Some(pid) if pid > 0 => pid,
            _ => return,
        };
        #[cfg(unix)]
        {
            // Stage 1: SIGTERM the process group
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
            }
            // Stage 2: SIGKILL after brief grace (std::thread survives runtime shutdown)
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(1500));
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            });
        }
        #[cfg(not(unix))]
        {
            let _ = pgid; // suppress unused warning on Windows
        }
    }
}

impl Drop for AcpConnection {
    fn drop(&mut self) {
        if let Some(handle) = self._stderr_handle.take() {
            handle.abort();
        }
        self.kill_process_group();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_agent_env, build_permission_response, pick_best_option, session_spawn_env,
        SessionSpawnContext,
    };
    use serde_json::json;

    #[test]
    fn no_session_spawn_context_adds_no_reserved_env() {
        assert!(session_spawn_env(None).is_empty());
    }

    #[test]
    fn openab_v1_session_spawn_context_maps_broker_owned_values() {
        let context = SessionSpawnContext::from_parts("discord:thread-123", "attempt-123");

        assert_eq!(
            session_spawn_env(Some(&context)),
            vec![
                ("OPENAB_SESSION_KEY", "discord:thread-123"),
                ("OPENAB_SESSION_ATTEMPT_ID", "attempt-123"),
            ]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn broker_session_context_overrides_programmatic_agent_env() {
        let temp = tempfile::tempdir().unwrap();
        let key_output = temp.path().join("session-key");
        let attempt_output = temp.path().join("session-attempt");
        let args = vec![
            "-c".to_string(),
            r#"printf '%s' "$OPENAB_SESSION_KEY" > "$1"; printf '%s' "$OPENAB_SESSION_ATTEMPT_ID" > "$2""#
                .to_string(),
            "openab-session-env-test".to_string(),
            key_output.to_string_lossy().to_string(),
            attempt_output.to_string_lossy().to_string(),
        ];
        let mut configured_env = std::collections::HashMap::new();
        configured_env.insert("OPENAB_SESSION_KEY".to_string(), "spoofed".to_string());
        configured_env.insert(
            "OPENAB_SESSION_ATTEMPT_ID".to_string(),
            "spoofed-attempt".to_string(),
        );
        let context = SessionSpawnContext::from_parts("discord:thread-123", "attempt-123");

        let connection = super::AcpConnection::spawn_with_context(
            "/bin/sh",
            &args,
            temp.path().to_string_lossy().as_ref(),
            &configured_env,
            &[],
            Some(&context),
        )
        .await
        .unwrap();

        let observed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let (Ok(key), Ok(attempt)) = (
                    tokio::fs::read_to_string(&key_output).await,
                    tokio::fs::read_to_string(&attempt_output).await,
                ) {
                    break (key, attempt);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            observed,
            ("discord:thread-123".to_string(), "attempt-123".to_string())
        );
        drop(connection);
    }

    #[test]
    fn picks_allow_always_over_other_options() {
        let options = vec![
            json!({"kind": "allow_once", "optionId": "once"}),
            json!({"kind": "allow_always", "optionId": "always"}),
            json!({"kind": "reject_once", "optionId": "reject"}),
        ];

        assert_eq!(pick_best_option(&options), Some("always".to_string()));
    }

    #[test]
    fn falls_back_to_first_unknown_non_reject_kind() {
        let options = vec![
            json!({"kind": "reject_once", "optionId": "reject"}),
            json!({"kind": "workspace_write", "optionId": "workspace-write"}),
        ];

        assert_eq!(
            pick_best_option(&options),
            Some("workspace-write".to_string())
        );
    }

    #[test]
    fn selects_bypass_permissions_for_exit_plan_mode() {
        let options = vec![
            json!({"optionId": "bypassPermissions", "kind": "allow_always"}),
            json!({"optionId": "acceptEdits", "kind": "allow_always"}),
            json!({"optionId": "default", "kind": "allow_once"}),
            json!({"optionId": "plan", "kind": "reject_once"}),
        ];

        assert_eq!(
            pick_best_option(&options),
            Some("bypassPermissions".to_string())
        );
    }

    #[test]
    fn returns_none_when_only_reject_options_exist() {
        let options = vec![
            json!({"kind": "reject_once", "optionId": "reject-once"}),
            json!({"kind": "reject_always", "optionId": "reject-always"}),
        ];

        assert_eq!(pick_best_option(&options), None);
    }

    #[test]
    fn builds_cancelled_outcome_when_no_selectable_option_exists() {
        let response = build_permission_response(Some(&json!({
            "options": [
                {"kind": "reject_once", "optionId": "reject-once"}
            ]
        })));

        assert_eq!(response, json!({"outcome": {"outcome": "cancelled"}}));
    }

    #[test]
    fn builds_cancelled_when_options_array_is_empty() {
        let response = build_permission_response(Some(&json!({
            "options": []
        })));

        assert_eq!(response, json!({"outcome": {"outcome": "cancelled"}}));
    }

    #[test]
    fn falls_back_to_allow_always_when_options_are_missing() {
        let response = build_permission_response(Some(&json!({
            "toolCall": {"title": "legacy"}
        })));

        assert_eq!(
            response,
            json!({"outcome": {"outcome": "selected", "optionId": "allow_always"}})
        );
    }

    #[test]
    fn falls_back_to_allow_always_when_params_is_none() {
        let response = build_permission_response(None);

        assert_eq!(
            response,
            json!({"outcome": {"outcome": "selected", "optionId": "allow_always"}})
        );
    }

    #[test]
    fn explicit_env_takes_precedence_over_inherit_env() {
        let key = "OAB_TEST_PRECEDENCE";
        std::env::set_var(key, "from_process");
        let mut explicit = std::collections::HashMap::new();
        explicit.insert(key.to_string(), "from_config".to_string());
        let inherit = vec![key.to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert_eq!(result.get(key).unwrap(), "from_config");
        assert!(!inherited.contains(&key.to_string()));
        std::env::remove_var(key);
    }

    #[test]
    fn inherit_env_copies_from_process() {
        let key = "OAB_TEST_INHERIT";
        std::env::set_var(key, "process_value");
        let explicit = std::collections::HashMap::new();
        let inherit = vec![key.to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert_eq!(result.get(key).unwrap(), "process_value");
        assert!(inherited.contains(&key.to_string()));
        std::env::remove_var(key);
    }

    #[test]
    fn inherit_env_skips_missing_vars() {
        let explicit = std::collections::HashMap::new();
        let inherit = vec!["OAB_TEST_NONEXISTENT_VAR_12345".to_string()];

        let (result, inherited) = build_agent_env(&explicit, &inherit);

        assert!(!result.contains_key("OAB_TEST_NONEXISTENT_VAR_12345"));
        assert!(inherited.is_empty());
    }
}

#[cfg(test)]
mod reader_loop_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncWriteExt};
    use tokio::sync::{mpsc, oneshot, Mutex};

    /// #732 stale-id path: when a response arrives for an id the broker has
    /// already abandoned, the reader must (a) not crash, (b) leave `pending`
    /// untouched, and (c) still forward the message to whoever is currently
    /// subscribed — the adapter recv loop is responsible for filtering by
    /// request_id so the stray response never leaks into the next prompt.
    #[tokio::test]
    async fn stale_id_response_is_forwarded_without_pending_entry() {
        let (mut agent_stdout_writer, agent_stdout_reader) = duplex(8 * 1024);
        let (agent_stdin_writer, _agent_stdin_reader) = duplex(8 * 1024);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        *notify_tx.lock().await = Some(sub_tx);

        let writer = Arc::new(Mutex::new(agent_stdin_writer));
        let handle = tokio::spawn(run_reader_loop(
            agent_stdout_reader,
            writer,
            pending.clone(),
            notify_tx.clone(),
        ));

        let stale = b"{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{\"stopReason\":\"ok\"}}\n";
        agent_stdout_writer.write_all(stale).await.unwrap();
        agent_stdout_writer.flush().await.unwrap();

        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(2), sub_rx.recv())
            .await
            .expect("subscriber should receive stale message before timeout")
            .expect("subscriber channel should not be closed");
        assert_eq!(forwarded.id, Some(42));
        assert!(pending.lock().await.is_empty());

        drop(agent_stdout_writer);
        handle.await.unwrap();
    }

    /// Matched-id path: when a response's id is in `pending`, the loop must
    /// resolve the oneshot AND forward a copy to the subscriber so the
    /// adapter's recv loop sees the completion. Guards against regressions
    /// that would suppress the forward branch while keeping resolve.
    #[tokio::test]
    async fn matched_id_response_resolves_pending_and_forwards() {
        let (mut agent_stdout_writer, agent_stdout_reader) = duplex(8 * 1024);
        let (agent_stdin_writer, _agent_stdin_reader) = duplex(8 * 1024);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));

        let (resp_tx, resp_rx) = oneshot::channel();
        pending.lock().await.insert(7, resp_tx);

        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
        *notify_tx.lock().await = Some(sub_tx);

        let writer = Arc::new(Mutex::new(agent_stdin_writer));
        let handle = tokio::spawn(run_reader_loop(
            agent_stdout_reader,
            writer,
            pending.clone(),
            notify_tx.clone(),
        ));

        let payload = b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"stopReason\":\"end_turn\"}}\n";
        agent_stdout_writer.write_all(payload).await.unwrap();
        agent_stdout_writer.flush().await.unwrap();

        let resolved = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx)
            .await
            .expect("oneshot should resolve")
            .expect("oneshot should not be cancelled");
        assert_eq!(resolved.id, Some(7));

        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(2), sub_rx.recv())
            .await
            .expect("subscriber should receive forwarded copy")
            .expect("subscriber channel should not be closed");
        assert_eq!(forwarded.id, Some(7));
        assert!(pending.lock().await.is_empty());

        drop(agent_stdout_writer);
        handle.await.unwrap();
    }

    #[test]
    fn session_activity_touch_advances_last_active() {
        let activity = SessionActivity::new();
        // Warm the monotonic clock past zero so a backdated value is older.
        std::thread::sleep(std::time::Duration::from_millis(10));
        activity.set_last_active_ms(0);
        let before = activity.last_active_ms();
        activity.touch();
        assert!(activity.last_active_ms() > before);
        // Backdated last_active yields a positive age; touch resets it near zero.
        activity.set_last_active_ms(0);
        assert!(activity.age() >= std::time::Duration::from_millis(10));
        activity.touch();
        assert!(activity.age() < std::time::Duration::from_secs(60));
        // A future timestamp must not underflow: age saturates at zero.
        activity.set_last_active_ms(u64::MAX);
        assert_eq!(activity.age(), std::time::Duration::ZERO);
    }

    #[test]
    fn session_activity_set_in_flight_round_trips() {
        let activity = SessionActivity::new();
        assert!(!activity.in_flight());
        activity.set_in_flight(true);
        assert!(activity.in_flight());
        activity.set_in_flight(false);
        assert!(!activity.in_flight());
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::{
        parse_lifecycle_capabilities, run_reader_loop, send_bounded_request, AcpLifecycleHandle,
        LifecycleCapabilities, PendingRequests,
    };
    use crate::acp::protocol::JsonRpcMessage;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::{mpsc, Mutex};
    use tokio::time::Duration;

    fn pending_requests() -> PendingRequests {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn response(id: u64, result: serde_json::Value) -> JsonRpcMessage {
        JsonRpcMessage {
            id: Some(id),
            method: None,
            result: Some(result),
            error: None,
            params: None,
        }
    }

    async fn read_json<R>(reader: &mut BufReader<R>) -> serde_json::Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    async fn run_lifecycle_request(
        result: serde_json::Value,
        close: bool,
    ) -> (serde_json::Value, anyhow::Result<()>) {
        let (writer, peer) = duplex(8 * 1024);
        let pending = pending_requests();
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&pending),
            "outer-session".to_string(),
            LifecycleCapabilities {
                close: true,
                release_v1: true,
            },
        );
        let request = tokio::spawn(async move {
            if close {
                handle.close_inner().await
            } else {
                handle.release_inner().await
            }
        });
        let sent = read_json(&mut BufReader::new(peer)).await;
        let id = sent["id"].as_u64().unwrap();
        pending
            .lock()
            .await
            .remove(&id)
            .expect("pending lifecycle request")
            .send(response(id, result))
            .expect("lifecycle receiver");
        (sent, request.await.unwrap())
    }

    #[test]
    fn lifecycle_capabilities_require_exact_shapes() {
        let supported = json!({
            "agentCapabilities": {
                "sessionCapabilities": {
                    "close": {},
                    "_meta": {
                        "openab.dev": {
                            "sessionRelease": {"version": 1}
                        }
                    }
                }
            }
        });
        assert_eq!(
            parse_lifecycle_capabilities(Some(&supported)),
            LifecycleCapabilities {
                close: true,
                release_v1: true,
            }
        );

        for (index, unsupported) in [
            json!({"agentCapabilities": {"sessionCapabilities": {
                "close": true,
                "_meta": {"openab.dev": {"sessionRelease": {"version": 2}}}
            }}}),
            json!({"agentCapabilities": {
                "sessionCapabilities": {"close": {}},
                "_meta": {"openab.dev": {"sessionRelease": {"version": 1}}}
            }}),
        ]
        .iter()
        .enumerate()
        {
            let capabilities = parse_lifecycle_capabilities(Some(unsupported));
            assert!(!capabilities.release_v1);
            if index == 0 {
                assert!(!capabilities.close);
            }
        }
        assert_eq!(
            parse_lifecycle_capabilities(None),
            LifecycleCapabilities::default()
        );
    }

    #[tokio::test]
    async fn bounded_request_uses_shared_unique_id_and_resolves_response() {
        let (writer, peer) = duplex(8 * 1024);
        let writer = Arc::new(Mutex::new(writer));
        let pending = pending_requests();
        let next_id = Arc::new(AtomicU64::new(7));
        let request = tokio::spawn({
            let writer = Arc::clone(&writer);
            let pending = Arc::clone(&pending);
            let next_id = Arc::clone(&next_id);
            async move {
                send_bounded_request(
                    &writer,
                    &next_id,
                    &pending,
                    "session/close",
                    Some(json!({"sessionId": "outer-session"})),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                )
                .await
            }
        });

        let sent = read_json(&mut BufReader::new(peer)).await;
        assert_eq!(sent["id"], 7);
        assert_eq!(sent["method"], "session/close");

        let responder = pending.lock().await.remove(&7).expect("pending request");
        responder
            .send(response(7, json!({})))
            .expect("response receiver");

        request.await.unwrap().unwrap();
        assert_eq!(next_id.load(Ordering::Relaxed), 8);
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bounded_request_failures_remove_pending_entries() {
        let (writer, _peer) = duplex(8 * 1024);
        let writer = Arc::new(Mutex::new(writer));
        let pending = pending_requests();
        let next_id = Arc::new(AtomicU64::new(1));

        let error = send_bounded_request(
            &writer,
            &next_id,
            &pending,
            "session/close",
            Some(json!({"sessionId": "outer-session"})),
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("timeout waiting for session/close"));
        assert!(pending.lock().await.is_empty());

        let (writer, peer) = duplex(8 * 1024);
        drop(peer);
        let writer = Arc::new(Mutex::new(writer));

        send_bounded_request(
            &writer,
            &next_id,
            &pending,
            "session/close",
            Some(json!({"sessionId": "outer-session"})),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn release_uses_extension_method_and_requires_object_ack() {
        let (sent, result) = run_lifecycle_request(json!({}), false).await;
        assert_eq!(sent["method"], "_openab/session/release");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        result.unwrap();

        let (_, result) = run_lifecycle_request(serde_json::Value::Null, false).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("invalid acknowledgement"));
    }

    #[tokio::test]
    async fn close_uses_standard_method_and_requires_object_ack() {
        let (sent, result) = run_lifecycle_request(json!({}), true).await;
        assert_eq!(sent["method"], "session/close");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        result.unwrap();

        let (_, result) = run_lifecycle_request(serde_json::Value::Null, true).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("invalid acknowledgement"));
    }

    #[tokio::test]
    async fn close_error_response_clears_shared_pending_entry() {
        let (client_writer, agent_reader) = duplex(8 * 1024);
        let (mut agent_writer, client_reader) = duplex(8 * 1024);
        let (permission_writer, _permission_reader) = duplex(8 * 1024);
        let pending = pending_requests();
        let notify_tx: Arc<Mutex<Option<mpsc::UnboundedSender<JsonRpcMessage>>>> =
            Arc::new(Mutex::new(None));
        let reader = tokio::spawn(run_reader_loop(
            client_reader,
            Arc::new(Mutex::new(permission_writer)),
            Arc::clone(&pending),
            notify_tx,
        ));
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(client_writer)),
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&pending),
            "outer-session".to_string(),
            LifecycleCapabilities {
                close: true,
                release_v1: false,
            },
        );

        let request = tokio::spawn(async move { handle.close_inner().await });
        let sent = read_json(&mut BufReader::new(agent_reader)).await;
        let id = sent["id"].as_u64().unwrap();
        let reply = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "close failed"}
        });
        agent_writer
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .unwrap();
        agent_writer.flush().await.unwrap();

        let error = request.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("close failed"));
        assert!(pending.lock().await.is_empty());

        drop(agent_writer);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_is_notification_without_request_id() {
        let (writer, peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        handle.cancel_inner().await.unwrap();

        let mut peer = BufReader::new(peer);
        let mut line = String::new();
        peer.read_line(&mut line).await.unwrap();
        let sent: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(sent["method"], "session/cancel");
        assert_eq!(sent["params"], json!({"sessionId": "outer-session"}));
        assert!(sent.get("id").is_none());
    }

    #[tokio::test]
    async fn unsupported_release_fails_before_writing() {
        let (writer, mut peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        let error = handle.release_inner().await.unwrap_err();
        assert!(error.to_string().contains("not advertised"));

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(
            Duration::from_millis(10),
            tokio::io::AsyncReadExt::read(&mut peer, &mut byte),
        )
        .await;
        assert!(read.is_err(), "unsupported release must not write");
    }

    #[tokio::test]
    async fn unsupported_close_fails_before_writing() {
        let (writer, mut peer) = duplex(8 * 1024);
        let handle = AcpLifecycleHandle::new(
            Arc::new(Mutex::new(writer)),
            Arc::new(AtomicU64::new(1)),
            pending_requests(),
            "outer-session".to_string(),
            LifecycleCapabilities::default(),
        );

        let error = handle.close_inner().await.unwrap_err();
        assert!(error.to_string().contains("not advertised"));

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(
            Duration::from_millis(10),
            tokio::io::AsyncReadExt::read(&mut peer, &mut byte),
        )
        .await;
        assert!(read.is_err(), "unsupported close must not write");
    }
}
