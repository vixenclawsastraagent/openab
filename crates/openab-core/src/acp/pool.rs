use crate::acp::connection::{
    AcpConnection, LifecycleHandle, SessionActivity, SessionSpawnContext,
};
use crate::acp::protocol::ConfigOption;
use crate::acp::SessionContextMode;
use crate::config::AgentConfig;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};

/// Error substrings produced by `AcpConnection::send_request` that indicate a
/// transient failure worth preserving the session ID for retry, as opposed to
/// a permanent agent-side rejection.
const TRANSIENT_LOAD_ERRORS: &[&str] = &["timeout waiting for", "channel closed"];

/// Combined state protected by a single lock to prevent deadlocks.
/// Lock ordering: never await a per-connection mutex while holding `state`.
struct PoolState {
    /// Active connections: thread_key → AcpConnection handle.
    active: HashMap<String, Arc<Mutex<AcpConnection>>>,
    /// Lock-free cancel handles: thread_key → (stdin, session_id).
    /// Stored separately so cancel can work without locking the connection.
    cancel_handles: HashMap<String, CancelHandle>,
    /// Acknowledged lifecycle controls exist only for explicitly configured
    /// isolated-session bridges.
    lifecycle_handles: HashMap<String, LifecycleHandle>,
    /// Lock-free activity handles for hung-session detection without the connection mutex.
    activity: HashMap<String, Arc<SessionActivity>>,
    /// Child process-group ids, captured at insert time so hung eviction can
    /// kill the agent process without ever locking the connection.
    pgids: HashMap<String, i32>,
    /// Suspended sessions: thread_key → ACP sessionId.
    /// Used at runtime to decide which thread can be resumed via `session/load`
    /// because it no longer has a live in-memory connection.
    suspended: HashMap<String, String>,
    /// Persisted resumable sessions: thread_key → ACP sessionId.
    /// Includes both suspended sessions and active sessions so a process restart
    /// can recover any live thread via `session/load`.
    persisted: HashMap<String, String>,
    /// Serializes create/resume work per thread so rapid same-thread requests
    /// cannot race each other into duplicate `session/load` attempts.
    creating: HashMap<String, Arc<Mutex<()>>>,
    /// Per-session working directory overrides (from control directives).
    /// thread_key → canonical workspace path.
    session_workdirs: HashMap<String, String>,
}

pub struct SessionPool {
    state: RwLock<PoolState>,
    config: AgentConfig,
    session_context: SessionContextMode,
    max_sessions: usize,
    /// Force-evict sessions stuck in-flight longer than this threshold
    /// (`prompt_hard_timeout_secs + hung_grace_secs`, wired in main.rs).
    hung_threshold_secs: u64,
    mapping_path: PathBuf,
    meta_path: PathBuf,
    mapping_load_error: Option<String>,
    default_config_options: HashMap<String, String>,
}

type CancelHandle = (Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>, String);
type SessionGate = Arc<Mutex<()>>;
type ActiveSnapshot = Vec<(String, Arc<Mutex<AcpConnection>>, SessionGate)>;
type EvictionCandidate = (
    String,
    Arc<Mutex<AcpConnection>>,
    Instant,
    Option<String>,
    SessionGate,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrictSuspendOutcome {
    Suspended,
    Orphaned,
    Skipped,
}

fn write_mapping_file(path: &Path, mapping: &HashMap<String, String>) -> Result<()> {
    let data = serde_json::to_vec_pretty(mapping).context("failed to serialize session mapping")?;

    #[cfg(unix)]
    {
        use std::fs::{File, OpenOptions};
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let parent_directory = File::open(parent)
            .with_context(|| format!("failed to open mapping directory {}", parent.display()))?;
        parent_directory.sync_all().with_context(|| {
            format!(
                "mapping directory {} does not support durable updates",
                parent.display()
            )
        })?;
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
        let sequence = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("mapping.json");
        let temporary = parent.join(format!(
            ".{file_name}.tmp.{}.{sequence}",
            std::process::id()
        ));

        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&data)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            // The rename is already committed and visible at this point. A
            // post-rename directory-sync error must not make the caller
            // release the worker while leaving its new mapping installed.
            // The preflight sync above rejects filesystems that do not support
            // directory durability before any mapping state is changed.
            if let Err(error) = parent_directory.sync_all() {
                warn!(
                    path = %path.display(),
                    %error,
                    "mapping was atomically installed but its directory sync failed"
                );
            }
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temporary);
            return Err(error).with_context(|| format!("failed to persist {}", path.display()));
        }
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, data)
            .with_context(|| format!("failed to persist {}", path.display()))?;
    }

    Ok(())
}

fn remove_if_same_handle<T>(
    map: &mut HashMap<String, Arc<Mutex<T>>>,
    key: &str,
    expected: &Arc<Mutex<T>>,
) -> Option<Arc<Mutex<T>>> {
    let should_remove = map
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected));
    if should_remove {
        map.remove(key)
    } else {
        None
    }
}

fn get_or_insert_gate(map: &mut HashMap<String, Arc<Mutex<()>>>, key: &str) -> Arc<Mutex<()>> {
    map.entry(key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn session_spawn_context(
    mode: SessionContextMode,
    logical_session_key: &str,
) -> Option<SessionSpawnContext> {
    match mode {
        SessionContextMode::None => None,
        SessionContextMode::OpenabV1 => Some(SessionSpawnContext::new(logical_session_key)),
    }
}

fn resolve_effective_workdir(
    mode: SessionContextMode,
    stored: Option<&str>,
    requested: Option<&str>,
    default: &str,
) -> Result<String> {
    if mode == SessionContextMode::OpenabV1 && (stored.is_some() || requested.is_some()) {
        return Err(anyhow!(
            "Kubernetes session mode does not accept broker workspace paths"
        ));
    }
    Ok(stored.or(requested).unwrap_or(default).to_string())
}

/// Returns true when a session should be treated as stale during idle cleanup.
fn classify_idle(last_active: Instant, alive: bool, cutoff: Instant) -> bool {
    last_active < cutoff || !alive
}

/// Returns true when a locked, in-flight session has exceeded the hung threshold.
fn classify_hung(
    in_flight: bool,
    last_active_age: std::time::Duration,
    threshold: std::time::Duration,
) -> bool {
    in_flight && last_active_age > threshold
}

/// Returns true when `candidate_last_active` is a better eviction target than `current_oldest`.
fn better_candidate(current_oldest: Option<Instant>, candidate_last_active: Instant) -> bool {
    match current_oldest {
        Some(oldest) => candidate_last_active < oldest,
        None => true,
    }
}

/// Remove every non-`active` pool entry for `key`, reset-style.
///
/// Hung eviction must NOT leave the session resumable: the old streaming task
/// still holds an Arc clone of the connection, so the agent process may be
/// alive and mid-turn. If the session id stayed in `suspended`/`persisted`,
/// the next message would `session/load` the same session while the old
/// process still owns an in-flight turn. Mirror `reset_session` instead.
fn purge_session_entries(state: &mut PoolState, key: &str) {
    state.cancel_handles.remove(key);
    state.lifecycle_handles.remove(key);
    state.activity.remove(key);
    state.pgids.remove(key);
    state.suspended.remove(key);
    state.persisted.remove(key);
    // Do NOT remove the creating gate: it is concurrency control, not session
    // state. Removing it while a holder still owns the old gate Arc would let
    // a concurrent get_or_create mint a fresh gate and run two creations for
    // the same key.
    state.session_workdirs.remove(key);
}

async fn release_strict_session(
    state: &RwLock<PoolState>,
    mapping_path: &Path,
    key: &str,
    expected: &LifecycleHandle,
) -> Result<()> {
    if !expected.capabilities().release_v1 {
        return Err(anyhow!(
            "_openab/session/release capability was not advertised"
        ));
    }

    expected.release().await?;

    let mut state = state.write().await;
    let same_handle = state
        .lifecycle_handles
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected));
    if !same_handle {
        return Err(anyhow!(
            "session lifecycle handle changed during release acknowledgement"
        ));
    }

    let mut persisted = state.persisted.clone();
    persisted.remove(key);
    let persistence = write_mapping_file(mapping_path, &persisted);

    state.active.remove(key);
    purge_session_entries(&mut state, key);
    persistence.map_err(|error| {
        anyhow!(
            "session release was accepted, but broker mapping removal could not be persisted: {error}"
        )
    })
}

async fn rollback_uncommitted_session(
    lifecycle: &LifecycleHandle,
    cause: anyhow::Error,
) -> anyhow::Error {
    match lifecycle.release().await {
        Ok(()) => anyhow!("{cause}; uncommitted isolated session was released"),
        Err(rollback) => {
            anyhow!("{cause}; failed to release uncommitted isolated session: {rollback}")
        }
    }
}

fn park_strict_session(
    state: &mut PoolState,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
    expected_lifecycle: &LifecycleHandle,
    clean_close: bool,
) -> Result<()> {
    let same_connection = state
        .active
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_connection));
    let same_lifecycle = state
        .lifecycle_handles
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_lifecycle));
    if !same_connection || !same_lifecycle {
        return Err(anyhow!(
            "isolated session changed during lifecycle transition"
        ));
    }
    let session_id = state.persisted.get(key).cloned().ok_or_else(|| {
        anyhow!("isolated session for thread {key} has no persisted session mapping")
    })?;

    state.active.remove(key);
    state.cancel_handles.remove(key);
    state.lifecycle_handles.remove(key);
    state.activity.remove(key);
    state.pgids.remove(key);
    if clean_close {
        state.suspended.insert(key.to_string(), session_id);
    } else {
        // `persisted` without `active` or `suspended` is the broker's
        // lightweight orphan marker. A fresh bridge attempt will reconcile it.
        state.suspended.remove(key);
    }
    Ok(())
}

async fn try_suspend_strict_session(
    state: &RwLock<PoolState>,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
    gate: &SessionGate,
    idle_cutoff: Option<Instant>,
) -> Result<StrictSuspendOutcome> {
    // Prompt dispatch takes the connection first and briefly takes this gate
    // for pointer revalidation. Both locks are non-blocking here so cleanup or
    // capacity management never queues behind a live turn.
    let Ok(connection) = expected_connection.try_lock() else {
        return Ok(StrictSuspendOutcome::Skipped);
    };
    let Ok(_gate_guard) = gate.try_lock() else {
        return Ok(StrictSuspendOutcome::Skipped);
    };

    let lifecycle =
        {
            let state = state.read().await;
            if !state
                .active
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, expected_connection))
            {
                return Ok(StrictSuspendOutcome::Skipped);
            }
            if let Some(cutoff) = idle_cutoff {
                if connection.alive() && connection.last_active >= cutoff {
                    return Ok(StrictSuspendOutcome::Skipped);
                }
            }
            if !state.persisted.contains_key(key) {
                return Err(anyhow!(
                    "isolated session for thread {key} has no persisted session mapping"
                ));
            }
            state.lifecycle_handles.get(key).cloned().ok_or_else(|| {
                anyhow!("isolated session for thread {key} has no lifecycle handle")
            })?
        };

    if !connection.alive() {
        let mut state = state.write().await;
        park_strict_session(&mut state, key, expected_connection, &lifecycle, false)?;
        return Ok(StrictSuspendOutcome::Orphaned);
    }

    let close_result = lifecycle.close().await;
    let mut state = state.write().await;
    match close_result {
        Ok(()) => {
            park_strict_session(&mut state, key, expected_connection, &lifecycle, true)?;
            Ok(StrictSuspendOutcome::Suspended)
        }
        Err(close_error) => {
            let orphan_result =
                park_strict_session(&mut state, key, expected_connection, &lifecycle, false);
            match orphan_result {
                Ok(()) => Err(anyhow!(
                    "session/close failed; isolated session was orphaned for reconciliation: {close_error}"
                )),
                Err(orphan_error) => Err(anyhow!(
                    "session/close failed ({close_error}) and orphan transition failed: {orphan_error}"
                )),
            }
        }
    }
}

async fn orphan_hung_strict_session(
    state: &RwLock<PoolState>,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
    expected_lifecycle: &LifecycleHandle,
    expected_activity: &Arc<SessionActivity>,
    gate: &SessionGate,
    hung_threshold: std::time::Duration,
) -> Result<StrictSuspendOutcome> {
    let Ok(_gate_guard) = gate.try_lock() else {
        return Ok(StrictSuspendOutcome::Skipped);
    };

    // The turn may have completed after cleanup's first hung classification.
    // Recheck only after entering the lifecycle gate so a stale snapshot cannot
    // detach a healthy bridge.
    if expected_connection.try_lock().is_ok() {
        if expected_activity.in_flight() {
            expected_activity.set_in_flight(false);
            expected_activity.touch();
        }
        return Ok(StrictSuspendOutcome::Skipped);
    }
    if !classify_hung(
        expected_activity.in_flight(),
        expected_activity.age(),
        hung_threshold,
    ) {
        return Ok(StrictSuspendOutcome::Skipped);
    }

    let mut state = state.write().await;
    let same_activity = state
        .activity
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_activity));
    if !same_activity {
        return Ok(StrictSuspendOutcome::Skipped);
    }
    park_strict_session(
        &mut state,
        key,
        expected_connection,
        expected_lifecycle,
        false,
    )?;
    Ok(StrictSuspendOutcome::Orphaned)
}

/// Escalating kill for a hung agent's process group: wait 10s after the
/// session/cancel attempt, SIGTERM, wait 2s, SIGKILL. Mirrors
/// `AcpConnection::kill_process_group`, which cannot run here because the
/// hung task never drops its connection Arc.
async fn kill_pgid_after_grace(pgid: Option<i32>) {
    let Some(pgid) = pgid.filter(|p| *p > 0) else {
        return;
    };
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(-pgid, libc::SIGTERM);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        // No process-group kill on non-unix; rely on AcpConnection::Drop's
        // Windows handling if/when the hung task eventually unwinds.
        let _ = pgid;
    }
}

/// Remove a hung session from all pool maps. Returns true if the exact
/// connection captured at classification time was still registered; when a
/// fresh replacement exists for the key, nothing is touched.
fn apply_hung_eviction(
    state: &mut PoolState,
    key: &str,
    expected: &Arc<Mutex<AcpConnection>>,
) -> bool {
    if remove_if_same_handle(&mut state.active, key, expected).is_none() {
        return false;
    }
    purge_session_entries(state, key);
    true
}

impl SessionPool {
    pub fn new(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
    ) -> Self {
        let openab_dir = std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp"))
            .join(".openab");
        let _ = std::fs::create_dir_all(&openab_dir);
        let mapping_path = openab_dir.join("thread_map.json");
        let meta_path = openab_dir.join("session_meta.json");
        Self::new_with_paths(
            config,
            max_sessions,
            hung_threshold_secs,
            default_config_options,
            mapping_path,
            meta_path,
        )
    }

    fn new_with_paths(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        mapping_path: PathBuf,
        meta_path: PathBuf,
    ) -> Self {
        let (suspended, mapping_load_error) = Self::load_mapping(&mapping_path);
        let (session_workdirs, _) = Self::load_mapping(&meta_path);
        Self {
            state: RwLock::new(PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                lifecycle_handles: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                persisted: suspended.clone(),
                suspended,
                creating: HashMap::new(),
                session_workdirs,
            }),
            config,
            session_context: SessionContextMode::None,
            max_sessions,
            hung_threshold_secs,
            mapping_path,
            meta_path,
            mapping_load_error,
            default_config_options,
        }
    }

    /// Enable broker-owned context for an explicitly configured session
    /// runtime bridge. The default constructor remains behavior-compatible
    /// with local ACP and AgentCore agents.
    pub fn try_with_session_context(mut self, mode: SessionContextMode) -> Result<Self> {
        if mode == SessionContextMode::OpenabV1 {
            if let Some(error) = self.mapping_load_error.as_deref() {
                return Err(anyhow!(
                    "cannot enable Kubernetes session isolation: {error}"
                ));
            }
            // Broker workspace metadata belongs to the local-process runtime.
            // In isolated mode the controller-owned worker profile chooses all
            // writable paths, so stale local metadata is intentionally ignored.
            self.state.get_mut().session_workdirs.clear();
        }
        self.session_context = mode;
        Ok(self)
    }

    pub(crate) fn allows_workspace_directives(&self) -> bool {
        self.session_context == SessionContextMode::None
    }

    fn load_mapping(path: &Path) -> (HashMap<String, String>, Option<String>) {
        match std::fs::read_to_string(path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(mapping) => (mapping, None),
                Err(error) => {
                    let message = format!(
                        "failed to parse persisted mapping {}: {error}",
                        path.display()
                    );
                    warn!(%message, "corrupt mapping file, starting fresh");
                    (HashMap::new(), Some(message))
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (HashMap::new(), None),
            Err(error) => {
                let message = format!(
                    "failed to read persisted mapping {}: {error}",
                    path.display()
                );
                warn!(%message, "unreadable mapping file, starting fresh");
                (HashMap::new(), Some(message))
            }
        }
    }

    fn save_mapping(&self, persisted: &HashMap<String, String>) {
        let data = match serde_json::to_string_pretty(persisted) {
            Ok(data) => data,
            Err(error) => {
                warn!(%error, "failed to serialize thread mapping");
                return;
            }
        };
        let temporary = self.mapping_path.with_extension("json.tmp");
        if let Err(error) = std::fs::write(&temporary, &data)
            .and_then(|_| std::fs::rename(&temporary, &self.mapping_path))
        {
            warn!(path = %self.mapping_path.display(), %error, "failed to persist thread mapping");
        }
    }

    fn save_meta(&self, workdirs: &HashMap<String, String>) {
        let data = match serde_json::to_string_pretty(workdirs) {
            Ok(data) => data,
            Err(error) => {
                warn!(%error, "failed to serialize session metadata");
                return;
            }
        };
        let temporary = self.meta_path.with_extension("json.tmp");
        if let Err(error) = std::fs::write(&temporary, &data)
            .and_then(|_| std::fs::rename(&temporary, &self.meta_path))
        {
            warn!(path = %self.meta_path.display(), %error, "failed to persist session metadata");
        }
    }

    /// Check if session state exists for this thread (active, suspended, or persisted).
    #[allow(dead_code)]
    pub async fn has_active_session(&self, thread_id: &str) -> bool {
        let state = self.state.read().await;
        // Any of these means the thread already has session state.
        if state.suspended.contains_key(thread_id) || state.persisted.contains_key(thread_id) {
            return true;
        }
        if let Some(conn) = state.active.get(thread_id) {
            match conn.try_lock() {
                Ok(c) => return c.alive(),
                Err(_) => return true, // lock held = connection busy streaming = alive
            }
        }
        false
    }

    pub async fn get_or_create(
        &self,
        thread_id: &str,
        working_dir_override: Option<&str>,
    ) -> Result<bool> {
        let create_gate = {
            let mut state = self.state.write().await;
            get_or_insert_gate(&mut state.creating, thread_id)
        };
        let _create_guard = create_gate.lock().await;

        let (existing, saved_session_id) = {
            let state = self.state.read().await;
            let saved_session_id = state.suspended.get(thread_id).cloned().or_else(|| {
                (self.session_context == SessionContextMode::OpenabV1)
                    .then(|| state.persisted.get(thread_id).cloned())
                    .flatten()
            });
            (state.active.get(thread_id).cloned(), saved_session_id)
        };

        let had_existing = existing.is_some();
        let mut saved_session_id = saved_session_id;
        if let Some(conn) = existing.clone() {
            // Never await the existing connection's mutex here: we hold the
            // per-thread creating gate, so blocking on a hung connection would
            // permanently jam ALL future messages for this thread_id (F1).
            // Lock held = busy streaming = alive (same convention as
            // has_active_session); cleanup_idle owns hung recovery.
            let Ok(conn) = conn.try_lock() else {
                return Ok(false);
            };
            if conn.alive() {
                return Ok(false);
            }
            if saved_session_id.is_none() {
                saved_session_id = conn.acp_session_id.clone();
            }
        }

        // Snapshot active handles so we can inspect them outside the state lock.
        let snapshot: ActiveSnapshot = {
            let state = self.state.read().await;
            state
                .active
                .iter()
                .filter_map(|(key, connection)| {
                    state
                        .creating
                        .get(key)
                        .map(|gate| (key.clone(), Arc::clone(connection), Arc::clone(gate)))
                })
                .collect()
        };

        let mut eviction_candidate: Option<EvictionCandidate> = None;
        let mut skipped_locked_candidates = 0usize;
        for (key, conn, gate) in snapshot {
            if key == thread_id {
                continue;
            }
            let Ok(_gate_guard) = gate.try_lock() else {
                skipped_locked_candidates += 1;
                continue;
            };
            let conn_handle = Arc::clone(&conn);
            let Ok(conn) = conn.try_lock() else {
                skipped_locked_candidates += 1;
                continue;
            };
            let candidate = (
                key,
                conn_handle,
                conn.last_active,
                conn.acp_session_id.clone(),
                Arc::clone(&gate),
            );
            if better_candidate(
                eviction_candidate.as_ref().map(|(_, _, t, _, _)| *t),
                candidate.2,
            ) {
                eviction_candidate = Some(candidate);
            }
        }

        if self.session_context == SessionContextMode::OpenabV1 && !had_existing {
            let at_capacity = self.state.read().await.active.len() >= self.max_sessions;
            if at_capacity {
                let Some((key, expected_connection, _, _, gate)) = eviction_candidate.as_ref()
                else {
                    return Err(anyhow!(
                        "pool exhausted ({} sessions); no idle isolated session can be suspended",
                        self.max_sessions
                    ));
                };
                match try_suspend_strict_session(&self.state, key, expected_connection, gate, None)
                    .await?
                {
                    StrictSuspendOutcome::Suspended => {
                        info!(evicted = %key, "pool full, suspended isolated session before provisioning");
                    }
                    StrictSuspendOutcome::Orphaned => {
                        return Err(anyhow!(
                            "pool full; isolated session {key} was orphaned for reconciliation"
                        ));
                    }
                    StrictSuspendOutcome::Skipped => {
                        return Err(anyhow!(
                            "pool exhausted ({} sessions); eviction candidate became busy",
                            self.max_sessions
                        ));
                    }
                }
            }
        }

        // Resolve effective working directory: stored per-session > explicit override > global config.
        // Stored value has highest priority to enforce immutability (ADR §4.5).
        let stored_workdir = {
            let state = self.state.read().await;
            state.session_workdirs.get(thread_id).cloned()
        };

        let effective_workdir = resolve_effective_workdir(
            self.session_context,
            stored_workdir.as_deref(),
            working_dir_override,
            &self.config.working_dir,
        )?;

        // Build the replacement connection outside the state lock so one stuck
        // initialization does not block all unrelated sessions.
        let session_spawn_context = session_spawn_context(self.session_context, thread_id);
        let mut new_conn = AcpConnection::spawn_with_context(
            &self.config.command,
            &self.config.args,
            &effective_workdir,
            &self.config.env,
            &self.config.inherit_env,
            session_spawn_context.as_ref(),
        )
        .await?;

        new_conn.initialize().await?;

        let mut resumed = false;
        let mut load_failed: Option<String> = None;
        if let Some(ref sid) = saved_session_id {
            if new_conn.supports_load_session {
                match new_conn.session_load(sid, &effective_workdir).await {
                    Ok(()) => {
                        info!(thread_id, session_id = %sid, "session resumed via session/load");
                        resumed = true;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let is_transient =
                            TRANSIENT_LOAD_ERRORS.iter().any(|s| err_str.contains(s));
                        if self.session_context == SessionContextMode::OpenabV1 || is_transient {
                            warn!(thread_id, session_id = %sid, error = %e,
                                "session/load failed, preserving session ID for retry");
                            load_failed = Some(if err_str.contains("timeout waiting for") {
                                "timeout".to_string()
                            } else {
                                err_str
                            });
                        } else {
                            warn!(thread_id, session_id = %sid, error = %e,
                                "session/load failed, creating new session");
                        }
                    }
                }
            }
        }

        if let Some(reason) = load_failed {
            // The original session ID is already in state.persisted, so the
            // next message retries session/load. Strict mode never falls back
            // to session/new for a retained outer ID: that would bypass the
            // controller's continuity and deletion fences.
            return Err(anyhow!(
                "session load {reason}: could not restore previous session"
            ));
        }

        if !resumed {
            new_conn.session_new(&effective_workdir).await?;

            // Apply default config options (e.g. mode=bypass, model=swe-1-6)
            for (config_id, value) in &self.default_config_options {
                if let Err(e) = new_conn.set_config_option(config_id, value).await {
                    warn!(config_id, value, error = %e, "failed to set default config option");
                }
            }

            // Surface the reset banner both for restored sessions and for stale
            // live entries that died before we could recover a resumable
            // session id. In both cases the caller is continuing after an
            // unexpected session loss.
            if had_existing || saved_session_id.is_some() {
                new_conn.session_reset = true;
            }
        }

        let cancel_handle = new_conn.cancel_handle();
        let lifecycle_handle = match self.session_context {
            SessionContextMode::None => None,
            SessionContextMode::OpenabV1 => Some(
                new_conn
                    .lifecycle_handle()
                    .ok_or_else(|| anyhow!("isolated session bridge has no ACP session ID"))?,
            ),
        };
        let activity_handle = new_conn.activity_handle();
        let child_pgid = new_conn.child_pgid();
        let cancel_session_id = new_conn.acp_session_id.clone().unwrap_or_default();
        let new_conn = Arc::new(Mutex::new(new_conn));

        let mut state = self.state.write().await;

        // Another task may have created a healthy connection while we were
        // initializing this one.
        if let Some(existing) = state.active.get(thread_id).cloned() {
            let Ok(existing) = existing.try_lock() else {
                return Ok(false);
            };
            if existing.alive() {
                return Ok(false);
            }
            warn!(thread_id, "stale connection, rebuilding");
            drop(existing);
            state.active.remove(thread_id);
            state.cancel_handles.remove(thread_id);
            state.lifecycle_handles.remove(thread_id);
            state.activity.remove(thread_id);
            state.pgids.remove(thread_id);
        }

        if self.session_context == SessionContextMode::None
            && state.active.len() >= self.max_sessions
        {
            if let Some((key, expected_conn, _, sid, gate)) = eviction_candidate {
                if let Ok(_gate_guard) = gate.try_lock() {
                    if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                        state.cancel_handles.remove(&key);
                        state.lifecycle_handles.remove(&key);
                        state.activity.remove(&key);
                        state.pgids.remove(&key);
                        info!(evicted = %key, "pool full, suspending oldest idle session");
                        if let Some(sid) = sid {
                            state.persisted.insert(key.clone(), sid.clone());
                            state.suspended.insert(key, sid);
                        } else {
                            state.persisted.remove(&key);
                        }
                    } else {
                        warn!(evicted = %key, "pool full but eviction candidate changed before removal");
                    }
                } else {
                    warn!(evicted = %key, "pool full but eviction candidate entered a lifecycle transition");
                }
            } else if skipped_locked_candidates > 0 {
                warn!(
                    max_sessions = self.max_sessions,
                    skipped_locked_candidates,
                    "pool full but all other sessions were busy during eviction scan"
                );
            }
        }

        if state.active.len() >= self.max_sessions {
            let error = anyhow!("pool exhausted ({} sessions)", self.max_sessions);
            if let Some(lifecycle) = lifecycle_handle.as_ref() {
                drop(state);
                return Err(rollback_uncommitted_session(lifecycle, error).await);
            }
            return Err(error);
        }

        let mut persisted = state.persisted.clone();
        if cancel_session_id.is_empty() {
            persisted.remove(thread_id);
        } else {
            persisted.insert(thread_id.to_string(), cancel_session_id.clone());
        }
        if self.session_context == SessionContextMode::OpenabV1 && persisted != state.persisted {
            if let Err(error) = write_mapping_file(&self.mapping_path, &persisted) {
                drop(state);
                let lifecycle = lifecycle_handle
                    .as_ref()
                    .expect("strict sessions always have a lifecycle handle");
                return Err(rollback_uncommitted_session(lifecycle, error).await);
            }
        }
        state.persisted = persisted;
        state.suspended.remove(thread_id);
        state.active.insert(thread_id.to_string(), new_conn);
        if let Some(lifecycle_handle) = lifecycle_handle {
            state
                .lifecycle_handles
                .insert(thread_id.to_string(), lifecycle_handle);
        }
        state
            .activity
            .insert(thread_id.to_string(), activity_handle);
        if let Some(pgid) = child_pgid {
            state.pgids.insert(thread_id.to_string(), pgid);
        }
        if !cancel_session_id.is_empty() {
            state
                .cancel_handles
                .insert(thread_id.to_string(), (cancel_handle, cancel_session_id));
        }
        if self.session_context == SessionContextMode::None {
            self.save_mapping(&state.persisted);
        }

        // Persist workspace override only after session spawn succeeded (口渡 F2).
        if working_dir_override.is_some() {
            state
                .session_workdirs
                .entry(thread_id.to_string())
                .or_insert_with(|| effective_workdir.clone());
            self.save_meta(&state.session_workdirs);
        }

        // Return true only for genuinely new sessions — not resumed or reconnected ones.
        // A session with prior state (saved_session_id or had_existing) is a resume,
        // even if we had to spawn a new ACP process. ADR §2.2: directives are first-message-only.
        let is_fresh = !had_existing && saved_session_id.is_none();
        Ok(is_fresh)
    }

    /// Get mutable access to a connection. Caller must have called get_or_create first.
    ///
    /// Only the per-connection `Mutex` is held during `f`; the pool-level
    /// `RwLock` is acquired briefly (read-only) to look up the `Arc` and then
    /// released, so other connections can be used concurrently.
    pub async fn with_connection<F, R>(&self, thread_id: &str, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut AcpConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>,
        >,
    {
        let (connection, lifecycle_gate) = {
            let state = self.state.read().await;
            let connection = state
                .active
                .get(thread_id)
                .cloned()
                .ok_or_else(|| anyhow!("no connection for thread {thread_id}"))?;
            let gate = if self.session_context == SessionContextMode::OpenabV1 {
                Some(state.creating.get(thread_id).cloned().ok_or_else(|| {
                    anyhow!("isolated session for thread {thread_id} has no lifecycle gate")
                })?)
            } else {
                None
            };
            (connection, gate)
        };

        let mut connection_guard = connection.lock().await;
        if let Some(gate) = lifecycle_gate {
            let _gate_guard = gate.lock().await;
            let state = self.state.read().await;
            let is_current = state
                .active
                .get(thread_id)
                .is_some_and(|current| Arc::ptr_eq(current, &connection));
            if !is_current {
                return Err(anyhow!(
                    "session for thread {thread_id} changed before prompt dispatch"
                ));
            }
        }
        f(&mut connection_guard).await
    }

    /// Get cached configOptions for a session (e.g. available models).
    pub async fn get_config_options(&self, thread_id: &str) -> Vec<ConfigOption> {
        let state = self.state.read().await;
        let conn = match state.active.get(thread_id) {
            Some(c) => c.clone(),
            None => return Vec::new(),
        };
        drop(state);
        let conn = conn.lock().await;
        conn.config_options.clone()
    }

    /// Set a config option (e.g. model) via ACP and return updated options.
    pub async fn set_config_option(
        &self,
        thread_id: &str,
        config_id: &str,
        value: &str,
    ) -> Result<Vec<ConfigOption>> {
        let conn = {
            let state = self.state.read().await;
            state
                .active
                .get(thread_id)
                .cloned()
                .ok_or_else(|| anyhow!("no connection for thread {thread_id}"))?
        };
        let mut conn = conn.lock().await;
        conn.set_config_option(config_id, value).await
    }

    /// Query account-level usage/billing from the backend agent for a session
    /// (kiro-cli extension). Fails when there is no active session for the
    /// thread or the backend does not support usage queries.
    pub async fn get_usage(&self, thread_id: &str) -> Result<crate::acp::protocol::UsageReport> {
        let conn = {
            let state = self.state.read().await;
            state
                .active
                .get(thread_id)
                .cloned()
                .ok_or_else(|| anyhow!("no connection for thread {thread_id}"))?
        };
        let mut conn = conn.lock().await;
        conn.get_usage().await
    }

    /// Cancel the current in-flight operation for a session.
    /// Uses pre-stored cancel handles to avoid locking the connection (which is held during streaming).
    pub async fn cancel_session(&self, thread_id: &str) -> Result<()> {
        if self.session_context == SessionContextMode::OpenabV1 {
            let lifecycle = {
                let state = self.state.read().await;
                state
                    .lifecycle_handles
                    .get(thread_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("no isolated session for thread {thread_id}"))?
            };
            return lifecycle.cancel().await;
        }

        let (stdin, session_id) = {
            let state = self.state.read().await;
            state
                .cancel_handles
                .get(thread_id)
                .cloned()
                .ok_or_else(|| anyhow!("no session for thread {thread_id}"))?
        };
        let data = serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id}
        }))?;
        tracing::info!(session_id, "sending session/cancel");
        use tokio::io::AsyncWriteExt;
        let mut w = stdin.lock().await;
        w.write_all(data.as_bytes()).await?;
        w.write_all(b"\n").await?;
        w.flush().await?;
        Ok(())
    }

    /// Reset a session: cancel any in-flight operation, remove the active connection,
    /// and clear all suspended state. The ACP process will be killed once the last
    /// Arc reference is dropped (after streaming finishes). The next message will
    /// trigger a fresh `get_or_create` with a new ACP session.
    pub async fn reset_session(&self, thread_id: &str) -> Result<()> {
        if self.session_context == SessionContextMode::OpenabV1 {
            let create_gate = {
                let mut state = self.state.write().await;
                get_or_insert_gate(&mut state.creating, thread_id)
            };
            let _create_guard = create_gate.lock().await;

            let lifecycle = {
                let state = self.state.read().await;
                if !state.active.contains_key(thread_id) {
                    return Err(anyhow!("no active isolated session for thread {thread_id}"));
                }
                state
                    .lifecycle_handles
                    .get(thread_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!("isolated session for thread {thread_id} has no lifecycle handle")
                    })?
            };

            release_strict_session(&self.state, &self.mapping_path, thread_id, &lifecycle).await?;

            info!(thread_id, "isolated session released");
            return Ok(());
        }

        // Send session/cancel via the lock-free stdin handle first.
        // This stops in-flight streaming even while with_connection() holds the
        // connection mutex, so the old process finishes promptly.
        if let Some((stdin, session_id)) = {
            let state = self.state.read().await;
            state.cancel_handles.get(thread_id).cloned()
        } {
            let data = serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": session_id}
            }))?;
            tracing::info!(session_id, "reset: sending session/cancel");
            use tokio::io::AsyncWriteExt;
            let mut w = stdin.lock().await;
            let _ = w.write_all(data.as_bytes()).await;
            let _ = w.write_all(b"\n").await;
            let _ = w.flush().await;
        }

        let mut state = self.state.write().await;
        let had_active = state.active.remove(thread_id).is_some();
        state.cancel_handles.remove(thread_id);
        state.lifecycle_handles.remove(thread_id);
        state.activity.remove(thread_id);
        state.pgids.remove(thread_id);
        state.suspended.remove(thread_id);
        state.persisted.remove(thread_id);
        state.creating.remove(thread_id);
        state.session_workdirs.remove(thread_id);
        self.save_mapping(&state.persisted);
        self.save_meta(&state.session_workdirs);
        if had_active {
            info!(thread_id, "session reset");
            Ok(())
        } else {
            Err(anyhow!("no session for thread {thread_id}"))
        }
    }

    async fn cleanup_idle_strict(&self, ttl_secs: u64) {
        let cutoff = Instant::now() - std::time::Duration::from_secs(ttl_secs);
        let hung_threshold = std::time::Duration::from_secs(self.hung_threshold_secs);
        let snapshot = {
            let state = self.state.read().await;
            let active_count = state.active.len();
            let snapshot: Vec<_> = state
                .active
                .iter()
                .filter_map(|(key, connection)| {
                    Some((
                        key.clone(),
                        Arc::clone(connection),
                        Arc::clone(state.creating.get(key)?),
                        Arc::clone(state.lifecycle_handles.get(key)?),
                        Arc::clone(state.activity.get(key)?),
                        state.pgids.get(key).copied(),
                    ))
                })
                .collect();
            if snapshot.len() != active_count {
                warn!(
                    active_count,
                    complete_count = snapshot.len(),
                    "isolated session pool contains incomplete lifecycle entries"
                );
            }
            snapshot
        };

        for (key, connection, gate, lifecycle, activity, pgid) in snapshot {
            match connection.try_lock() {
                Ok(connection_guard) => {
                    if activity.in_flight() {
                        activity.set_in_flight(false);
                        activity.touch();
                    }
                    if !classify_idle(
                        connection_guard.last_active,
                        connection_guard.alive(),
                        cutoff,
                    ) {
                        continue;
                    }
                    drop(connection_guard);
                    match try_suspend_strict_session(
                        &self.state,
                        &key,
                        &connection,
                        &gate,
                        Some(cutoff),
                    )
                    .await
                    {
                        Ok(StrictSuspendOutcome::Suspended) => {
                            info!(thread_id = %key, "suspended idle isolated session");
                        }
                        Ok(StrictSuspendOutcome::Orphaned) => {
                            warn!(thread_id = %key, "dead isolated bridge detached as orphan");
                        }
                        Ok(StrictSuspendOutcome::Skipped) => {}
                        Err(error) => {
                            warn!(
                                thread_id = %key,
                                %error,
                                "isolated session close failed during idle cleanup"
                            );
                        }
                    }
                }
                Err(_) if classify_hung(activity.in_flight(), activity.age(), hung_threshold) => {
                    match orphan_hung_strict_session(
                        &self.state,
                        &key,
                        &connection,
                        &lifecycle,
                        &activity,
                        &gate,
                        hung_threshold,
                    )
                    .await
                    {
                        Ok(StrictSuspendOutcome::Orphaned) => {
                            warn!(
                                thread_id = %key,
                                age_secs = activity.age().as_secs(),
                                "hung isolated bridge detached as orphan"
                            );
                            tokio::spawn(async move {
                                let _ = lifecycle.cancel().await;
                                kill_pgid_after_grace(pgid).await;
                            });
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                thread_id = %key,
                                %error,
                                "failed to detach hung isolated bridge"
                            );
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    pub async fn cleanup_idle(&self, ttl_secs: u64) {
        if self.session_context == SessionContextMode::OpenabV1 {
            self.cleanup_idle_strict(ttl_secs).await;
            return;
        }

        let cutoff = Instant::now() - std::time::Duration::from_secs(ttl_secs);
        let hung_threshold = std::time::Duration::from_secs(self.hung_threshold_secs);

        let (snapshot, activity_map, cancel_map, pgid_map) = {
            let state = self.state.read().await;
            let snapshot: ActiveSnapshot = state
                .active
                .iter()
                .filter_map(|(key, connection)| {
                    state
                        .creating
                        .get(key)
                        .map(|gate| (key.clone(), Arc::clone(connection), Arc::clone(gate)))
                })
                .collect();
            (
                snapshot,
                state.activity.clone(),
                state.cancel_handles.clone(),
                state.pgids.clone(),
            )
        };

        let mut stale = Vec::new();
        let mut hung: Vec<(String, Arc<Mutex<AcpConnection>>, SessionGate)> = Vec::new();
        for (key, conn, gate) in snapshot {
            // Create, resume, close, and release all own this gate. Cleanup
            // must never race a lifecycle acknowledgement for the same key.
            let Ok(_gate_guard) = gate.try_lock() else {
                continue;
            };
            // Skip active sessions for this cleanup round instead of waiting on
            // their per-connection mutex. A busy session is not idle unless hung.
            let conn_handle = Arc::clone(&conn);
            let Ok(conn) = conn.try_lock() else {
                if let Some(activity) = activity_map.get(&key) {
                    if classify_hung(activity.in_flight(), activity.age(), hung_threshold) {
                        let session_id = cancel_map.get(&key).map(|(_, sid)| sid.clone());
                        warn!(
                            thread_id = %key,
                            session_id = session_id.as_deref().unwrap_or(""),
                            age_secs = activity.age().as_secs(),
                            threshold_secs = self.hung_threshold_secs,
                            "force-evicting hung session"
                        );
                        // Best-effort session/cancel via the lock-free stdin
                        // handle, detached so a wedged stdin can never block
                        // cleanup (and never while holding `state`). The hung
                        // task never unwinds, so AcpConnection::Drop never
                        // fires; after the cancel attempt, kill the child
                        // process group directly or the agent leaks forever (F4).
                        let stdin_handle = cancel_map.get(&key).map(|(stdin, _)| Arc::clone(stdin));
                        let pgid = pgid_map.get(&key).copied();
                        tokio::spawn(async move {
                            if let (Some(stdin), Some(session_id)) = (stdin_handle, session_id) {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_secs(5),
                                    async move {
                                        if let Ok(data) =
                                            serde_json::to_string(&serde_json::json!({
                                                "jsonrpc": "2.0",
                                                "method": "session/cancel",
                                                "params": {"sessionId": session_id}
                                            }))
                                        {
                                            use tokio::io::AsyncWriteExt;
                                            let mut w = stdin.lock().await;
                                            let _ = w.write_all(data.as_bytes()).await;
                                            let _ = w.write_all(b"\n").await;
                                            let _ = w.flush().await;
                                        }
                                    },
                                )
                                .await;
                            }
                            kill_pgid_after_grace(pgid).await;
                        });
                        hung.push((key, conn_handle, Arc::clone(&gate)));
                    }
                }
                continue;
            };
            // try_lock success means no turn is streaming under
            // with_connection, so a true in_flight flag is stale (the turn
            // aborted without prompt_done). Self-heal it so the session can
            // never be falsely classified as hung later.
            if let Some(activity) = activity_map.get(&key) {
                if activity.in_flight() {
                    activity.set_in_flight(false);
                    activity.touch();
                }
            }
            if classify_idle(conn.last_active, conn.alive(), cutoff) {
                stale.push((
                    key,
                    conn_handle,
                    conn.acp_session_id.clone(),
                    Arc::clone(&gate),
                ));
            }
        }

        if stale.is_empty() && hung.is_empty() {
            return;
        }

        let mut state = self.state.write().await;
        for (key, expected_conn, sid, gate) in stale {
            let Ok(_gate_guard) = gate.try_lock() else {
                continue;
            };
            if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                info!(thread_id = %key, "cleaning up idle session");
                state.cancel_handles.remove(&key);
                state.lifecycle_handles.remove(&key);
                state.activity.remove(&key);
                state.pgids.remove(&key);
                if let Some(sid) = sid {
                    state.persisted.insert(key.clone(), sid.clone());
                    state.suspended.insert(key, sid);
                } else {
                    state.persisted.remove(&key);
                    state.session_workdirs.remove(&key);
                }
            }
        }
        for (key, expected_conn, gate) in hung {
            let Ok(_gate_guard) = gate.try_lock() else {
                continue;
            };
            if !apply_hung_eviction(&mut state, &key, &expected_conn) {
                warn!(thread_id = %key, "hung session was replaced before eviction; maps untouched");
            }
        }
        self.save_mapping(&state.persisted);
        self.save_meta(&state.session_workdirs);
    }

    pub async fn shutdown(&self) {
        // Snapshot active handles, then drop state lock before awaiting
        // per-connection mutexes (lock ordering: never hold state while
        // awaiting a connection lock).
        let snapshot: Vec<(String, Arc<Mutex<AcpConnection>>)> = {
            let state = self.state.read().await;
            state
                .active
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect()
        };

        let mut session_ids: Vec<(String, String)> = Vec::new();
        for (key, conn) in snapshot {
            let conn = conn.lock().await;
            if let Some(sid) = conn.acp_session_id.clone() {
                session_ids.push((key, sid));
            }
        }

        let mut state = self.state.write().await;
        for (key, sid) in session_ids {
            state.persisted.insert(key.clone(), sid.clone());
            state.suspended.insert(key, sid);
        }
        self.save_mapping(&state.persisted);
        let count = state.active.len();
        state.active.clear();
        state.cancel_handles.clear();
        state.lifecycle_handles.clear();
        state.activity.clear();
        state.pgids.clear();
        info!(count, "pool shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        better_candidate, classify_hung, classify_idle, get_or_insert_gate,
        orphan_hung_strict_session, purge_session_entries, release_strict_session,
        remove_if_same_handle, resolve_effective_workdir, rollback_uncommitted_session,
        session_spawn_context, write_mapping_file, PoolState, SessionPool, StrictSuspendOutcome,
    };
    use crate::acp::connection::{
        LifecycleCapabilities, LifecycleHandle, SessionActivity, SessionLifecycleControl,
    };
    use crate::acp::SessionContextMode;
    use crate::config::AgentConfig;
    use anyhow::{anyhow, Result};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify, RwLock};
    use tokio::time::Instant;

    #[derive(Clone, Copy)]
    enum ReleaseBehavior {
        Succeed,
        Fail,
        Block,
    }

    struct FakeLifecycle {
        capabilities: LifecycleCapabilities,
        release_calls: AtomicUsize,
        behavior: ReleaseBehavior,
        release_started: Notify,
        release_continue: Notify,
    }

    impl FakeLifecycle {
        fn new(behavior: ReleaseBehavior) -> Arc<Self> {
            Arc::new(Self {
                capabilities: LifecycleCapabilities {
                    close: true,
                    release_v1: true,
                },
                release_calls: AtomicUsize::new(0),
                behavior,
                release_started: Notify::new(),
                release_continue: Notify::new(),
            })
        }

        fn unsupported() -> Arc<Self> {
            Arc::new(Self {
                capabilities: LifecycleCapabilities::default(),
                release_calls: AtomicUsize::new(0),
                behavior: ReleaseBehavior::Succeed,
                release_started: Notify::new(),
                release_continue: Notify::new(),
            })
        }

        fn handle(self: &Arc<Self>) -> LifecycleHandle {
            Arc::clone(self) as LifecycleHandle
        }
    }

    #[async_trait::async_trait]
    impl SessionLifecycleControl for FakeLifecycle {
        fn capabilities(&self) -> LifecycleCapabilities {
            self.capabilities
        }

        async fn cancel(&self) -> Result<()> {
            Ok(())
        }

        async fn release(&self) -> Result<()> {
            self.release_calls.fetch_add(1, Ordering::Relaxed);
            self.release_started.notify_waiters();
            match self.behavior {
                ReleaseBehavior::Succeed => Ok(()),
                ReleaseBehavior::Fail => Err(anyhow!("controller rejected release")),
                ReleaseBehavior::Block => {
                    self.release_continue.notified().await;
                    Ok(())
                }
            }
        }
    }

    fn strict_state(handle: LifecycleHandle) -> PoolState {
        PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            lifecycle_handles: HashMap::from([("thread".to_string(), handle)]),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::from([("thread".to_string(), "outer-session".to_string())]),
            persisted: HashMap::from([("thread".to_string(), "outer-session".to_string())]),
            creating: HashMap::from([("thread".to_string(), Arc::new(Mutex::new(())))]),
            session_workdirs: HashMap::from([("thread".to_string(), "/private/ws".to_string())]),
        }
    }

    fn strict_mapping_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("thread_map.json");
        write_mapping_file(
            &path,
            &HashMap::from([("thread".to_string(), "outer-session".to_string())]),
        )
        .unwrap();
        (temp, path)
    }

    #[cfg(unix)]
    fn strict_pool_from_script(
        temp: &std::path::Path,
        max_sessions: usize,
        script: &str,
        env: HashMap<String, String>,
    ) -> SessionPool {
        let config = AgentConfig {
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            working_dir: temp.display().to_string(),
            env,
            ..AgentConfig::default()
        };
        SessionPool::new_with_paths(
            config,
            max_sessions,
            60,
            HashMap::new(),
            temp.join("thread_map.json"),
            temp.join("session_meta.json"),
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
        .unwrap()
    }

    #[cfg(unix)]
    fn strict_test_pool(temp: &std::path::Path, max_sessions: usize) -> SessionPool {
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/load"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}'
      ;;
    *'"method":"session/close"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        strict_pool_from_script(temp, max_sessions, script, HashMap::new())
    }

    #[test]
    fn session_context_none_has_no_spawn_context() {
        assert_eq!(
            session_spawn_context(SessionContextMode::None, "discord:thread-123"),
            None
        );
    }

    #[test]
    fn session_context_openab_v1_preserves_exact_logical_key() {
        let context = session_spawn_context(SessionContextMode::OpenabV1, "discord:thread-123")
            .expect("OpenAB v1 should create broker-owned context");

        assert_eq!(context.logical_session_key(), "discord:thread-123");
        assert!(uuid::Uuid::parse_str(context.attempt_id()).is_ok());
    }

    #[test]
    fn session_context_openab_v1_mints_a_fresh_attempt_per_spawn() {
        let first = session_spawn_context(SessionContextMode::OpenabV1, "discord:thread-123")
            .expect("first context");
        let second = session_spawn_context(SessionContextMode::OpenabV1, "discord:thread-123")
            .expect("second context");

        assert_ne!(first.attempt_id(), second.attempt_id());
    }

    #[test]
    fn local_workdir_resolution_preserves_existing_precedence() {
        assert_eq!(
            resolve_effective_workdir(
                SessionContextMode::None,
                Some("/stored"),
                Some("/requested"),
                "/default",
            )
            .unwrap(),
            "/stored"
        );
        assert_eq!(
            resolve_effective_workdir(
                SessionContextMode::None,
                None,
                Some("/requested"),
                "/default",
            )
            .unwrap(),
            "/requested"
        );
    }

    #[test]
    fn isolated_session_rejects_broker_workspace_paths() {
        for (stored, requested) in [
            (Some("/stored"), None),
            (None, Some("/requested")),
            (Some("/stored"), Some("/requested")),
        ] {
            let error = resolve_effective_workdir(
                SessionContextMode::OpenabV1,
                stored,
                requested,
                "/bridge",
            )
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("does not accept broker workspace paths"));
        }
        assert_eq!(
            resolve_effective_workdir(SessionContextMode::OpenabV1, None, None, "/bridge").unwrap(),
            "/bridge"
        );
    }

    #[test]
    fn remove_if_same_handle_removes_matching_entry() {
        let expected = Arc::new(Mutex::new(1_u8));
        let mut map = HashMap::from([("thread".to_string(), Arc::clone(&expected))]);

        let removed = remove_if_same_handle(&mut map, "thread", &expected);

        assert!(removed.is_some());
        assert!(map.is_empty());
    }

    #[test]
    fn remove_if_same_handle_keeps_replaced_entry() {
        let stale = Arc::new(Mutex::new(1_u8));
        let fresh = Arc::new(Mutex::new(2_u8));
        let mut map = HashMap::from([("thread".to_string(), Arc::clone(&fresh))]);

        let removed = remove_if_same_handle(&mut map, "thread", &stale);

        assert!(removed.is_none());
        let current = map.get("thread").expect("entry should remain");
        assert!(Arc::ptr_eq(current, &fresh));
    }

    #[test]
    fn get_or_insert_gate_reuses_gate_for_same_thread() {
        let mut map = HashMap::new();

        let first = get_or_insert_gate(&mut map, "thread");
        let second = get_or_insert_gate(&mut map, "thread");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn classify_idle_marks_stale_by_time() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        let last_active = now - std::time::Duration::from_secs(120);
        assert!(classify_idle(last_active, true, cutoff));
    }

    #[test]
    fn classify_idle_marks_stale_by_death() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        assert!(classify_idle(now, false, cutoff));
    }

    #[test]
    fn classify_idle_keeps_fresh_alive_sessions() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        assert!(!classify_idle(now, true, cutoff));
    }

    #[test]
    fn better_candidate_prefers_empty_current() {
        assert!(better_candidate(None, Instant::now()));
    }

    #[test]
    fn better_candidate_prefers_older_last_active() {
        let older = Instant::now() - std::time::Duration::from_secs(120);
        let newer = Instant::now() - std::time::Duration::from_secs(30);
        assert!(better_candidate(Some(newer), older));
    }

    #[test]
    fn better_candidate_rejects_newer_last_active() {
        let older = Instant::now() - std::time::Duration::from_secs(120);
        let newer = Instant::now() - std::time::Duration::from_secs(30);
        assert!(!better_candidate(Some(older), newer));
    }

    #[test]
    fn classify_hung_detects_in_flight_session_past_threshold() {
        assert!(classify_hung(
            true,
            std::time::Duration::from_secs(200),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn classify_hung_ignores_in_flight_session_within_threshold() {
        assert!(!classify_hung(
            true,
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn classify_hung_never_marks_idle_sessions() {
        assert!(!classify_hung(
            false,
            std::time::Duration::from_secs(200),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn better_candidate_keeps_existing_on_equal_last_active() {
        let ts = Instant::now() - std::time::Duration::from_secs(60);
        assert!(!better_candidate(Some(ts), ts));
    }

    #[test]
    fn purge_session_entries_drops_all_entries_for_evicted_key_only() {
        let mut state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            lifecycle_handles: HashMap::new(),
            activity: HashMap::from([
                ("hung".to_string(), Arc::new(SessionActivity::new())),
                ("other".to_string(), Arc::new(SessionActivity::new())),
            ]),
            pgids: HashMap::from([("hung".to_string(), 1234), ("other".to_string(), 5678)]),
            suspended: HashMap::from([
                ("hung".to_string(), "session-hung".to_string()),
                ("other".to_string(), "session-other".to_string()),
            ]),
            persisted: HashMap::from([
                ("hung".to_string(), "session-hung".to_string()),
                ("other".to_string(), "session-other".to_string()),
            ]),
            creating: HashMap::from([("hung".to_string(), Arc::new(Mutex::new(())))]),
            session_workdirs: HashMap::from([("hung".to_string(), "/tmp/ws".to_string())]),
        };

        purge_session_entries(&mut state, "hung");

        // Evicted key must not be resumable: no suspended/persisted entry left.
        assert!(!state.activity.contains_key("hung"));
        assert!(!state.cancel_handles.contains_key("hung"));
        assert!(!state.pgids.contains_key("hung"));
        assert!(!state.suspended.contains_key("hung"));
        assert!(!state.persisted.contains_key("hung"));
        assert!(!state.session_workdirs.contains_key("hung"));
        // The creating gate is concurrency control, not session state: it must
        // survive so an in-flight get_or_create holder stays serialized.
        assert!(state.creating.contains_key("hung"));
        assert_eq!(state.pgids.get("other"), Some(&5678));
        // Other keys survive untouched.
        assert_eq!(
            state.persisted.get("other"),
            Some(&"session-other".to_string())
        );
        assert_eq!(
            state.suspended.get("other"),
            Some(&"session-other".to_string())
        );
        assert!(state.activity.contains_key("other"));
    }

    #[test]
    fn persisted_mapping_can_include_active_and_suspended_sessions() {
        let persisted = HashMap::from([
            ("active-thread".to_string(), "session-active".to_string()),
            (
                "suspended-thread".to_string(),
                "session-suspended".to_string(),
            ),
        ]);

        let serialized =
            serde_json::to_string_pretty(&persisted).expect("serialize persisted mapping");
        let roundtrip: HashMap<String, String> =
            serde_json::from_str(&serialized).expect("deserialize persisted mapping");

        assert_eq!(
            roundtrip.get("active-thread"),
            Some(&"session-active".to_string())
        );
        assert_eq!(
            roundtrip.get("suspended-thread"),
            Some(&"session-suspended".to_string())
        );
    }

    #[test]
    fn mapping_file_write_is_atomic_and_round_trips() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("thread_map.json");
        let expected = HashMap::from([("thread".to_string(), "session".to_string())]);

        write_mapping_file(&path, &expected).unwrap();

        let actual: HashMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn mapping_file_write_surfaces_parent_errors() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("not-a-directory");
        std::fs::write(&parent, "file").unwrap();
        let path = parent.join("thread_map.json");

        let error = write_mapping_file(&path, &HashMap::new()).unwrap_err();

        assert!(error.to_string().contains("thread_map.json"));
    }

    #[test]
    fn isolated_context_rejects_corrupt_startup_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let mapping_path = temp.path().join("thread_map.json");
        std::fs::write(&mapping_path, "{not-json").unwrap();
        let pool = SessionPool::new_with_paths(
            AgentConfig::default(),
            1,
            60,
            HashMap::new(),
            mapping_path,
            temp.path().join("session_meta.json"),
        );

        let error = pool
            .try_with_session_context(SessionContextMode::OpenabV1)
            .err()
            .expect("strict mode must reject corrupt mappings");

        assert!(error
            .to_string()
            .contains("cannot enable Kubernetes session isolation"));
    }

    #[test]
    fn isolated_context_ignores_local_process_workspace_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let meta_path = temp.path().join("session_meta.json");
        write_mapping_file(
            &meta_path,
            &HashMap::from([("discord:thread".to_string(), "/broker/worktree".to_string())]),
        )
        .unwrap();
        let pool = SessionPool::new_with_paths(
            AgentConfig::default(),
            1,
            60,
            HashMap::new(),
            temp.path().join("thread_map.json"),
            meta_path,
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
        .unwrap();

        assert!(pool
            .state
            .try_read()
            .expect("pool state should be unlocked")
            .session_workdirs
            .is_empty());
    }

    #[tokio::test]
    async fn uncommitted_session_rollback_reports_primary_and_release_errors() {
        let success = FakeLifecycle::new(ReleaseBehavior::Succeed);
        let success_error =
            rollback_uncommitted_session(&success.handle(), anyhow!("mapping write failed")).await;
        assert!(success_error.to_string().contains("mapping write failed"));
        assert_eq!(success.release_calls.load(Ordering::Relaxed), 1);

        let failure = FakeLifecycle::new(ReleaseBehavior::Fail);
        let failure_error =
            rollback_uncommitted_session(&failure.handle(), anyhow!("mapping write failed")).await;
        let message = failure_error.to_string();
        assert!(message.contains("mapping write failed"));
        assert!(message.contains("controller rejected release"));
        assert_eq!(failure.release_calls.load(Ordering::Relaxed), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_create_releases_bridge_when_new_mapping_cannot_persist() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("released");
        let mapping_dir = temp.path().join("mapping");
        std::fs::create_dir(&mapping_dir).unwrap();
        let mapping_path = mapping_dir.join("thread_map.json");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s' released > "$MARKER"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let mut config = AgentConfig {
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            working_dir: temp.path().display().to_string(),
            ..AgentConfig::default()
        };
        config
            .env
            .insert("MARKER".to_string(), marker.display().to_string());
        let pool = SessionPool::new_with_paths(
            config,
            1,
            60,
            HashMap::new(),
            mapping_path,
            temp.path().join("session_meta.json"),
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
        .unwrap();

        std::fs::remove_dir(&mapping_dir).unwrap();
        std::fs::write(&mapping_dir, "not a directory").unwrap();
        let error = pool
            .get_or_create("discord:thread", None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("uncommitted isolated session"));
        assert_eq!(std::fs::read_to_string(marker).unwrap(), "released");
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(!state.persisted.contains_key("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_skips_session_while_its_lifecycle_gate_is_held() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());
        let gate = {
            let state = pool.state.read().await;
            Arc::clone(state.creating.get("discord:thread-a").unwrap())
        };
        let _lifecycle_guard = gate.lock().await;

        pool.cleanup_idle(0).await;

        assert!(pool
            .state
            .read()
            .await
            .active
            .contains_key("discord:thread-a"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hung_orphan_rechecks_a_turn_that_completed_after_classification() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let (connection, lifecycle, activity, gate) = {
            let state = pool.state.read().await;
            (
                Arc::clone(state.active.get("discord:thread").unwrap()),
                Arc::clone(state.lifecycle_handles.get("discord:thread").unwrap()),
                Arc::clone(state.activity.get("discord:thread").unwrap()),
                Arc::clone(state.creating.get("discord:thread").unwrap()),
            )
        };
        activity.set_in_flight(true);
        activity.set_last_active_ms(0);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        assert!(classify_hung(
            activity.in_flight(),
            activity.age(),
            std::time::Duration::ZERO,
        ));

        let outcome = orphan_hung_strict_session(
            &pool.state,
            "discord:thread",
            &connection,
            &lifecycle,
            &activity,
            &gate,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(outcome, StrictSuspendOutcome::Skipped);
        assert!(!activity.in_flight());
        assert!(pool
            .state
            .read()
            .await
            .active
            .contains_key("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_idle_cleanup_closes_before_marking_suspended() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());

        pool.cleanup_idle(0).await;

        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(!state.lifecycle_handles.contains_key("discord:thread"));
        assert_eq!(
            state.persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert_eq!(
            state.suspended.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_orphan_resumes_from_persisted_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let (connection, lifecycle, activity, gate) = {
            let state = pool.state.read().await;
            (
                Arc::clone(state.active.get("discord:thread").unwrap()),
                Arc::clone(state.lifecycle_handles.get("discord:thread").unwrap()),
                Arc::clone(state.activity.get("discord:thread").unwrap()),
                Arc::clone(state.creating.get("discord:thread").unwrap()),
            )
        };
        activity.set_in_flight(true);
        activity.set_last_active_ms(0);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let connection_guard = connection.lock().await;

        assert_eq!(
            orphan_hung_strict_session(
                &pool.state,
                "discord:thread",
                &connection,
                &lifecycle,
                &activity,
                &gate,
                std::time::Duration::ZERO,
            )
            .await
            .unwrap(),
            StrictSuspendOutcome::Orphaned
        );
        drop(connection_guard);
        drop(lifecycle);
        drop(connection);
        {
            let state = pool.state.read().await;
            assert!(state.persisted.contains_key("discord:thread"));
            assert!(!state.suspended.contains_key("discord:thread"));
        }

        assert!(!pool.get_or_create("discord:thread", None).await.unwrap());
        let connection = {
            let state = pool.state.read().await;
            Arc::clone(state.active.get("discord:thread").unwrap())
        };
        assert!(!connection.lock().await.session_reset);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_capacity_closes_old_worker_before_provisioning_new_one() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf 'new:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      printf 'close:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([("LOG".to_string(), log.display().to_string())]),
        );
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());

        assert!(pool.get_or_create("discord:thread-b", None).await.unwrap());

        let events = std::fs::read_to_string(log).unwrap();
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            vec![
                "new:discord:thread-a",
                "close:discord:thread-a",
                "new:discord:thread-b"
            ]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_capacity_does_not_provision_after_close_failure() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf 'new:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      printf 'close:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"close failed"}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([("LOG".to_string(), log.display().to_string())]),
        );
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());

        let error = pool
            .get_or_create("discord:thread-b", None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("orphaned"));
        let events = std::fs::read_to_string(log).unwrap();
        assert_eq!(
            events.lines().collect::<Vec<_>>(),
            vec!["new:discord:thread-a", "close:discord:thread-a"]
        );
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread-a"));
        assert!(state.persisted.contains_key("discord:thread-a"));
        assert!(!state.suspended.contains_key("discord:thread-a"));
        assert!(!state.persisted.contains_key("discord:thread-b"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_load_failure_never_falls_back_to_session_new() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("unexpected-new");
        write_mapping_file(
            &temp.path().join("thread_map.json"),
            &HashMap::from([("discord:thread".to_string(), "outer-session".to_string())]),
        )
        .unwrap();
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/load"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"retained state unavailable"}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s' new > "$MARKER"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"replacement"}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([("MARKER".to_string(), marker.display().to_string())]),
        );

        let error = pool
            .get_or_create("discord:thread", None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("could not restore"));
        assert!(!marker.exists());
        assert_eq!(
            pool.state.read().await.persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capacity_eviction_skips_session_in_a_lifecycle_transition() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());
        let gate = {
            let state = pool.state.read().await;
            Arc::clone(state.creating.get("discord:thread-a").unwrap())
        };
        let _lifecycle_guard = gate.lock().await;

        let error = pool
            .get_or_create("discord:thread-b", None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("pool exhausted"));
        let state = pool.state.read().await;
        assert!(state.active.contains_key("discord:thread-a"));
        assert!(!state.active.contains_key("discord:thread-b"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_prompt_rejects_a_connection_removed_while_queued() {
        let temp = tempfile::tempdir().unwrap();
        let pool = Arc::new(strict_test_pool(temp.path(), 1));
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let (connection, gate) = {
            let state = pool.state.read().await;
            (
                Arc::clone(state.active.get("discord:thread").unwrap()),
                Arc::clone(state.creating.get("discord:thread").unwrap()),
            )
        };
        let connection_guard = connection.lock().await;
        let closure_called = Arc::new(AtomicBool::new(false));
        let queued = tokio::spawn({
            let pool = Arc::clone(&pool);
            let closure_called = Arc::clone(&closure_called);
            async move {
                pool.with_connection("discord:thread", move |_| {
                    closure_called.store(true, Ordering::Relaxed);
                    Box::pin(async { Ok(()) })
                })
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while Arc::strong_count(&connection) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued prompt should capture the connection");

        let lifecycle_guard = gate.lock().await;
        pool.state.write().await.active.remove("discord:thread");
        drop(lifecycle_guard);
        drop(connection_guard);

        let error = queued.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("changed before prompt dispatch"));
        assert!(!closure_called.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn strict_reset_keeps_state_until_release_ack() {
        let fake = FakeLifecycle::new(ReleaseBehavior::Block);
        let handle = fake.handle();
        let state = Arc::new(RwLock::new(strict_state(Arc::clone(&handle))));
        let (_temp, mapping_path) = strict_mapping_file();
        let started = fake.release_started.notified();
        let release = tokio::spawn({
            let state = Arc::clone(&state);
            async move { release_strict_session(&state, &mapping_path, "thread", &handle).await }
        });

        started.await;
        assert!(state.read().await.persisted.contains_key("thread"));
        fake.release_continue.notify_waiters();
        release.await.unwrap().unwrap();

        let state = state.read().await;
        assert!(!state.persisted.contains_key("thread"));
        assert!(!state.suspended.contains_key("thread"));
        assert!(!state.session_workdirs.contains_key("thread"));
        assert!(!state.lifecycle_handles.contains_key("thread"));
        assert!(
            state.creating.contains_key("thread"),
            "strict reset must preserve the creation gate"
        );
        let persisted: HashMap<String, String> = serde_json::from_str(
            &std::fs::read_to_string(_temp.path().join("thread_map.json")).unwrap(),
        )
        .unwrap();
        assert!(!persisted.contains_key("thread"));
    }

    #[tokio::test]
    async fn strict_reset_preserves_state_when_release_fails() {
        let fake = FakeLifecycle::new(ReleaseBehavior::Fail);
        let handle = fake.handle();
        let state = RwLock::new(strict_state(Arc::clone(&handle)));
        let (_temp, mapping_path) = strict_mapping_file();

        let error = release_strict_session(&state, &mapping_path, "thread", &handle)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("controller rejected release"));
        let state = state.read().await;
        assert!(state.persisted.contains_key("thread"));
        assert!(state.suspended.contains_key("thread"));
        assert!(state.session_workdirs.contains_key("thread"));
        assert!(state.lifecycle_handles.contains_key("thread"));
    }

    #[tokio::test]
    async fn strict_reset_purges_dispatch_state_after_ack_even_if_persistence_fails() {
        let fake = FakeLifecycle::new(ReleaseBehavior::Succeed);
        let handle = fake.handle();
        let state = RwLock::new(strict_state(Arc::clone(&handle)));
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("not-a-directory");
        std::fs::write(&parent, "file").unwrap();
        let mapping_path = parent.join("thread_map.json");

        let error = release_strict_session(&state, &mapping_path, "thread", &handle)
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("release was accepted, but broker mapping removal"));
        let state = state.read().await;
        assert!(!state.active.contains_key("thread"));
        assert!(!state.lifecycle_handles.contains_key("thread"));
        assert!(!state.persisted.contains_key("thread"));
        assert!(!state.suspended.contains_key("thread"));
        assert!(!state.session_workdirs.contains_key("thread"));
    }

    #[tokio::test]
    async fn strict_reset_rejects_unadvertised_release_without_calling_it() {
        let fake = FakeLifecycle::unsupported();
        let handle = fake.handle();
        let state = RwLock::new(strict_state(Arc::clone(&handle)));
        let (_temp, mapping_path) = strict_mapping_file();

        let error = release_strict_session(&state, &mapping_path, "thread", &handle)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not advertised"));
        assert_eq!(fake.release_calls.load(Ordering::Relaxed), 0);
        assert!(state.read().await.persisted.contains_key("thread"));
    }

    #[tokio::test]
    async fn strict_reset_does_not_remove_a_replacement_handle() {
        let stale = FakeLifecycle::new(ReleaseBehavior::Block);
        let stale_handle = stale.handle();
        let replacement = FakeLifecycle::new(ReleaseBehavior::Succeed);
        let replacement_handle = replacement.handle();
        let state = Arc::new(RwLock::new(strict_state(Arc::clone(&stale_handle))));
        let (_temp, mapping_path) = strict_mapping_file();
        let started = stale.release_started.notified();
        let release = tokio::spawn({
            let state = Arc::clone(&state);
            async move { release_strict_session(&state, &mapping_path, "thread", &stale_handle).await }
        });

        started.await;
        state
            .write()
            .await
            .lifecycle_handles
            .insert("thread".to_string(), Arc::clone(&replacement_handle));
        stale.release_continue.notify_waiters();
        let error = release.await.unwrap().unwrap_err();

        assert!(error.to_string().contains("changed during release"));
        let state = state.read().await;
        assert!(state.persisted.contains_key("thread"));
        let current = state.lifecycle_handles.get("thread").unwrap();
        assert!(Arc::ptr_eq(current, &replacement_handle));
    }
}
