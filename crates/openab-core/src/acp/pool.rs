mod isolated;

use crate::acp::connection::{AcpConnection, LifecycleHandle, SessionActivity};
use crate::acp::protocol::ConfigOption;
use crate::acp::SessionContextMode;
use crate::config::AgentConfig;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
    /// Strict-runtime capacity includes workers that are still provisioning;
    /// default local/AgentCore behavior never consults it.
    strict_capacity: isolated::StrictCapacity,
}

type CancelHandle = (Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>, String);
type SessionGate = Arc<Mutex<()>>;
type ActiveSnapshot = Vec<(String, Arc<Mutex<AcpConnection>>)>;
type EvictionCandidate = (String, Arc<Mutex<AcpConnection>>, Instant, Option<String>);

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
            strict_capacity: isolated::StrictCapacity::new(max_sessions),
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
        if self.session_context == SessionContextMode::OpenabV1 {
            self.strict_capacity
                .ensure_session_admission_open(thread_id)?;
        }
        let create_gate = {
            let mut state = self.state.write().await;
            // Linearize gate installation with strict reset admission. A
            // creator that passed the fast check above cannot install a new
            // provisioning gate after reset has fenced this key.
            if self.session_context == SessionContextMode::OpenabV1 {
                self.strict_capacity
                    .ensure_session_admission_open(thread_id)?;
            }
            get_or_insert_gate(&mut state.creating, thread_id)
        };
        let _create_guard = create_gate.lock().await;

        if self.session_context == SessionContextMode::OpenabV1 {
            self.strict_capacity
                .ensure_session_admission_open(thread_id)?;
        }

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

        let (eviction_candidate, skipped_locked_candidates, mut strict_reservation) =
            if self.session_context == SessionContextMode::None {
                // Snapshot active handles so we can inspect them outside the state lock.
                let snapshot: Vec<(String, Arc<Mutex<AcpConnection>>)> = {
                    let state = self.state.read().await;
                    state
                        .active
                        .iter()
                        .map(|(k, v)| (k.clone(), Arc::clone(v)))
                        .collect()
                };

                let mut eviction_candidate: Option<EvictionCandidate> = None;
                let mut skipped_locked_candidates = 0usize;
                for (key, conn) in snapshot {
                    if key == thread_id {
                        continue;
                    }
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
                    );
                    if better_candidate(
                        eviction_candidate.as_ref().map(|(_, _, t, _)| *t),
                        candidate.2,
                    ) {
                        eviction_candidate = Some(candidate);
                    }
                }
                (eviction_candidate, skipped_locked_candidates, None)
            } else {
                let reservation = isolated::reserve_for_provisioning(self, thread_id).await?;
                (None, 0, Some(reservation))
            };

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
        let session_spawn_context =
            isolated::session_spawn_context(self.session_context, thread_id);
        let mut new_conn = AcpConnection::spawn_with_context(
            &self.config.command,
            &self.config.args,
            &effective_workdir,
            &self.config.env,
            &self.config.inherit_env,
            session_spawn_context.as_ref(),
        )
        .await?;

        // Once the bridge process has started, a worker may exist even if ACP
        // initialization fails. Keep the strict slot occupied until a later
        // controller-acknowledged release proves that capacity is free.
        if let Some(reservation) = strict_reservation.as_mut() {
            reservation.mark_uncertain();
        }

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
        let uncommitted_provenance = if resumed {
            isolated::UncommittedSessionProvenance::ResumedBorrowed
        } else {
            isolated::UncommittedSessionProvenance::FreshOwned
        };
        let activity_handle = new_conn.activity_handle();
        let child_pgid = new_conn.child_pgid();
        let cancel_session_id = new_conn.acp_session_id.clone().unwrap_or_default();
        let new_conn = Arc::new(Mutex::new(new_conn));

        let mut state = self.state.write().await;

        // This check and the active-map publish are linearized by the state
        // write lock. Shutdown closes strict admission before taking its
        // snapshot: a publish accepted here is therefore visible to that
        // snapshot, while a later publish is rolled back and never installed.
        if self.session_context == SessionContextMode::OpenabV1 {
            if let Err(error) = self
                .strict_capacity
                .ensure_session_admission_open(thread_id)
            {
                drop(state);
                let lifecycle = lifecycle_handle
                    .as_ref()
                    .expect("strict sessions always have a lifecycle handle");
                return Err(isolated::rollback_uncommitted_session(
                    lifecycle,
                    error,
                    strict_reservation,
                    uncommitted_provenance,
                )
                .await);
            }
        }

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
            if let Some((key, expected_conn, _, sid)) = eviction_candidate {
                if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                    state.cancel_handles.remove(&key);
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
                return Err(isolated::rollback_uncommitted_session(
                    lifecycle,
                    error,
                    strict_reservation,
                    uncommitted_provenance,
                )
                .await);
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
            if let Err(error) = isolated::write_mapping_file(&self.mapping_path, &persisted) {
                drop(state);
                let lifecycle = lifecycle_handle
                    .as_ref()
                    .expect("strict sessions always have a lifecycle handle");
                return Err(isolated::rollback_uncommitted_session(
                    lifecycle,
                    error,
                    strict_reservation,
                    uncommitted_provenance,
                )
                .await);
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

        if let Some(reservation) = strict_reservation {
            reservation.commit();
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
        if self.session_context == SessionContextMode::OpenabV1 {
            self.strict_capacity
                .ensure_session_admission_open(thread_id)?;
        }
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
            self.strict_capacity
                .ensure_session_admission_open(thread_id)?;
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
        if self.session_context == SessionContextMode::OpenabV1 {
            let config_id = config_id.to_string();
            let value = value.to_string();
            return self
                .with_connection(thread_id, move |connection| {
                    Box::pin(async move { connection.set_config_option(&config_id, &value).await })
                })
                .await;
        }
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
        if self.session_context == SessionContextMode::OpenabV1 {
            return self
                .with_connection(thread_id, |connection| Box::pin(connection.get_usage()))
                .await;
        }
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
            isolated::reset_strict_session(self, thread_id, isolated::STRICT_RESET_BUDGET).await?;

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
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect();
            (
                snapshot,
                state.activity.clone(),
                state.cancel_handles.clone(),
                state.pgids.clone(),
            )
        };

        let mut stale = Vec::new();
        let mut hung: Vec<(String, Arc<Mutex<AcpConnection>>)> = Vec::new();
        for (key, conn) in snapshot {
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
                        hung.push((key, conn_handle));
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
                stale.push((key, conn_handle, conn.acp_session_id.clone()));
            }
        }

        if stale.is_empty() && hung.is_empty() {
            return;
        }

        let mut state = self.state.write().await;
        for (key, expected_conn, sid) in stale {
            if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                info!(thread_id = %key, "cleaning up idle session");
                state.cancel_handles.remove(&key);
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
        for (key, expected_conn) in hung {
            if !apply_hung_eviction(&mut state, &key, &expected_conn) {
                warn!(thread_id = %key, "hung session was replaced before eviction; maps untouched");
            }
        }
        self.save_mapping(&state.persisted);
        self.save_meta(&state.session_workdirs);
    }

    pub async fn shutdown(&self) {
        if self.session_context == SessionContextMode::OpenabV1 {
            isolated::shutdown_strict(self).await;
            return;
        }

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
        better_candidate, classify_hung, classify_idle, get_or_insert_gate, purge_session_entries,
        remove_if_same_handle, resolve_effective_workdir, PoolState, SessionPool,
    };
    use crate::acp::connection::SessionActivity;
    use crate::acp::SessionContextMode;
    use crate::config::AgentConfig;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::Instant;

    #[cfg(unix)]
    fn default_test_pool(temp: &std::path::Path, max_sessions: usize) -> SessionPool {
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"local-session"}}'
      ;;
    *'"method":"session/load"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}'
      ;;
  esac
done
"#;
        let config = AgentConfig {
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            working_dir: temp.display().to_string(),
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
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn default_capacity_eviction_ignores_held_lifecycle_gate() {
        let temp = tempfile::tempdir().unwrap();
        let pool = default_test_pool(temp.path(), 1);
        assert_eq!(pool.session_context, SessionContextMode::None);
        assert!(pool.get_or_create("thread-a", None).await.unwrap());
        let gate = {
            let state = pool.state.read().await;
            Arc::clone(state.creating.get("thread-a").unwrap())
        };
        let _gate_guard = gate.lock().await;

        assert!(pool.get_or_create("thread-b", None).await.unwrap());

        let state = pool.state.read().await;
        assert!(!state.active.contains_key("thread-a"));
        assert!(state.active.contains_key("thread-b"));
        assert_eq!(
            state.suspended.get("thread-a"),
            Some(&"local-session".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn default_idle_cleanup_ignores_held_lifecycle_gate() {
        let temp = tempfile::tempdir().unwrap();
        let pool = default_test_pool(temp.path(), 1);
        assert_eq!(pool.session_context, SessionContextMode::None);
        assert!(pool.get_or_create("thread-a", None).await.unwrap());
        let gate = {
            let state = pool.state.read().await;
            Arc::clone(state.creating.get("thread-a").unwrap())
        };
        let _gate_guard = gate.lock().await;

        pool.cleanup_idle(0).await;

        let state = pool.state.read().await;
        assert!(!state.active.contains_key("thread-a"));
        assert_eq!(
            state.suspended.get("thread-a"),
            Some(&"local-session".to_string())
        );
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
}
