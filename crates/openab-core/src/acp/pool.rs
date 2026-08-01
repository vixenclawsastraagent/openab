mod isolated;

use crate::acp::connection::{
    AcpConnection, BrokerMappingExpectation, LifecycleHandle, SessionActivity,
};
use crate::acp::lifecycle::MappingAbsentInitialization;
use crate::acp::protocol::ConfigOption;
use crate::acp::SessionContextMode;
use crate::config::AgentConfig;
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};

/// Error substrings produced by `AcpConnection::send_request` that indicate a
/// transient failure worth preserving the session ID for retry, as opposed to
/// a permanent agent-side rejection.
const TRANSIENT_LOAD_ERRORS: &[&str] = &["timeout waiting for", "channel closed"];
const KUBERNETES_RUNTIME_STATE_VERSION: &str = "kubernetes-v1";
const MAX_KUBERNETES_SCOPE_BYTES: usize = 253;

fn kubernetes_scope_partition(scope: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"openab-scope-v1");
    hasher.update([0]);
    hasher.update(scope.as_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn validate_kubernetes_scope(scope: &str) -> Result<()> {
    if scope.trim().is_empty() || scope != scope.trim() || scope.len() > MAX_KUBERNETES_SCOPE_BYTES
    {
        return Err(anyhow!(
            "Kubernetes session scope must be non-empty, trimmed, and at most {MAX_KUBERNETES_SCOPE_BYTES} bytes"
        ));
    }
    Ok(())
}

fn kubernetes_openab_dir_from_home(home: Option<OsString>) -> Result<PathBuf> {
    let home = home
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME must be set for Kubernetes session isolation"))?;
    if !home.is_absolute() || home.parent().is_none() {
        return Err(anyhow!(
            "HOME must be an absolute, non-root directory for Kubernetes session isolation"
        ));
    }
    let metadata = std::fs::metadata(&home)
        .map_err(|error| anyhow!("failed to inspect HOME {}: {error}", home.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "HOME {} is not a directory for Kubernetes session isolation",
            home.display()
        ));
    }
    let home = std::fs::canonicalize(&home)
        .map_err(|error| anyhow!("failed to resolve HOME {}: {error}", home.display()))?;
    if !home.is_absolute() || home.parent().is_none() {
        return Err(anyhow!(
            "resolved HOME must be an absolute, non-root directory for Kubernetes session isolation"
        ));
    }
    Ok(home.join(".openab"))
}

fn reject_unsafe_mapping_entry(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(anyhow!(
            "Kubernetes session mapping {} must not be a symbolic link",
            path.display()
        )),
        Ok(metadata) if !metadata.is_file() => Err(anyhow!(
            "Kubernetes session mapping {} is not a regular file",
            path.display()
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(anyhow!(
            "failed to inspect Kubernetes session mapping {}: {error}",
            path.display()
        )),
    }
}

fn reject_symlinked_runtime_path(root: &Path, target: &Path) -> Result<()> {
    let relative = target.strip_prefix(root).map_err(|_| {
        anyhow!(
            "Kubernetes session state path {} escapes {}",
            target.display(),
            root.display()
        )
    })?;
    let mut current = root.to_path_buf();
    for component in std::iter::once(root.as_os_str()).chain(relative.iter()) {
        if component != root.as_os_str() {
            current.push(component);
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(anyhow!(
                    "Kubernetes session state path {} must not contain symbolic links",
                    current.display()
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(anyhow!(
                    "Kubernetes session state path {} is not a directory",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(anyhow!(
                    "failed to inspect Kubernetes session state path {}: {error}",
                    current.display()
                ));
            }
        }
    }
    Ok(())
}

fn create_private_runtime_directory(root: &Path, path: &Path) -> Result<()> {
    reject_symlinked_runtime_path(root, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700).create(path)?;
        reject_symlinked_runtime_path(root, path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(path)?;
        reject_symlinked_runtime_path(root, path)?;
    }

    Ok(())
}

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

fn durable_mapping_expectation(
    expected_persisted_session_id: Option<&str>,
) -> BrokerMappingExpectation {
    if expected_persisted_session_id.is_some() {
        BrokerMappingExpectation::Present
    } else {
        BrokerMappingExpectation::Absent
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

    /// Construct the explicitly enabled Kubernetes session runtime without
    /// reading or writing the local-process runtime's mapping or workspace
    /// metadata. Each configured scope owns a domain-separated state path.
    pub fn try_new_with_kubernetes_session_isolation(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        scope: &str,
    ) -> Result<Self> {
        let openab_dir = kubernetes_openab_dir_from_home(std::env::var_os("HOME"))?;
        Self::try_new_kubernetes_with_root(
            config,
            max_sessions,
            hung_threshold_secs,
            default_config_options,
            &openab_dir,
            scope,
        )
    }

    fn try_new_kubernetes_with_root(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        openab_dir: &Path,
        scope: &str,
    ) -> Result<Self> {
        validate_kubernetes_scope(scope)?;
        let runtime_directory = openab_dir
            .join("session-runtimes")
            .join(KUBERNETES_RUNTIME_STATE_VERSION)
            .join(kubernetes_scope_partition(scope));
        create_private_runtime_directory(openab_dir, &runtime_directory).map_err(|error| {
            anyhow!(
                "failed to create Kubernetes session state directory {}: {error}",
                runtime_directory.display()
            )
        })?;
        let mapping_path = runtime_directory.join("thread_map.json");
        reject_unsafe_mapping_entry(&mapping_path)?;

        Self::new_with_runtime_paths(
            config,
            max_sessions,
            hung_threshold_secs,
            default_config_options,
            mapping_path,
            runtime_directory.join("session_meta.json"),
            false,
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
    }

    fn new_with_paths(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        mapping_path: PathBuf,
        meta_path: PathBuf,
    ) -> Self {
        Self::new_with_runtime_paths(
            config,
            max_sessions,
            hung_threshold_secs,
            default_config_options,
            mapping_path,
            meta_path,
            true,
        )
    }

    fn new_with_runtime_paths(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        mapping_path: PathBuf,
        meta_path: PathBuf,
        load_session_workdirs: bool,
    ) -> Self {
        let (suspended, mapping_load_error) = Self::load_mapping(&mapping_path);
        let session_workdirs = if load_session_workdirs {
            Self::load_mapping(&meta_path).0
        } else {
            HashMap::new()
        };
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
    fn try_with_session_context(mut self, mode: SessionContextMode) -> Result<Self> {
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

        let (
            existing,
            saved_session_id,
            expected_persisted_session_id,
            broker_mapping_expectation,
            mut strict_session_snapshot,
        ) = {
            let state = self.state.read().await;
            let expected_persisted_session_id = state.persisted.get(thread_id).cloned();
            let suspended_session_id = state.suspended.get(thread_id).cloned();
            let saved_session_id = if self.session_context == SessionContextMode::OpenabV1 {
                if suspended_session_id.as_ref().is_some_and(|suspended| {
                    Some(suspended) != expected_persisted_session_id.as_ref()
                }) {
                    return Err(anyhow!(
                        "isolated suspended session does not match its durable mapping"
                    ));
                }
                expected_persisted_session_id.clone()
            } else {
                suspended_session_id
            };
            let strict_session_snapshot = (self.session_context == SessionContextMode::OpenabV1)
                .then(|| isolated::strict_session_snapshot(&state, thread_id));
            (
                state.active.get(thread_id).cloned(),
                saved_session_id,
                expected_persisted_session_id.clone(),
                durable_mapping_expectation(expected_persisted_session_id.as_deref()),
                strict_session_snapshot,
            )
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
            if self.session_context == SessionContextMode::None && saved_session_id.is_none() {
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
        let mut broker_mapping_expectation = broker_mapping_expectation;
        let mut mapping_was_repaired = false;
        // This loop can spawn at most twice. The only retry flips both the
        // expectation to Absent and the repair marker before the next spawn.
        let mut new_conn = loop {
            let session_spawn_context = isolated::session_spawn_context(
                self.session_context,
                thread_id,
                broker_mapping_expectation,
            );
            let mut candidate = AcpConnection::spawn_with_context(
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

            match candidate.initialize().await {
                Ok(()) => break candidate,
                Err(error)
                    if self.session_context == SessionContextMode::OpenabV1
                        && !mapping_was_repaired
                        && broker_mapping_expectation == BrokerMappingExpectation::Present
                        && error
                            .downcast_ref::<MappingAbsentInitialization>()
                            .is_some() =>
                {
                    // The typed response proves this activation did not attach
                    // to the broker's retained outer session. Drop the bridge
                    // before repairing broker state and starting a fresh,
                    // independently fenced activation attempt.
                    drop(candidate);
                    let expected_session_id = expected_persisted_session_id
                        .as_deref()
                        .ok_or_else(|| anyhow!("mapping repair requires a captured durable ID"))?;
                    let mut state = self.state.write().await;
                    let repaired_snapshot = isolated::repair_absent_mapping(
                        &mut state,
                        &self.mapping_path,
                        thread_id,
                        expected_session_id,
                        strict_session_snapshot
                            .as_ref()
                            .expect("strict sessions capture a mapping repair snapshot"),
                    )?;
                    strict_session_snapshot = Some(repaired_snapshot);
                    drop(state);

                    saved_session_id = None;
                    broker_mapping_expectation = BrokerMappingExpectation::Absent;
                    mapping_was_repaired = true;
                }
                Err(error) => return Err(error),
            }
        };

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
            if had_existing || saved_session_id.is_some() || mapping_was_repaired {
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

        if self.session_context == SessionContextMode::OpenabV1 {
            let snapshot = strict_session_snapshot
                .as_ref()
                .expect("strict sessions capture a publication snapshot");
            if let Err(error) =
                isolated::ensure_strict_session_snapshot(&state, thread_id, snapshot)
            {
                drop(state);
                let lifecycle = lifecycle_handle
                    .as_ref()
                    .expect("strict sessions always have a lifecycle handle");
                // A competing state may own this thread's worker. Release only
                // our unpublished candidate and retain the uncertain slot.
                return Err(isolated::rollback_uncommitted_session(
                    lifecycle,
                    error,
                    None,
                    uncommitted_provenance,
                )
                .await);
            }
        }

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
                if let Some(lifecycle) = lifecycle_handle.as_ref() {
                    drop(state);
                    return Err(isolated::rollback_uncommitted_session(
                        lifecycle,
                        anyhow!(
                            "isolated session became active while a replacement was provisioning"
                        ),
                        strict_reservation,
                        uncommitted_provenance,
                    )
                    .await);
                }
                return Ok(false);
            };
            if existing.alive() {
                if let Some(lifecycle) = lifecycle_handle.as_ref() {
                    drop(existing);
                    drop(state);
                    return Err(isolated::rollback_uncommitted_session(
                        lifecycle,
                        anyhow!(
                            "isolated session became active while a replacement was provisioning"
                        ),
                        strict_reservation,
                        uncommitted_provenance,
                    )
                    .await);
                }
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

        if self.session_context == SessionContextMode::OpenabV1 {
            let mut persisted = state.persisted.clone();
            if cancel_session_id.is_empty() {
                persisted.remove(thread_id);
            } else {
                persisted.insert(thread_id.to_string(), cancel_session_id.clone());
            }
            if persisted != state.persisted {
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
        } else if cancel_session_id.is_empty() {
            state.persisted.remove(thread_id);
        } else {
            state
                .persisted
                .insert(thread_id.to_string(), cancel_session_id.clone());
        }
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
        // A session with prior state (saved_session_id, had_existing, or a
        // repaired durable mapping) is a continuation even if a replacement
        // ACP session was created. ADR §2.2: directives are first-message-only.
        let is_fresh = !had_existing && saved_session_id.is_none() && !mapping_was_repaired;
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
        better_candidate, classify_hung, classify_idle, durable_mapping_expectation,
        get_or_insert_gate, purge_session_entries, remove_if_same_handle,
        resolve_effective_workdir, PoolState, SessionPool,
    };
    use crate::acp::connection::{BrokerMappingExpectation, SessionActivity};
    use crate::acp::SessionContextMode;
    use crate::config::AgentConfig;
    use std::collections::HashMap;
    use std::path::Path;
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

    fn kubernetes_test_pool(root: &Path, scope: &str) -> anyhow::Result<SessionPool> {
        SessionPool::try_new_kubernetes_with_root(
            AgentConfig::default(),
            2,
            60,
            HashMap::new(),
            root,
            scope,
        )
    }

    #[test]
    fn kubernetes_scope_partition_matches_the_controller_identity_vector() {
        assert_eq!(
            super::kubernetes_scope_partition("team-a"),
            "c7d126d05da76b40b912226a894e8acdc3c4f80d9b0f14f8f24a782ab0e61d67"
        );
    }

    #[test]
    fn mapping_expectation_uses_only_the_durable_broker_mapping() {
        let mut persisted = HashMap::new();
        assert_eq!(
            durable_mapping_expectation(persisted.get("discord:thread").map(String::as_str)),
            BrokerMappingExpectation::Absent
        );

        persisted.insert(
            "discord:thread".to_string(),
            "durable-outer-session".to_string(),
        );
        assert_eq!(
            durable_mapping_expectation(persisted.get("discord:thread").map(String::as_str)),
            BrokerMappingExpectation::Present
        );
        assert_eq!(
            durable_mapping_expectation(persisted.get("discord:other-thread").map(String::as_str)),
            BrokerMappingExpectation::Absent
        );
    }

    #[test]
    fn kubernetes_state_root_requires_a_real_absolute_home() {
        let temp = tempfile::tempdir().unwrap();
        let regular_file = temp.path().join("not-a-home");
        std::fs::write(&regular_file, "file").unwrap();

        for home in [
            None,
            Some(std::ffi::OsString::new()),
            Some(std::ffi::OsString::from("relative/home")),
            Some(std::ffi::OsString::from("/")),
            Some(regular_file.into_os_string()),
            Some(temp.path().join("missing").into_os_string()),
        ] {
            assert!(super::kubernetes_openab_dir_from_home(home).is_err());
        }

        assert_eq!(
            super::kubernetes_openab_dir_from_home(Some(temp.path().as_os_str().to_owned()))
                .unwrap(),
            temp.path().canonicalize().unwrap().join(".openab")
        );
    }

    #[cfg(unix)]
    #[test]
    fn kubernetes_state_root_rejects_a_home_symlink_resolving_to_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let linked_home = temp.path().join("linked-home");
        symlink("/", &linked_home).unwrap();

        assert!(
            super::kubernetes_openab_dir_from_home(Some(linked_home.into_os_string())).is_err()
        );
    }

    #[test]
    fn kubernetes_mapping_never_reads_the_local_runtime_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let local_mapping = temp.path().join("thread_map.json");
        std::fs::write(&local_mapping, r#"{"discord:local":"local-session"}"#).unwrap();

        let pool = kubernetes_test_pool(temp.path(), "team-a").unwrap();
        let state = pool.state.try_read().unwrap();

        assert!(state.persisted.is_empty());
        assert!(state.suspended.is_empty());
        assert_eq!(
            pool.mapping_path,
            temp.path()
                .join("session-runtimes/kubernetes-v1")
                .join(super::kubernetes_scope_partition("team-a"))
                .join("thread_map.json")
        );
        assert!(!pool.mapping_path.to_string_lossy().contains("team-a"));
        assert_eq!(
            std::fs::read_to_string(local_mapping).unwrap(),
            r#"{"discord:local":"local-session"}"#
        );
    }

    #[test]
    fn local_mapping_never_reads_a_kubernetes_scope_partition() {
        let temp = tempfile::tempdir().unwrap();
        let scope_directory = temp
            .path()
            .join("session-runtimes/kubernetes-v1")
            .join(super::kubernetes_scope_partition("team-a"));
        std::fs::create_dir_all(&scope_directory).unwrap();
        std::fs::write(
            scope_directory.join("thread_map.json"),
            r#"{"discord:strict":"strict-session"}"#,
        )
        .unwrap();
        std::fs::write(
            temp.path().join("thread_map.json"),
            r#"{"discord:local":"local-session"}"#,
        )
        .unwrap();

        let pool = SessionPool::new_with_paths(
            AgentConfig::default(),
            2,
            60,
            HashMap::new(),
            temp.path().join("thread_map.json"),
            temp.path().join("session_meta.json"),
        );
        let state = pool.state.try_read().unwrap();

        assert_eq!(
            state.persisted.get("discord:local").map(String::as_str),
            Some("local-session")
        );
        assert!(!state.persisted.contains_key("discord:strict"));
    }

    #[test]
    fn kubernetes_mapping_rejects_invalid_scopes_before_touching_disk() {
        let temp = tempfile::tempdir().unwrap();
        for scope in ["", " team-a", "team-a ", &"x".repeat(254)] {
            assert!(kubernetes_test_pool(temp.path(), scope).is_err());
        }
        assert!(!temp.path().join("session-runtimes").exists());
    }

    #[cfg(unix)]
    #[test]
    fn kubernetes_scope_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let pool = kubernetes_test_pool(temp.path(), "team-a").unwrap();
        let mode = pool
            .mapping_path
            .parent()
            .unwrap()
            .metadata()
            .unwrap()
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn kubernetes_state_path_rejects_preexisting_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let foreign = tempfile::tempdir().unwrap();
        symlink(foreign.path(), temp.path().join("session-runtimes")).unwrap();

        let error = kubernetes_test_pool(temp.path(), "team-a")
            .err()
            .expect("symlinked state parent must be rejected");

        assert!(error.to_string().contains("symbolic links"));
        assert!(foreign.path().read_dir().unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn kubernetes_mapping_rejects_a_preexisting_file_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let foreign = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(foreign.path(), r#"{"discord:foreign":"session"}"#).unwrap();
        let scope_directory = temp
            .path()
            .join("session-runtimes/kubernetes-v1")
            .join(super::kubernetes_scope_partition("team-a"));
        std::fs::create_dir_all(&scope_directory).unwrap();
        symlink(foreign.path(), scope_directory.join("thread_map.json")).unwrap();

        let error = kubernetes_test_pool(temp.path(), "team-a")
            .err()
            .expect("symlinked mapping must be rejected");

        assert!(error.to_string().contains("must not be a symbolic link"));
        assert_eq!(
            std::fs::read_to_string(foreign.path()).unwrap(),
            r#"{"discord:foreign":"session"}"#
        );
    }

    #[test]
    fn kubernetes_mapping_is_partitioned_between_scopes() {
        let temp = tempfile::tempdir().unwrap();
        let scope_a_directory = temp
            .path()
            .join("session-runtimes/kubernetes-v1")
            .join(super::kubernetes_scope_partition("team-a"));
        std::fs::create_dir_all(&scope_a_directory).unwrap();
        std::fs::write(
            scope_a_directory.join("thread_map.json"),
            r#"{"discord:thread":"scope-a-session"}"#,
        )
        .unwrap();

        let scope_a = kubernetes_test_pool(temp.path(), "team-a").unwrap();
        let scope_b = kubernetes_test_pool(temp.path(), "team-b").unwrap();

        assert_eq!(
            scope_a
                .state
                .try_read()
                .unwrap()
                .persisted
                .get("discord:thread")
                .map(String::as_str),
            Some("scope-a-session")
        );
        assert!(scope_b.state.try_read().unwrap().persisted.is_empty());
        assert_ne!(scope_a.mapping_path, scope_b.mapping_path);
    }

    #[test]
    fn corrupt_local_mapping_cannot_block_kubernetes_mode() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("thread_map.json"), "{not-json").unwrap();

        let pool = kubernetes_test_pool(temp.path(), "team-a").unwrap();

        assert!(pool.mapping_load_error.is_none());
        assert!(pool.state.try_read().unwrap().persisted.is_empty());
    }

    #[test]
    fn corrupt_kubernetes_mapping_blocks_only_its_scope() {
        let temp = tempfile::tempdir().unwrap();
        let scope_a_directory = temp
            .path()
            .join("session-runtimes/kubernetes-v1")
            .join(super::kubernetes_scope_partition("team-a"));
        std::fs::create_dir_all(&scope_a_directory).unwrap();
        std::fs::write(scope_a_directory.join("thread_map.json"), "{not-json").unwrap();

        let error = kubernetes_test_pool(temp.path(), "team-a")
            .err()
            .expect("scope A must reject its corrupt mapping");
        let scope_b = kubernetes_test_pool(temp.path(), "team-b").unwrap();

        assert!(error
            .to_string()
            .contains("cannot enable Kubernetes session isolation"));
        assert!(scope_b.state.try_read().unwrap().persisted.is_empty());
    }

    #[test]
    fn kubernetes_mode_does_not_load_session_workdir_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let scope_directory = temp
            .path()
            .join("session-runtimes/kubernetes-v1")
            .join(super::kubernetes_scope_partition("team-a"));
        std::fs::create_dir_all(&scope_directory).unwrap();
        std::fs::write(
            scope_directory.join("session_meta.json"),
            r#"{"discord:thread":"/broker/worktree"}"#,
        )
        .unwrap();

        let pool = kubernetes_test_pool(temp.path(), "team-a").unwrap();

        assert!(pool.state.try_read().unwrap().session_workdirs.is_empty());
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
