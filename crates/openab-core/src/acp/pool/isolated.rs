use super::{
    classify_hung, classify_idle, kill_pgid_after_grace, purge_session_entries, PoolState,
    SessionGate, SessionPool,
};
use crate::acp::connection::{
    AcpConnection, LifecycleHandle, SessionActivity, SessionSpawnContext,
};
use crate::acp::SessionContextMode;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StrictSuspendOutcome {
    Suspended,
    Orphaned,
    Skipped,
}

pub(super) fn write_mapping_file(path: &Path, mapping: &HashMap<String, String>) -> Result<()> {
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

pub(super) fn session_spawn_context(
    mode: SessionContextMode,
    logical_session_key: &str,
) -> Option<SessionSpawnContext> {
    match mode {
        SessionContextMode::None => None,
        SessionContextMode::OpenabV1 => Some(SessionSpawnContext::new(logical_session_key)),
    }
}

pub(super) async fn release_strict_session(
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

pub(super) async fn rollback_uncommitted_session(
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

pub(super) async fn try_suspend_strict_session(
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

impl SessionPool {
    pub(super) async fn cleanup_idle_strict(&self, ttl_secs: u64) {
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
}

#[cfg(test)]
mod tests {
    use super::super::{classify_hung, resolve_effective_workdir, PoolState, SessionPool};
    use super::{
        orphan_hung_strict_session, release_strict_session, rollback_uncommitted_session,
        session_spawn_context, write_mapping_file, StrictSuspendOutcome,
    };
    use crate::acp::connection::{LifecycleCapabilities, LifecycleHandle, SessionLifecycleControl};
    use crate::acp::SessionContextMode;
    use crate::config::AgentConfig;
    use anyhow::{anyhow, Result};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify, RwLock};

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
