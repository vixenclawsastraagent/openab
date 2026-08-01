use super::{
    better_candidate, classify_hung, classify_idle, kill_pgid_after_grace, purge_session_entries,
    PoolState, SessionGate, SessionPool,
};
use crate::acp::connection::{
    AcpConnection, LifecycleHandle, SessionActivity, SessionSpawnContext,
};
use crate::acp::SessionContextMode;
use anyhow::{anyhow, Context, Result};
use futures_util::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

type StrictEvictionCandidate = (
    String,
    Arc<Mutex<AcpConnection>>,
    Instant,
    Option<String>,
    SessionGate,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StrictSuspendOutcome {
    Suspended,
    Orphaned,
    Skipped,
}

pub(super) const STRICT_RESET_BUDGET: Duration = Duration::from_secs(45);
pub(super) const STRICT_SHUTDOWN_SESSION_BUDGET: Duration = Duration::from_secs(35);
pub(super) const STRICT_SHUTDOWN_TOTAL_BUDGET: Duration = Duration::from_secs(45);
pub(super) const STRICT_SHUTDOWN_CONCURRENCY: usize = 8;

/// Tracks strict-runtime slots from admission until a worker is suspended or
/// released. A provisioning attempt occupies a slot before it starts, so
/// concurrent threads cannot temporarily create more workers than the pool
/// limit. The synchronous mutex keeps reservation cleanup usable from `Drop`.
pub(super) struct StrictCapacity {
    max: usize,
    state: StdMutex<StrictCapacityState>,
}

struct StrictCapacityState {
    occupied: HashSet<String>,
    resetting: HashSet<String>,
    admission_open: bool,
}

impl StrictCapacity {
    pub(super) fn new(max: usize) -> Self {
        Self {
            max,
            state: StdMutex::new(StrictCapacityState {
                occupied: HashSet::new(),
                resetting: HashSet::new(),
                admission_open: true,
            }),
        }
    }

    fn reserve(&self, key: &str) -> Result<Option<StrictSlotReservation<'_>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("isolated session capacity lock is poisoned"))?;
        if !state.admission_open {
            return Err(anyhow!("isolated session pool is shutting down"));
        }
        if state.resetting.contains(key) {
            return Err(anyhow!(
                "isolated session for thread {key} is quarantined during reset"
            ));
        }
        if state.occupied.contains(key) {
            return Ok(Some(StrictSlotReservation::existing(self, key)));
        }
        if state.occupied.len() >= self.max {
            return Ok(None);
        }
        state.occupied.insert(key.to_string());
        Ok(Some(StrictSlotReservation::new(self, key)))
    }

    fn transfer(&self, from: &str, to: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("isolated session capacity lock is poisoned"))?;
        if !state.admission_open {
            return Err(anyhow!("isolated session pool is shutting down"));
        }
        if state.resetting.contains(to) {
            return Err(anyhow!(
                "isolated session for thread {to} is quarantined during reset"
            ));
        }
        if !state.occupied.remove(from) {
            return Err(anyhow!(
                "isolated session {from} did not own its capacity slot"
            ));
        }
        if !state.occupied.insert(to.to_string()) {
            state.occupied.insert(from.to_string());
            return Err(anyhow!(
                "isolated session {to} already owns a capacity slot"
            ));
        }
        Ok(())
    }

    fn release(&self, key: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.occupied.remove(key);
    }

    pub(super) fn ensure_session_admission_open(&self, key: &str) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("isolated session capacity lock is poisoned"))?;
        if !state.admission_open {
            return Err(anyhow!("isolated session pool is shutting down"));
        }
        if state.resetting.contains(key) {
            return Err(anyhow!(
                "isolated session for thread {key} is quarantined during reset"
            ));
        }
        Ok(())
    }

    fn begin_reset(&self, key: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("isolated session capacity lock is poisoned"))?;
        if !state.admission_open {
            return Err(anyhow!("isolated session pool is shutting down"));
        }
        state.resetting.insert(key.to_string());
        Ok(())
    }

    fn complete_reset(&self, key: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.resetting.remove(key);
    }

    fn close_admission(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.admission_open = false;
    }

    fn contains(&self, key: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .occupied
            .contains(key)
    }
}

pub(super) struct StrictSlotReservation<'a> {
    capacity: &'a StrictCapacity,
    key: String,
    release_on_drop: bool,
}

#[derive(Clone, Copy)]
pub(super) enum UncommittedSessionProvenance {
    /// The bridge created a new controller-owned session during this attempt.
    FreshOwned,
    /// The bridge loaded a pre-existing persisted session that the broker must
    /// preserve across rollback.
    ResumedBorrowed,
}

enum StrictCapacityTransition<'a> {
    Release(&'a StrictCapacity),
    Transfer {
        capacity: &'a StrictCapacity,
        to: &'a str,
    },
}

impl<'a> StrictSlotReservation<'a> {
    fn new(capacity: &'a StrictCapacity, key: &str) -> Self {
        Self {
            capacity,
            key: key.to_string(),
            release_on_drop: true,
        }
    }

    fn existing(capacity: &'a StrictCapacity, key: &str) -> Self {
        Self {
            capacity,
            key: key.to_string(),
            release_on_drop: false,
        }
    }

    /// Keep the slot occupied if provisioning fails after the bridge has
    /// started. At that point a worker may exist even when ACP initialization
    /// has not completed, so releasing capacity on drop would admit N+1.
    pub(super) fn mark_uncertain(&mut self) {
        self.release_on_drop = false;
    }

    /// Release a reserved slot only after the controller acknowledges that the
    /// associated worker has been released.
    fn release(mut self) {
        self.capacity.release(&self.key);
        self.release_on_drop = false;
    }

    pub(super) fn commit(mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for StrictSlotReservation<'_> {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.capacity.release(&self.key);
        }
    }
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

pub(super) async fn reserve_for_provisioning<'a>(
    pool: &'a SessionPool,
    thread_id: &str,
) -> Result<StrictSlotReservation<'a>> {
    if let Some(reservation) = pool.strict_capacity.reserve(thread_id)? {
        return Ok(reservation);
    }

    let snapshot = {
        let state = pool.state.read().await;
        state
            .active
            .iter()
            .filter_map(|(key, connection)| {
                state
                    .creating
                    .get(key)
                    .map(|gate| (key.clone(), Arc::clone(connection), Arc::clone(gate)))
            })
            .collect::<Vec<_>>()
    };

    let mut eviction_candidate: Option<StrictEvictionCandidate> = None;
    for (key, connection, gate) in snapshot {
        if key == thread_id {
            continue;
        }
        let Ok(_gate_guard) = gate.try_lock() else {
            continue;
        };
        let connection_handle = Arc::clone(&connection);
        let Ok(connection) = connection.try_lock() else {
            continue;
        };
        let candidate = (
            key,
            connection_handle,
            connection.last_active,
            connection.acp_session_id.clone(),
            Arc::clone(&gate),
        );
        if better_candidate(
            eviction_candidate.as_ref().map(|(_, _, time, _, _)| *time),
            candidate.2,
        ) {
            eviction_candidate = Some(candidate);
        }
    }

    let Some((key, expected_connection, _, _, gate)) = eviction_candidate.as_ref() else {
        return Err(anyhow!(
            "pool exhausted ({} sessions); no idle isolated session can be suspended",
            pool.max_sessions
        ));
    };
    match try_suspend_strict_session(
        &pool.state,
        key,
        expected_connection,
        gate,
        None,
        StrictCapacityTransition::Transfer {
            capacity: &pool.strict_capacity,
            to: thread_id,
        },
    )
    .await?
    {
        StrictSuspendOutcome::Suspended => {
            info!(evicted = %key, "pool full, suspended isolated session before provisioning");
            Ok(StrictSlotReservation::new(&pool.strict_capacity, thread_id))
        }
        StrictSuspendOutcome::Orphaned => Err(anyhow!(
            "pool full; isolated session {key} was orphaned for reconciliation"
        )),
        StrictSuspendOutcome::Skipped => Err(anyhow!(
            "pool exhausted ({} sessions); eviction candidate became busy",
            pool.max_sessions
        )),
    }
}

async fn orphan_if_current(
    pool: &SessionPool,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
    expected_lifecycle: &LifecycleHandle,
) -> Result<bool> {
    let mut state = pool.state.write().await;
    let same_connection = state
        .active
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_connection));
    let same_lifecycle = state
        .lifecycle_handles
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_lifecycle));
    if !same_connection || !same_lifecycle {
        return Ok(false);
    }
    park_strict_session(
        &mut state,
        key,
        expected_connection,
        expected_lifecycle,
        false,
    )?;
    Ok(true)
}

fn orphan_connection_in_state(
    state: &mut PoolState,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
) -> Result<bool> {
    let same_connection = state
        .active
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected_connection));
    if !same_connection {
        return Ok(false);
    }
    if !state.persisted.contains_key(key) {
        return Err(anyhow!(
            "isolated session for thread {key} has no persisted session mapping"
        ));
    }
    state.active.remove(key);
    state.cancel_handles.remove(key);
    state.lifecycle_handles.remove(key);
    state.activity.remove(key);
    state.pgids.remove(key);
    state.suspended.remove(key);
    Ok(true)
}

fn try_orphan_connection_if_current(
    pool: &SessionPool,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
) -> Result<Option<bool>> {
    let Ok(mut state) = pool.state.try_write() else {
        return Ok(None);
    };
    orphan_connection_in_state(&mut state, key, expected_connection).map(Some)
}

fn try_orphan_all_active(pool: &SessionPool) {
    let Ok(mut state) = pool.state.try_write() else {
        warn!("isolated shutdown could not acquire state for final orphan pass");
        return;
    };
    let remaining = state
        .active
        .iter()
        .map(|(key, connection)| (key.clone(), Arc::clone(connection)))
        .collect::<Vec<_>>();
    for (key, connection) in remaining {
        match orphan_connection_in_state(&mut state, &key, &connection) {
            Ok(true) => {
                warn!(thread_id = %key, "isolated session orphaned at shutdown boundary")
            }
            Ok(false) => {}
            Err(error) => {
                warn!(thread_id = %key, %error, "failed to mark isolated session orphaned")
            }
        }
    }
}

fn reset_failure_now(
    pool: &SessionPool,
    key: &str,
    expected_connection: &Arc<Mutex<AcpConnection>>,
    cause: impl std::fmt::Display,
) -> anyhow::Error {
    match try_orphan_connection_if_current(pool, key, expected_connection) {
        Ok(Some(true)) => anyhow!(
            "isolated session reset failed; session was orphaned for reconciliation: {cause}"
        ),
        Ok(Some(false)) => anyhow!(
            "isolated session reset failed and the session changed before it could be orphaned: {cause}"
        ),
        Ok(None) => anyhow!(
            "isolated session reset failed; session remains quarantined for reconciliation: {cause}"
        ),
        Err(orphan_error) => anyhow!(
            "isolated session reset failed ({cause}) and orphan transition failed: {orphan_error}"
        ),
    }
}

pub(super) async fn reset_strict_session(
    pool: &SessionPool,
    key: &str,
    budget: Duration,
) -> Result<()> {
    let deadline = Instant::now() + budget;
    // The synchronous fence is the logical quarantine when the async state
    // lock itself is unavailable. It is idempotent so a later reset can retry
    // reconciliation for the same key.
    pool.strict_capacity.begin_reset(key)?;

    let (mut connection, mut lifecycle, gate, mut persisted_state) =
        match tokio::time::timeout_at(deadline, pool.state.read()).await {
            Ok(state) => (
                state.active.get(key).cloned(),
                state.lifecycle_handles.get(key).cloned(),
                state.creating.get(key).cloned(),
                state
                    .persisted
                    .contains_key(key)
                    .then(|| state.suspended.contains_key(key)),
            ),
            Err(_) => {
                return Err(anyhow!(
                    "isolated session reset reached its deadline while reading state; session remains quarantined for reconciliation"
                ))
            }
        };
    let mut held_gate = None;
    if connection.is_none() {
        if let Some(provisioning_gate) = gate.as_ref() {
            // A creator owns this gate from before process spawn through final
            // publish or controller-acknowledged rollback. Keep the reset
            // fence installed until that attempt has fully quiesced.
            held_gate = Some(
                match tokio::time::timeout_at(
                    deadline,
                    Arc::clone(provisioning_gate).lock_owned(),
                )
                .await
                {
                    Ok(guard) => guard,
                    Err(_) => {
                        return Err(anyhow!(
                            "isolated session reset reached its deadline while provisioning was still active; session remains quarantined"
                        ))
                    }
                },
            );
            match tokio::time::timeout_at(deadline, pool.state.read()).await {
                Ok(state) => {
                    connection = state.active.get(key).cloned();
                    lifecycle = state.lifecycle_handles.get(key).cloned();
                    persisted_state = state
                        .persisted
                        .contains_key(key)
                        .then(|| state.suspended.contains_key(key));
                }
                Err(_) => {
                    return Err(anyhow!(
                        "isolated session reset reached its deadline while rechecking provisioning state; session remains quarantined"
                    ))
                }
            }
        }
    }
    if connection.is_some() && held_gate.is_some() {
        // Preserve the global strict lock order (connection, then lifecycle
        // gate) if an already-committed connection appeared while the reset
        // was waiting for a provisioning attempt to leave the gate.
        held_gate.take();
    }
    let Some(connection) = connection else {
        if pool.strict_capacity.contains(key) {
            return Err(anyhow!(
                "isolated session for thread {key} has no active connection but still owns controller capacity; session remains quarantined and durable recovery is required by #1461"
            ));
        }
        // Suspended/orphaned sessions must reconnect before fenced release.
        // Clear the transient reset fence so that reconciliation is possible.
        pool.strict_capacity.complete_reset(key);
        if let Some(suspended) = persisted_state {
            let state_name = if suspended { "suspended" } else { "orphaned" };
            return Err(anyhow!(
                "isolated session for thread {key} is {state_name}; reconnect it before requesting fenced release"
            ));
        }
        return Err(anyhow!("no isolated session for thread {key}"));
    };
    let Some(lifecycle) = lifecycle else {
        return Err(reset_failure_now(
            pool,
            key,
            &connection,
            format_args!("isolated session for thread {key} has no lifecycle handle"),
        ));
    };
    let Some(gate) = gate else {
        return Err(reset_failure_now(
            pool,
            key,
            &connection,
            format_args!("isolated session for thread {key} has no lifecycle gate"),
        ));
    };

    match tokio::time::timeout_at(deadline, lifecycle.cancel()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(reset_failure_now(pool, key, &connection, error)),
        Err(_) => {
            return Err(reset_failure_now(
                pool,
                key,
                &connection,
                "session/cancel timed out",
            ))
        }
    }

    let connection_guard = match tokio::time::timeout_at(deadline, connection.lock()).await {
        Ok(guard) => guard,
        Err(_) => {
            return Err(reset_failure_now(
                pool,
                key,
                &connection,
                "active prompt did not quiesce before the reset deadline",
            ))
        }
    };
    let _gate_guard = match held_gate {
        Some(guard) => guard,
        None => match tokio::time::timeout_at(deadline, Arc::clone(&gate).lock_owned()).await {
            Ok(guard) => guard,
            Err(_) => {
                return Err(reset_failure_now(
                    pool,
                    key,
                    &connection,
                    "lifecycle gate did not become available before the reset deadline",
                ))
            }
        },
    };

    if !connection_guard.alive() {
        return Err(reset_failure_now(
            pool,
            key,
            &connection,
            "isolated bridge exited before fenced release",
        ));
    }
    if !lifecycle.capabilities().release_v1 {
        return Err(reset_failure_now(
            pool,
            key,
            &connection,
            "_openab/session/release capability was not advertised",
        ));
    }

    // Physically detach the current connection before asking the controller
    // to perform destructive release. The persisted mapping and capacity slot
    // intentionally survive this transition as reconciliation evidence.
    let expected_session_id = match tokio::time::timeout_at(deadline, pool.state.write()).await {
        Ok(mut state) => {
            let same_connection = state
                .active
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, &connection));
            let same_lifecycle = state
                .lifecycle_handles
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, &lifecycle));
            if !same_connection || !same_lifecycle {
                return Err(anyhow!("isolated session changed before fenced release"));
            }
            let expected_session_id = state.persisted.get(key).cloned().ok_or_else(|| {
                anyhow!("isolated session for thread {key} has no persisted session mapping")
            })?;
            park_strict_session(&mut state, key, &connection, &lifecycle, false)?;
            expected_session_id
        }
        Err(_) => {
            return Err(reset_failure_now(
                pool,
                key,
                &connection,
                "state could not be orphaned before the reset deadline",
            ))
        }
    };

    match tokio::time::timeout_at(deadline, lifecycle.release()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(anyhow!(
                "isolated session release failed; session was orphaned for reconciliation: {error}"
            ))
        }
        Err(_) => {
            return Err(anyhow!(
                "isolated session release timed out; session was orphaned for reconciliation"
            ))
        }
    }

    let mut state = match tokio::time::timeout_at(deadline, pool.state.write()).await {
        Ok(state) => state,
        Err(_) => {
            return Err(anyhow!(
                "isolated session release was acknowledged, but reset finalization missed its deadline; mapping and capacity were retained, the session remains quarantined, and durable recovery is required by #1461"
            ))
        }
    };
    if Instant::now() >= deadline {
        return Err(anyhow!(
            "isolated session release was acknowledged, but reset finalization missed its deadline; mapping and capacity were retained, the session remains quarantined, and durable recovery is required by #1461"
        ));
    }
    if state.active.contains_key(key)
        || state.persisted.get(key) != Some(&expected_session_id)
        || state.suspended.contains_key(key)
    {
        return Err(anyhow!(
            "isolated session changed after release acknowledgement; mapping and capacity were retained, the session remains quarantined, and durable recovery is required by #1461"
        ));
    }

    let mut persisted = state.persisted.clone();
    persisted.remove(key);
    write_mapping_file(&pool.mapping_path, &persisted).map_err(|error| {
        anyhow!(
            "isolated session release was acknowledged, but broker mapping removal could not be persisted; mapping and capacity were retained, the session remains quarantined, and durable recovery is required by #1461: {error}"
        )
    })?;
    state.persisted = persisted;
    purge_session_entries(&mut state, key);
    pool.strict_capacity.release(key);
    pool.strict_capacity.complete_reset(key);
    Ok(())
}

async fn shutdown_one_strict(
    pool: &SessionPool,
    key: &str,
    connection: &Arc<Mutex<AcpConnection>>,
    lifecycle: &LifecycleHandle,
    gate: &SessionGate,
) -> Result<StrictSuspendOutcome> {
    let connection_guard = connection.lock().await;
    let _gate_guard = gate.lock().await;

    {
        let state = pool.state.read().await;
        let same_connection = state
            .active
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, connection));
        let same_lifecycle = state
            .lifecycle_handles
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, lifecycle));
        if !same_connection || !same_lifecycle {
            return Ok(StrictSuspendOutcome::Skipped);
        }
    }

    if !connection_guard.alive() {
        orphan_if_current(pool, key, connection, lifecycle).await?;
        return Ok(StrictSuspendOutcome::Orphaned);
    }
    if !lifecycle.capabilities().close {
        orphan_if_current(pool, key, connection, lifecycle).await?;
        return Err(anyhow!(
            "session/close capability was not advertised; session orphaned"
        ));
    }

    match lifecycle.close().await {
        Ok(()) => {
            let mut state = pool.state.write().await;
            park_strict_session(&mut state, key, connection, lifecycle, true)?;
            pool.strict_capacity.release(key);
            Ok(StrictSuspendOutcome::Suspended)
        }
        Err(error) => {
            orphan_if_current(pool, key, connection, lifecycle).await?;
            Err(anyhow!(
                "session/close failed during shutdown; session orphaned: {error}"
            ))
        }
    }
}

pub(super) async fn shutdown_strict_with_limits(
    pool: &SessionPool,
    per_session_budget: Duration,
    total_budget: Duration,
    concurrency: usize,
) {
    let deadline = Instant::now() + total_budget;
    pool.strict_capacity.close_admission();

    let (snapshot, all_connections, provisioning_gates) =
        match tokio::time::timeout_at(deadline, pool.state.read()).await {
            Ok(state) => {
                let all_connections = state
                    .active
                    .iter()
                    .map(|(key, connection)| (key.clone(), Arc::clone(connection)))
                    .collect::<Vec<_>>();
                let snapshot = all_connections
                    .iter()
                    .filter_map(|(key, connection)| {
                        Some((
                            key.clone(),
                            Arc::clone(connection),
                            Arc::clone(state.lifecycle_handles.get(key)?),
                            Arc::clone(state.creating.get(key)?),
                        ))
                    })
                    .collect::<Vec<_>>();
                // Admission is already closed, and get_or_create rechecks it
                // while installing a gate under this same state lock. These
                // are therefore all gates that could belong to an attempt
                // admitted before shutdown began.
                let provisioning_gates = state
                    .creating
                    .iter()
                    .map(|(key, gate)| (key.clone(), Arc::clone(gate)))
                    .collect::<Vec<_>>();
                if snapshot.len() != all_connections.len() {
                    warn!(
                        active_count = all_connections.len(),
                        complete_count = snapshot.len(),
                        "isolated shutdown found incomplete lifecycle entries"
                    );
                }
                (snapshot, all_connections, provisioning_gates)
            }
            Err(_) => {
                warn!(
                total_budget_secs = total_budget.as_secs_f64(),
                "isolated session shutdown could not snapshot state before its overall deadline"
            );
                try_orphan_all_active(pool);
                return;
            }
        };
    let count = all_connections.len();

    let shutdown = stream::iter(snapshot).for_each_concurrent(
        concurrency.max(1),
        |(key, connection, lifecycle, gate)| async move {
            let session_deadline = std::cmp::min(deadline, Instant::now() + per_session_budget);
            match tokio::time::timeout_at(
                session_deadline,
                shutdown_one_strict(pool, &key, &connection, &lifecycle, &gate),
            )
            .await
            {
                Ok(Ok(StrictSuspendOutcome::Suspended)) => {
                    info!(thread_id = %key, "suspended isolated session during shutdown");
                }
                Ok(Ok(StrictSuspendOutcome::Orphaned)) => {
                    warn!(thread_id = %key, "dead isolated session orphaned during shutdown");
                }
                Ok(Ok(StrictSuspendOutcome::Skipped)) => {}
                Ok(Err(error)) => {
                    warn!(thread_id = %key, %error, "isolated session shutdown was not acknowledged");
                }
                Err(_) => match try_orphan_connection_if_current(pool, &key, &connection) {
                    Ok(Some(true)) => warn!(
                        thread_id = %key,
                        "isolated session exceeded its shutdown deadline and was orphaned"
                    ),
                    Ok(Some(false)) => {}
                    Ok(None) => warn!(
                        thread_id = %key,
                        "isolated session exceeded its shutdown deadline; final orphan pass deferred"
                    ),
                    Err(error) => warn!(
                        thread_id = %key,
                        %error,
                        "isolated session exceeded its shutdown deadline and could not be orphaned"
                    ),
                },
            }
        },
    );

    if tokio::time::timeout_at(deadline, shutdown).await.is_err() {
        warn!(
            total_budget_secs = total_budget.as_secs_f64(),
            "isolated session shutdown reached its overall deadline"
        );
    }

    let provisioning_count = provisioning_gates.len();
    let quiesce_provisioning = stream::iter(provisioning_gates).for_each_concurrent(
        concurrency.max(1),
        |(_key, gate)| async move {
            // A creator holds this gate through final admission rejection and
            // controller-acknowledged close/release rollback. Acquiring it
            // proves that attempt can no longer publish or leak silently.
            let _gate_guard = gate.lock().await;
        },
    );
    if tokio::time::timeout_at(deadline, quiesce_provisioning)
        .await
        .is_err()
    {
        warn!(
            provisioning_count,
            "isolated shutdown reached its deadline before provisioning rollback quiesced; unfinished workers remain uncertain for controller reconciliation"
        );
    }

    // Anything still active, including a replacement installed after the
    // initial snapshot, is uncertain. Detach it as an orphan while preserving
    // the outer mapping and capacity slot for reconciliation. This pass is
    // deliberately non-blocking: no awaited lock may extend the hard overall
    // shutdown deadline.
    try_orphan_all_active(pool);

    info!(count, "isolated session pool shutdown complete");
}

pub(super) async fn shutdown_strict(pool: &SessionPool) {
    shutdown_strict_with_limits(
        pool,
        STRICT_SHUTDOWN_SESSION_BUDGET,
        STRICT_SHUTDOWN_TOTAL_BUDGET,
        STRICT_SHUTDOWN_CONCURRENCY,
    )
    .await;
}

pub(super) async fn rollback_uncommitted_session(
    lifecycle: &LifecycleHandle,
    cause: anyhow::Error,
    reservation: Option<StrictSlotReservation<'_>>,
    provenance: UncommittedSessionProvenance,
) -> anyhow::Error {
    match provenance {
        UncommittedSessionProvenance::FreshOwned => match lifecycle.release().await {
            Ok(()) => {
                if let Some(reservation) = reservation {
                    reservation.release();
                }
                anyhow!("{cause}; uncommitted isolated session was released")
            }
            Err(rollback) => {
                anyhow!("{cause}; failed to release uncommitted isolated session: {rollback}")
            }
        },
        UncommittedSessionProvenance::ResumedBorrowed => match lifecycle.close().await {
            Ok(()) => {
                if let Some(reservation) = reservation {
                    reservation.release();
                }
                anyhow!("{cause}; unpublished resumed isolated session was closed")
            }
            Err(rollback) => anyhow!(
                "{cause}; failed to close unpublished resumed isolated session; mapping and capacity were retained for reconciliation: {rollback}"
            ),
        },
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
    capacity_transition: StrictCapacityTransition<'_>,
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
            match capacity_transition {
                StrictCapacityTransition::Release(capacity) => capacity.release(key),
                StrictCapacityTransition::Transfer { capacity, to } => {
                    capacity.transfer(key, to)?;
                }
            }
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
                        StrictCapacityTransition::Release(&self.strict_capacity),
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
    use super::super::{classify_hung, resolve_effective_workdir, SessionPool};
    use super::{
        orphan_hung_strict_session, reset_strict_session, rollback_uncommitted_session,
        session_spawn_context, shutdown_strict_with_limits, write_mapping_file, StrictCapacity,
        StrictSuspendOutcome, UncommittedSessionProvenance,
    };
    use crate::acp::connection::{LifecycleCapabilities, LifecycleHandle, SessionLifecycleControl};
    use crate::acp::SessionContextMode;
    use crate::config::AgentConfig;
    use anyhow::{anyhow, Result};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    enum ReleaseBehavior {
        Succeed,
        Fail,
    }

    struct FakeLifecycle {
        capabilities: LifecycleCapabilities,
        release_calls: AtomicUsize,
        behavior: ReleaseBehavior,
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
            match self.behavior {
                ReleaseBehavior::Succeed => Ok(()),
                ReleaseBehavior::Fail => Err(anyhow!("controller rejected release")),
            }
        }
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
        let success_error = rollback_uncommitted_session(
            &success.handle(),
            anyhow!("mapping write failed"),
            None,
            UncommittedSessionProvenance::FreshOwned,
        )
        .await;
        assert!(success_error.to_string().contains("mapping write failed"));
        assert_eq!(success.release_calls.load(Ordering::Relaxed), 1);

        let failure = FakeLifecycle::new(ReleaseBehavior::Fail);
        let failure_error = rollback_uncommitted_session(
            &failure.handle(),
            anyhow!("mapping write failed"),
            None,
            UncommittedSessionProvenance::FreshOwned,
        )
        .await;
        let message = failure_error.to_string();
        assert!(message.contains("mapping write failed"));
        assert!(message.contains("controller rejected release"));
        assert_eq!(failure.release_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn acknowledged_rollback_releases_an_existing_strict_slot() {
        let capacity = StrictCapacity::new(1);
        capacity
            .reserve("discord:thread")
            .unwrap()
            .unwrap()
            .commit();
        let reservation = capacity.reserve("discord:thread").unwrap().unwrap();
        let lifecycle = FakeLifecycle::new(ReleaseBehavior::Succeed);

        rollback_uncommitted_session(
            &lifecycle.handle(),
            anyhow!("publish rejected"),
            Some(reservation),
            UncommittedSessionProvenance::FreshOwned,
        )
        .await;

        assert!(!capacity.contains("discord:thread"));
    }

    #[tokio::test]
    async fn failed_rollback_keeps_an_existing_strict_slot() {
        let capacity = StrictCapacity::new(1);
        capacity
            .reserve("discord:thread")
            .unwrap()
            .unwrap()
            .commit();
        let reservation = capacity.reserve("discord:thread").unwrap().unwrap();
        let lifecycle = FakeLifecycle::new(ReleaseBehavior::Fail);

        rollback_uncommitted_session(
            &lifecycle.handle(),
            anyhow!("publish rejected"),
            Some(reservation),
            UncommittedSessionProvenance::FreshOwned,
        )
        .await;

        assert!(capacity.contains("discord:thread"));
    }

    #[test]
    fn shutdown_rejects_capacity_transfer_without_losing_the_source_slot() {
        let capacity = StrictCapacity::new(1);
        capacity
            .reserve("discord:thread-a")
            .unwrap()
            .unwrap()
            .commit();
        capacity.close_admission();

        let error = capacity
            .transfer("discord:thread-a", "discord:thread-b")
            .unwrap_err();

        assert!(error.to_string().contains("shutting down"));
        assert!(capacity.contains("discord:thread-a"));
        assert!(!capacity.contains("discord:thread-b"));
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
        assert!(!pool.strict_capacity.contains("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_spawn_failure_releases_an_unstarted_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let config = AgentConfig {
            command: temp.path().join("missing-bridge").display().to_string(),
            working_dir: temp.path().display().to_string(),
            ..AgentConfig::default()
        };
        let pool = SessionPool::new_with_paths(
            config,
            1,
            60,
            HashMap::new(),
            temp.path().join("thread_map.json"),
            temp.path().join("session_meta.json"),
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
        .unwrap();

        let error = pool
            .get_or_create("discord:thread", None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("failed to spawn"));
        assert!(!pool.strict_capacity.contains("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_initialize_failure_keeps_uncertain_slot_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"worker unavailable"}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(temp.path(), 1, script, HashMap::new());

        let error = pool
            .get_or_create("discord:thread-a", None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("worker unavailable"));
        assert!(pool.strict_capacity.contains("discord:thread-a"));

        let other_error = pool
            .get_or_create("discord:thread-b", None)
            .await
            .unwrap_err();
        assert!(other_error.to_string().contains("pool exhausted"));
        assert!(!pool.strict_capacity.contains("discord:thread-b"));
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
        assert!(!pool.strict_capacity.contains("discord:thread-a"));
        assert!(pool.strict_capacity.contains("discord:thread-b"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn isolated_capacity_reserves_a_slot_before_worker_provisioning() {
        let temp = tempfile::tempdir().unwrap();
        let started = temp.path().join("started");
        let unblock = temp.path().join("unblock");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' "$OPENAB_SESSION_KEY" >> "$STARTED"
      while [ ! -f "$UNBLOCK" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                ("STARTED".to_string(), started.display().to_string()),
                ("UNBLOCK".to_string(), unblock.display().to_string()),
            ]),
        ));

        let first = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.get_or_create("discord:thread-a", None).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if std::fs::read_to_string(&started)
                    .is_ok_and(|contents| contents.contains("discord:thread-a"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first worker should begin provisioning");

        let second = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            pool.get_or_create("discord:thread-b", None),
        )
        .await;
        std::fs::write(&unblock, "go").unwrap();
        let second_error = second
            .expect("a reserved pool must reject before provisioning")
            .unwrap_err();
        assert!(second_error.to_string().contains("pool exhausted"));

        assert!(first.await.unwrap().unwrap());
        assert_eq!(
            std::fs::read_to_string(started)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["discord:thread-a"]
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
        assert!(pool.strict_capacity.contains("discord:thread"));
        let other_error = pool
            .get_or_create("discord:other-thread", None)
            .await
            .unwrap_err();
        assert!(other_error.to_string().contains("pool exhausted"));
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

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_cancels_and_waits_for_the_active_prompt_before_release() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/cancel"'*)
      printf '%s\n' cancel >> "$LOG"
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([("LOG".to_string(), log.display().to_string())]),
        ));
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let connection = {
            let state = pool.state.read().await;
            Arc::clone(state.active.get("discord:thread").unwrap())
        };
        let prompt_guard = connection.lock().await;

        let reset = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(2))
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if std::fs::read_to_string(&log).is_ok_and(|contents| contents.contains("cancel")) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reset should cancel the active prompt first");
        assert_eq!(std::fs::read_to_string(&log).unwrap(), "cancel\n");
        assert!(pool
            .state
            .read()
            .await
            .persisted
            .contains_key("discord:thread"));

        drop(prompt_guard);
        reset.await.unwrap().unwrap();

        assert_eq!(std::fs::read_to_string(log).unwrap(), "cancel\nrelease\n");
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(!state.persisted.contains_key("discord:thread"));
        assert!(!pool.strict_capacity.contains("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_parks_before_destructive_release_ack() {
        let temp = tempfile::tempdir().unwrap();
        let release_started = temp.path().join("release-started");
        let release_continue = temp.path().join("release-continue");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/cancel"'*) ;;
    *'"method":"_openab/session/release"'*)
      printf '%s' started > "$RELEASE_STARTED"
      while [ ! -f "$RELEASE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "RELEASE_STARTED".to_string(),
                    release_started.display().to_string(),
                ),
                (
                    "RELEASE_CONTINUE".to_string(),
                    release_continue.display().to_string(),
                ),
            ]),
        ));
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let reset = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(1))
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !release_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reset should reach controller release");

        {
            let state = pool.state.read().await;
            assert!(!state.active.contains_key("discord:thread"));
            assert!(state.persisted.contains_key("discord:thread"));
            assert!(!state.suspended.contains_key("discord:thread"));
        }
        assert!(pool.strict_capacity.contains("discord:thread"));
        let prompt_error = pool
            .with_connection("discord:thread", |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap_err();
        assert!(prompt_error.to_string().contains("reset"));

        std::fs::write(release_continue, "continue").unwrap();
        reset.await.unwrap().unwrap();
        assert!(!pool.strict_capacity.contains("discord:thread"));
        assert!(!pool
            .state
            .read()
            .await
            .persisted
            .contains_key("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_post_ack_state_timeout_retains_reconciliation_state() {
        let temp = tempfile::tempdir().unwrap();
        let release_started = temp.path().join("release-started");
        let release_continue = temp.path().join("release-continue");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/cancel"'*) ;;
    *'"method":"_openab/session/release"'*)
      printf '%s' started > "$RELEASE_STARTED"
      while [ ! -f "$RELEASE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "RELEASE_STARTED".to_string(),
                    release_started.display().to_string(),
                ),
                (
                    "RELEASE_CONTINUE".to_string(),
                    release_continue.display().to_string(),
                ),
            ]),
        ));
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let reset = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                reset_strict_session(
                    &pool,
                    "discord:thread",
                    std::time::Duration::from_millis(100),
                )
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !release_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reset should reach controller release");
        let state_guard = pool.state.read().await;
        std::fs::write(release_continue, "continue").unwrap();

        let error = tokio::time::timeout(std::time::Duration::from_millis(250), reset)
            .await
            .expect("post-ACK finalization must obey the original reset deadline")
            .unwrap()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("finalization missed its deadline"));
        assert!(state_guard.persisted.contains_key("discord:thread"));
        assert!(!state_guard.active.contains_key("discord:thread"));
        assert!(pool.strict_capacity.contains("discord:thread"));
        let persisted: HashMap<String, String> = serde_json::from_str(
            &std::fs::read_to_string(temp.path().join("thread_map.json")).unwrap(),
        )
        .unwrap();
        assert!(persisted.contains_key("discord:thread"));
        drop(state_guard);
        let retry_error = reset_strict_session(
            &pool,
            "discord:thread",
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(retry_error
            .to_string()
            .contains("durable recovery is required by #1461"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_post_ack_persistence_error_retains_mapping_and_capacity() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
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
    *'"method":"session/cancel"'*) ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let config = AgentConfig {
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            working_dir: temp.path().display().to_string(),
            ..AgentConfig::default()
        };
        let pool = SessionPool::new_with_paths(
            config,
            1,
            60,
            HashMap::new(),
            mapping_path.clone(),
            temp.path().join("session_meta.json"),
        )
        .try_with_session_context(SessionContextMode::OpenabV1)
        .unwrap();
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        std::fs::set_permissions(&mapping_dir, std::fs::Permissions::from_mode(0o500)).unwrap();

        let error =
            reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(1))
                .await
                .unwrap_err();

        std::fs::set_permissions(&mapping_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(error
            .to_string()
            .contains("mapping and capacity were retained"));
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(state.persisted.contains_key("discord:thread"));
        assert!(pool.strict_capacity.contains("discord:thread"));
        let persisted: HashMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(mapping_path).unwrap()).unwrap();
        assert!(persisted.contains_key("discord:thread"));
        drop(state);
        let retry_error = reset_strict_session(
            &pool,
            "discord:thread",
            std::time::Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(retry_error
            .to_string()
            .contains("durable recovery is required by #1461"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_timeout_orphans_without_releasing_or_deleting_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/cancel"'*)
      printf '%s\n' cancel >> "$LOG"
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LOG"
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
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let connection = {
            let state = pool.state.read().await;
            Arc::clone(state.active.get("discord:thread").unwrap())
        };
        let prompt_guard = connection.lock().await;

        let error = reset_strict_session(
            &pool,
            "discord:thread",
            std::time::Duration::from_millis(25),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("orphaned for reconciliation"));
        assert!(error.to_string().contains("did not quiesce"));
        assert_eq!(std::fs::read_to_string(log).unwrap(), "cancel\n");
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(state.persisted.contains_key("discord:thread"));
        assert!(!state.suspended.contains_key("discord:thread"));
        assert!(pool.strict_capacity.contains("discord:thread"));
        drop(prompt_guard);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_hard_deadline_quarantines_when_state_write_is_blocked() {
        let temp = tempfile::tempdir().unwrap();
        let lifecycle_log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/cancel"'*)
      printf '%s\n' cancel >> "$LIFECYCLE_LOG"
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LIFECYCLE_LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([(
                "LIFECYCLE_LOG".to_string(),
                lifecycle_log.display().to_string(),
            )]),
        );
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let state_guard = pool.state.read().await;
        let started_at = std::time::Instant::now();

        let reset_result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reset_strict_session(
                &pool,
                "discord:thread",
                std::time::Duration::from_millis(20),
            ),
        )
        .await
        .expect("reset must return within its hard deadline");
        let reset_error = reset_result.unwrap_err();

        assert!(started_at.elapsed() < std::time::Duration::from_millis(100));
        assert!(reset_error.to_string().contains("deadline"));
        let prompt_error = pool
            .with_connection("discord:thread", |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap_err();
        assert!(prompt_error.to_string().contains("reset"));
        let config_error = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            pool.set_config_option("discord:thread", "model", "isolated"),
        )
        .await
        .expect("config mutation must fail fast while reset is quarantined")
        .unwrap_err();
        assert!(config_error.to_string().contains("reset"));
        let usage_error = pool.get_usage("discord:thread").await.unwrap_err();
        assert!(usage_error.to_string().contains("reset"));
        let reconnect_error = pool
            .get_or_create("discord:thread", None)
            .await
            .unwrap_err();
        assert!(reconnect_error.to_string().contains("reset"));
        assert_eq!(
            state_guard.persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert!(pool.strict_capacity.contains("discord:thread"));
        let persisted: HashMap<String, String> = serde_json::from_str(
            &std::fs::read_to_string(temp.path().join("thread_map.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert_eq!(std::fs::read_to_string(lifecycle_log).unwrap(), "cancel\n");

        drop(state_guard);
        reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(1))
            .await
            .expect("a quarantined reset must remain retryable");
        assert!(!pool.strict_capacity.contains("discord:thread"));
        assert!(pool.state.read().await.persisted.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_fences_an_in_flight_provision_before_publish() {
        let temp = tempfile::tempdir().unwrap();
        let initialize_started = temp.path().join("initialize-started");
        let initialize_continue = temp.path().join("initialize-continue");
        let lifecycle_log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s' started > "$INITIALIZE_STARTED"
      while [ ! -f "$INITIALIZE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LIFECYCLE_LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "INITIALIZE_STARTED".to_string(),
                    initialize_started.display().to_string(),
                ),
                (
                    "INITIALIZE_CONTINUE".to_string(),
                    initialize_continue.display().to_string(),
                ),
                (
                    "LIFECYCLE_LOG".to_string(),
                    lifecycle_log.display().to_string(),
                ),
            ]),
        ));
        let create = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.get_or_create("discord:thread", None).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !initialize_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provisioning should reach initialize");

        let reset = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(2))
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !reset.is_finished(),
            "reset must retain its fence until in-flight provisioning exits"
        );

        std::fs::write(initialize_continue, "continue").unwrap();
        let create_error = create.await.unwrap().unwrap_err();
        assert!(create_error.to_string().contains("reset"));
        let reset_error = reset.await.unwrap().unwrap_err();
        assert!(reset_error.to_string().contains("no isolated session"));
        assert_eq!(std::fs::read_to_string(lifecycle_log).unwrap(), "release\n");
        assert!(!pool.strict_capacity.contains("discord:thread"));
        let state = pool.state.read().await;
        assert!(state.active.is_empty());
        assert!(state.persisted.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_dead_bridge_becomes_orphan_without_release() {
        let temp = tempfile::tempdir().unwrap();
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      exit 0
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(temp.path(), 1, script, HashMap::new());
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let fake = FakeLifecycle::new(ReleaseBehavior::Succeed);
        {
            let mut state = pool.state.write().await;
            state
                .lifecycle_handles
                .insert("discord:thread".to_string(), fake.handle());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        let error =
            reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(1))
                .await
                .unwrap_err();

        assert!(error.to_string().contains("bridge exited"));
        assert_eq!(fake.release_calls.load(Ordering::Relaxed), 0);
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(state.persisted.contains_key("discord:thread"));
        assert!(!state.suspended.contains_key("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_release_error_becomes_orphan_without_deleting_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let fake = FakeLifecycle::new(ReleaseBehavior::Fail);
        {
            let mut state = pool.state.write().await;
            state
                .lifecycle_handles
                .insert("discord:thread".to_string(), fake.handle());
        }

        let error =
            reset_strict_session(&pool, "discord:thread", std::time::Duration::from_secs(1))
                .await
                .unwrap_err();

        assert!(error.to_string().contains("orphaned for reconciliation"));
        assert!(error.to_string().contains("controller rejected release"));
        assert_eq!(fake.release_calls.load(Ordering::Relaxed), 1);
        let state = pool.state.read().await;
        assert!(!state.active.contains_key("discord:thread"));
        assert!(state.persisted.contains_key("discord:thread"));
        assert!(!state.suspended.contains_key("discord:thread"));
        assert!(pool.strict_capacity.contains("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_reset_rejects_suspended_session_without_deleting_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let pool = strict_test_pool(temp.path(), 1);
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        pool.cleanup_idle(0).await;

        let error = pool.reset_session("discord:thread").await.unwrap_err();

        assert!(error.to_string().contains("is suspended"));
        let state = pool.state.read().await;
        assert_eq!(
            state.persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert_eq!(
            state.suspended.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert!(!pool.strict_capacity.contains("discord:thread"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_suspends_only_close_acknowledged_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      printf 'close:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      if [ "$OPENAB_SESSION_KEY" = "discord:thread-a" ]; then
        printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      else
        printf '%s\n' '{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"close failed"}}'
      fi
      ;;
    *'"method":"_openab/session/release"'*)
      printf 'release:%s\n' "$OPENAB_SESSION_KEY" >> "$LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{}}'
      ;;
  esac
done
"#;
        let pool = strict_pool_from_script(
            temp.path(),
            2,
            script,
            HashMap::from([("LOG".to_string(), log.display().to_string())]),
        );
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());
        assert!(pool.get_or_create("discord:thread-b", None).await.unwrap());

        shutdown_strict_with_limits(
            &pool,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(2),
            2,
        )
        .await;

        let state = pool.state.read().await;
        assert!(state.active.is_empty());
        assert!(state.persisted.contains_key("discord:thread-a"));
        assert!(state.persisted.contains_key("discord:thread-b"));
        assert!(state.suspended.contains_key("discord:thread-a"));
        assert!(!state.suspended.contains_key("discord:thread-b"));
        assert!(!pool.strict_capacity.contains("discord:thread-a"));
        assert!(pool.strict_capacity.contains("discord:thread-b"));
        let events = std::fs::read_to_string(log).unwrap();
        assert!(!events.contains("release:"));
        assert_eq!(
            events
                .lines()
                .filter(|line| line.starts_with("close:"))
                .count(),
            2
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_rejects_a_provisioning_attempt_before_publish() {
        let temp = tempfile::tempdir().unwrap();
        let initialize_started = temp.path().join("initialize-started");
        let initialize_continue = temp.path().join("initialize-continue");
        let release_started = temp.path().join("release-started");
        let release_continue = temp.path().join("release-continue");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s' started > "$INITIALIZE_STARTED"
      while [ ! -f "$INITIALIZE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s' started > "$RELEASE_STARTED"
      while [ ! -f "$RELEASE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "INITIALIZE_STARTED".to_string(),
                    initialize_started.display().to_string(),
                ),
                (
                    "INITIALIZE_CONTINUE".to_string(),
                    initialize_continue.display().to_string(),
                ),
                (
                    "RELEASE_STARTED".to_string(),
                    release_started.display().to_string(),
                ),
                (
                    "RELEASE_CONTINUE".to_string(),
                    release_continue.display().to_string(),
                ),
            ]),
        ));
        let create = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.get_or_create("discord:thread", None).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !initialize_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provisioning should reach initialize");

        let shutdown = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                shutdown_strict_with_limits(
                    &pool,
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(2),
                    1,
                )
                .await;
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must wait for pre-admitted provisioning"
        );
        std::fs::write(initialize_continue, "continue").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !release_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publish rejection should release the fresh worker");
        assert!(
            !shutdown.is_finished(),
            "shutdown must wait for controller release acknowledgement"
        );
        std::fs::write(release_continue, "continue").unwrap();

        let error = create.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("shutting down"));
        shutdown.await.unwrap();
        assert!(!pool.strict_capacity.contains("discord:thread"));
        assert!(pool.state.read().await.active.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_closes_unpublished_resume_without_releasing_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let mapping_path = temp.path().join("thread_map.json");
        write_mapping_file(
            &mapping_path,
            &HashMap::from([("discord:thread".to_string(), "outer-session".to_string())]),
        )
        .unwrap();
        let load_started = temp.path().join("load-started");
        let load_continue = temp.path().join("load-continue");
        let lifecycle_log = temp.path().join("lifecycle.log");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/load"'*)
      printf '%s' started > "$LOAD_STARTED"
      while [ ! -f "$LOAD_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' new >> "$LIFECYCLE_LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"unexpected-new"}}'
      ;;
    *'"method":"session/close"'*)
      printf '%s\n' close >> "$LIFECYCLE_LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LIFECYCLE_LOG"
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "LOAD_STARTED".to_string(),
                    load_started.display().to_string(),
                ),
                (
                    "LOAD_CONTINUE".to_string(),
                    load_continue.display().to_string(),
                ),
                (
                    "LIFECYCLE_LOG".to_string(),
                    lifecycle_log.display().to_string(),
                ),
            ]),
        ));
        let create = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.get_or_create("discord:thread", None).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !load_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resume should reach session/load");
        let state_guard = pool.state.read().await;
        std::fs::write(load_continue, "continue").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let shutdown = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                shutdown_strict_with_limits(
                    &pool,
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(2),
                    1,
                )
                .await;
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must wait for resumed rollback to quiesce"
        );
        drop(state_guard);

        let error = create.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("shutting down"));
        shutdown.await.unwrap();
        assert_eq!(std::fs::read_to_string(lifecycle_log).unwrap(), "close\n");
        let state = pool.state.read().await;
        assert!(state.active.is_empty());
        assert_eq!(
            state.persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert_eq!(
            state.suspended.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
        assert!(!pool.strict_capacity.contains("discord:thread"));
        let persisted: HashMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(mapping_path).unwrap()).unwrap();
        assert_eq!(
            persisted.get("discord:thread"),
            Some(&"outer-session".to_string())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_bounds_close_concurrency() {
        let temp = tempfile::tempdir().unwrap();
        let started = temp.path().join("close-started");
        let unblock = temp.path().join("unblock-close");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      printf '%s\n' "$OPENAB_SESSION_KEY" >> "$STARTED"
      while [ ! -f "$UNBLOCK" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            3,
            script,
            HashMap::from([
                ("STARTED".to_string(), started.display().to_string()),
                ("UNBLOCK".to_string(), unblock.display().to_string()),
            ]),
        ));
        for key in ["discord:thread-a", "discord:thread-b", "discord:thread-c"] {
            assert!(pool.get_or_create(key, None).await.unwrap());
        }

        let shutdown = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                shutdown_strict_with_limits(
                    &pool,
                    std::time::Duration::from_secs(2),
                    std::time::Duration::from_secs(3),
                    2,
                )
                .await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let count = std::fs::read_to_string(&started)
                    .map(|contents| contents.lines().count())
                    .unwrap_or(0);
                if count >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two close operations should start");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            std::fs::read_to_string(&started).unwrap().lines().count(),
            2,
            "the third close must wait for one of the two bounded slots"
        );

        std::fs::write(unblock, "go").unwrap();
        shutdown.await.unwrap();

        assert_eq!(std::fs::read_to_string(started).unwrap().lines().count(), 3);
        let state = pool.state.read().await;
        assert!(state.active.is_empty());
        assert_eq!(state.suspended.len(), 3);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_overall_deadline_orphans_unfinished_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      while :; do sleep 1; done
      ;;
    *'"method":"_openab/session/release"'*)
      printf '%s\n' release >> "$LOG"
      ;;
  esac
done
"#;
        let release_log = temp.path().join("release.log");
        let pool = strict_pool_from_script(
            temp.path(),
            2,
            script,
            HashMap::from([("LOG".to_string(), release_log.display().to_string())]),
        );
        assert!(pool.get_or_create("discord:thread-a", None).await.unwrap());
        assert!(pool.get_or_create("discord:thread-b", None).await.unwrap());

        let started_at = std::time::Instant::now();
        shutdown_strict_with_limits(
            &pool,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(50),
            1,
        )
        .await;
        assert!(
            started_at.elapsed() < std::time::Duration::from_millis(500),
            "shutdown exceeded its hard overall wall-clock budget: {:?}",
            started_at.elapsed()
        );

        let state = pool.state.read().await;
        assert!(state.active.is_empty());
        assert!(state.suspended.is_empty());
        assert_eq!(state.persisted.len(), 2);
        assert!(pool.strict_capacity.contains("discord:thread-a"));
        assert!(pool.strict_capacity.contains("discord:thread-b"));
        assert!(!release_log.exists(), "shutdown must never call release");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_shutdown_deadline_includes_close_acknowledgement_parking() {
        let temp = tempfile::tempdir().unwrap();
        let close_started = temp.path().join("close-started");
        let close_continue = temp.path().join("close-continue");
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"close":{},"_meta":{"openab.dev":{"sessionRelease":{"version":1}}}}}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"outer-session"}}'
      ;;
    *'"method":"session/close"'*)
      printf '%s' started > "$CLOSE_STARTED"
      while [ ! -f "$CLOSE_CONTINUE" ]; do sleep 0.01; done
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      ;;
  esac
done
"#;
        let pool = Arc::new(strict_pool_from_script(
            temp.path(),
            1,
            script,
            HashMap::from([
                (
                    "CLOSE_STARTED".to_string(),
                    close_started.display().to_string(),
                ),
                (
                    "CLOSE_CONTINUE".to_string(),
                    close_continue.display().to_string(),
                ),
            ]),
        ));
        assert!(pool.get_or_create("discord:thread", None).await.unwrap());
        let shutdown = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                shutdown_strict_with_limits(
                    &pool,
                    std::time::Duration::from_millis(50),
                    std::time::Duration::from_secs(1),
                    1,
                )
                .await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !close_started.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown should request close");
        let state_guard = pool.state.read().await;
        std::fs::write(close_continue, "continue").unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(250), shutdown)
            .await
            .expect("the per-session deadline must include parking after close ACK")
            .unwrap();
        assert!(state_guard.active.contains_key("discord:thread"));
    }
}
