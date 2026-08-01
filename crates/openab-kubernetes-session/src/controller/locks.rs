use crate::identity::SessionId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, Weak};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Process-local coordination for every operation that mutates one session.
///
/// A controller must acquire this lock before activating, registering,
/// suspending, or releasing a session. The lock complements durable
/// Kubernetes compare-and-swap fences; it is not a distributed lock for
/// multiple controller replicas.
#[derive(Clone, Default)]
pub struct SessionLocks {
    inner: Arc<SessionLocksInner>,
}

#[derive(Default)]
struct SessionLocksInner {
    entries: StdMutex<HashMap<SessionId, Weak<SessionLockEntry>>>,
}

struct SessionLockEntry {
    session_id: SessionId,
    gate: Arc<AsyncMutex<()>>,
    registry: Weak<SessionLocksInner>,
}

/// Exclusive ownership of one session's controller operation.
///
/// Dropping the guard releases the session immediately. It also allows the
/// registry entry to be reclaimed once no holder or waiter remains.
#[must_use = "dropping the guard releases the session lock"]
pub struct SessionLockGuard {
    // Field order is intentional: unlock before releasing the entry that
    // removes itself from the registry.
    _gate: OwnedMutexGuard<()>,
    _entry: Arc<SessionLockEntry>,
}

impl SessionLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait for exclusive access to `session_id`.
    ///
    /// Cancellation is safe: the waiting future owns no mutex guard, and its
    /// per-session entry is reclaimed when it was the last user.
    pub async fn lock(&self, session_id: SessionId) -> SessionLockGuard {
        let entry = {
            let mut entries = lock_entries(&self.inner.entries);
            match entries.get(&session_id).and_then(Weak::upgrade) {
                Some(entry) => entry,
                None => {
                    let entry = Arc::new(SessionLockEntry {
                        session_id,
                        gate: Arc::new(AsyncMutex::new(())),
                        registry: Arc::downgrade(&self.inner),
                    });
                    entries.insert(session_id, Arc::downgrade(&entry));
                    entry
                }
            }
        };

        let gate = Arc::clone(&entry.gate).lock_owned().await;
        SessionLockGuard {
            _gate: gate,
            _entry: entry,
        }
    }

    #[cfg(test)]
    fn tracked_session_count(&self) -> usize {
        let mut entries = lock_entries(&self.inner.entries);
        entries.retain(|_, entry| entry.strong_count() > 0);
        entries.len()
    }
}

impl Drop for SessionLockEntry {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut entries = lock_entries(&registry.entries);
        let is_current_entry = entries
            .get(&self.session_id)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self));
        if is_current_entry {
            entries.remove(&self.session_id);
        }
    }
}

fn lock_entries<T>(mutex: &StdMutex<T>) -> StdMutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::SessionLocks;
    use crate::identity::SessionId;
    use std::time::Duration;

    fn session(name: &str) -> SessionId {
        SessionId::derive("test-scope", name)
    }

    #[tokio::test]
    async fn same_session_is_serialized() {
        let locks = SessionLocks::new();
        let session_id = session("same");
        let first = locks.lock(session_id).await;
        let mut second = Box::pin(locks.lock(session_id));

        assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err());

        drop(first);
        let _second = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("second operation should acquire after the first releases");
    }

    #[tokio::test]
    async fn different_sessions_can_run_concurrently() {
        let locks = SessionLocks::new();
        let first = locks.lock(session("first")).await;

        let second = tokio::time::timeout(Duration::from_secs(1), locks.lock(session("second")))
            .await
            .expect("a different session must not wait for the first");

        drop((first, second));
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_leak_or_block_the_session() {
        let locks = SessionLocks::new();
        let session_id = session("cancelled");
        let first = locks.lock(session_id).await;
        let mut waiting = Box::pin(locks.lock(session_id));

        tokio::select! {
            biased;
            _ = &mut waiting => panic!("same-session waiter acquired too early"),
            _ = tokio::task::yield_now() => {}
        }
        drop(waiting);
        assert_eq!(locks.tracked_session_count(), 1);

        drop(first);
        assert_eq!(locks.tracked_session_count(), 0);

        let _next = tokio::time::timeout(Duration::from_secs(1), locks.lock(session_id))
            .await
            .expect("the cancelled waiter must not leave the session locked");
    }

    #[tokio::test]
    async fn idle_entries_are_reclaimed() {
        let locks = SessionLocks::new();

        for index in 0..128 {
            let guard = locks.lock(session(&format!("session-{index}"))).await;
            drop(guard);
        }

        assert_eq!(locks.tracked_session_count(), 0);
    }
}
