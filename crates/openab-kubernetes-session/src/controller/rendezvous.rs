use super::OrphanAuthority;
use crate::identity::{ScopeId, SessionId};
use crate::resources::SESSION_WORKSPACE_V1;
use crate::state::ProfileRef;
use crate::wire::{
    AcpMessageV1, ActivatedSessionV1, ActivationRequestV1, ControllerToBridgeV1,
    ControllerToWorkerV1, ProtocolResultV1, WireMessage, WireProtocolError,
    MAX_CONTROL_FRAME_BYTES,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

mod lifecycle;

use lifecycle::LifecycleSessionState;
pub use lifecycle::RendezvousLifecycleError;
pub(crate) use lifecycle::{
    LifecycleAcquireOutcome, LifecycleAdmission, LifecycleDeliveryOutcome, RelayLifecycleTerminal,
};

const HANDSHAKE_BYTE_RESERVE: usize = 2 * MAX_CONTROL_FRAME_BYTES;
pub const MIN_RELAY_BYTE_BUDGET: usize = HANDSHAKE_BYTE_RESERVE;

/// One authenticated side of a session relay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayLane {
    Bridge,
    Worker,
}

impl RelayLane {
    fn peer(self) -> Self {
        match self {
            Self::Bridge => Self::Worker,
            Self::Worker => Self::Bridge,
        }
    }
}

/// Process-local identity for one installed relay connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RelayConnectionId(Uuid);

impl RelayConnectionId {
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Opaque lease returned only after a relay lane is installed.
///
/// Close callbacks must retain and present this exact value. A session binding
/// alone cannot distinguish two sockets for the same worker generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayConnection {
    session_id: SessionId,
    lane: RelayLane,
    connection_id: RelayConnectionId,
}

impl RelayConnection {
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn lane(&self) -> RelayLane {
        self.lane
    }

    pub fn connection_id(&self) -> RelayConnectionId {
        self.connection_id
    }
}

/// Controller-owned metadata retained until an exact worker is registered.
///
/// Construction validates the eventual `Activated` response before any
/// process-local slot is installed. The response remains withheld until both
/// relay lanes have matching durable authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingActivation {
    response: ControllerToBridgeV1,
    profile: ProfileRef,
}

impl PendingActivation {
    pub fn new(
        request: &ActivationRequestV1,
        profile: ProfileRef,
        authority: &OrphanAuthority,
    ) -> Result<Self, WireProtocolError> {
        let activated = ActivatedSessionV1::new(
            request,
            profile.clone(),
            authority.binding(),
            SESSION_WORKSPACE_V1,
        )?;
        Ok(Self {
            response: ControllerToBridgeV1::Activation(
                crate::wire::ActivationResponseV1::activated(activated),
            ),
            profile,
        })
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }
}

/// Result of attempting the two-lane handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayPairingOutcome {
    /// Exactly one lane or its activation metadata is still absent.
    AwaitingPeer,
    /// Both handshake frames were enqueued and ACP routing is now enabled.
    Active,
    /// Both lanes remain installed while the process byte budget is occupied.
    Backpressured,
    /// A handshake receiver was closed; the session is now fail-closed.
    ContainmentRequired(Box<RelayContainmentTicket>),
    /// The session disappeared or is already quiescing.
    Unavailable,
}

/// Result of one atomic active-lane ACP enqueue.
#[derive(Debug, PartialEq, Eq)]
pub enum AcpRouteOutcome {
    Delivered,
    Backpressured {
        message: AcpMessageV1,
        reason: RelayBackpressure,
    },
    ContainmentRequired(Box<RelayContainmentTicket>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayBackpressure {
    LaneItems,
    ProcessBytes,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RelayByteBudgetError {
    #[error(
        "relay byte budget is {configured} bytes; at least {minimum} bytes are required for one atomic handshake"
    )]
    BelowAtomicHandshake { configured: usize, minimum: usize },
}

/// Process-wide queue byte budget shared by every relay orchestrator in one
/// executable. Clone this value when several controller scopes share a
/// process; constructing one budget per scope would weaken the global bound.
#[derive(Clone, Debug)]
pub struct RelayByteBudget {
    inner: Arc<RelayByteBudgetInner>,
}

impl RelayByteBudget {
    pub fn new(bytes: NonZeroUsize) -> Result<Self, RelayByteBudgetError> {
        if bytes.get() < MIN_RELAY_BYTE_BUDGET {
            return Err(RelayByteBudgetError::BelowAtomicHandshake {
                configured: bytes.get(),
                minimum: MIN_RELAY_BYTE_BUDGET,
            });
        }
        let (release_generation, _) = watch::channel(0_u64);
        Ok(Self {
            inner: Arc::new(RelayByteBudgetInner {
                limit: bytes.get(),
                used: AtomicUsize::new(0),
                control_waiters: AtomicUsize::new(0),
                release_generation,
            }),
        })
    }

    pub fn available_bytes(&self) -> usize {
        self.inner
            .limit
            .saturating_sub(self.inner.used.load(Ordering::Acquire))
    }

    fn try_acquire(&self, bytes: usize) -> Option<RelayByteLease> {
        self.try_acquire_up_to(bytes, self.inner.limit)
    }

    fn try_acquire_acp(&self, bytes: usize) -> Option<RelayByteLease> {
        if self.inner.control_waiters.load(Ordering::Acquire) != 0 {
            return None;
        }
        let lease = self.try_acquire(bytes)?;
        if self.inner.control_waiters.load(Ordering::Acquire) != 0 {
            drop(lease);
            return None;
        }
        Some(lease)
    }

    fn try_acquire_up_to(&self, bytes: usize, admission_limit: usize) -> Option<RelayByteLease> {
        let mut used = self.inner.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes)?;
            if next > admission_limit {
                return None;
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(RelayByteLease {
                        inner: Arc::clone(&self.inner),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn subscribe_releases(&self) -> watch::Receiver<u64> {
        self.inner.release_generation.subscribe()
    }

    fn begin_control_wait(&self) -> RelayControlWaiter {
        self.inner.control_waiters.fetch_add(1, Ordering::AcqRel);
        RelayControlWaiter {
            inner: Arc::clone(&self.inner),
        }
    }

    #[cfg(test)]
    pub(super) fn hold_for_test(&self, bytes: usize) -> RelayByteLease {
        self.try_acquire(bytes)
            .expect("the test byte hold must fit the configured budget")
    }

    #[cfg(test)]
    pub(super) fn control_waiters_for_test(&self) -> usize {
        self.inner.control_waiters.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct RelayByteBudgetInner {
    limit: usize,
    used: AtomicUsize,
    control_waiters: AtomicUsize,
    release_generation: watch::Sender<u64>,
}

#[derive(Debug)]
pub(crate) struct RelayControlWaiter {
    inner: Arc<RelayByteBudgetInner>,
}

impl Drop for RelayControlWaiter {
    fn drop(&mut self) {
        let previous = self.inner.control_waiters.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

#[derive(Debug)]
pub(super) struct RelayByteLease {
    inner: Arc<RelayByteBudgetInner>,
    bytes: usize,
}

impl RelayByteLease {
    fn split_off(&mut self, bytes: usize) -> Self {
        assert!(bytes <= self.bytes, "a byte lease cannot be oversplit");
        self.bytes -= bytes;
        Self {
            inner: Arc::clone(&self.inner),
            bytes,
        }
    }
}

impl Drop for RelayByteLease {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let previous = self.inner.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
        self.inner
            .release_generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

/// One bounded outbound item.
///
/// Every item retains its process-wide byte-budget permit until the network
/// writer drops this value after the write completes.
#[must_use = "the relay byte lease is released when this queued item is dropped"]
pub struct RelayOutboundItem<M> {
    message: M,
    _byte_budget: RelayByteLease,
    write_reporter: Option<RelayWriteReporter>,
}

impl<M> RelayOutboundItem<M> {
    fn budgeted(message: M, permit: RelayByteLease) -> Self {
        Self {
            message,
            _byte_budget: permit,
            write_reporter: None,
        }
    }

    fn reported(message: M, permit: RelayByteLease, write_reporter: RelayWriteReporter) -> Self {
        Self {
            message,
            _byte_budget: permit,
            write_reporter: Some(write_reporter),
        }
    }

    fn into_message(self) -> M {
        self.message
    }
}

impl<M: WireMessage> RelayOutboundItem<M> {
    /// Consume a queued message and transfer its byte lease to the exact bytes
    /// that a transport writer must retain through write completion.
    pub fn into_encoded_frame(self) -> Result<RelayOutboundFrame, WireProtocolError> {
        let Self {
            message,
            _byte_budget,
            write_reporter,
        } = self;
        let bytes = crate::wire::encode_frame(&message)?;
        Ok(RelayOutboundFrame {
            bytes,
            _byte_budget,
            write_reporter,
        })
    }
}

impl<M> std::fmt::Debug for RelayOutboundItem<M> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayOutboundItem")
            .field("charged_bytes", &self._byte_budget.bytes)
            .finish_non_exhaustive()
    }
}

/// Encoded outbound bytes that retain their process-wide queue lease.
///
/// A transport writer either borrows [`Self::as_bytes`] for the actual write,
/// or consumes this value with [`Self::into_write_parts`] when its transport
/// requires ownership of the payload.
#[must_use = "hold the encoded frame until the transport write completes"]
pub struct RelayOutboundFrame {
    bytes: Vec<u8>,
    _byte_budget: RelayByteLease,
    write_reporter: Option<RelayWriteReporter>,
}

impl RelayOutboundFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Transfer the encoded payload without copying it.
    ///
    /// The returned guard retains both the process-wide byte lease and any
    /// lifecycle delivery reporter. The writer must keep it alive until the
    /// write future completes, then call [`RelayOutboundWriteGuard::mark_written`]
    /// only after a successful write. Dropping the guard keeps lifecycle
    /// delivery fail closed.
    pub fn into_write_parts(self) -> (Vec<u8>, RelayOutboundWriteGuard) {
        let Self {
            bytes,
            _byte_budget,
            write_reporter,
        } = self;
        (
            bytes,
            RelayOutboundWriteGuard {
                _byte_budget,
                write_reporter,
            },
        )
    }

    /// Report that the exact encoded frame reached the transport successfully.
    ///
    /// Lifecycle completion remains fail closed unless the writer consumes the
    /// frame through this method after its write future returns success.
    pub fn mark_written(self) {
        let (_, write_guard) = self.into_write_parts();
        write_guard.mark_written();
    }
}

impl std::fmt::Debug for RelayOutboundFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayOutboundFrame")
            .field("bytes", &self.bytes.len())
            .field("charged_bytes", &self._byte_budget.bytes)
            .finish_non_exhaustive()
    }
}

/// Completion guard for one owned outbound transport payload.
///
/// This value is deliberately not cloneable: there is exactly one byte-budget
/// lease and at most one lifecycle completion report for each encoded frame.
#[must_use = "hold the write guard until the transport write completes"]
pub struct RelayOutboundWriteGuard {
    _byte_budget: RelayByteLease,
    write_reporter: Option<RelayWriteReporter>,
}

impl RelayOutboundWriteGuard {
    /// Report that the exact payload reached the transport successfully.
    pub fn mark_written(mut self) {
        if let Some(reporter) = self.write_reporter.as_mut() {
            reporter.report_written();
        }
    }
}

impl std::fmt::Debug for RelayOutboundWriteGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayOutboundWriteGuard")
            .field("charged_bytes", &self._byte_budget.bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RelayWriteStatus {
    Written,
    Dropped,
}

pub(super) struct RelayWriteReporter {
    result: Option<oneshot::Sender<()>>,
}

impl RelayWriteReporter {
    fn report_written(&mut self) {
        if let Some(result) = self.result.take() {
            let _ = result.send(());
        }
    }
}

impl std::fmt::Debug for RelayWriteReporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayWriteReporter")
            .finish_non_exhaustive()
    }
}

pub(super) fn relay_write_report() -> (RelayWriteReporter, oneshot::Receiver<()>) {
    let (result, receiver) = oneshot::channel();
    (
        RelayWriteReporter {
            result: Some(result),
        },
        receiver,
    )
}

/// One installed lane plus the session-wide quiesce notification.
pub struct RelayInstallation {
    connection: RelayConnection,
    quiesced: watch::Receiver<bool>,
    pairing: RelayPairingOutcome,
}

impl RelayInstallation {
    pub fn connection(&self) -> &RelayConnection {
        &self.connection
    }

    pub fn into_connection(self) -> RelayConnection {
        self.connection
    }

    pub fn quiesced(&self) -> watch::Receiver<bool> {
        self.quiesced.clone()
    }

    pub fn pairing(&self) -> &RelayPairingOutcome {
        &self.pairing
    }
}

/// A retryable token created by the atomic routing-to-quiescing transition.
///
/// The transport calls the controller with [`Self::authority`]. It calls
/// [`RendezvousRegistry::complete_containment`] only after the controller has
/// returned a non-error outcome. On persistence failure it retains this token
/// and the registry remains fail-closed in `Quiescing`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayContainmentTicket {
    authority: OrphanAuthority,
    trigger: RelayConnection,
    bridge: Option<RelayConnectionId>,
    worker: Option<RelayConnectionId>,
    origin: RelayContainmentOrigin,
}

impl RelayContainmentTicket {
    pub fn authority(&self) -> &OrphanAuthority {
        &self.authority
    }

    pub fn trigger(&self) -> &RelayConnection {
        &self.trigger
    }

    pub fn connection_id(&self, lane: RelayLane) -> Option<RelayConnectionId> {
        match lane {
            RelayLane::Bridge => self.bridge,
            RelayLane::Worker => self.worker,
        }
    }

    pub(crate) fn is_detached(&self) -> bool {
        self.origin == RelayContainmentOrigin::Detached
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelayContainmentOrigin {
    Installed,
    Detached,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RendezvousInstallError {
    #[error("the relay connection does not belong to this controller scope")]
    ScopeMismatch,
    #[error("another worker generation already owns this session rendezvous")]
    AuthorityConflict,
    #[error("the registered worker profile does not match the pending activation")]
    ProfileConflict,
    #[error("this relay lane already has an active connection")]
    LaneOccupied,
    #[error("this session rendezvous is quiescing")]
    Quiescing,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RendezvousRouteError {
    #[error("the relay connection is no longer current")]
    StaleConnection,
    #[error("the exact peer handshake has not completed")]
    AwaitingPeer,
    #[error("this session has a lifecycle operation in progress")]
    LifecyclePending,
    #[error("this session rendezvous is quiescing")]
    Quiescing,
    #[error("the ACP frame cannot fit within the configured relay byte budget")]
    FrameExceedsByteBudget { bytes: usize, capacity: usize },
}

/// Result of atomically handling one socket-close callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayConnectionLoss {
    /// The exact connection changed the session to `Quiescing`; persist the
    /// included authority before removing the rendezvous.
    ContainmentRequired(Box<RelayContainmentTicket>),
    /// A lifecycle operation already fenced ACP, so this exact worker may
    /// detach without evicting the bridge or creating orphan intent.
    LifecycleWorkerDetached,
    /// Another exact lane already started containment for this session.
    AlreadyQuiescing,
    /// The callback belongs to an absent or replaced connection.
    StaleConnection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayContainmentCompletion {
    Removed,
    StaleTicket,
}

#[derive(Clone, Debug)]
struct BridgeSlot {
    connection_id: RelayConnectionId,
    outbound: mpsc::Sender<RelayOutboundItem<ControllerToBridgeV1>>,
}

#[derive(Clone, Debug)]
struct WorkerSlot {
    connection_id: RelayConnectionId,
    outbound: mpsc::Sender<RelayOutboundItem<ControllerToWorkerV1>>,
    profile: ProfileRef,
}

#[derive(Debug)]
enum RelaySessionState {
    Pairing {
        bridge: Option<BridgeSlot>,
        worker: Option<WorkerSlot>,
        activation: Option<Box<PendingActivation>>,
    },
    Active {
        bridge: BridgeSlot,
        worker: WorkerSlot,
    },
    Lifecycle(LifecycleSessionState),
    Quiescing {
        bridge: Option<RelayConnectionId>,
        worker: Option<RelayConnectionId>,
        trigger: RelayConnection,
    },
}

impl RelaySessionState {
    fn connection(&self, lane: RelayLane) -> Option<RelayConnectionId> {
        match (self, lane) {
            (Self::Pairing { bridge, .. }, RelayLane::Bridge) => {
                bridge.as_ref().map(|slot| slot.connection_id)
            }
            (Self::Pairing { worker, .. }, RelayLane::Worker) => {
                worker.as_ref().map(|slot| slot.connection_id)
            }
            (Self::Active { bridge, .. }, RelayLane::Bridge) => Some(bridge.connection_id),
            (Self::Active { worker, .. }, RelayLane::Worker) => Some(worker.connection_id),
            (Self::Lifecycle(lifecycle), RelayLane::Bridge) => Some(lifecycle.bridge.connection_id),
            (Self::Lifecycle(lifecycle), RelayLane::Worker) => {
                lifecycle.worker.as_ref().map(|worker| worker.connection_id)
            }
            (Self::Quiescing { bridge, .. }, RelayLane::Bridge) => *bridge,
            (Self::Quiescing { worker, .. }, RelayLane::Worker) => *worker,
        }
    }

    fn contains(&self, connection: &RelayConnection) -> bool {
        self.connection(connection.lane) == Some(connection.connection_id)
    }
}

#[derive(Debug)]
struct RelaySession {
    authority: OrphanAuthority,
    state: RelaySessionState,
    quiesced: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct RendezvousState {
    sessions: HashMap<SessionId, RelaySession>,
    detached_containments: HashMap<RelayConnectionId, RelayContainmentTicket>,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RendezvousFatalError {
    #[error("the rendezvous state mutex was poisoned")]
    StatePoisoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RendezvousHealth {
    Healthy,
    Fatal(RendezvousFatalError),
}

struct RendezvousGuard<'a> {
    state: MutexGuard<'a, RendezvousState>,
    health: watch::Sender<RendezvousHealth>,
}

impl Deref for RendezvousGuard<'_> {
    type Target = RendezvousState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for RendezvousGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl Drop for RendezvousGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.health
                .send_replace(RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned));
        }
    }
}

/// In-memory rendezvous, handshake pairing, and close fencing for one process.
///
/// This registry is deliberately not durable state. Kubernetes lifecycle
/// anchors remain the source of truth, while controller restart containment
/// handles every lane forgotten with this process. The single short-lived
/// mutex makes lane installation, handshake enqueue, routing admission, and
/// the transition to `Quiescing` one ordered decision. No external I/O or
/// async wait occurs while it is held.
#[derive(Clone)]
pub struct RendezvousRegistry {
    scope_id: ScopeId,
    state: Arc<Mutex<RendezvousState>>,
    health: watch::Sender<RendezvousHealth>,
    byte_budget: RelayByteBudget,
}

impl RendezvousRegistry {
    pub fn with_byte_budget(scope_id: ScopeId, byte_budget: RelayByteBudget) -> Self {
        let (health, _) = watch::channel(RendezvousHealth::Healthy);
        Self {
            scope_id,
            state: Arc::new(Mutex::new(RendezvousState::default())),
            health,
            byte_budget,
        }
    }

    pub(crate) fn byte_budget_releases(&self) -> watch::Receiver<u64> {
        self.byte_budget.subscribe_releases()
    }

    pub(crate) fn begin_control_wait(&self) -> RelayControlWaiter {
        self.byte_budget.begin_control_wait()
    }

    /// Subscribe to process-fatal registry health.
    ///
    /// A controller executable must remove readiness and terminate when this
    /// signal becomes [`RendezvousHealth::Fatal`]. Continuing after mutex
    /// poisoning could preserve a partially-mutated routing decision, so the
    /// guard latches fatal health while the original task is unwinding.
    pub fn health(&self) -> watch::Receiver<RendezvousHealth> {
        self.health.subscribe()
    }

    /// Install an authenticated broker lane and retain its correlated
    /// activation response until an exact worker lane is ready.
    pub fn install_bridge(
        &self,
        authority: OrphanAuthority,
        activation: PendingActivation,
        outbound: mpsc::Sender<RelayOutboundItem<ControllerToBridgeV1>>,
    ) -> Result<RelayInstallation, RendezvousInstallError> {
        self.validate_scope(&authority)?;
        let session_id = authority.binding().session_id();
        let mut state = self.lock_state();
        if state
            .detached_containments
            .values()
            .any(|ticket| ticket.authority == authority)
        {
            return Err(RendezvousInstallError::Quiescing);
        }
        let session = state.sessions.entry(session_id).or_insert_with(|| {
            let (quiesced, _) = watch::channel(false);
            RelaySession {
                authority: authority.clone(),
                state: RelaySessionState::Pairing {
                    bridge: None,
                    worker: None,
                    activation: None,
                },
                quiesced,
            }
        });
        validate_existing_session(session, &authority, RelayLane::Bridge)?;
        let connection_id = fresh_connection_id(&session.state);
        if let RelaySessionState::Pairing {
            bridge,
            worker,
            activation: pending,
        } = &mut session.state
        {
            if worker
                .as_ref()
                .is_some_and(|slot| slot.profile != *activation.profile())
            {
                return Err(RendezvousInstallError::ProfileConflict);
            }
            *bridge = Some(BridgeSlot {
                connection_id,
                outbound,
            });
            *pending = Some(Box::new(activation));
            let connection = RelayConnection {
                session_id,
                lane: RelayLane::Bridge,
                connection_id,
            };
            let pairing = pair_if_ready(session, session_id, &self.byte_budget);
            return Ok(RelayInstallation {
                connection,
                quiesced: session.quiesced.subscribe(),
                pairing,
            });
        }
        unreachable!("validated rendezvous must still be in Pairing")
    }

    /// Install a bootstrap-authenticated worker lane. Worker-first is allowed
    /// because a Pod can register while activation is returning its durable
    /// observation; ACP remains disabled until the bridge metadata arrives.
    pub fn install_worker(
        &self,
        authority: OrphanAuthority,
        profile: ProfileRef,
        outbound: mpsc::Sender<RelayOutboundItem<ControllerToWorkerV1>>,
    ) -> Result<RelayInstallation, RendezvousInstallError> {
        self.validate_scope(&authority)?;
        let session_id = authority.binding().session_id();
        let mut state = self.lock_state();
        if state
            .detached_containments
            .values()
            .any(|ticket| ticket.authority == authority)
        {
            return Err(RendezvousInstallError::Quiescing);
        }
        let session = state.sessions.entry(session_id).or_insert_with(|| {
            let (quiesced, _) = watch::channel(false);
            RelaySession {
                authority: authority.clone(),
                state: RelaySessionState::Pairing {
                    bridge: None,
                    worker: None,
                    activation: None,
                },
                quiesced,
            }
        });
        validate_existing_session(session, &authority, RelayLane::Worker)?;
        let connection_id = fresh_connection_id(&session.state);
        if let RelaySessionState::Pairing {
            bridge: _,
            worker,
            activation,
        } = &mut session.state
        {
            if activation
                .as_ref()
                .is_some_and(|pending| pending.profile != profile)
            {
                return Err(RendezvousInstallError::ProfileConflict);
            }
            *worker = Some(WorkerSlot {
                connection_id,
                outbound,
                profile,
            });
            let connection = RelayConnection {
                session_id,
                lane: RelayLane::Worker,
                connection_id,
            };
            let pairing = pair_if_ready(session, session_id, &self.byte_budget);
            return Ok(RelayInstallation {
                connection,
                quiesced: session.quiesced.subscribe(),
                pairing,
            });
        }
        unreachable!("validated rendezvous must still be in Pairing")
    }

    /// Retry an otherwise complete handshake after bounded queue pressure.
    pub fn retry_pairing(&self, session_id: SessionId) -> RelayPairingOutcome {
        let mut state = self.lock_state();
        state
            .sessions
            .get_mut(&session_id)
            .map_or(RelayPairingOutcome::Unavailable, |session| {
                pair_if_ready(session, session_id, &self.byte_budget)
            })
    }

    /// Resolve the exact peer only after both handshake frames were enqueued.
    ///
    /// This is an inspection primitive. A transport must not use the returned
    /// ID across an await; delivery must use [`Self::route_acp`] instead.
    pub fn route_target(
        &self,
        connection: &RelayConnection,
    ) -> Result<RelayConnectionId, RendezvousRouteError> {
        let state = self.lock_state();
        let Some(session) = state.sessions.get(&connection.session_id) else {
            return Err(RendezvousRouteError::StaleConnection);
        };
        if !session.state.contains(connection) {
            return Err(RendezvousRouteError::StaleConnection);
        }
        match &session.state {
            RelaySessionState::Active { .. } => session
                .state
                .connection(connection.lane.peer())
                .ok_or(RendezvousRouteError::AwaitingPeer),
            RelaySessionState::Pairing { .. } => Err(RendezvousRouteError::AwaitingPeer),
            RelaySessionState::Lifecycle(_) => Err(RendezvousRouteError::LifecyclePending),
            RelaySessionState::Quiescing { .. } => Err(RendezvousRouteError::Quiescing),
        }
    }

    /// Atomically validate one exact source lane and enqueue ACP to its peer.
    ///
    /// Source validation, active-state admission, byte-budget reservation,
    /// bounded `try_send`, and a closed-peer transition to `Quiescing` all
    /// occur while the same registry mutex is held. `Full` or exhausted global
    /// byte budget returns the original message without adding another buffer.
    pub fn route_acp(
        &self,
        connection: &RelayConnection,
        message: AcpMessageV1,
    ) -> Result<AcpRouteOutcome, RendezvousRouteError> {
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&connection.session_id) else {
            return Err(RendezvousRouteError::StaleConnection);
        };
        if !session.state.contains(connection) {
            return Err(RendezvousRouteError::StaleConnection);
        }
        match &session.state {
            RelaySessionState::Pairing { .. } => Err(RendezvousRouteError::AwaitingPeer),
            RelaySessionState::Lifecycle(_) => Err(RendezvousRouteError::LifecyclePending),
            RelaySessionState::Quiescing { .. } => Err(RendezvousRouteError::Quiescing),
            RelaySessionState::Active { bridge, worker } => {
                let queued_bytes = message
                    .encoded_payload_bytes()
                    .checked_add(MAX_CONTROL_FRAME_BYTES)
                    .expect("a validated ACP payload plus frame overhead fits usize");
                if queued_bytes > self.byte_budget.inner.limit {
                    return Err(RendezvousRouteError::FrameExceedsByteBudget {
                        bytes: queued_bytes,
                        capacity: self.byte_budget.inner.limit,
                    });
                }
                let permit = match self.byte_budget.try_acquire_acp(queued_bytes) {
                    Some(permit) => permit,
                    None => {
                        return Ok(AcpRouteOutcome::Backpressured {
                            message,
                            reason: RelayBackpressure::ProcessBytes,
                        });
                    }
                };
                match connection.lane {
                    RelayLane::Bridge => {
                        let peer_id = worker.connection_id;
                        let queued =
                            RelayOutboundItem::budgeted(ControllerToWorkerV1::Acp(message), permit);
                        match worker.outbound.try_send(queued) {
                            Ok(()) => Ok(AcpRouteOutcome::Delivered),
                            Err(mpsc::error::TrySendError::Full(queued)) => {
                                let ControllerToWorkerV1::Acp(message) = queued.into_message()
                                else {
                                    unreachable!("the ACP route created an ACP worker item")
                                };
                                Ok(AcpRouteOutcome::Backpressured {
                                    message,
                                    reason: RelayBackpressure::LaneItems,
                                })
                            }
                            Err(mpsc::error::TrySendError::Closed(queued)) => {
                                drop(queued);
                                let trigger = RelayConnection {
                                    session_id: connection.session_id,
                                    lane: RelayLane::Worker,
                                    connection_id: peer_id,
                                };
                                Ok(AcpRouteOutcome::ContainmentRequired(Box::new(quiesce(
                                    session, trigger,
                                ))))
                            }
                        }
                    }
                    RelayLane::Worker => {
                        let peer_id = bridge.connection_id;
                        let queued =
                            RelayOutboundItem::budgeted(ControllerToBridgeV1::Acp(message), permit);
                        match bridge.outbound.try_send(queued) {
                            Ok(()) => Ok(AcpRouteOutcome::Delivered),
                            Err(mpsc::error::TrySendError::Full(queued)) => {
                                let ControllerToBridgeV1::Acp(message) = queued.into_message()
                                else {
                                    unreachable!("the ACP route created an ACP bridge item")
                                };
                                Ok(AcpRouteOutcome::Backpressured {
                                    message,
                                    reason: RelayBackpressure::LaneItems,
                                })
                            }
                            Err(mpsc::error::TrySendError::Closed(queued)) => {
                                drop(queued);
                                let trigger = RelayConnection {
                                    session_id: connection.session_id,
                                    lane: RelayLane::Bridge,
                                    connection_id: peer_id,
                                };
                                Ok(AcpRouteOutcome::ContainmentRequired(Box::new(quiesce(
                                    session, trigger,
                                ))))
                            }
                        }
                    }
                }
            }
        }
    }

    /// Atomically stop routing an exact connection before durable containment.
    ///
    /// Repeating the triggering callback returns the same ticket. A callback
    /// for the peer observes `AlreadyQuiescing`; a delayed callback from a
    /// replaced or removed socket is a benign stale observation.
    pub fn begin_connection_loss(&self, connection: &RelayConnection) -> RelayConnectionLoss {
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&connection.session_id) else {
            return RelayConnectionLoss::StaleConnection;
        };
        if !session.state.contains(connection) {
            return RelayConnectionLoss::StaleConnection;
        }

        if let RelaySessionState::Lifecycle(lifecycle) = &mut session.state {
            if connection.lane == RelayLane::Worker {
                if lifecycle.worker_detach_is_expected_or_provisional() {
                    lifecycle.worker = None;
                    return RelayConnectionLoss::LifecycleWorkerDetached;
                }
                let ticket = quiesce(session, connection.clone());
                return RelayConnectionLoss::ContainmentRequired(Box::new(ticket));
            }
            let ticket = quiesce(session, connection.clone());
            return RelayConnectionLoss::ContainmentRequired(Box::new(ticket));
        }

        match &session.state {
            RelaySessionState::Pairing { .. } | RelaySessionState::Active { .. } => {
                let ticket = quiesce(session, connection.clone());
                RelayConnectionLoss::ContainmentRequired(Box::new(ticket))
            }
            RelaySessionState::Quiescing { trigger, .. } if trigger == connection => {
                RelayConnectionLoss::ContainmentRequired(Box::new(containment_ticket(
                    session, trigger,
                )))
            }
            RelaySessionState::Quiescing { .. } => RelayConnectionLoss::AlreadyQuiescing,
            RelaySessionState::Lifecycle(_) => {
                unreachable!("lifecycle connection loss returned before this match")
            }
        }
    }

    /// Retain retryable containment when trusted post-mutation setup fails
    /// before a socket attachment can be handed to its caller.
    ///
    /// An absent entry receives a synthetic trigger but no installed lane IDs.
    /// An exact existing entry is quiesced through one of its real lanes. A
    /// different authority receives a detached retry ticket without replacing
    /// or adopting the installed generation.
    pub fn begin_unattached_containment(&self, authority: OrphanAuthority) -> RelayConnectionLoss {
        if authority.binding().scope_id() != self.scope_id {
            return RelayConnectionLoss::StaleConnection;
        }
        let session_id = authority.binding().session_id();
        let mut state = self.lock_state();
        if let Some(ticket) = state
            .detached_containments
            .values()
            .find(|ticket| ticket.authority == authority)
            .cloned()
        {
            return RelayConnectionLoss::ContainmentRequired(Box::new(ticket));
        }

        let authority_matches = state
            .sessions
            .get(&session_id)
            .map(|session| session.authority == authority);
        if authority_matches.is_none() {
            let ticket =
                synthetic_containment_ticket(authority.clone(), RelayContainmentOrigin::Installed);
            let (quiesced, _) = watch::channel(true);
            state.sessions.insert(
                session_id,
                RelaySession {
                    authority,
                    state: RelaySessionState::Quiescing {
                        bridge: None,
                        worker: None,
                        trigger: ticket.trigger.clone(),
                    },
                    quiesced,
                },
            );
            return RelayConnectionLoss::ContainmentRequired(Box::new(ticket));
        }

        if authority_matches == Some(false) {
            let ticket = synthetic_containment_ticket(authority, RelayContainmentOrigin::Detached);
            state
                .detached_containments
                .insert(ticket.trigger.connection_id, ticket.clone());
            return RelayConnectionLoss::ContainmentRequired(Box::new(ticket));
        }

        let session = state
            .sessions
            .get_mut(&session_id)
            .expect("the exact session authority was observed while locked");
        match &session.state {
            RelaySessionState::Pairing { .. }
            | RelaySessionState::Active { .. }
            | RelaySessionState::Lifecycle(_) => {
                let trigger = session
                    .state
                    .connection(RelayLane::Bridge)
                    .map(|connection_id| RelayConnection {
                        session_id,
                        lane: RelayLane::Bridge,
                        connection_id,
                    })
                    .or_else(|| {
                        session
                            .state
                            .connection(RelayLane::Worker)
                            .map(|connection_id| RelayConnection {
                                session_id,
                                lane: RelayLane::Worker,
                                connection_id,
                            })
                    })
                    .expect("a pairing or active rendezvous has an installed lane");
                RelayConnectionLoss::ContainmentRequired(Box::new(quiesce(session, trigger)))
            }
            RelaySessionState::Quiescing { .. } => RelayConnectionLoss::AlreadyQuiescing,
        }
    }

    pub(crate) fn begin_session_containment(&self, session_id: SessionId) -> RelayConnectionLoss {
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return RelayConnectionLoss::StaleConnection;
        };
        match &session.state {
            RelaySessionState::Pairing { .. }
            | RelaySessionState::Active { .. }
            | RelaySessionState::Lifecycle(_) => {
                let trigger = session
                    .state
                    .connection(RelayLane::Bridge)
                    .map(|connection_id| RelayConnection {
                        session_id,
                        lane: RelayLane::Bridge,
                        connection_id,
                    })
                    .or_else(|| {
                        session
                            .state
                            .connection(RelayLane::Worker)
                            .map(|connection_id| RelayConnection {
                                session_id,
                                lane: RelayLane::Worker,
                                connection_id,
                            })
                    })
                    .expect("a pairing or active rendezvous has an installed lane");
                RelayConnectionLoss::ContainmentRequired(Box::new(quiesce(session, trigger)))
            }
            RelaySessionState::Quiescing { .. } => RelayConnectionLoss::AlreadyQuiescing,
        }
    }

    /// Remove one quiescing rendezvous only after durable containment succeeds.
    /// A delayed completion token can never remove a replacement generation.
    pub fn complete_containment(
        &self,
        ticket: &RelayContainmentTicket,
    ) -> RelayContainmentCompletion {
        let session_id = ticket.trigger.session_id;
        let mut state = self.lock_state();
        match ticket.origin {
            RelayContainmentOrigin::Installed => {
                let removable = state.sessions.get(&session_id).is_some_and(|session| {
                    session.authority == ticket.authority
                        && matches!(
                            &session.state,
                            RelaySessionState::Quiescing { trigger, .. }
                                if trigger == &ticket.trigger
                        )
                });
                if removable {
                    state.sessions.remove(&session_id);
                    RelayContainmentCompletion::Removed
                } else {
                    RelayContainmentCompletion::StaleTicket
                }
            }
            RelayContainmentOrigin::Detached => {
                if state
                    .detached_containments
                    .get(&ticket.trigger.connection_id)
                    == Some(ticket)
                {
                    state
                        .detached_containments
                        .remove(&ticket.trigger.connection_id);
                    RelayContainmentCompletion::Removed
                } else {
                    RelayContainmentCompletion::StaleTicket
                }
            }
        }
    }

    /// Snapshot retryable containment work without performing external I/O.
    pub fn pending_containments(&self) -> Vec<RelayContainmentTicket> {
        let state = self.lock_state();
        let mut pending = state
            .sessions
            .values()
            .filter_map(|session| match &session.state {
                RelaySessionState::Quiescing { trigger, .. } => {
                    Some(containment_ticket(session, trigger))
                }
                RelaySessionState::Pairing { .. }
                | RelaySessionState::Active { .. }
                | RelaySessionState::Lifecycle(_) => None,
            })
            .chain(state.detached_containments.values().cloned())
            .collect::<Vec<_>>();
        pending.sort_by_key(|ticket| {
            (
                ticket.trigger.session_id.as_hex(),
                ticket.trigger.connection_id.as_uuid(),
            )
        });
        pending
    }

    fn validate_scope(&self, authority: &OrphanAuthority) -> Result<(), RendezvousInstallError> {
        if authority.binding().scope_id() != self.scope_id {
            Err(RendezvousInstallError::ScopeMismatch)
        } else {
            Ok(())
        }
    }

    fn lock_state(&self) -> RendezvousGuard<'_> {
        match self.state.lock() {
            Ok(state) => RendezvousGuard {
                state,
                health: self.health.clone(),
            },
            Err(_) => {
                self.health
                    .send_replace(RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned));
                panic!("rendezvous mutex poisoning requires controller shutdown");
            }
        }
    }
}

fn validate_existing_session(
    session: &RelaySession,
    authority: &OrphanAuthority,
    lane: RelayLane,
) -> Result<(), RendezvousInstallError> {
    if session.authority != *authority {
        return Err(RendezvousInstallError::AuthorityConflict);
    }
    if matches!(
        session.state,
        RelaySessionState::Lifecycle(_) | RelaySessionState::Quiescing { .. }
    ) {
        return Err(RendezvousInstallError::Quiescing);
    }
    if session.state.connection(lane).is_some() {
        return Err(RendezvousInstallError::LaneOccupied);
    }
    Ok(())
}

fn pair_if_ready(
    session: &mut RelaySession,
    session_id: SessionId,
    byte_budget: &RelayByteBudget,
) -> RelayPairingOutcome {
    let RelaySessionState::Pairing {
        bridge,
        worker,
        activation,
    } = &session.state
    else {
        return match session.state {
            RelaySessionState::Active { .. } => RelayPairingOutcome::Active,
            RelaySessionState::Lifecycle(_) => RelayPairingOutcome::Unavailable,
            RelaySessionState::Quiescing { .. } => RelayPairingOutcome::Unavailable,
            RelaySessionState::Pairing { .. } => unreachable!(),
        };
    };
    let (Some(bridge), Some(worker), Some(activation)) = (bridge, worker, activation) else {
        return RelayPairingOutcome::AwaitingPeer;
    };

    let bridge_sender = bridge.outbound.clone();
    let worker_sender = worker.outbound.clone();
    let bridge_id = bridge.connection_id;
    let worker_id = worker.connection_id;
    let activation_response = activation.response.clone();
    let worker_ack = ProtocolResultV1::ack(None)
        .expect("a handshake ACK without requestId is structurally valid");

    let Some(mut handshake_budget) = byte_budget.try_acquire(HANDSHAKE_BYTE_RESERVE) else {
        return RelayPairingOutcome::Backpressured;
    };

    let worker_permit = match worker_sender.try_reserve_owned() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let trigger = RelayConnection {
                session_id,
                lane: RelayLane::Worker,
                connection_id: worker_id,
            };
            return RelayPairingOutcome::ContainmentRequired(Box::new(quiesce(session, trigger)));
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            let trigger = RelayConnection {
                session_id,
                lane: RelayLane::Worker,
                connection_id: worker_id,
            };
            return RelayPairingOutcome::ContainmentRequired(Box::new(quiesce(session, trigger)));
        }
    };
    let bridge_permit = match bridge_sender.try_reserve_owned() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let trigger = RelayConnection {
                session_id,
                lane: RelayLane::Bridge,
                connection_id: bridge_id,
            };
            return RelayPairingOutcome::ContainmentRequired(Box::new(quiesce(session, trigger)));
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            let trigger = RelayConnection {
                session_id,
                lane: RelayLane::Bridge,
                connection_id: bridge_id,
            };
            return RelayPairingOutcome::ContainmentRequired(Box::new(quiesce(session, trigger)));
        }
    };

    let worker_budget = handshake_budget.split_off(MAX_CONTROL_FRAME_BYTES);
    let bridge_budget = handshake_budget;

    worker_permit.send(RelayOutboundItem::budgeted(
        ControllerToWorkerV1::ProtocolResult(worker_ack),
        worker_budget,
    ));
    bridge_permit.send(RelayOutboundItem::budgeted(
        activation_response,
        bridge_budget,
    ));

    let RelaySessionState::Pairing {
        bridge,
        worker,
        activation: _,
    } = std::mem::replace(
        &mut session.state,
        RelaySessionState::Pairing {
            bridge: None,
            worker: None,
            activation: None,
        },
    )
    else {
        unreachable!("pairing state was validated while the registry mutex was held")
    };
    session.state = RelaySessionState::Active {
        bridge: bridge.expect("validated bridge slot"),
        worker: worker.expect("validated worker slot"),
    };
    RelayPairingOutcome::Active
}

fn quiesce(session: &mut RelaySession, trigger: RelayConnection) -> RelayContainmentTicket {
    let bridge = session.state.connection(RelayLane::Bridge);
    let worker = session.state.connection(RelayLane::Worker);
    session.state = RelaySessionState::Quiescing {
        bridge,
        worker,
        trigger: trigger.clone(),
    };
    session.quiesced.send_replace(true);
    RelayContainmentTicket {
        authority: session.authority.clone(),
        trigger,
        bridge,
        worker,
        origin: RelayContainmentOrigin::Installed,
    }
}

fn containment_ticket(session: &RelaySession, trigger: &RelayConnection) -> RelayContainmentTicket {
    RelayContainmentTicket {
        authority: session.authority.clone(),
        trigger: trigger.clone(),
        bridge: session.state.connection(RelayLane::Bridge),
        worker: session.state.connection(RelayLane::Worker),
        origin: RelayContainmentOrigin::Installed,
    }
}

fn synthetic_containment_ticket(
    authority: OrphanAuthority,
    origin: RelayContainmentOrigin,
) -> RelayContainmentTicket {
    RelayContainmentTicket {
        trigger: RelayConnection {
            session_id: authority.binding().session_id(),
            lane: RelayLane::Bridge,
            connection_id: RelayConnectionId(Uuid::new_v4()),
        },
        authority,
        bridge: None,
        worker: None,
        origin,
    }
}

fn fresh_connection_id(state: &RelaySessionState) -> RelayConnectionId {
    loop {
        let candidate = RelayConnectionId(Uuid::new_v4());
        if state.connection(RelayLane::Bridge) != Some(candidate)
            && state.connection(RelayLane::Worker) != Some(candidate)
        {
            return candidate;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn mutex_poison_latches_process_fatal_health_during_unwind() {
        let budget =
            RelayByteBudget::new(NonZeroUsize::new(MIN_RELAY_BYTE_BUDGET).unwrap()).unwrap();
        let registry =
            RendezvousRegistry::with_byte_budget(ScopeId::derive("relay-health-test"), budget);
        let health = registry.health();
        assert_eq!(*health.borrow(), RendezvousHealth::Healthy);

        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _state = registry.lock_state();
            panic!("poison the rendezvous state for this test");
        }));
        assert!(poisoned.is_err());
        assert_eq!(
            *health.borrow(),
            RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned)
        );

        let follow_up = catch_unwind(AssertUnwindSafe(|| registry.pending_containments()));
        assert!(follow_up.is_err());
        assert_eq!(
            *health.borrow(),
            RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned)
        );
    }
}
