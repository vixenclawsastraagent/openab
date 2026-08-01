use super::OrphanAuthority;
use crate::identity::{ScopeId, SessionId};
use crate::resources::SESSION_WORKSPACE_V1;
use crate::state::ProfileRef;
use crate::wire::{
    ActivatedSessionV1, ActivationRequestV1, ControllerToBridgeV1, ControllerToWorkerV1,
    ProtocolResultV1, WireProtocolError,
};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

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
    /// Both lanes remain installed, but at least one bounded queue is full.
    Backpressured,
    /// A handshake receiver was closed; the session is now fail-closed.
    ContainmentRequired(Box<RelayContainmentTicket>),
    /// The session disappeared or is already quiescing.
    Unavailable,
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
    #[error("this session rendezvous is quiescing")]
    Quiescing,
}

/// Result of atomically handling one socket-close callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayConnectionLoss {
    /// The exact connection changed the session to `Quiescing`; persist the
    /// included authority before removing the rendezvous.
    ContainmentRequired(Box<RelayContainmentTicket>),
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

#[derive(Debug)]
struct BridgeSlot {
    connection_id: RelayConnectionId,
    outbound: mpsc::Sender<ControllerToBridgeV1>,
}

#[derive(Debug)]
struct WorkerSlot {
    connection_id: RelayConnectionId,
    outbound: mpsc::Sender<ControllerToWorkerV1>,
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
}

impl RendezvousRegistry {
    pub fn new(scope_id: ScopeId) -> Self {
        let (health, _) = watch::channel(RendezvousHealth::Healthy);
        Self {
            scope_id,
            state: Arc::new(Mutex::new(RendezvousState::default())),
            health,
        }
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
        outbound: mpsc::Sender<ControllerToBridgeV1>,
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
            let pairing = pair_if_ready(session, session_id);
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
        outbound: mpsc::Sender<ControllerToWorkerV1>,
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
            let pairing = pair_if_ready(session, session_id);
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
                pair_if_ready(session, session_id)
            })
    }

    /// Resolve the exact peer only after both handshake frames were enqueued.
    ///
    /// This is an inspection primitive. A transport must not use the returned
    /// ID across an await; bounded ACP enqueue is added as a registry operation
    /// in the following relay-delivery slice.
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
            RelaySessionState::Quiescing { .. } => Err(RendezvousRouteError::Quiescing),
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
            RelaySessionState::Pairing { .. } | RelaySessionState::Active { .. } => {
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
            RelaySessionState::Pairing { .. } | RelaySessionState::Active { .. } => {
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
                RelaySessionState::Pairing { .. } | RelaySessionState::Active { .. } => None,
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
    if matches!(session.state, RelaySessionState::Quiescing { .. }) {
        return Err(RendezvousInstallError::Quiescing);
    }
    if session.state.connection(lane).is_some() {
        return Err(RendezvousInstallError::LaneOccupied);
    }
    Ok(())
}

fn pair_if_ready(session: &mut RelaySession, session_id: SessionId) -> RelayPairingOutcome {
    let RelaySessionState::Pairing {
        bridge,
        worker,
        activation,
    } = &session.state
    else {
        return match session.state {
            RelaySessionState::Active { .. } => RelayPairingOutcome::Active,
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

    let worker_permit = match worker_sender.try_reserve_owned() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(_)) => return RelayPairingOutcome::Backpressured,
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
        Err(mpsc::error::TrySendError::Full(_)) => return RelayPairingOutcome::Backpressured,
        Err(mpsc::error::TrySendError::Closed(_)) => {
            let trigger = RelayConnection {
                session_id,
                lane: RelayLane::Bridge,
                connection_id: bridge_id,
            };
            return RelayPairingOutcome::ContainmentRequired(Box::new(quiesce(session, trigger)));
        }
    };

    worker_permit.send(ControllerToWorkerV1::ProtocolResult(worker_ack));
    bridge_permit.send(activation_response);

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
        let registry = RendezvousRegistry::new(ScopeId::derive("relay-health-test"));
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
