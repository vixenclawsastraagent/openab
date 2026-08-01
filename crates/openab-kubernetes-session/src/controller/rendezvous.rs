use super::OrphanAuthority;
use crate::identity::{ScopeId, SessionId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
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

/// A retryable token created by the atomic `Active -> Quiescing` transition.
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
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RendezvousInstallError {
    #[error("the relay connection does not belong to this controller scope")]
    ScopeMismatch,
    #[error("another worker generation already owns this session rendezvous")]
    AuthorityConflict,
    #[error("this relay lane already has an active connection")]
    LaneOccupied,
    #[error("this session rendezvous is quiescing")]
    Quiescing,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RendezvousRouteError {
    #[error("the relay connection is no longer current")]
    StaleConnection,
    #[error("the peer relay lane has not connected")]
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
enum RelaySessionState {
    Active {
        bridge: Option<RelayConnectionId>,
        worker: Option<RelayConnectionId>,
    },
    Quiescing {
        bridge: Option<RelayConnectionId>,
        worker: Option<RelayConnectionId>,
        trigger: RelayConnection,
    },
}

impl RelaySessionState {
    fn connection(&self, lane: RelayLane) -> Option<RelayConnectionId> {
        let (bridge, worker) = match self {
            Self::Active { bridge, worker } | Self::Quiescing { bridge, worker, .. } => {
                (bridge, worker)
            }
        };
        match lane {
            RelayLane::Bridge => *bridge,
            RelayLane::Worker => *worker,
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
}

/// In-memory rendezvous and close-fencing for one controller process.
///
/// This registry is deliberately not durable state. Kubernetes lifecycle
/// anchors remain the source of truth, while controller restart containment
/// handles every lane forgotten with this process. One mutex makes installing
/// a lane and changing its session to `Quiescing` a single atomic decision.
#[derive(Clone)]
pub struct RendezvousRegistry {
    scope_id: ScopeId,
    sessions: Arc<Mutex<HashMap<SessionId, RelaySession>>>,
}

impl RendezvousRegistry {
    pub fn new(scope_id: ScopeId) -> Self {
        Self {
            scope_id,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Install one exact authenticated lane without replacing an existing
    /// socket or a session whose containment is still pending.
    pub async fn install(
        &self,
        lane: RelayLane,
        authority: OrphanAuthority,
    ) -> Result<RelayConnection, RendezvousInstallError> {
        if authority.binding().scope_id() != self.scope_id {
            return Err(RendezvousInstallError::ScopeMismatch);
        }

        let session_id = authority.binding().session_id();
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            if matches!(session.state, RelaySessionState::Quiescing { .. }) {
                return Err(RendezvousInstallError::Quiescing);
            }
            if session.authority != authority {
                return Err(RendezvousInstallError::AuthorityConflict);
            }
            if session.state.connection(lane).is_some() {
                return Err(RendezvousInstallError::LaneOccupied);
            }

            let connection_id = fresh_connection_id(&session.state);
            let RelaySessionState::Active { bridge, worker } = &mut session.state else {
                unreachable!("quiescing state returned before active lane installation")
            };
            match lane {
                RelayLane::Bridge => *bridge = Some(connection_id),
                RelayLane::Worker => *worker = Some(connection_id),
            }
            return Ok(RelayConnection {
                session_id,
                lane,
                connection_id,
            });
        }

        let connection_id = RelayConnectionId(Uuid::new_v4());
        let (bridge, worker) = match lane {
            RelayLane::Bridge => (Some(connection_id), None),
            RelayLane::Worker => (None, Some(connection_id)),
        };
        sessions.insert(
            session_id,
            RelaySession {
                authority,
                state: RelaySessionState::Active { bridge, worker },
            },
        );
        Ok(RelayConnection {
            session_id,
            lane,
            connection_id,
        })
    }

    /// Resolve the exact peer only while both connections remain active.
    pub async fn route_target(
        &self,
        connection: &RelayConnection,
    ) -> Result<RelayConnectionId, RendezvousRouteError> {
        let sessions = self.sessions.lock().await;
        let Some(session) = sessions.get(&connection.session_id) else {
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
            RelaySessionState::Quiescing { .. } => Err(RendezvousRouteError::Quiescing),
        }
    }

    /// Atomically stop routing an exact connection before durable containment.
    ///
    /// Repeating the triggering callback returns the same ticket. A callback
    /// for the peer observes `AlreadyQuiescing`; a delayed callback from a
    /// replaced or removed socket is a benign stale observation.
    pub async fn begin_connection_loss(&self, connection: &RelayConnection) -> RelayConnectionLoss {
        let mut sessions = self.sessions.lock().await;
        let Some(session) = sessions.get_mut(&connection.session_id) else {
            return RelayConnectionLoss::StaleConnection;
        };
        if !session.state.contains(connection) {
            return RelayConnectionLoss::StaleConnection;
        }

        match &session.state {
            RelaySessionState::Active { bridge, worker } => {
                let ticket = RelayContainmentTicket {
                    authority: session.authority.clone(),
                    trigger: connection.clone(),
                    bridge: *bridge,
                    worker: *worker,
                };
                session.state = RelaySessionState::Quiescing {
                    bridge: *bridge,
                    worker: *worker,
                    trigger: connection.clone(),
                };
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

    /// Remove one quiescing rendezvous only after durable containment succeeds.
    /// A delayed completion token can never remove a replacement generation.
    pub async fn complete_containment(
        &self,
        ticket: &RelayContainmentTicket,
    ) -> RelayContainmentCompletion {
        let session_id = ticket.trigger.session_id;
        let mut sessions = self.sessions.lock().await;
        let removable = sessions.get(&session_id).is_some_and(|session| {
            session.authority == ticket.authority
                && matches!(
                    &session.state,
                    RelaySessionState::Quiescing { trigger, .. } if trigger == &ticket.trigger
                )
        });
        if removable {
            sessions.remove(&session_id);
            RelayContainmentCompletion::Removed
        } else {
            RelayContainmentCompletion::StaleTicket
        }
    }

    /// Snapshot retryable containment work without performing external I/O.
    ///
    /// This prevents a cancelled connection task from stranding a quiescing
    /// entry. Callers must release this method before awaiting the controller.
    pub async fn pending_containments(&self) -> Vec<RelayContainmentTicket> {
        let sessions = self.sessions.lock().await;
        let mut pending = sessions
            .values()
            .filter_map(|session| match &session.state {
                RelaySessionState::Quiescing { trigger, .. } => {
                    Some(containment_ticket(session, trigger))
                }
                RelaySessionState::Active { .. } => None,
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|ticket| ticket.trigger.session_id.as_hex());
        pending
    }
}

fn containment_ticket(session: &RelaySession, trigger: &RelayConnection) -> RelayContainmentTicket {
    RelayContainmentTicket {
        authority: session.authority.clone(),
        trigger: trigger.clone(),
        bridge: session.state.connection(RelayLane::Bridge),
        worker: session.state.connection(RelayLane::Worker),
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
