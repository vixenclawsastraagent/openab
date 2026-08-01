use super::{
    containment_ticket, quiesce, relay_write_report, BridgeSlot, RelayByteLease, RelayConnection,
    RelayContainmentTicket, RelayControlWaiter, RelayLane, RelayOutboundItem, RelaySessionState,
    RelayWriteStatus, RendezvousRegistry, WorkerSlot,
};
use crate::controller::OrphanAuthority;
use crate::wire::{ControllerToBridgeV1, LifecycleRequestV1, MAX_CONTROL_FRAME_BYTES};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RendezvousLifecycleError {
    #[error("the relay connection is no longer current")]
    StaleConnection,
    #[error("lifecycle requests require an active relay pair")]
    AwaitingPeer,
    #[error("only the authenticated bridge lane may request lifecycle changes")]
    WrongLane,
    #[error("the lifecycle request binding does not match durable relay authority")]
    BindingMismatch,
    #[error("another lifecycle request already owns this session")]
    ConflictingRequest,
    #[error("this session rendezvous is quiescing")]
    Quiescing,
    #[error("the lifecycle reservation is no longer current")]
    ReservationLost,
}

pub(crate) enum LifecycleAdmission {
    Reserve(Box<LifecycleReservation>),
    Coalesced,
    Rejected(RendezvousLifecycleError),
    ContainmentRequired {
        error: RendezvousLifecycleError,
        ticket: Box<RelayContainmentTicket>,
    },
}

pub(crate) enum LifecycleAcquireOutcome {
    Ready(Box<RelayLifecycleExecution>),
    Rejected(RendezvousLifecycleError),
    ContainmentRequired(Box<RelayContainmentTicket>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RelayLifecycleTerminal {
    Suspended,
    Released,
}

pub(crate) enum LifecycleDeliveryOutcome {
    Written(RelayLifecycleTerminal),
    ContainmentRequired(Box<RelayContainmentTicket>),
    Lost,
}

#[derive(Debug)]
pub(super) struct LifecycleSessionState {
    pub(super) bridge: BridgeSlot,
    pub(super) worker: Option<WorkerSlot>,
    pub(super) request: LifecycleRequestV1,
    release_accepted: bool,
    pub(super) phase: LifecyclePhase,
}

impl LifecycleSessionState {
    pub(super) fn worker_detach_is_expected_or_provisional(&self) -> bool {
        self.release_accepted
            || matches!(
                self.phase,
                LifecyclePhase::Running { .. } | LifecyclePhase::ResultQueued { .. }
            )
    }
}

#[derive(Debug)]
pub(super) enum LifecyclePhase {
    ReservingQueue {
        reservation_id: Uuid,
    },
    ReservingBytes {
        reservation_id: Uuid,
        _control_waiter: RelayControlWaiter,
    },
    Running {
        pass_id: Uuid,
    },
    Pending,
    ResultQueued {
        pass_id: Uuid,
        delivery_id: Uuid,
        terminal: RelayLifecycleTerminal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecycleReservationKey {
    authority: OrphanAuthority,
    bridge: RelayConnection,
    request: LifecycleRequestV1,
    reservation_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecyclePassKey {
    authority: OrphanAuthority,
    bridge: RelayConnection,
    request: LifecycleRequestV1,
    pass_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecycleDeliveryKey {
    pass: LifecyclePassKey,
    delivery_id: Uuid,
    terminal: RelayLifecycleTerminal,
}

pub(crate) struct LifecycleReservation {
    registry: RendezvousRegistry,
    key: LifecycleReservationKey,
    bridge_outbound: mpsc::Sender<RelayOutboundItem<ControllerToBridgeV1>>,
    quiesced: tokio::sync::watch::Receiver<bool>,
    armed: bool,
}

impl LifecycleReservation {
    pub(crate) async fn acquire(mut self) -> LifecycleAcquireOutcome {
        let bridge_outbound = self.bridge_outbound.clone();
        let reserve = bridge_outbound.reserve_owned();
        tokio::pin!(reserve);
        let permit = match tokio::select! {
            result = &mut reserve => Some(result),
            _ = self.quiesced.changed() => None,
        } {
            None => {
                self.armed = false;
                return LifecycleAcquireOutcome::Rejected(RendezvousLifecycleError::Quiescing);
            }
            Some(result) => match result {
                Ok(permit) => permit,
                Err(_) => {
                    self.armed = false;
                    return self.registry.fail_closed_reservation(&self.key);
                }
            },
        };

        if let Err(error) = self.registry.begin_lifecycle_byte_wait(&self.key) {
            self.armed = false;
            return LifecycleAcquireOutcome::Rejected(error);
        }

        let mut releases = self.registry.byte_budget_releases();
        let byte_lease = loop {
            if let Some(lease) = self
                .registry
                .byte_budget
                .try_acquire(MAX_CONTROL_FRAME_BYTES)
            {
                break lease;
            }
            tokio::select! {
                released = releases.changed() => {
                    if released.is_err() {
                        self.armed = false;
                        return LifecycleAcquireOutcome::Rejected(
                            RendezvousLifecycleError::ReservationLost,
                        );
                    }
                }
                _ = self.quiesced.changed() => {
                    self.armed = false;
                    return LifecycleAcquireOutcome::Rejected(
                        RendezvousLifecycleError::Quiescing,
                    );
                }
            }
        };

        self.armed = false;
        self.registry
            .activate_lifecycle_reservation(&self.key, permit, byte_lease)
    }
}

impl Drop for LifecycleReservation {
    fn drop(&mut self) {
        if self.armed {
            self.registry.defer_lifecycle_reservation(&self.key);
        }
    }
}

pub(crate) struct RelayLifecycleExecution {
    registry: RendezvousRegistry,
    key: LifecyclePassKey,
    response_permit: Option<mpsc::OwnedPermit<RelayOutboundItem<ControllerToBridgeV1>>>,
    response_bytes: Option<RelayByteLease>,
    armed: bool,
}

impl RelayLifecycleExecution {
    pub(crate) fn request(&self) -> &LifecycleRequestV1 {
        &self.key.request
    }

    pub(crate) fn defer_release_pending(mut self) {
        self.registry.defer_lifecycle_execution(&self.key, true);
        self.armed = false;
    }

    pub(crate) fn defer_controller_error(mut self) -> Option<Box<RelayContainmentTicket>> {
        let containment = self.registry.defer_lifecycle_execution(&self.key, false);
        self.armed = false;
        containment
    }

    pub(crate) fn queue_result(
        mut self,
        message: ControllerToBridgeV1,
        terminal: RelayLifecycleTerminal,
    ) -> Result<RelayLifecycleDelivery, RendezvousLifecycleError> {
        let delivery_id = Uuid::new_v4();
        let delivery_key = LifecycleDeliveryKey {
            pass: self.key.clone(),
            delivery_id,
            terminal,
        };
        let (write_reporter, write_result) = relay_write_report();
        let permit = self
            .response_permit
            .take()
            .expect("an armed lifecycle execution owns one response slot");
        let byte_lease = self
            .response_bytes
            .take()
            .expect("an armed lifecycle execution owns one response byte lease");

        let mut state = self.registry.lock_state();
        let Some(session) = state.sessions.get_mut(&self.key.bridge.session_id) else {
            self.armed = false;
            return Err(RendezvousLifecycleError::ReservationLost);
        };
        if session.authority != self.key.authority {
            self.armed = false;
            return Err(RendezvousLifecycleError::ReservationLost);
        }
        let RelaySessionState::Lifecycle(lifecycle) = &mut session.state else {
            self.armed = false;
            return Err(RendezvousLifecycleError::ReservationLost);
        };
        if lifecycle.bridge.connection_id != self.key.bridge.connection_id
            || lifecycle.request != self.key.request
            || !matches!(
                lifecycle.phase,
                LifecyclePhase::Running { pass_id } if pass_id == self.key.pass_id
            )
        {
            self.armed = false;
            return Err(RendezvousLifecycleError::ReservationLost);
        }

        permit.send(RelayOutboundItem::reported(
            message,
            byte_lease,
            write_reporter,
        ));
        lifecycle.phase = LifecyclePhase::ResultQueued {
            pass_id: self.key.pass_id,
            delivery_id,
            terminal,
        };
        drop(state);
        self.armed = false;
        Ok(RelayLifecycleDelivery {
            registry: self.registry.clone(),
            key: delivery_key,
            write_result,
        })
    }
}

impl Drop for RelayLifecycleExecution {
    fn drop(&mut self) {
        if self.armed {
            self.registry.defer_lifecycle_execution(&self.key, false);
        }
    }
}

pub(crate) struct RelayLifecycleDelivery {
    registry: RendezvousRegistry,
    key: LifecycleDeliveryKey,
    write_result: oneshot::Receiver<()>,
}

impl RelayLifecycleDelivery {
    pub(crate) async fn wait(self) -> LifecycleDeliveryOutcome {
        let status = self
            .write_result
            .await
            .map_or(RelayWriteStatus::Dropped, |()| RelayWriteStatus::Written);
        self.registry.complete_lifecycle_delivery(&self.key, status)
    }
}

impl RendezvousRegistry {
    pub(crate) fn begin_lifecycle(
        &self,
        connection: &RelayConnection,
        request: LifecycleRequestV1,
    ) -> LifecycleAdmission {
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&connection.session_id) else {
            return LifecycleAdmission::Rejected(RendezvousLifecycleError::StaleConnection);
        };
        if !session.state.contains(connection) {
            return LifecycleAdmission::Rejected(RendezvousLifecycleError::StaleConnection);
        }
        if matches!(session.state, RelaySessionState::Quiescing { .. }) {
            return LifecycleAdmission::Rejected(RendezvousLifecycleError::Quiescing);
        }
        if connection.lane != RelayLane::Bridge {
            return lifecycle_violation(session, connection, RendezvousLifecycleError::WrongLane);
        }
        if request.to_binding().ok().as_ref() != Some(session.authority.binding()) {
            return lifecycle_violation(
                session,
                connection,
                RendezvousLifecycleError::BindingMismatch,
            );
        }

        match &mut session.state {
            RelaySessionState::Active { bridge, worker } => {
                let reservation_id = Uuid::new_v4();
                let bridge = bridge.clone();
                let worker = worker.clone();
                let authority = session.authority.clone();
                let bridge_connection = connection.clone();
                let bridge_outbound = bridge.outbound.clone();
                session.state = RelaySessionState::Lifecycle(LifecycleSessionState {
                    bridge,
                    worker: Some(worker),
                    request: request.clone(),
                    release_accepted: false,
                    phase: LifecyclePhase::ReservingQueue { reservation_id },
                });
                LifecycleAdmission::Reserve(Box::new(LifecycleReservation {
                    registry: self.clone(),
                    key: LifecycleReservationKey {
                        authority,
                        bridge: bridge_connection,
                        request,
                        reservation_id,
                    },
                    bridge_outbound,
                    quiesced: session.quiesced.subscribe(),
                    armed: true,
                }))
            }
            RelaySessionState::Lifecycle(lifecycle) => {
                if lifecycle.request != request {
                    return lifecycle_violation(
                        session,
                        connection,
                        RendezvousLifecycleError::ConflictingRequest,
                    );
                }
                if !matches!(lifecycle.phase, LifecyclePhase::Pending) {
                    return LifecycleAdmission::Coalesced;
                }
                let reservation_id = Uuid::new_v4();
                lifecycle.phase = LifecyclePhase::ReservingQueue { reservation_id };
                LifecycleAdmission::Reserve(Box::new(LifecycleReservation {
                    registry: self.clone(),
                    key: LifecycleReservationKey {
                        authority: session.authority.clone(),
                        bridge: connection.clone(),
                        request,
                        reservation_id,
                    },
                    bridge_outbound: lifecycle.bridge.outbound.clone(),
                    quiesced: session.quiesced.subscribe(),
                    armed: true,
                }))
            }
            RelaySessionState::Pairing { .. } => {
                lifecycle_violation(session, connection, RendezvousLifecycleError::AwaitingPeer)
            }
            RelaySessionState::Quiescing { .. } => {
                LifecycleAdmission::Rejected(RendezvousLifecycleError::Quiescing)
            }
        }
    }

    fn begin_lifecycle_byte_wait(
        &self,
        key: &LifecycleReservationKey,
    ) -> Result<(), RendezvousLifecycleError> {
        let mut state = self.lock_state();
        let lifecycle = exact_lifecycle_mut(&mut state, &key.authority, &key.bridge, &key.request)?;
        if !matches!(
            lifecycle.phase,
            LifecyclePhase::ReservingQueue { reservation_id }
                if reservation_id == key.reservation_id
        ) {
            return Err(RendezvousLifecycleError::ReservationLost);
        }
        lifecycle.phase = LifecyclePhase::ReservingBytes {
            reservation_id: key.reservation_id,
            _control_waiter: self.byte_budget.begin_control_wait(),
        };
        Ok(())
    }

    fn activate_lifecycle_reservation(
        &self,
        key: &LifecycleReservationKey,
        response_permit: mpsc::OwnedPermit<RelayOutboundItem<ControllerToBridgeV1>>,
        response_bytes: RelayByteLease,
    ) -> LifecycleAcquireOutcome {
        let mut state = self.lock_state();
        let lifecycle =
            match exact_lifecycle_mut(&mut state, &key.authority, &key.bridge, &key.request) {
                Ok(lifecycle) => lifecycle,
                Err(error) => return LifecycleAcquireOutcome::Rejected(error),
            };
        if !matches!(
            lifecycle.phase,
            LifecyclePhase::ReservingBytes { reservation_id, .. }
                if reservation_id == key.reservation_id
        ) {
            return LifecycleAcquireOutcome::Rejected(RendezvousLifecycleError::ReservationLost);
        }
        let pass_id = Uuid::new_v4();
        lifecycle.phase = LifecyclePhase::Running { pass_id };
        LifecycleAcquireOutcome::Ready(Box::new(RelayLifecycleExecution {
            registry: self.clone(),
            key: LifecyclePassKey {
                authority: key.authority.clone(),
                bridge: key.bridge.clone(),
                request: key.request.clone(),
                pass_id,
            },
            response_permit: Some(response_permit),
            response_bytes: Some(response_bytes),
            armed: true,
        }))
    }

    fn defer_lifecycle_reservation(&self, key: &LifecycleReservationKey) {
        let mut state = self.lock_state();
        let Ok(lifecycle) =
            exact_lifecycle_mut(&mut state, &key.authority, &key.bridge, &key.request)
        else {
            return;
        };
        if matches!(
            lifecycle.phase,
            LifecyclePhase::ReservingQueue { reservation_id }
                | LifecyclePhase::ReservingBytes { reservation_id, .. }
                if reservation_id == key.reservation_id
        ) {
            lifecycle.phase = LifecyclePhase::Pending;
        }
    }

    fn defer_lifecycle_execution(
        &self,
        key: &LifecyclePassKey,
        release_accepted: bool,
    ) -> Option<Box<RelayContainmentTicket>> {
        let mut state = self.lock_state();
        let session = state.sessions.get_mut(&key.bridge.session_id)?;
        if session.authority != key.authority || !session.state.contains(&key.bridge) {
            return None;
        }
        let RelaySessionState::Lifecycle(lifecycle) = &mut session.state else {
            return None;
        };
        if lifecycle.request != key.request
            || !matches!(
                lifecycle.phase,
                LifecyclePhase::Running { pass_id } if pass_id == key.pass_id
            )
        {
            return None;
        }
        if release_accepted {
            lifecycle.release_accepted = true;
        }
        if !lifecycle.release_accepted && lifecycle.worker.is_none() {
            return Some(Box::new(quiesce(session, key.bridge.clone())));
        }
        lifecycle.phase = LifecyclePhase::Pending;
        None
    }

    fn fail_closed_reservation(&self, key: &LifecycleReservationKey) -> LifecycleAcquireOutcome {
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&key.bridge.session_id) else {
            return LifecycleAcquireOutcome::Rejected(RendezvousLifecycleError::StaleConnection);
        };
        if session.authority != key.authority || !session.state.contains(&key.bridge) {
            return LifecycleAcquireOutcome::Rejected(RendezvousLifecycleError::StaleConnection);
        }
        let exact_reservation = matches!(
            &session.state,
            RelaySessionState::Lifecycle(lifecycle)
                if lifecycle.request == key.request
                    && matches!(
                        lifecycle.phase,
                        LifecyclePhase::ReservingQueue { reservation_id }
                            | LifecyclePhase::ReservingBytes { reservation_id, .. }
                            if reservation_id == key.reservation_id
                    )
        );
        if !exact_reservation {
            return LifecycleAcquireOutcome::Rejected(RendezvousLifecycleError::ReservationLost);
        }
        LifecycleAcquireOutcome::ContainmentRequired(Box::new(quiesce(session, key.bridge.clone())))
    }

    fn complete_lifecycle_delivery(
        &self,
        key: &LifecycleDeliveryKey,
        status: RelayWriteStatus,
    ) -> LifecycleDeliveryOutcome {
        let session_id = key.pass.bridge.session_id;
        let mut state = self.lock_state();
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return LifecycleDeliveryOutcome::Lost;
        };
        if session.authority != key.pass.authority {
            return LifecycleDeliveryOutcome::Lost;
        }
        if matches!(session.state, RelaySessionState::Quiescing { .. }) {
            return LifecycleDeliveryOutcome::Lost;
        }
        let exact = matches!(
            &session.state,
            RelaySessionState::Lifecycle(lifecycle)
                if lifecycle.bridge.connection_id == key.pass.bridge.connection_id
                    && lifecycle.request == key.pass.request
                    && matches!(
                        lifecycle.phase,
                        LifecyclePhase::ResultQueued {
                            pass_id,
                            delivery_id,
                            terminal,
                        } if pass_id == key.pass.pass_id
                            && delivery_id == key.delivery_id
                            && terminal == key.terminal
                    )
        );
        if !exact {
            return LifecycleDeliveryOutcome::Lost;
        }
        match status {
            RelayWriteStatus::Written => {
                session.quiesced.send_replace(true);
                state.sessions.remove(&session_id);
                LifecycleDeliveryOutcome::Written(key.terminal)
            }
            RelayWriteStatus::Dropped => LifecycleDeliveryOutcome::ContainmentRequired(Box::new(
                quiesce(session, key.pass.bridge.clone()),
            )),
        }
    }
}

fn exact_lifecycle_mut<'a>(
    state: &'a mut super::RendezvousState,
    authority: &OrphanAuthority,
    bridge: &RelayConnection,
    request: &LifecycleRequestV1,
) -> Result<&'a mut LifecycleSessionState, RendezvousLifecycleError> {
    let Some(session) = state.sessions.get_mut(&bridge.session_id) else {
        return Err(RendezvousLifecycleError::StaleConnection);
    };
    if session.authority != *authority || !session.state.contains(bridge) {
        return Err(RendezvousLifecycleError::StaleConnection);
    }
    let lifecycle = match &mut session.state {
        RelaySessionState::Lifecycle(lifecycle) => lifecycle,
        RelaySessionState::Quiescing { .. } => {
            return Err(RendezvousLifecycleError::Quiescing);
        }
        RelaySessionState::Pairing { .. } | RelaySessionState::Active { .. } => {
            return Err(RendezvousLifecycleError::ReservationLost);
        }
    };
    if lifecycle.request != *request {
        return Err(RendezvousLifecycleError::ReservationLost);
    }
    Ok(lifecycle)
}

fn lifecycle_violation(
    session: &mut super::RelaySession,
    connection: &RelayConnection,
    error: RendezvousLifecycleError,
) -> LifecycleAdmission {
    let ticket = if matches!(session.state, RelaySessionState::Quiescing { .. }) {
        containment_ticket(session, connection)
    } else {
        quiesce(session, connection.clone())
    };
    LifecycleAdmission::ContainmentRequired {
        error,
        ticket: Box::new(ticket),
    }
}
