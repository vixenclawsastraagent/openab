#![cfg(feature = "controller")]

use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    OrphanAuthority, PendingActivation, RelayConnectionLoss, RelayContainmentCompletion,
    RelayInstallation, RelayLane, RelayPairingOutcome, RendezvousInstallError, RendezvousRegistry,
    RendezvousRouteError,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::{Fence, ProfileRef};
use openab_kubernetes_session::wire::{
    ActivationRequestV1, BrokerMappingExpectationV1, ControllerToBridgeV1, ControllerToWorkerV1,
    HandshakeOutcomeV1, ProtocolResultV1,
};
use std::thread;
use tokio::sync::mpsc;
use uuid::Uuid;

const RAW_SCOPE: &str = "organization-secret-team-a";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const POD_UID: &str = "worker-pod-uid-rendezvous";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn profile() -> ProfileRef {
    ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap()
}

fn authority(logical_session: &str, generation: u64, attempt_id: u128) -> OrphanAuthority {
    let binding = SessionBinding::new(
        scope_id(),
        SessionId::derive(RAW_SCOPE, logical_session),
        Fence::new(generation, Uuid::from_u128(attempt_id)).unwrap(),
        Uuid::from_u128(0x100),
    )
    .unwrap();
    OrphanAuthority::new(binding, POD_UID).unwrap()
}

fn activation(
    logical_session: &str,
    attempt_id: u128,
    authority: &OrphanAuthority,
) -> (ActivationRequestV1, PendingActivation) {
    let request = ActivationRequestV1::new(
        scope_id(),
        SessionId::derive(RAW_SCOPE, logical_session),
        Uuid::from_u128(attempt_id),
        PROFILE_NAME,
        BrokerMappingExpectationV1::Absent,
    )
    .unwrap();
    let pending = PendingActivation::new(&request, profile(), authority).unwrap();
    (request, pending)
}

fn install_bridge(
    registry: &RendezvousRegistry,
    authority: OrphanAuthority,
    logical_session: &str,
    attempt_id: u128,
) -> (
    ActivationRequestV1,
    RelayInstallation,
    mpsc::Receiver<ControllerToBridgeV1>,
) {
    let (request, pending) = activation(logical_session, attempt_id, &authority);
    let (sender, receiver) = mpsc::channel(1);
    let installation = registry.install_bridge(authority, pending, sender).unwrap();
    (request, installation, receiver)
}

fn install_worker(
    registry: &RendezvousRegistry,
    authority: OrphanAuthority,
) -> (RelayInstallation, mpsc::Receiver<ControllerToWorkerV1>) {
    let (sender, receiver) = mpsc::channel(1);
    let installation = registry
        .install_worker(authority, profile(), sender)
        .unwrap();
    (installation, receiver)
}

fn assert_worker_ack(receiver: &mut mpsc::Receiver<ControllerToWorkerV1>) {
    let ControllerToWorkerV1::ProtocolResult(result) = receiver.try_recv().unwrap() else {
        panic!("worker handshake must receive a protocol result")
    };
    assert_eq!(
        result.into_handshake_outcome().unwrap(),
        HandshakeOutcomeV1::Ack
    );
}

fn assert_bridge_activated(
    receiver: &mut mpsc::Receiver<ControllerToBridgeV1>,
    request: &ActivationRequestV1,
) {
    let ControllerToBridgeV1::Activation(response) = receiver.try_recv().unwrap() else {
        panic!("bridge handshake must receive an activation response")
    };
    let outcome = response.into_validated_outcome(request).unwrap();
    assert!(matches!(
        outcome,
        openab_kubernetes_session::wire::ValidatedActivationOutcomeV1::Activated {
            worker_cwd,
            ..
        } if worker_cwd == "/session/workspace"
    ));
}

#[tokio::test]
async fn bridge_first_withholds_activation_and_routing_until_worker_is_exact() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:paired", 1, 0x201);

    let (request, bridge, mut bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:paired", 0x201);
    assert_eq!(bridge.pairing(), &RelayPairingOutcome::AwaitingPeer);
    assert!(matches!(
        bridge_outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        registry.route_target(bridge.connection()),
        Err(RendezvousRouteError::AwaitingPeer)
    );

    let (worker, mut worker_outbound) = install_worker(&registry, authority);
    assert_eq!(worker.pairing(), &RelayPairingOutcome::Active);
    assert_ne!(
        bridge.connection().connection_id(),
        worker.connection().connection_id()
    );
    assert_worker_ack(&mut worker_outbound);
    assert_bridge_activated(&mut bridge_outbound, &request);
    assert_eq!(
        registry.route_target(bridge.connection()).unwrap(),
        worker.connection().connection_id()
    );
    assert_eq!(
        registry.route_target(worker.connection()).unwrap(),
        bridge.connection().connection_id()
    );
}

#[tokio::test]
async fn worker_first_is_held_until_the_later_exact_bridge_arrives() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:worker-first", 1, 0x209);
    let (worker, mut worker_outbound) = install_worker(&registry, authority.clone());
    assert_eq!(worker.pairing(), &RelayPairingOutcome::AwaitingPeer);
    assert!(matches!(
        worker_outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    let (request, bridge, mut bridge_outbound) =
        install_bridge(&registry, authority, "discord:worker-first", 0x209);
    assert_eq!(bridge.pairing(), &RelayPairingOutcome::Active);
    assert_worker_ack(&mut worker_outbound);
    assert_bridge_activated(&mut bridge_outbound, &request);
}

#[tokio::test]
async fn duplicate_conflicting_or_wrong_profile_lane_never_replaces_current() {
    let registry = RendezvousRegistry::new(scope_id());
    let current = authority("discord:no-replace", 1, 0x202);
    let replacement = authority("discord:no-replace", 2, 0x203);
    let (_, bridge, _bridge_outbound) =
        install_bridge(&registry, current.clone(), "discord:no-replace", 0x202);

    let (_, duplicate_pending) = activation("discord:no-replace", 0x202, &current);
    let (duplicate_sender, _duplicate_receiver) = mpsc::channel(1);
    assert!(matches!(
        registry.install_bridge(current.clone(), duplicate_pending, duplicate_sender),
        Err(RendezvousInstallError::LaneOccupied)
    ));

    let (replacement_sender, _replacement_receiver) = mpsc::channel(1);
    assert!(matches!(
        registry.install_worker(replacement, profile(), replacement_sender),
        Err(RendezvousInstallError::AuthorityConflict)
    ));

    let (profile_sender, _profile_receiver) = mpsc::channel(1);
    let other_profile = ProfileRef::new(PROFILE_NAME, "2026-08-02").unwrap();
    assert!(matches!(
        registry.install_worker(current, other_profile, profile_sender),
        Err(RendezvousInstallError::ProfileConflict)
    ));
    assert_eq!(
        registry.route_target(bridge.connection()),
        Err(RendezvousRouteError::AwaitingPeer)
    );
}

#[tokio::test]
async fn handshake_reserves_both_queues_before_exposing_either_result() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:backpressure", 1, 0x210);
    let (bridge_sender, mut bridge_receiver) = mpsc::channel(1);
    bridge_sender
        .try_send(ControllerToBridgeV1::ProtocolResult(
            ProtocolResultV1::ack(None).unwrap(),
        ))
        .unwrap();
    let (request, pending) = activation("discord:backpressure", 0x210, &authority);
    let bridge = registry
        .install_bridge(authority.clone(), pending, bridge_sender)
        .unwrap();

    let (worker, mut worker_receiver) = install_worker(&registry, authority);
    assert_eq!(worker.pairing(), &RelayPairingOutcome::Backpressured);
    assert!(matches!(
        worker_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        registry.route_target(bridge.connection()),
        Err(RendezvousRouteError::AwaitingPeer)
    ));

    assert!(matches!(
        bridge_receiver.try_recv().unwrap(),
        ControllerToBridgeV1::ProtocolResult(_)
    ));
    assert_eq!(
        registry.retry_pairing(bridge.connection().session_id()),
        RelayPairingOutcome::Active
    );
    assert_worker_ack(&mut worker_receiver);
    assert_bridge_activated(&mut bridge_receiver, &request);
}

#[tokio::test]
async fn closed_handshake_queue_quiesces_both_lanes_without_partial_ack() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:closed-handshake", 1, 0x211);
    let (_, bridge, bridge_receiver) = install_bridge(
        &registry,
        authority.clone(),
        "discord:closed-handshake",
        0x211,
    );
    let mut quiesced = bridge.quiesced();
    drop(bridge_receiver);

    let (worker, mut worker_receiver) = install_worker(&registry, authority);
    let RelayPairingOutcome::ContainmentRequired(ticket) = worker.pairing() else {
        panic!("closed bridge queue must require containment")
    };
    assert_eq!(ticket.trigger().lane(), RelayLane::Bridge);
    assert!(*quiesced.borrow_and_update());
    assert!(matches!(
        worker_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    assert_eq!(registry.pending_containments(), vec![(**ticket).clone()]);
    assert_eq!(
        registry.route_target(worker.connection()),
        Err(RendezvousRouteError::Quiescing)
    );
}

#[tokio::test]
async fn connection_loss_quiesces_before_reconnect_and_retries_same_ticket() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:quiescing", 1, 0x204);
    let (_, bridge, _bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:quiescing", 0x204);
    let (worker, _worker_outbound) = install_worker(&registry, authority.clone());

    let ticket = match registry.begin_connection_loss(bridge.connection()) {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected containment ticket, got {other:?}"),
    };
    assert_eq!(ticket.authority(), &authority);
    assert_eq!(
        ticket.connection_id(RelayLane::Bridge),
        Some(bridge.connection().connection_id())
    );
    assert_eq!(
        ticket.connection_id(RelayLane::Worker),
        Some(worker.connection().connection_id())
    );
    assert_eq!(
        registry.route_target(worker.connection()),
        Err(RendezvousRouteError::Quiescing)
    );

    let (_, pending) = activation("discord:quiescing", 0x204, &authority);
    let (sender, _receiver) = mpsc::channel(1);
    assert!(matches!(
        registry.install_bridge(authority, pending, sender),
        Err(RendezvousInstallError::Quiescing)
    ));
    let retry = match registry.begin_connection_loss(bridge.connection()) {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected retryable containment ticket, got {other:?}"),
    };
    assert_eq!(retry, ticket);
    assert_eq!(registry.pending_containments(), vec![ticket.clone()]);
    assert_eq!(
        registry.begin_connection_loss(worker.connection()),
        RelayConnectionLoss::AlreadyQuiescing
    );
    assert_eq!(
        registry.complete_containment(&ticket),
        RelayContainmentCompletion::Removed
    );
    assert_eq!(
        registry.complete_containment(&ticket),
        RelayContainmentCompletion::StaleTicket
    );
    assert!(registry.pending_containments().is_empty());
}

#[tokio::test]
async fn concurrent_close_and_reconnect_never_installs_replacement_lane() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:close-race", 1, 0x205);
    let (_, bridge, _bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:close-race", 0x205);
    let connection = bridge.connection().clone();
    let (_, pending) = activation("discord:close-race", 0x205, &authority);
    let (sender, _receiver) = mpsc::channel(1);

    let (loss, reconnect) = thread::scope(|scope| {
        let close_registry = registry.clone();
        let close = scope.spawn(move || close_registry.begin_connection_loss(&connection));
        let install_registry = registry.clone();
        let install =
            scope.spawn(move || install_registry.install_bridge(authority, pending, sender));
        (close.join().unwrap(), install.join().unwrap())
    });

    assert!(matches!(loss, RelayConnectionLoss::ContainmentRequired(_)));
    assert!(matches!(
        reconnect,
        Err(RendezvousInstallError::LaneOccupied | RendezvousInstallError::Quiescing)
    ));
}

#[tokio::test]
async fn stale_completion_cannot_remove_replacement_generation() {
    let registry = RendezvousRegistry::new(scope_id());
    let old_authority = authority("discord:stale-completion", 1, 0x206);
    let (_, old_bridge, _old_outbound) =
        install_bridge(&registry, old_authority, "discord:stale-completion", 0x206);
    let old_ticket = match registry.begin_connection_loss(old_bridge.connection()) {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected containment ticket, got {other:?}"),
    };
    assert_eq!(
        registry.complete_containment(&old_ticket),
        RelayContainmentCompletion::Removed
    );

    let new_authority = authority("discord:stale-completion", 2, 0x207);
    let (_, new_bridge, _new_outbound) =
        install_bridge(&registry, new_authority, "discord:stale-completion", 0x207);
    assert_eq!(
        registry.complete_containment(&old_ticket),
        RelayContainmentCompletion::StaleTicket
    );
    assert_eq!(
        registry.route_target(new_bridge.connection()),
        Err(RendezvousRouteError::AwaitingPeer)
    );
    assert_eq!(
        registry.begin_connection_loss(old_bridge.connection()),
        RelayConnectionLoss::StaleConnection
    );
}

#[tokio::test]
async fn wrong_scope_is_rejected_before_session_slot_is_created() {
    let registry = RendezvousRegistry::new(scope_id());
    let other_scope = "another-private-scope";
    let binding = SessionBinding::new(
        ScopeId::derive(other_scope),
        SessionId::derive(other_scope, "discord:wrong-scope"),
        Fence::new(1, Uuid::from_u128(0x208)).unwrap(),
        Uuid::from_u128(0x100),
    )
    .unwrap();
    let wrong = OrphanAuthority::new(binding, POD_UID).unwrap();
    let request = ActivationRequestV1::new(
        ScopeId::derive(other_scope),
        SessionId::derive(other_scope, "discord:wrong-scope"),
        Uuid::from_u128(0x208),
        PROFILE_NAME,
        BrokerMappingExpectationV1::Absent,
    )
    .unwrap();
    let pending = PendingActivation::new(&request, profile(), &wrong).unwrap();
    let (sender, _receiver) = mpsc::channel(1);

    assert!(matches!(
        registry.install_bridge(wrong, pending, sender),
        Err(RendezvousInstallError::ScopeMismatch)
    ));
}

#[tokio::test]
async fn post_mutation_failure_without_a_lane_retains_retryable_containment() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:unattached", 1, 0x212);

    let ticket = match registry.begin_unattached_containment(authority.clone()) {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("unattached authority must create a ticket, got {other:?}"),
    };
    assert_eq!(ticket.authority(), &authority);
    assert_eq!(ticket.connection_id(RelayLane::Bridge), None);
    assert_eq!(ticket.connection_id(RelayLane::Worker), None);
    assert_eq!(registry.pending_containments(), vec![ticket.clone()]);
    assert_eq!(
        registry.begin_unattached_containment(authority),
        RelayConnectionLoss::AlreadyQuiescing
    );
    assert_eq!(
        registry.complete_containment(&ticket),
        RelayContainmentCompletion::Removed
    );
}
