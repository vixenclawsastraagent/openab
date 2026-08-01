#![cfg(feature = "controller")]

use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    AcpRouteOutcome, OrphanAuthority, PendingActivation, RelayBackpressure, RelayByteBudget,
    RelayByteBudgetError, RelayConnectionLoss, RelayContainmentCompletion, RelayInstallation,
    RelayLane, RelayOutboundItem, RelayPairingOutcome, RendezvousInstallError, RendezvousRegistry,
    RendezvousRouteError, MIN_RELAY_BYTE_BUDGET,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::{Fence, ProfileRef};
use openab_kubernetes_session::wire::{
    decode_frame, AcpMessageV1, ActivationRequestV1, BrokerMappingExpectationV1,
    ControllerToBridgeV1, ControllerToWorkerV1, HandshakeOutcomeV1, WireMessage,
    MAX_CONTROL_FRAME_BYTES,
};
use serde_json::json;
use std::num::NonZeroUsize;
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

fn relay_budget(bytes: usize) -> RelayByteBudget {
    RelayByteBudget::new(NonZeroUsize::new(bytes).unwrap()).unwrap()
}

fn registry() -> RendezvousRegistry {
    RendezvousRegistry::with_byte_budget(scope_id(), relay_budget(MIN_RELAY_BYTE_BUDGET))
}

fn decode_outbound<M: WireMessage>(queued: RelayOutboundItem<M>) -> M {
    let frame = queued.into_encoded_frame().unwrap();
    decode_frame(frame.as_bytes()).unwrap()
}

fn profile() -> ProfileRef {
    ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap()
}

fn acp_message(label: &str) -> AcpMessageV1 {
    AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "method": "session/prompt",
        "params": {"label": label}
    }))
    .unwrap()
}

fn acp_queue_bytes(message: &AcpMessageV1) -> usize {
    serde_json::to_vec(message.payload()).unwrap().len() + MAX_CONTROL_FRAME_BYTES
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
    mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
) {
    let (request, pending) = activation(logical_session, attempt_id, &authority);
    let (sender, receiver) = mpsc::channel(1);
    let installation = registry.install_bridge(authority, pending, sender).unwrap();
    (request, installation, receiver)
}

fn install_worker(
    registry: &RendezvousRegistry,
    authority: OrphanAuthority,
) -> (
    RelayInstallation,
    mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>,
) {
    let (sender, receiver) = mpsc::channel(1);
    let installation = registry
        .install_worker(authority, profile(), sender)
        .unwrap();
    (installation, receiver)
}

fn assert_worker_ack(receiver: &mut mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>) {
    let queued = receiver.try_recv().unwrap();
    let ControllerToWorkerV1::ProtocolResult(result) = decode_outbound(queued) else {
        panic!("worker handshake must receive a protocol result")
    };
    assert_eq!(
        result.into_handshake_outcome().unwrap(),
        HandshakeOutcomeV1::Ack
    );
}

fn assert_bridge_activated(
    receiver: &mut mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
    request: &ActivationRequestV1,
) {
    let queued = receiver.try_recv().unwrap();
    let ControllerToBridgeV1::Activation(response) = decode_outbound(queued) else {
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
    let registry = registry();
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
    let registry = registry();
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
    let registry = registry();
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
async fn unexpected_handshake_queue_pressure_fails_closed_without_partial_result() {
    let registry = registry();
    let authority = authority("discord:backpressure", 1, 0x210);
    let (bridge_sender, mut bridge_receiver) = mpsc::channel(1);
    let held_capacity = bridge_sender.clone().try_reserve_owned().unwrap();
    let (request, pending) = activation("discord:backpressure", 0x210, &authority);
    let bridge = registry
        .install_bridge(authority.clone(), pending, bridge_sender)
        .unwrap();

    let (worker, mut worker_receiver) = install_worker(&registry, authority);
    let RelayPairingOutcome::ContainmentRequired(ticket) = worker.pairing() else {
        panic!("a full fresh handshake queue must fail closed")
    };
    assert_eq!(ticket.trigger().lane(), RelayLane::Bridge);
    assert!(matches!(
        worker_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        registry.route_target(bridge.connection()),
        Err(RendezvousRouteError::Quiescing)
    ));

    drop(held_capacity);
    assert!(matches!(
        worker_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        bridge_receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    let _ = request;
}

#[tokio::test]
async fn closed_handshake_queue_quiesces_both_lanes_without_partial_ack() {
    let registry = registry();
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
    let registry = registry();
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
    let registry = registry();
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
    let registry = registry();
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
    let registry = registry();
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
    let registry = registry();
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

#[tokio::test]
async fn active_acp_is_enqueued_to_the_exact_peer() {
    let registry = registry();
    let authority = authority("discord:acp-route", 1, 0x213);
    let (request, bridge, mut bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:acp-route", 0x213);
    let (worker, mut worker_outbound) = install_worker(&registry, authority);
    assert_bridge_activated(&mut bridge_outbound, &request);
    assert_worker_ack(&mut worker_outbound);
    let message = acp_message("bridge-to-worker");

    assert_eq!(
        registry.route_acp(bridge.connection(), message.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    let queued = worker_outbound.try_recv().unwrap();
    assert_eq!(decode_outbound(queued), ControllerToWorkerV1::Acp(message));

    let response = acp_message("worker-to-bridge");
    assert_eq!(
        registry.route_acp(worker.connection(), response.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    let queued = bridge_outbound.try_recv().unwrap();
    assert_eq!(decode_outbound(queued), ControllerToBridgeV1::Acp(response));
    assert_eq!(
        registry.route_target(worker.connection()).unwrap(),
        bridge.connection().connection_id()
    );
}

#[tokio::test]
async fn lane_item_backpressure_returns_the_original_acp_message() {
    let registry =
        RendezvousRegistry::with_byte_budget(scope_id(), relay_budget(4 * MAX_CONTROL_FRAME_BYTES));
    let authority = authority("discord:item-budget", 1, 0x214);
    let (request, bridge, mut bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:item-budget", 0x214);
    let (_worker, mut worker_outbound) = install_worker(&registry, authority);
    assert_bridge_activated(&mut bridge_outbound, &request);
    assert_worker_ack(&mut worker_outbound);

    let first = acp_message("first");
    let retry = acp_message("retry");
    assert_eq!(
        registry.route_acp(bridge.connection(), first.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    assert_eq!(
        registry.route_acp(bridge.connection(), retry.clone()),
        Ok(AcpRouteOutcome::Backpressured {
            message: retry.clone(),
            reason: RelayBackpressure::LaneItems,
        })
    );
    assert!(registry.pending_containments().is_empty());

    let queued = worker_outbound.try_recv().unwrap();
    assert_eq!(decode_outbound(queued), ControllerToWorkerV1::Acp(first));
    assert_eq!(
        registry.route_acp(bridge.connection(), retry.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    let queued = worker_outbound.try_recv().unwrap();
    assert_eq!(decode_outbound(queued), ControllerToWorkerV1::Acp(retry));
}

#[tokio::test]
async fn pairing_and_stale_sources_never_consume_delivery_budget() {
    let limit = 4 * MAX_CONTROL_FRAME_BYTES;
    let budget = relay_budget(limit);
    let registry = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());
    let authority = authority("discord:rejected-acp", 1, 0x219);
    let (_request, bridge, _bridge_outbound) =
        install_bridge(&registry, authority, "discord:rejected-acp", 0x219);

    assert_eq!(
        registry.route_acp(bridge.connection(), acp_message("pairing")),
        Err(RendezvousRouteError::AwaitingPeer)
    );
    assert_eq!(budget.available_bytes(), limit);
    let RelayConnectionLoss::ContainmentRequired(ticket) =
        registry.begin_connection_loss(bridge.connection())
    else {
        panic!("the exact bridge must enter containment")
    };
    assert_eq!(
        registry.complete_containment(&ticket),
        RelayContainmentCompletion::Removed
    );
    assert_eq!(
        registry.route_acp(bridge.connection(), acp_message("stale")),
        Err(RendezvousRouteError::StaleConnection)
    );
    assert_eq!(budget.available_bytes(), limit);
}

#[tokio::test]
async fn acp_frame_larger_than_the_configured_budget_is_terminal() {
    let limit = MIN_RELAY_BYTE_BUDGET;
    let budget = relay_budget(limit);
    let registry = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());
    let authority = authority("discord:oversized-budget", 1, 0x21a);
    let (request, bridge, mut bridge_outbound) = install_bridge(
        &registry,
        authority.clone(),
        "discord:oversized-budget",
        0x21a,
    );
    let (_worker, mut worker_outbound) = install_worker(&registry, authority);
    assert_bridge_activated(&mut bridge_outbound, &request);
    assert_worker_ack(&mut worker_outbound);
    let message = AcpMessageV1::new(json!({"data": "x".repeat(MAX_CONTROL_FRAME_BYTES)})).unwrap();
    let bytes = acp_queue_bytes(&message);
    assert!(bytes > limit);

    assert_eq!(
        registry.route_acp(bridge.connection(), message),
        Err(RendezvousRouteError::FrameExceedsByteBudget {
            bytes,
            capacity: limit,
        })
    );
    assert_eq!(budget.available_bytes(), limit);
}

#[tokio::test]
async fn process_byte_budget_is_shared_and_held_until_the_writer_drops_delivery() {
    let message = AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "method": "session/prompt",
        "params": {"data": "x".repeat(2 * MAX_CONTROL_FRAME_BYTES)}
    }))
    .unwrap();
    let limit = acp_queue_bytes(&message);
    let budget = relay_budget(limit);
    let registry_a = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());
    let registry_b = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());

    let authority_a = authority("discord:byte-budget-a", 1, 0x215);
    let (request_a, bridge_a, mut bridge_outbound_a) = install_bridge(
        &registry_a,
        authority_a.clone(),
        "discord:byte-budget-a",
        0x215,
    );
    let (_worker_a, mut worker_outbound_a) = install_worker(&registry_a, authority_a);
    assert_bridge_activated(&mut bridge_outbound_a, &request_a);
    assert_worker_ack(&mut worker_outbound_a);

    let authority_b = authority("discord:byte-budget-b", 1, 0x216);
    let (request_b, bridge_b, mut bridge_outbound_b) = install_bridge(
        &registry_b,
        authority_b.clone(),
        "discord:byte-budget-b",
        0x216,
    );
    let (_worker_b, mut worker_outbound_b) = install_worker(&registry_b, authority_b);
    assert_bridge_activated(&mut bridge_outbound_b, &request_b);
    assert_worker_ack(&mut worker_outbound_b);
    assert_eq!(budget.available_bytes(), limit);

    assert_eq!(
        registry_a.route_acp(bridge_a.connection(), message.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    let held_by_writer = worker_outbound_a
        .try_recv()
        .unwrap()
        .into_encoded_frame()
        .unwrap();
    assert_eq!(
        decode_frame::<ControllerToWorkerV1>(held_by_writer.as_bytes()).unwrap(),
        ControllerToWorkerV1::Acp(message.clone())
    );
    assert_eq!(budget.available_bytes(), 0);
    assert_eq!(
        registry_b.route_acp(bridge_b.connection(), message.clone()),
        Ok(AcpRouteOutcome::Backpressured {
            message: message.clone(),
            reason: RelayBackpressure::ProcessBytes,
        })
    );

    drop(held_by_writer);
    assert_eq!(budget.available_bytes(), limit);
    assert_eq!(
        registry_b.route_acp(bridge_b.connection(), message.clone()),
        Ok(AcpRouteOutcome::Delivered)
    );
    let queued = worker_outbound_b.try_recv().unwrap();
    assert_eq!(decode_outbound(queued), ControllerToWorkerV1::Acp(message));
}

#[tokio::test]
async fn closed_peer_queue_quiesces_under_the_route_lock_and_releases_bytes() {
    let limit = 4 * MAX_CONTROL_FRAME_BYTES;
    let budget = relay_budget(limit);
    let registry = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());
    let authority = authority("discord:closed-peer", 1, 0x217);
    let (request, bridge, mut bridge_outbound) =
        install_bridge(&registry, authority.clone(), "discord:closed-peer", 0x217);
    let (_worker, mut worker_outbound) = install_worker(&registry, authority);
    assert_bridge_activated(&mut bridge_outbound, &request);
    assert_worker_ack(&mut worker_outbound);
    assert_eq!(budget.available_bytes(), limit);
    drop(worker_outbound);

    let AcpRouteOutcome::ContainmentRequired(ticket) = registry
        .route_acp(bridge.connection(), acp_message("closed"))
        .unwrap()
    else {
        panic!("a closed peer queue must require containment")
    };
    assert_eq!(ticket.trigger().lane(), RelayLane::Worker);
    assert_eq!(registry.pending_containments(), vec![(*ticket).clone()]);
    assert_eq!(
        registry.route_target(bridge.connection()),
        Err(RendezvousRouteError::Quiescing)
    );
    assert_eq!(budget.available_bytes(), limit);
}

#[test]
fn relay_byte_budget_rejects_less_than_one_atomic_handshake() {
    let limit = MIN_RELAY_BYTE_BUDGET - 1;
    assert_eq!(
        RelayByteBudget::new(NonZeroUsize::new(limit).unwrap()).unwrap_err(),
        RelayByteBudgetError::BelowAtomicHandshake {
            configured: limit,
            minimum: MIN_RELAY_BYTE_BUDGET,
        }
    );
}

#[test]
fn route_and_close_are_linearized_by_the_same_registry_lock() {
    for iteration in 0..64_u128 {
        let registry = registry();
        let logical_session = format!("discord:route-close-{iteration}");
        let attempt = 0x300 + iteration;
        let authority = authority(&logical_session, 1, attempt);
        let (request, bridge, mut bridge_outbound) =
            install_bridge(&registry, authority.clone(), &logical_session, attempt);
        let (_worker, mut worker_outbound) = install_worker(&registry, authority);
        assert_bridge_activated(&mut bridge_outbound, &request);
        assert_worker_ack(&mut worker_outbound);
        let connection = bridge.connection().clone();
        let message = acp_message("race");

        let (route, loss) = thread::scope(|scope| {
            let route_registry = registry.clone();
            let route_connection = connection.clone();
            let route = scope.spawn(move || route_registry.route_acp(&route_connection, message));
            let close_registry = registry.clone();
            let close = scope.spawn(move || close_registry.begin_connection_loss(&connection));
            (route.join().unwrap(), close.join().unwrap())
        });
        assert!(matches!(loss, RelayConnectionLoss::ContainmentRequired(_)));
        match route {
            Ok(AcpRouteOutcome::Delivered) => {
                let queued = worker_outbound.try_recv().unwrap();
                assert!(matches!(
                    decode_outbound(queued),
                    ControllerToWorkerV1::Acp(_)
                ));
                assert!(matches!(
                    worker_outbound.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
                ));
            }
            Err(RendezvousRouteError::Quiescing) => assert!(matches!(
                worker_outbound.try_recv(),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
            )),
            other => panic!("unexpected route/close outcome: {other:?}"),
        }
    }
}
