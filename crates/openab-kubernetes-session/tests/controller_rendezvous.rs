#![cfg(feature = "controller")]

use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    OrphanAuthority, RelayConnectionLoss, RelayContainmentCompletion, RelayLane,
    RendezvousInstallError, RendezvousRegistry, RendezvousRouteError,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::Fence;
use std::sync::Arc;
use uuid::Uuid;

const RAW_SCOPE: &str = "organization-secret-team-a";
const POD_UID: &str = "worker-pod-uid-rendezvous";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
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

#[tokio::test]
async fn exact_bridge_and_worker_are_paired_for_bidirectional_routing() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:paired", 1, 0x201);

    let bridge = registry
        .install(RelayLane::Bridge, authority.clone())
        .await
        .unwrap();
    assert_eq!(
        registry.route_target(&bridge).await,
        Err(RendezvousRouteError::AwaitingPeer)
    );

    let worker = registry
        .install(RelayLane::Worker, authority)
        .await
        .unwrap();
    assert_ne!(bridge.connection_id(), worker.connection_id());
    assert_eq!(
        registry.route_target(&bridge).await.unwrap(),
        worker.connection_id()
    );
    assert_eq!(
        registry.route_target(&worker).await.unwrap(),
        bridge.connection_id()
    );
}

#[tokio::test]
async fn worker_first_is_paired_with_the_later_exact_bridge() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:worker-first", 1, 0x209);
    let worker = registry
        .install(RelayLane::Worker, authority.clone())
        .await
        .unwrap();
    let bridge = registry
        .install(RelayLane::Bridge, authority)
        .await
        .unwrap();

    assert_eq!(
        registry.route_target(&worker).await.unwrap(),
        bridge.connection_id()
    );
}

#[tokio::test]
async fn duplicate_or_conflicting_lane_never_replaces_an_active_connection() {
    let registry = RendezvousRegistry::new(scope_id());
    let current = authority("discord:no-replace", 1, 0x202);
    let replacement = authority("discord:no-replace", 2, 0x203);
    let bridge = registry
        .install(RelayLane::Bridge, current.clone())
        .await
        .unwrap();

    assert_eq!(
        registry.install(RelayLane::Bridge, current.clone()).await,
        Err(RendezvousInstallError::LaneOccupied)
    );
    assert_eq!(
        registry.install(RelayLane::Worker, replacement).await,
        Err(RendezvousInstallError::AuthorityConflict)
    );
    assert_eq!(
        registry.route_target(&bridge).await,
        Err(RendezvousRouteError::AwaitingPeer)
    );
}

#[tokio::test]
async fn connection_loss_quiesces_before_reconnect_and_retries_the_same_ticket() {
    let registry = RendezvousRegistry::new(scope_id());
    let authority = authority("discord:quiescing", 1, 0x204);
    let bridge = registry
        .install(RelayLane::Bridge, authority.clone())
        .await
        .unwrap();
    let worker = registry
        .install(RelayLane::Worker, authority.clone())
        .await
        .unwrap();

    let ticket = match registry.begin_connection_loss(&bridge).await {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected containment ticket, got {other:?}"),
    };
    assert_eq!(ticket.authority(), &authority);
    assert_eq!(
        ticket.connection_id(RelayLane::Bridge),
        Some(bridge.connection_id())
    );
    assert_eq!(
        ticket.connection_id(RelayLane::Worker),
        Some(worker.connection_id())
    );
    assert_eq!(
        registry.route_target(&worker).await,
        Err(RendezvousRouteError::Quiescing)
    );
    assert_eq!(
        registry.install(RelayLane::Bridge, authority).await,
        Err(RendezvousInstallError::Quiescing)
    );

    let retry = match registry.begin_connection_loss(&bridge).await {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected retryable containment ticket, got {other:?}"),
    };
    assert_eq!(retry, ticket);
    assert_eq!(registry.pending_containments().await, vec![ticket.clone()]);
    assert_eq!(
        registry.begin_connection_loss(&worker).await,
        RelayConnectionLoss::AlreadyQuiescing
    );
    assert_eq!(
        registry.complete_containment(&ticket).await,
        RelayContainmentCompletion::Removed
    );
    assert_eq!(
        registry.complete_containment(&ticket).await,
        RelayContainmentCompletion::StaleTicket
    );
    assert!(registry.pending_containments().await.is_empty());
    assert_eq!(
        registry.route_target(&worker).await,
        Err(RendezvousRouteError::StaleConnection)
    );
}

#[tokio::test]
async fn concurrent_close_and_reconnect_never_installs_a_replacement_lane() {
    let registry = Arc::new(RendezvousRegistry::new(scope_id()));
    let authority = authority("discord:close-race", 1, 0x205);
    let bridge = registry
        .install(RelayLane::Bridge, authority.clone())
        .await
        .unwrap();

    let close_registry = Arc::clone(&registry);
    let install_registry = Arc::clone(&registry);
    let (loss, reconnect) = tokio::join!(
        close_registry.begin_connection_loss(&bridge),
        install_registry.install(RelayLane::Bridge, authority),
    );

    assert!(matches!(loss, RelayConnectionLoss::ContainmentRequired(_)));
    assert!(matches!(
        reconnect,
        Err(RendezvousInstallError::LaneOccupied | RendezvousInstallError::Quiescing)
    ));
}

#[tokio::test]
async fn stale_completion_cannot_remove_a_replacement_generation() {
    let registry = RendezvousRegistry::new(scope_id());
    let old_authority = authority("discord:stale-completion", 1, 0x206);
    let old_bridge = registry
        .install(RelayLane::Bridge, old_authority)
        .await
        .unwrap();
    let old_ticket = match registry.begin_connection_loss(&old_bridge).await {
        RelayConnectionLoss::ContainmentRequired(ticket) => *ticket,
        other => panic!("expected containment ticket, got {other:?}"),
    };
    assert_eq!(
        registry.complete_containment(&old_ticket).await,
        RelayContainmentCompletion::Removed
    );

    let new_authority = authority("discord:stale-completion", 2, 0x207);
    let new_bridge = registry
        .install(RelayLane::Bridge, new_authority)
        .await
        .unwrap();
    assert_eq!(
        registry.complete_containment(&old_ticket).await,
        RelayContainmentCompletion::StaleTicket
    );
    assert_eq!(
        registry.route_target(&new_bridge).await,
        Err(RendezvousRouteError::AwaitingPeer)
    );
    assert_eq!(
        registry.begin_connection_loss(&old_bridge).await,
        RelayConnectionLoss::StaleConnection
    );
}

#[tokio::test]
async fn wrong_scope_is_rejected_before_any_session_slot_is_created() {
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

    assert_eq!(
        registry.install(RelayLane::Bridge, wrong).await,
        Err(RendezvousInstallError::ScopeMismatch)
    );
}
