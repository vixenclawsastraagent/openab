#![cfg(feature = "controller")]

use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::controller::{
    ActivityCoordinator, ActivityError, ActivityEvent, ActivityOutcome, ActivityTurnId,
    SessionLocks,
};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::ControllerPolicy;
use openab_kubernetes_session::state::{Fence, ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::{AnchorStoreError, ConfigMapAnchorStore, StoreOperation};
use serde_json::{json, Value};
use std::time::Duration as StdDuration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const POD_UID: &str = "worker-pod-uid";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn session_id(label: &str) -> SessionId {
    SessionId::derive(RAW_SCOPE, label)
}

fn policy() -> ControllerPolicy {
    ControllerPolicy::new(15 * 60, 72 * 60 * 60, 20).unwrap()
}

fn fixed_turn_id(value: u128) -> ActivityTurnId {
    ActivityTurnId::from_uuid(Uuid::from_u128(value)).unwrap()
}

fn anchor_in_phase(phase: SessionPhase) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    let mut anchor = SessionAnchorV1::new(
        session_id("discord:activity"),
        scope_id(),
        ProfileRef::new("codex-strict", "2026-08-01").unwrap(),
        Uuid::from_u128(0x100),
        Uuid::from_u128(0x200),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap();
    let fence = anchor.fence().clone();
    anchor.observe_pod(&fence, POD_UID).unwrap();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    if phase == SessionPhase::Busy {
        anchor.transition(&fence, SessionPhase::Busy).unwrap();
    }
    anchor
}

fn with_activity(
    mut anchor: SessionAnchorV1,
    turn_id: ActivityTurnId,
    at: chrono::DateTime<Utc>,
) -> SessionAnchorV1 {
    let fence = anchor.fence().clone();
    anchor
        .record_prompt_started(
            &fence,
            turn_id.as_uuid(),
            at,
            at + Duration::minutes(15),
            at + Duration::hours(72),
        )
        .unwrap();
    anchor
}

fn with_completed_activity(
    anchor: SessionAnchorV1,
    turn_id: ActivityTurnId,
    started_at: chrono::DateTime<Utc>,
) -> SessionAnchorV1 {
    let mut anchor = with_activity(anchor, turn_id, started_at);
    let fence = anchor.fence().clone();
    let finished_at = started_at + Duration::minutes(1);
    anchor
        .record_prompt_finished(
            &fence,
            turn_id.as_uuid(),
            finished_at,
            finished_at + Duration::minutes(15),
            finished_at + Duration::hours(72),
        )
        .unwrap();
    anchor
}

fn binding(anchor: &SessionAnchorV1) -> SessionBinding {
    SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap()
}

fn config_map(anchor: &SessionAnchorV1, resource_version: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": ResourceNames::new(anchor.session_id()).anchor(),
            "namespace": NAMESPACE,
            "uid": format!("uid-{}", anchor.session_id().as_hex()),
            "resourceVersion": resource_version,
            "labels": {
                "app.kubernetes.io/managed-by": "openab-session-controller",
                "openab.dev/resource": "session-anchor"
            }
        },
        "data": { "anchor.json": serde_json::to_string(anchor).unwrap() }
    })
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn missing_response() -> Response<Body> {
    json_response(
        StatusCode::NOT_FOUND,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "NotFound",
            "code": 404
        }),
    )
}

fn new_coordinator() -> (
    ActivityCoordinator,
    mock::Handle<Request<Body>, Response<Body>>,
) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let store =
        ConfigMapAnchorStore::new(Client::new(service, "default"), NAMESPACE, scope_id()).unwrap();
    (
        ActivityCoordinator::new(store, SessionLocks::new(), policy()),
        handle,
    )
}

async fn request_body(request: Request<Body>) -> Value {
    let bytes = request.into_body().collect_bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn respond_get(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
    anchor: &SessionAnchorV1,
    resource_version: &str,
) {
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::GET);
    send.send_response(json_response(
        StatusCode::OK,
        config_map(anchor, resource_version),
    ));
}

async fn respond_replace(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) -> SessionAnchorV1 {
    let (request, send) = handle.next_request().await.unwrap();
    assert_eq!(request.method(), Method::PUT);
    let mut body = request_body(request).await;
    let anchor = serde_json::from_str(body["data"]["anchor.json"].as_str().unwrap()).unwrap();
    body["metadata"]["resourceVersion"] = json!("rv-written");
    send.send_response(json_response(StatusCode::OK, body));
    anchor
}

async fn assert_no_request(
    handle: &mut std::pin::Pin<&mut mock::Handle<Request<Body>, Response<Body>>>,
) {
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn prompt_start_records_busy_with_controller_policy_deadlines() {
    let anchor = anchor_in_phase(SessionPhase::Ready);
    let expected_binding = binding(&anchor);
    let turn_id = fixed_turn_id(0x400);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(&mut handle, &anchor, "rv-ready").await;
    let written = respond_replace(&mut handle).await;

    assert_eq!(task.await.unwrap().unwrap(), ActivityOutcome::Recorded);
    assert_eq!(written.phase(), SessionPhase::Busy);
    assert_eq!(written.last_prompt_turn_id(), Some(turn_id.as_uuid()));
    assert!(written.last_activity_at() > anchor.last_activity_at());
    assert_eq!(
        written.compute_deadline_at() - written.last_activity_at(),
        Duration::minutes(15)
    );
    assert_eq!(
        written.storage_deadline_at() - written.last_activity_at(),
        Duration::hours(72)
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn prompt_finish_records_ready_with_fresh_deadlines() {
    let ready = anchor_in_phase(SessionPhase::Ready);
    let turn_id = fixed_turn_id(0x401);
    let anchor = with_activity(
        ready.clone(),
        turn_id,
        ready.last_activity_at() + Duration::minutes(1),
    );
    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptFinished)
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(&mut handle, &anchor, "rv-busy").await;
    let written = respond_replace(&mut handle).await;

    assert_eq!(task.await.unwrap().unwrap(), ActivityOutcome::Recorded);
    assert_eq!(written.phase(), SessionPhase::Ready);
    assert_eq!(written.last_prompt_turn_id(), Some(turn_id.as_uuid()));
    assert!(written.last_activity_at() > anchor.last_activity_at());
    assert_eq!(
        written.compute_deadline_at() - written.last_activity_at(),
        Duration::minutes(15)
    );
    assert_eq!(
        written.storage_deadline_at() - written.last_activity_at(),
        Duration::hours(72)
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn exact_retry_is_idempotent_without_another_write() {
    let initial = anchor_in_phase(SessionPhase::Ready);
    let at = initial.last_activity_at() + Duration::minutes(2);
    let turn_id = fixed_turn_id(0x402);
    let anchor = with_activity(initial, turn_id, at);
    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(&mut handle, &anchor, "rv-busy").await;

    assert_eq!(
        task.await.unwrap().unwrap(),
        ActivityOutcome::AlreadyRecorded
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn finished_turn_retry_is_idempotent_and_old_start_does_not_reopen_it() {
    let initial = anchor_in_phase(SessionPhase::Ready);
    let turn_id = fixed_turn_id(0x40a);
    let anchor = with_completed_activity(
        initial.clone(),
        turn_id,
        initial.last_activity_at() + Duration::minutes(2),
    );
    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptFinished)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &anchor, "rv-ready").await;
    assert_eq!(
        task.await.unwrap().unwrap(),
        ActivityOutcome::AlreadyRecorded
    );
    assert_no_request(&mut handle).await;

    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &anchor, "rv-ready").await;
    assert_eq!(task.await.unwrap().unwrap(), ActivityOutcome::Stale);
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn legacy_busy_without_a_turn_fails_closed_on_finish() {
    let anchor = anchor_in_phase(SessionPhase::Busy);
    assert_eq!(anchor.last_prompt_turn_id(), None);
    let expected_binding = binding(&anchor);
    let turn_id = fixed_turn_id(0x40b);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptFinished)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &anchor, "rv-legacy-busy").await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, ActivityError::TurnMismatch));
    assert_eq!(
        error.to_string(),
        "the activity turn does not match the durable prompt"
    );
    assert!(!error.to_string().contains("00000000"));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn duplicate_cannot_pump_ttl_while_the_target_phase_is_active() {
    let initial = anchor_in_phase(SessionPhase::Ready);
    let recorded_at = initial.last_activity_at() + Duration::minutes(2);
    let turn_id = fixed_turn_id(0x403);
    let anchor = with_activity(initial, turn_id, recorded_at);
    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(&mut handle, &anchor, "rv-busy").await;

    assert_eq!(
        task.await.unwrap().unwrap(),
        ActivityOutcome::AlreadyRecorded
    );
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn delayed_old_finish_cannot_clear_a_newer_busy_turn() {
    let initial = anchor_in_phase(SessionPhase::Ready);
    let current_at = initial.last_activity_at() + Duration::minutes(5);
    let current_turn = fixed_turn_id(0x405);
    let stale_turn = fixed_turn_id(0x404);
    let anchor = with_activity(initial, current_turn, current_at);
    let expected_binding = binding(&anchor);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, stale_turn, ActivityEvent::PromptFinished)
            .await
    });
    let mut handle = std::pin::pin!(handle);

    respond_get(&mut handle, &anchor, "rv-newer-busy").await;

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivityError::TurnMismatch)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn wrong_scope_fails_before_kubernetes_and_stale_binding_never_writes() {
    let anchor = anchor_in_phase(SessionPhase::Ready);
    let turn_id = fixed_turn_id(0x406);
    let wrong_scope = SessionBinding::new(
        ScopeId::derive("another-team"),
        anchor.session_id(),
        anchor.fence().clone(),
        anchor.incarnation_id(),
    )
    .unwrap();
    let (coordinator, handle) = new_coordinator();
    assert!(matches!(
        coordinator
            .record(&wrong_scope, turn_id, ActivityEvent::PromptStarted)
            .await,
        Err(ActivityError::ScopeMismatch)
    ));
    let mut handle = std::pin::pin!(handle);
    assert_no_request(&mut handle).await;

    let stale = SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        Fence::new(2, Uuid::from_u128(0x300)).unwrap(),
        anchor.incarnation_id(),
    )
    .unwrap();
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&stale, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &anchor, "rv-ready").await;

    assert!(matches!(
        task.await.unwrap(),
        Err(ActivityError::StaleBinding)
    ));
    assert_no_request(&mut handle).await;

    let stale = SessionBinding::new(
        anchor.scope_id(),
        anchor.session_id(),
        anchor.fence().clone(),
        Uuid::from_u128(0x999),
    )
    .unwrap();
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&stale, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &anchor, "rv-ready").await;
    assert!(matches!(
        task.await.unwrap(),
        Err(ActivityError::StaleBinding)
    ));
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn phase_rejection_and_cas_conflict_are_never_reported_as_activity() {
    let mut suspended = anchor_in_phase(SessionPhase::Ready);
    let fence = suspended.fence().clone();
    suspended
        .transition(&fence, SessionPhase::Suspending)
        .unwrap();
    suspended.confirm_pod_deleted(&fence, POD_UID).unwrap();
    suspended
        .transition(&fence, SessionPhase::Suspended)
        .unwrap();
    let expected_binding = binding(&suspended);
    let turn_id = fixed_turn_id(0x407);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &suspended, "rv-suspended").await;
    assert!(matches!(
        task.await.unwrap(),
        Err(ActivityError::PhaseRejected)
    ));
    assert_no_request(&mut handle).await;

    let ready = anchor_in_phase(SessionPhase::Ready);
    let expected_binding = binding(&ready);
    let turn_id = fixed_turn_id(0x408);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    respond_get(&mut handle, &ready, "rv-ready").await;
    let (replace, send) = handle.next_request().await.unwrap();
    assert_eq!(replace.method(), Method::PUT);
    send.send_response(json_response(
        StatusCode::CONFLICT,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "Conflict",
            "code": 409
        }),
    ));

    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        ActivityError::Store(AnchorStoreError::Conflict {
            operation: StoreOperation::Replace,
            ..
        })
    ));
    assert_eq!(error.to_string(), "activity anchor persistence failed");
    assert_no_request(&mut handle).await;
}

#[tokio::test]
async fn absent_anchor_and_nil_turn_fail_without_mutation() {
    let anchor = anchor_in_phase(SessionPhase::Ready);
    let expected_binding = binding(&anchor);
    let turn_id = fixed_turn_id(0x409);
    let (coordinator, handle) = new_coordinator();
    let task = tokio::spawn(async move {
        coordinator
            .record(&expected_binding, turn_id, ActivityEvent::PromptStarted)
            .await
    });
    let mut handle = std::pin::pin!(handle);
    let (get, send) = handle.next_request().await.unwrap();
    assert_eq!(get.method(), Method::GET);
    send.send_response(missing_response());
    assert!(matches!(
        task.await.unwrap(),
        Err(ActivityError::AnchorNotFound)
    ));
    assert_no_request(&mut handle).await;

    assert!(ActivityTurnId::from_uuid(Uuid::nil()).is_err());
}
