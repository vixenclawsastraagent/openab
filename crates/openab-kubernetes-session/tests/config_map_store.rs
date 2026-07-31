#![cfg(feature = "controller")]

use chrono::{Duration, TimeZone, Utc};
use http::{Method, Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use openab_kubernetes_session::store::{
    AnchorStoreError, ConfigMapAnchorStore, DeleteOutcome, StoreOperation, WriteRecovery,
};
use serde_json::{json, Value};
use std::time::Duration as StdDuration;
use tower_test::mock;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const RAW_SESSION_KEY: &str = "discord:customer-secret-thread-123";

fn scope_id() -> ScopeId {
    ScopeId::derive(RAW_SCOPE)
}

fn session_id() -> SessionId {
    SessionId::derive(RAW_SCOPE, RAW_SESSION_KEY)
}

fn test_anchor(session_id: SessionId, scope_id: ScopeId) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 7, 31, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        session_id,
        scope_id,
        ProfileRef::new("codex-strict", "sha256-abc123").unwrap(),
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap()
}

fn config_map(
    anchor: &SessionAnchorV1,
    uid: Option<&str>,
    resource_version: Option<&str>,
) -> Value {
    let mut metadata = json!({
        "name": ResourceNames::new(anchor.session_id()).anchor(),
        "namespace": NAMESPACE,
        "labels": {
            "app.kubernetes.io/managed-by": "openab-session-controller",
            "openab.dev/resource": "session-anchor"
        }
    });
    if let Some(uid) = uid {
        metadata["uid"] = json!(uid);
    }
    if let Some(resource_version) = resource_version {
        metadata["resourceVersion"] = json!(resource_version);
    }
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
        "data": {
            "anchor.json": serde_json::to_string(anchor).unwrap()
        }
    })
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn failure_response(status: StatusCode, reason: &str) -> Response<Body> {
    json_response(
        status,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": reason,
            "reason": reason,
            "code": status.as_u16()
        }),
    )
}

async fn request_body(request: Request<Body>) -> Value {
    let bytes = request.into_body().collect_bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn store_rejects_invalid_namespace() {
    let (service, _handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");

    assert!(matches!(
        ConfigMapAnchorStore::new(client, "INVALID_NAMESPACE", scope_id()),
        Err(AnchorStoreError::InvalidNamespace { .. })
    ));
}

#[tokio::test]
async fn create_rejects_another_scope_without_an_api_request() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let other_scope = ScopeId::derive("another-team");
    let anchor = test_anchor(
        SessionId::derive("another-team", RAW_SESSION_KEY),
        other_scope,
    );

    assert!(matches!(
        store.create(&anchor).await,
        Err(AnchorStoreError::ScopeMismatch { .. })
    ));

    let mut handle = std::pin::pin!(handle);
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn create_posts_one_opaque_anchor_and_returns_observation_metadata() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let expected_state = anchor.clone();
    let store_for_task = store.clone();
    let create_task = tokio::spawn(async move { store_for_task.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("create request");
    assert_eq!(request.method(), Method::POST);
    assert_eq!(
        request.uri().path(),
        format!("/api/v1/namespaces/{NAMESPACE}/configmaps")
    );
    let mut body = request_body(request).await;
    assert_eq!(
        body["metadata"]["name"],
        ResourceNames::new(session_id()).anchor()
    );
    assert_eq!(body["metadata"]["namespace"], NAMESPACE);
    assert_eq!(
        body["metadata"]["labels"]["app.kubernetes.io/managed-by"],
        "openab-session-controller"
    );
    assert_eq!(
        body["metadata"]["labels"]["openab.dev/resource"],
        "session-anchor"
    );
    assert_eq!(body["data"].as_object().unwrap().len(), 1);
    let serialized_request = serde_json::to_string(&body).unwrap();
    assert!(!serialized_request.contains(RAW_SCOPE));
    assert!(!serialized_request.contains(RAW_SESSION_KEY));

    body["metadata"]["uid"] = json!("uid-a");
    body["metadata"]["resourceVersion"] = json!("rv-10");
    send.send_response(json_response(StatusCode::CREATED, body));

    let stored = create_task.await.unwrap().unwrap();
    assert_eq!(stored.state(), &expected_state);
    assert_eq!(stored.uid(), "uid-a");
    assert_eq!(stored.resource_version(), "rv-10");
    assert_eq!(stored.namespace(), NAMESPACE);
}

#[tokio::test]
async fn create_maps_already_exists_without_adopting_the_object() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let name = ResourceNames::new(session_id()).anchor();
    let create_task = tokio::spawn(async move { store.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("create request");
    send.send_response(failure_response(StatusCode::CONFLICT, "AlreadyExists"));

    assert!(matches!(
        create_task.await.unwrap(),
        Err(AnchorStoreError::AlreadyExists { name: actual }) if actual == name
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn get_returns_none_for_a_missing_anchor() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let get_task = tokio::spawn(async move { store.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("get request");
    assert_eq!(request.method(), Method::GET);
    assert_eq!(
        request.uri().path(),
        format!(
            "/api/v1/namespaces/{NAMESPACE}/configmaps/{}",
            ResourceNames::new(session_id()).anchor()
        )
    );
    send.send_response(failure_response(StatusCode::NOT_FOUND, "NotFound"));

    assert!(get_task.await.unwrap().unwrap().is_none());
}

#[tokio::test]
async fn get_rejects_a_truncated_name_collision() {
    let requested = session_id();
    let mut colliding_hex = requested.as_hex();
    let replacement = if colliding_hex.ends_with('0') {
        "1"
    } else {
        "0"
    };
    colliding_hex.replace_range(63..64, replacement);
    let colliding_session: SessionId =
        serde_json::from_value(Value::String(colliding_hex)).unwrap();
    assert_eq!(
        ResourceNames::new(requested).anchor(),
        ResourceNames::new(colliding_session).anchor()
    );

    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let get_task = tokio::spawn(async move { store.get(requested).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(colliding_session, scope_id()),
            Some("uid-collision"),
            Some("rv-10"),
        ),
    ));

    assert!(matches!(
        get_task.await.unwrap(),
        Err(AnchorStoreError::SessionMismatch { .. })
    ));
}

#[tokio::test]
async fn get_rejects_an_anchor_from_another_bound_scope() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let get_task = tokio::spawn(async move { store.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), ScopeId::derive("another-team")),
            Some("uid-other"),
            Some("rv-10"),
        ),
    ));

    assert!(matches!(
        get_task.await.unwrap(),
        Err(AnchorStoreError::ScopeMismatch { .. })
    ));
}

#[tokio::test]
async fn get_rejects_missing_observation_metadata() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let get_task = tokio::spawn(async move { store.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&test_anchor(session_id(), scope_id()), None, Some("rv-10")),
    ));

    assert!(matches!(
        get_task.await.unwrap(),
        Err(AnchorStoreError::MalformedObject { .. })
    ));
}

#[tokio::test]
async fn replace_uses_uid_and_resource_version_as_cas_observation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    let mut observed_object = config_map(
        &test_anchor(session_id(), scope_id()),
        Some("uid-a"),
        Some("opaque-rv-10"),
    );
    observed_object["metadata"]["annotations"] = json!({"third-party.example/note": "preserve-me"});
    send.send_response(json_response(StatusCode::OK, observed_object));
    let stored = get_task.await.unwrap().unwrap().unwrap();

    let mut next = stored.state().clone();
    let fence = next.fence().clone();
    let activity = next.last_activity_at() + Duration::minutes(2);
    next.refresh_activity(
        &fence,
        activity,
        activity + Duration::minutes(15),
        activity + Duration::hours(72),
    )
    .unwrap();
    let expected_state = next.clone();
    let store_for_replace = store.clone();
    let replace_task = tokio::spawn(async move { store_for_replace.replace(&stored, &next).await });

    let (request, send) = handle.next_request().await.expect("replace request");
    assert_eq!(request.method(), Method::PUT);
    let mut body = request_body(request).await;
    assert_eq!(body["metadata"]["uid"], "uid-a");
    assert_eq!(body["metadata"]["resourceVersion"], "opaque-rv-10");
    assert_eq!(
        body["metadata"]["annotations"]["third-party.example/note"],
        "preserve-me"
    );
    body["metadata"]["resourceVersion"] = json!("opaque-rv-11");
    send.send_response(json_response(StatusCode::OK, body));

    let replaced = replace_task.await.unwrap().unwrap();
    assert_eq!(replaced.state(), &expected_state);
    assert_eq!(replaced.uid(), "uid-a");
    assert_eq!(replaced.resource_version(), "opaque-rv-11");
}

#[tokio::test]
async fn stale_replace_maps_conflict_without_retry() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-a"),
            Some("rv-stale"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let next = stored.state().clone();
    let store_for_replace = store.clone();
    let replace_task = tokio::spawn(async move { store_for_replace.replace(&stored, &next).await });

    let (_request, send) = handle.next_request().await.expect("replace request");
    send.send_response(failure_response(StatusCode::CONFLICT, "Conflict"));

    assert!(matches!(
        replace_task.await.unwrap(),
        Err(AnchorStoreError::Conflict {
            operation: StoreOperation::Replace,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn delete_sends_uid_and_resource_version_preconditions() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-a"),
            Some("opaque-rv-10"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let delete_task = tokio::spawn(async move { store.delete(&stored).await });

    let (request, send) = handle.next_request().await.expect("delete request");
    assert_eq!(request.method(), Method::DELETE);
    let body = request_body(request).await;
    assert_eq!(body["preconditions"]["uid"], "uid-a");
    assert_eq!(body["preconditions"]["resourceVersion"], "opaque-rv-10");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    ));

    assert_eq!(
        delete_task.await.unwrap().unwrap(),
        DeleteOutcome::Requested
    );
}

#[tokio::test]
async fn delete_response_with_a_finalizer_is_only_a_request_to_delete() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let response_anchor = anchor.clone();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&anchor, Some("uid-a"), Some("rv-10")),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let delete_task = tokio::spawn(async move { store.delete(&stored).await });

    let (_request, send) = handle.next_request().await.expect("delete request");
    let mut terminating = config_map(&response_anchor, Some("uid-a"), Some("rv-11"));
    terminating["metadata"]["deletionTimestamp"] = json!("2026-07-31T08:01:00Z");
    terminating["metadata"]["finalizers"] = json!(["admission.example.test/hold"]);
    send.send_response(json_response(StatusCode::ACCEPTED, terminating));

    assert_eq!(
        delete_task.await.unwrap().unwrap(),
        DeleteOutcome::Requested
    );
}

#[tokio::test]
async fn get_rejects_an_anchor_that_is_already_terminating() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let get_task = tokio::spawn(async move { store.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    let mut object = config_map(
        &test_anchor(session_id(), scope_id()),
        Some("uid-a"),
        Some("rv-10"),
    );
    object["metadata"]["deletionTimestamp"] = json!("2026-07-31T08:01:00Z");
    send.send_response(json_response(StatusCode::OK, object));

    assert!(matches!(
        get_task.await.unwrap(),
        Err(AnchorStoreError::MalformedObject { .. })
    ));
}

#[tokio::test]
async fn get_rejects_unexpected_anchor_lifecycle_metadata() {
    for (field, value) in [
        (
            "ownerReferences",
            json!([{
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "name": "unexpected-owner",
                "uid": "owner-uid"
            }]),
        ),
        ("finalizers", json!(["unexpected.example/finalizer"])),
    ] {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
        let get_task = tokio::spawn(async move { store.get(session_id()).await });

        let mut handle = std::pin::pin!(handle);
        let (_request, send) = handle.next_request().await.expect("get request");
        let mut object = config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-a"),
            Some("rv-10"),
        );
        object["metadata"][field] = value;
        send.send_response(json_response(StatusCode::OK, object));

        assert!(matches!(
            get_task.await.unwrap(),
            Err(AnchorStoreError::MalformedObject { .. })
        ));
    }
}

#[tokio::test]
async fn replace_rejects_immutable_profile_changes_without_an_api_request() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-a"),
            Some("rv-10"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let mut next = serde_json::to_value(stored.state()).unwrap();
    next["profile"]["version"] = json!("sha256-another-profile");
    let next: SessionAnchorV1 = serde_json::from_value(next).unwrap();

    assert!(matches!(
        store.replace(&stored, &next).await,
        Err(AnchorStoreError::ImmutableFieldChanged { field: "profile" })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn replace_rejects_a_same_name_object_with_a_new_uid() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-old"),
            Some("rv-10"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let next = stored.state().clone();
    let store_for_replace = store.clone();
    let replace_task = tokio::spawn(async move { store_for_replace.replace(&stored, &next).await });

    let (request, send) = handle.next_request().await.expect("replace request");
    let mut body = request_body(request).await;
    body["metadata"]["uid"] = json!("uid-new");
    body["metadata"]["resourceVersion"] = json!("rv-11");
    send.send_response(json_response(StatusCode::OK, body));

    assert!(matches!(
        replace_task.await.unwrap(),
        Err(AnchorStoreError::WriteRecoveryFailed {
            operation: StoreOperation::Replace,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn delete_is_idempotent_when_the_observed_anchor_is_absent() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-old"),
            Some("rv-10"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let delete_task = tokio::spawn(async move { store.delete(&stored).await });

    let (_request, send) = handle.next_request().await.expect("delete request");
    send.send_response(failure_response(StatusCode::NOT_FOUND, "NotFound"));

    assert_eq!(
        delete_task.await.unwrap().unwrap(),
        DeleteOutcome::AlreadyAbsent
    );
}

#[tokio::test]
async fn stale_delete_maps_conflict_without_retrying_a_new_object() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(
            &test_anchor(session_id(), scope_id()),
            Some("uid-old"),
            Some("rv-stale"),
        ),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let delete_task = tokio::spawn(async move { store.delete(&stored).await });

    let (_request, send) = handle.next_request().await.expect("delete request");
    send.send_response(failure_response(StatusCode::CONFLICT, "Conflict"));

    assert!(matches!(
        delete_task.await.unwrap(),
        Err(AnchorStoreError::Conflict {
            operation: StoreOperation::Delete,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn replace_rejects_forged_fence_successors_before_put() {
    let mut current = serde_json::to_value(test_anchor(session_id(), scope_id())).unwrap();
    current["fence"]["generation"] = json!(2);
    current["fence"]["attemptId"] = json!(Uuid::from_u128(20));
    let current: SessionAnchorV1 = serde_json::from_value(current).unwrap();

    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&current, Some("uid-a"), Some("rv-10")),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();

    for (generation, attempt) in [
        (2, Uuid::from_u128(21)),
        (1, Uuid::from_u128(22)),
        (4, Uuid::from_u128(23)),
    ] {
        let mut forged = serde_json::to_value(stored.state()).unwrap();
        forged["fence"]["generation"] = json!(generation);
        forged["fence"]["attemptId"] = json!(attempt);
        let forged: SessionAnchorV1 = serde_json::from_value(forged).unwrap();

        assert!(matches!(
            store.replace(&stored, &forged).await,
            Err(AnchorStoreError::InvalidSuccessor { .. })
        ));
    }

    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn create_guardedly_deletes_an_admission_mutated_anchor() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let store_for_create = store.clone();
    let create_task = tokio::spawn(async move { store_for_create.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("create request");
    let mut response = request_body(request).await;
    let mut mutated: Value =
        serde_json::from_str(response["data"]["anchor.json"].as_str().unwrap()).unwrap();
    mutated["profile"]["version"] = json!("sha256-mutated");
    response["data"]["anchor.json"] = json!(serde_json::to_string(&mutated).unwrap());
    response["metadata"]["uid"] = json!("uid-mutated");
    response["metadata"]["resourceVersion"] = json!("rv-10");
    send.send_response(json_response(StatusCode::CREATED, response));

    let (request, send) = handle
        .next_request()
        .await
        .expect("guarded cleanup request");
    assert_eq!(request.method(), Method::DELETE);
    let cleanup = request_body(request).await;
    assert_eq!(cleanup["preconditions"]["uid"], "uid-mutated");
    assert_eq!(cleanup["preconditions"]["resourceVersion"], "rv-10");
    send.send_response(json_response(
        StatusCode::OK,
        json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 200
        }),
    ));

    assert!(matches!(
        create_task.await.unwrap(),
        Err(AnchorStoreError::WriteResponseRejected {
            operation: StoreOperation::Create,
            recovery: WriteRecovery::GuardedDeleteRequested,
            ..
        })
    ));
}

#[tokio::test]
async fn rejected_create_records_when_guarded_cleanup_observes_absence() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let create_task = tokio::spawn(async move { store.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("create request");
    let mut response = request_body(request).await;
    response["metadata"]["labels"]["openab.dev/resource"] = json!("mutated");
    response["metadata"]["uid"] = json!("uid-mutated");
    response["metadata"]["resourceVersion"] = json!("rv-10");
    send.send_response(json_response(StatusCode::CREATED, response));

    let (_request, send) = handle
        .next_request()
        .await
        .expect("guarded cleanup request");
    send.send_response(failure_response(StatusCode::NOT_FOUND, "NotFound"));

    assert!(matches!(
        create_task.await.unwrap(),
        Err(AnchorStoreError::WriteResponseRejected {
            operation: StoreOperation::Create,
            recovery: WriteRecovery::GuardedDeleteObservedAbsent,
            ..
        })
    ));
}

#[tokio::test]
async fn create_refuses_to_remove_foreign_lifecycle_metadata() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let create_task = tokio::spawn(async move { store.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("create request");
    let mut response = request_body(request).await;
    response["metadata"]["uid"] = json!("uid-finalized");
    response["metadata"]["resourceVersion"] = json!("rv-10");
    response["metadata"]["finalizers"] = json!(["admission.example.test/hold"]);
    response["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "example.test/v1",
        "kind": "ForeignOwner",
        "name": "foreign-owner",
        "uid": "foreign-owner-uid"
    }]);
    send.send_response(json_response(StatusCode::CREATED, response));

    assert!(matches!(
        create_task.await.unwrap(),
        Err(AnchorStoreError::WriteRecoveryFailed {
            operation: StoreOperation::Create,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn rejected_create_surfaces_guarded_cleanup_conflict_without_retry() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let anchor = test_anchor(session_id(), scope_id());
    let create_task = tokio::spawn(async move { store.create(&anchor).await });

    let mut handle = std::pin::pin!(handle);
    let (request, send) = handle.next_request().await.expect("create request");
    let mut response = request_body(request).await;
    response["metadata"]["labels"]["openab.dev/resource"] = json!("mutated");
    response["metadata"]["uid"] = json!("uid-mutated");
    response["metadata"]["resourceVersion"] = json!("rv-10");
    send.send_response(json_response(StatusCode::CREATED, response));

    let (_request, send) = handle
        .next_request()
        .await
        .expect("guarded cleanup request");
    send.send_response(failure_response(StatusCode::CONFLICT, "Conflict"));

    assert!(matches!(
        create_task.await.unwrap(),
        Err(AnchorStoreError::WriteRecoveryFailed {
            operation: StoreOperation::Create,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}

#[tokio::test]
async fn replace_repairs_an_admission_mutation_without_regressing_generation() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let mut original = test_anchor(session_id(), scope_id());
    let original_fence = original.fence().clone();
    original
        .transition(&original_fence, SessionPhase::Blocked)
        .unwrap();
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&original, Some("uid-a"), Some("rv-10")),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let previous_generation = stored.state().fence().generation();

    let mut next = stored.state().clone();
    let fence = next.fence().clone();
    let activity = next.last_activity_at() + Duration::minutes(20);
    next.advance_generation(
        &fence,
        Uuid::from_u128(3),
        activity,
        activity + Duration::minutes(15),
        activity + Duration::hours(72),
    )
    .unwrap();
    let expected_repair = next.clone();
    assert_eq!(
        expected_repair.fence().generation(),
        previous_generation + 1
    );
    let store_for_replace = store.clone();
    let replace_task = tokio::spawn(async move { store_for_replace.replace(&stored, &next).await });

    let (request, send) = handle.next_request().await.expect("replace request");
    let mut mutated_response = request_body(request).await;
    mutated_response["metadata"]["labels"]["openab.dev/resource"] = json!("admission-mutated");
    mutated_response["metadata"]["resourceVersion"] = json!("rv-11");
    mutated_response["metadata"]["annotations"] = json!({
        "admission.example.test/revision": "7"
    });
    send.send_response(json_response(StatusCode::OK, mutated_response));

    let (request, send) = handle
        .next_request()
        .await
        .expect("guarded canonical repair request");
    assert_eq!(request.method(), Method::PUT);
    let mut repair = request_body(request).await;
    assert_eq!(repair["metadata"]["uid"], "uid-a");
    assert_eq!(repair["metadata"]["resourceVersion"], "rv-11");
    assert_eq!(
        repair["metadata"]["annotations"]["admission.example.test/revision"],
        "7"
    );
    let repair_state: SessionAnchorV1 =
        serde_json::from_str(repair["data"]["anchor.json"].as_str().unwrap()).unwrap();
    assert_eq!(repair_state, expected_repair);
    assert_ne!(repair_state.fence().generation(), previous_generation);
    repair["metadata"]["resourceVersion"] = json!("rv-12");
    send.send_response(json_response(StatusCode::OK, repair));

    let repaired = replace_task.await.unwrap().unwrap();
    assert_eq!(repaired.state(), &expected_repair);
    assert_eq!(repaired.uid(), "uid-a");
    assert_eq!(repaired.resource_version(), "rv-12");
}

#[tokio::test]
async fn replace_never_repairs_an_unexpected_persisted_fence() {
    for (generation, attempt_id) in [(3, Uuid::from_u128(4)), (2, Uuid::from_u128(5))] {
        let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(service, "default");
        let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
        let mut original = test_anchor(session_id(), scope_id());
        let original_fence = original.fence().clone();
        original
            .transition(&original_fence, SessionPhase::Blocked)
            .unwrap();
        let store_for_get = store.clone();
        let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

        let mut handle = std::pin::pin!(handle);
        let (_request, send) = handle.next_request().await.expect("get request");
        send.send_response(json_response(
            StatusCode::OK,
            config_map(&original, Some("uid-a"), Some("rv-10")),
        ));
        let stored = get_task.await.unwrap().unwrap().unwrap();
        let mut intended = stored.state().clone();
        let fence = intended.fence().clone();
        let activity = intended.last_activity_at() + Duration::minutes(20);
        intended
            .advance_generation(
                &fence,
                Uuid::from_u128(3),
                activity,
                activity + Duration::minutes(15),
                activity + Duration::hours(72),
            )
            .unwrap();
        let store_for_replace = store.clone();
        let replace_task =
            tokio::spawn(async move { store_for_replace.replace(&stored, &intended).await });

        let (request, send) = handle.next_request().await.expect("replace request");
        let mut response = request_body(request).await;
        let mut persisted: Value =
            serde_json::from_str(response["data"]["anchor.json"].as_str().unwrap()).unwrap();
        persisted["fence"]["generation"] = json!(generation);
        persisted["fence"]["attemptId"] = json!(attempt_id);
        response["data"]["anchor.json"] = json!(serde_json::to_string(&persisted).unwrap());
        response["metadata"]["resourceVersion"] = json!("rv-11");
        send.send_response(json_response(StatusCode::OK, response));

        assert!(matches!(
            replace_task.await.unwrap(),
            Err(AnchorStoreError::WriteRecoveryFailed {
                operation: StoreOperation::Replace,
                ..
            })
        ));
        let unexpected =
            tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
        assert!(!matches!(unexpected, Ok(Some(_))));
    }
}

#[tokio::test]
async fn replace_refuses_to_remove_a_foreign_finalizer() {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();
    let client = Client::new(service, "default");
    let store = ConfigMapAnchorStore::new(client, NAMESPACE, scope_id()).unwrap();
    let original = test_anchor(session_id(), scope_id());
    let store_for_get = store.clone();
    let get_task = tokio::spawn(async move { store_for_get.get(session_id()).await });

    let mut handle = std::pin::pin!(handle);
    let (_request, send) = handle.next_request().await.expect("get request");
    send.send_response(json_response(
        StatusCode::OK,
        config_map(&original, Some("uid-a"), Some("rv-10")),
    ));
    let stored = get_task.await.unwrap().unwrap().unwrap();
    let next = stored.state().clone();
    let store_for_replace = store.clone();
    let replace_task = tokio::spawn(async move { store_for_replace.replace(&stored, &next).await });

    let (request, send) = handle.next_request().await.expect("replace request");
    let mut response = request_body(request).await;
    response["metadata"]["resourceVersion"] = json!("rv-11");
    response["metadata"]["finalizers"] = json!(["admission.example.test/hold"]);
    send.send_response(json_response(StatusCode::OK, response));

    assert!(matches!(
        replace_task.await.unwrap(),
        Err(AnchorStoreError::WriteRecoveryFailed {
            operation: StoreOperation::Replace,
            ..
        })
    ));
    let unexpected =
        tokio::time::timeout(StdDuration::from_millis(20), handle.next_request()).await;
    assert!(!matches!(unexpected, Ok(Some(_))));
}
