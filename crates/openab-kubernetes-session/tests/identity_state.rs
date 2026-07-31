use chrono::{Duration, TimeZone, Utc};
use openab_kubernetes_session::identity::{IdentityError, ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::state::{
    Fence, ProfileRef, SessionAnchorV1, SessionPhase, StateError,
};
use uuid::Uuid;

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn test_anchor() -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 7, 31, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        SessionId::derive("team-a", "discord:thread-123"),
        ScopeId::derive("team-a"),
        ProfileRef::new("codex-strict", "sha256-abc123").unwrap(),
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap()
}

#[test]
fn scoped_session_identity_is_stable_and_partitioned() {
    let first = SessionId::derive("team-a", "discord:thread-123");
    let repeated = SessionId::derive("team-a", "discord:thread-123");
    let other_scope = SessionId::derive("team-b", "discord:thread-123");
    let other_thread = SessionId::derive("team-a", "discord:thread-456");

    assert_eq!(first, repeated);
    assert_ne!(first, other_scope);
    assert_ne!(first, other_thread);
    assert_eq!(first.as_hex().len(), 64);
}

#[test]
fn scoped_identity_derivation_matches_stable_vectors() {
    assert_eq!(
        ScopeId::derive("team-a").as_hex(),
        "c7d126d05da76b40b912226a894e8acdc3c4f80d9b0f14f8f24a782ab0e61d67"
    );
    assert_eq!(
        SessionId::derive("team-a", "discord:thread-123").as_hex(),
        "a225a30857eafb0ea23ce8642aa1649a8ca77a14cb6c4ad0117058c74aaa9b2c"
    );
}

#[test]
fn resource_names_are_dns_safe_and_do_not_leak_chat_identity() {
    let raw_thread = "discord:customer-secret-thread-123";
    let names = ResourceNames::new(SessionId::derive("team-a", raw_thread));
    let rendered = [
        names.anchor(),
        names.pvc(),
        names.pod(1).unwrap(),
        names.registration_secret(1).unwrap(),
        names.service_account(1).unwrap(),
    ];

    for name in rendered {
        assert!(dns_label(&name), "invalid Kubernetes name: {name}");
        assert!(!name.contains("discord"));
        assert!(!name.contains("customer"));
        assert!(!name.contains("thread"));
    }
}

#[test]
fn generation_resource_names_reject_zero_and_fit_at_the_u64_limit() {
    let names = ResourceNames::new(SessionId::derive("team-a", "discord:thread-123"));

    assert_eq!(names.pod(0), Err(IdentityError::InvalidGeneration));
    let largest = names.pod(u64::MAX).unwrap();
    assert!(dns_label(&largest));
    assert!(largest.len() <= 63);
}

#[test]
fn digest_serialization_is_canonical_and_fail_closed() {
    let session = SessionId::derive("team-a", "discord:thread-123");
    let encoded = serde_json::to_string(&session).unwrap();
    assert_eq!(encoded.len(), 66);
    assert_eq!(
        serde_json::from_str::<SessionId>(&encoded).unwrap(),
        session
    );

    assert!(serde_json::from_str::<SessionId>(r#""abc""#).is_err());
    assert!(serde_json::from_str::<SessionId>(
        r#""AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA""#
    )
    .is_err());
}

#[test]
fn anchor_v1_round_trips_without_raw_scope_or_thread() {
    let anchor = test_anchor();
    let encoded = serde_json::to_string(&anchor).unwrap();
    let decoded: SessionAnchorV1 = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, anchor);
    assert!(!encoded.contains("team-a"));
    assert!(!encoded.contains("discord"));
    assert!(!encoded.contains("thread-123"));
}

#[test]
fn unknown_anchor_schema_version_is_rejected() {
    let anchor = test_anchor();
    let mut encoded = serde_json::to_value(anchor).unwrap();
    encoded["schemaVersion"] = serde_json::json!(2);

    assert!(serde_json::from_value::<SessionAnchorV1>(encoded).is_err());
}

#[test]
fn standalone_state_values_are_validated_when_deserialized() {
    assert!(
        serde_json::from_str::<ProfileRef>(r#"{"name":"INVALID","version":"sha256-abc123"}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<ProfileRef>(r#"{"name":"codex-strict","version":""}"#).is_err());
    assert!(serde_json::from_str::<Fence>(
        r#"{"generation":1,"attemptId":"00000000-0000-0000-0000-000000000000"}"#
    )
    .is_err());
}

#[test]
fn stale_fence_cannot_mutate_anchor() {
    let mut anchor = test_anchor();
    let stale = Fence::new(1, Uuid::from_u128(99)).unwrap();

    assert!(matches!(
        anchor.observe_pod(&stale, "pod-uid-a"),
        Err(StateError::FenceMismatch { .. })
    ));
}

#[test]
fn phase_transitions_require_the_expected_runtime_state() {
    let mut anchor = test_anchor();
    let fence = anchor.fence().clone();

    assert!(matches!(
        anchor.transition(&fence, SessionPhase::Ready),
        Err(StateError::MissingPodUid)
    ));

    anchor.observe_pod(&fence, "pod-uid-a").unwrap();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    anchor.transition(&fence, SessionPhase::Busy).unwrap();

    assert!(matches!(
        anchor.transition(&fence, SessionPhase::Provisioning),
        Err(StateError::InvalidTransition { .. })
    ));
    assert!(matches!(
        anchor.confirm_pod_deleted(&fence, "pod-uid-a"),
        Err(StateError::InvalidTransition { .. })
    ));
}

#[test]
fn replacement_waits_for_the_observed_old_pod_uid() {
    let mut anchor = test_anchor();
    let first_fence = anchor.fence().clone();
    let replacement_activity = anchor.last_activity_at() + Duration::minutes(20);
    let replacement_compute_deadline = replacement_activity + Duration::minutes(15);
    let replacement_storage_deadline = replacement_activity + Duration::hours(72);
    anchor.observe_pod(&first_fence, "pod-uid-a").unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Ready)
        .unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Suspending)
        .unwrap();

    assert!(matches!(
        anchor.advance_generation(
            &first_fence,
            Uuid::from_u128(3),
            replacement_activity,
            replacement_compute_deadline,
            replacement_storage_deadline,
        ),
        Err(StateError::PodStillPresent { .. })
    ));
    assert!(matches!(
        anchor.confirm_pod_deleted(&first_fence, "pod-uid-b"),
        Err(StateError::PodUidMismatch { .. })
    ));

    anchor
        .confirm_pod_deleted(&first_fence, "pod-uid-a")
        .unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Suspended)
        .unwrap();
    assert!(matches!(
        anchor.transition(&first_fence, SessionPhase::Provisioning),
        Err(StateError::InvalidTransition { .. })
    ));
    let second_fence = anchor
        .advance_generation(
            &first_fence,
            Uuid::from_u128(3),
            replacement_activity,
            replacement_compute_deadline,
            replacement_storage_deadline,
        )
        .unwrap();

    assert_eq!(second_fence.generation(), 2);
    assert_eq!(second_fence.attempt_id(), Uuid::from_u128(3));
    assert_eq!(anchor.phase(), SessionPhase::Provisioning);
    assert!(anchor.pod_uid().is_none());
}

#[test]
fn activity_refresh_is_fenced_monotonic_and_phase_aware() {
    let mut anchor = test_anchor();
    let fence = anchor.fence().clone();
    let original_activity = anchor.last_activity_at();
    let refreshed_activity = original_activity + Duration::minutes(2);
    let refreshed_compute_deadline = refreshed_activity + Duration::minutes(15);
    let refreshed_storage_deadline = refreshed_activity + Duration::hours(72);

    anchor
        .refresh_activity(
            &fence,
            refreshed_activity,
            refreshed_compute_deadline,
            refreshed_storage_deadline,
        )
        .unwrap();

    assert_eq!(anchor.last_activity_at(), refreshed_activity);
    assert_eq!(anchor.compute_deadline_at(), refreshed_compute_deadline);
    assert_eq!(anchor.storage_deadline_at(), refreshed_storage_deadline);
    assert!(matches!(
        anchor.refresh_activity(
            &fence,
            original_activity,
            refreshed_compute_deadline,
            refreshed_storage_deadline,
        ),
        Err(StateError::ActivityTimeRegression { .. })
    ));

    anchor.observe_pod(&fence, "pod-uid-a").unwrap();
    anchor.transition(&fence, SessionPhase::Ready).unwrap();
    anchor.transition(&fence, SessionPhase::Suspending).unwrap();

    assert!(matches!(
        anchor.refresh_activity(
            &fence,
            refreshed_activity + Duration::minutes(1),
            refreshed_compute_deadline + Duration::minutes(1),
            refreshed_storage_deadline + Duration::minutes(1),
        ),
        Err(StateError::ActivityNotAllowed {
            phase: SessionPhase::Suspending
        })
    ));
}

#[test]
fn replacement_requires_explicit_fresh_deadlines() {
    let mut anchor = test_anchor();
    let first_fence = anchor.fence().clone();
    let old_activity = anchor.last_activity_at();
    anchor.observe_pod(&first_fence, "pod-uid-a").unwrap();
    anchor
        .transition(&first_fence, SessionPhase::Blocked)
        .unwrap();
    anchor
        .confirm_pod_deleted(&first_fence, "pod-uid-a")
        .unwrap();

    let new_activity = old_activity + Duration::minutes(20);
    let new_compute_deadline = new_activity + Duration::minutes(15);
    let new_storage_deadline = new_activity + Duration::hours(72);
    let second_fence = anchor
        .advance_generation(
            &first_fence,
            Uuid::from_u128(3),
            new_activity,
            new_compute_deadline,
            new_storage_deadline,
        )
        .unwrap();

    assert_eq!(second_fence.generation(), 2);
    assert_eq!(anchor.last_activity_at(), new_activity);
    assert_eq!(anchor.compute_deadline_at(), new_compute_deadline);
    assert_eq!(anchor.storage_deadline_at(), new_storage_deadline);
}

#[test]
fn failed_generation_advance_does_not_partially_mutate_state() {
    let mut anchor = test_anchor();
    let first_fence = anchor.fence().clone();
    anchor
        .transition(&first_fence, SessionPhase::Blocked)
        .unwrap();
    let before = anchor.clone();
    let old_activity = anchor.last_activity_at();

    assert_eq!(
        anchor.advance_generation(
            &first_fence,
            Uuid::nil(),
            old_activity + Duration::minutes(20),
            old_activity + Duration::minutes(35),
            old_activity + Duration::hours(72),
        ),
        Err(StateError::InvalidRuntimeIdentifier)
    );
    assert_eq!(anchor, before);

    assert!(matches!(
        anchor.advance_generation(
            &first_fence,
            Uuid::from_u128(3),
            old_activity - Duration::minutes(1),
            old_activity + Duration::minutes(15),
            old_activity + Duration::hours(72),
        ),
        Err(StateError::ActivityTimeRegression { .. })
    ));
    assert_eq!(anchor, before);

    assert_eq!(
        anchor.advance_generation(
            &first_fence,
            Uuid::from_u128(3),
            old_activity + Duration::minutes(20),
            old_activity + Duration::minutes(20),
            old_activity + Duration::hours(72),
        ),
        Err(StateError::InvalidDeadlines)
    );
    assert_eq!(anchor, before);
}
