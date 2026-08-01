use chrono::{Duration, TimeZone, Utc};
use openab_kubernetes_session::identity::{IdentityError, ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::state::{
    Fence, ProfileRef, SessionAnchorV1, SessionPhase, StateError,
};
use serde_json::json;
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

    let mut nil_turn = serde_json::to_value(test_anchor()).unwrap();
    nil_turn["lastPromptTurnId"] = json!(Uuid::nil());
    assert!(serde_json::from_value::<SessionAnchorV1>(nil_turn).is_err());

    let mut inactive_turn = serde_json::to_value(test_anchor()).unwrap();
    inactive_turn["lastPromptTurnId"] = json!(Uuid::from_u128(0x400));
    assert!(serde_json::from_value::<SessionAnchorV1>(inactive_turn).is_err());
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
fn prompt_turn_fences_ready_busy_successors_and_clears_on_lifecycle_exit() {
    let mut ready = test_anchor();
    let fence = ready.fence().clone();
    ready.observe_pod(&fence, "pod-uid-a").unwrap();
    ready.transition(&fence, SessionPhase::Ready).unwrap();

    let turn_id = Uuid::from_u128(0x400);
    let started_at = ready.last_activity_at() + Duration::minutes(2);
    let mut busy = ready.clone();
    busy.record_prompt_started(
        &fence,
        turn_id,
        started_at,
        started_at + Duration::minutes(15),
        started_at + Duration::hours(72),
    )
    .unwrap();
    ready.validate_successor(&busy).unwrap();
    assert_eq!(busy.last_prompt_turn_id(), Some(turn_id));

    let before_mismatch = busy.clone();
    assert_eq!(
        busy.record_prompt_finished(
            &fence,
            Uuid::from_u128(0x401),
            started_at + Duration::minutes(1),
            started_at + Duration::minutes(16),
            started_at + Duration::hours(72),
        ),
        Err(StateError::PromptTurnMismatch)
    );
    assert_eq!(busy, before_mismatch);

    let mut completed = busy.clone();
    let finished_at = started_at + Duration::minutes(1);
    completed
        .record_prompt_finished(
            &fence,
            turn_id,
            finished_at,
            finished_at + Duration::minutes(15),
            finished_at + Duration::hours(72),
        )
        .unwrap();
    busy.validate_successor(&completed).unwrap();
    assert_eq!(completed.last_prompt_turn_id(), Some(turn_id));

    let mut suspending = busy.clone();
    suspending
        .transition(&fence, SessionPhase::Suspending)
        .unwrap();
    assert_eq!(suspending.last_prompt_turn_id(), None);
    busy.validate_successor(&suspending).unwrap();

    let mut tampered_value = serde_json::to_value(&busy).unwrap();
    tampered_value["lastPromptTurnId"] = serde_json::json!(Uuid::from_u128(0x402));
    let tampered: SessionAnchorV1 = serde_json::from_value(tampered_value).unwrap();
    assert_eq!(
        busy.validate_successor(&tampered),
        Err(StateError::InvalidPromptTurnSuccessor)
    );

    let mut legacy_busy = ready.clone();
    legacy_busy.transition(&fence, SessionPhase::Busy).unwrap();
    let mut unsafe_ready = legacy_busy.clone();
    unsafe_ready
        .transition(&fence, SessionPhase::Ready)
        .unwrap();
    assert_eq!(
        legacy_busy.validate_successor(&unsafe_ready),
        Err(StateError::InvalidPromptTurnSuccessor)
    );
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

    assert_eq!(
        anchor.advance_generation(
            &first_fence,
            first_fence.attempt_id(),
            old_activity + Duration::minutes(20),
            old_activity + Duration::minutes(35),
            old_activity + Duration::hours(72),
        ),
        Err(StateError::ReusedAttemptIdentifier)
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

#[test]
fn serialized_snapshots_cannot_bypass_successor_fencing() {
    let current = test_anchor();

    let mut changed_attempt = serde_json::to_value(&current).unwrap();
    changed_attempt["fence"]["attemptId"] = json!(Uuid::from_u128(99));
    let changed_attempt: SessionAnchorV1 = serde_json::from_value(changed_attempt).unwrap();
    assert!(matches!(
        current.validate_successor(&changed_attempt),
        Err(StateError::InvalidFenceSuccessor { .. })
    ));

    let mut skipped_generation = serde_json::to_value(&current).unwrap();
    skipped_generation["fence"]["generation"] = json!(3);
    skipped_generation["fence"]["attemptId"] = json!(Uuid::from_u128(3));
    let skipped_generation: SessionAnchorV1 = serde_json::from_value(skipped_generation).unwrap();
    assert!(matches!(
        current.validate_successor(&skipped_generation),
        Err(StateError::InvalidFenceSuccessor { .. })
    ));
}

#[test]
fn adjacent_generation_cannot_reuse_the_current_attempt() {
    let mut current = test_anchor();
    let fence = current.fence().clone();
    current.transition(&fence, SessionPhase::Blocked).unwrap();

    let mut reused_attempt = serde_json::to_value(&current).unwrap();
    reused_attempt["fence"]["generation"] = json!(fence.generation() + 1);
    reused_attempt["phase"] = json!("provisioning");
    let reused_attempt: SessionAnchorV1 = serde_json::from_value(reused_attempt).unwrap();

    assert!(matches!(
        current.validate_successor(&reused_attempt),
        Err(StateError::InvalidFenceSuccessor { .. })
    ));
}

#[test]
fn generation_overflow_does_not_partially_mutate_state() {
    let mut encoded = serde_json::to_value(test_anchor()).unwrap();
    encoded["fence"]["generation"] = json!(u64::MAX);
    encoded["phase"] = json!("blocked");
    let mut anchor: SessionAnchorV1 = serde_json::from_value(encoded).unwrap();
    let before = anchor.clone();
    let fence = anchor.fence().clone();
    let activity = anchor.last_activity_at() + Duration::minutes(20);

    assert_eq!(
        anchor.advance_generation(
            &fence,
            Uuid::from_u128(3),
            activity,
            activity + Duration::minutes(15),
            activity + Duration::hours(72),
        ),
        Err(StateError::GenerationOverflow)
    );
    assert_eq!(anchor, before);
}

#[test]
fn domain_mutations_produce_valid_successor_snapshots() {
    let current = test_anchor();
    let mut next = current.clone();
    let fence = next.fence().clone();
    let activity = next.last_activity_at() + Duration::minutes(2);
    next.refresh_activity(
        &fence,
        activity,
        activity + Duration::minutes(15),
        activity + Duration::hours(72),
    )
    .unwrap();

    current.validate_successor(&next).unwrap();

    let mut suspended = current.clone();
    suspended.transition(&fence, SessionPhase::Blocked).unwrap();
    let mut replacement = suspended.clone();
    let replacement_activity = activity + Duration::minutes(20);
    replacement
        .advance_generation(
            &fence,
            Uuid::from_u128(3),
            replacement_activity,
            replacement_activity + Duration::minutes(15),
            replacement_activity + Duration::hours(72),
        )
        .unwrap();

    suspended.validate_successor(&replacement).unwrap();
}
