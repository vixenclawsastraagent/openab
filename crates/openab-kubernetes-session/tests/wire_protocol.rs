use openab_kubernetes_session::bridge::{
    BridgeAction, BridgeIdentity, BridgeKernel, ControllerLifecycleAction, LifecycleKind,
    SessionBinding,
};
use openab_kubernetes_session::identity::{ScopeId, SessionId};
use openab_kubernetes_session::state::{Fence, ProfileRef};
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, max_frame_len, validate_frame_len, AcpMessageV1,
    ActivatedSessionV1, ActivationRequestV1, ActivationResponseV1, BrokerMappingExpectationV1,
    FatalCode, LifecycleRequestV1, ProtocolResultV1, SessionBindingV1,
    ValidatedActivationOutcomeV1, WireMessage, WireProtocolError, WorkerRegistrationV1,
    MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES, MAX_PROFILE_VERSION_BYTES, MAX_WORKER_CWD_BYTES,
    MAX_WORKER_SESSION_ID_BYTES,
};
use serde_json::{json, Value};
use uuid::Uuid;

const ATTEMPT_ID: Uuid = Uuid::from_u128(100);
const INCARNATION_ID: Uuid = Uuid::from_u128(200);

fn identity() -> BridgeIdentity {
    BridgeIdentity::from_values(
        "team-a",
        "discord:thread-123",
        &ATTEMPT_ID.to_string(),
        "codex-strict",
    )
    .unwrap()
}

fn activation() -> ActivationRequestV1 {
    ActivationRequestV1::from_identity(&identity(), BrokerMappingExpectationV1::Present)
}

fn binding() -> SessionBinding {
    let identity = identity();
    SessionBinding::new(
        identity.scope_id(),
        identity.session_id(),
        Fence::new(7, ATTEMPT_ID).unwrap(),
        INCARNATION_ID,
    )
    .unwrap()
}

fn jsonrpc_request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn lifecycle_action(kind: LifecycleKind, worker_session_id: &str) -> ControllerLifecycleAction {
    let mut kernel = BridgeKernel::new(identity(), binding(), "/workspace").unwrap();
    let initialize = jsonrpc_request(
        1,
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    );
    kernel
        .handle_broker_message(&serde_json::to_vec(&initialize).unwrap())
        .unwrap();
    let initialized = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "protocolVersion": 1,
            "agentCapabilities": {"loadSession": true}
        }
    });
    kernel
        .handle_worker_message(&serde_json::to_vec(&initialized).unwrap())
        .unwrap();

    let new_session = jsonrpc_request(2, "session/new", json!({}));
    kernel
        .handle_broker_message(&serde_json::to_vec(&new_session).unwrap())
        .unwrap();
    let created = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"sessionId": worker_session_id}
    });
    kernel
        .handle_worker_message(&serde_json::to_vec(&created).unwrap())
        .unwrap();

    let method = match kind {
        LifecycleKind::Suspend => "session/close",
        LifecycleKind::Release => "_openab/session/release",
    };
    let close = jsonrpc_request(3, method, json!({"sessionId": worker_session_id}));
    let BridgeAction::Controller(action) = kernel
        .handle_broker_message(&serde_json::to_vec(&close).unwrap())
        .unwrap()
    else {
        panic!("expected controller lifecycle action");
    };
    action
}

fn add_unknown_field(value: &mut Value) {
    value
        .as_object_mut()
        .unwrap()
        .insert("controllerDebug".to_string(), json!("secret detail"));
}

fn assert_unknown_field_rejected<T>(message: &T)
where
    T: WireMessage,
{
    let mut value = serde_json::to_value(message).unwrap();
    add_unknown_field(&mut value);
    assert!(decode_frame::<T>(&serde_json::to_vec(&value).unwrap()).is_err());
}

fn assert_version_rejected<T>(message: &T)
where
    T: WireMessage,
{
    let mut value = serde_json::to_value(message).unwrap();
    value["version"] = json!(2);
    assert!(decode_frame::<T>(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn activation_contains_only_derived_identity_and_requested_profile_name() {
    let request = activation();
    let encoded = encode_frame(&request).unwrap();
    let json: Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(
        json,
        json!({
            "version": 1,
            "scopeId": identity().scope_id().as_hex(),
            "sessionId": identity().session_id().as_hex(),
            "attemptId": ATTEMPT_ID.to_string(),
            "brokerMappingExpectation": "present",
            "requestedProfileName": "codex-strict",
        })
    );

    let text = String::from_utf8(encoded).unwrap();
    assert!(!text.contains("team-a"));
    assert!(!text.contains("discord:thread-123"));
    assert!(!text.contains("sha256-image-v7"));
    assert_eq!(
        decode_frame::<ActivationRequestV1>(text.as_bytes()).unwrap(),
        request
    );
}

#[test]
fn activation_rejects_unknown_versions_fields_and_nil_attempts() {
    let value = serde_json::to_value(activation()).unwrap();

    for (field, replacement) in [("version", json!(2)), ("attemptId", json!(Uuid::nil()))] {
        let mut invalid = value.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .insert(field.to_string(), replacement);
        assert!(
            decode_frame::<ActivationRequestV1>(&serde_json::to_vec(&invalid).unwrap()).is_err()
        );
    }
    assert!(ActivationRequestV1::new(
        identity().scope_id(),
        identity().session_id(),
        ATTEMPT_ID,
        "Not_A_DNS_Label",
        BrokerMappingExpectationV1::Absent,
    )
    .is_err());
    assert_unknown_field_rejected(&activation());
}

#[test]
fn session_binding_round_trips_and_conversion_revalidates_authority() {
    let expected = binding();
    let wire = SessionBindingV1::from(&expected);
    let decoded: SessionBindingV1 = decode_frame(&encode_frame(&wire).unwrap()).unwrap();
    let actual = decoded.to_binding().unwrap();

    assert_eq!(actual, expected);
    assert_eq!(decoded.generation(), 7);
    assert_eq!(decoded.attempt_id(), ATTEMPT_ID);
    assert_eq!(decoded.incarnation_id(), INCARNATION_ID);

    let value = serde_json::to_value(&wire).unwrap();
    for (field, replacement) in [
        ("generation", json!(0)),
        ("attemptId", json!(Uuid::nil())),
        ("incarnationId", json!(Uuid::nil())),
    ] {
        let mut invalid = value.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .insert(field.to_string(), replacement);
        assert!(decode_frame::<SessionBindingV1>(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    assert_unknown_field_rejected(&wire);
}

#[test]
fn activated_session_returns_validated_profile_binding_and_fixed_cwd() {
    let activation = activation();
    let profile = ProfileRef::new("codex-strict", "sha256-image-v7").unwrap();
    let response =
        ActivatedSessionV1::new(&activation, profile.clone(), &binding(), "/workspace").unwrap();
    let decoded: ActivatedSessionV1 =
        serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();

    let (actual_profile, actual_binding, actual_cwd) =
        decoded.into_validated_parts(&activation).unwrap();
    assert_eq!(actual_profile, profile);
    assert_eq!(actual_profile.version(), "sha256-image-v7");
    assert_eq!(actual_binding, binding());
    assert_eq!(actual_cwd, "/workspace");

    let mut relative = serde_json::to_value(&response).unwrap();
    relative["workerCwd"] = json!("broker/workspace");
    assert!(
        serde_json::from_slice::<ActivatedSessionV1>(&serde_json::to_vec(&relative).unwrap())
            .is_err()
    );

    let other = BridgeIdentity::from_values(
        "team-b",
        "discord:thread-999",
        &ATTEMPT_ID.to_string(),
        "codex-strict",
    )
    .unwrap();
    let other_request =
        ActivationRequestV1::from_identity(&other, BrokerMappingExpectationV1::Present);
    assert!(response
        .clone()
        .into_validated_parts(&other_request)
        .is_err());

    let other_profile_request = ActivationRequestV1::new(
        activation.scope_id(),
        activation.session_id(),
        activation.attempt_id(),
        "other-profile",
        BrokerMappingExpectationV1::Present,
    )
    .unwrap();
    assert!(matches!(
        response
            .clone()
            .into_validated_parts(&other_profile_request),
        Err(WireProtocolError::ProfileMismatch)
    ));
    let mut unknown = serde_json::to_value(&response).unwrap();
    add_unknown_field(&mut unknown);
    assert!(
        serde_json::from_slice::<ActivatedSessionV1>(&serde_json::to_vec(&unknown).unwrap())
            .is_err()
    );
}

#[test]
fn activation_requires_an_explicit_broker_mapping_expectation() {
    let present = activation();
    assert_eq!(
        present.broker_mapping_expectation(),
        BrokerMappingExpectationV1::Present
    );

    let absent =
        ActivationRequestV1::from_identity(&identity(), BrokerMappingExpectationV1::Absent);
    let absent_json = serde_json::to_value(&absent).unwrap();
    assert_eq!(absent_json["brokerMappingExpectation"], "absent");

    let mut missing = serde_json::to_value(&present).unwrap();
    missing
        .as_object_mut()
        .unwrap()
        .remove("brokerMappingExpectation");
    assert!(decode_frame::<ActivationRequestV1>(&serde_json::to_vec(&missing).unwrap()).is_err());

    let mut unknown = serde_json::to_value(&present).unwrap();
    unknown["brokerMappingExpectation"] = json!("unknown");
    assert!(decode_frame::<ActivationRequestV1>(&serde_json::to_vec(&unknown).unwrap()).is_err());
}

#[test]
fn activation_response_returns_one_typed_validated_outcome() {
    let request = activation();
    let profile = ProfileRef::new("codex-strict", "sha256-image-v7").unwrap();
    let activated =
        ActivatedSessionV1::new(&request, profile.clone(), &binding(), "/workspace").unwrap();
    let response = ActivationResponseV1::activated(activated);
    let encoded = encode_frame(&response).unwrap();
    let json: Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(json["outcome"], "activated");
    assert_eq!(json["payload"]["version"], 1);
    let decoded: ActivationResponseV1 = decode_frame(&encoded).unwrap();
    match decoded.into_validated_outcome(&request).unwrap() {
        ValidatedActivationOutcomeV1::Activated {
            profile: actual_profile,
            binding: actual_binding,
            worker_cwd,
        } => {
            assert_eq!(actual_profile, profile);
            assert_eq!(actual_binding, binding());
            assert_eq!(worker_cwd, "/workspace");
        }
        ValidatedActivationOutcomeV1::MappingAbsent => {
            panic!("expected an activated outcome")
        }
    }
    assert_unknown_field_rejected(&response);

    let mut unsupported_payload_version = json;
    unsupported_payload_version["payload"]["version"] = json!(2);
    assert!(decode_frame::<ActivationResponseV1>(
        &serde_json::to_vec(&unsupported_payload_version).unwrap()
    )
    .is_err());
}

#[test]
fn mapping_absent_response_requires_present_expectation_and_exact_correlation() {
    let request = activation();
    let response = ActivationResponseV1::mapping_absent(&request).unwrap();
    let encoded = encode_frame(&response).unwrap();
    let json: Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(json["outcome"], "mapping_absent");
    assert_eq!(json["payload"]["version"], 1);
    assert_eq!(json["payload"]["scopeId"], request.scope_id().as_hex());
    assert_eq!(json["payload"]["sessionId"], request.session_id().as_hex());
    assert_eq!(
        json["payload"]["attemptId"],
        request.attempt_id().to_string()
    );
    assert_eq!(
        decode_frame::<ActivationResponseV1>(&encoded)
            .unwrap()
            .into_validated_outcome(&request)
            .unwrap(),
        ValidatedActivationOutcomeV1::MappingAbsent
    );

    let absent_request =
        ActivationRequestV1::from_identity(&identity(), BrokerMappingExpectationV1::Absent);
    assert!(matches!(
        decode_frame::<ActivationResponseV1>(&encoded)
            .unwrap()
            .into_validated_outcome(&absent_request),
        Err(WireProtocolError::UnexpectedMappingAbsent)
    ));
    assert!(matches!(
        ActivationResponseV1::mapping_absent(&absent_request),
        Err(WireProtocolError::UnexpectedMappingAbsent)
    ));

    for (field, replacement) in [
        ("scopeId", json!(ScopeId::derive("other-scope").as_hex())),
        (
            "sessionId",
            json!(SessionId::derive("other-scope", "other-thread").as_hex()),
        ),
        ("attemptId", json!(Uuid::from_u128(999).to_string())),
    ] {
        let mut mismatched = json.clone();
        mismatched["payload"][field] = replacement;
        let decoded: ActivationResponseV1 =
            decode_frame(&serde_json::to_vec(&mismatched).unwrap()).unwrap();
        assert!(matches!(
            decoded.into_validated_outcome(&request),
            Err(WireProtocolError::MappingAbsentMismatch(actual)) if actual == field
        ));
    }
}

#[test]
fn mapping_absent_wire_payload_fails_closed() {
    let request = activation();
    let response = ActivationResponseV1::mapping_absent(&request).unwrap();
    let value = serde_json::to_value(&response).unwrap();

    for (field, replacement) in [("version", json!(2)), ("attemptId", json!(Uuid::nil()))] {
        let mut invalid = value.clone();
        invalid["payload"][field] = replacement;
        assert!(
            decode_frame::<ActivationResponseV1>(&serde_json::to_vec(&invalid).unwrap()).is_err()
        );
    }

    let mut unknown_payload = value.clone();
    unknown_payload["payload"]["controllerDebug"] = json!("secret detail");
    assert!(
        decode_frame::<ActivationResponseV1>(&serde_json::to_vec(&unknown_payload).unwrap())
            .is_err()
    );

    let mut unknown_outcome = value.clone();
    unknown_outcome["outcome"] = json!("mapping_maybe_absent");
    assert!(
        decode_frame::<ActivationResponseV1>(&serde_json::to_vec(&unknown_outcome).unwrap())
            .is_err()
    );

    assert_unknown_field_rejected(&response);
}

#[test]
fn activation_response_envelope_rejects_variant_confusion() {
    let request = activation();
    let absent =
        serde_json::to_value(ActivationResponseV1::mapping_absent(&request).unwrap()).unwrap();
    let activated = serde_json::to_value(ActivationResponseV1::activated(
        ActivatedSessionV1::new(
            &request,
            ProfileRef::new("codex-strict", "sha256-image-v7").unwrap(),
            &binding(),
            "/workspace",
        )
        .unwrap(),
    ))
    .unwrap();

    let mut activated_tag_with_absent_payload = absent.clone();
    activated_tag_with_absent_payload["outcome"] = json!("activated");

    let mut absent_tag_with_activated_payload = activated;
    absent_tag_with_activated_payload["outcome"] = json!("mapping_absent");

    let mut missing_payload = absent.clone();
    missing_payload.as_object_mut().unwrap().remove("payload");

    let mut outer_version = absent;
    outer_version["version"] = json!(1);

    for invalid in [
        activated_tag_with_absent_payload,
        absent_tag_with_activated_payload,
        missing_payload,
        outer_version,
    ] {
        assert!(
            decode_frame::<ActivationResponseV1>(&serde_json::to_vec(&invalid).unwrap()).is_err()
        );
    }
}

#[test]
fn control_plane_strings_are_bounded_and_cwd_is_canonical_by_construction() {
    let profile = ProfileRef::new("codex-strict", "v1").unwrap();
    for cwd in [
        "relative/workspace".to_string(),
        "/workspace/../other".to_string(),
        "/workspace/./repo".to_string(),
        "/workspace\n/repo".to_string(),
        format!("/{}", "a".repeat(MAX_WORKER_CWD_BYTES)),
    ] {
        assert!(ActivatedSessionV1::new(&activation(), profile.clone(), &binding(), cwd).is_err());
    }

    let oversized_profile =
        ProfileRef::new("codex-strict", "v".repeat(MAX_PROFILE_VERSION_BYTES + 1)).unwrap();
    assert!(ActivatedSessionV1::new(
        &activation(),
        oversized_profile.clone(),
        &binding(),
        "/workspace"
    )
    .is_err());
    let valid = ActivatedSessionV1::new(&activation(), profile, &binding(), "/workspace").unwrap();
    let mut oversized_on_wire = serde_json::to_value(valid).unwrap();
    oversized_on_wire["profile"]["version"] = json!(oversized_profile.version());
    assert!(serde_json::from_slice::<ActivatedSessionV1>(
        &serde_json::to_vec(&oversized_on_wire).unwrap()
    )
    .is_err());

    let oversized_worker_session = "s".repeat(MAX_WORKER_SESSION_ID_BYTES + 1);
    let action = lifecycle_action(LifecycleKind::Suspend, &oversized_worker_session);
    assert!(LifecycleRequestV1::from_bridge_action(&action).is_err());
}

#[test]
fn lifecycle_has_independent_request_id_and_treats_worker_session_as_data() {
    let suspicious_worker_data = "../../another-session";
    let action = lifecycle_action(LifecycleKind::Release, suspicious_worker_data);
    let request = LifecycleRequestV1::from_bridge_action(&action).unwrap();
    let decoded: LifecycleRequestV1 = decode_frame(&encode_frame(&request).unwrap()).unwrap();

    assert_eq!(decoded.request_id(), action.action_id());
    assert_eq!(decoded.kind(), LifecycleKind::Release);
    assert_eq!(decoded.to_binding().unwrap(), binding());
    assert_eq!(decoded.worker_session_id(), suspicious_worker_data);
    assert_ne!(
        decoded.request_id(),
        decoded.to_binding().unwrap().fence().attempt_id()
    );

    let value = serde_json::to_value(&request).unwrap();
    for (field, replacement) in [
        ("requestId", json!(Uuid::nil())),
        ("workerSessionId", json!("")),
        ("version", json!(9)),
    ] {
        let mut invalid = value.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .insert(field.to_string(), replacement);
        assert!(
            decode_frame::<LifecycleRequestV1>(&serde_json::to_vec(&invalid).unwrap()).is_err()
        );
    }
    assert_unknown_field_rejected(&request);
}

#[test]
fn lifecycle_uses_the_stable_bridge_action_id_and_correlates_results_exactly() {
    let action = lifecycle_action(LifecycleKind::Suspend, "worker-1");
    let first = LifecycleRequestV1::from_bridge_action(&action).unwrap();
    let retry = LifecycleRequestV1::from_bridge_action(&action).unwrap();

    assert_eq!(first.request_id(), action.action_id());
    assert_eq!(retry.request_id(), action.action_id());

    let outcome = ProtocolResultV1::ack(Some(action.action_id()))
        .unwrap()
        .into_lifecycle_outcome(&first)
        .unwrap();
    assert_eq!(outcome, None);

    assert!(matches!(
        ProtocolResultV1::ack(None)
            .unwrap()
            .into_lifecycle_outcome(&first),
        Err(WireProtocolError::ResultRequestIdMismatch)
    ));
    assert!(matches!(
        ProtocolResultV1::fatal(Some(Uuid::from_u128(999)), FatalCode::Unavailable)
            .unwrap()
            .into_lifecycle_outcome(&first),
        Err(WireProtocolError::ResultRequestIdMismatch)
    ));
}

#[test]
fn worker_registration_carries_only_the_validated_binding() {
    let registration = WorkerRegistrationV1::new(&binding());
    let json = serde_json::to_value(&registration).unwrap();

    assert_eq!(json["version"], 1);
    assert!(json.get("binding").is_some());
    assert!(!json.as_object().unwrap().contains_key("token"));
    let decoded: WorkerRegistrationV1 =
        decode_frame(&encode_frame(&registration).unwrap()).unwrap();
    assert_eq!(
        decoded.clone().into_validated_binding(&binding()).unwrap(),
        binding()
    );

    let identity = identity();
    let newer_binding = SessionBinding::new(
        identity.scope_id(),
        identity.session_id(),
        Fence::new(8, Uuid::from_u128(101)).unwrap(),
        INCARNATION_ID,
    )
    .unwrap();
    assert!(decoded.into_validated_binding(&newer_binding).is_err());
    assert_unknown_field_rejected(&registration);
}

#[test]
fn acp_payload_is_a_json_value_not_a_string_and_is_bounded() {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 41,
        "method": "session/prompt",
        "params": {"prompt": [{"type": "text", "text": "hello"}]}
    });
    let message = AcpMessageV1::new(payload.clone()).unwrap();
    let encoded = encode_frame(&message).unwrap();
    let json: Value = serde_json::from_slice(&encoded).unwrap();

    assert!(json["payload"].is_object());
    let decoded: AcpMessageV1 = decode_frame(&encoded).unwrap();
    assert_eq!(decoded.payload(), &payload);
    assert_unknown_field_rejected(&message);
}

#[test]
fn public_protocol_results_have_codes_but_no_controller_detail_channel() {
    let action = lifecycle_action(LifecycleKind::Suspend, "worker-1");
    let request = LifecycleRequestV1::from_bridge_action(&action).unwrap();
    let fatal = ProtocolResultV1::fatal(Some(request.request_id()), FatalCode::Internal).unwrap();
    let encoded = encode_frame(&fatal).unwrap();
    let text = String::from_utf8(encoded.clone()).unwrap();
    let json: Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(json["requestId"], request.request_id().to_string());
    assert_eq!(json["fatalCode"], "internal");
    assert!(!text.contains("detail"));
    assert!(!text.contains("postgres"));
    assert_eq!(decode_frame::<ProtocolResultV1>(&encoded).unwrap(), fatal);
    assert_eq!(
        fatal.clone().into_lifecycle_outcome(&request).unwrap(),
        Some(FatalCode::Internal)
    );

    let mut injected = serde_json::to_value(&fatal).unwrap();
    injected["detail"] = json!("postgres password leaked here");
    assert!(decode_frame::<ProtocolResultV1>(&serde_json::to_vec(&injected).unwrap()).is_err());

    let ack = ProtocolResultV1::ack(Some(request.request_id())).unwrap();
    assert_eq!(ack.clone().into_lifecycle_outcome(&request).unwrap(), None);
    assert_unknown_field_rejected(&ack);
}

#[test]
fn every_top_level_message_rejects_an_unsupported_version() {
    assert_version_rejected(&activation());
    assert_version_rejected(&SessionBindingV1::from(&binding()));
    assert_version_rejected(
        &LifecycleRequestV1::from_bridge_action(&lifecycle_action(
            LifecycleKind::Suspend,
            "worker-1",
        ))
        .unwrap(),
    );
    assert_version_rejected(&WorkerRegistrationV1::new(&binding()));
    assert_version_rejected(&AcpMessageV1::new(json!({})).unwrap());
    assert_version_rejected(&ProtocolResultV1::fatal(None, FatalCode::Unauthorized).unwrap());
}

#[test]
fn standalone_control_frames_keep_the_small_data_plane_limit() {
    assert_eq!(
        MAX_ACP_FRAME_BYTES,
        openab_kubernetes_session::bridge::MAX_LOGICAL_MESSAGE_BYTES + MAX_CONTROL_FRAME_BYTES
    );
    assert_eq!(max_frame_len::<AcpMessageV1>(), MAX_ACP_FRAME_BYTES);
    for limit in [
        max_frame_len::<ActivationRequestV1>(),
        max_frame_len::<SessionBindingV1>(),
        max_frame_len::<ActivationResponseV1>(),
        max_frame_len::<LifecycleRequestV1>(),
        max_frame_len::<WorkerRegistrationV1>(),
        max_frame_len::<ProtocolResultV1>(),
    ] {
        assert_eq!(limit, MAX_CONTROL_FRAME_BYTES);
    }

    assert!(validate_frame_len::<AcpMessageV1>(MAX_ACP_FRAME_BYTES).is_ok());
    assert!(matches!(
        validate_frame_len::<AcpMessageV1>(MAX_ACP_FRAME_BYTES + 1),
        Err(WireProtocolError::FrameTooLarge { .. })
    ));

    let oversized_control = vec![b' '; MAX_CONTROL_FRAME_BYTES + 1];
    assert!(matches!(
        decode_frame::<ActivationRequestV1>(&oversized_control),
        Err(WireProtocolError::FrameTooLarge { .. })
    ));

    let payload = json!({"blob": "a".repeat(MAX_CONTROL_FRAME_BYTES)});
    let acp = AcpMessageV1::new(payload).unwrap();
    let encoded = encode_frame(&acp).unwrap();
    assert!(encoded.len() > MAX_CONTROL_FRAME_BYTES);
    decode_frame::<AcpMessageV1>(&encoded).unwrap();
}
