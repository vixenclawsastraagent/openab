use openab_kubernetes_session::bridge::{BridgeIdentity, SessionBinding};
use openab_kubernetes_session::state::Fence;
use openab_kubernetes_session::wire::{
    decode_frame, encode_frame, max_frame_len, AcpMessageV1, ActivationRequestV1,
    ActivationResponseV1, BridgeToControllerV1, BrokerMappingExpectationV1, ControllerToBridgeV1,
    ControllerToWorkerV1, FatalCode, HandshakeOutcomeV1, LifecycleRequestV1, ProtocolResultV1,
    SessionBindingV1, WireMessage, WireProtocolError, WorkerRegistrationV1, WorkerToControllerV1,
    MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
};
use serde_json::{json, Value};
use std::fmt::Debug;
use uuid::Uuid;

const ATTEMPT_ID: Uuid = Uuid::from_u128(0x301);
const INCARNATION_ID: Uuid = Uuid::from_u128(0x302);

fn identity() -> BridgeIdentity {
    BridgeIdentity::from_values(
        "team-a",
        "discord:relay-envelope",
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
        Fence::new(1, ATTEMPT_ID).unwrap(),
        INCARNATION_ID,
    )
    .unwrap()
}

fn lifecycle() -> LifecycleRequestV1 {
    serde_json::from_value(json!({
        "version": 1,
        "requestId": Uuid::from_u128(0x303),
        "kind": "suspend",
        "binding": serde_json::to_value(SessionBindingV1::from(&binding())).unwrap(),
        "workerSessionId": "worker-session-relay",
    }))
    .unwrap()
}

fn acp(size: usize) -> AcpMessageV1 {
    AcpMessageV1::new(json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"text": "x".repeat(size)},
    }))
    .unwrap()
}

fn assert_round_trip<T>(message: T)
where
    T: WireMessage + Debug + PartialEq,
{
    let encoded = encode_frame(&message).unwrap();
    assert_eq!(decode_frame::<T>(&encoded).unwrap(), message);
}

fn assert_control_envelope_limit<T>(message: &T)
where
    T: WireMessage + Debug + PartialEq,
{
    let mut exact = encode_frame(message).unwrap();
    exact.resize(MAX_CONTROL_FRAME_BYTES, b' ');
    assert_eq!(decode_frame::<T>(&exact).unwrap(), *message);

    exact.push(b' ');
    assert!(matches!(
        decode_frame::<T>(&exact),
        Err(WireProtocolError::FrameTooLarge {
            maximum: MAX_CONTROL_FRAME_BYTES,
            ..
        })
    ));
}

#[test]
fn every_direction_round_trips_only_its_closed_variant_set() {
    let activation = activation();
    let absent = ActivationResponseV1::mapping_absent(&activation).unwrap();
    let registration = WorkerRegistrationV1::new(&binding());
    let lifecycle = lifecycle();
    let ack = ProtocolResultV1::ack(None).unwrap();
    let acp = acp(32);

    for message in [
        BridgeToControllerV1::Activation(activation),
        BridgeToControllerV1::Acp(acp.clone()),
        BridgeToControllerV1::Lifecycle(lifecycle),
    ] {
        assert_round_trip(message);
    }
    for message in [
        ControllerToBridgeV1::Activation(absent),
        ControllerToBridgeV1::Acp(acp.clone()),
        ControllerToBridgeV1::ProtocolResult(ack.clone()),
    ] {
        assert_round_trip(message);
    }
    for message in [
        WorkerToControllerV1::Registration(registration),
        WorkerToControllerV1::Acp(acp.clone()),
    ] {
        assert_round_trip(message);
    }
    for message in [
        ControllerToWorkerV1::ProtocolResult(ack),
        ControllerToWorkerV1::Acp(acp),
    ] {
        assert_round_trip(message);
    }
}

#[test]
fn direction_tags_are_explicit_stable_and_role_specific() {
    let bridge_activation = BridgeToControllerV1::Activation(activation());
    let controller_activation = ControllerToBridgeV1::Activation(
        ActivationResponseV1::mapping_absent(&activation()).unwrap(),
    );
    let worker_registration =
        WorkerToControllerV1::Registration(WorkerRegistrationV1::new(&binding()));
    let controller_result =
        ControllerToWorkerV1::ProtocolResult(ProtocolResultV1::ack(None).unwrap());

    assert_eq!(
        serde_json::to_value(&bridge_activation).unwrap()["kind"],
        "activation"
    );
    assert_eq!(
        serde_json::to_value(&worker_registration).unwrap()["kind"],
        "registration"
    );
    assert_eq!(
        serde_json::to_value(&controller_result).unwrap()["kind"],
        "protocol_result"
    );

    let worker_bytes = encode_frame(&worker_registration).unwrap();
    assert!(decode_frame::<BridgeToControllerV1>(&worker_bytes).is_err());
    let bridge_bytes = encode_frame(&bridge_activation).unwrap();
    assert!(decode_frame::<WorkerToControllerV1>(&bridge_bytes).is_err());
    assert!(decode_frame::<ControllerToBridgeV1>(&bridge_bytes).is_err());
    let controller_bytes = encode_frame(&controller_activation).unwrap();
    assert!(decode_frame::<BridgeToControllerV1>(&controller_bytes).is_err());
}

#[test]
fn every_direction_rejects_unknown_outer_fields_kinds_and_inner_versions() {
    let messages = [
        serde_json::to_value(BridgeToControllerV1::Activation(activation())).unwrap(),
        serde_json::to_value(ControllerToBridgeV1::ProtocolResult(
            ProtocolResultV1::ack(None).unwrap(),
        ))
        .unwrap(),
        serde_json::to_value(WorkerToControllerV1::Registration(
            WorkerRegistrationV1::new(&binding()),
        ))
        .unwrap(),
        serde_json::to_value(ControllerToWorkerV1::ProtocolResult(
            ProtocolResultV1::ack(None).unwrap(),
        ))
        .unwrap(),
    ];

    let reject = |mut value: Value, direction: usize| {
        value["unexpected"] = json!(true);
        let bytes = serde_json::to_vec(&value).unwrap();
        let rejected = match direction {
            0 => decode_frame::<BridgeToControllerV1>(&bytes).is_err(),
            1 => decode_frame::<ControllerToBridgeV1>(&bytes).is_err(),
            2 => decode_frame::<WorkerToControllerV1>(&bytes).is_err(),
            3 => decode_frame::<ControllerToWorkerV1>(&bytes).is_err(),
            _ => unreachable!(),
        };
        assert!(rejected);
    };
    for (direction, message) in messages.into_iter().enumerate() {
        reject(message, direction);
    }

    let mut unknown_kind =
        serde_json::to_value(BridgeToControllerV1::Activation(activation())).unwrap();
    unknown_kind["kind"] = json!("registration");
    assert!(
        decode_frame::<BridgeToControllerV1>(&serde_json::to_vec(&unknown_kind).unwrap()).is_err()
    );

    let mut future_inner =
        serde_json::to_value(BridgeToControllerV1::Activation(activation())).unwrap();
    future_inner["payload"]["version"] = json!(2);
    assert!(
        decode_frame::<BridgeToControllerV1>(&serde_json::to_vec(&future_inner).unwrap()).is_err()
    );
}

#[test]
fn control_variants_keep_the_small_limit_inside_mixed_envelopes() {
    assert_eq!(max_frame_len::<BridgeToControllerV1>(), MAX_ACP_FRAME_BYTES);
    assert_eq!(max_frame_len::<ControllerToBridgeV1>(), MAX_ACP_FRAME_BYTES);
    assert_eq!(max_frame_len::<WorkerToControllerV1>(), MAX_ACP_FRAME_BYTES);
    assert_eq!(max_frame_len::<ControllerToWorkerV1>(), MAX_ACP_FRAME_BYTES);

    assert_control_envelope_limit(&BridgeToControllerV1::Activation(activation()));
    assert_control_envelope_limit(&ControllerToBridgeV1::ProtocolResult(
        ProtocolResultV1::ack(None).unwrap(),
    ));
    assert_control_envelope_limit(&WorkerToControllerV1::Registration(
        WorkerRegistrationV1::new(&binding()),
    ));
    assert_control_envelope_limit(&ControllerToWorkerV1::ProtocolResult(
        ProtocolResultV1::ack(None).unwrap(),
    ));
}

#[test]
fn acp_variants_receive_the_large_but_still_bounded_limit() {
    let acp = acp(MAX_CONTROL_FRAME_BYTES + 1);
    let directions = [
        encode_frame(&BridgeToControllerV1::Acp(acp.clone())).unwrap(),
        encode_frame(&ControllerToBridgeV1::Acp(acp.clone())).unwrap(),
        encode_frame(&WorkerToControllerV1::Acp(acp.clone())).unwrap(),
        encode_frame(&ControllerToWorkerV1::Acp(acp)).unwrap(),
    ];
    assert!(directions
        .iter()
        .all(|encoded| encoded.len() > MAX_CONTROL_FRAME_BYTES));
    decode_frame::<BridgeToControllerV1>(&directions[0]).unwrap();
    decode_frame::<ControllerToBridgeV1>(&directions[1]).unwrap();
    decode_frame::<WorkerToControllerV1>(&directions[2]).unwrap();
    decode_frame::<ControllerToWorkerV1>(&directions[3]).unwrap();

    let oversized = vec![b' '; MAX_ACP_FRAME_BYTES + 1];
    assert!(matches!(
        decode_frame::<BridgeToControllerV1>(&oversized),
        Err(WireProtocolError::FrameTooLarge {
            maximum: MAX_ACP_FRAME_BYTES,
            ..
        })
    ));
}

#[test]
fn registration_envelope_never_serializes_transport_credentials_or_pod_identity() {
    let encoded = encode_frame(&WorkerToControllerV1::Registration(
        WorkerRegistrationV1::new(&binding()),
    ))
    .unwrap();
    let text = String::from_utf8(encoded).unwrap();

    assert!(!text.contains("token"));
    assert!(!text.contains("podUid"));
    assert!(!text.contains("worker-pod"));
}

#[test]
fn handshake_results_reject_lifecycle_correlation_ids() {
    assert_eq!(
        ProtocolResultV1::ack(None)
            .unwrap()
            .into_handshake_outcome()
            .unwrap(),
        HandshakeOutcomeV1::Ack
    );
    assert_eq!(
        ProtocolResultV1::fatal(None, FatalCode::Unauthorized)
            .unwrap()
            .into_handshake_outcome()
            .unwrap(),
        HandshakeOutcomeV1::Fatal(FatalCode::Unauthorized)
    );
    assert!(matches!(
        ProtocolResultV1::ack(Some(Uuid::from_u128(0x304)))
            .unwrap()
            .into_handshake_outcome(),
        Err(WireProtocolError::UnexpectedHandshakeRequestId)
    ));
    assert!(matches!(
        ProtocolResultV1::fatal(Some(Uuid::from_u128(0x305)), FatalCode::Unavailable)
            .unwrap()
            .into_handshake_outcome(),
        Err(WireProtocolError::UnexpectedHandshakeRequestId)
    ));
}
