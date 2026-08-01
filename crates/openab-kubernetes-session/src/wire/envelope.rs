use super::{
    AcpMessageV1, ActivationRequestV1, ActivationResponseV1, LifecycleRequestV1, ProtocolResultV1,
    WireMessage, WorkerRegistrationV1, MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
};
use serde::{Deserialize, Serialize};

/// Messages accepted from one authenticated broker-side bridge connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum BridgeToControllerV1 {
    Activation(ActivationRequestV1),
    Acp(AcpMessageV1),
    Lifecycle(LifecycleRequestV1),
}

/// Messages a controller may send to one authenticated broker-side bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ControllerToBridgeV1 {
    Activation(ActivationResponseV1),
    Acp(AcpMessageV1),
    ProtocolResult(ProtocolResultV1),
}

/// Messages accepted from one bootstrap-authenticated worker connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WorkerToControllerV1 {
    Registration(WorkerRegistrationV1),
    Acp(AcpMessageV1),
}

/// Messages a controller may send to one registered worker connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ControllerToWorkerV1 {
    ProtocolResult(ProtocolResultV1),
    Acp(AcpMessageV1),
}

macro_rules! impl_direction_wire_message {
    ($message:ty, $acp:path) => {
        impl super::sealed::Sealed for $message {}

        impl WireMessage for $message {
            const MAX_FRAME_BYTES: usize = MAX_ACP_FRAME_BYTES;

            fn encoded_frame_limit(&self) -> usize {
                if matches!(self, $acp(_)) {
                    MAX_ACP_FRAME_BYTES
                } else {
                    MAX_CONTROL_FRAME_BYTES
                }
            }
        }
    };
}

impl_direction_wire_message!(BridgeToControllerV1, BridgeToControllerV1::Acp);
impl_direction_wire_message!(ControllerToBridgeV1, ControllerToBridgeV1::Acp);
impl_direction_wire_message!(WorkerToControllerV1, WorkerToControllerV1::Acp);
impl_direction_wire_message!(ControllerToWorkerV1, ControllerToWorkerV1::Acp);
