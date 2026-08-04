#[cfg(feature = "agentcore")]
pub mod agentcore;
pub mod connection;
pub(crate) mod lifecycle;
pub mod pool;
pub mod protocol;

pub use connection::ContentBlock;
pub use pool::SessionPool;
pub use protocol::{classify_notification, parse_turn_result, AcpEvent, TurnResult};

/// Broker-owned context passed only to ACP bridge processes that explicitly
/// opt into OpenAB's versioned session runtime contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SessionContextMode {
    #[default]
    None,
    OpenabV1,
}

pub(crate) const SESSION_KEY_ENV: &str = "OPENAB_SESSION_KEY";
pub(crate) const SESSION_ATTEMPT_ID_ENV: &str = "OPENAB_SESSION_ATTEMPT_ID";
pub(crate) const SESSION_MAPPING_EXPECTATION_ENV: &str = "OPENAB_SESSION_MAPPING_EXPECTATION";
pub(crate) const SESSION_TOKEN_ENV: &str = "OPENAB_SESSION_TOKEN";

pub(crate) const RESERVED_SESSION_ENV: [&str; 4] = [
    SESSION_KEY_ENV,
    SESSION_ATTEMPT_ID_ENV,
    SESSION_MAPPING_EXPECTATION_ENV,
    SESSION_TOKEN_ENV,
];
