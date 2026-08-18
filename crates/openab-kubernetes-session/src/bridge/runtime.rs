use super::{BridgeIdentity, BridgeIdentityError, SESSION_ATTEMPT_ID_ENV, SESSION_KEY_ENV};
use crate::wire::BrokerMappingExpectationV1;
use std::env;
use thiserror::Error;

pub const SESSION_MAPPING_EXPECTATION_ENV: &str = "OPENAB_SESSION_MAPPING_EXPECTATION";
pub const MAPPING_ABSENT_INITIALIZATION_ERROR_CODE: i64 = -32041;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BridgeEnvironmentError {
    #[error(transparent)]
    Identity(#[from] BridgeIdentityError),
    #[error("required broker-owned environment variable {name} is unavailable")]
    EnvironmentVariable { name: &'static str },
    #[error("{SESSION_MAPPING_EXPECTATION_ENV} must be exactly either absent or present")]
    InvalidMappingExpectation,
}

/// Broker-owned launch context captured before the bridge performs network I/O.
///
/// The raw logical session key is reduced to [`BridgeIdentity`], while the
/// mapping expectation remains reconciliation input rather than authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeEnvironment {
    identity: BridgeIdentity,
    broker_mapping_expectation: BrokerMappingExpectationV1,
}

impl BridgeEnvironment {
    pub fn from_environment(
        scope: &str,
        requested_profile_name: &str,
    ) -> Result<Self, BridgeEnvironmentError> {
        let session_key =
            env::var(SESSION_KEY_ENV).map_err(|_| BridgeEnvironmentError::EnvironmentVariable {
                name: SESSION_KEY_ENV,
            })?;
        let attempt_id = env::var(SESSION_ATTEMPT_ID_ENV).map_err(|_| {
            BridgeEnvironmentError::EnvironmentVariable {
                name: SESSION_ATTEMPT_ID_ENV,
            }
        })?;
        let mapping_expectation = env::var(SESSION_MAPPING_EXPECTATION_ENV).map_err(|_| {
            BridgeEnvironmentError::EnvironmentVariable {
                name: SESSION_MAPPING_EXPECTATION_ENV,
            }
        })?;
        Self::from_values(
            scope,
            &session_key,
            &attempt_id,
            requested_profile_name,
            &mapping_expectation,
        )
    }

    pub fn from_values(
        scope: &str,
        logical_session_key: &str,
        broker_attempt_id: &str,
        requested_profile_name: &str,
        mapping_expectation: &str,
    ) -> Result<Self, BridgeEnvironmentError> {
        let identity = BridgeIdentity::from_values(
            scope,
            logical_session_key,
            broker_attempt_id,
            requested_profile_name,
        )?;
        let broker_mapping_expectation = match mapping_expectation {
            "absent" => BrokerMappingExpectationV1::Absent,
            "present" => BrokerMappingExpectationV1::Present,
            _ => return Err(BridgeEnvironmentError::InvalidMappingExpectation),
        };
        Ok(Self {
            identity,
            broker_mapping_expectation,
        })
    }

    pub fn identity(&self) -> &BridgeIdentity {
        &self.identity
    }

    pub fn broker_mapping_expectation(&self) -> BrokerMappingExpectationV1 {
        self.broker_mapping_expectation
    }

    pub fn into_parts(self) -> (BridgeIdentity, BrokerMappingExpectationV1) {
        (self.identity, self.broker_mapping_expectation)
    }
}
