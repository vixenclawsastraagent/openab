use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;

const DIGEST_BYTES: usize = 32;
const DIGEST_HEX_LEN: usize = DIGEST_BYTES * 2;
const RESOURCE_DIGEST_HEX_LEN: usize = 40;
const GENERATION_RESOURCE_DIGEST_HEX_LEN: usize = 24;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdentityError {
    #[error("digest must be exactly 64 lowercase hexadecimal characters")]
    InvalidDigest,
    #[error("generation must be greater than zero")]
    InvalidGeneration,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId([u8; DIGEST_BYTES]);

impl ScopeId {
    pub fn derive(scope: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"openab-scope-v1");
        hasher.update([0]);
        hasher.update(scope.as_bytes());
        Self(hasher.finalize().into())
    }

    pub fn as_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for ScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ScopeId")
            .field(&self.as_hex())
            .finish()
    }
}

impl Serialize for ScopeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for ScopeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_digest(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId([u8; DIGEST_BYTES]);

impl SessionId {
    /// Derive a deployment-scoped logical session identity without retaining
    /// the raw scope or chat-thread key.
    pub fn derive(scope: &str, logical_session_key: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(scope.as_bytes());
        hasher.update([0]);
        hasher.update(logical_session_key.as_bytes());
        Self(hasher.finalize().into())
    }

    pub fn as_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SessionId")
            .field(&self.as_hex())
            .finish()
    }
}

impl Serialize for SessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_digest(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

fn parse_digest(value: &str) -> Result<[u8; DIGEST_BYTES], IdentityError> {
    if value.len() != DIGEST_HEX_LEN
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(IdentityError::InvalidDigest);
    }

    let mut digest = [0_u8; DIGEST_BYTES];
    hex::decode_to_slice(value, &mut digest).map_err(|_| IdentityError::InvalidDigest)?;
    Ok(digest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceNames {
    session_id: SessionId,
}

impl ResourceNames {
    pub fn new(session_id: SessionId) -> Self {
        Self { session_id }
    }

    pub fn anchor(&self) -> String {
        format!(
            "oab-session-{}",
            &self.session_id.as_hex()[..RESOURCE_DIGEST_HEX_LEN]
        )
    }

    pub fn pvc(&self) -> String {
        format!(
            "oab-work-{}",
            &self.session_id.as_hex()[..RESOURCE_DIGEST_HEX_LEN]
        )
    }

    pub fn pod(&self, generation: u64) -> Result<String, IdentityError> {
        self.generation_name("oab-worker", generation)
    }

    pub fn registration_secret(&self, generation: u64) -> Result<String, IdentityError> {
        self.generation_name("oab-register", generation)
    }

    pub fn service_account(&self, generation: u64) -> Result<String, IdentityError> {
        self.generation_name("oab-worker-sa", generation)
    }

    fn generation_name(&self, prefix: &str, generation: u64) -> Result<String, IdentityError> {
        if generation == 0 {
            return Err(IdentityError::InvalidGeneration);
        }
        Ok(format!(
            "{prefix}-{}-g{generation}",
            &self.session_id.as_hex()[..GENERATION_RESOURCE_DIGEST_HEX_LEN]
        ))
    }
}
