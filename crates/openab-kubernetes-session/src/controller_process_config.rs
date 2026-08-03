//! Strict, local process configuration for the opt-in controller executable.
//!
//! Worker policy remains in [`crate::profile_config`]. This module owns only
//! process-local transport, mounted-file, and supervision settings so the
//! add-on can be deployed without changing default OpenAB configuration.

use crate::controller::{ControllerEndpointConfig, ControllerSupervisorConfig};
use crate::identity::ScopeId;
use crate::wire::MAX_ACP_FRAME_BYTES;
use serde::Deserialize;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

const SUPPORTED_SCHEMA_VERSION: u32 = 1;
/// Maximum accepted UTF-8 bytes in one controller process TOML document.
pub const MAX_PROCESS_CONFIG_BYTES: usize = 64 * 1024;
const MAX_SCOPE_BYTES: usize = 253;
const MAX_MOUNTED_PATH_BYTES: usize = 4 * 1024;
// Two admitted sockets per active worker, aligned with the profile parser's
// 10,000-worker implementation ceiling.
const MAX_CONNECTIONS: u64 = 20_000;
const MAX_QUEUE_CAPACITY: u64 = 1_024;
const MAX_RELAY_BYTE_BUDGET: u64 = 16 * MAX_ACP_FRAME_BYTES as u64;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const WEBSOCKET_UPGRADE_TIMEOUT: Duration = Duration::from_secs(10);
const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(300);
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(120);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const RELEASE_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Validated process-local settings for one fixed-scope controller.
///
/// The raw scope is reduced to [`ScopeId`] during parsing and is not retained.
/// Credential and private-key contents are never accepted in this structure;
/// only absolute paths to operator-mounted files are allowed.
#[derive(Clone)]
pub struct ControllerProcessConfigV1 {
    scope_id: ScopeId,
    worker_namespace: String,
    profiles_file: PathBuf,
    relay_address: SocketAddr,
    probe_address: SocketAddr,
    tls_certificate_file: PathBuf,
    tls_private_key_file: PathBuf,
    bridge_credential_file: PathBuf,
    relay_queue_capacity: NonZeroUsize,
    relay_byte_budget_bytes: NonZeroUsize,
    endpoint_config: ControllerEndpointConfig,
    supervisor_config: ControllerSupervisorConfig,
}

impl ControllerProcessConfigV1 {
    /// Parse and validate the complete version-one process configuration.
    pub fn from_toml(source: &str) -> Result<Self, ProcessConfigError> {
        if source.len() > MAX_PROCESS_CONFIG_BYTES {
            return Err(ProcessConfigError::TooLarge);
        }
        let decoded: ProcessConfigDto =
            toml::from_str(source).map_err(|_| ProcessConfigError::Decode)?;
        if decoded.schema_version != SUPPORTED_SCHEMA_VERSION {
            return Err(ProcessConfigError::UnsupportedSchemaVersion(
                decoded.schema_version,
            ));
        }

        validate_scope(&decoded.scope)?;
        validate_dns_label("worker_namespace", &decoded.worker_namespace)?;
        let profiles_file = absolute_path("profiles_file", decoded.profiles_file)?;

        let relay_address = socket_address("listen.relay_address", decoded.listen.relay_address)?;
        let probe_address = socket_address("listen.probe_address", decoded.listen.probe_address)?;
        if relay_address.port() == probe_address.port() {
            return Err(invalid(
                "listen.probe_address",
                "must use a different port from the relay listener",
            ));
        }

        let tls_certificate_file =
            absolute_path("tls.certificate_file", decoded.tls.certificate_file)?;
        let tls_private_key_file =
            absolute_path("tls.private_key_file", decoded.tls.private_key_file)?;
        let bridge_credential_file = absolute_path(
            "authentication.bridge_credential_file",
            decoded.authentication.bridge_credential_file,
        )?;
        let max_connections = bounded_usize(
            "relay.max_connections",
            decoded.relay.max_connections,
            2,
            MAX_CONNECTIONS,
        )?;
        let relay_queue_capacity = bounded_usize(
            "relay.queue_capacity",
            decoded.relay.queue_capacity,
            1,
            MAX_QUEUE_CAPACITY,
        )?;
        let relay_byte_budget_bytes = bounded_usize(
            "relay.byte_budget_bytes",
            decoded.relay.byte_budget_bytes,
            MAX_ACP_FRAME_BYTES as u64,
            MAX_RELAY_BYTE_BUDGET,
        )?;
        let endpoint_config = ControllerEndpointConfig::new(
            max_connections,
            WEBSOCKET_UPGRADE_TIMEOUT,
            ACTIVATION_TIMEOUT,
            REGISTRATION_TIMEOUT,
            WRITE_TIMEOUT,
            RELEASE_RETRY_INTERVAL,
        )
        .map_err(|_| invalid("relay", "contains incompatible endpoint limits"))?;
        let supervisor_config =
            ControllerSupervisorConfig::new(MAINTENANCE_INTERVAL, SHUTDOWN_GRACE)
                .map_err(|_| invalid("runtime", "contains incompatible supervisor limits"))?;

        Ok(Self {
            scope_id: ScopeId::derive(&decoded.scope),
            worker_namespace: decoded.worker_namespace,
            profiles_file,
            relay_address,
            probe_address,
            tls_certificate_file,
            tls_private_key_file,
            bridge_credential_file,
            relay_queue_capacity,
            relay_byte_budget_bytes,
            endpoint_config,
            supervisor_config,
        })
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn worker_namespace(&self) -> &str {
        &self.worker_namespace
    }

    pub fn profiles_file(&self) -> &Path {
        &self.profiles_file
    }

    pub fn relay_address(&self) -> SocketAddr {
        self.relay_address
    }

    pub fn probe_address(&self) -> SocketAddr {
        self.probe_address
    }

    pub fn tls_certificate_file(&self) -> &Path {
        &self.tls_certificate_file
    }

    pub fn tls_private_key_file(&self) -> &Path {
        &self.tls_private_key_file
    }

    pub fn bridge_credential_file(&self) -> &Path {
        &self.bridge_credential_file
    }

    pub fn tls_handshake_timeout(&self) -> Duration {
        TLS_HANDSHAKE_TIMEOUT
    }

    pub fn relay_queue_capacity(&self) -> NonZeroUsize {
        self.relay_queue_capacity
    }

    pub fn relay_byte_budget_bytes(&self) -> NonZeroUsize {
        self.relay_byte_budget_bytes
    }

    pub fn endpoint_config(&self) -> ControllerEndpointConfig {
        self.endpoint_config
    }

    pub fn supervisor_config(&self) -> ControllerSupervisorConfig {
        self.supervisor_config
    }
}

/// Sanitized process-configuration failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProcessConfigError {
    #[error("controller process configuration exceeds its size limit")]
    TooLarge,
    #[error("controller process configuration is not valid TOML")]
    Decode,
    #[error("unsupported controller process configuration schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("invalid controller process configuration field {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
}

fn invalid(field: &'static str, reason: &'static str) -> ProcessConfigError {
    ProcessConfigError::InvalidField { field, reason }
}

fn validate_scope(scope: &str) -> Result<(), ProcessConfigError> {
    if scope.is_empty() {
        return Err(invalid("scope", "must not be empty"));
    }
    if scope != scope.trim() {
        return Err(invalid(
            "scope",
            "must not have leading or trailing whitespace",
        ));
    }
    if scope.len() > MAX_SCOPE_BYTES {
        return Err(invalid("scope", "exceeds the supported byte limit"));
    }
    Ok(())
}

fn validate_dns_label(field: &'static str, value: &str) -> Result<(), ProcessConfigError> {
    let bytes = value.as_bytes();
    let valid = (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if !valid {
        return Err(invalid(field, "must be a lowercase Kubernetes DNS label"));
    }
    Ok(())
}

fn absolute_path(field: &'static str, value: String) -> Result<PathBuf, ProcessConfigError> {
    let is_linux_absolute = value.starts_with('/');
    let is_host_absolute = Path::new(&value).is_absolute();
    if value.is_empty()
        || value.len() > MAX_MOUNTED_PATH_BYTES
        || value.contains('\0')
        || !(is_linux_absolute || is_host_absolute)
    {
        return Err(invalid(field, "must be an absolute mounted-file path"));
    }
    Ok(PathBuf::from(value))
}

fn socket_address(field: &'static str, value: String) -> Result<SocketAddr, ProcessConfigError> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| invalid(field, "must be a literal IP socket address"))?;
    if address.port() == 0 {
        return Err(invalid(field, "must use a nonzero port"));
    }
    Ok(address)
}

fn bounded_usize(
    field: &'static str,
    value: u64,
    minimum: u64,
    maximum: u64,
) -> Result<NonZeroUsize, ProcessConfigError> {
    if !(minimum..=maximum).contains(&value) {
        return Err(invalid(field, "is outside the supported safety bounds"));
    }
    let value = usize::try_from(value)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| invalid(field, "is not representable on this platform"))?;
    Ok(value)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfigDto {
    schema_version: u32,
    scope: String,
    worker_namespace: String,
    profiles_file: String,
    listen: ListenDto,
    tls: TlsDto,
    authentication: AuthenticationDto,
    relay: RelayDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListenDto {
    relay_address: String,
    probe_address: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsDto {
    certificate_file: String,
    private_key_file: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthenticationDto {
    bridge_credential_file: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayDto {
    max_connections: u64,
    queue_capacity: u64,
    byte_budget_bytes: u64,
}
