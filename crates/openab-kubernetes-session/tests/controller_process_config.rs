#![cfg(feature = "controller-runtime")]

use openab_kubernetes_session::controller::ControllerEndpointConfig;
use openab_kubernetes_session::controller_process_config::{
    ControllerProcessConfigV1, ProcessConfigError, MAX_PROCESS_CONFIG_BYTES,
};
use openab_kubernetes_session::identity::ScopeId;
use openab_kubernetes_session::wire::MAX_ACP_FRAME_BYTES;
use std::io::{self, Cursor, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

const VALID_CONFIG: &str = r#"
schema_version = 1
scope = "team-a"
worker_namespace = "openab-sessions"
profiles_file = "/etc/openab-session/profiles.toml"

[listen]
relay_address = "0.0.0.0:8443"
probe_address = "0.0.0.0:8080"

[tls]
certificate_file = "/var/run/openab-session/tls/tls.crt"
private_key_file = "/var/run/openab-session/tls/tls.key"

[authentication]
bridge_credential_file = "/var/run/openab-session/auth/token"

[relay]
max_connections = 128
queue_capacity = 16
byte_budget_bytes = 134217728
"#;

#[test]
fn parses_the_complete_strict_process_contract() {
    let config = ControllerProcessConfigV1::from_toml(VALID_CONFIG).expect("valid process config");

    assert_eq!(config.scope_id(), ScopeId::derive("team-a"));
    assert_eq!(config.worker_namespace(), "openab-sessions");
    assert_eq!(
        config.profiles_file(),
        Path::new("/etc/openab-session/profiles.toml")
    );
    assert_eq!(
        config.relay_address(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8443)
    );
    assert_eq!(
        config.probe_address(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8080)
    );
    assert_eq!(
        config.tls_certificate_file(),
        Path::new("/var/run/openab-session/tls/tls.crt")
    );
    assert_eq!(
        config.tls_private_key_file(),
        Path::new("/var/run/openab-session/tls/tls.key")
    );
    assert_eq!(
        config.bridge_credential_file(),
        Path::new("/var/run/openab-session/auth/token")
    );
    assert_eq!(config.tls_handshake_timeout(), Duration::from_secs(10));
    assert_eq!(config.relay_queue_capacity().get(), 16);
    assert_eq!(config.relay_byte_budget_bytes().get(), 134_217_728);
    assert_eq!(
        config.endpoint_config(),
        ControllerEndpointConfig::new(
            NonZeroUsize::new(128).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(300),
            Duration::from_secs(120),
            Duration::from_secs(30),
            Duration::from_millis(250),
        )
        .unwrap()
    );
    assert_eq!(
        config.supervisor_config().maintenance_interval(),
        Duration::from_secs(30)
    );
    assert_eq!(
        config.supervisor_config().shutdown_grace(),
        Duration::from_secs(30)
    );
}

#[test]
fn rejects_unknown_fields_at_every_nesting_level() {
    for source in [
        VALID_CONFIG.replace("schema_version = 1", "schema_version = 1\nextra = true"),
        VALID_CONFIG.replace(
            "relay_address = \"0.0.0.0:8443\"",
            "relay_address = \"0.0.0.0:8443\"\nextra = true",
        ),
        VALID_CONFIG.replace(
            "certificate_file = \"/var/run/openab-session/tls/tls.crt\"",
            "certificate_file = \"/var/run/openab-session/tls/tls.crt\"\nextra = true",
        ),
        VALID_CONFIG.replace(
            "bridge_credential_file = \"/var/run/openab-session/auth/token\"",
            "bridge_credential_file = \"/var/run/openab-session/auth/token\"\nextra = true",
        ),
        VALID_CONFIG.replace(
            "max_connections = 128",
            "max_connections = 128\nextra = true",
        ),
    ] {
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::Decode)
        ));
    }
}

#[test]
fn rejects_unsupported_schema_versions() {
    let source = VALID_CONFIG.replace("schema_version = 1", "schema_version = 2");
    assert!(matches!(
        ControllerProcessConfigV1::from_toml(&source),
        Err(ProcessConfigError::UnsupportedSchemaVersion(2))
    ));
}

#[test]
fn rejects_missing_required_fields_at_every_nesting_level() {
    for source in [
        VALID_CONFIG.replace("scope = \"team-a\"\n", ""),
        VALID_CONFIG.replace("probe_address = \"0.0.0.0:8080\"\n", ""),
        VALID_CONFIG.replace(
            "private_key_file = \"/var/run/openab-session/tls/tls.key\"\n",
            "",
        ),
        VALID_CONFIG.replace(
            "bridge_credential_file = \"/var/run/openab-session/auth/token\"\n",
            "",
        ),
        VALID_CONFIG.replace("queue_capacity = 16\n", ""),
    ] {
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::Decode)
        ));
    }
}

#[test]
fn rejects_scope_values_outside_the_broker_configuration_contract() {
    for scope in ["", " team-a", "team-a ", &"x".repeat(254)] {
        let source = VALID_CONFIG.replace("scope = \"team-a\"", &format!("scope = \"{scope}\""));
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::InvalidField { field: "scope", .. })
        ));
    }
}

#[test]
fn rejects_invalid_namespace_and_listener_addresses() {
    for (needle, replacement, field) in [
        (
            "worker_namespace = \"openab-sessions\"",
            "worker_namespace = \"OpenAB\"",
            "worker_namespace",
        ),
        (
            "relay_address = \"0.0.0.0:8443\"",
            "relay_address = \"controller:8443\"",
            "listen.relay_address",
        ),
        (
            "probe_address = \"0.0.0.0:8080\"",
            "probe_address = \"127.0.0.1:0\"",
            "listen.probe_address",
        ),
    ] {
        let source = VALID_CONFIG.replace(needle, replacement);
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::InvalidField { field: actual, .. }) if actual == field
        ));
    }

    let source = VALID_CONFIG.replace(
        "probe_address = \"0.0.0.0:8080\"",
        "probe_address = \"0.0.0.0:8443\"",
    );
    assert!(matches!(
        ControllerProcessConfigV1::from_toml(&source),
        Err(ProcessConfigError::InvalidField {
            field: "listen.probe_address",
            ..
        })
    ));
}

#[test]
fn rejects_relative_or_empty_mounted_file_paths() {
    for (needle, replacement, field) in [
        (
            "profiles_file = \"/etc/openab-session/profiles.toml\"",
            "profiles_file = \"profiles.toml\"",
            "profiles_file",
        ),
        (
            "certificate_file = \"/var/run/openab-session/tls/tls.crt\"",
            "certificate_file = \"\"",
            "tls.certificate_file",
        ),
        (
            "private_key_file = \"/var/run/openab-session/tls/tls.key\"",
            "private_key_file = \"tls.key\"",
            "tls.private_key_file",
        ),
        (
            "bridge_credential_file = \"/var/run/openab-session/auth/token\"",
            "bridge_credential_file = \"token\"",
            "authentication.bridge_credential_file",
        ),
        (
            "profiles_file = \"/etc/openab-session/profiles.toml\"",
            &format!("profiles_file = \"/{}\"", "x".repeat(4_096)),
            "profiles_file",
        ),
        (
            "profiles_file = \"/etc/openab-session/profiles.toml\"",
            "profiles_file = \"/tmp/\\u0000profiles.toml\"",
            "profiles_file",
        ),
    ] {
        let source = VALID_CONFIG.replace(needle, replacement);
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::InvalidField { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn enforces_resource_safety_bounds() {
    let cases = [
        (
            "max_connections = 128",
            "max_connections = 1",
            "relay.max_connections",
        ),
        (
            "max_connections = 128",
            "max_connections = 20001",
            "relay.max_connections",
        ),
        (
            "queue_capacity = 16",
            "queue_capacity = 0",
            "relay.queue_capacity",
        ),
        (
            "queue_capacity = 16",
            "queue_capacity = 1025",
            "relay.queue_capacity",
        ),
        (
            "byte_budget_bytes = 134217728",
            &format!("byte_budget_bytes = {}", MAX_ACP_FRAME_BYTES - 1),
            "relay.byte_budget_bytes",
        ),
        (
            "byte_budget_bytes = 134217728",
            &format!("byte_budget_bytes = {}", 16 * MAX_ACP_FRAME_BYTES + 1),
            "relay.byte_budget_bytes",
        ),
    ];

    for (needle, replacement, field) in cases {
        let source = VALID_CONFIG.replace(needle, replacement);
        assert!(matches!(
            ControllerProcessConfigV1::from_toml(&source),
            Err(ProcessConfigError::InvalidField { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn rejects_process_toml_over_the_parser_ceiling_before_decode() {
    let source = format!("{VALID_CONFIG}\n#{}", "x".repeat(MAX_PROCESS_CONFIG_BYTES));
    assert!(matches!(
        ControllerProcessConfigV1::from_toml(&source),
        Err(ProcessConfigError::TooLarge)
    ));
}

#[test]
fn reads_process_toml_through_the_bounded_utf8_api() {
    let config = ControllerProcessConfigV1::from_reader(Cursor::new(VALID_CONFIG.as_bytes()))
        .expect("valid bounded process config");

    assert_eq!(config.scope_id(), ScopeId::derive("team-a"));
}

#[test]
fn bounded_process_reader_accepts_the_exact_ceiling() {
    let prefix = format!("{VALID_CONFIG}\n#");
    let source = format!(
        "{prefix}{}",
        "x".repeat(MAX_PROCESS_CONFIG_BYTES - prefix.len())
    );
    assert_eq!(source.len(), MAX_PROCESS_CONFIG_BYTES);

    assert!(ControllerProcessConfigV1::from_reader(Cursor::new(source)).is_ok());
}

#[test]
fn bounded_process_reader_rejects_oversize_and_non_utf8_input() {
    let oversized = vec![b'#'; MAX_PROCESS_CONFIG_BYTES + 1];
    assert!(matches!(
        ControllerProcessConfigV1::from_reader(Cursor::new(oversized)),
        Err(ProcessConfigError::TooLarge)
    ));
    assert!(matches!(
        ControllerProcessConfigV1::from_reader(Cursor::new([0xff])),
        Err(ProcessConfigError::InvalidUtf8)
    ));
}

#[test]
fn process_reader_errors_are_sanitized() {
    let error = ControllerProcessConfigV1::from_reader(SensitiveReadFailure)
        .err()
        .expect("reader must fail");
    let rendered = error.to_string();

    assert_eq!(error, ProcessConfigError::Read);
    assert!(!rendered.contains("private-team"));
    assert!(!rendered.contains("/var/run/openab-session"));
}

#[test]
fn errors_do_not_echo_sensitive_or_operator_supplied_values() {
    let source = VALID_CONFIG.replace("scope = \"team-a\"", "scope = \" private-team \"");
    let error = ControllerProcessConfigV1::from_toml(&source)
        .err()
        .expect("invalid scope");
    let rendered = error.to_string();

    assert!(!rendered.contains("private-team"));
    assert!(!rendered.contains("/var/run/openab-session"));
}

struct SensitiveReadFailure;

impl Read for SensitiveReadFailure {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other(
            "private-team at /var/run/openab-session/profiles.toml",
        ))
    }
}
