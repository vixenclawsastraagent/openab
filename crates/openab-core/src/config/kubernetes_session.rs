use super::{AgentConfig, Config};
use anyhow::{anyhow, ensure, Result};
use serde::Deserialize;
use std::path::Path;

const DEFAULT_CREDENTIAL_FILE: &str = "/var/run/secrets/openab-session/token";
const MAX_SCOPE_BYTES: usize = 253;

/// Opt-in Kubernetes session-isolation add-on configuration.
///
/// Presence selects the trusted ACP bridge instead of a local agent process.
/// The bridge executable is packaged only in the add-on-flavoured image.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesSessionConfig {
    /// Authenticated controller relay endpoint.
    pub controller_url: String,
    /// Cluster-owned, versioned worker profile.
    pub profile: String,
    /// Stable team/agent ownership boundary for lifecycle state and quotas.
    pub scope: String,
    /// Projected credential read by the trusted bridge, never by workers.
    #[serde(default = "default_credential_file")]
    pub credential_file: String,
    /// Optional PEM CA bundle used by the trusted bridge for the controller.
    #[serde(default)]
    pub controller_ca_file: Option<String>,
}

fn default_credential_file() -> String {
    DEFAULT_CREDENTIAL_FILE.to_string()
}

impl KubernetesSessionConfig {
    fn validate(&self) -> Result<()> {
        let controller_url = reqwest::Url::parse(&self.controller_url).map_err(|error| {
            anyhow!("kubernetes_session.controller_url must be a valid URL: {error}")
        })?;
        ensure!(
            controller_url.scheme() == "wss",
            "kubernetes_session.controller_url must use wss://"
        );
        ensure!(
            controller_url
                .host_str()
                .is_some_and(|host| !host.is_empty()),
            "kubernetes_session.controller_url must include a host"
        );
        ensure!(
            controller_url.username().is_empty() && controller_url.password().is_none(),
            "kubernetes_session.controller_url must not include user credentials"
        );
        ensure!(
            is_kubernetes_dns_label(&self.profile),
            "kubernetes_session.profile must be a lowercase Kubernetes DNS label"
        );
        ensure!(
            !self.scope.trim().is_empty(),
            "kubernetes_session.scope must not be empty"
        );
        ensure!(
            self.scope == self.scope.trim(),
            "kubernetes_session.scope must not have leading or trailing whitespace"
        );
        ensure!(
            self.scope.len() <= MAX_SCOPE_BYTES,
            "kubernetes_session.scope must be {MAX_SCOPE_BYTES} bytes or fewer"
        );
        ensure!(
            !self.credential_file.trim().is_empty(),
            "kubernetes_session.credential_file must not be empty"
        );
        ensure!(
            is_absolute_credential_path(&self.credential_file),
            "kubernetes_session.credential_file must be an absolute path"
        );
        if let Some(controller_ca_file) = &self.controller_ca_file {
            ensure!(
                !controller_ca_file.trim().is_empty(),
                "kubernetes_session.controller_ca_file must not be empty"
            );
            ensure!(
                controller_ca_file.starts_with('/'),
                "kubernetes_session.controller_ca_file must be an absolute Linux path"
            );
        }
        Ok(())
    }

    fn bridge_agent(&self, agent: &AgentConfig) -> AgentConfig {
        let mut args = vec![
            "bridge".to_string(),
            "--controller-url".to_string(),
            self.controller_url.clone(),
            "--profile".to_string(),
            self.profile.clone(),
            "--scope".to_string(),
            self.scope.clone(),
            "--credential-file".to_string(),
            self.credential_file.clone(),
        ];
        if let Some(controller_ca_file) = &self.controller_ca_file {
            args.push("--controller-ca-file".to_string());
            args.push(controller_ca_file.clone());
        }

        AgentConfig {
            command: "openab-kubernetes-session".to_string(),
            args,
            working_dir: agent.working_dir.clone(),
            env: agent.env.clone(),
            inherit_env: agent.inherit_env.clone(),
            command_explicit: true,
        }
    }
}

fn is_kubernetes_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn is_absolute_credential_path(value: &str) -> bool {
    // Kubernetes worker and broker images are Linux-based. Accepting a leading
    // slash explicitly also keeps config validation deterministic on non-Unix
    // hosts used to build or test OpenAB.
    value.starts_with('/') || Path::new(value).is_absolute()
}

fn configured_reserved_session_env(agent: &AgentConfig) -> Option<&'static str> {
    crate::acp::RESERVED_SESSION_ENV
        .iter()
        .copied()
        .find(|reserved| {
            agent
                .env
                .keys()
                .chain(agent.inherit_env.iter())
                .any(|key| key.eq_ignore_ascii_case(reserved))
        })
}

pub(super) fn apply(config: &mut Config) -> Result<()> {
    let Some(kubernetes_session) = config.kubernetes_session.as_ref() else {
        return Ok(());
    };

    ensure!(
        config.agentcore.is_none(),
        "[kubernetes_session] and [agentcore] are mutually exclusive"
    );
    ensure!(
        !config.agent.command_explicit,
        "[kubernetes_session] cannot be combined with an explicit [agent].command"
    );
    kubernetes_session.validate()?;
    if let Some(reserved) = configured_reserved_session_env(&config.agent) {
        anyhow::bail!(
            "[kubernetes_session] reserves {reserved}; remove it from agent.env and agent.inherit_env"
        );
    }

    config.agent = kubernetes_session.bridge_agent(&config.agent);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_config_str;

    const CONTROLLER_URL: &str = "wss://session-controller.openab-system.svc/relay";
    const PROFILE: &str = "codex-strict";
    const SCOPE: &str = "team-a";

    fn session_config(
        controller_url: &str,
        profile: &str,
        scope: &str,
        credential_file: Option<&str>,
        section_extra: &str,
        trailing: &str,
    ) -> String {
        let credential_file = credential_file
            .map(|path| format!("credential_file = \"{path}\"\n"))
            .unwrap_or_default();
        format!(
            r#"
[discord]
bot_token = "x"

[kubernetes_session]
controller_url = "{controller_url}"
profile = "{profile}"
scope = "{scope}"
{credential_file}{section_extra}
{trailing}
"#
        )
    }

    fn valid_config(trailing: &str) -> String {
        session_config(CONTROLLER_URL, PROFILE, SCOPE, None, "", trailing)
    }

    #[test]
    fn kubernetes_session_is_absent_by_default() {
        let cfg = parse_config_str("[discord]\nbot_token = \"x\"\n", "test").unwrap();
        assert!(cfg.kubernetes_session.is_none());
        assert!(!cfg.agent.command_explicit);
    }

    #[test]
    fn kubernetes_session_selects_bridge_and_preserves_bridge_env() {
        let cfg = parse_config_str(
            &valid_config(
                r#"
[agent]
inherit_env = ["HTTPS_PROXY"]
"#,
            ),
            "test",
        )
        .unwrap();

        let runtime = cfg.kubernetes_session.as_ref().unwrap();
        assert_eq!(runtime.scope, SCOPE);
        assert_eq!(runtime.credential_file, DEFAULT_CREDENTIAL_FILE);
        assert_eq!(runtime.controller_ca_file, None);
        assert_eq!(cfg.agent.command, "openab-kubernetes-session");
        assert_eq!(
            cfg.agent.args,
            vec![
                "bridge",
                "--controller-url",
                CONTROLLER_URL,
                "--profile",
                PROFILE,
                "--scope",
                SCOPE,
                "--credential-file",
                DEFAULT_CREDENTIAL_FILE,
            ]
        );
        assert_eq!(cfg.agent.inherit_env, vec!["HTTPS_PROXY"]);
    }

    #[test]
    fn kubernetes_session_and_broker_local_mcp_can_coexist() {
        let cfg = parse_config_str(
            &valid_config(
                r#"
[mcp]
listen = "127.0.0.1:8848"
"#,
            ),
            "test",
        )
        .unwrap();

        assert!(cfg.kubernetes_session.is_some());
        assert!(cfg.mcp.is_some());
        assert_eq!(cfg.agent.command, "openab-kubernetes-session");
    }

    #[test]
    fn kubernetes_session_appends_controller_ca_file_to_bridge_args() {
        const CONTROLLER_CA_FILE: &str = "/var/run/secrets/openab-session/ca.crt";
        let cfg = parse_config_str(
            &session_config(
                CONTROLLER_URL,
                PROFILE,
                SCOPE,
                None,
                &format!("controller_ca_file = \"{CONTROLLER_CA_FILE}\"\n"),
                "",
            ),
            "test",
        )
        .unwrap();

        assert_eq!(
            cfg.kubernetes_session
                .as_ref()
                .unwrap()
                .controller_ca_file
                .as_deref(),
            Some(CONTROLLER_CA_FILE)
        );
        assert_eq!(
            cfg.agent.args,
            vec![
                "bridge",
                "--controller-url",
                CONTROLLER_URL,
                "--profile",
                PROFILE,
                "--scope",
                SCOPE,
                "--credential-file",
                DEFAULT_CREDENTIAL_FILE,
                "--controller-ca-file",
                CONTROLLER_CA_FILE,
            ]
        );
    }

    #[test]
    fn unknown_agent_session_context_is_ignored_and_keeps_isolation_disabled() {
        let cfg = parse_config_str(
            "[discord]\nbot_token = \"x\"\n[agent]\nsession_context = \"openab-v1\"\n",
            "test",
        )
        .unwrap();

        assert!(cfg.kubernetes_session.is_none());
        assert!(!cfg.agent.command_explicit);
    }

    #[test]
    fn kubernetes_session_rejects_configured_session_key_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent.env]
openab_session_key = "spoofed"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_KEY"));
    }

    #[test]
    fn kubernetes_session_rejects_inherited_session_key() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent]
inherit_env = ["OPENAB_SESSION_KEY"]
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_KEY"));
    }

    #[test]
    fn kubernetes_session_rejects_configured_attempt_id_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent.env]
openab_session_attempt_id = "spoofed"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_ATTEMPT_ID"));
    }

    #[test]
    fn kubernetes_session_rejects_inherited_attempt_id() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent]
inherit_env = ["OPENAB_SESSION_ATTEMPT_ID"]
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_ATTEMPT_ID"));
    }

    #[test]
    fn kubernetes_session_rejects_configured_mapping_expectation_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent.env]
openab_session_mapping_expectation = "spoofed"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("OPENAB_SESSION_MAPPING_EXPECTATION"));
    }

    #[test]
    fn kubernetes_session_rejects_inherited_mapping_expectation_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent]
inherit_env = ["openab_session_mapping_expectation"]
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("OPENAB_SESSION_MAPPING_EXPECTATION"));
    }

    #[test]
    fn kubernetes_session_rejects_configured_facade_token_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent.env]
openab_session_token = "broker-local-token"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_TOKEN"));
    }

    #[test]
    fn kubernetes_session_rejects_inherited_facade_token_case_insensitively() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent]
inherit_env = ["openab_session_token"]
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("OPENAB_SESSION_TOKEN"));
    }

    #[test]
    fn kubernetes_session_rejects_agentcore() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agentcore]
runtime_arn = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/example"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn kubernetes_session_rejects_explicit_local_command() {
        let err = parse_config_str(
            &valid_config(
                r#"
[agent]
command = "codex-acp"
"#,
            ),
            "test",
        )
        .unwrap_err();
        assert!(err.to_string().contains("explicit [agent].command"));
    }

    #[test]
    fn kubernetes_session_rejects_insecure_controller_url() {
        let config = session_config(
            "ws://session-controller.openab-system.svc/relay",
            PROFILE,
            SCOPE,
            None,
            "",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("must use wss://"));
    }

    #[test]
    fn kubernetes_session_rejects_empty_scope() {
        let config = session_config(CONTROLLER_URL, PROFILE, "   ", None, "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("scope must not be empty"));
    }

    #[test]
    fn kubernetes_session_rejects_unknown_fields() {
        let config = session_config(
            CONTROLLER_URL,
            PROFILE,
            SCOPE,
            None,
            "worker_image = \"untrusted:latest\"\n",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn kubernetes_session_rejects_controller_url_without_host() {
        let config = session_config("wss://", PROFILE, SCOPE, None, "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("controller_url"));
    }

    #[test]
    fn kubernetes_session_rejects_controller_url_userinfo() {
        let config = session_config(
            "wss://user:password@session-controller.openab-system.svc/relay",
            PROFILE,
            SCOPE,
            None,
            "",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err
            .to_string()
            .contains("must not include user credentials"));
    }

    #[test]
    fn kubernetes_session_rejects_relative_credential_file() {
        let config = session_config(
            CONTROLLER_URL,
            PROFILE,
            SCOPE,
            Some("secrets/session-token"),
            "",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("must be an absolute path"));
    }

    #[test]
    fn kubernetes_session_rejects_empty_credential_file() {
        let config = session_config(CONTROLLER_URL, PROFILE, SCOPE, Some(""), "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn kubernetes_session_rejects_empty_controller_ca_file() {
        let config = session_config(
            CONTROLLER_URL,
            PROFILE,
            SCOPE,
            None,
            "controller_ca_file = \"\"\n",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err
            .to_string()
            .contains("controller_ca_file must not be empty"));
    }

    #[test]
    fn kubernetes_session_rejects_non_linux_controller_ca_path() {
        let config = session_config(
            CONTROLLER_URL,
            PROFILE,
            SCOPE,
            None,
            "controller_ca_file = \"secrets/controller-ca.crt\"\n",
            "",
        );
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err
            .to_string()
            .contains("controller_ca_file must be an absolute Linux path"));
    }

    #[test]
    fn kubernetes_session_rejects_invalid_profile() {
        let config = session_config(CONTROLLER_URL, "Codex_Strict", SCOPE, None, "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("profile"));
    }

    #[test]
    fn kubernetes_session_accepts_non_dns_scope() {
        let config = session_config(CONTROLLER_URL, PROFILE, "Team A / platform", None, "", "");
        let cfg = parse_config_str(&config, "test").unwrap();
        assert_eq!(
            cfg.kubernetes_session.as_ref().unwrap().scope,
            "Team A / platform"
        );
    }

    #[test]
    fn kubernetes_session_rejects_scope_over_limit() {
        let scope = "a".repeat(MAX_SCOPE_BYTES + 1);
        let config = session_config(CONTROLLER_URL, PROFILE, &scope, None, "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("253 bytes or fewer"));
    }

    #[test]
    fn kubernetes_session_rejects_scope_edge_whitespace() {
        let config = session_config(CONTROLLER_URL, PROFILE, " team-a ", None, "", "");
        let err = parse_config_str(&config, "test").unwrap_err();
        assert!(err.to_string().contains("leading or trailing whitespace"));
    }

    #[test]
    fn legacy_mode_does_not_reserve_isolation_environment_names() {
        let cfg = parse_config_str(
            r#"
[discord]
bot_token = "x"

[agent.env]
OPENAB_SESSION_KEY = "legacy-value"
OPENAB_SESSION_ATTEMPT_ID = "legacy-attempt"
OPENAB_SESSION_MAPPING_EXPECTATION = "legacy-expectation"
OPENAB_SESSION_TOKEN = "legacy-facade-token"
"#,
            "test",
        )
        .unwrap();

        assert!(cfg.kubernetes_session.is_none());
        assert_eq!(cfg.agent.env["OPENAB_SESSION_KEY"], "legacy-value");
        assert_eq!(cfg.agent.env["OPENAB_SESSION_ATTEMPT_ID"], "legacy-attempt");
        assert_eq!(
            cfg.agent.env["OPENAB_SESSION_MAPPING_EXPECTATION"],
            "legacy-expectation"
        );
        assert_eq!(
            cfg.agent.env["OPENAB_SESSION_TOKEN"],
            "legacy-facade-token"
        );
    }
}
