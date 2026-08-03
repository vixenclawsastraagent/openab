#![cfg(feature = "controller")]

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::node::v1::RuntimeClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::profile_config::{
    ControllerPolicy, ResolvedClusterReferences, TrustedControllerConfigV1, MAX_ACTIVE_WORKERS,
    MAX_LIFECYCLE_TTL_SECONDS, MAX_PROFILE_CONFIG_BYTES,
};
use openab_kubernetes_session::resources::{
    AllowedRuntimeClass, DesiredGeneration, GenerationContext, PinnedSkillsConfigMap,
    RuntimeClassSelection,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1};
use std::io::{self, Cursor, Read};
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const RAW_SCOPE: &str = "organization-secret-team-a";
const ANCHOR_UID: &str = "f6d6f3dd-1274-4a17-83dd-d14be72edb86";

const VALID_CONFIG: &str = r#"
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20

[profiles.codex-strict]
current_version = "2026-08-01"

[profiles.codex-strict.revisions."2026-08-01"]
image = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[profiles.codex-strict.revisions."2026-08-01".supervisor]
executable = "/usr/local/bin/openab-session-supervisor"
args = ["serve"]

[profiles.codex-strict.revisions."2026-08-01".workspace]
size = "20Gi"
storage_class = "encrypted-rwo"
access_mode = "read_write_once_pod"

[profiles.codex-strict.revisions."2026-08-01".resources.requests]
cpu = "250m"
memory = "256Mi"
ephemeral_storage = "1Gi"

[profiles.codex-strict.revisions."2026-08-01".resources.limits]
cpu = "1"
memory = "2Gi"
ephemeral_storage = "8Gi"

[profiles.codex-strict.revisions."2026-08-01".run_as]
uid = 10001
gid = 10001

[[profiles.codex-strict.revisions."2026-08-01".egress]]
target = "cidr"
cidr = "10.96.0.10/32"

[[profiles.codex-strict.revisions."2026-08-01".egress.ports]]
protocol = "udp"
port = 53

[[profiles.codex-strict.revisions."2026-08-01".egress]]
target = "selectors"
namespace_labels = { "kubernetes.io/metadata.name" = "openab-system" }
pod_labels = { "app.kubernetes.io/name" = "openab-session-controller" }

[[profiles.codex-strict.revisions."2026-08-01".egress.ports]]
protocol = "tcp"
port = 8443
"#;

fn with_second_revision(current_version: &str) -> String {
    let revision_start = VALID_CONFIG
        .find("[profiles.codex-strict.revisions")
        .unwrap();
    let second = VALID_CONFIG[revision_start..]
        .replace("2026-08-01", "2026-08-02")
        .replace(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
    let first = VALID_CONFIG.replacen(
        "current_version = \"2026-08-01\"",
        &format!("current_version = \"{current_version}\""),
        1,
    );
    format!("{first}\n{second}")
}

fn anchor(profile: ProfileRef) -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        SessionId::derive(RAW_SCOPE, "discord:profile-loader"),
        ScopeId::derive(RAW_SCOPE),
        profile,
        Uuid::from_u128(0x10),
        Uuid::from_u128(0x20),
        now,
        now + ChronoDuration::minutes(15),
        now + ChronoDuration::hours(72),
    )
    .unwrap()
}

fn generation(
    profile: openab_kubernetes_session::resources::MvpWorkerProfile,
) -> DesiredGeneration {
    let anchor = anchor(profile.profile().clone());
    let names = ResourceNames::new(anchor.session_id());
    let context =
        GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, &anchor, names)
            .unwrap();
    DesiredGeneration::build(context, profile, [0x5a; 32]).unwrap()
}

#[test]
fn controller_config_builds_policy_and_existing_worker_domain_types() {
    let config = TrustedControllerConfigV1::from_toml(VALID_CONFIG).unwrap();
    assert_eq!(config.profiles().len(), 1);
    assert!(config.profile("missing-profile").is_none());
    assert_eq!(config.policy().compute_idle_ttl().as_secs(), 900);
    assert_eq!(config.policy().storage_retention_ttl().as_secs(), 259_200);
    assert_eq!(config.policy().max_active_workers(), 20);

    let loaded = config.profile("codex-strict").unwrap();
    assert_eq!(loaded.profile_ref().name(), "codex-strict");
    assert_eq!(loaded.profile_ref().version(), "2026-08-01");
    assert!(loaded.runtime_class_intent().is_none());
    assert!(loaded.skills_intent().is_none());

    let resolved = loaded
        .clone()
        .resolve_cluster_references(ResolvedClusterReferences::none())
        .unwrap();
    let desired = generation(resolved.into_worker_profile());
    let pod_spec = desired.pod().spec.as_ref().unwrap();
    let worker = &pod_spec.containers[0];
    assert_eq!(
        worker.image.as_deref(),
        Some("ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert_eq!(
        worker.command.as_deref(),
        Some(&["/usr/local/bin/openab-session-supervisor".to_string()][..])
    );
    assert_eq!(worker.args.as_deref(), Some(&["serve".to_string()][..]));
    assert_eq!(
        desired
            .persistent_volume_claim()
            .spec
            .as_ref()
            .unwrap()
            .access_modes
            .as_deref(),
        Some(&["ReadWriteOncePod".to_string()][..])
    );
    assert_eq!(
        worker
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()["ephemeral-storage"]
            .0,
        "1Gi"
    );
    assert_eq!(
        desired
            .network_policy()
            .spec
            .as_ref()
            .unwrap()
            .egress
            .as_ref()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn controller_config_keeps_historical_revisions_but_selects_one_current_revision() {
    let config = TrustedControllerConfigV1::from_toml(&with_second_revision("2026-08-02"))
        .expect("two valid revisions");

    let revisions = config.profiles().get("codex-strict").unwrap();
    assert_eq!(revisions.revisions().len(), 2);
    assert_eq!(
        config
            .profile("codex-strict")
            .unwrap()
            .profile_ref()
            .version(),
        "2026-08-02"
    );
    assert_eq!(
        config
            .profile_revision("codex-strict", "2026-08-01")
            .unwrap()
            .profile_ref()
            .version(),
        "2026-08-01"
    );
    assert_eq!(
        config
            .all_revisions()
            .map(|profile| profile.profile_ref().version())
            .collect::<Vec<_>>(),
        ["2026-08-01", "2026-08-02"]
    );
    assert_eq!(
        config
            .current_profile_refs()
            .map(ProfileRef::version)
            .collect::<Vec<_>>(),
        ["2026-08-02"]
    );
}

#[test]
fn controller_config_rejects_a_missing_current_or_empty_revision_set() {
    assert!(TrustedControllerConfigV1::from_toml(&with_second_revision("2026-08-03")).is_err());

    let empty = r#"
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20

[profiles.codex-strict]
current_version = "2026-08-01"

[profiles.codex-strict.revisions]
"#;
    assert!(TrustedControllerConfigV1::from_toml(empty).is_err());
}

#[test]
fn every_dto_level_rejects_unknown_fields_including_transport_and_secrets() {
    let cases = [
        format!("relay_url = \"wss://forbidden.example\"\n{VALID_CONFIG}"),
        VALID_CONFIG.replacen(
            "max_active_workers = 20",
            "max_active_workers = 20\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen(
            "current_version = \"2026-08-01\"",
            "current_version = \"2026-08-01\"\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen(
            "image = \"ghcr.io/example/openab-worker",
            "credential_file = \"/secret\"\nimage = \"ghcr.io/example/openab-worker",
            1,
        ),
        VALID_CONFIG.replacen(
            "args = [\"serve\"]",
            "args = [\"serve\"]\nsecret = \"forbidden\"",
            1,
        ),
        VALID_CONFIG.replacen(
            "access_mode = \"read_write_once_pod\"",
            "access_mode = \"read_write_once_pod\"\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen(
            "ephemeral_storage = \"1Gi\"",
            "ephemeral_storage = \"1Gi\"\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen(
            "ephemeral_storage = \"8Gi\"",
            "ephemeral_storage = \"8Gi\"\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen("gid = 10001", "gid = 10001\nunknown = true", 1),
        VALID_CONFIG.replacen(
            "cidr = \"10.96.0.10/32\"",
            "cidr = \"10.96.0.10/32\"\nunknown = true",
            1,
        ),
        VALID_CONFIG.replacen("port = 53", "port = 53\nunknown = true", 1),
    ];

    for config in cases {
        assert!(TrustedControllerConfigV1::from_toml(&config).is_err());
    }
}

#[test]
fn decode_errors_do_not_echo_secret_like_input() {
    const SENTINEL: &str = "do-not-leak-this-token";
    let invalid = format!("secret = \"{SENTINEL}\"\n{VALID_CONFIG}");
    let error = TrustedControllerConfigV1::from_toml(&invalid).unwrap_err();
    assert!(!error.to_string().contains(SENTINEL));
    assert!(!format!("{error:?}").contains(SENTINEL));
}

#[test]
fn invalid_profile_keys_do_not_inject_into_errors() {
    const SENTINEL: &str = "forged-controller-log-line";
    let quoted_key = format!("profiles.\"bad\\n{SENTINEL}\"");
    let invalid = VALID_CONFIG.replace("profiles.codex-strict", &quoted_key);
    let error = TrustedControllerConfigV1::from_toml(&invalid).unwrap_err();
    assert!(!error.to_string().contains(SENTINEL));
    assert!(!format!("{error:?}").contains(SENTINEL));
}

#[test]
fn duplicate_profile_name_or_revision_fails_closed() {
    let duplicate_name =
        format!("{VALID_CONFIG}\n[profiles.codex-strict]\ncurrent_version = \"another-version\"\n");
    assert!(TrustedControllerConfigV1::from_toml(&duplicate_name).is_err());

    let revision_start = VALID_CONFIG
        .find("[profiles.codex-strict.revisions")
        .unwrap();
    let duplicate_revision = format!("{VALID_CONFIG}\n{}", &VALID_CONFIG[revision_start..]);
    assert!(TrustedControllerConfigV1::from_toml(&duplicate_revision).is_err());
}

#[test]
fn controller_requires_at_least_one_profile() {
    let missing = r#"
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20
"#;
    assert!(TrustedControllerConfigV1::from_toml(missing).is_err());

    let empty = format!("{missing}\n[profiles]\n");
    assert!(TrustedControllerConfigV1::from_toml(&empty).is_err());
}

#[test]
fn schema_version_is_required_and_only_version_one_is_supported() {
    assert!(TrustedControllerConfigV1::from_toml(&VALID_CONFIG.replacen(
        "schema_version = 1\n\n",
        "",
        1
    ))
    .is_err());
    assert!(TrustedControllerConfigV1::from_toml(&VALID_CONFIG.replacen(
        "schema_version = 1",
        "schema_version = 2",
        1,
    ))
    .is_err());
}

#[test]
fn controller_policy_values_are_nonzero_bounded_and_ordered() {
    for invalid in [
        VALID_CONFIG.replacen("compute_idle_seconds = 900", "compute_idle_seconds = 0", 1),
        VALID_CONFIG.replacen(
            "storage_retention_seconds = 259200",
            "storage_retention_seconds = 899",
            1,
        ),
        VALID_CONFIG.replacen("max_active_workers = 20", "max_active_workers = 0", 1),
        VALID_CONFIG.replacen(
            "compute_idle_seconds = 900",
            &format!("compute_idle_seconds = {}", MAX_LIFECYCLE_TTL_SECONDS + 1),
            1,
        ),
        VALID_CONFIG.replacen(
            "max_active_workers = 20",
            &format!("max_active_workers = {}", MAX_ACTIVE_WORKERS + 1),
            1,
        ),
    ] {
        assert!(TrustedControllerConfigV1::from_toml(&invalid).is_err());
    }
}

#[test]
fn programmatic_controller_policy_uses_the_same_validation_boundary() {
    let policy = ControllerPolicy::new(900, 259_200, 20).unwrap();
    assert_eq!(policy.compute_idle_ttl().as_secs(), 900);
    assert_eq!(policy.storage_retention_ttl().as_secs(), 259_200);
    assert_eq!(policy.max_active_workers(), 20);

    for invalid in [
        ControllerPolicy::new(0, 259_200, 20),
        ControllerPolicy::new(900, 899, 20),
        ControllerPolicy::new(900, 259_200, 0),
        ControllerPolicy::new(MAX_LIFECYCLE_TTL_SECONDS + 1, 259_200, 20),
        ControllerPolicy::new(900, 259_200, MAX_ACTIVE_WORKERS + 1),
    ] {
        assert!(invalid.is_err());
    }
}

#[test]
fn existing_profile_validators_reject_invalid_config_values() {
    for invalid in [
        VALID_CONFIG.replace(
            "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "ghcr.io/example/openab-worker:latest",
        ),
        VALID_CONFIG.replace(
            "/usr/local/bin/openab-session-supervisor",
            "relative-supervisor",
        ),
        VALID_CONFIG.replace("2026-08-01", ""),
        VALID_CONFIG.replace("2026-08-01", "unsafe version"),
        VALID_CONFIG.replace("2026-08-01", &"v".repeat(257)),
        VALID_CONFIG.replacen("size = \"20Gi\"", "size = \"20GB\"", 1),
        VALID_CONFIG.replacen("cpu = \"1\"", "cpu = \"100m\"", 1),
        VALID_CONFIG.replacen("uid = 10001", "uid = 0", 1),
        VALID_CONFIG.replacen("10.96.0.10/32", "10.96.0.10/8", 1),
        VALID_CONFIG.replacen("access_mode = \"read_write_once_pod\"", "access_mode = \"rwx\"", 1),
        VALID_CONFIG.replacen("protocol = \"udp\"", "protocol = \"sctp\"", 1),
    ] {
        assert!(TrustedControllerConfigV1::from_toml(&invalid).is_err());
    }
}

fn observed_runtime_class(name: &str, handler: &str) -> RuntimeClass {
    RuntimeClass {
        handler: handler.into(),
        metadata: ObjectMeta {
            name: Some(name.into()),
            uid: Some(format!("{name}-uid")),
            resource_version: Some("runtime-rv-1".into()),
            ..ObjectMeta::default()
        },
        overhead: None,
        scheduling: None,
    }
}

fn observed_skills_config_map(name: &str, namespace: &str) -> ConfigMap {
    ConfigMap {
        immutable: Some(true),
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(namespace.into()),
            uid: Some(format!("{name}-uid")),
            resource_version: Some("skills-rv-1".into()),
            ..ObjectMeta::default()
        },
        ..ConfigMap::default()
    }
}

#[test]
fn skills_pin_requires_an_exact_immutable_live_observation() {
    let observed = observed_skills_config_map("team-skills-v1", NAMESPACE);
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &observed).is_ok());

    let mut mutable = observed.clone();
    mutable.immutable = Some(false);
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &mutable).is_err());

    let mut deleting = observed.clone();
    deleting.metadata.deletion_timestamp = Some(Time("2026-08-01T08:00:00Z".parse().unwrap()));
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &deleting).is_err());

    let mut missing_uid = observed.clone();
    missing_uid.metadata.uid = None;
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &missing_uid).is_err());

    let mut missing_resource_version = observed.clone();
    missing_resource_version.metadata.resource_version = None;
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &missing_resource_version).is_err());

    let mut invalid_uid = observed.clone();
    invalid_uid.metadata.uid = Some("invalid/uid".into());
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &invalid_uid).is_err());

    let mut invalid_resource_version = observed.clone();
    invalid_resource_version.metadata.resource_version = Some("invalid\nversion".into());
    assert!(PinnedSkillsConfigMap::from_observed(NAMESPACE, &invalid_resource_version).is_err());

    assert!(PinnedSkillsConfigMap::from_observed("other-namespace", &observed).is_err());
}

#[test]
fn cluster_references_remain_intents_until_exact_observations_are_resolved() {
    let config = format!(
        r#"{VALID_CONFIG}
[profiles.codex-strict.revisions."2026-08-01".runtime_class]
name = "kata"
expected_handler = "kata-qemu"

[profiles.codex-strict.revisions."2026-08-01".skills]
config_map_name = "team-skills-v1"
"#
    );
    let config = TrustedControllerConfigV1::from_toml(&config).unwrap();
    let loaded = config.profile("codex-strict").unwrap();
    let runtime_intent = loaded.runtime_class_intent().unwrap();
    assert_eq!(runtime_intent.name(), "kata");
    assert_eq!(runtime_intent.expected_handler(), "kata-qemu");
    let skills_intent = loaded.skills_intent().unwrap();
    assert_eq!(skills_intent.name(), "team-skills-v1");
    assert!(loaded
        .clone()
        .resolve_cluster_references(ResolvedClusterReferences::none())
        .is_err());

    let observed = observed_runtime_class("kata", "kata-qemu");
    let runtime = runtime_intent.resolve_observed(&observed).unwrap();
    let observed_skills = observed_skills_config_map("team-skills-v1", NAMESPACE);
    let skills = skills_intent
        .resolve_observed(NAMESPACE, &observed_skills)
        .unwrap();
    let resolved = loaded
        .clone()
        .resolve_cluster_references(ResolvedClusterReferences::new(
            Some(runtime.clone()),
            Some(skills),
        ))
        .unwrap();
    let desired = generation(resolved.into_worker_profile());
    assert_eq!(
        desired
            .pod()
            .spec
            .as_ref()
            .unwrap()
            .runtime_class_name
            .as_deref(),
        Some("kata")
    );
    assert!(desired
        .pod()
        .spec
        .as_ref()
        .unwrap()
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .any(|volume| volume.name == "skills"));

    assert!(PinnedSkillsConfigMap::from_observed("other-namespace", &observed_skills).is_err());

    let wrong_skills = observed_skills_config_map("other-skills-v1", NAMESPACE);
    assert!(loaded
        .clone()
        .resolve_cluster_references(ResolvedClusterReferences::new(
            Some(runtime.clone()),
            Some(PinnedSkillsConfigMap::from_observed(NAMESPACE, &wrong_skills).unwrap()),
        ))
        .is_err());

    let wrong_runtime = RuntimeClassSelection::from_observed(
        &observed_runtime_class("gvisor", "runsc"),
        [AllowedRuntimeClass::new("gvisor", "runsc").unwrap()],
    )
    .unwrap();
    assert!(loaded
        .clone()
        .resolve_cluster_references(ResolvedClusterReferences::new(
            Some(wrong_runtime),
            Some(PinnedSkillsConfigMap::from_observed(NAMESPACE, &observed_skills).unwrap()),
        ))
        .is_err());
}

#[test]
fn config_cannot_forge_cluster_observation_metadata() {
    let forged = format!(
        r#"{VALID_CONFIG}
[profiles.codex-strict.revisions."2026-08-01".skills]
config_map_name = "team-skills-v1"
uid = "forged-uid"
resource_version = "forged-rv"
"#
    );
    assert!(TrustedControllerConfigV1::from_toml(&forged).is_err());

    let ambiguous_name = format!(
        r#"{VALID_CONFIG}
[profiles.codex-strict.revisions."2026-08-01".skills]
name = "team-skills-v1"
"#
    );
    assert!(TrustedControllerConfigV1::from_toml(&ambiguous_name).is_err());
}

#[test]
fn reads_profile_toml_through_the_bounded_utf8_api() {
    let config = TrustedControllerConfigV1::from_reader(Cursor::new(VALID_CONFIG.as_bytes()))
        .expect("valid bounded profile config");

    assert_eq!(config.profiles().len(), 1);
}

#[test]
fn bounded_profile_reader_accepts_the_exact_ceiling() {
    let prefix = format!("{VALID_CONFIG}\n#");
    let source = format!(
        "{prefix}{}",
        "x".repeat(MAX_PROFILE_CONFIG_BYTES - prefix.len())
    );
    assert_eq!(source.len(), MAX_PROFILE_CONFIG_BYTES);

    assert!(TrustedControllerConfigV1::from_reader(Cursor::new(source)).is_ok());
}

#[test]
fn profile_parser_and_reader_reject_oversize_input_before_decode() {
    let oversized = "#".repeat(MAX_PROFILE_CONFIG_BYTES + 1);
    assert!(matches!(
        TrustedControllerConfigV1::from_toml(&oversized),
        Err(openab_kubernetes_session::profile_config::ProfileConfigError::TooLarge)
    ));
    assert!(matches!(
        TrustedControllerConfigV1::from_reader(Cursor::new(oversized)),
        Err(openab_kubernetes_session::profile_config::ProfileConfigError::TooLarge)
    ));
}

#[test]
fn profile_reader_rejects_non_utf8_and_sanitizes_io_errors() {
    use openab_kubernetes_session::profile_config::ProfileConfigError;

    assert!(matches!(
        TrustedControllerConfigV1::from_reader(Cursor::new([0xff])),
        Err(ProfileConfigError::InvalidUtf8)
    ));

    let error =
        TrustedControllerConfigV1::from_reader(SensitiveReadFailure).expect_err("reader must fail");
    let rendered = error.to_string();
    assert!(matches!(error, ProfileConfigError::Read));
    assert!(!rendered.contains("secret-profile"));
    assert!(!rendered.contains("/etc/openab-session"));
}

struct SensitiveReadFailure;

impl Read for SensitiveReadFailure {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other(
            "secret-profile at /etc/openab-session/profiles.toml",
        ))
    }
}
