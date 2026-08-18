use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use openab_kubernetes_session::profile_config::{
    ResolvedClusterReferences, TrustedControllerConfigV1,
};
use openab_kubernetes_session::resources::MvpWorkerProfile;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub const RELAY_CA_NAME: &str = "openab-session-controller-ca-2026-08";
pub const RELAY_CA_UID: &str = "relay-ca-uid-2026-08";
pub const RELAY_CA_RESOURCE_VERSION: &str = "relay-ca-rv-42";

const TRANSPORT_PROFILE_CONFIG: &str = r#"
schema_version = 1

[policy]
compute_idle_seconds = 900
storage_retention_seconds = 259200
max_active_workers = 20

[profiles.codex-strict]
current_version = "2026-08-01"

[profiles.codex-strict.revisions."2026-08-01"]
image = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[profiles.codex-strict.revisions."2026-08-01".relay]
url = "wss://openab-session-controller.openab-system.svc:8443/v1/worker"
ca_config_map_name = "openab-session-controller-ca-2026-08"

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
"#;

pub fn observed_relay_ca(namespace: &str) -> ConfigMap {
    ConfigMap {
        data: Some(BTreeMap::from([(
            "ca.crt".to_owned(),
            relay_ca_pem().to_owned(),
        )])),
        immutable: Some(true),
        metadata: ObjectMeta {
            name: Some(RELAY_CA_NAME.into()),
            namespace: Some(namespace.into()),
            uid: Some(RELAY_CA_UID.into()),
            resource_version: Some(RELAY_CA_RESOURCE_VERSION.into()),
            ..ObjectMeta::default()
        },
        ..ConfigMap::default()
    }
}

pub fn transport_profile(namespace: &str) -> MvpWorkerProfile {
    let config = TrustedControllerConfigV1::from_toml(TRANSPORT_PROFILE_CONFIG).unwrap();
    let loaded = config.profile("codex-strict").unwrap().clone();
    let relay_ca = loaded
        .relay()
        .ca_config_map()
        .resolve_observed(namespace, &observed_relay_ca(namespace))
        .unwrap();
    loaded
        .resolve_cluster_references(ResolvedClusterReferences::new(None, None, relay_ca))
        .unwrap()
        .into_worker_profile()
}

fn relay_ca_pem() -> &'static str {
    static PEM: OnceLock<String> = OnceLock::new();
    PEM.get_or_init(|| {
        let CertifiedKey { cert, .. } =
            generate_simple_self_signed(vec!["controller.example.test".to_owned()])
                .expect("test relay CA certificate");
        cert.pem()
    })
}
