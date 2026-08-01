#![cfg(feature = "controller")]

use chrono::{Duration, TimeZone, Utc};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerResizePolicy, HostPathVolumeSource,
};
use k8s_openapi::api::node::v1::{Overhead, RuntimeClass, Scheduling};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::ByteString;
use openab_kubernetes_session::bridge::SessionBinding;
use openab_kubernetes_session::identity::{ResourceNames, ScopeId, SessionId};
use openab_kubernetes_session::resources::{
    AllowedRuntimeClass, DesiredGeneration, EgressPort, EgressProtocol, GenerationContext,
    MvpWorkerProfile, PersistentWorkspace, PinnedSkillsConfigMap, PvcAccessMode,
    ResourceValidationError, RunAsIdentity, RuntimeClassSelection, TrustedEgressRule,
    WorkerResources,
};
use openab_kubernetes_session::state::{ProfileRef, SessionAnchorV1};
use openab_kubernetes_session::wire::WorkerRegistrationV1;
use std::collections::BTreeMap;
use uuid::Uuid;

const NAMESPACE: &str = "team-a-workers";
const ANCHOR_UID: &str = "f6d6f3dd-1274-4a17-83dd-d14be72edb86";
const PROFILE_NAME: &str = "codex-strict";
const PROFILE_VERSION: &str = "2026-08-01";
const IMAGE: &str = "ghcr.io/example/openab-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn anchor() -> SessionAnchorV1 {
    let now = Utc.with_ymd_and_hms(2026, 8, 1, 8, 0, 0).unwrap();
    SessionAnchorV1::new(
        SessionId::derive("private-team-scope", "discord:private-thread-123"),
        ScopeId::derive("private-team-scope"),
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
        Uuid::from_u128(0x10),
        Uuid::from_u128(0x20),
        now,
        now + Duration::minutes(15),
        now + Duration::hours(72),
    )
    .unwrap()
}

fn context() -> GenerationContext {
    let anchor = anchor();
    let names = ResourceNames::new(anchor.session_id());
    GenerationContext::from_anchor(NAMESPACE, names.anchor(), ANCHOR_UID, &anchor, names).unwrap()
}

fn profile(
    access_mode: PvcAccessMode,
    runtime_class: Option<RuntimeClassSelection>,
    skills: Option<PinnedSkillsConfigMap>,
) -> MvpWorkerProfile {
    MvpWorkerProfile::new(
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
        IMAGE,
        ["/usr/local/bin/openab-session-supervisor"],
        ["serve"],
        PersistentWorkspace::new("20Gi", "encrypted-rwo", access_mode).unwrap(),
        WorkerResources::new("250m", "1", "256Mi", "2Gi", "1Gi", "8Gi").unwrap(),
        trusted_egress(),
        RunAsIdentity::new(10001, 10001).unwrap(),
        runtime_class,
        skills,
    )
    .unwrap()
}

fn trusted_egress() -> Vec<TrustedEgressRule> {
    vec![
        TrustedEgressRule::for_selectors(
            BTreeMap::from([(
                "kubernetes.io/metadata.name".to_string(),
                "openab-system".to_string(),
            )]),
            BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "session-relay".to_string(),
            )]),
            [EgressPort::new(EgressProtocol::Tcp, 443).unwrap()],
        )
        .unwrap(),
        TrustedEgressRule::for_cidr(
            "10.96.0.10/32",
            [
                EgressPort::new(EgressProtocol::Udp, 53).unwrap(),
                EgressPort::new(EgressProtocol::Tcp, 53).unwrap(),
            ],
        )
        .unwrap(),
    ]
}

fn runtime_class(name: &str, handler: &str) -> RuntimeClass {
    RuntimeClass {
        handler: handler.into(),
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.into()),
            uid: Some(format!("{name}-uid")),
            resource_version: Some("rv-11".into()),
            ..Default::default()
        },
        overhead: None,
        scheduling: None,
    }
}

fn selected_runtime(name: &str, handler: &str) -> RuntimeClassSelection {
    let observed = runtime_class(name, handler);
    RuntimeClassSelection::from_observed(
        &observed,
        [AllowedRuntimeClass::new(name, handler).unwrap()],
    )
    .unwrap()
}

fn desired() -> DesiredGeneration {
    DesiredGeneration::build(
        context(),
        profile(PvcAccessMode::default(), None, None),
        [0x5a; 32],
    )
    .unwrap()
}

fn annotations(
    object: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
) -> &BTreeMap<String, String> {
    object.annotations.as_ref().unwrap()
}

fn mark_observed(
    metadata: &mut k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    uid: &str,
) {
    metadata.uid = Some(uid.into());
    metadata.resource_version = Some("rv-observed".into());
}

#[test]
fn context_rejects_untrusted_or_mismatched_anchor_metadata() {
    let anchor = anchor();
    let names = ResourceNames::new(anchor.session_id());

    assert!(GenerationContext::from_anchor(
        "INVALID_NAMESPACE",
        names.anchor(),
        ANCHOR_UID,
        &anchor,
        names,
    )
    .is_err());
    assert!(GenerationContext::from_anchor(
        NAMESPACE,
        "oab-session-foreign",
        ANCHOR_UID,
        &anchor,
        names,
    )
    .is_err());
    assert!(
        GenerationContext::from_anchor(NAMESPACE, names.anchor(), "", &anchor, names,).is_err()
    );
    assert!(GenerationContext::from_anchor(
        NAMESPACE,
        names.anchor(),
        ANCHOR_UID,
        &anchor,
        ResourceNames::new(SessionId::derive("another", "session")),
    )
    .is_err());
}

#[test]
fn default_generation_is_hardened_and_fully_bound() {
    let desired = desired();
    let context = context();
    let expected_owner_uid = context.anchor_uid();

    for metadata in [
        &desired.persistent_volume_claim().metadata,
        &desired.registration_secret().metadata,
        &desired.service_account().metadata,
        &desired.pod().metadata,
        &desired.network_policy().metadata,
    ] {
        assert_eq!(metadata.namespace.as_deref(), Some(NAMESPACE));
        let owners = metadata.owner_references.as_ref().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].uid, expected_owner_uid);
        assert_eq!(owners[0].name, context.anchor_name());
        assert_eq!(owners[0].kind, "ConfigMap");
        assert_eq!(owners[0].controller, Some(true));
        assert_eq!(owners[0].block_owner_deletion, Some(true));

        let annotations = annotations(metadata);
        assert_eq!(
            annotations.get("openab.dev/scope-id").map(String::as_str),
            Some(context.scope_id().as_hex().as_str())
        );
        assert_eq!(
            annotations.get("openab.dev/session-id").map(String::as_str),
            Some(context.session_id().as_hex().as_str())
        );
        assert_eq!(
            annotations.get("openab.dev/generation").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            annotations.get("openab.dev/attempt-id").map(String::as_str),
            Some(context.fence().attempt_id().to_string().as_str())
        );
        assert_eq!(
            annotations
                .get("openab.dev/incarnation-id")
                .map(String::as_str),
            Some(context.incarnation_id().to_string().as_str())
        );
        assert_eq!(
            annotations
                .get("openab.dev/profile-name")
                .map(String::as_str),
            Some(PROFILE_NAME)
        );
        assert_eq!(
            annotations
                .get("openab.dev/profile-version")
                .map(String::as_str),
            Some(PROFILE_VERSION)
        );
        assert_eq!(
            annotations.get("openab.dev/anchor-uid").map(String::as_str),
            Some(ANCHOR_UID)
        );
        assert_eq!(
            annotations
                .get("openab.dev/worker-image-contract")
                .map(String::as_str),
            Some("session-layout-v1")
        );
    }

    let claim = desired.persistent_volume_claim();
    let claim_spec = claim.spec.as_ref().unwrap();
    assert_eq!(
        claim_spec.access_modes.as_deref(),
        Some(&["ReadWriteOncePod".to_string()][..])
    );
    assert_eq!(
        claim_spec.storage_class_name.as_deref(),
        Some("encrypted-rwo")
    );
    assert_eq!(claim_spec.volume_mode.as_deref(), Some("Filesystem"));
    assert_eq!(
        claim_spec
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .get("storage")
            .unwrap()
            .0,
        "20Gi"
    );

    let secret = desired.registration_secret();
    assert_eq!(secret.immutable, Some(true));
    assert_eq!(secret.type_.as_deref(), Some("Opaque"));
    assert_eq!(secret.data.as_ref().unwrap().len(), 2);
    assert_eq!(
        secret.data.as_ref().unwrap().get("token"),
        Some(&ByteString(vec![0x5a; 32]))
    );
    assert!(secret.string_data.is_none());
    let binding_json = &secret.data.as_ref().unwrap().get("binding.json").unwrap().0;
    assert!(binding_json.len() <= 4 * 1024);
    let registration: WorkerRegistrationV1 = serde_json::from_slice(binding_json).unwrap();
    let expected_binding = SessionBinding::new(
        context.scope_id(),
        context.session_id(),
        context.fence().clone(),
        context.incarnation_id(),
    )
    .unwrap();
    assert_eq!(
        registration
            .into_validated_binding(&expected_binding)
            .unwrap(),
        expected_binding
    );

    assert_eq!(
        desired.service_account().automount_service_account_token,
        Some(false)
    );

    let pod_spec = desired.pod().spec.as_ref().unwrap();
    assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
    assert_eq!(pod_spec.enable_service_links, Some(false));
    assert_eq!(pod_spec.automount_service_account_token, Some(false));
    assert!(pod_spec.host_network.is_none());
    assert!(pod_spec.host_pid.is_none());
    assert!(pod_spec.host_ipc.is_none());
    assert_eq!(pod_spec.os.as_ref().unwrap().name, "linux");
    assert_eq!(
        pod_spec
            .node_selector
            .as_ref()
            .unwrap()
            .get("kubernetes.io/os")
            .map(String::as_str),
        Some("linux")
    );
    assert_eq!(pod_spec.share_process_namespace, Some(false));
    assert_eq!(pod_spec.tolerations.as_ref().unwrap().len(), 2);
    for (toleration, key) in pod_spec.tolerations.as_ref().unwrap().iter().zip([
        "node.kubernetes.io/not-ready",
        "node.kubernetes.io/unreachable",
    ]) {
        assert_eq!(toleration.key.as_deref(), Some(key));
        assert_eq!(toleration.operator.as_deref(), Some("Exists"));
        assert_eq!(toleration.effect.as_deref(), Some("NoExecute"));
        assert_eq!(toleration.toleration_seconds, Some(300));
        assert!(toleration.value.is_none());
    }
    assert_eq!(
        pod_spec.service_account_name.as_deref(),
        Some(
            context
                .resource_names()
                .service_account(1)
                .unwrap()
                .as_str()
        )
    );
    assert_eq!(pod_spec.service_account, pod_spec.service_account_name);

    let session_volume = pod_spec
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .find(|volume| volume.name == "session")
        .unwrap();
    assert!(session_volume
        .persistent_volume_claim
        .as_ref()
        .unwrap()
        .read_only
        .is_none());

    let pod_security = pod_spec.security_context.as_ref().unwrap();
    assert_eq!(pod_security.run_as_non_root, Some(true));
    assert_eq!(pod_security.run_as_user, Some(10001));
    assert_eq!(pod_security.run_as_group, Some(10001));
    assert_eq!(pod_security.fs_group, Some(10001));
    assert_eq!(
        pod_security.seccomp_profile.as_ref().unwrap().type_,
        "RuntimeDefault"
    );

    let container = &pod_spec.containers[0];
    assert_eq!(container.working_dir.as_deref(), Some("/session"));
    assert_eq!(
        container.command.as_deref(),
        Some(&["/usr/local/bin/openab-session-supervisor".to_string()][..])
    );
    assert_eq!(container.args.as_deref(), Some(&["serve".to_string()][..]));
    assert!(container
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .filter(|mount| mount.name != "registration" && mount.name != "skills")
        .all(|mount| mount.read_only.is_none()));
    let security = container.security_context.as_ref().unwrap();
    assert_eq!(security.allow_privilege_escalation, Some(false));
    assert_eq!(security.privileged, Some(false));
    assert_eq!(security.read_only_root_filesystem, Some(true));
    assert_eq!(security.run_as_non_root, Some(true));
    assert_eq!(
        security.capabilities.as_ref().unwrap().drop.as_deref(),
        Some(&["ALL".to_string()][..])
    );

    let resources = container.resources.as_ref().unwrap();
    for name in ["cpu", "memory", "ephemeral-storage"] {
        assert!(resources.requests.as_ref().unwrap().contains_key(name));
        assert!(resources.limits.as_ref().unwrap().contains_key(name));
    }
}

#[test]
fn token_is_only_a_read_only_secret_file_and_no_host_path_is_present() {
    let desired = desired();
    let pod_spec = desired.pod().spec.as_ref().unwrap();
    assert!(pod_spec
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .all(|volume| volume.host_path.is_none()));

    let container = &pod_spec.containers[0];
    let env = container.env.as_ref().unwrap();
    assert!(env
        .iter()
        .all(|entry| entry.value.as_deref() != Some(&"Z".repeat(32))));
    assert!(env.iter().all(|entry| entry.value_from.is_none()));
    assert_eq!(
        env.iter()
            .find(|entry| entry.name == "OPENAB_REGISTRATION_TOKEN_FILE")
            .unwrap()
            .value
            .as_deref(),
        Some("/var/run/openab-registration/token")
    );
    assert_eq!(
        env.iter()
            .find(|entry| entry.name == "OPENAB_REGISTRATION_BINDING_FILE")
            .unwrap()
            .value
            .as_deref(),
        Some("/var/run/openab-registration/binding.json")
    );
    assert_eq!(
        env.iter()
            .find(|entry| entry.name == "OPENAB_SESSION_ROOT")
            .unwrap()
            .value
            .as_deref(),
        Some("/session")
    );

    let token_mount = container
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .find(|mount| mount.name == "registration")
        .unwrap();
    assert_eq!(token_mount.mount_path, "/var/run/openab-registration");
    assert_eq!(token_mount.read_only, Some(true));
    let registration_volume = pod_spec
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .find(|volume| volume.name == "registration")
        .unwrap();
    let secret_source = registration_volume.secret.as_ref().unwrap();
    assert_eq!(secret_source.default_mode, Some(0o400));
    let projected_items = secret_source.items.as_ref().unwrap();
    assert!(projected_items.iter().all(|item| item.mode == Some(0o400)));
    assert_eq!(
        pod_spec.security_context.as_ref().unwrap().fs_group,
        Some(10001)
    );
    assert_eq!(
        container.security_context.as_ref().unwrap().run_as_group,
        Some(10001)
    );
    let projected_keys: Vec<&str> = projected_items
        .iter()
        .map(|item| item.key.as_str())
        .collect();
    assert_eq!(projected_keys, ["token", "binding.json"]);

    for path in ["/tmp", "/var/tmp", "/run/openab"] {
        assert!(container
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .any(|mount| mount.mount_path == path));
    }
}

#[test]
fn runtime_and_pinned_immutable_skills_are_opt_in() {
    let runtime = selected_runtime("kata-qemu", "kata-qemu");
    let skills = PinnedSkillsConfigMap::new("skills-2026-08-01", "skills-uid", "rv-42").unwrap();
    let desired = DesiredGeneration::build(
        context(),
        profile(PvcAccessMode::ReadWriteOnce, Some(runtime), Some(skills)),
        [7; 32],
    )
    .unwrap();

    let pod_spec = desired.pod().spec.as_ref().unwrap();
    assert_eq!(pod_spec.runtime_class_name.as_deref(), Some("kata-qemu"));
    desired
        .validate_runtime_class(&runtime_class("kata-qemu", "kata-qemu"))
        .unwrap();
    assert_eq!(
        desired
            .persistent_volume_claim()
            .spec
            .as_ref()
            .unwrap()
            .access_modes
            .as_deref(),
        Some(&["ReadWriteOnce".to_string()][..])
    );
    let skills_volume = pod_spec
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .find(|volume| volume.name == "skills")
        .unwrap();
    assert_eq!(
        skills_volume.config_map.as_ref().unwrap().name,
        "skills-2026-08-01"
    );
    let skills_mount = pod_spec.containers[0]
        .volume_mounts
        .as_ref()
        .unwrap()
        .iter()
        .find(|mount| mount.name == "skills")
        .unwrap();
    assert_eq!(skills_mount.mount_path, "/opt/openab/skills");
    assert_eq!(skills_mount.read_only, Some(true));

    let observed = ConfigMap {
        immutable: Some(true),
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("skills-2026-08-01".into()),
            namespace: Some(NAMESPACE.into()),
            uid: Some("skills-uid".into()),
            resource_version: Some("rv-42".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    desired.validate_skills_config_map(&observed).unwrap();

    let mut mutable = observed;
    mutable.immutable = Some(false);
    assert!(desired.validate_skills_config_map(&mutable).is_err());
}

#[test]
fn observed_validation_rejects_foreign_owner_and_stale_fence() {
    let desired = desired();

    let mut foreign = desired.pod().clone();
    mark_observed(&mut foreign.metadata, "pod-uid");
    foreign.metadata.owner_references.as_mut().unwrap()[0].uid = "foreign-uid".into();
    assert!(matches!(
        desired.validate_pod(&foreign),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut stale = desired.pod().clone();
    mark_observed(&mut stale.metadata, "pod-uid");
    stale.metadata.annotations.as_mut().unwrap().insert(
        "openab.dev/attempt-id".into(),
        Uuid::from_u128(0x99).to_string(),
    );
    assert!(matches!(
        desired.validate_pod(&stale),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut wrong_generation = desired.pod().clone();
    mark_observed(&mut wrong_generation.metadata, "pod-uid");
    wrong_generation
        .metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("openab.dev/generation".into(), "2".into());
    assert!(matches!(
        desired.validate_pod(&wrong_generation),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut wrong_name = desired.pod().clone();
    mark_observed(&mut wrong_name.metadata, "pod-uid");
    wrong_name.metadata.name = Some("oab-worker-foreign-g2".into());
    assert!(matches!(
        desired.validate_pod(&wrong_name),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut foreign_binding = desired.pod().clone();
    mark_observed(&mut foreign_binding.metadata, "pod-uid");
    foreign_binding
        .metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("openab.dev/foreign-binding".into(), "true".into());
    assert!(matches!(
        desired.validate_pod(&foreign_binding),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut injected_label = desired.pod().clone();
    mark_observed(&mut injected_label.metadata, "pod-uid");
    injected_label
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .insert("mesh.example/injected".into(), "true".into());
    assert!(matches!(
        desired.validate_pod(&injected_label),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));
}

#[test]
fn observed_validation_rejects_sidecars_host_mounts_and_weakened_security() {
    let desired = desired();

    let mut sidecar = desired.pod().clone();
    mark_observed(&mut sidecar.metadata, "pod-uid");
    sidecar.spec.as_mut().unwrap().containers.push(Container {
        name: "injected-sidecar".into(),
        image: Some("example.invalid/sidecar@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
        ..Default::default()
    });
    assert!(matches!(
        desired.validate_pod(&sidecar),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut host_mount = desired.pod().clone();
    mark_observed(&mut host_mount.metadata, "pod-uid");
    host_mount.spec.as_mut().unwrap().volumes.as_mut().unwrap()[0].host_path =
        Some(HostPathVolumeSource {
            path: "/".into(),
            type_: None,
        });
    assert!(matches!(
        desired.validate_pod(&host_mount),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut weakened = desired.pod().clone();
    mark_observed(&mut weakened.metadata, "pod-uid");
    weakened.spec.as_mut().unwrap().containers[0]
        .security_context
        .as_mut()
        .unwrap()
        .allow_privilege_escalation = Some(true);
    assert!(matches!(
        desired.validate_pod(&weakened),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
}

#[test]
fn observed_validation_accepts_server_metadata_and_status_only() {
    let desired = desired();
    let mut observed = desired.pod().clone();
    observed.metadata.uid = Some("pod-uid".into());
    observed.metadata.resource_version = Some("rv-5".into());
    observed.metadata.creation_timestamp = Some(Time("2026-08-01T08:00:00Z".parse().unwrap()));
    observed.status = Some(Default::default());
    desired.validate_pod(&observed).unwrap();
}

#[test]
fn observed_validation_accepts_only_known_api_server_canonicalization() {
    let desired = desired();
    let mut observed = desired.pod().clone();
    mark_observed(&mut observed.metadata, "pod-uid");
    let spec = observed.spec.as_mut().unwrap();

    // Go non-pointer booleans with `omitempty` are normally absent on
    // read-back, but accepting an explicit false is semantically identical.
    spec.host_network = Some(false);
    spec.host_pid = Some(false);
    spec.host_ipc = Some(false);
    spec.volumes
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|volume| volume.name == "session")
        .unwrap()
        .persistent_volume_claim
        .as_mut()
        .unwrap()
        .read_only = Some(false);
    for mount in spec.containers[0]
        .volume_mounts
        .as_mut()
        .unwrap()
        .iter_mut()
        .filter(|mount| mount.read_only.is_none())
    {
        mount.read_only = Some(false);
    }
    spec.containers[0].resize_policy = Some(vec![
        ContainerResizePolicy {
            resource_name: "memory".into(),
            restart_policy: "NotRequired".into(),
        },
        ContainerResizePolicy {
            resource_name: "cpu".into(),
            restart_policy: "NotRequired".into(),
        },
    ]);
    desired.validate_pod(&observed).unwrap();

    let mut widened = observed;
    widened.spec.as_mut().unwrap().host_network = Some(true);
    assert!(matches!(
        desired.validate_pod(&widened),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
}

#[test]
fn observed_validation_requires_api_identity_metadata() {
    let desired = desired();
    assert!(matches!(
        desired.validate_pod(desired.pod()),
        Err(ResourceValidationError::MetadataMismatch {
            field: "metadata.uid",
            ..
        })
    ));
    let mut observed = desired.pod().clone();
    observed.metadata.uid = Some("pod-uid".into());
    assert!(matches!(
        desired.validate_pod(&observed),
        Err(ResourceValidationError::MetadataMismatch {
            field: "metadata.resourceVersion",
            ..
        })
    ));
}

#[test]
fn pvc_validation_allows_only_known_binding_metadata() {
    let desired = desired();
    let mut bound = desired.persistent_volume_claim().clone();
    mark_observed(&mut bound.metadata, "pvc-uid");
    bound.spec.as_mut().unwrap().volume_name = Some("pvc-01234567".into());
    bound.metadata.finalizers = Some(vec!["kubernetes.io/pvc-protection".into()]);
    bound.metadata.annotations.as_mut().unwrap().extend([
        ("pv.kubernetes.io/bind-completed".into(), "yes".into()),
        ("pv.kubernetes.io/bound-by-controller".into(), "yes".into()),
        (
            "volume.kubernetes.io/selected-node".into(),
            "worker-node-01".into(),
        ),
        (
            "volume.kubernetes.io/storage-provisioner".into(),
            "csi.example.com".into(),
        ),
    ]);
    desired.validate_persistent_volume_claim(&bound).unwrap();

    let mut foreign_finalizer = bound.clone();
    foreign_finalizer
        .metadata
        .finalizers
        .as_mut()
        .unwrap()
        .push("foreign.example/hold".into());
    assert!(matches!(
        desired.validate_persistent_volume_claim(&foreign_finalizer),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut foreign_annotation = bound;
    foreign_annotation
        .metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("foreign.example/injected".into(), "true".into());
    assert!(matches!(
        desired.validate_persistent_volume_claim(&foreign_annotation),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));
}

#[test]
fn observed_validation_allows_only_known_binder_and_scheduler_fields() {
    let desired = desired();

    let mut scheduled = desired.pod().clone();
    mark_observed(&mut scheduled.metadata, "pod-uid");
    scheduled.spec.as_mut().unwrap().node_name = Some("worker-node-01".into());
    scheduled.spec.as_mut().unwrap().priority = Some(0);
    desired.validate_pod(&scheduled).unwrap();
    scheduled.spec.as_mut().unwrap().priority = Some(1000);
    assert!(matches!(
        desired.validate_pod(&scheduled),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
    scheduled.spec.as_mut().unwrap().priority = Some(0);
    scheduled.spec.as_mut().unwrap().node_name = Some("INVALID_NODE".into());
    assert!(matches!(
        desired.validate_pod(&scheduled),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut bound = desired.persistent_volume_claim().clone();
    mark_observed(&mut bound.metadata, "pvc-uid");
    bound.spec.as_mut().unwrap().volume_name = Some("pvc-01234567".into());
    desired.validate_persistent_volume_claim(&bound).unwrap();
    bound.spec.as_mut().unwrap().volume_name = Some("INVALID_VOLUME".into());
    assert!(matches!(
        desired.validate_persistent_volume_claim(&bound),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
}

#[test]
fn supervisor_starts_on_the_fresh_pvc_root_before_creating_private_directories() {
    let desired = desired();
    let container = &desired.pod().spec.as_ref().unwrap().containers[0];
    assert_eq!(container.working_dir.as_deref(), Some("/session"));
    assert_eq!(
        container.command.as_deref(),
        Some(&["/usr/local/bin/openab-session-supervisor".to_string()][..])
    );
    let env = container.env.as_ref().unwrap();
    assert_eq!(
        env.iter()
            .find(|entry| entry.name == "HOME")
            .unwrap()
            .value
            .as_deref(),
        Some("/session/home")
    );
    assert_eq!(
        env.iter()
            .find(|entry| entry.name == "OPENAB_WORKSPACE")
            .unwrap()
            .value
            .as_deref(),
        Some("/session/workspace")
    );
}

#[test]
fn runtime_class_selection_is_observed_pinned_and_admission_stable() {
    let allowed = AllowedRuntimeClass::new("kata-qemu", "kata-qemu").unwrap();
    let mut observed = runtime_class("kata-qemu", "kata-qemu");
    let selected = RuntimeClassSelection::from_observed(&observed, [allowed.clone()]).unwrap();

    observed.overhead = Some(Overhead::default());
    assert!(RuntimeClassSelection::from_observed(&observed, [allowed.clone()]).is_err());
    observed.overhead = None;
    observed.scheduling = Some(Scheduling::default());
    assert!(RuntimeClassSelection::from_observed(&observed, [allowed]).is_err());

    let desired = DesiredGeneration::build(
        context(),
        profile(PvcAccessMode::default(), Some(selected), None),
        [9; 32],
    )
    .unwrap();
    let mut replaced = runtime_class("kata-qemu", "kata-qemu");
    replaced.metadata.uid = Some("replacement-uid".into());
    assert!(desired.validate_runtime_class(&replaced).is_err());
    let mut mutated = runtime_class("kata-qemu", "runc");
    mutated.metadata.uid = Some("kata-qemu-uid".into());
    assert!(desired.validate_runtime_class(&mutated).is_err());
}

#[test]
fn generation_network_policy_is_default_deny_with_only_explicit_egress() {
    let desired = desired();
    let policy = desired.network_policy();
    let spec = policy.spec.as_ref().unwrap();
    assert!(spec.ingress.is_none());
    assert_eq!(
        spec.policy_types.as_deref(),
        Some(&["Ingress".to_string(), "Egress".to_string()][..])
    );

    let pod_labels = desired.pod().metadata.labels.as_ref().unwrap();
    let selected = spec
        .pod_selector
        .as_ref()
        .unwrap()
        .match_labels
        .as_ref()
        .unwrap();
    assert_eq!(
        selected.get("openab.dev/session"),
        pod_labels.get("openab.dev/session")
    );
    assert_eq!(
        selected.get("openab.dev/generation"),
        Some(&"1".to_string())
    );
    assert_eq!(
        selected.get("openab.dev/resource"),
        Some(&"worker-pod".to_string())
    );

    let egress = spec.egress.as_ref().unwrap();
    assert_eq!(egress.len(), 2);
    let selector_peer = &egress[0].to.as_ref().unwrap()[0];
    assert_eq!(
        selector_peer
            .namespace_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap()
            .get("kubernetes.io/metadata.name")
            .map(String::as_str),
        Some("openab-system")
    );
    assert_eq!(
        selector_peer
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap()
            .get("app.kubernetes.io/name")
            .map(String::as_str),
        Some("session-relay")
    );
    assert_eq!(
        egress[0].ports.as_ref().unwrap()[0].protocol.as_deref(),
        Some("TCP")
    );
    let cidr_peer = &egress[1].to.as_ref().unwrap()[0];
    assert_eq!(cidr_peer.ip_block.as_ref().unwrap().cidr, "10.96.0.10/32");
    assert_eq!(egress[1].ports.as_ref().unwrap().len(), 2);
}

#[test]
fn network_policy_observation_rejects_foreign_or_mutated_objects() {
    let desired = desired();
    let mut foreign = desired.network_policy().clone();
    mark_observed(&mut foreign.metadata, "network-policy-uid");
    foreign.metadata.owner_references.as_mut().unwrap()[0].uid = "foreign".into();
    assert!(matches!(
        desired.validate_network_policy(&foreign),
        Err(ResourceValidationError::MetadataMismatch { .. })
    ));

    let mut canonical_empty = desired.network_policy().clone();
    mark_observed(&mut canonical_empty.metadata, "network-policy-uid");
    canonical_empty.spec.as_mut().unwrap().ingress = Some(Vec::new());
    desired.validate_network_policy(&canonical_empty).unwrap();

    let mut widened = desired.network_policy().clone();
    mark_observed(&mut widened.metadata, "network-policy-uid");
    widened.spec.as_mut().unwrap().ingress = Some(vec![Default::default()]);
    assert!(matches!(
        desired.validate_network_policy(&widened),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut extra_egress = desired.network_policy().clone();
    mark_observed(&mut extra_egress.metadata, "network-policy-uid");
    extra_egress
        .spec
        .as_mut()
        .unwrap()
        .egress
        .as_mut()
        .unwrap()
        .push(Default::default());
    assert!(matches!(
        desired.validate_network_policy(&extra_egress),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
}

#[test]
fn egress_profile_rejects_unbounded_or_malformed_rules() {
    assert!(EgressPort::new(EgressProtocol::Tcp, 0).is_err());
    let https = EgressPort::new(EgressProtocol::Tcp, 443).unwrap();
    assert!(TrustedEgressRule::for_cidr("10.0.0.1", [https.clone()]).is_err());
    assert!(TrustedEgressRule::for_cidr("10.0.0.1/24", [https.clone()]).is_err());
    assert!(TrustedEgressRule::for_cidr("0.0.0.0/0", [https.clone()]).is_err());
    assert!(TrustedEgressRule::for_cidr("10.0.0.1/32", Vec::<EgressPort>::new()).is_err());
    assert!(TrustedEgressRule::for_selectors(
        BTreeMap::from([("INVALID KEY".into(), "namespace".into())]),
        BTreeMap::from([("app".into(), "relay".into())]),
        [https]
    )
    .is_err());

    assert!(MvpWorkerProfile::new(
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
        IMAGE,
        ["/usr/local/bin/openab-session-supervisor"],
        ["serve"],
        PersistentWorkspace::new("20Gi", "encrypted-rwo", PvcAccessMode::default()).unwrap(),
        WorkerResources::new("250m", "1", "256Mi", "2Gi", "1Gi", "8Gi").unwrap(),
        Vec::<TrustedEgressRule>::new(),
        RunAsIdentity::new(10001, 10001).unwrap(),
        None,
        None,
    )
    .is_err());
}

#[test]
fn every_generated_name_is_dns_safe() {
    let desired = desired();
    for name in [
        desired
            .persistent_volume_claim()
            .metadata
            .name
            .as_deref()
            .unwrap(),
        desired
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .unwrap(),
        desired.service_account().metadata.name.as_deref().unwrap(),
        desired.pod().metadata.name.as_deref().unwrap(),
        desired.network_policy().metadata.name.as_deref().unwrap(),
    ] {
        assert!(name.len() <= 63);
        assert!(name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
        assert!(!name.starts_with('-'));
        assert!(!name.ends_with('-'));
    }
}

#[test]
fn profile_requires_digest_image_and_allowlisted_runtime() {
    let base = || {
        (
            ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap(),
            PersistentWorkspace::new("20Gi", "encrypted-rwo", PvcAccessMode::default()).unwrap(),
            WorkerResources::new("250m", "1", "256Mi", "2Gi", "1Gi", "8Gi").unwrap(),
            RunAsIdentity::new(10001, 10001).unwrap(),
        )
    };
    let (profile_ref, workspace, resources, identity) = base();
    assert!(MvpWorkerProfile::new(
        profile_ref,
        "ghcr.io/example/openab-worker:latest",
        ["worker"],
        ["serve"],
        workspace,
        resources,
        trusted_egress(),
        identity,
        None,
        None,
    )
    .is_err());
    assert!(RuntimeClassSelection::from_observed(
        &runtime_class("kata-qemu", "kata-qemu"),
        [AllowedRuntimeClass::new("gvisor", "runsc").unwrap()]
    )
    .is_err());
    let (profile_ref, workspace, resources, identity) = base();
    assert!(MvpWorkerProfile::new(
        profile_ref,
        IMAGE,
        ["relative-supervisor"],
        ["serve"],
        workspace,
        resources,
        trusted_egress(),
        identity,
        None,
        None,
    )
    .is_err());
    assert!(WorkerResources::new("1--", "1", "256Mi", "2Gi", "1Gi", "8Gi").is_err());
    for resources in [
        WorkerResources::new("2", "1", "256Mi", "2Gi", "1Gi", "8Gi"),
        WorkerResources::new("250m", "1", "3Gi", "2Gi", "1Gi", "8Gi"),
        WorkerResources::new("250m", "1", "256Mi", "2Gi", "9Gi", "8Gi"),
    ] {
        assert!(resources.is_err());
    }
    for cpu in ["1000m", "01", "1.5", "1e3"] {
        assert!(WorkerResources::new(cpu, "1", "256Mi", "2Gi", "1Gi", "8Gi").is_err());
    }
    assert!(WorkerResources::new(
        "9223372036854775808",
        "9223372036854775808",
        "256Mi",
        "2Gi",
        "1Gi",
        "8Gi"
    )
    .is_err());
    for storage in ["1.5Gi", "1024Mi", "01Gi", "+1Gi", "1e3"] {
        assert!(
            PersistentWorkspace::new(storage, "encrypted-rwo", PvcAccessMode::default()).is_err()
        );
    }
    assert!(PersistentWorkspace::new("0Gi", "encrypted-rwo", PvcAccessMode::default()).is_err());
    assert!(PersistentWorkspace::new("20Gi", "", PvcAccessMode::default()).is_err());
}

#[test]
fn observed_validation_covers_every_generated_resource() {
    let desired = desired();
    let mut claim = desired.persistent_volume_claim().clone();
    mark_observed(&mut claim.metadata, "pvc-uid");
    let mut secret = desired.registration_secret().clone();
    mark_observed(&mut secret.metadata, "secret-uid");
    let mut account = desired.service_account().clone();
    mark_observed(&mut account.metadata, "service-account-uid");
    let mut pod = desired.pod().clone();
    mark_observed(&mut pod.metadata, "pod-uid");
    let mut policy = desired.network_policy().clone();
    mark_observed(&mut policy.metadata, "network-policy-uid");
    desired.validate_persistent_volume_claim(&claim).unwrap();
    desired.validate_registration_secret(&secret).unwrap();
    desired.validate_service_account(&account).unwrap();
    desired.validate_pod(&pod).unwrap();
    desired.validate_network_policy(&policy).unwrap();

    let mut changed_secret = desired.registration_secret().clone();
    mark_observed(&mut changed_secret.metadata, "secret-uid");
    changed_secret
        .data
        .as_mut()
        .unwrap()
        .insert("token".into(), ByteString(vec![0; 32]));
    assert!(matches!(
        desired.validate_registration_secret(&changed_secret),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut changed_claim = desired.persistent_volume_claim().clone();
    mark_observed(&mut changed_claim.metadata, "pvc-uid");
    changed_claim.spec.as_mut().unwrap().access_modes = Some(vec!["ReadWriteMany".into()]);
    assert!(matches!(
        desired.validate_persistent_volume_claim(&changed_claim),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));

    let mut changed_account = desired.service_account().clone();
    mark_observed(&mut changed_account.metadata, "service-account-uid");
    changed_account.automount_service_account_token = Some(true);
    assert!(matches!(
        desired.validate_service_account(&changed_account),
        Err(ResourceValidationError::SpecMismatch { .. })
    ));
}

#[test]
fn generated_resources_never_retain_raw_scope_or_thread_keys() {
    let desired = desired();
    let serialized = serde_json::to_string(&(
        desired.persistent_volume_claim(),
        desired.registration_secret(),
        desired.service_account(),
        desired.pod(),
        desired.network_policy(),
    ))
    .unwrap();
    assert!(!serialized.contains("private-team-scope"));
    assert!(!serialized.contains("discord:private-thread-123"));
}

#[test]
fn desired_generation_debug_output_redacts_registration_token() {
    let desired = DesiredGeneration::build(
        context(),
        profile(PvcAccessMode::default(), None, None),
        *b"secret-token-must-never-appear!!",
    )
    .unwrap();
    let debug = format!("{desired:?}");
    assert!(!debug.contains("secret-token-must-never-appear"));
    assert!(debug.contains("registration_secret: <redacted>"));
}
