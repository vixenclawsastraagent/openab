use crate::bridge::SessionBinding;
use crate::identity::{IdentityError, ResourceNames, ScopeId, SessionId};
use crate::state::{Fence, ProfileRef, SessionAnchorV1};
use crate::wire::WorkerRegistrationV1;
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerResizePolicy,
    EmptyDirVolumeSource, EnvVar, EnvVarSource, KeyToPath, LocalObjectReference,
    ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource, Pod, PodOS, PodSecurityContext, PodSpec,
    ResourceRequirements, SeccompProfile, Secret, SecretVolumeSource, SecurityContext,
    ServiceAccount, Toleration, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyPeer, NetworkPolicyPort,
    NetworkPolicySpec,
};
use k8s_openapi::api::node::v1::RuntimeClass;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use k8s_openapi::ByteString;
use rustls::RootCertStore;
use rustls_pemfile::Item;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::net::IpAddr;
use thiserror::Error;
use uuid::Uuid;

const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const MANAGED_BY_VALUE: &str = "openab-session-controller";
const RESOURCE_LABEL: &str = "openab.dev/resource";
const SESSION_LABEL: &str = "openab.dev/session";
const GENERATION_LABEL: &str = "openab.dev/generation";
const SCOPE_ANNOTATION: &str = "openab.dev/scope-id";
const SESSION_ANNOTATION: &str = "openab.dev/session-id";
const GENERATION_ANNOTATION: &str = "openab.dev/generation";
const ATTEMPT_ANNOTATION: &str = "openab.dev/attempt-id";
const INCARNATION_ANNOTATION: &str = "openab.dev/incarnation-id";
const PROFILE_NAME_ANNOTATION: &str = "openab.dev/profile-name";
const PROFILE_VERSION_ANNOTATION: &str = "openab.dev/profile-version";
const ANCHOR_NAME_ANNOTATION: &str = "openab.dev/anchor-name";
const ANCHOR_UID_ANNOTATION: &str = "openab.dev/anchor-uid";
const SKILLS_NAME_ANNOTATION: &str = "openab.dev/skills-config-map-name";
const SKILLS_UID_ANNOTATION: &str = "openab.dev/skills-config-map-uid";
const SKILLS_RESOURCE_VERSION_ANNOTATION: &str = "openab.dev/skills-config-map-resource-version";
const RUNTIME_NAME_ANNOTATION: &str = "openab.dev/runtime-class-name";
const RUNTIME_HANDLER_ANNOTATION: &str = "openab.dev/runtime-class-handler";
const RUNTIME_UID_ANNOTATION: &str = "openab.dev/runtime-class-uid";
const RUNTIME_RESOURCE_VERSION_ANNOTATION: &str = "openab.dev/runtime-class-resource-version";
const WORKER_RELAY_CA_NAME_ANNOTATION: &str = "openab.dev/worker-relay-ca-config-map-name";
const WORKER_RELAY_CA_UID_ANNOTATION: &str = "openab.dev/worker-relay-ca-config-map-uid";
const WORKER_RELAY_CA_RESOURCE_VERSION_ANNOTATION: &str =
    "openab.dev/worker-relay-ca-config-map-resource-version";
const WORKER_IMAGE_CONTRACT_ANNOTATION: &str = "openab.dev/worker-image-contract";
const WORKER_IMAGE_CONTRACT: &str = "session-layout-v1";
/// Fixed key containing the worker relay trust bundle.
pub const WORKER_RELAY_CA_CONFIG_MAP_KEY: &str = "ca.crt";
/// Maximum accepted UTF-8 bytes in the worker relay trust bundle.
pub const MAX_WORKER_RELAY_CA_PEM_BYTES: usize = 256 * 1024;
const CERTIFICATE_BEGIN: &[u8] = b"-----BEGIN CERTIFICATE-----";
const CERTIFICATE_END: &[u8] = b"-----END CERTIFICATE-----";
const TOKEN_KEY: &str = "token";
const BINDING_KEY: &str = "binding.json";
const TOKEN_DIRECTORY: &str = "/var/run/openab-registration";
const TOKEN_FILE: &str = "/var/run/openab-registration/token";
const BINDING_FILE: &str = "/var/run/openab-registration/binding.json";
const WORKER_RELAY_CA_VOLUME: &str = "controller-ca";
const WORKER_RELAY_CA_DIRECTORY: &str = "/var/run/openab-controller-ca";
const WORKER_RELAY_CA_FILE: &str = "/var/run/openab-controller-ca/ca.crt";
const WORKER_INIT_EXECUTABLE: &str = "/usr/bin/tini";
const SESSION_ROOT: &str = "/session";
const SESSION_HOME: &str = "/session/home";
/// Writable workspace root promised by the `session-layout-v1` worker image.
///
/// Relay activation responses and generated worker Pods must use this single
/// value so a future layout revision cannot silently split the contract.
pub const SESSION_WORKSPACE_V1: &str = "/session/workspace";
const MAX_REGISTRATION_BINDING_BYTES: usize = 4 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResourceBuildError {
    #[error("invalid Kubernetes namespace")]
    InvalidNamespace,
    #[error("anchor name does not match the session resource names")]
    AnchorNameMismatch,
    #[error("anchor UID must be a non-empty, printable identifier")]
    InvalidAnchorUid,
    #[error("resource names do not belong to the anchor session")]
    ResourceNamesMismatch,
    #[error("profile version is not safe for Kubernetes binding metadata")]
    InvalidProfileVersion,
    #[error("worker profile does not match the profile bound to the session anchor")]
    ProfileMismatch,
    #[error("worker image must be pinned by a lowercase sha256 digest")]
    UnpinnedImage,
    #[error("worker supervisor command must be absolute, non-empty, and NUL-free")]
    InvalidCommand,
    #[error("worker arguments must not contain empty or NUL-containing values")]
    InvalidArguments,
    #[error("invalid or non-canonical Kubernetes resource quantity for {field}")]
    InvalidQuantity { field: &'static str },
    #[error("{resource} request exceeds its limit")]
    RequestExceedsLimit { resource: &'static str },
    #[error("invalid storage class name")]
    InvalidStorageClass,
    #[error("runAs user and group IDs must be positive")]
    InvalidRunAsIdentity,
    #[error("runtime class name is invalid")]
    InvalidRuntimeClass,
    #[error("runtime class is not in the controller allowlist")]
    RuntimeClassNotAllowed,
    #[error("runtime class observation is missing trusted metadata")]
    InvalidRuntimeClassObservation,
    #[error("runtime class would mutate Pod scheduling or resource overhead")]
    RuntimeClassMutatesPod,
    #[error("worker egress policy must contain at least one explicit rule")]
    EmptyEgressPolicy,
    #[error("trusted egress rule must contain at least one explicit port")]
    EmptyEgressPorts,
    #[error("trusted egress port must be between 1 and 65535")]
    InvalidEgressPort,
    #[error("trusted egress selector contains an invalid label")]
    InvalidEgressSelector,
    #[error("trusted egress CIDR must be canonical and explicitly scoped")]
    InvalidEgressCidr,
    #[error("invalid pinned skills ConfigMap {field}")]
    InvalidSkillsConfigMap { field: &'static str },
    #[error("invalid pinned worker relay CA ConfigMap {field}")]
    InvalidWorkerRelayCaConfigMap { field: &'static str },
    #[error("could not derive a generation resource name")]
    ResourceName(#[source] IdentityError),
    #[error("session anchor could not produce a worker registration binding")]
    InvalidRegistrationBinding,
    #[error("worker registration binding could not be encoded")]
    RegistrationBindingEncoding,
    #[error("worker registration binding exceeds its fixed size limit")]
    RegistrationBindingTooLarge,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResourceValidationError {
    #[error("{resource} metadata does not match the desired generation ({field})")]
    MetadataMismatch {
        resource: &'static str,
        field: &'static str,
    },
    #[error("{resource} specification or protected data does not match the desired generation")]
    SpecMismatch { resource: &'static str },
    #[error("this generation does not use a skills ConfigMap")]
    SkillsConfigMapNotConfigured,
    #[error("this generation does not use a worker relay CA ConfigMap")]
    WorkerRelayCaConfigMapNotConfigured,
    #[error("this generation does not use a RuntimeClass")]
    RuntimeClassNotConfigured,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationContext {
    namespace: String,
    anchor_name: String,
    anchor_uid: String,
    resource_names: ResourceNames,
    session_id: SessionId,
    scope_id: ScopeId,
    fence: Fence,
    incarnation_id: Uuid,
    profile: ProfileRef,
}

impl GenerationContext {
    pub fn from_anchor(
        namespace: impl Into<String>,
        anchor_name: impl Into<String>,
        anchor_uid: impl Into<String>,
        anchor: &SessionAnchorV1,
        resource_names: ResourceNames,
    ) -> Result<Self, ResourceBuildError> {
        let namespace = namespace.into();
        if !is_dns_label(&namespace) {
            return Err(ResourceBuildError::InvalidNamespace);
        }

        let expected_names = ResourceNames::new(anchor.session_id());
        if resource_names != expected_names {
            return Err(ResourceBuildError::ResourceNamesMismatch);
        }

        let anchor_name = anchor_name.into();
        if anchor_name != resource_names.anchor() {
            return Err(ResourceBuildError::AnchorNameMismatch);
        }

        let anchor_uid = anchor_uid.into();
        if !is_printable_identifier(&anchor_uid) {
            return Err(ResourceBuildError::InvalidAnchorUid);
        }
        if !is_annotation_identifier(anchor.profile().version()) {
            return Err(ResourceBuildError::InvalidProfileVersion);
        }

        Ok(Self {
            namespace,
            anchor_name,
            anchor_uid,
            resource_names,
            session_id: anchor.session_id(),
            scope_id: anchor.scope_id(),
            fence: anchor.fence().clone(),
            incarnation_id: anchor.incarnation_id(),
            profile: anchor.profile().clone(),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn anchor_name(&self) -> &str {
        &self.anchor_name
    }

    pub fn anchor_uid(&self) -> &str {
        &self.anchor_uid
    }

    pub fn resource_names(&self) -> ResourceNames {
        self.resource_names
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub fn fence(&self) -> &Fence {
        &self.fence
    }

    pub fn incarnation_id(&self) -> Uuid {
        self.incarnation_id
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PvcAccessMode {
    #[default]
    ReadWriteOncePod,
    ReadWriteOnce,
}

impl PvcAccessMode {
    fn kubernetes_value(self) -> &'static str {
        match self {
            Self::ReadWriteOncePod => "ReadWriteOncePod",
            Self::ReadWriteOnce => "ReadWriteOnce",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentWorkspace {
    size: String,
    storage_class: String,
    access_mode: PvcAccessMode,
}

impl PersistentWorkspace {
    pub fn new(
        size: impl Into<String>,
        storage_class: impl Into<String>,
        access_mode: PvcAccessMode,
    ) -> Result<Self, ResourceBuildError> {
        let size = size.into();
        validate_binary_quantity(&size, "workspace.storage")?;
        let storage_class = storage_class.into();
        if !is_dns_subdomain(&storage_class) {
            return Err(ResourceBuildError::InvalidStorageClass);
        }
        Ok(Self {
            size,
            storage_class,
            access_mode,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerResources {
    cpu_request: String,
    cpu_limit: String,
    memory_request: String,
    memory_limit: String,
    ephemeral_request: String,
    ephemeral_limit: String,
}

impl WorkerResources {
    pub fn new(
        cpu_request: impl Into<String>,
        cpu_limit: impl Into<String>,
        memory_request: impl Into<String>,
        memory_limit: impl Into<String>,
        ephemeral_request: impl Into<String>,
        ephemeral_limit: impl Into<String>,
    ) -> Result<Self, ResourceBuildError> {
        let resources = Self {
            cpu_request: cpu_request.into(),
            cpu_limit: cpu_limit.into(),
            memory_request: memory_request.into(),
            memory_limit: memory_limit.into(),
            ephemeral_request: ephemeral_request.into(),
            ephemeral_limit: ephemeral_limit.into(),
        };
        let cpu_request = validate_cpu_quantity(&resources.cpu_request, "resources.requests.cpu")?;
        let cpu_limit = validate_cpu_quantity(&resources.cpu_limit, "resources.limits.cpu")?;
        let memory_request =
            validate_binary_quantity(&resources.memory_request, "resources.requests.memory")?;
        let memory_limit =
            validate_binary_quantity(&resources.memory_limit, "resources.limits.memory")?;
        let ephemeral_request = validate_binary_quantity(
            &resources.ephemeral_request,
            "resources.requests.ephemeral-storage",
        )?;
        let ephemeral_limit = validate_binary_quantity(
            &resources.ephemeral_limit,
            "resources.limits.ephemeral-storage",
        )?;
        for (resource, request, limit) in [
            ("cpu", cpu_request, cpu_limit),
            ("memory", memory_request, memory_limit),
            ("ephemeral-storage", ephemeral_request, ephemeral_limit),
        ] {
            if request > limit {
                return Err(ResourceBuildError::RequestExceedsLimit { resource });
            }
        }
        Ok(resources)
    }

    fn kubernetes(&self) -> ResourceRequirements {
        ResourceRequirements {
            claims: None,
            limits: Some(BTreeMap::from([
                ("cpu".into(), Quantity(self.cpu_limit.clone())),
                ("memory".into(), Quantity(self.memory_limit.clone())),
                (
                    "ephemeral-storage".into(),
                    Quantity(self.ephemeral_limit.clone()),
                ),
            ])),
            requests: Some(BTreeMap::from([
                ("cpu".into(), Quantity(self.cpu_request.clone())),
                ("memory".into(), Quantity(self.memory_request.clone())),
                (
                    "ephemeral-storage".into(),
                    Quantity(self.ephemeral_request.clone()),
                ),
            ])),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunAsIdentity {
    uid: i64,
    gid: i64,
}

impl RunAsIdentity {
    pub fn new(uid: i64, gid: i64) -> Result<Self, ResourceBuildError> {
        if !(1..=u32::MAX as i64).contains(&uid) || !(1..=u32::MAX as i64).contains(&gid) {
            return Err(ResourceBuildError::InvalidRunAsIdentity);
        }
        Ok(Self { uid, gid })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedRuntimeClass {
    name: String,
    handler: String,
}

impl AllowedRuntimeClass {
    pub fn new(
        name: impl Into<String>,
        handler: impl Into<String>,
    ) -> Result<Self, ResourceBuildError> {
        let allowed = Self {
            name: name.into(),
            handler: handler.into(),
        };
        if !is_dns_subdomain(&allowed.name) || !is_dns_label(&allowed.handler) {
            return Err(ResourceBuildError::InvalidRuntimeClass);
        }
        Ok(allowed)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn handler(&self) -> &str {
        &self.handler
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeClassSelection {
    name: String,
    handler: String,
    uid: String,
    resource_version: String,
}

impl RuntimeClassSelection {
    pub fn from_observed<I>(
        observed: &RuntimeClass,
        allowlist: I,
    ) -> Result<Self, ResourceBuildError>
    where
        I: IntoIterator<Item = AllowedRuntimeClass>,
    {
        let name = observed
            .metadata
            .name
            .as_deref()
            .ok_or(ResourceBuildError::InvalidRuntimeClassObservation)?;
        if !is_dns_subdomain(name) || !is_dns_label(&observed.handler) {
            return Err(ResourceBuildError::InvalidRuntimeClass);
        }
        if observed.metadata.namespace.is_some()
            || observed.metadata.deletion_timestamp.is_some()
            || observed.overhead.is_some()
            || observed.scheduling.is_some()
        {
            if observed.overhead.is_some() || observed.scheduling.is_some() {
                return Err(ResourceBuildError::RuntimeClassMutatesPod);
            }
            return Err(ResourceBuildError::InvalidRuntimeClassObservation);
        }
        let uid = observed
            .metadata
            .uid
            .as_deref()
            .filter(|value| is_printable_identifier(value))
            .ok_or(ResourceBuildError::InvalidRuntimeClassObservation)?;
        let resource_version = observed
            .metadata
            .resource_version
            .as_deref()
            .filter(|value| is_printable_identifier(value))
            .ok_or(ResourceBuildError::InvalidRuntimeClassObservation)?;
        if !allowlist
            .into_iter()
            .any(|allowed| allowed.name == name && allowed.handler == observed.handler)
        {
            return Err(ResourceBuildError::RuntimeClassNotAllowed);
        }
        Ok(Self {
            name: name.into(),
            handler: observed.handler.clone(),
            uid: uid.into(),
            resource_version: resource_version.into(),
        })
    }

    pub(crate) fn matches_intent(&self, name: &str, handler: &str) -> bool {
        self.name == name && self.handler == handler
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EgressProtocol {
    Tcp,
    Udp,
}

impl EgressProtocol {
    fn kubernetes_value(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressPort {
    protocol: EgressProtocol,
    port: u16,
}

impl EgressPort {
    pub fn new(protocol: EgressProtocol, port: u16) -> Result<Self, ResourceBuildError> {
        if port == 0 {
            return Err(ResourceBuildError::InvalidEgressPort);
        }
        Ok(Self { protocol, port })
    }

    fn kubernetes(&self) -> NetworkPolicyPort {
        NetworkPolicyPort {
            end_port: None,
            port: Some(IntOrString::Int(i32::from(self.port))),
            protocol: Some(self.protocol.kubernetes_value().into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TrustedEgressTarget {
    Selectors {
        namespace_labels: BTreeMap<String, String>,
        pod_labels: BTreeMap<String, String>,
    },
    Cidr(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedEgressRule {
    target: TrustedEgressTarget,
    ports: Vec<EgressPort>,
}

impl TrustedEgressRule {
    pub fn for_selectors<I>(
        namespace_labels: BTreeMap<String, String>,
        pod_labels: BTreeMap<String, String>,
        ports: I,
    ) -> Result<Self, ResourceBuildError>
    where
        I: IntoIterator<Item = EgressPort>,
    {
        if namespace_labels.is_empty()
            || pod_labels.is_empty()
            || !valid_label_map(&namespace_labels)
            || !valid_label_map(&pod_labels)
        {
            return Err(ResourceBuildError::InvalidEgressSelector);
        }
        Self::with_target(
            TrustedEgressTarget::Selectors {
                namespace_labels,
                pod_labels,
            },
            ports,
        )
    }

    pub fn for_cidr<I>(cidr: impl Into<String>, ports: I) -> Result<Self, ResourceBuildError>
    where
        I: IntoIterator<Item = EgressPort>,
    {
        let cidr = cidr.into();
        if !is_scoped_canonical_cidr(&cidr) {
            return Err(ResourceBuildError::InvalidEgressCidr);
        }
        Self::with_target(TrustedEgressTarget::Cidr(cidr), ports)
    }

    fn with_target<I>(target: TrustedEgressTarget, ports: I) -> Result<Self, ResourceBuildError>
    where
        I: IntoIterator<Item = EgressPort>,
    {
        let ports: Vec<EgressPort> = ports.into_iter().collect();
        if ports.is_empty() {
            return Err(ResourceBuildError::EmptyEgressPorts);
        }
        Ok(Self { target, ports })
    }

    fn kubernetes(&self) -> NetworkPolicyEgressRule {
        let peer = match &self.target {
            TrustedEgressTarget::Selectors {
                namespace_labels,
                pod_labels,
            } => NetworkPolicyPeer {
                ip_block: None,
                namespace_selector: Some(exact_selector(namespace_labels.clone())),
                pod_selector: Some(exact_selector(pod_labels.clone())),
            },
            TrustedEgressTarget::Cidr(cidr) => NetworkPolicyPeer {
                ip_block: Some(IPBlock {
                    cidr: cidr.clone(),
                    except: None,
                }),
                namespace_selector: None,
                pod_selector: None,
            },
        };
        NetworkPolicyEgressRule {
            ports: Some(self.ports.iter().map(EgressPort::kubernetes).collect()),
            to: Some(vec![peer]),
        }
    }
}

/// A preflight and audit pin for centrally managed, immutable skills.
///
/// Kubernetes ConfigMap volumes reference a name, not a UID or resourceVersion.
/// The deployment must therefore use versioned names that are never reused and
/// deny update/delete/recreate through RBAC or admission. The controller must
/// revalidate this pin immediately before Pod creation and after the Pod is
/// observed ready. A stronger future backend can snapshot content into a
/// generation-owned immutable object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinnedSkillsConfigMap {
    name: String,
    uid: String,
    resource_version: String,
}

impl PinnedSkillsConfigMap {
    fn new(
        name: impl Into<String>,
        uid: impl Into<String>,
        resource_version: impl Into<String>,
    ) -> Result<Self, ResourceBuildError> {
        let pin = Self {
            name: name.into(),
            uid: uid.into(),
            resource_version: resource_version.into(),
        };
        Self::validate_name(&pin.name)?;
        if !is_printable_identifier(&pin.uid) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap { field: "uid" });
        }
        if !is_printable_identifier(&pin.resource_version) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap {
                field: "resourceVersion",
            });
        }
        Ok(pin)
    }

    /// Pins a versioned, immutable skills ConfigMap from trusted Kubernetes
    /// observation rather than administrator-supplied UID metadata.
    pub fn from_observed(
        expected_namespace: &str,
        observed: &ConfigMap,
    ) -> Result<Self, ResourceBuildError> {
        if !is_dns_label(expected_namespace) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap {
                field: "expectedNamespace",
            });
        }
        let name = observed.metadata.name.as_deref().ok_or(
            ResourceBuildError::InvalidSkillsConfigMap {
                field: "metadata.name",
            },
        )?;
        Self::validate_name(name)?;
        if observed.metadata.namespace.as_deref() != Some(expected_namespace) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap {
                field: "metadata.namespace",
            });
        }
        if observed.metadata.deletion_timestamp.is_some() {
            return Err(ResourceBuildError::InvalidSkillsConfigMap {
                field: "metadata.deletionTimestamp",
            });
        }
        if observed.immutable != Some(true) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap { field: "immutable" });
        }
        let uid =
            observed
                .metadata
                .uid
                .as_deref()
                .ok_or(ResourceBuildError::InvalidSkillsConfigMap {
                    field: "metadata.uid",
                })?;
        let resource_version = observed.metadata.resource_version.as_deref().ok_or(
            ResourceBuildError::InvalidSkillsConfigMap {
                field: "metadata.resourceVersion",
            },
        )?;
        Self::new(name, uid, resource_version)
    }

    pub(crate) fn validate_name(name: &str) -> Result<(), ResourceBuildError> {
        if !is_dns_subdomain(name) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap { field: "name" });
        }
        Ok(())
    }

    pub(crate) fn matches_intent(&self, name: &str) -> bool {
        self.name == name
    }
}

/// Identity pin for the centrally managed worker-relay trust bundle.
///
/// The certificate bytes are validated while observing the ConfigMap and are
/// deliberately discarded. Only the Kubernetes identity needed for later
/// drift checks is retained. The operator must use versioned names that are
/// never reused because a ConfigMap volume ultimately selects by name.
#[derive(Clone, PartialEq, Eq)]
pub struct PinnedWorkerRelayCaConfigMap {
    name: String,
    uid: String,
    resource_version: String,
}

impl PinnedWorkerRelayCaConfigMap {
    fn new(
        name: impl Into<String>,
        uid: impl Into<String>,
        resource_version: impl Into<String>,
    ) -> Result<Self, ResourceBuildError> {
        let pin = Self {
            name: name.into(),
            uid: uid.into(),
            resource_version: resource_version.into(),
        };
        if !is_dns_subdomain(&pin.name) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.name",
            });
        }
        if !is_printable_identifier(&pin.uid) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.uid",
            });
        }
        if !is_printable_identifier(&pin.resource_version) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.resourceVersion",
            });
        }
        Ok(pin)
    }

    /// Validate and pin one live, immutable CA ConfigMap observation.
    ///
    /// The returned value never retains the PEM or the observed Kubernetes
    /// object, keeping trust material out of session state and debug output.
    pub fn from_observed(
        expected_namespace: &str,
        observed: &ConfigMap,
    ) -> Result<Self, ResourceBuildError> {
        if !is_dns_label(expected_namespace) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "expectedNamespace",
            });
        }
        let name = observed.metadata.name.as_deref().ok_or(
            ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.name",
            },
        )?;
        if !is_dns_subdomain(name) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.name",
            });
        }
        if observed.metadata.namespace.as_deref() != Some(expected_namespace) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.namespace",
            });
        }
        if observed.metadata.deletion_timestamp.is_some() {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.deletionTimestamp",
            });
        }
        if observed.immutable != Some(true) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap { field: "immutable" });
        }
        if observed
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|references| !references.is_empty())
        {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.ownerReferences",
            });
        }
        if has_session_management_labels(observed.metadata.labels.as_ref()) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.labels",
            });
        }
        if observed
            .binary_data
            .as_ref()
            .is_some_and(|data| !data.is_empty())
        {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "binaryData",
            });
        }
        let data = observed
            .data
            .as_ref()
            .ok_or(ResourceBuildError::InvalidWorkerRelayCaConfigMap { field: "data" })?;
        if data.len() != 1 {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap { field: "data" });
        }
        let pem = data.get(WORKER_RELAY_CA_CONFIG_MAP_KEY).ok_or(
            ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "data.ca.crt",
            },
        )?;
        validate_worker_relay_ca_pem(pem)?;

        let uid = observed.metadata.uid.as_deref().ok_or(
            ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.uid",
            },
        )?;
        let resource_version = observed.metadata.resource_version.as_deref().ok_or(
            ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.resourceVersion",
            },
        )?;
        Self::new(name, uid, resource_version)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn resource_version(&self) -> &str {
        &self.resource_version
    }

    pub(crate) fn matches_intent(&self, name: &str) -> bool {
        self.name == name
    }

    pub(crate) fn validate_observed(
        &self,
        expected_namespace: &str,
        observed: &ConfigMap,
    ) -> Result<(), ResourceValidationError> {
        let observed_pin = Self::from_observed(expected_namespace, observed).map_err(|_| {
            ResourceValidationError::SpecMismatch {
                resource: "worker relay CA ConfigMap",
            }
        })?;
        for (field, matches) in [
            ("metadata.name", observed_pin.name == self.name),
            ("metadata.uid", observed_pin.uid == self.uid),
            (
                "metadata.resourceVersion",
                observed_pin.resource_version == self.resource_version,
            ),
        ] {
            if !matches {
                return Err(ResourceValidationError::MetadataMismatch {
                    resource: "worker relay CA ConfigMap",
                    field,
                });
            }
        }
        Ok(())
    }
}

impl fmt::Debug for PinnedWorkerRelayCaConfigMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedWorkerRelayCaConfigMap")
            .finish_non_exhaustive()
    }
}

fn has_session_management_labels(labels: Option<&BTreeMap<String, String>>) -> bool {
    labels.is_some_and(|labels| {
        labels.get(MANAGED_BY_LABEL).map(String::as_str) == Some(MANAGED_BY_VALUE)
            || labels.contains_key(RESOURCE_LABEL)
            || labels.contains_key(SESSION_LABEL)
            || labels.contains_key(GENERATION_LABEL)
    })
}

fn validate_worker_relay_ca_pem(pem: &str) -> Result<(), ResourceBuildError> {
    let bytes = pem.as_bytes();
    if bytes.len() > MAX_WORKER_RELAY_CA_PEM_BYTES {
        return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
            field: "data.ca.crt",
        });
    }
    validate_certificate_only_envelope(bytes)?;

    let mut roots = RootCertStore::empty();
    let mut certificate_count = 0usize;
    for item in rustls_pemfile::read_all(&mut Cursor::new(bytes)) {
        let item = item.map_err(|_| ResourceBuildError::InvalidWorkerRelayCaConfigMap {
            field: "data.ca.crt",
        })?;
        let Item::X509Certificate(certificate) = item else {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "data.ca.crt",
            });
        };
        roots
            .add(certificate)
            .map_err(|_| ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "data.ca.crt",
            })?;
        certificate_count += 1;
    }
    if certificate_count == 0 {
        return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
            field: "data.ca.crt",
        });
    }
    Ok(())
}

fn validate_certificate_only_envelope(bytes: &[u8]) -> Result<(), ResourceBuildError> {
    let invalid = || ResourceBuildError::InvalidWorkerRelayCaConfigMap {
        field: "data.ca.crt",
    };
    let mut inside_certificate = false;
    let mut body_line_seen = false;
    let mut certificate_count = 0usize;

    for raw_line in bytes.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if !inside_certificate {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            if line == CERTIFICATE_BEGIN {
                inside_certificate = true;
                body_line_seen = false;
                continue;
            }
            return Err(invalid());
        }

        if line == CERTIFICATE_END {
            if !body_line_seen {
                return Err(invalid());
            }
            inside_certificate = false;
            certificate_count += 1;
            continue;
        }
        if line.is_empty()
            || line.starts_with(b"-----")
            || !line
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
        {
            return Err(invalid());
        }
        body_line_seen = true;
    }

    if inside_certificate || certificate_count == 0 {
        return Err(invalid());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkerTransportProfile {
    relay_url: String,
    relay_ca: PinnedWorkerRelayCaConfigMap,
    image_pull_secrets: Vec<String>,
}

/// A narrow, immutable worker profile.
///
/// Images admitted to a profile must implement the `session-layout-v1`
/// contract. Every admitted image provides `/usr/bin/tini`; the generated Pod
/// owns the literal `tini --` prefix because Kubernetes `command` replaces the
/// image entrypoint. Profile `command` names an absolute supervisor executable
/// inside the pinned image; from the existing `/session` mount root it must
/// create and verify writable `/session/home` and `/session/workspace`, change
/// directory to the workspace, and only then exec the ACP worker. Keeping that
/// responsibility in the pinned image avoids a privileged or shell-dependent
/// init container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvpWorkerProfile {
    profile: ProfileRef,
    image: String,
    command: Vec<String>,
    args: Vec<String>,
    workspace: PersistentWorkspace,
    resources: WorkerResources,
    egress: Vec<TrustedEgressRule>,
    identity: RunAsIdentity,
    runtime_class: Option<RuntimeClassSelection>,
    skills: Option<PinnedSkillsConfigMap>,
    transport: Option<WorkerTransportProfile>,
}

impl MvpWorkerProfile {
    #[allow(clippy::too_many_arguments)]
    pub fn new<I, C, J, A, E>(
        profile: ProfileRef,
        image: impl Into<String>,
        command: I,
        args: J,
        workspace: PersistentWorkspace,
        resources: WorkerResources,
        egress: E,
        identity: RunAsIdentity,
        runtime_class: Option<RuntimeClassSelection>,
        skills: Option<PinnedSkillsConfigMap>,
    ) -> Result<Self, ResourceBuildError>
    where
        I: IntoIterator<Item = C>,
        C: Into<String>,
        J: IntoIterator<Item = A>,
        A: Into<String>,
        E: IntoIterator<Item = TrustedEgressRule>,
    {
        if !is_annotation_identifier(profile.version()) {
            return Err(ResourceBuildError::InvalidProfileVersion);
        }
        let image = image.into();
        if !is_pinned_image(&image) {
            return Err(ResourceBuildError::UnpinnedImage);
        }
        let command: Vec<String> = command.into_iter().map(Into::into).collect();
        if command.is_empty()
            || !command[0].starts_with('/')
            || command.iter().any(|value| !is_process_value(value))
        {
            return Err(ResourceBuildError::InvalidCommand);
        }
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        if args.iter().any(|value| !is_process_value(value)) {
            return Err(ResourceBuildError::InvalidArguments);
        }
        let egress: Vec<TrustedEgressRule> = egress.into_iter().collect();
        if egress.is_empty() {
            return Err(ResourceBuildError::EmptyEgressPolicy);
        }
        Ok(Self {
            profile,
            image,
            command,
            args,
            workspace,
            resources,
            egress,
            identity,
            runtime_class,
            skills,
            transport: None,
        })
    }

    /// The immutable profile revision selected by trusted controller
    /// configuration.
    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }

    pub(crate) fn with_cluster_references(
        mut self,
        runtime_class: Option<RuntimeClassSelection>,
        skills: Option<PinnedSkillsConfigMap>,
        relay_url: String,
        relay_ca: PinnedWorkerRelayCaConfigMap,
        image_pull_secrets: Vec<String>,
    ) -> Self {
        self.runtime_class = runtime_class;
        self.skills = skills;
        self.transport = Some(WorkerTransportProfile {
            relay_url,
            relay_ca,
            image_pull_secrets,
        });
        self
    }

    pub fn relay_ca_config_map(&self) -> Option<&PinnedWorkerRelayCaConfigMap> {
        self.transport.as_ref().map(|transport| &transport.relay_ca)
    }
}

#[derive(Clone)]
pub struct DesiredGeneration {
    context: GenerationContext,
    skills: Option<PinnedSkillsConfigMap>,
    runtime_class: Option<RuntimeClassSelection>,
    relay_ca: Option<PinnedWorkerRelayCaConfigMap>,
    persistent_volume_claim: PersistentVolumeClaim,
    registration_secret: Secret,
    service_account: ServiceAccount,
    pod: Pod,
    network_policy: NetworkPolicy,
}

impl fmt::Debug for DesiredGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DesiredGeneration {{ persistent_volume_claim: {:?}, registration_secret: <redacted>, service_account: {:?}, pod: {:?}, network_policy: {:?} }}",
            self.persistent_volume_claim.metadata.name,
            self.service_account.metadata.name,
            self.pod.metadata.name,
            self.network_policy.metadata.name
        )
    }
}

impl DesiredGeneration {
    pub fn build(
        context: GenerationContext,
        profile: MvpWorkerProfile,
        registration_token: [u8; 32],
    ) -> Result<Self, ResourceBuildError> {
        if profile.profile != context.profile {
            return Err(ResourceBuildError::ProfileMismatch);
        }
        let worker_transport = profile.transport.clone();
        let relay_ca = worker_transport
            .as_ref()
            .map(|transport| transport.relay_ca.clone());

        let generation = context.fence.generation();
        let pod_name = context
            .resource_names
            .pod(generation)
            .map_err(ResourceBuildError::ResourceName)?;
        let network_policy_name = format!("{pod_name}-net");
        let secret_name = context
            .resource_names
            .registration_secret(generation)
            .map_err(ResourceBuildError::ResourceName)?;
        let service_account_name = context
            .resource_names
            .service_account(generation)
            .map_err(ResourceBuildError::ResourceName)?;
        let binding = SessionBinding::new(
            context.scope_id,
            context.session_id,
            context.fence.clone(),
            context.incarnation_id,
        )
        .map_err(|_| ResourceBuildError::InvalidRegistrationBinding)?;
        let registration_binding = serde_json::to_vec(&WorkerRegistrationV1::new(&binding))
            .map_err(|_| ResourceBuildError::RegistrationBindingEncoding)?;
        if registration_binding.len() > MAX_REGISTRATION_BINDING_BYTES {
            return Err(ResourceBuildError::RegistrationBindingTooLarge);
        }

        let persistent_volume_claim = PersistentVolumeClaim {
            // The workspace survives worker replacement. Its identity is
            // therefore bound to the session incarnation, not one worker
            // generation or activation attempt.
            metadata: persistent_workspace_metadata(&context),
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec![profile
                    .workspace
                    .access_mode
                    .kubernetes_value()
                    .into()]),
                resources: Some(VolumeResourceRequirements {
                    limits: None,
                    requests: Some(BTreeMap::from([(
                        "storage".into(),
                        Quantity(profile.workspace.size.clone()),
                    )])),
                }),
                storage_class_name: Some(profile.workspace.storage_class.clone()),
                volume_mode: Some("Filesystem".into()),
                ..PersistentVolumeClaimSpec::default()
            }),
            status: None,
        };

        let registration_secret = Secret {
            data: Some(BTreeMap::from([
                (TOKEN_KEY.into(), ByteString(registration_token.to_vec())),
                (BINDING_KEY.into(), ByteString(registration_binding)),
            ])),
            immutable: Some(true),
            metadata: metadata(
                &context,
                secret_name.clone(),
                "registration-secret",
                profile.skills.as_ref(),
                profile.runtime_class.as_ref(),
                relay_ca.as_ref(),
            ),
            string_data: None,
            type_: Some("Opaque".into()),
        };

        let service_account = ServiceAccount {
            automount_service_account_token: Some(false),
            image_pull_secrets: None,
            metadata: metadata(
                &context,
                service_account_name.clone(),
                "worker-service-account",
                profile.skills.as_ref(),
                profile.runtime_class.as_ref(),
                relay_ca.as_ref(),
            ),
            secrets: None,
        };

        let mut volumes = vec![
            Volume {
                name: "session".into(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: context.resource_names.pvc(),
                    // `readOnly` is a non-pointer Go bool with `omitempty`.
                    // Absence is the API-canonical representation of writable.
                    read_only: None,
                }),
                ..Volume::default()
            },
            Volume {
                name: "registration".into(),
                secret: Some(SecretVolumeSource {
                    // The Linux kubelet Secret plugin applies the Pod fsGroup
                    // and ORs read-only files with 0440. Starting at 0400 keeps
                    // the mounted token readable by only the worker UID/GID.
                    default_mode: Some(0o400),
                    items: Some(vec![
                        KeyToPath {
                            key: TOKEN_KEY.into(),
                            mode: Some(0o400),
                            path: TOKEN_KEY.into(),
                        },
                        KeyToPath {
                            key: BINDING_KEY.into(),
                            mode: Some(0o400),
                            path: BINDING_KEY.into(),
                        },
                    ]),
                    optional: Some(false),
                    secret_name: Some(secret_name),
                }),
                ..Volume::default()
            },
            empty_dir_volume("tmp"),
            empty_dir_volume("var-tmp"),
            empty_dir_volume("openab-run"),
        ];
        let mut volume_mounts = vec![
            VolumeMount {
                mount_path: SESSION_ROOT.into(),
                name: "session".into(),
                // `readOnly: false` is omitted by the Kubernetes API.
                read_only: None,
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: TOKEN_DIRECTORY.into(),
                name: "registration".into(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            private_mount("tmp", "/tmp"),
            private_mount("var-tmp", "/var/tmp"),
            private_mount("openab-run", "/run/openab"),
        ];
        if let Some(skills) = profile.skills.as_ref() {
            volumes.push(Volume {
                name: "skills".into(),
                config_map: Some(ConfigMapVolumeSource {
                    default_mode: Some(0o444),
                    items: None,
                    name: skills.name.clone(),
                    optional: Some(false),
                }),
                ..Volume::default()
            });
            volume_mounts.push(VolumeMount {
                mount_path: "/opt/openab/skills".into(),
                name: "skills".into(),
                read_only: Some(true),
                ..VolumeMount::default()
            });
        }
        if let Some(transport) = worker_transport.as_ref() {
            volumes.push(Volume {
                name: WORKER_RELAY_CA_VOLUME.into(),
                config_map: Some(ConfigMapVolumeSource {
                    default_mode: Some(0o444),
                    items: Some(vec![KeyToPath {
                        key: WORKER_RELAY_CA_CONFIG_MAP_KEY.into(),
                        mode: None,
                        path: WORKER_RELAY_CA_CONFIG_MAP_KEY.into(),
                    }]),
                    name: transport.relay_ca.name.clone(),
                    optional: Some(false),
                }),
                ..Volume::default()
            });
            volume_mounts.push(VolumeMount {
                mount_path: WORKER_RELAY_CA_DIRECTORY.into(),
                name: WORKER_RELAY_CA_VOLUME.into(),
                read_only: Some(true),
                ..VolumeMount::default()
            });
        }

        let image_pull_secrets = worker_transport.as_ref().and_then(|transport| {
            (!transport.image_pull_secrets.is_empty()).then(|| {
                transport
                    .image_pull_secrets
                    .iter()
                    .map(|name| LocalObjectReference { name: name.clone() })
                    .collect()
            })
        });

        let identity = profile.identity;
        let mut supervisor_argv = profile.command;
        supervisor_argv.extend(profile.args);
        let pod = Pod {
            metadata: metadata(
                &context,
                pod_name,
                "worker-pod",
                profile.skills.as_ref(),
                profile.runtime_class.as_ref(),
                relay_ca.as_ref(),
            ),
            spec: Some(PodSpec {
                automount_service_account_token: Some(false),
                containers: vec![Container {
                    args: Some(supervisor_argv),
                    // Kubernetes `command` replaces an image ENTRYPOINT. Pin
                    // the init wrapper here as part of the desired PodSpec so
                    // worker Pods retain PID 1 orphan reaping even when the
                    // selected profile supplies a custom supervisor argv.
                    command: Some(vec![WORKER_INIT_EXECUTABLE.into(), "--".into()]),
                    env: Some(worker_environment(worker_transport.as_ref())),
                    image: Some(profile.image),
                    image_pull_policy: Some("IfNotPresent".into()),
                    name: "worker".into(),
                    resources: Some(profile.resources.kubernetes()),
                    security_context: Some(SecurityContext {
                        allow_privilege_escalation: Some(false),
                        capabilities: Some(Capabilities {
                            add: None,
                            drop: Some(vec!["ALL".into()]),
                        }),
                        privileged: Some(false),
                        read_only_root_filesystem: Some(true),
                        run_as_group: Some(identity.gid),
                        run_as_non_root: Some(true),
                        run_as_user: Some(identity.uid),
                        seccomp_profile: Some(runtime_default_seccomp()),
                        ..SecurityContext::default()
                    }),
                    termination_message_path: Some("/dev/termination-log".into()),
                    termination_message_policy: Some("File".into()),
                    volume_mounts: Some(volume_mounts),
                    working_dir: Some(SESSION_ROOT.into()),
                    ..Container::default()
                }],
                dns_policy: Some("ClusterFirst".into()),
                enable_service_links: Some(false),
                host_ipc: None,
                host_network: None,
                host_pid: None,
                image_pull_secrets,
                node_selector: Some(BTreeMap::from([(
                    "kubernetes.io/os".into(),
                    "linux".into(),
                )])),
                os: Some(PodOS {
                    name: "linux".into(),
                }),
                preemption_policy: Some("PreemptLowerPriority".into()),
                restart_policy: Some("Never".into()),
                runtime_class_name: profile
                    .runtime_class
                    .as_ref()
                    .map(|runtime| runtime.name.clone()),
                scheduler_name: Some("default-scheduler".into()),
                security_context: Some(PodSecurityContext {
                    fs_group: Some(identity.gid),
                    run_as_group: Some(identity.gid),
                    run_as_non_root: Some(true),
                    run_as_user: Some(identity.uid),
                    seccomp_profile: Some(runtime_default_seccomp()),
                    ..PodSecurityContext::default()
                }),
                // New Kubernetes releases mirror serviceAccountName into the
                // deprecated serviceAccount alias during defaulting. Supplying
                // both makes read-back stable across supported API servers.
                service_account: Some(service_account_name.clone()),
                service_account_name: Some(service_account_name),
                share_process_namespace: Some(false),
                termination_grace_period_seconds: Some(30),
                tolerations: Some(standard_unavailable_tolerations()),
                volumes: Some(volumes),
                ..PodSpec::default()
            }),
            status: None,
        };

        let network_policy = NetworkPolicy {
            metadata: metadata(
                &context,
                network_policy_name,
                "worker-network-policy",
                profile.skills.as_ref(),
                profile.runtime_class.as_ref(),
                relay_ca.as_ref(),
            ),
            spec: Some(NetworkPolicySpec {
                egress: Some(
                    profile
                        .egress
                        .iter()
                        .map(TrustedEgressRule::kubernetes)
                        .collect(),
                ),
                // policyTypes includes Ingress, so an absent rule list is
                // canonical deny-all ingress after API JSON round-tripping.
                ingress: None,
                pod_selector: Some(exact_selector(worker_selector_labels(&context))),
                policy_types: Some(vec!["Ingress".into(), "Egress".into()]),
            }),
        };

        Ok(Self {
            context,
            skills: profile.skills,
            runtime_class: profile.runtime_class,
            relay_ca,
            persistent_volume_claim,
            registration_secret,
            service_account,
            pod,
            network_policy,
        })
    }

    pub fn context(&self) -> &GenerationContext {
        &self.context
    }

    pub fn persistent_volume_claim(&self) -> &PersistentVolumeClaim {
        &self.persistent_volume_claim
    }

    pub fn registration_secret(&self) -> &Secret {
        &self.registration_secret
    }

    pub fn service_account(&self) -> &ServiceAccount {
        &self.service_account
    }

    pub fn pod(&self) -> &Pod {
        &self.pod
    }

    /// The driver must create and observe this policy before creating the Pod.
    /// MVP admission also requires an enforcing NetworkPolicy CNI and a
    /// dedicated namespace without additive policies that broaden worker
    /// traffic.
    pub fn network_policy(&self) -> &NetworkPolicy {
        &self.network_policy
    }

    pub fn skills_config_map_name(&self) -> Option<&str> {
        self.skills.as_ref().map(|skills| skills.name.as_str())
    }

    pub fn runtime_class_name(&self) -> Option<&str> {
        self.runtime_class
            .as_ref()
            .map(|runtime_class| runtime_class.name.as_str())
    }

    pub fn relay_ca_config_map(&self) -> Option<&PinnedWorkerRelayCaConfigMap> {
        self.relay_ca.as_ref()
    }

    pub fn validate_persistent_volume_claim(
        &self,
        observed: &PersistentVolumeClaim,
    ) -> Result<(), ResourceValidationError> {
        let observed_metadata =
            normalized_pvc_metadata(&self.persistent_volume_claim.metadata, &observed.metadata)?;
        validate_metadata(
            "PersistentVolumeClaim",
            &self.persistent_volume_claim.metadata,
            &observed_metadata,
        )?;
        let Some(mut observed_spec) = observed.spec.clone() else {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "PersistentVolumeClaim",
            });
        };
        if let Some(volume_name) = observed_spec.volume_name.as_deref() {
            if !is_dns_subdomain(volume_name) {
                return Err(ResourceValidationError::SpecMismatch {
                    resource: "PersistentVolumeClaim",
                });
            }
            observed_spec.volume_name = None;
        }
        if Some(observed_spec) != self.persistent_volume_claim.spec {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "PersistentVolumeClaim",
            });
        }
        Ok(())
    }

    pub fn validate_registration_secret(
        &self,
        observed: &Secret,
    ) -> Result<(), ResourceValidationError> {
        validate_metadata(
            "Secret",
            &self.registration_secret.metadata,
            &observed.metadata,
        )?;
        if observed.data != self.registration_secret.data
            || observed.string_data != self.registration_secret.string_data
            || observed.immutable != self.registration_secret.immutable
            || observed.type_ != self.registration_secret.type_
        {
            return Err(ResourceValidationError::SpecMismatch { resource: "Secret" });
        }
        Ok(())
    }

    pub fn validate_service_account(
        &self,
        observed: &ServiceAccount,
    ) -> Result<(), ResourceValidationError> {
        validate_metadata(
            "ServiceAccount",
            &self.service_account.metadata,
            &observed.metadata,
        )?;
        if observed.automount_service_account_token
            != self.service_account.automount_service_account_token
            || observed.image_pull_secrets != self.service_account.image_pull_secrets
            || observed.secrets != self.service_account.secrets
        {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "ServiceAccount",
            });
        }
        Ok(())
    }

    pub fn validate_pod(&self, observed: &Pod) -> Result<(), ResourceValidationError> {
        validate_metadata("Pod", &self.pod.metadata, &observed.metadata)?;
        let Some(mut observed_spec) = observed.spec.clone() else {
            return Err(ResourceValidationError::SpecMismatch { resource: "Pod" });
        };
        normalize_api_server_pod_spec(&mut observed_spec);
        if let Some(node_name) = observed_spec.node_name.as_deref() {
            if !is_dns_subdomain(node_name) {
                return Err(ResourceValidationError::SpecMismatch { resource: "Pod" });
            }
            observed_spec.node_name = None;
        }
        if observed_spec.priority == Some(0) {
            observed_spec.priority = None;
        }
        if Some(observed_spec) != self.pod.spec {
            return Err(ResourceValidationError::SpecMismatch { resource: "Pod" });
        }
        Ok(())
    }

    pub fn validate_network_policy(
        &self,
        observed: &NetworkPolicy,
    ) -> Result<(), ResourceValidationError> {
        validate_metadata(
            "NetworkPolicy",
            &self.network_policy.metadata,
            &observed.metadata,
        )?;
        let mut observed_spec = observed.spec.clone();
        if let Some(spec) = observed_spec.as_mut() {
            if spec.ingress.as_ref().is_some_and(Vec::is_empty) {
                spec.ingress = None;
            }
        }
        if observed_spec != self.network_policy.spec {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "NetworkPolicy",
            });
        }
        Ok(())
    }

    pub fn validate_runtime_class(
        &self,
        observed: &RuntimeClass,
    ) -> Result<(), ResourceValidationError> {
        let expected = self
            .runtime_class
            .as_ref()
            .ok_or(ResourceValidationError::RuntimeClassNotConfigured)?;
        for (field, matches) in [
            (
                "metadata.name",
                observed.metadata.name.as_deref() == Some(expected.name.as_str()),
            ),
            (
                "metadata.uid",
                observed.metadata.uid.as_deref() == Some(expected.uid.as_str()),
            ),
            (
                "metadata.resourceVersion",
                observed.metadata.resource_version.as_deref()
                    == Some(expected.resource_version.as_str()),
            ),
            ("metadata.namespace", observed.metadata.namespace.is_none()),
            (
                "metadata.deletionTimestamp",
                observed.metadata.deletion_timestamp.is_none(),
            ),
        ] {
            if !matches {
                return Err(ResourceValidationError::MetadataMismatch {
                    resource: "RuntimeClass",
                    field,
                });
            }
        }
        if observed.handler != expected.handler
            || observed.overhead.is_some()
            || observed.scheduling.is_some()
        {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "RuntimeClass",
            });
        }
        Ok(())
    }

    pub fn validate_skills_config_map(
        &self,
        observed: &ConfigMap,
    ) -> Result<(), ResourceValidationError> {
        let expected = self
            .skills
            .as_ref()
            .ok_or(ResourceValidationError::SkillsConfigMapNotConfigured)?;
        if observed.metadata.name.as_deref() != Some(expected.name.as_str()) {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "skills ConfigMap",
                field: "metadata.name",
            });
        }
        if observed.metadata.namespace.as_deref() != Some(self.context.namespace.as_str()) {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "skills ConfigMap",
                field: "metadata.namespace",
            });
        }
        if observed.metadata.uid.as_deref() != Some(expected.uid.as_str()) {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "skills ConfigMap",
                field: "metadata.uid",
            });
        }
        if observed.metadata.resource_version.as_deref() != Some(expected.resource_version.as_str())
        {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "skills ConfigMap",
                field: "metadata.resourceVersion",
            });
        }
        if observed.metadata.deletion_timestamp.is_some() {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "skills ConfigMap",
                field: "metadata.deletionTimestamp",
            });
        }
        if observed.immutable != Some(true) {
            return Err(ResourceValidationError::SpecMismatch {
                resource: "skills ConfigMap",
            });
        }
        Ok(())
    }

    pub fn validate_worker_relay_ca_config_map(
        &self,
        observed: &ConfigMap,
    ) -> Result<(), ResourceValidationError> {
        self.relay_ca
            .as_ref()
            .ok_or(ResourceValidationError::WorkerRelayCaConfigMapNotConfigured)?
            .validate_observed(self.context.namespace(), observed)
    }
}

fn metadata(
    context: &GenerationContext,
    name: String,
    resource: &'static str,
    skills: Option<&PinnedSkillsConfigMap>,
    runtime_class: Option<&RuntimeClassSelection>,
    relay_ca: Option<&PinnedWorkerRelayCaConfigMap>,
) -> ObjectMeta {
    let mut annotations = BTreeMap::from([
        (SCOPE_ANNOTATION.into(), context.scope_id.as_hex()),
        (SESSION_ANNOTATION.into(), context.session_id.as_hex()),
        (
            GENERATION_ANNOTATION.into(),
            context.fence.generation().to_string(),
        ),
        (
            ATTEMPT_ANNOTATION.into(),
            context.fence.attempt_id().to_string(),
        ),
        (
            INCARNATION_ANNOTATION.into(),
            context.incarnation_id.to_string(),
        ),
        (
            PROFILE_NAME_ANNOTATION.into(),
            context.profile.name().to_string(),
        ),
        (
            PROFILE_VERSION_ANNOTATION.into(),
            context.profile.version().to_string(),
        ),
        (ANCHOR_NAME_ANNOTATION.into(), context.anchor_name.clone()),
        (ANCHOR_UID_ANNOTATION.into(), context.anchor_uid.clone()),
        (
            WORKER_IMAGE_CONTRACT_ANNOTATION.into(),
            WORKER_IMAGE_CONTRACT.into(),
        ),
    ]);
    if let Some(skills) = skills {
        annotations.insert(SKILLS_NAME_ANNOTATION.into(), skills.name.clone());
        annotations.insert(SKILLS_UID_ANNOTATION.into(), skills.uid.clone());
        annotations.insert(
            SKILLS_RESOURCE_VERSION_ANNOTATION.into(),
            skills.resource_version.clone(),
        );
    }
    if let Some(runtime_class) = runtime_class {
        annotations.insert(RUNTIME_NAME_ANNOTATION.into(), runtime_class.name.clone());
        annotations.insert(
            RUNTIME_HANDLER_ANNOTATION.into(),
            runtime_class.handler.clone(),
        );
        annotations.insert(RUNTIME_UID_ANNOTATION.into(), runtime_class.uid.clone());
        annotations.insert(
            RUNTIME_RESOURCE_VERSION_ANNOTATION.into(),
            runtime_class.resource_version.clone(),
        );
    }
    if let Some(relay_ca) = relay_ca {
        annotations.insert(
            WORKER_RELAY_CA_NAME_ANNOTATION.into(),
            relay_ca.name.clone(),
        );
        annotations.insert(WORKER_RELAY_CA_UID_ANNOTATION.into(), relay_ca.uid.clone());
        annotations.insert(
            WORKER_RELAY_CA_RESOURCE_VERSION_ANNOTATION.into(),
            relay_ca.resource_version.clone(),
        );
    }
    ObjectMeta {
        annotations: Some(annotations),
        labels: Some(BTreeMap::from([
            (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
            (RESOURCE_LABEL.into(), resource.into()),
            (
                SESSION_LABEL.into(),
                context.session_id.as_hex()[..40].to_string(),
            ),
            (
                GENERATION_LABEL.into(),
                context.fence.generation().to_string(),
            ),
        ])),
        name: Some(name),
        namespace: Some(context.namespace.clone()),
        owner_references: Some(vec![OwnerReference {
            api_version: "v1".into(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: "ConfigMap".into(),
            name: context.anchor_name.clone(),
            uid: context.anchor_uid.clone(),
        }]),
        ..ObjectMeta::default()
    }
}

fn persistent_workspace_metadata(context: &GenerationContext) -> ObjectMeta {
    ObjectMeta {
        annotations: Some(BTreeMap::from([
            (SCOPE_ANNOTATION.into(), context.scope_id.as_hex()),
            (SESSION_ANNOTATION.into(), context.session_id.as_hex()),
            (
                INCARNATION_ANNOTATION.into(),
                context.incarnation_id.to_string(),
            ),
            (
                PROFILE_NAME_ANNOTATION.into(),
                context.profile.name().to_string(),
            ),
            (
                PROFILE_VERSION_ANNOTATION.into(),
                context.profile.version().to_string(),
            ),
            (ANCHOR_NAME_ANNOTATION.into(), context.anchor_name.clone()),
            (ANCHOR_UID_ANNOTATION.into(), context.anchor_uid.clone()),
        ])),
        labels: Some(BTreeMap::from([
            (MANAGED_BY_LABEL.into(), MANAGED_BY_VALUE.into()),
            (RESOURCE_LABEL.into(), "workspace-pvc".into()),
            (
                SESSION_LABEL.into(),
                context.session_id.as_hex()[..40].to_string(),
            ),
        ])),
        name: Some(context.resource_names.pvc()),
        namespace: Some(context.namespace.clone()),
        owner_references: Some(vec![OwnerReference {
            api_version: "v1".into(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: "ConfigMap".into(),
            name: context.anchor_name.clone(),
            uid: context.anchor_uid.clone(),
        }]),
        ..ObjectMeta::default()
    }
}

fn validate_metadata(
    resource: &'static str,
    expected: &ObjectMeta,
    observed: &ObjectMeta,
) -> Result<(), ResourceValidationError> {
    for (field, matches) in [
        ("metadata.name", observed.name == expected.name),
        (
            "metadata.namespace",
            observed.namespace == expected.namespace,
        ),
        (
            "metadata.ownerReferences",
            observed.owner_references == expected.owner_references,
        ),
        (
            "metadata.finalizers",
            observed.finalizers == expected.finalizers,
        ),
    ] {
        if !matches {
            return Err(ResourceValidationError::MetadataMismatch { resource, field });
        }
    }
    if observed.deletion_timestamp.is_some() {
        return Err(ResourceValidationError::MetadataMismatch {
            resource,
            field: "metadata.deletionTimestamp",
        });
    }
    if !observed.uid.as_deref().is_some_and(is_printable_identifier) {
        return Err(ResourceValidationError::MetadataMismatch {
            resource,
            field: "metadata.uid",
        });
    }
    if !observed
        .resource_version
        .as_deref()
        .is_some_and(is_printable_identifier)
    {
        return Err(ResourceValidationError::MetadataMismatch {
            resource,
            field: "metadata.resourceVersion",
        });
    }
    validate_metadata_map(
        resource,
        "metadata.labels",
        &expected.labels,
        &observed.labels,
    )?;
    validate_metadata_map(
        resource,
        "metadata.annotations",
        &expected.annotations,
        &observed.annotations,
    )
}

fn normalized_pvc_metadata(
    expected: &ObjectMeta,
    observed: &ObjectMeta,
) -> Result<ObjectMeta, ResourceValidationError> {
    let mut normalized = observed.clone();
    if let Some(finalizers) = normalized.finalizers.as_ref() {
        if finalizers.as_slice() != ["kubernetes.io/pvc-protection"] {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "PersistentVolumeClaim",
                field: "metadata.finalizers",
            });
        }
        normalized.finalizers = None;
    }

    let expected_annotations = expected
        .annotations
        .as_ref()
        .expect("desired PVC binding annotations");
    let annotations =
        normalized
            .annotations
            .as_mut()
            .ok_or(ResourceValidationError::MetadataMismatch {
                resource: "PersistentVolumeClaim",
                field: "metadata.annotations",
            })?;
    let controller_keys: Vec<String> = annotations
        .iter()
        .filter(|(key, _)| !expected_annotations.contains_key(*key))
        .map(|(key, _)| key.clone())
        .collect();
    for key in controller_keys {
        let value = annotations.get(&key).expect("collected annotation key");
        let valid = match key.as_str() {
            "pv.kubernetes.io/bind-completed" | "pv.kubernetes.io/bound-by-controller" => {
                value == "yes"
            }
            "volume.kubernetes.io/selected-node" => is_dns_subdomain(value),
            "volume.kubernetes.io/storage-provisioner"
            | "volume.beta.kubernetes.io/storage-provisioner"
            | "volume.kubernetes.io/storage-resizer" => is_dns_subdomain(value),
            _ => false,
        };
        if !valid {
            return Err(ResourceValidationError::MetadataMismatch {
                resource: "PersistentVolumeClaim",
                field: "metadata.annotations",
            });
        }
        annotations.remove(&key);
    }
    Ok(normalized)
}

fn validate_metadata_map(
    resource: &'static str,
    field: &'static str,
    expected: &Option<BTreeMap<String, String>>,
    observed: &Option<BTreeMap<String, String>>,
) -> Result<(), ResourceValidationError> {
    let expected = expected.as_ref().expect("desired binding metadata");
    let Some(observed) = observed.as_ref() else {
        return Err(ResourceValidationError::MetadataMismatch { resource, field });
    };
    if observed != expected {
        return Err(ResourceValidationError::MetadataMismatch { resource, field });
    }
    Ok(())
}

fn empty_dir_volume(name: &str) -> Volume {
    Volume {
        empty_dir: Some(EmptyDirVolumeSource::default()),
        name: name.into(),
        ..Volume::default()
    }
}

fn private_mount(name: &str, path: &str) -> VolumeMount {
    VolumeMount {
        mount_path: path.into(),
        name: name.into(),
        // `readOnly` is a non-pointer Go bool with `omitempty`.
        read_only: None,
        ..VolumeMount::default()
    }
}

fn normalize_api_server_pod_spec(spec: &mut PodSpec) {
    for value in [
        &mut spec.host_ipc,
        &mut spec.host_network,
        &mut spec.host_pid,
    ] {
        if *value == Some(false) {
            *value = None;
        }
    }
    if let Some(volumes) = spec.volumes.as_mut() {
        for volume in volumes {
            if let Some(claim) = volume.persistent_volume_claim.as_mut() {
                if claim.read_only == Some(false) {
                    claim.read_only = None;
                }
            }
        }
    }
    for container in &mut spec.containers {
        if let Some(mounts) = container.volume_mounts.as_mut() {
            for mount in mounts {
                if mount.read_only == Some(false) {
                    mount.read_only = None;
                }
            }
        }
        if container
            .resize_policy
            .as_deref()
            .is_some_and(is_default_resize_policy)
        {
            container.resize_policy = None;
        }
    }
}

fn is_default_resize_policy(policies: &[ContainerResizePolicy]) -> bool {
    if policies.is_empty() {
        return true;
    }
    if policies.len() != 2 {
        return false;
    }
    let mut cpu = false;
    let mut memory = false;
    for policy in policies {
        if policy.restart_policy != "NotRequired" {
            return false;
        }
        match policy.resource_name.as_str() {
            "cpu" if !cpu => cpu = true,
            "memory" if !memory => memory = true,
            _ => return false,
        }
    }
    cpu && memory
}

fn worker_environment(transport: Option<&WorkerTransportProfile>) -> Vec<EnvVar> {
    if let Some(transport) = transport {
        vec![
            literal_env("OPENAB_SESSION_CONTROLLER_URL", &transport.relay_url),
            literal_env("OPENAB_SESSION_CONTROLLER_CA_FILE", WORKER_RELAY_CA_FILE),
            literal_env("OPENAB_REGISTRATION_TOKEN_FILE", TOKEN_FILE),
            literal_env("OPENAB_REGISTRATION_BINDING_FILE", BINDING_FILE),
            worker_pod_uid_env(),
            literal_env("OPENAB_SESSION_ROOT", SESSION_ROOT),
            literal_env("OPENAB_WORKSPACE", SESSION_WORKSPACE_V1),
            literal_env("HOME", SESSION_HOME),
        ]
    } else {
        vec![
            literal_env("HOME", SESSION_HOME),
            literal_env("OPENAB_WORKSPACE", SESSION_WORKSPACE_V1),
            literal_env("OPENAB_SESSION_ROOT", SESSION_ROOT),
            literal_env("OPENAB_REGISTRATION_TOKEN_FILE", TOKEN_FILE),
            literal_env("OPENAB_REGISTRATION_BINDING_FILE", BINDING_FILE),
            worker_pod_uid_env(),
        ]
    }
}

fn worker_pod_uid_env() -> EnvVar {
    EnvVar {
        name: "OPENAB_WORKER_POD_UID".into(),
        value: None,
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".into()),
                field_path: "metadata.uid".into(),
            }),
            ..EnvVarSource::default()
        }),
    }
}

fn literal_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        value_from: None,
    }
}

fn runtime_default_seccomp() -> SeccompProfile {
    SeccompProfile {
        localhost_profile: None,
        type_: "RuntimeDefault".into(),
    }
}

fn standard_unavailable_tolerations() -> Vec<Toleration> {
    [
        "node.kubernetes.io/not-ready",
        "node.kubernetes.io/unreachable",
    ]
    .into_iter()
    .map(|key| Toleration {
        effect: Some("NoExecute".into()),
        key: Some(key.into()),
        operator: Some("Exists".into()),
        toleration_seconds: Some(300),
        value: None,
    })
    .collect()
}

fn worker_selector_labels(context: &GenerationContext) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            SESSION_LABEL.into(),
            context.session_id.as_hex()[..40].to_string(),
        ),
        (
            GENERATION_LABEL.into(),
            context.fence.generation().to_string(),
        ),
        (RESOURCE_LABEL.into(), "worker-pod".into()),
    ])
}

fn exact_selector(match_labels: BTreeMap<String, String>) -> LabelSelector {
    LabelSelector {
        match_expressions: None,
        match_labels: Some(match_labels),
    }
}

fn valid_label_map(labels: &BTreeMap<String, String>) -> bool {
    labels
        .iter()
        .all(|(key, value)| is_label_key(key) && is_label_value(value))
}

fn is_label_key(value: &str) -> bool {
    let mut parts = value.split('/');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return false;
    }
    match second {
        Some(name) => is_dns_subdomain(first) && is_label_name(name),
        None => is_label_name(first),
    }
}

fn is_label_value(value: &str) -> bool {
    value.is_empty() || is_label_name(value)
}

fn is_label_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_scoped_canonical_cidr(value: &str) -> bool {
    let Some((address, prefix)) = value.split_once('/') else {
        return false;
    };
    let Ok(address) = address.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    if prefix == 0 {
        return false;
    }
    match address {
        IpAddr::V4(address) if prefix <= 32 => {
            let address = u32::from(address);
            let mask = u32::MAX << (32 - prefix);
            address & mask == address
        }
        IpAddr::V6(address) if prefix <= 128 => {
            let address = u128::from(address);
            let mask = u128::MAX << (128 - prefix);
            address & mask == address
        }
        _ => false,
    }
}

fn validate_cpu_quantity(value: &str, field: &'static str) -> Result<u128, ResourceBuildError> {
    const MAX_MILLI_CPU: u128 = (i64::MAX as u128) * 1000;
    let milli_cpu = if let Some(value) = value.strip_suffix('m') {
        parse_canonical_positive_integer(value).filter(|coefficient| coefficient % 1000 != 0)
    } else {
        parse_canonical_positive_integer(value)
            .filter(|coefficient| *coefficient <= i64::MAX as u128)
            .filter(|coefficient| coefficient % 1000 != 0)
            .and_then(|coefficient| coefficient.checked_mul(1000))
    };
    if !(1..=64).contains(&value.len())
        || milli_cpu.is_none_or(|milli_cpu| milli_cpu > MAX_MILLI_CPU)
    {
        return Err(ResourceBuildError::InvalidQuantity { field });
    }
    Ok(milli_cpu.expect("validated CPU quantity"))
}

fn validate_binary_quantity(value: &str, field: &'static str) -> Result<u128, ResourceBuildError> {
    const SUFFIXES: [(&str, u32); 6] = [
        ("Ki", 10),
        ("Mi", 20),
        ("Gi", 30),
        ("Ti", 40),
        ("Pi", 50),
        ("Ei", 60),
    ];
    let parsed = SUFFIXES.iter().find_map(|(suffix, shift)| {
        value
            .strip_suffix(suffix)
            .and_then(parse_canonical_positive_integer)
            .map(|coefficient| (coefficient, *shift, *suffix))
    });
    let bytes = parsed.and_then(|(coefficient, shift, suffix)| {
        if suffix != "Ei" && coefficient % 1024 == 0 {
            return None;
        }
        coefficient
            .checked_mul(1_u128 << shift)
            .filter(|bytes| *bytes <= i64::MAX as u128)
    });
    if !(1..=64).contains(&value.len()) || bytes.is_none() {
        return Err(ResourceBuildError::InvalidQuantity { field });
    }
    Ok(bytes.expect("validated binary quantity"))
}

fn parse_canonical_positive_integer(value: &str) -> Option<u128> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse::<u128>().ok().filter(|value| *value > 0)
}

fn is_process_value(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}

fn is_pinned_image(value: &str) -> bool {
    let Some((repository, digest)) = value.split_once("@sha256:") else {
        return false;
    };
    !repository.is_empty()
        && !repository.contains('@')
        && repository.bytes().all(|byte| byte.is_ascii_graphic())
        && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_printable_identifier(value: &str) -> bool {
    (1..=256).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/' && byte != b'\\')
}

fn is_annotation_identifier(value: &str) -> bool {
    (1..=256).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn is_dns_label(value: &str) -> bool {
    is_dns_component(value, 63)
}

fn is_dns_subdomain(value: &str) -> bool {
    (1..=253).contains(&value.len())
        && value
            .split('.')
            .all(|component| is_dns_component(component, 63))
}

fn is_dns_component(value: &str, maximum: usize) -> bool {
    let bytes = value.as_bytes();
    (1..=maximum).contains(&bytes.len())
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
