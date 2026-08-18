use crate::resources::{
    AllowedRuntimeClass, EgressPort, EgressProtocol, MvpWorkerProfile, PersistentWorkspace,
    PinnedSkillsConfigMap, PinnedWorkerRelayCaConfigMap, PvcAccessMode, ResourceBuildError,
    RunAsIdentity, RuntimeClassSelection, TrustedEgressRule, WorkerResources,
};
use crate::state::{validate_profile_name, ProfileRef, StateError};
use http::Uri;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::node::v1::RuntimeClass;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::time::Duration;
use thiserror::Error;

/// Parser safety ceiling, not a default retention policy.
pub const MAX_LIFECYCLE_TTL_SECONDS: u64 = 365 * 24 * 60 * 60;

/// Parser safety ceiling, not a default namespace quota.
pub const MAX_ACTIVE_WORKERS: usize = 10_000;

/// Maximum accepted UTF-8 bytes in one trusted worker-profile TOML document.
pub const MAX_PROFILE_CONFIG_BYTES: usize = 1024 * 1024;

/// Maximum accepted UTF-8 bytes in a worker relay URL.
pub const MAX_WORKER_RELAY_URL_BYTES: usize = 2_048;

/// Maximum image-pull Secret references in one immutable profile revision.
pub const MAX_IMAGE_PULL_SECRETS: usize = 16;

const WORKER_RELAY_PATH: &str = "/v1/worker";

#[derive(Debug, Error)]
pub enum ProfileConfigError {
    #[error("worker configuration could not be read")]
    Read,
    #[error("worker configuration exceeds its size limit")]
    TooLarge,
    #[error("worker configuration is not valid UTF-8")]
    InvalidUtf8,
    #[error("worker configuration is not valid TOML")]
    Decode,
    #[error("unsupported worker configuration schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("worker configuration must define at least one profile")]
    EmptyProfiles,
    #[error("worker profile {profile} must define at least one revision")]
    EmptyProfileRevisions { profile: String },
    #[error("worker profile {profile} does not contain its selected current revision")]
    CurrentProfileRevisionUnavailable { profile: String },
    #[error("invalid controller policy field {field}: {reason}")]
    InvalidPolicy {
        field: &'static str,
        reason: &'static str,
    },
    #[error("invalid worker profile identity: {source}")]
    InvalidProfileIdentity {
        #[source]
        source: StateError,
    },
    #[error("invalid worker profile {profile}: {source}")]
    InvalidProfile {
        profile: String,
        #[source]
        source: ResourceBuildError,
    },
    #[error("resolved {reference} does not match worker profile {profile}")]
    ClusterReferenceMismatch {
        profile: String,
        reference: &'static str,
    },
    #[error("worker relay URL must be an absolute wss URL for /v1/worker")]
    InvalidWorkerRelayUrl,
    #[error("worker relay CA ConfigMap name must be a lowercase Kubernetes DNS subdomain")]
    InvalidWorkerRelayCaConfigMapName,
    #[error("image-pull Secret name must be a lowercase Kubernetes DNS subdomain")]
    InvalidImagePullSecretName,
    #[error("worker profile references too many image-pull Secrets")]
    TooManyImagePullSecrets,
    #[error("worker profile contains a duplicate image-pull Secret reference")]
    DuplicateImagePullSecret,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerPolicy {
    compute_idle_ttl: Duration,
    storage_retention_ttl: Duration,
    max_active_workers: usize,
}

impl ControllerPolicy {
    /// Construct a validated controller policy for programmatic embedders.
    pub fn new(
        compute_idle_seconds: u64,
        storage_retention_seconds: u64,
        max_active_workers: usize,
    ) -> Result<Self, ProfileConfigError> {
        validate_bounded_nonzero_ttl("compute_idle_seconds", compute_idle_seconds)?;
        validate_bounded_nonzero_ttl("storage_retention_seconds", storage_retention_seconds)?;
        if storage_retention_seconds < compute_idle_seconds {
            return Err(ProfileConfigError::InvalidPolicy {
                field: "storage_retention_seconds",
                reason: "must be greater than or equal to compute_idle_seconds",
            });
        }
        if !(1..=MAX_ACTIVE_WORKERS).contains(&max_active_workers) {
            return Err(ProfileConfigError::InvalidPolicy {
                field: "max_active_workers",
                reason: "must be nonzero and within the implementation limit",
            });
        }
        Ok(Self {
            compute_idle_ttl: Duration::from_secs(compute_idle_seconds),
            storage_retention_ttl: Duration::from_secs(storage_retention_seconds),
            max_active_workers,
        })
    }

    pub fn compute_idle_ttl(&self) -> Duration {
        self.compute_idle_ttl
    }

    pub fn storage_retention_ttl(&self) -> Duration {
        self.storage_retention_ttl
    }

    /// Maximum active workers across all profiles in this controller scope.
    pub fn max_active_workers(&self) -> usize {
        self.max_active_workers
    }
}

fn validate_bounded_nonzero_ttl(
    field: &'static str,
    seconds: u64,
) -> Result<(), ProfileConfigError> {
    if !(1..=MAX_LIFECYCLE_TTL_SECONDS).contains(&seconds) {
        return Err(ProfileConfigError::InvalidPolicy {
            field,
            reason: "must be nonzero and within the implementation limit",
        });
    }
    Ok(())
}

/// An operator-owned, exact TLS relay endpoint for worker registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRelayUrl(String);

impl WorkerRelayUrl {
    fn new(value: String) -> Result<Self, ProfileConfigError> {
        if value.len() > MAX_WORKER_RELAY_URL_BYTES {
            return Err(ProfileConfigError::InvalidWorkerRelayUrl);
        }
        let uri = value
            .parse::<Uri>()
            .map_err(|_| ProfileConfigError::InvalidWorkerRelayUrl)?;
        let authority = uri
            .authority()
            .ok_or(ProfileConfigError::InvalidWorkerRelayUrl)?;
        let explicit_port = authority
            .as_str()
            .strip_prefix(authority.host())
            .ok_or(ProfileConfigError::InvalidWorkerRelayUrl)?;
        let port_is_valid = explicit_port.is_empty()
            || (explicit_port.starts_with(':') && authority.port_u16().is_some());
        if uri.scheme_str() != Some("wss")
            || authority.host().is_empty()
            || authority.as_str().contains('@')
            || !port_is_valid
            || value.contains('#')
            || uri.query().is_some()
            || uri.path() != WORKER_RELAY_PATH
        {
            return Err(ProfileConfigError::InvalidWorkerRelayUrl);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An operator-versioned CA ConfigMap name that must never be reused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRelayCaConfigMapIntent {
    name: String,
}

impl WorkerRelayCaConfigMapIntent {
    fn new(name: String) -> Result<Self, ProfileConfigError> {
        if !is_dns_subdomain(&name) {
            return Err(ProfileConfigError::InvalidWorkerRelayCaConfigMapName);
        }
        Ok(Self { name })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn resolve_observed(
        &self,
        expected_namespace: &str,
        observed: &ConfigMap,
    ) -> Result<PinnedWorkerRelayCaConfigMap, ResourceBuildError> {
        let pinned = PinnedWorkerRelayCaConfigMap::from_observed(expected_namespace, observed)?;
        if !pinned.matches_intent(&self.name) {
            return Err(ResourceBuildError::InvalidWorkerRelayCaConfigMap {
                field: "metadata.name",
            });
        }
        Ok(pinned)
    }
}

/// Required transport configuration owned by one immutable profile revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRelayIntent {
    url: WorkerRelayUrl,
    ca_config_map: WorkerRelayCaConfigMapIntent,
}

impl WorkerRelayIntent {
    fn new(url: String, ca_config_map_name: String) -> Result<Self, ProfileConfigError> {
        Ok(Self {
            url: WorkerRelayUrl::new(url)?,
            ca_config_map: WorkerRelayCaConfigMapIntent::new(ca_config_map_name)?,
        })
    }

    pub fn url(&self) -> &WorkerRelayUrl {
        &self.url
    }

    pub fn ca_config_map(&self) -> &WorkerRelayCaConfigMapIntent {
        &self.ca_config_map
    }
}

/// A validated Pod-only image-pull Secret reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImagePullSecretName(String);

impl ImagePullSecretName {
    fn new(name: String) -> Result<Self, ProfileConfigError> {
        if !is_dns_subdomain(&name) {
            return Err(ProfileConfigError::InvalidImagePullSecretName);
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ImagePullSecretNames(Vec<ImagePullSecretName>);

impl ImagePullSecretNames {
    fn new(names: Vec<String>) -> Result<Self, ProfileConfigError> {
        if names.len() > MAX_IMAGE_PULL_SECRETS {
            return Err(ProfileConfigError::TooManyImagePullSecrets);
        }

        let mut seen = BTreeSet::new();
        let mut validated = Vec::with_capacity(names.len());
        for name in names {
            if !seen.insert(name.clone()) {
                return Err(ProfileConfigError::DuplicateImagePullSecret);
            }
            validated.push(ImagePullSecretName::new(name)?);
        }
        Ok(Self(validated))
    }

    fn as_slice(&self) -> &[ImagePullSecretName] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeClassIntent {
    allowed: AllowedRuntimeClass,
}

impl RuntimeClassIntent {
    fn new(name: String, expected_handler: String) -> Result<Self, ResourceBuildError> {
        Ok(Self {
            allowed: AllowedRuntimeClass::new(name, expected_handler)?,
        })
    }

    pub fn name(&self) -> &str {
        self.allowed.name()
    }

    pub fn expected_handler(&self) -> &str {
        self.allowed.handler()
    }

    pub fn resolve_observed(
        &self,
        observed: &RuntimeClass,
    ) -> Result<RuntimeClassSelection, ResourceBuildError> {
        RuntimeClassSelection::from_observed(observed, [self.allowed.clone()])
    }
}

/// An operator-owned, versioned ConfigMap name that must never be reused.
///
/// The name remains an unresolved intent until the controller observes the
/// exact immutable ConfigMap and pins its Kubernetes UID and resourceVersion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillsConfigMapIntent {
    name: String,
}

impl SkillsConfigMapIntent {
    fn new(name: String) -> Result<Self, ResourceBuildError> {
        PinnedSkillsConfigMap::validate_name(&name)?;
        Ok(Self { name })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn resolve_observed(
        &self,
        expected_namespace: &str,
        observed: &ConfigMap,
    ) -> Result<PinnedSkillsConfigMap, ResourceBuildError> {
        let pinned = PinnedSkillsConfigMap::from_observed(expected_namespace, observed)?;
        if !pinned.matches_intent(&self.name) {
            return Err(ResourceBuildError::InvalidSkillsConfigMap {
                field: "metadata.name",
            });
        }
        Ok(pinned)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedWorkerProfile {
    profile: MvpWorkerProfile,
    runtime_class: Option<RuntimeClassIntent>,
    skills: Option<SkillsConfigMapIntent>,
    relay: WorkerRelayIntent,
    image_pull_secrets: ImagePullSecretNames,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedProfileRevisions {
    current: ProfileRef,
    revisions: BTreeMap<String, LoadedWorkerProfile>,
}

impl LoadedProfileRevisions {
    pub fn current(&self) -> &LoadedWorkerProfile {
        self.revisions
            .get(self.current.version())
            .expect("validated profile revisions retain their current entry")
    }

    pub fn current_profile_ref(&self) -> &ProfileRef {
        &self.current
    }

    pub fn revisions(&self) -> &BTreeMap<String, LoadedWorkerProfile> {
        &self.revisions
    }

    pub fn revision(&self, version: &str) -> Option<&LoadedWorkerProfile> {
        self.revisions.get(version)
    }
}

impl LoadedWorkerProfile {
    pub fn profile_ref(&self) -> &ProfileRef {
        self.profile.profile()
    }

    pub fn runtime_class_intent(&self) -> Option<&RuntimeClassIntent> {
        self.runtime_class.as_ref()
    }

    pub fn skills_intent(&self) -> Option<&SkillsConfigMapIntent> {
        self.skills.as_ref()
    }

    pub fn relay(&self) -> &WorkerRelayIntent {
        &self.relay
    }

    pub fn image_pull_secrets(&self) -> &[ImagePullSecretName] {
        self.image_pull_secrets.as_slice()
    }

    pub fn resolve_cluster_references(
        self,
        references: ResolvedClusterReferences,
    ) -> Result<ResolvedWorkerProfile, ProfileConfigError> {
        let profile_name = self.profile.profile().name().to_string();
        let runtime_matches = match (&self.runtime_class, &references.runtime_class) {
            (None, None) => true,
            (Some(intent), Some(selection)) => {
                selection.matches_intent(intent.name(), intent.expected_handler())
            }
            _ => false,
        };
        if !runtime_matches {
            return Err(ProfileConfigError::ClusterReferenceMismatch {
                profile: profile_name.clone(),
                reference: "RuntimeClass",
            });
        }

        let skills_match = match (&self.skills, &references.skills) {
            (None, None) => true,
            (Some(intent), Some(pin)) => pin.matches_intent(intent.name()),
            _ => false,
        };
        if !skills_match {
            return Err(ProfileConfigError::ClusterReferenceMismatch {
                profile: profile_name.clone(),
                reference: "skills ConfigMap",
            });
        }

        if !references
            .relay_ca
            .matches_intent(self.relay.ca_config_map().name())
        {
            return Err(ProfileConfigError::ClusterReferenceMismatch {
                profile: profile_name,
                reference: "worker relay CA ConfigMap",
            });
        }

        let relay_url = self.relay.url().as_str().to_owned();
        let image_pull_secrets = self
            .image_pull_secrets
            .0
            .into_iter()
            .map(|name| name.0)
            .collect();

        Ok(ResolvedWorkerProfile {
            profile: self.profile.with_cluster_references(
                references.runtime_class,
                references.skills,
                relay_url,
                references.relay_ca,
                image_pull_secrets,
            ),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedClusterReferences {
    runtime_class: Option<RuntimeClassSelection>,
    skills: Option<PinnedSkillsConfigMap>,
    relay_ca: PinnedWorkerRelayCaConfigMap,
}

impl ResolvedClusterReferences {
    pub fn new(
        runtime_class: Option<RuntimeClassSelection>,
        skills: Option<PinnedSkillsConfigMap>,
        relay_ca: PinnedWorkerRelayCaConfigMap,
    ) -> Self {
        Self {
            runtime_class,
            skills,
            relay_ca,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedWorkerProfile {
    profile: MvpWorkerProfile,
}

impl ResolvedWorkerProfile {
    pub fn into_worker_profile(self) -> MvpWorkerProfile {
        self.profile
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedControllerConfigV1 {
    policy: ControllerPolicy,
    profiles: BTreeMap<String, LoadedProfileRevisions>,
}

impl TrustedControllerConfigV1 {
    /// Read, bound, decode, and validate version-one worker configuration.
    pub fn from_reader<R>(reader: R) -> Result<Self, ProfileConfigError>
    where
        R: Read,
    {
        let mut source = Vec::new();
        reader
            .take((MAX_PROFILE_CONFIG_BYTES + 1) as u64)
            .read_to_end(&mut source)
            .map_err(|_| ProfileConfigError::Read)?;
        if source.len() > MAX_PROFILE_CONFIG_BYTES {
            return Err(ProfileConfigError::TooLarge);
        }
        let source = std::str::from_utf8(&source).map_err(|_| ProfileConfigError::InvalidUtf8)?;
        Self::from_toml(source)
    }

    pub fn from_toml(source: &str) -> Result<Self, ProfileConfigError> {
        if source.len() > MAX_PROFILE_CONFIG_BYTES {
            return Err(ProfileConfigError::TooLarge);
        }
        let decoded: ControllerConfigDto =
            toml::from_str(source).map_err(|_| ProfileConfigError::Decode)?;
        if decoded.schema_version != 1 {
            return Err(ProfileConfigError::UnsupportedSchemaVersion(
                decoded.schema_version,
            ));
        }
        if decoded.profiles.is_empty() {
            return Err(ProfileConfigError::EmptyProfiles);
        }

        let policy = ControllerPolicy::new(
            decoded.policy.compute_idle_seconds,
            decoded.policy.storage_retention_seconds,
            decoded.policy.max_active_workers,
        )?;
        let mut profiles = BTreeMap::new();
        for (name, profile) in decoded.profiles {
            validate_profile_name(&name)
                .map_err(|source| ProfileConfigError::InvalidProfileIdentity { source })?;
            if profile.revisions.is_empty() {
                return Err(ProfileConfigError::EmptyProfileRevisions { profile: name });
            }

            let mut revisions = BTreeMap::new();
            for (version, revision) in profile.revisions {
                let loaded = build_profile(name.clone(), version.clone(), revision)?;
                revisions.insert(version, loaded);
            }
            let current = revisions
                .get(&profile.current_version)
                .map(|loaded| loaded.profile_ref().clone())
                .ok_or_else(|| ProfileConfigError::CurrentProfileRevisionUnavailable {
                    profile: name.clone(),
                })?;
            profiles.insert(name, LoadedProfileRevisions { current, revisions });
        }
        Ok(Self { policy, profiles })
    }

    pub fn policy(&self) -> &ControllerPolicy {
        &self.policy
    }

    pub fn profiles(&self) -> &BTreeMap<String, LoadedProfileRevisions> {
        &self.profiles
    }

    pub fn profile(&self, name: &str) -> Option<&LoadedWorkerProfile> {
        self.profiles.get(name).map(LoadedProfileRevisions::current)
    }

    pub fn profile_revision(&self, name: &str, version: &str) -> Option<&LoadedWorkerProfile> {
        self.profiles
            .get(name)
            .and_then(|profile| profile.revision(version))
    }

    pub fn all_revisions(&self) -> impl Iterator<Item = &LoadedWorkerProfile> {
        self.profiles
            .values()
            .flat_map(|profile| profile.revisions().values())
    }

    pub fn current_profile_refs(&self) -> impl Iterator<Item = &ProfileRef> {
        self.profiles
            .values()
            .map(LoadedProfileRevisions::current_profile_ref)
    }
}

fn build_profile(
    name: String,
    version: String,
    decoded: WorkerProfileDto,
) -> Result<LoadedWorkerProfile, ProfileConfigError> {
    let profile = ProfileRef::new(name.clone(), version)
        .map_err(|source| ProfileConfigError::InvalidProfileIdentity { source })?;
    let workspace = PersistentWorkspace::new(
        decoded.workspace.size,
        decoded.workspace.storage_class,
        decoded.workspace.access_mode.into(),
    )
    .map_err(|source| invalid_profile(&name, source))?;
    let resources = WorkerResources::new(
        decoded.resources.requests.cpu,
        decoded.resources.limits.cpu,
        decoded.resources.requests.memory,
        decoded.resources.limits.memory,
        decoded.resources.requests.ephemeral_storage,
        decoded.resources.limits.ephemeral_storage,
    )
    .map_err(|source| invalid_profile(&name, source))?;
    let identity = RunAsIdentity::new(decoded.run_as.uid, decoded.run_as.gid)
        .map_err(|source| invalid_profile(&name, source))?;
    let egress = decoded
        .egress
        .into_iter()
        .map(|rule| build_egress_rule(rule).map_err(|source| invalid_profile(&name, source)))
        .collect::<Result<Vec<_>, _>>()?;
    let runtime_class = decoded
        .runtime_class
        .map(|intent| RuntimeClassIntent::new(intent.name, intent.expected_handler))
        .transpose()
        .map_err(|source| invalid_profile(&name, source))?;
    let skills = decoded
        .skills
        .map(|intent| SkillsConfigMapIntent::new(intent.config_map_name))
        .transpose()
        .map_err(|source| invalid_profile(&name, source))?;
    let relay = WorkerRelayIntent::new(decoded.relay.url, decoded.relay.ca_config_map_name)?;
    let image_pull_secrets = ImagePullSecretNames::new(decoded.image_pull_secrets)?;
    let profile = MvpWorkerProfile::new(
        profile,
        decoded.image,
        [decoded.supervisor.executable],
        decoded.supervisor.args,
        workspace,
        resources,
        egress,
        identity,
        None,
        None,
    )
    .map_err(|source| invalid_profile(&name, source))?;

    Ok(LoadedWorkerProfile {
        profile,
        runtime_class,
        skills,
        relay,
        image_pull_secrets,
    })
}

fn build_egress_rule(decoded: EgressRuleDto) -> Result<TrustedEgressRule, ResourceBuildError> {
    match decoded {
        EgressRuleDto::Cidr { cidr, ports } => {
            TrustedEgressRule::for_cidr(cidr, build_egress_ports(ports)?)
        }
        EgressRuleDto::Selectors {
            namespace_labels,
            pod_labels,
            ports,
        } => TrustedEgressRule::for_selectors(
            namespace_labels,
            pod_labels,
            build_egress_ports(ports)?,
        ),
    }
}

fn build_egress_ports(decoded: Vec<EgressPortDto>) -> Result<Vec<EgressPort>, ResourceBuildError> {
    decoded
        .into_iter()
        .map(|port| EgressPort::new(port.protocol.into(), port.port))
        .collect()
}

fn invalid_profile(profile: &str, source: ResourceBuildError) -> ProfileConfigError {
    ProfileConfigError::InvalidProfile {
        profile: profile.to_string(),
        source,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControllerConfigDto {
    schema_version: u32,
    policy: ControllerPolicyDto,
    profiles: BTreeMap<String, ProfileRevisionsDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileRevisionsDto {
    current_version: String,
    revisions: BTreeMap<String, WorkerProfileDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControllerPolicyDto {
    compute_idle_seconds: u64,
    storage_retention_seconds: u64,
    max_active_workers: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerProfileDto {
    image: String,
    #[serde(default)]
    image_pull_secrets: Vec<String>,
    relay: WorkerRelayIntentDto,
    supervisor: SupervisorDto,
    workspace: WorkspaceDto,
    resources: ResourcesDto,
    run_as: RunAsDto,
    egress: Vec<EgressRuleDto>,
    runtime_class: Option<RuntimeClassIntentDto>,
    skills: Option<SkillsConfigMapIntentDto>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRelayIntentDto {
    url: String,
    ca_config_map_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorDto {
    executable: String,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceDto {
    size: String,
    storage_class: String,
    access_mode: PvcAccessModeDto,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourcesDto {
    requests: ResourceValuesDto,
    limits: ResourceValuesDto,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceValuesDto {
    cpu: String,
    memory: String,
    ephemeral_storage: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunAsDto {
    uid: i64,
    gid: i64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "target", rename_all = "snake_case", deny_unknown_fields)]
enum EgressRuleDto {
    Cidr {
        cidr: String,
        ports: Vec<EgressPortDto>,
    },
    Selectors {
        namespace_labels: BTreeMap<String, String>,
        pod_labels: BTreeMap<String, String>,
        ports: Vec<EgressPortDto>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EgressPortDto {
    protocol: EgressProtocolDto,
    port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeClassIntentDto {
    name: String,
    expected_handler: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillsConfigMapIntentDto {
    /// Operator-versioned name; versioned names must never be reused.
    config_map_name: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PvcAccessModeDto {
    ReadWriteOncePod,
    ReadWriteOnce,
}

impl From<PvcAccessModeDto> for PvcAccessMode {
    fn from(value: PvcAccessModeDto) -> Self {
        match value {
            PvcAccessModeDto::ReadWriteOncePod => Self::ReadWriteOncePod,
            PvcAccessModeDto::ReadWriteOnce => Self::ReadWriteOnce,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum EgressProtocolDto {
    Tcp,
    Udp,
}

impl From<EgressProtocolDto> for EgressProtocol {
    fn from(value: EgressProtocolDto) -> Self {
        match value {
            EgressProtocolDto::Tcp => Self::Tcp,
            EgressProtocolDto::Udp => Self::Udp,
        }
    }
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
