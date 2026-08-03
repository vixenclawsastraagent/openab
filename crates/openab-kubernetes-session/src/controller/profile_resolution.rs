use crate::profile_config::{
    LoadedWorkerProfile, ResolvedClusterReferences, RuntimeClassIntent, SkillsConfigMapIntent,
    TrustedControllerConfigV1,
};
use crate::resources::{MvpWorkerProfile, PinnedSkillsConfigMap};
use crate::state::ProfileRef;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::node::v1::RuntimeClass;
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

/// Resolved current and retained historical worker-profile revisions.
///
/// Historical revisions whose cluster references are no longer observable are
/// deliberately omitted. Existing sessions pinned to those revisions then use
/// the controller's existing unavailable-profile containment path without
/// making the whole controller unready.
pub struct ResolvedProfileRevisions {
    profiles: Vec<MvpWorkerProfile>,
    current_profile_refs: Vec<ProfileRef>,
    unavailable_historical_revision_count: usize,
}

impl ResolvedProfileRevisions {
    pub fn profiles(&self) -> &[MvpWorkerProfile] {
        &self.profiles
    }

    pub fn current_profile_refs(&self) -> &[ProfileRef] {
        &self.current_profile_refs
    }

    pub fn unavailable_historical_revision_count(&self) -> usize {
        self.unavailable_historical_revision_count
    }

    pub fn into_parts(self) -> (Vec<MvpWorkerProfile>, Vec<ProfileRef>, usize) {
        (
            self.profiles,
            self.current_profile_refs,
            self.unavailable_historical_revision_count,
        )
    }
}

impl fmt::Debug for ResolvedProfileRevisions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedProfileRevisions")
            .field("profile_count", &self.profiles.len())
            .field("current_profile_count", &self.current_profile_refs.len())
            .field(
                "unavailable_historical_revision_count",
                &self.unavailable_historical_revision_count,
            )
            .finish()
    }
}

/// A sanitized startup failure that never retains Kubernetes response bodies
/// or operator-selected resource identifiers.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ProfileResolutionError {
    #[error("the worker namespace is invalid")]
    InvalidWorkerNamespace,
    #[error("a current worker profile cluster reference could not be observed")]
    CurrentReferenceUnavailable,
    #[error("a current worker profile cluster reference observation is invalid")]
    CurrentReferenceInvalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferenceFailure {
    Unavailable,
    Invalid,
}

struct ClusterReferenceResolver {
    runtime_classes: Api<RuntimeClass>,
    skills_config_maps: Api<ConfigMap>,
    runtime_class_cache: BTreeMap<String, Result<RuntimeClass, ReferenceFailure>>,
    skills_config_map_cache: BTreeMap<String, Result<PinnedSkillsConfigMap, ReferenceFailure>>,
    worker_namespace: String,
}

impl ClusterReferenceResolver {
    fn new(client: Client, worker_namespace: &str) -> Self {
        Self {
            runtime_classes: Api::all(client.clone()),
            skills_config_maps: Api::namespaced(client, worker_namespace),
            runtime_class_cache: BTreeMap::new(),
            skills_config_map_cache: BTreeMap::new(),
            worker_namespace: worker_namespace.to_string(),
        }
    }

    async fn resolve(
        &mut self,
        loaded: &LoadedWorkerProfile,
    ) -> Result<MvpWorkerProfile, ReferenceFailure> {
        let runtime_class = match loaded.runtime_class_intent() {
            Some(intent) => Some(self.resolve_runtime_class(intent).await?),
            None => None,
        };
        let skills = match loaded.skills_intent() {
            Some(intent) => Some(self.resolve_skills_config_map(intent).await?),
            None => None,
        };

        loaded
            .clone()
            .resolve_cluster_references(ResolvedClusterReferences::new(runtime_class, skills))
            .map(|resolved| resolved.into_worker_profile())
            .map_err(|_| ReferenceFailure::Invalid)
    }

    async fn resolve_runtime_class(
        &mut self,
        intent: &RuntimeClassIntent,
    ) -> Result<crate::resources::RuntimeClassSelection, ReferenceFailure> {
        let observed = if let Some(cached) = self.runtime_class_cache.get(intent.name()) {
            cached.clone()?
        } else {
            let observed = self
                .runtime_classes
                .get(intent.name())
                .await
                .map_err(|_| ReferenceFailure::Unavailable);
            self.runtime_class_cache
                .insert(intent.name().to_string(), observed.clone());
            observed?
        };

        intent
            .resolve_observed(&observed)
            .map_err(|_| ReferenceFailure::Invalid)
    }

    async fn resolve_skills_config_map(
        &mut self,
        intent: &SkillsConfigMapIntent,
    ) -> Result<PinnedSkillsConfigMap, ReferenceFailure> {
        if let Some(cached) = self.skills_config_map_cache.get(intent.name()) {
            return cached.clone();
        }

        // Convert the observation immediately. In particular, ConfigMap data
        // is never placed in the cache or returned from this startup boundary.
        let resolved = self
            .skills_config_maps
            .get(intent.name())
            .await
            .map_err(|_| ReferenceFailure::Unavailable)
            .and_then(|observed| {
                intent
                    .resolve_observed(&self.worker_namespace, &observed)
                    .map_err(|_| ReferenceFailure::Invalid)
            });
        self.skills_config_map_cache
            .insert(intent.name().to_string(), resolved.clone());
        resolved
    }
}

/// Resolve trusted profile configuration against current Kubernetes
/// observations before constructing controller coordinators.
///
/// Every configured current revision is resolved first and fails startup
/// closed. Only after all current revisions succeed are historical revisions
/// attempted; a failed historical revision is omitted and counted.
pub async fn resolve_profile_revisions(
    client: Client,
    worker_namespace: &str,
    config: &TrustedControllerConfigV1,
) -> Result<ResolvedProfileRevisions, ProfileResolutionError> {
    if !is_dns_label(worker_namespace) {
        return Err(ProfileResolutionError::InvalidWorkerNamespace);
    }
    let mut resolver = ClusterReferenceResolver::new(client, worker_namespace);
    let mut profiles = Vec::new();
    let mut current_profile_refs = Vec::with_capacity(config.profiles().len());

    for revisions in config.profiles().values() {
        let profile =
            resolver
                .resolve(revisions.current())
                .await
                .map_err(|failure| match failure {
                    ReferenceFailure::Unavailable => {
                        ProfileResolutionError::CurrentReferenceUnavailable
                    }
                    ReferenceFailure::Invalid => ProfileResolutionError::CurrentReferenceInvalid,
                })?;
        current_profile_refs.push(revisions.current_profile_ref().clone());
        profiles.push(profile);
    }

    let mut unavailable_historical_revision_count = 0;
    for revisions in config.profiles().values() {
        for loaded in revisions.revisions().values() {
            if loaded.profile_ref() == revisions.current_profile_ref() {
                continue;
            }
            match resolver.resolve(loaded).await {
                Ok(profile) => profiles.push(profile),
                Err(_) => unavailable_historical_revision_count += 1,
            }
        }
    }

    Ok(ResolvedProfileRevisions {
        profiles,
        current_profile_refs,
        unavailable_historical_revision_count,
    })
}

fn is_dns_label(value: &str) -> bool {
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
