use super::{
    BootstrapPresence, ConsumedBootstrap, GenerationProvisioner, GenerationProvisionerError,
    GenerationResource, ObservedWorker, ProvisionerOperation, RegistrationOperation,
    RegistrationProvisioner, RegistrationProvisionerError, VerifiedBootstrap, WorkerBootstrapAuth,
};
use crate::bridge::SessionBinding;
use crate::identity::{ResourceNames, ScopeId, SessionId};
use crate::resources::{DesiredGeneration, GenerationContext, MvpWorkerProfile};
use crate::state::SessionPhase;
use crate::store::StoredAnchor;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Secret, ServiceAccount};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::node::v1::RuntimeClass;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, ListParams, PostParams, Preconditions};
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use subtle::ConstantTimeEq;
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
const ANCHOR_NAME_ANNOTATION: &str = "openab.dev/anchor-name";
const ANCHOR_UID_ANNOTATION: &str = "openab.dev/anchor-uid";

#[derive(Clone, Copy)]
enum ChildKind {
    PersistentVolumeClaim,
    NetworkPolicy,
    ServiceAccount,
    RegistrationSecret,
    Pod,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChildUidSnapshot {
    persistent_volume_claim: String,
    network_policy: String,
    service_account: String,
    registration_secret: String,
}

impl ChildUidSnapshot {
    fn from_observed(
        pvc: &PersistentVolumeClaim,
        policy: &NetworkPolicy,
        account: &ServiceAccount,
        secret: &Secret,
    ) -> Result<Self, GenerationProvisionerError> {
        Ok(Self {
            persistent_volume_claim: required_uid(pvc, GenerationResource::PersistentVolumeClaim)?
                .to_string(),
            network_policy: required_uid(policy, GenerationResource::NetworkPolicy)?.to_string(),
            service_account: required_uid(account, GenerationResource::ServiceAccount)?.to_string(),
            registration_secret: required_uid(secret, GenerationResource::RegistrationSecret)?
                .to_string(),
        })
    }

    fn validate_same(&self, expected: &Self) -> Result<(), GenerationProvisionerError> {
        for (resource, matches) in [
            (
                GenerationResource::PersistentVolumeClaim,
                self.persistent_volume_claim == expected.persistent_volume_claim,
            ),
            (
                GenerationResource::NetworkPolicy,
                self.network_policy == expected.network_policy,
            ),
            (
                GenerationResource::ServiceAccount,
                self.service_account == expected.service_account,
            ),
            (
                GenerationResource::RegistrationSecret,
                self.registration_secret == expected.registration_secret,
            ),
        ] {
            if !matches {
                return Err(GenerationProvisionerError::ResourceRejected { resource });
            }
        }
        Ok(())
    }
}

struct GenerationPreflight {
    children: ChildUidSnapshot,
    pod: Option<Pod>,
}

impl ChildKind {
    fn resource_label(self) -> &'static str {
        match self {
            Self::PersistentVolumeClaim => "workspace-pvc",
            Self::NetworkPolicy => "worker-network-policy",
            Self::ServiceAccount => "worker-service-account",
            Self::RegistrationSecret => "registration-secret",
            Self::Pod => "worker-pod",
        }
    }

    fn expected_name(self, names: ResourceNames, generation: u64) -> Option<String> {
        match self {
            Self::PersistentVolumeClaim => Some(names.pvc()),
            Self::NetworkPolicy => names.pod(generation).ok().map(|name| format!("{name}-net")),
            Self::ServiceAccount => names.service_account(generation).ok(),
            Self::RegistrationSecret => names.registration_secret(generation).ok(),
            Self::Pod => names.pod(generation).ok(),
        }
    }
}

/// Kubernetes-backed generation driver for the default-off session add-on.
///
/// The driver is deliberately namespaced and scope-bound. It has no fallback
/// path to local execution and never changes a lifecycle anchor itself.
#[derive(Clone)]
pub struct KubernetesGenerationProvisioner {
    namespace: String,
    scope_id: ScopeId,
    persistent_volume_claims: Api<PersistentVolumeClaim>,
    network_policies: Api<NetworkPolicy>,
    service_accounts: Api<ServiceAccount>,
    registration_secrets: Api<Secret>,
    pods: Api<Pod>,
    skills_config_maps: Api<ConfigMap>,
    runtime_classes: Api<RuntimeClass>,
}

impl KubernetesGenerationProvisioner {
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        scope_id: ScopeId,
    ) -> Result<Self, GenerationProvisionerError> {
        let namespace = namespace.into();
        if !is_dns_label(&namespace) {
            return Err(GenerationProvisionerError::InvalidGeneration);
        }
        Ok(Self {
            persistent_volume_claims: Api::namespaced(client.clone(), &namespace),
            network_policies: Api::namespaced(client.clone(), &namespace),
            service_accounts: Api::namespaced(client.clone(), &namespace),
            registration_secrets: Api::namespaced(client.clone(), &namespace),
            pods: Api::namespaced(client.clone(), &namespace),
            skills_config_maps: Api::namespaced(client.clone(), &namespace),
            runtime_classes: Api::all(client),
            namespace,
            scope_id,
        })
    }

    async fn prove_listed_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        let selector = format!(
            "{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{SESSION_LABEL}={}",
            &session_id.as_hex()[..40]
        );
        let params = ListParams::default().labels(&selector);

        let claims = self
            .persistent_volume_claims
            .list(&params)
            .await
            .map_err(proof_api_error)?;
        self.reject_discovered(
            session_id,
            ChildKind::PersistentVolumeClaim,
            claims.items.iter().map(|object| &object.metadata),
        )?;

        let policies = self
            .network_policies
            .list(&params)
            .await
            .map_err(proof_api_error)?;
        self.reject_discovered(
            session_id,
            ChildKind::NetworkPolicy,
            policies.items.iter().map(|object| &object.metadata),
        )?;

        let accounts = self
            .service_accounts
            .list(&params)
            .await
            .map_err(proof_api_error)?;
        self.reject_discovered(
            session_id,
            ChildKind::ServiceAccount,
            accounts.items.iter().map(|object| &object.metadata),
        )?;

        let secrets = self
            .registration_secrets
            .list(&params)
            .await
            .map_err(proof_api_error)?;
        self.reject_discovered(
            session_id,
            ChildKind::RegistrationSecret,
            secrets.items.iter().map(|object| &object.metadata),
        )?;

        let pods = self.pods.list(&params).await.map_err(proof_api_error)?;
        self.reject_discovered(
            session_id,
            ChildKind::Pod,
            pods.items.iter().map(|object| &object.metadata),
        )
    }

    fn reject_discovered<'a, I>(
        &self,
        session_id: SessionId,
        kind: ChildKind,
        objects: I,
    ) -> Result<(), GenerationProvisionerError>
    where
        I: IntoIterator<Item = &'a ObjectMeta>,
    {
        let mut present = false;
        for metadata in objects {
            validate_discovered_metadata(
                metadata,
                &self.namespace,
                self.scope_id,
                session_id,
                kind,
            )?;
            present = true;
        }
        if present {
            Err(GenerationProvisionerError::ChildrenPresent)
        } else {
            Ok(())
        }
    }

    async fn prove_deterministic_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        let names = ResourceNames::new(session_id);
        let generation = 1;
        let network_policy_name = ChildKind::NetworkPolicy
            .expected_name(names, generation)
            .ok_or(GenerationProvisionerError::ChildrenAmbiguous)?;
        let service_account_name = names
            .service_account(generation)
            .map_err(|_| GenerationProvisionerError::ChildrenAmbiguous)?;
        let registration_secret_name = names
            .registration_secret(generation)
            .map_err(|_| GenerationProvisionerError::ChildrenAmbiguous)?;
        let pod_name = names
            .pod(generation)
            .map_err(|_| GenerationProvisionerError::ChildrenAmbiguous)?;

        if let Some(object) = self
            .persistent_volume_claims
            .get_opt(&names.pvc())
            .await
            .map_err(proof_api_error)?
        {
            return self.reject_direct(
                session_id,
                ChildKind::PersistentVolumeClaim,
                &object.metadata,
            );
        }
        if let Some(object) = self
            .network_policies
            .get_opt(&network_policy_name)
            .await
            .map_err(proof_api_error)?
        {
            return self.reject_direct(session_id, ChildKind::NetworkPolicy, &object.metadata);
        }
        if let Some(object) = self
            .service_accounts
            .get_opt(&service_account_name)
            .await
            .map_err(proof_api_error)?
        {
            return self.reject_direct(session_id, ChildKind::ServiceAccount, &object.metadata);
        }
        if let Some(object) = self
            .registration_secrets
            .get_opt(&registration_secret_name)
            .await
            .map_err(proof_api_error)?
        {
            return self.reject_direct(session_id, ChildKind::RegistrationSecret, &object.metadata);
        }
        if let Some(object) = self
            .pods
            .get_opt(&pod_name)
            .await
            .map_err(proof_api_error)?
        {
            return self.reject_direct(session_id, ChildKind::Pod, &object.metadata);
        }
        Ok(())
    }

    fn reject_direct(
        &self,
        session_id: SessionId,
        kind: ChildKind,
        metadata: &ObjectMeta,
    ) -> Result<(), GenerationProvisionerError> {
        validate_discovered_metadata(metadata, &self.namespace, self.scope_id, session_id, kind)?;
        Err(GenerationProvisionerError::ChildrenPresent)
    }

    async fn ensure_generation(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        if anchor.namespace() != self.namespace
            || anchor.state().scope_id() != self.scope_id
            || anchor.state().phase() != SessionPhase::Provisioning
            || anchor.state().profile() != profile.profile()
        {
            return Err(GenerationProvisionerError::InvalidGeneration);
        }

        let names = ResourceNames::new(anchor.state().session_id());
        let context = GenerationContext::from_anchor(
            &self.namespace,
            anchor.name(),
            anchor.uid(),
            anchor.state(),
            names,
        )
        .map_err(|_| GenerationProvisionerError::InvalidGeneration)?;
        // Pod structure does not depend on the bootstrap token. Use a local
        // placeholder for the read-only started-generation probe so recovery
        // never depends on fresh randomness.
        let probe_desired = DesiredGeneration::build(context.clone(), profile.clone(), [0_u8; 32])
            .map_err(|_| GenerationProvisionerError::InvalidGeneration)?;

        if let Some(started_pod) = self
            .observe_started_pod(&probe_desired, anchor.state().pod_uid())
            .await?
        {
            // Once a Pod exists, all required children are adopt-only. In
            // particular, a consumed bootstrap Secret must never be recreated
            // with a new token for that running Pod.
            let desired = self
                .adopt_existing_registration_secret(context, profile.clone(), &probe_desired)
                .await?;
            let started_uid = required_uid(&started_pod, GenerationResource::Pod)?;
            let preflight = self
                .preflight_before_pod(&desired, Some(started_uid), None)
                .await?;
            let pod = preflight
                .pod
                .ok_or(GenerationProvisionerError::ResourceRejected {
                    resource: GenerationResource::Pod,
                })?;
            return observed_worker(pod);
        }

        let mut token = [0_u8; 32];
        getrandom::fill(&mut token)
            .map_err(|_| GenerationProvisionerError::RandomnessUnavailable)?;
        let desired = DesiredGeneration::build(context.clone(), profile.clone(), token)
            .map_err(|_| GenerationProvisionerError::InvalidGeneration)?;

        self.ensure_resource(
            &self.persistent_volume_claims,
            desired
                .persistent_volume_claim()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            desired.persistent_volume_claim(),
            GenerationResource::PersistentVolumeClaim,
            |observed| desired.validate_persistent_volume_claim(observed),
        )
        .await?;
        self.ensure_resource(
            &self.network_policies,
            desired
                .network_policy()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            desired.network_policy(),
            GenerationResource::NetworkPolicy,
            |observed| desired.validate_network_policy(observed),
        )
        .await?;
        self.ensure_resource(
            &self.service_accounts,
            desired
                .service_account()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            desired.service_account(),
            GenerationResource::ServiceAccount,
            |observed| desired.validate_service_account(observed),
        )
        .await?;

        let desired = self
            .ensure_registration_secret(context, profile.clone(), desired)
            .await?;
        let preflight = self.preflight_before_pod(&desired, None, None).await?;
        let pod = match preflight.pod {
            Some(pod) => pod,
            None => {
                self.create_and_confirm_pod(&desired, &preflight.children)
                    .await?
            }
        };
        observed_worker(pod)
    }

    async fn create_and_confirm_pod(
        &self,
        desired: &DesiredGeneration,
        children: &ChildUidSnapshot,
    ) -> Result<Pod, GenerationProvisionerError> {
        let pod_name = desired
            .pod()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let candidate = match self
            .pods
            .create(&PostParams::default(), desired.pod())
            .await
        {
            Ok(created) => created,
            Err(kube::Error::Api(status)) if status.is_already_exists() => self
                .pods
                .get(pod_name)
                .await
                .map_err(|_| ensure_api_error())?,
            Err(_) => return Err(ensure_api_error()),
        };
        desired.validate_pod(&candidate).map_err(|_| {
            GenerationProvisionerError::ResourceRejected {
                resource: GenerationResource::Pod,
            }
        })?;
        let candidate_uid = required_uid(&candidate, GenerationResource::Pod)?;
        let confirmed = self
            .preflight_before_pod(desired, Some(candidate_uid), Some(children))
            .await?
            .pod
            .ok_or(GenerationProvisionerError::ResourceRejected {
                resource: GenerationResource::Pod,
            })?;
        Ok(confirmed)
    }

    async fn observe_started_pod(
        &self,
        desired: &DesiredGeneration,
        recorded_uid: Option<&str>,
    ) -> Result<Option<Pod>, GenerationProvisionerError> {
        let pod_name = desired
            .pod()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let observed = self
            .pods
            .get_opt(pod_name)
            .await
            .map_err(|_| ensure_api_error())?;
        if let Some(pod) = observed.as_ref() {
            desired.validate_pod(pod).map_err(|_| {
                GenerationProvisionerError::ResourceRejected {
                    resource: GenerationResource::Pod,
                }
            })?;
        }
        match (recorded_uid, observed.as_ref()) {
            (Some(expected), Some(pod)) if pod.metadata.uid.as_deref() == Some(expected) => {}
            (Some(_), _) => {
                return Err(GenerationProvisionerError::ResourceRejected {
                    resource: GenerationResource::Pod,
                })
            }
            (None, _) => {}
        }
        Ok(observed)
    }

    async fn ensure_resource<K, F, E>(
        &self,
        api: &Api<K>,
        name: &str,
        desired: &K,
        resource: GenerationResource,
        validate: F,
    ) -> Result<K, GenerationProvisionerError>
    where
        K: Clone + Debug + DeserializeOwned + Serialize,
        F: Fn(&K) -> Result<(), E>,
    {
        let observed = match api.create(&PostParams::default(), desired).await {
            Ok(created) => created,
            Err(kube::Error::Api(status)) if status.is_already_exists() => {
                api.get(name).await.map_err(|_| ensure_api_error())?
            }
            Err(_) => return Err(ensure_api_error()),
        };
        validate(&observed)
            .map_err(|_| GenerationProvisionerError::ResourceRejected { resource })?;
        Ok(observed)
    }

    async fn get_exact<K, F, E>(
        &self,
        api: &Api<K>,
        name: &str,
        resource: GenerationResource,
        validate: F,
    ) -> Result<K, GenerationProvisionerError>
    where
        K: Clone + Debug + DeserializeOwned,
        F: Fn(&K) -> Result<(), E>,
    {
        let observed = api
            .get_opt(name)
            .await
            .map_err(|_| ensure_api_error())?
            .ok_or(GenerationProvisionerError::ResourceRejected { resource })?;
        validate(&observed)
            .map_err(|_| GenerationProvisionerError::ResourceRejected { resource })?;
        Ok(observed)
    }

    async fn adopt_existing_registration_secret(
        &self,
        context: GenerationContext,
        profile: MvpWorkerProfile,
        desired: &DesiredGeneration,
    ) -> Result<DesiredGeneration, GenerationProvisionerError> {
        let name = desired
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let observed = self
            .registration_secrets
            .get_opt(name)
            .await
            .map_err(|_| ensure_api_error())?
            .ok_or(GenerationProvisionerError::BootstrapCredentialConsumedOrMissing)?;
        desired_from_observed_secret(context, profile, &observed)
    }

    async fn ensure_registration_secret(
        &self,
        context: GenerationContext,
        profile: MvpWorkerProfile,
        desired: DesiredGeneration,
    ) -> Result<DesiredGeneration, GenerationProvisionerError> {
        let name = desired
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        match self
            .registration_secrets
            .create(&PostParams::default(), desired.registration_secret())
            .await
        {
            Ok(created) => {
                desired
                    .validate_registration_secret(&created)
                    .map_err(|_| GenerationProvisionerError::ResourceRejected {
                        resource: GenerationResource::RegistrationSecret,
                    })?;
                Ok(desired)
            }
            Err(kube::Error::Api(status)) if status.is_already_exists() => {
                let observed = self
                    .registration_secrets
                    .get(name)
                    .await
                    .map_err(|_| ensure_api_error())?;
                desired_from_observed_secret(context, profile, &observed)
            }
            Err(_) => Err(ensure_api_error()),
        }
    }

    /// Final pre-create check for cluster-scoped or shared read-only inputs.
    ///
    /// Registration must perform the same check after observing worker
    /// readiness before the lifecycle anchor may advance to Ready. That
    /// post-ready gate belongs to the registration slice.
    async fn revalidate_external_pins(
        &self,
        desired: &DesiredGeneration,
    ) -> Result<(), GenerationProvisionerError> {
        if let Some(name) = desired.skills_config_map_name() {
            self.get_exact(
                &self.skills_config_maps,
                name,
                GenerationResource::SkillsConfigMap,
                |observed| desired.validate_skills_config_map(observed),
            )
            .await?;
        }
        if let Some(name) = desired.runtime_class_name() {
            self.get_exact(
                &self.runtime_classes,
                name,
                GenerationResource::RuntimeClass,
                |observed| desired.validate_runtime_class(observed),
            )
            .await?;
        }
        Ok(())
    }

    async fn preflight_before_pod(
        &self,
        desired: &DesiredGeneration,
        expected_started_uid: Option<&str>,
        expected_children: Option<&ChildUidSnapshot>,
    ) -> Result<GenerationPreflight, GenerationProvisionerError> {
        let pvc_name = required_name(desired.persistent_volume_claim())?;
        let pvc = self
            .get_exact(
                &self.persistent_volume_claims,
                pvc_name,
                GenerationResource::PersistentVolumeClaim,
                |observed| desired.validate_persistent_volume_claim(observed),
            )
            .await?;
        let policy_name = desired
            .network_policy()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let policy = self
            .get_exact(
                &self.network_policies,
                policy_name,
                GenerationResource::NetworkPolicy,
                |observed| desired.validate_network_policy(observed),
            )
            .await?;
        let account_name = desired
            .service_account()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let account = self
            .get_exact(
                &self.service_accounts,
                account_name,
                GenerationResource::ServiceAccount,
                |observed| desired.validate_service_account(observed),
            )
            .await?;
        let secret_name = desired
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let secret = self
            .get_exact(
                &self.registration_secrets,
                secret_name,
                GenerationResource::RegistrationSecret,
                |observed| desired.validate_registration_secret(observed),
            )
            .await?;
        let children = ChildUidSnapshot::from_observed(&pvc, &policy, &account, &secret)?;
        if let Some(expected) = expected_children {
            children.validate_same(expected)?;
        }

        let pod_name = desired
            .pod()
            .metadata
            .name
            .as_deref()
            .ok_or(GenerationProvisionerError::InvalidGeneration)?;
        let pod = self
            .pods
            .get_opt(pod_name)
            .await
            .map_err(|_| ensure_api_error())?;
        if let Some(pod) = pod.as_ref() {
            desired.validate_pod(pod).map_err(|_| {
                GenerationProvisionerError::ResourceRejected {
                    resource: GenerationResource::Pod,
                }
            })?;
        }
        if let Some(expected_uid) = expected_started_uid {
            if pod.as_ref().and_then(|pod| pod.metadata.uid.as_deref()) != Some(expected_uid) {
                return Err(GenerationProvisionerError::ResourceRejected {
                    resource: GenerationResource::Pod,
                });
            }
        }

        self.prove_no_conflicting_children(desired, &children, pod.as_ref())
            .await?;
        // Keep this as the last Kubernetes read before either adopting or
        // creating the Pod. A pinned dependency changing during the earlier
        // child inventory proof must fail closed.
        self.revalidate_external_pins(desired).await?;
        Ok(GenerationPreflight { children, pod })
    }

    async fn prove_no_conflicting_children(
        &self,
        desired: &DesiredGeneration,
        children: &ChildUidSnapshot,
        pod: Option<&Pod>,
    ) -> Result<(), GenerationProvisionerError> {
        let selector = format!(
            "{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{SESSION_LABEL}={}",
            &desired.context().session_id().as_hex()[..40]
        );
        let params = ListParams::default().labels(&selector);

        let claims = self
            .persistent_volume_claims
            .list(&params)
            .await
            .map_err(|_| ensure_api_error())?;
        require_exact_inventory(
            claims.items.iter(),
            required_name(desired.persistent_volume_claim())?,
            Some(children.persistent_volume_claim.as_str()),
            GenerationResource::PersistentVolumeClaim,
            |observed| desired.validate_persistent_volume_claim(observed),
        )?;
        let policies = self
            .network_policies
            .list(&params)
            .await
            .map_err(|_| ensure_api_error())?;
        require_exact_inventory(
            policies.items.iter(),
            desired
                .network_policy()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            Some(children.network_policy.as_str()),
            GenerationResource::NetworkPolicy,
            |observed| desired.validate_network_policy(observed),
        )?;
        let accounts = self
            .service_accounts
            .list(&params)
            .await
            .map_err(|_| ensure_api_error())?;
        require_exact_inventory(
            accounts.items.iter(),
            desired
                .service_account()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            Some(children.service_account.as_str()),
            GenerationResource::ServiceAccount,
            |observed| desired.validate_service_account(observed),
        )?;
        let secrets = self
            .registration_secrets
            .list(&params)
            .await
            .map_err(|_| ensure_api_error())?;
        require_exact_inventory(
            secrets.items.iter(),
            desired
                .registration_secret()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            Some(children.registration_secret.as_str()),
            GenerationResource::RegistrationSecret,
            |observed| desired.validate_registration_secret(observed),
        )?;
        let pods = self
            .pods
            .list(&params)
            .await
            .map_err(|_| ensure_api_error())?;
        require_exact_inventory(
            pods.items.iter(),
            desired
                .pod()
                .metadata
                .name
                .as_deref()
                .ok_or(GenerationProvisionerError::InvalidGeneration)?,
            pod.map(|pod| required_uid(pod, GenerationResource::Pod))
                .transpose()?,
            GenerationResource::Pod,
            |observed| desired.validate_pod(observed),
        )
    }
}

#[async_trait]
impl GenerationProvisioner for KubernetesGenerationProvisioner {
    async fn prove_v1_children_absent(
        &self,
        session_id: SessionId,
    ) -> Result<(), GenerationProvisionerError> {
        self.prove_listed_children_absent(session_id).await?;
        self.prove_deterministic_children_absent(session_id).await
    }

    async fn ensure_provisioning_generation(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<ObservedWorker, GenerationProvisionerError> {
        self.ensure_generation(anchor, profile).await
    }
}

#[async_trait]
impl RegistrationProvisioner for KubernetesGenerationProvisioner {
    async fn verify_bootstrap(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
        expected_binding: &SessionBinding,
        auth: &WorkerBootstrapAuth,
    ) -> Result<VerifiedBootstrap, RegistrationProvisionerError> {
        let pod_uid = validate_registration_anchor(
            anchor,
            profile,
            self.scope_id,
            &self.namespace,
            expected_binding,
        )?;
        if pod_uid != auth.pod_uid() {
            return Err(RegistrationProvisionerError::Unauthorized);
        }

        let names = ResourceNames::new(anchor.state().session_id());
        let context = GenerationContext::from_anchor(
            &self.namespace,
            anchor.name(),
            anchor.uid(),
            anchor.state(),
            names,
        )
        .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
        let probe = DesiredGeneration::build(context.clone(), profile.clone(), [0_u8; 32])
            .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
        let secret_name = probe
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .ok_or(RegistrationProvisionerError::InvalidGeneration)?;
        let initial_secret = self
            .registration_secrets
            .get_opt(secret_name)
            .await
            .map_err(|_| registration_api_error(RegistrationOperation::VerifyResources))?
            .ok_or(RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing)?;
        let token = registration_token(&initial_secret)?;
        let desired = desired_from_observed_secret(context, profile.clone(), &initial_secret)
            .map_err(map_generation_registration_error)?;
        let initial_secret_uid = registration_metadata(&initial_secret, "uid")?.to_owned();
        let initial_secret_resource_version =
            registration_metadata(&initial_secret, "resourceVersion")?.to_owned();

        let preflight = self
            .preflight_before_pod(&desired, Some(pod_uid), None)
            .await
            .map_err(map_generation_registration_error)?;
        if preflight.children.registration_secret != initial_secret_uid {
            return Err(RegistrationProvisionerError::ResourceRejected);
        }
        let observed_pod_uid = preflight
            .pod
            .as_ref()
            .and_then(|pod| pod.metadata.uid.as_deref())
            .ok_or(RegistrationProvisionerError::ResourceRejected)?;
        if observed_pod_uid != pod_uid {
            return Err(RegistrationProvisionerError::ResourceRejected);
        }

        // Re-read the Secret after the full resource and pin proof so the
        // deletion preconditions fence the latest exact credential object.
        let final_secret = self
            .registration_secrets
            .get_opt(secret_name)
            .await
            .map_err(|_| registration_api_error(RegistrationOperation::VerifyResources))?
            .ok_or(RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing)?;
        desired
            .validate_registration_secret(&final_secret)
            .map_err(|_| RegistrationProvisionerError::ResourceRejected)?;
        let final_secret_uid = registration_metadata(&final_secret, "uid")?;
        let final_resource_version = registration_metadata(&final_secret, "resourceVersion")?;
        if final_secret_uid != initial_secret_uid
            || final_resource_version != initial_secret_resource_version
        {
            return Err(RegistrationProvisionerError::ResourceRejected);
        }

        if !bool::from(token.ct_eq(auth.token())) {
            return Err(RegistrationProvisionerError::Unauthorized);
        }
        VerifiedBootstrap::new(
            expected_binding.clone(),
            anchor.uid(),
            pod_uid,
            secret_name,
            final_secret_uid,
            final_resource_version,
        )
    }

    async fn consume_bootstrap(
        &self,
        verified: VerifiedBootstrap,
    ) -> Result<ConsumedBootstrap, RegistrationProvisionerError> {
        let preconditions = Preconditions {
            resource_version: Some(verified.secret_resource_version().to_owned()),
            uid: Some(verified.secret_uid().to_owned()),
        };
        let delete = DeleteParams::default().preconditions(preconditions);
        match self
            .registration_secrets
            .delete(verified.secret_name(), &delete)
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(status)) if status.is_not_found() => {}
            Err(_) => {
                return Err(registration_api_error(
                    RegistrationOperation::DeleteBootstrapSecret,
                ))
            }
        }

        match self
            .registration_secrets
            .get_opt(verified.secret_name())
            .await
            .map_err(|_| {
                registration_api_error(RegistrationOperation::ObserveBootstrapSecretDeletion)
            })? {
            None => Ok(verified.into_consumed()),
            Some(secret) if secret.metadata.uid.as_deref() == Some(verified.secret_uid()) => {
                Err(RegistrationProvisionerError::BootstrapDeletionNotObserved)
            }
            Some(_) => Err(RegistrationProvisionerError::ResourceRejected),
        }
    }

    async fn observe_bootstrap_presence(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<BootstrapPresence, RegistrationProvisionerError> {
        let expected_binding = SessionBinding::new(
            anchor.state().scope_id(),
            anchor.state().session_id(),
            anchor.state().fence().clone(),
            anchor.state().incarnation_id(),
        )
        .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
        let pod_uid = validate_registration_anchor(
            anchor,
            profile,
            self.scope_id,
            &self.namespace,
            &expected_binding,
        )?;
        let names = ResourceNames::new(anchor.state().session_id());
        let context = GenerationContext::from_anchor(
            &self.namespace,
            anchor.name(),
            anchor.uid(),
            anchor.state(),
            names,
        )
        .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
        let probe = DesiredGeneration::build(context.clone(), profile.clone(), [0_u8; 32])
            .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
        let secret_name = probe
            .registration_secret()
            .metadata
            .name
            .as_deref()
            .ok_or(RegistrationProvisionerError::InvalidGeneration)?;
        let Some(secret) = self
            .registration_secrets
            .get_opt(secret_name)
            .await
            .map_err(|_| {
                registration_api_error(RegistrationOperation::ObserveBootstrapSecretPresence)
            })?
        else {
            return Ok(BootstrapPresence::Absent);
        };
        let desired = desired_from_observed_secret(context, profile.clone(), &secret)
            .map_err(map_generation_registration_error)?;
        let secret_uid = registration_metadata(&secret, "uid")?;
        registration_metadata(&secret, "resourceVersion")?;
        let preflight = self
            .preflight_before_pod(&desired, Some(pod_uid), None)
            .await
            .map_err(map_generation_registration_error)?;
        if preflight.children.registration_secret != secret_uid {
            return Err(RegistrationProvisionerError::ResourceRejected);
        }
        Ok(BootstrapPresence::Present)
    }
}

fn validate_registration_anchor<'a>(
    anchor: &'a StoredAnchor,
    profile: &MvpWorkerProfile,
    scope_id: ScopeId,
    namespace: &str,
    expected_binding: &SessionBinding,
) -> Result<&'a str, RegistrationProvisionerError> {
    if anchor.namespace() != namespace
        || anchor.state().scope_id() != scope_id
        || anchor.state().phase() != SessionPhase::Provisioning
        || anchor.state().profile() != profile.profile()
    {
        return Err(RegistrationProvisionerError::InvalidGeneration);
    }
    let actual_binding = SessionBinding::new(
        anchor.state().scope_id(),
        anchor.state().session_id(),
        anchor.state().fence().clone(),
        anchor.state().incarnation_id(),
    )
    .map_err(|_| RegistrationProvisionerError::InvalidGeneration)?;
    if &actual_binding != expected_binding {
        return Err(RegistrationProvisionerError::InvalidGeneration);
    }
    anchor
        .state()
        .pod_uid()
        .filter(|uid| is_printable_identifier(uid))
        .ok_or(RegistrationProvisionerError::InvalidGeneration)
}

fn registration_token(secret: &Secret) -> Result<[u8; 32], RegistrationProvisionerError> {
    secret
        .data
        .as_ref()
        .and_then(|data| data.get("token"))
        .and_then(|token| <[u8; 32]>::try_from(token.0.as_slice()).ok())
        .ok_or(RegistrationProvisionerError::InvalidBootstrapToken)
}

fn registration_metadata<'a>(
    secret: &'a Secret,
    field: &'static str,
) -> Result<&'a str, RegistrationProvisionerError> {
    let value = match field {
        "uid" => secret.metadata.uid.as_deref(),
        "resourceVersion" => secret.metadata.resource_version.as_deref(),
        _ => None,
    };
    value
        .filter(|value| is_printable_identifier(value))
        .ok_or(RegistrationProvisionerError::ResourceRejected)
}

fn map_generation_registration_error(
    error: GenerationProvisionerError,
) -> RegistrationProvisionerError {
    match error {
        GenerationProvisionerError::InvalidGeneration
        | GenerationProvisionerError::InvalidPodUid
        | GenerationProvisionerError::RandomnessUnavailable => {
            RegistrationProvisionerError::InvalidGeneration
        }
        GenerationProvisionerError::InvalidBootstrapToken => {
            RegistrationProvisionerError::InvalidBootstrapToken
        }
        GenerationProvisionerError::BootstrapCredentialConsumedOrMissing => {
            RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing
        }
        GenerationProvisionerError::KubernetesApi { .. } => {
            registration_api_error(RegistrationOperation::VerifyResources)
        }
        GenerationProvisionerError::ChildrenPresent
        | GenerationProvisionerError::ChildrenAmbiguous
        | GenerationProvisionerError::ResourceRejected { .. } => {
            RegistrationProvisionerError::ResourceRejected
        }
    }
}

fn registration_api_error(operation: RegistrationOperation) -> RegistrationProvisionerError {
    RegistrationProvisionerError::KubernetesApi { operation }
}

fn require_exact_inventory<'a, K, I, F, E>(
    objects: I,
    expected_name: &str,
    expected_uid: Option<&str>,
    resource: GenerationResource,
    validate: F,
) -> Result<(), GenerationProvisionerError>
where
    I: IntoIterator<Item = &'a K>,
    K: 'a + kube::Resource,
    F: Fn(&K) -> Result<(), E>,
{
    let mut count = 0_usize;
    for object in objects {
        count += 1;
        if object.meta().name.as_deref() != Some(expected_name)
            || object.meta().uid.as_deref() != expected_uid
            || validate(object).is_err()
        {
            return Err(GenerationProvisionerError::ResourceRejected { resource });
        }
    }
    if count != usize::from(expected_uid.is_some()) {
        return Err(GenerationProvisionerError::ResourceRejected { resource });
    }
    Ok(())
}

fn required_uid<K>(
    object: &K,
    resource: GenerationResource,
) -> Result<&str, GenerationProvisionerError>
where
    K: kube::Resource,
{
    object
        .meta()
        .uid
        .as_deref()
        .ok_or(GenerationProvisionerError::ResourceRejected { resource })
}

fn observed_worker(pod: Pod) -> Result<ObservedWorker, GenerationProvisionerError> {
    let pod_uid = pod
        .metadata
        .uid
        .ok_or(GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::Pod,
        })?;
    ObservedWorker::new(pod_uid)
}

fn desired_from_observed_secret(
    context: GenerationContext,
    profile: MvpWorkerProfile,
    observed: &Secret,
) -> Result<DesiredGeneration, GenerationProvisionerError> {
    let token = observed
        .data
        .as_ref()
        .and_then(|data| data.get("token"))
        .and_then(|token| <[u8; 32]>::try_from(token.0.as_slice()).ok())
        .ok_or(GenerationProvisionerError::InvalidBootstrapToken)?;
    let adopted = DesiredGeneration::build(context, profile, token)
        .map_err(|_| GenerationProvisionerError::InvalidGeneration)?;
    adopted
        .validate_registration_secret(observed)
        .map_err(|_| GenerationProvisionerError::ResourceRejected {
            resource: GenerationResource::RegistrationSecret,
        })?;
    Ok(adopted)
}

fn required_name(object: &PersistentVolumeClaim) -> Result<&str, GenerationProvisionerError> {
    object
        .metadata
        .name
        .as_deref()
        .ok_or(GenerationProvisionerError::InvalidGeneration)
}

fn ensure_api_error() -> GenerationProvisionerError {
    GenerationProvisionerError::KubernetesApi {
        operation: ProvisionerOperation::EnsureGeneration,
    }
}

fn validate_discovered_metadata(
    metadata: &ObjectMeta,
    namespace: &str,
    scope_id: ScopeId,
    session_id: SessionId,
    kind: ChildKind,
) -> Result<(), GenerationProvisionerError> {
    let ambiguous = || GenerationProvisionerError::ChildrenAmbiguous;
    if metadata.namespace.as_deref() != Some(namespace)
        || metadata.deletion_timestamp.is_some()
        || !metadata.uid.as_deref().is_some_and(is_printable_identifier)
        || !metadata
            .resource_version
            .as_deref()
            .is_some_and(is_printable_identifier)
    {
        return Err(ambiguous());
    }

    let labels = metadata.labels.as_ref().ok_or_else(ambiguous)?;
    let annotations = metadata.annotations.as_ref().ok_or_else(ambiguous)?;
    let names = ResourceNames::new(session_id);
    let generation = if matches!(kind, ChildKind::PersistentVolumeClaim) {
        if labels.contains_key(GENERATION_LABEL)
            || annotations.contains_key(GENERATION_ANNOTATION)
            || annotations.contains_key(ATTEMPT_ANNOTATION)
        {
            return Err(ambiguous());
        }
        None
    } else {
        Some(
            annotations
                .get(GENERATION_ANNOTATION)
                .and_then(|value| parse_generation(value))
                .ok_or_else(ambiguous)?,
        )
    };
    let expected_name = kind
        .expected_name(names, generation.unwrap_or(1))
        .ok_or_else(ambiguous)?;
    if metadata.name.as_deref() != Some(expected_name.as_str())
        || labels.get(MANAGED_BY_LABEL).map(String::as_str) != Some(MANAGED_BY_VALUE)
        || labels.get(RESOURCE_LABEL).map(String::as_str) != Some(kind.resource_label())
        || labels.get(SESSION_LABEL).map(String::as_str) != Some(&session_id.as_hex()[..40])
        || annotations.get(SCOPE_ANNOTATION).map(String::as_str) != Some(scope_id.as_hex().as_str())
        || annotations.get(SESSION_ANNOTATION).map(String::as_str)
            != Some(session_id.as_hex().as_str())
        || annotations
            .get(INCARNATION_ANNOTATION)
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_none()
    {
        return Err(ambiguous());
    }
    if let Some(generation) = generation {
        if labels.get(GENERATION_LABEL).map(String::as_str) != Some(generation.to_string().as_str())
            || annotations
                .get(ATTEMPT_ANNOTATION)
                .and_then(|value| Uuid::parse_str(value).ok())
                .is_none()
        {
            return Err(ambiguous());
        }
    }

    let anchor_name = annotations
        .get(ANCHOR_NAME_ANNOTATION)
        .filter(|name| name.as_str() == names.anchor())
        .ok_or_else(ambiguous)?;
    let anchor_uid = annotations
        .get(ANCHOR_UID_ANNOTATION)
        .filter(|uid| is_printable_identifier(uid))
        .ok_or_else(ambiguous)?;
    let owners = metadata.owner_references.as_ref().ok_or_else(ambiguous)?;
    if owners.len() != 1 {
        return Err(ambiguous());
    }
    let owner = &owners[0];
    if owner.api_version != "v1"
        || owner.kind != "ConfigMap"
        || owner.name != *anchor_name
        || owner.uid != *anchor_uid
        || owner.controller != Some(true)
        || owner.block_owner_deletion != Some(true)
    {
        return Err(ambiguous());
    }
    Ok(())
}

fn parse_generation(value: &str) -> Option<u64> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return None;
    }
    value.parse().ok().filter(|generation| *generation > 0)
}

fn proof_api_error(_error: kube::Error) -> GenerationProvisionerError {
    GenerationProvisionerError::KubernetesApi {
        operation: ProvisionerOperation::ProveChildrenAbsent,
    }
}

fn is_printable_identifier(value: &str) -> bool {
    (1..=256).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/' && byte != b'\\')
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_rejects_same_name_resource_recreated_between_reads() {
        let mut listed = PersistentVolumeClaim::default();
        listed.metadata.name = Some("workspace".into());
        listed.metadata.uid = Some("new-uid".into());

        assert_eq!(
            require_exact_inventory(
                [&listed],
                "workspace",
                Some("observed-uid"),
                GenerationResource::PersistentVolumeClaim,
                |_| Ok::<_, ()>(()),
            ),
            Err(GenerationProvisionerError::ResourceRejected {
                resource: GenerationResource::PersistentVolumeClaim,
            })
        );
    }
}
