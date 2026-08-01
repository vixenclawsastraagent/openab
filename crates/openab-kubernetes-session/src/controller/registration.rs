use super::SessionLocks;
use crate::bridge::SessionBinding;
use crate::identity::SessionId;
use crate::resources::{MvpWorkerProfile, ResourceValidationError};
use crate::state::{ProfileRef, SessionAnchorV1, SessionPhase};
use crate::store::{AnchorStoreError, ConfigMapAnchorStore, StoredAnchor};
use crate::wire::{FatalCode, WireProtocolError, WorkerRegistrationV1};
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

const BOOTSTRAP_TOKEN_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationOperation {
    VerifyResources,
    DeleteBootstrapSecret,
    ObserveBootstrapSecretDeletion,
    ObserveBootstrapSecretPresence,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RegistrationProvisionerError {
    #[error("the lifecycle anchor cannot identify this worker generation")]
    InvalidGeneration,
    #[error("the worker generation resource set is not exact")]
    ResourceRejected,
    #[error("the immutable bootstrap Secret does not contain one 32-byte token")]
    InvalidBootstrapToken,
    #[error("the presented bootstrap credential is unauthorized")]
    Unauthorized,
    #[error("the bootstrap credential was consumed or is missing")]
    BootstrapCredentialConsumedOrMissing,
    #[error("bootstrap Secret deletion was not authoritatively observed")]
    BootstrapDeletionNotObserved,
    #[error("Kubernetes API failed during {operation:?}")]
    KubernetesApi { operation: RegistrationOperation },
    #[error("the registration proof is malformed")]
    InvalidProof,
    #[error("worker resource validation failed")]
    ResourceValidation,
}

impl From<ResourceValidationError> for RegistrationProvisionerError {
    fn from(_value: ResourceValidationError) -> Self {
        Self::ResourceValidation
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerBootstrapAuthError {
    #[error("worker Pod UID must be a non-empty printable identifier")]
    InvalidPodUid,
    #[error("bootstrap token must be exactly 32 bytes")]
    InvalidTokenLength,
}

/// Transport-authenticated worker bootstrap material.
///
/// The token is deliberately absent from the JSON wire protocol and is never
/// included in `Debug` or an error. The relay must construct this value from a
/// bounded authenticated handshake before forwarding a registration message.
pub struct WorkerBootstrapAuth {
    pod_uid: String,
    token: [u8; BOOTSTRAP_TOKEN_BYTES],
}

impl WorkerBootstrapAuth {
    pub fn new(pod_uid: impl Into<String>, token: &[u8]) -> Result<Self, WorkerBootstrapAuthError> {
        let pod_uid = pod_uid.into();
        if !is_printable_identifier(&pod_uid) {
            return Err(WorkerBootstrapAuthError::InvalidPodUid);
        }
        let token = <[u8; BOOTSTRAP_TOKEN_BYTES]>::try_from(token)
            .map_err(|_| WorkerBootstrapAuthError::InvalidTokenLength)?;
        Ok(Self { pod_uid, token })
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub(crate) fn token(&self) -> &[u8; BOOTSTRAP_TOKEN_BYTES] {
        &self.token
    }
}

impl fmt::Debug for WorkerBootstrapAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerBootstrapAuth")
            .field("pod_uid", &self.pod_uid)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl Drop for WorkerBootstrapAuth {
    fn drop(&mut self) {
        self.token.fill(0);
    }
}

/// Exact, non-secret resource observation produced only after authentication.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedBootstrap {
    binding: SessionBinding,
    anchor_uid: String,
    pod_uid: String,
    secret_name: String,
    secret_uid: String,
    secret_resource_version: String,
}

impl VerifiedBootstrap {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: SessionBinding,
        anchor_uid: impl Into<String>,
        pod_uid: impl Into<String>,
        secret_name: impl Into<String>,
        secret_uid: impl Into<String>,
        secret_resource_version: impl Into<String>,
    ) -> Result<Self, RegistrationProvisionerError> {
        let proof = Self {
            binding,
            anchor_uid: anchor_uid.into(),
            pod_uid: pod_uid.into(),
            secret_name: secret_name.into(),
            secret_uid: secret_uid.into(),
            secret_resource_version: secret_resource_version.into(),
        };
        if !is_printable_identifier(&proof.anchor_uid)
            || !is_printable_identifier(&proof.pod_uid)
            || !is_printable_identifier(&proof.secret_name)
            || !is_printable_identifier(&proof.secret_uid)
            || !is_printable_identifier(&proof.secret_resource_version)
        {
            return Err(RegistrationProvisionerError::InvalidProof);
        }
        Ok(proof)
    }

    pub fn binding(&self) -> &SessionBinding {
        &self.binding
    }

    pub fn anchor_uid(&self) -> &str {
        &self.anchor_uid
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub(crate) fn secret_name(&self) -> &str {
        &self.secret_name
    }

    pub(crate) fn secret_uid(&self) -> &str {
        &self.secret_uid
    }

    pub(crate) fn secret_resource_version(&self) -> &str {
        &self.secret_resource_version
    }

    /// Convert a trusted provisioner's proof after it has observed the
    /// bootstrap Secret as absent. Implementations must not call this merely
    /// because the DELETE request returned successfully.
    pub fn into_consumed(self) -> ConsumedBootstrap {
        ConsumedBootstrap {
            binding: self.binding,
            anchor_uid: self.anchor_uid,
            pod_uid: self.pod_uid,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumedBootstrap {
    binding: SessionBinding,
    anchor_uid: String,
    pod_uid: String,
}

impl ConsumedBootstrap {
    pub fn binding(&self) -> &SessionBinding {
        &self.binding
    }

    pub fn anchor_uid(&self) -> &str {
        &self.anchor_uid
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapPresence {
    Present,
    Absent,
}

#[async_trait]
pub trait RegistrationProvisioner: Send + Sync {
    async fn verify_bootstrap(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
        expected_binding: &SessionBinding,
        auth: &WorkerBootstrapAuth,
    ) -> Result<VerifiedBootstrap, RegistrationProvisionerError>;

    async fn consume_bootstrap(
        &self,
        verified: VerifiedBootstrap,
    ) -> Result<ConsumedBootstrap, RegistrationProvisionerError>;

    async fn observe_bootstrap_presence(
        &self,
        anchor: &StoredAnchor,
        profile: &MvpWorkerProfile,
    ) -> Result<BootstrapPresence, RegistrationProvisionerError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredWorker {
    binding: SessionBinding,
    profile: ProfileRef,
    pod_uid: String,
}

impl RegisteredWorker {
    pub fn binding(&self) -> &SessionBinding {
        &self.binding
    }

    pub fn profile(&self) -> &ProfileRef {
        &self.profile
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationRecovery {
    AwaitingRegistration,
    RecycleRequired {
        binding: SessionBinding,
        pod_uid: String,
    },
    NotApplicable {
        phase: SessionPhase,
    },
}

#[derive(Debug, Error)]
pub enum RegistrationError {
    #[error("the target session anchor was not found")]
    AnchorNotFound,
    #[error("the durable anchor profile does not match trusted controller configuration")]
    ProfileMismatch,
    #[error("registration requires a Provisioning anchor, got {phase:?}")]
    InvalidPhase { phase: SessionPhase },
    #[error("the Provisioning anchor has no observed worker Pod UID")]
    MissingPodUid,
    #[error("the presented worker Pod UID does not match the durable anchor")]
    PodUidMismatch,
    #[error("the durable anchor cannot form a worker binding")]
    InvalidAnchor,
    #[error("worker registration binding is stale or invalid")]
    Binding(#[from] WireProtocolError),
    #[error("worker registration resource verification failed")]
    Provisioner(#[from] RegistrationProvisionerError),
    #[error("worker registration proof did not correlate with the durable anchor")]
    ProofMismatch,
    #[error("the lifecycle anchor changed during worker registration")]
    AnchorChanged,
    #[error("bootstrap was consumed but Ready could not be persisted; recycle is required")]
    ReadyPersistenceFailed,
    #[error("the worker generation is blocked and must be recycled with a fresh attempt")]
    RecycleRequired,
    #[error("anchor persistence failed")]
    Store(#[from] AnchorStoreError),
}

impl RegistrationError {
    /// Stable transport-facing classification with no nested error details.
    pub fn fatal_code(&self) -> FatalCode {
        match self {
            Self::PodUidMismatch
            | Self::Provisioner(RegistrationProvisionerError::Unauthorized) => {
                FatalCode::Unauthorized
            }
            Self::AnchorNotFound
            | Self::ProfileMismatch
            | Self::InvalidPhase { .. }
            | Self::MissingPodUid
            | Self::Binding(_)
            | Self::AnchorChanged
            | Self::RecycleRequired
            | Self::Provisioner(
                RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing,
            ) => FatalCode::StaleBinding,
            Self::Provisioner(RegistrationProvisionerError::KubernetesApi { .. })
            | Self::Provisioner(RegistrationProvisionerError::BootstrapDeletionNotObserved)
            | Self::ReadyPersistenceFailed
            | Self::Store(_) => FatalCode::Unavailable,
            Self::InvalidAnchor
            | Self::ProofMismatch
            | Self::Provisioner(RegistrationProvisionerError::InvalidGeneration)
            | Self::Provisioner(RegistrationProvisionerError::ResourceRejected)
            | Self::Provisioner(RegistrationProvisionerError::InvalidBootstrapToken)
            | Self::Provisioner(RegistrationProvisionerError::InvalidProof)
            | Self::Provisioner(RegistrationProvisionerError::ResourceValidation) => {
                FatalCode::Internal
            }
        }
    }
}

/// Serializes, authenticates, and durably accepts worker registration.
pub struct RegistrationCoordinator {
    store: ConfigMapAnchorStore,
    locks: SessionLocks,
    profile: MvpWorkerProfile,
    provisioner: Arc<dyn RegistrationProvisioner>,
}

impl RegistrationCoordinator {
    pub fn new(
        store: ConfigMapAnchorStore,
        locks: SessionLocks,
        profile: MvpWorkerProfile,
        provisioner: Arc<dyn RegistrationProvisioner>,
    ) -> Self {
        Self {
            store,
            locks,
            profile,
            provisioner,
        }
    }

    pub async fn register(
        &self,
        target_session_id: SessionId,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RegisteredWorker, RegistrationError> {
        let _guard = self.locks.lock(target_session_id).await;
        let observed = self
            .store
            .get(target_session_id)
            .await?
            .ok_or(RegistrationError::AnchorNotFound)?;
        let (expected_binding, recorded_pod_uid) = self.registration_target(&observed)?;
        if auth.pod_uid() != recorded_pod_uid {
            return Err(RegistrationError::PodUidMismatch);
        }
        registration.into_validated_binding(&expected_binding)?;

        let verified = match self
            .provisioner
            .verify_bootstrap(&observed, &self.profile, &expected_binding, &auth)
            .await
        {
            Ok(verified) => verified,
            Err(RegistrationProvisionerError::BootstrapCredentialConsumedOrMissing) => {
                self.block_for_recycle(observed).await?;
                return Err(RegistrationError::RecycleRequired);
            }
            Err(error) => return Err(error.into()),
        };
        self.validate_verified(
            &verified,
            &expected_binding,
            observed.uid(),
            &recorded_pod_uid,
        )?;

        // The resource proof may involve several API calls. Re-read the
        // anchor before consuming the one-time credential so stale work never
        // deletes a newer generation's Secret.
        let current = self
            .store
            .get(target_session_id)
            .await?
            .ok_or(RegistrationError::AnchorChanged)?;
        if !same_registration_anchor(&observed, &current) {
            return Err(RegistrationError::AnchorChanged);
        }

        let consumed = self.provisioner.consume_bootstrap(verified).await?;
        self.validate_consumed(
            &consumed,
            &expected_binding,
            current.uid(),
            &recorded_pod_uid,
        )?;

        let mut ready = current.state().clone();
        let fence = ready.fence().clone();
        ready
            .transition(&fence, SessionPhase::Ready)
            .map_err(|_| RegistrationError::InvalidAnchor)?;
        let ready = self.persist_ready_or_recover(current, ready).await?;
        Ok(RegisteredWorker {
            binding: expected_binding,
            profile: ready.state().profile().clone(),
            pod_uid: recorded_pod_uid,
        })
    }

    pub async fn recover_incomplete(
        &self,
        session_id: SessionId,
    ) -> Result<RegistrationRecovery, RegistrationError> {
        let _guard = self.locks.lock(session_id).await;
        let observed = self
            .store
            .get(session_id)
            .await?
            .ok_or(RegistrationError::AnchorNotFound)?;
        if observed.state().profile() != self.profile.profile() {
            return Err(RegistrationError::ProfileMismatch);
        }
        if observed.state().phase() != SessionPhase::Provisioning {
            return Ok(RegistrationRecovery::NotApplicable {
                phase: observed.state().phase(),
            });
        }
        self.registration_target(&observed)?;
        let presence = match self
            .provisioner
            .observe_bootstrap_presence(&observed, &self.profile)
            .await
        {
            Ok(presence) => presence,
            Err(error) if error.invalidates_generation() => {
                return self.block_for_recycle(observed).await;
            }
            Err(error) => return Err(error.into()),
        };
        match presence {
            BootstrapPresence::Present => Ok(RegistrationRecovery::AwaitingRegistration),
            BootstrapPresence::Absent => self.block_for_recycle(observed).await,
        }
    }

    fn registration_target(
        &self,
        anchor: &StoredAnchor,
    ) -> Result<(SessionBinding, String), RegistrationError> {
        if anchor.state().profile() != self.profile.profile() {
            return Err(RegistrationError::ProfileMismatch);
        }
        if anchor.state().phase() != SessionPhase::Provisioning {
            return Err(RegistrationError::InvalidPhase {
                phase: anchor.state().phase(),
            });
        }
        let pod_uid = anchor
            .state()
            .pod_uid()
            .ok_or(RegistrationError::MissingPodUid)?;
        let binding = SessionBinding::new(
            anchor.state().scope_id(),
            anchor.state().session_id(),
            anchor.state().fence().clone(),
            anchor.state().incarnation_id(),
        )
        .map_err(|_| RegistrationError::InvalidAnchor)?;
        Ok((binding, pod_uid.to_owned()))
    }

    fn validate_verified(
        &self,
        verified: &VerifiedBootstrap,
        expected_binding: &SessionBinding,
        anchor_uid: &str,
        pod_uid: &str,
    ) -> Result<(), RegistrationError> {
        if verified.binding() != expected_binding
            || verified.anchor_uid() != anchor_uid
            || verified.pod_uid() != pod_uid
        {
            return Err(RegistrationError::ProofMismatch);
        }
        Ok(())
    }

    fn validate_consumed(
        &self,
        consumed: &ConsumedBootstrap,
        expected_binding: &SessionBinding,
        anchor_uid: &str,
        pod_uid: &str,
    ) -> Result<(), RegistrationError> {
        if consumed.binding() != expected_binding
            || consumed.anchor_uid() != anchor_uid
            || consumed.pod_uid() != pod_uid
        {
            return Err(RegistrationError::ProofMismatch);
        }
        Ok(())
    }

    async fn persist_ready_or_recover(
        &self,
        current: StoredAnchor,
        ready: SessionAnchorV1,
    ) -> Result<StoredAnchor, RegistrationError> {
        match self.store.replace(&current, &ready).await {
            Ok(stored) => Ok(stored),
            Err(original_error) => match self.store.get(current.state().session_id()).await {
                Ok(Some(observed))
                    if observed.uid() == current.uid() && observed.state() == &ready =>
                {
                    Ok(observed)
                }
                Ok(Some(observed)) if same_registration_anchor(&current, &observed) => {
                    self.block_for_recycle(observed).await?;
                    Err(RegistrationError::ReadyPersistenceFailed)
                }
                _ => Err(RegistrationError::Store(original_error)),
            },
        }
    }

    async fn block_for_recycle(
        &self,
        observed: StoredAnchor,
    ) -> Result<RegistrationRecovery, RegistrationError> {
        let (binding, pod_uid) = self.registration_target(&observed)?;
        let mut blocked = observed.state().clone();
        let fence = blocked.fence().clone();
        blocked
            .transition(&fence, SessionPhase::Blocked)
            .map_err(|_| RegistrationError::InvalidAnchor)?;
        self.store.replace(&observed, &blocked).await?;
        Ok(RegistrationRecovery::RecycleRequired { binding, pod_uid })
    }
}

impl RegistrationProvisionerError {
    fn invalidates_generation(&self) -> bool {
        matches!(
            self,
            Self::InvalidGeneration
                | Self::ResourceRejected
                | Self::InvalidBootstrapToken
                | Self::BootstrapCredentialConsumedOrMissing
                | Self::InvalidProof
                | Self::ResourceValidation
        )
    }
}

fn same_registration_anchor(first: &StoredAnchor, second: &StoredAnchor) -> bool {
    first.uid() == second.uid()
        && first.resource_version() == second.resource_version()
        && first.name() == second.name()
        && first.namespace() == second.namespace()
        && first.state() == second.state()
}

fn is_printable_identifier(value: &str) -> bool {
    (1..=256).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/' && byte != b'\\')
}
