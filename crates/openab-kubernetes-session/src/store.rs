use crate::identity::{ResourceNames, ScopeId, SessionId};
use crate::state::{SessionAnchorV1, SessionPhase, StateError};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, PostParams, Preconditions};
use kube::{Api, Client};
use std::collections::BTreeMap;
use thiserror::Error;

const ANCHOR_DATA_KEY: &str = "anchor.json";
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const MANAGED_BY_VALUE: &str = "openab-session-controller";
const RESOURCE_LABEL: &str = "openab.dev/resource";
const RESOURCE_VALUE: &str = "session-anchor";
const MAX_ANCHOR_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    Create,
    Get,
    ObserveDeletion,
    Replace,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRecovery {
    GuardedDeleteRequested,
    GuardedDeleteObservedAbsent,
}

#[derive(Debug, Error)]
pub enum AnchorStoreError {
    #[error("invalid Kubernetes namespace {namespace:?}")]
    InvalidNamespace { namespace: String },
    #[error("anchor scope does not match the scope bound to this store")]
    ScopeMismatch { expected: ScopeId, actual: ScopeId },
    #[error("anchor session identity does not match the requested resource")]
    SessionMismatch {
        expected: SessionId,
        actual: SessionId,
    },
    #[error("immutable anchor field {field} changed")]
    ImmutableFieldChanged { field: &'static str },
    #[error("replacement is not a valid anchor state successor")]
    InvalidSuccessor {
        #[source]
        source: Box<StateError>,
    },
    #[error("malformed ConfigMap {name}: {reason}")]
    MalformedObject { name: String, reason: String },
    #[error("serialized anchor is {bytes} bytes; maximum is {maximum}")]
    AnchorTooLarge { bytes: usize, maximum: usize },
    #[error("ConfigMap {name} already exists")]
    AlreadyExists { name: String },
    #[error("ConfigMap {name} was not found during {operation:?}")]
    NotFound {
        operation: StoreOperation,
        name: String,
    },
    #[error("ConfigMap {name} changed concurrently during {operation:?}")]
    Conflict {
        operation: StoreOperation,
        name: String,
    },
    #[error("anchor deletion requires phase Deleting, but the observed phase is {actual:?}")]
    DeletePhaseNotDeleting { actual: SessionPhase },
    #[error("anchor deletion requires the worker Pod to be absent, but Pod {pod_uid} remains")]
    DeletePodStillPresent { pod_uid: String },
    #[error("expected anchor UID must not be empty")]
    InvalidExpectedUid,
    #[error("could not encode anchor state")]
    Encode(#[source] serde_json::Error),
    #[error(
        "Kubernetes {operation:?} response for ConfigMap {name} was rejected; \
         recovery status {recovery:?}: {rejection}"
    )]
    WriteResponseRejected {
        operation: StoreOperation,
        name: String,
        recovery: WriteRecovery,
        rejection: Box<AnchorStoreError>,
    },
    #[error(
        "Kubernetes {operation:?} response for ConfigMap {name} was rejected and guarded \
         recovery failed: {recovery_error}; original rejection: {rejection}"
    )]
    WriteRecoveryFailed {
        operation: StoreOperation,
        name: String,
        rejection: Box<AnchorStoreError>,
        recovery_error: Box<AnchorStoreError>,
    },
    #[error("Kubernetes {operation:?} failed for ConfigMap {name}")]
    Kubernetes {
        operation: StoreOperation,
        name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredAnchor {
    state: SessionAnchorV1,
    object: ConfigMap,
    name: String,
    namespace: String,
    uid: String,
    resource_version: String,
}

impl StoredAnchor {
    pub fn state(&self) -> &SessionAnchorV1 {
        &self.state
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn resource_version(&self) -> &str {
        &self.resource_version
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Requested,
    AlreadyAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorDeletionObservation {
    Absent,
    Present,
    Terminating,
}

#[derive(Clone)]
pub struct ConfigMapAnchorStore {
    api: Api<ConfigMap>,
    namespace: String,
    scope_id: ScopeId,
}

impl ConfigMapAnchorStore {
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        scope_id: ScopeId,
    ) -> Result<Self, AnchorStoreError> {
        let namespace = namespace.into();
        if !is_dns_label(&namespace) {
            return Err(AnchorStoreError::InvalidNamespace { namespace });
        }
        Ok(Self {
            api: Api::namespaced(client, &namespace),
            namespace,
            scope_id,
        })
    }

    /// The deployment scope that every anchor read and write is bound to.
    pub fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    pub async fn create(&self, state: &SessionAnchorV1) -> Result<StoredAnchor, AnchorStoreError> {
        self.validate_scope(state)?;
        let name = ResourceNames::new(state.session_id()).anchor();
        let object = self.config_map_for(state, &name)?;
        let created = self
            .api
            .create(&PostParams::default(), &object)
            .await
            .map_err(|error| map_api_error(error, StoreOperation::Create, &name))?;
        match self.validate_write_response(created.clone(), state, None) {
            Ok(stored) => Ok(stored),
            Err(rejection) => Err(self
                .recover_rejected_create(&name, &created, rejection)
                .await),
        }
    }

    pub async fn get(
        &self,
        session_id: SessionId,
    ) -> Result<Option<StoredAnchor>, AnchorStoreError> {
        let name = ResourceNames::new(session_id).anchor();
        let object = self
            .api
            .get_opt(&name)
            .await
            .map_err(|error| map_api_error(error, StoreOperation::Get, &name))?;
        object
            .map(|object| self.decode(object, session_id))
            .transpose()
    }

    pub async fn observe_deletion(
        &self,
        session_id: SessionId,
        expected_uid: &str,
    ) -> Result<AnchorDeletionObservation, AnchorStoreError> {
        if expected_uid.trim().is_empty() {
            return Err(AnchorStoreError::InvalidExpectedUid);
        }
        let name = ResourceNames::new(session_id).anchor();
        let object = self
            .api
            .get_opt(&name)
            .await
            .map_err(|error| map_api_error(error, StoreOperation::ObserveDeletion, &name))?;
        let Some(object) = object else {
            return Ok(AnchorDeletionObservation::Absent);
        };

        self.validate_deletion_observation(object, session_id, expected_uid)
    }

    pub async fn replace(
        &self,
        observed: &StoredAnchor,
        next: &SessionAnchorV1,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        self.validate_observation(observed)?;
        self.validate_replacement(observed.state(), next)?;

        let name = observed.name.clone();
        let mut object = observed.object.clone();
        object.data = Some(self.anchor_data(next)?);
        object.binary_data = None;
        object.immutable = None;
        object.metadata.name = Some(name.clone());
        object.metadata.namespace = Some(self.namespace.clone());
        object.metadata.uid = Some(observed.uid.clone());
        object.metadata.resource_version = Some(observed.resource_version.clone());
        ensure_labels(&mut object.metadata);

        let replaced = self
            .api
            .replace(&name, &PostParams::default(), &object)
            .await
            .map_err(|error| map_api_error(error, StoreOperation::Replace, &name))?;
        match self.validate_write_response(replaced.clone(), next, Some(&observed.uid)) {
            Ok(stored) => Ok(stored),
            Err(rejection) => {
                self.recover_rejected_replace(observed, next, &replaced, rejection)
                    .await
            }
        }
    }

    pub async fn delete(&self, observed: &StoredAnchor) -> Result<DeleteOutcome, AnchorStoreError> {
        self.validate_observation(observed)?;
        self.validate_delete_ready(observed.state())?;
        let name = observed.name.clone();
        let params = DeleteParams::default().preconditions(Preconditions {
            resource_version: Some(observed.resource_version.clone()),
            uid: Some(observed.uid.clone()),
        });

        match self.api.delete(&name, &params).await {
            Ok(_) => Ok(DeleteOutcome::Requested),
            Err(kube::Error::Api(status)) if status.is_not_found() => {
                Ok(DeleteOutcome::AlreadyAbsent)
            }
            Err(error) => Err(map_api_error(error, StoreOperation::Delete, &name)),
        }
    }

    fn config_map_for(
        &self,
        state: &SessionAnchorV1,
        name: &str,
    ) -> Result<ConfigMap, AnchorStoreError> {
        let mut metadata = ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(self.namespace.clone()),
            ..ObjectMeta::default()
        };
        ensure_labels(&mut metadata);
        Ok(ConfigMap {
            data: Some(self.anchor_data(state)?),
            metadata,
            ..ConfigMap::default()
        })
    }

    fn anchor_data(
        &self,
        state: &SessionAnchorV1,
    ) -> Result<BTreeMap<String, String>, AnchorStoreError> {
        let serialized = serde_json::to_string(state).map_err(AnchorStoreError::Encode)?;
        if serialized.len() > MAX_ANCHOR_BYTES {
            return Err(AnchorStoreError::AnchorTooLarge {
                bytes: serialized.len(),
                maximum: MAX_ANCHOR_BYTES,
            });
        }
        Ok(BTreeMap::from([(ANCHOR_DATA_KEY.to_string(), serialized)]))
    }

    fn validate_write_response(
        &self,
        object: ConfigMap,
        expected_state: &SessionAnchorV1,
        expected_uid: Option<&str>,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        let name = ResourceNames::new(expected_state.session_id()).anchor();
        let stored = self.decode(object, expected_state.session_id())?;
        if expected_uid.is_some_and(|uid| stored.uid != uid) {
            return Err(malformed(
                &name,
                "API response UID does not match the observed object",
            ));
        }
        if stored.state != *expected_state {
            return Err(malformed(&name, "API response changed the anchor state"));
        }
        Ok(stored)
    }

    fn validate_recovery_target_metadata(
        &self,
        object: &ConfigMap,
        expected_name: &str,
        expected_uid: Option<&str>,
    ) -> Result<(), AnchorStoreError> {
        if object.metadata.name.as_deref() != Some(expected_name) {
            return Err(malformed(
                expected_name,
                "recovery target metadata.name does not match",
            ));
        }
        if object.metadata.namespace.as_deref() != Some(self.namespace.as_str()) {
            return Err(malformed(
                expected_name,
                "recovery target metadata.namespace does not match",
            ));
        }
        if object.metadata.deletion_timestamp.is_some() {
            return Err(malformed(
                expected_name,
                "recovery target is already terminating",
            ));
        }
        if has_lifecycle_metadata(object) {
            return Err(unsafe_lifecycle_metadata(expected_name));
        }
        let uid = required_metadata(
            object.metadata.uid.as_deref(),
            expected_name,
            "metadata.uid",
        )?;
        if expected_uid.is_some_and(|expected| uid != expected) {
            return Err(malformed(
                expected_name,
                "recovery target UID does not match the observed object",
            ));
        }
        required_metadata(
            object.metadata.resource_version.as_deref(),
            expected_name,
            "metadata.resourceVersion",
        )?;
        Ok(())
    }

    fn validate_recovery_anchor_state(
        &self,
        object: &ConfigMap,
        intended: &SessionAnchorV1,
        name: &str,
    ) -> Result<(), AnchorStoreError> {
        let data = object
            .data
            .as_ref()
            .ok_or_else(|| malformed(name, "recovery target data is missing"))?;
        let serialized = data
            .get(ANCHOR_DATA_KEY)
            .ok_or_else(|| malformed(name, "recovery target anchor.json is missing"))?;
        if serialized.len() > MAX_ANCHOR_BYTES {
            return Err(AnchorStoreError::AnchorTooLarge {
                bytes: serialized.len(),
                maximum: MAX_ANCHOR_BYTES,
            });
        }
        let persisted: SessionAnchorV1 = serde_json::from_str(serialized).map_err(|error| {
            malformed(
                name,
                format!("recovery target anchor state is invalid: {error}"),
            )
        })?;
        if persisted != *intended {
            return Err(malformed(
                name,
                "recovery target anchor state differs from the intended successor",
            ));
        }
        Ok(())
    }

    async fn recover_rejected_create(
        &self,
        name: &str,
        created: &ConfigMap,
        rejection: AnchorStoreError,
    ) -> AnchorStoreError {
        let recovery = if has_lifecycle_metadata(created) {
            Err(unsafe_lifecycle_metadata(name))
        } else {
            self.guarded_delete_response(name, created).await
        };
        match recovery {
            Ok(outcome) => AnchorStoreError::WriteResponseRejected {
                operation: StoreOperation::Create,
                name: name.to_string(),
                recovery: match outcome {
                    DeleteOutcome::Requested => WriteRecovery::GuardedDeleteRequested,
                    DeleteOutcome::AlreadyAbsent => WriteRecovery::GuardedDeleteObservedAbsent,
                },
                rejection: Box::new(rejection),
            },
            Err(recovery_error) => AnchorStoreError::WriteRecoveryFailed {
                operation: StoreOperation::Create,
                name: name.to_string(),
                rejection: Box::new(rejection),
                recovery_error: Box::new(recovery_error),
            },
        }
    }

    async fn guarded_delete_response(
        &self,
        name: &str,
        object: &ConfigMap,
    ) -> Result<DeleteOutcome, AnchorStoreError> {
        self.validate_recovery_target_metadata(object, name, None)?;
        let uid = required_metadata(object.metadata.uid.as_deref(), name, "metadata.uid")?;
        let resource_version = required_metadata(
            object.metadata.resource_version.as_deref(),
            name,
            "metadata.resourceVersion",
        )?;
        let params = DeleteParams::default().preconditions(Preconditions {
            resource_version: Some(resource_version),
            uid: Some(uid),
        });
        match self.api.delete(name, &params).await {
            Ok(_) => Ok(DeleteOutcome::Requested),
            Err(kube::Error::Api(status)) if status.is_not_found() => {
                Ok(DeleteOutcome::AlreadyAbsent)
            }
            Err(error) => Err(map_api_error(error, StoreOperation::Delete, name)),
        }
    }

    async fn recover_rejected_replace(
        &self,
        observed: &StoredAnchor,
        intended: &SessionAnchorV1,
        rejected: &ConfigMap,
        rejection: AnchorStoreError,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        let recovery = self
            .repair_rejected_replace(observed, intended, rejected)
            .await;
        match recovery {
            Ok(stored) => Ok(stored),
            Err(recovery_error) => Err(AnchorStoreError::WriteRecoveryFailed {
                operation: StoreOperation::Replace,
                name: observed.name.clone(),
                rejection: Box::new(rejection),
                recovery_error: Box::new(recovery_error),
            }),
        }
    }

    async fn repair_rejected_replace(
        &self,
        observed: &StoredAnchor,
        intended: &SessionAnchorV1,
        rejected: &ConfigMap,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        self.validate_recovery_target_metadata(rejected, &observed.name, Some(&observed.uid))?;
        self.validate_recovery_anchor_state(rejected, intended, &observed.name)?;
        let response_resource_version = required_metadata(
            rejected.metadata.resource_version.as_deref(),
            &observed.name,
            "metadata.resourceVersion",
        )?;

        let mut repair = rejected.clone();
        repair.data = Some(self.anchor_data(intended)?);
        repair.binary_data = None;
        repair.immutable = None;
        repair.metadata.name = Some(observed.name.clone());
        repair.metadata.namespace = Some(self.namespace.clone());
        repair.metadata.uid = Some(observed.uid.clone());
        repair.metadata.resource_version = Some(response_resource_version);
        ensure_labels(&mut repair.metadata);

        let repaired = self
            .api
            .replace(&observed.name, &PostParams::default(), &repair)
            .await
            .map_err(|error| map_api_error(error, StoreOperation::Replace, &observed.name))?;
        self.validate_write_response(repaired, intended, Some(&observed.uid))
    }

    fn validate_deletion_observation(
        &self,
        object: ConfigMap,
        expected_session_id: SessionId,
        expected_uid: &str,
    ) -> Result<AnchorDeletionObservation, AnchorStoreError> {
        let expected_name = ResourceNames::new(expected_session_id).anchor();
        if object.metadata.name.as_deref() != Some(expected_name.as_str()) {
            return Err(malformed(&expected_name, "metadata.name does not match"));
        }
        if object.metadata.namespace.as_deref() != Some(self.namespace.as_str()) {
            return Err(malformed(
                &expected_name,
                "metadata.namespace does not match",
            ));
        }
        let uid = required_metadata(
            object.metadata.uid.as_deref(),
            &expected_name,
            "metadata.uid",
        )?;
        if uid != expected_uid {
            return Err(AnchorStoreError::Conflict {
                operation: StoreOperation::ObserveDeletion,
                name: expected_name,
            });
        }

        let terminating = object.metadata.deletion_timestamp.is_some();
        let stored = self.decode_with_lifecycle(object, expected_session_id, true)?;
        self.validate_delete_ready(stored.state())?;
        Ok(if terminating {
            AnchorDeletionObservation::Terminating
        } else {
            AnchorDeletionObservation::Present
        })
    }

    fn decode(
        &self,
        object: ConfigMap,
        expected_session_id: SessionId,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        self.decode_with_lifecycle(object, expected_session_id, false)
    }

    fn decode_with_lifecycle(
        &self,
        object: ConfigMap,
        expected_session_id: SessionId,
        allow_terminating: bool,
    ) -> Result<StoredAnchor, AnchorStoreError> {
        let expected_name = ResourceNames::new(expected_session_id).anchor();
        if object.metadata.name.as_deref() != Some(expected_name.as_str()) {
            return Err(malformed(&expected_name, "metadata.name does not match"));
        }
        if object.metadata.namespace.as_deref() != Some(self.namespace.as_str()) {
            return Err(malformed(
                &expected_name,
                "metadata.namespace does not match",
            ));
        }
        let terminating = object.metadata.deletion_timestamp.is_some();
        if terminating && !allow_terminating {
            return Err(malformed(&expected_name, "anchor is already terminating"));
        }
        if object
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|references| !references.is_empty())
        {
            return Err(malformed(
                &expected_name,
                "anchor ownerReferences must be empty",
            ));
        }
        if object
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|finalizers| !finalizers.is_empty())
            && !(allow_terminating && terminating)
        {
            return Err(malformed(
                &expected_name,
                "anchor finalizers must be empty in the v1 store",
            ));
        }
        validate_labels(&object.metadata, &expected_name)?;
        if object
            .binary_data
            .as_ref()
            .is_some_and(|data| !data.is_empty())
        {
            return Err(malformed(&expected_name, "binaryData must be empty"));
        }
        if object.immutable == Some(true) {
            return Err(malformed(&expected_name, "anchor must remain mutable"));
        }

        let data = object
            .data
            .as_ref()
            .ok_or_else(|| malformed(&expected_name, "data is missing"))?;
        if data.len() != 1 {
            return Err(malformed(
                &expected_name,
                "data must contain only anchor.json",
            ));
        }
        let serialized = data
            .get(ANCHOR_DATA_KEY)
            .ok_or_else(|| malformed(&expected_name, "anchor.json is missing"))?;
        if serialized.len() > MAX_ANCHOR_BYTES {
            return Err(AnchorStoreError::AnchorTooLarge {
                bytes: serialized.len(),
                maximum: MAX_ANCHOR_BYTES,
            });
        }
        let state: SessionAnchorV1 = serde_json::from_str(serialized)
            .map_err(|error| malformed(&expected_name, format!("invalid anchor state: {error}")))?;
        self.validate_scope(&state)?;
        if state.session_id() != expected_session_id {
            return Err(AnchorStoreError::SessionMismatch {
                expected: expected_session_id,
                actual: state.session_id(),
            });
        }

        let uid = required_metadata(
            object.metadata.uid.as_deref(),
            &expected_name,
            "metadata.uid",
        )?;
        let resource_version = required_metadata(
            object.metadata.resource_version.as_deref(),
            &expected_name,
            "metadata.resourceVersion",
        )?;

        Ok(StoredAnchor {
            state,
            object,
            name: expected_name,
            namespace: self.namespace.clone(),
            uid,
            resource_version,
        })
    }

    fn validate_delete_ready(&self, state: &SessionAnchorV1) -> Result<(), AnchorStoreError> {
        if state.phase() != SessionPhase::Deleting {
            return Err(AnchorStoreError::DeletePhaseNotDeleting {
                actual: state.phase(),
            });
        }
        if let Some(pod_uid) = state.pod_uid() {
            return Err(AnchorStoreError::DeletePodStillPresent {
                pod_uid: pod_uid.to_string(),
            });
        }
        Ok(())
    }

    fn validate_scope(&self, state: &SessionAnchorV1) -> Result<(), AnchorStoreError> {
        if state.scope_id() == self.scope_id {
            return Ok(());
        }
        Err(AnchorStoreError::ScopeMismatch {
            expected: self.scope_id,
            actual: state.scope_id(),
        })
    }

    fn validate_observation(&self, observed: &StoredAnchor) -> Result<(), AnchorStoreError> {
        self.validate_scope(observed.state())?;
        let expected_name = ResourceNames::new(observed.state().session_id()).anchor();
        if observed.name != expected_name
            || observed.namespace != self.namespace
            || observed.object.metadata.uid.as_deref() != Some(observed.uid.as_str())
            || observed.object.metadata.resource_version.as_deref()
                != Some(observed.resource_version.as_str())
        {
            return Err(malformed(
                &expected_name,
                "stored observation metadata is inconsistent",
            ));
        }
        Ok(())
    }

    fn validate_replacement(
        &self,
        current: &SessionAnchorV1,
        next: &SessionAnchorV1,
    ) -> Result<(), AnchorStoreError> {
        match current.validate_successor(next) {
            Ok(()) => Ok(()),
            Err(StateError::ImmutableAnchorFieldChanged { field }) => {
                Err(AnchorStoreError::ImmutableFieldChanged { field })
            }
            Err(source) => Err(AnchorStoreError::InvalidSuccessor {
                source: Box::new(source),
            }),
        }
    }
}

fn ensure_labels(metadata: &mut ObjectMeta) {
    let labels = metadata.labels.get_or_insert_with(BTreeMap::new);
    labels.insert(MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string());
    labels.insert(RESOURCE_LABEL.to_string(), RESOURCE_VALUE.to_string());
}

fn has_lifecycle_metadata(object: &ConfigMap) -> bool {
    object
        .metadata
        .owner_references
        .as_ref()
        .is_some_and(|references| !references.is_empty())
        || object
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|finalizers| !finalizers.is_empty())
}

fn unsafe_lifecycle_metadata(name: &str) -> AnchorStoreError {
    malformed(
        name,
        "recovery refuses to remove ownerReferences or finalizers not owned by this store",
    )
}

fn validate_labels(metadata: &ObjectMeta, name: &str) -> Result<(), AnchorStoreError> {
    let labels = metadata
        .labels
        .as_ref()
        .ok_or_else(|| malformed(name, "required labels are missing"))?;
    if labels.get(MANAGED_BY_LABEL).map(String::as_str) != Some(MANAGED_BY_VALUE)
        || labels.get(RESOURCE_LABEL).map(String::as_str) != Some(RESOURCE_VALUE)
    {
        return Err(malformed(name, "required labels do not match"));
    }
    Ok(())
}

fn required_metadata(
    value: Option<&str>,
    name: &str,
    field: &str,
) -> Result<String, AnchorStoreError> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| malformed(name, format!("{field} is missing or empty")))
}

fn malformed(name: impl Into<String>, reason: impl Into<String>) -> AnchorStoreError {
    AnchorStoreError::MalformedObject {
        name: name.into(),
        reason: reason.into(),
    }
}

fn map_api_error(error: kube::Error, operation: StoreOperation, name: &str) -> AnchorStoreError {
    match error {
        kube::Error::Api(status)
            if operation == StoreOperation::Create && status.is_already_exists() =>
        {
            AnchorStoreError::AlreadyExists {
                name: name.to_string(),
            }
        }
        kube::Error::Api(status) if status.is_conflict() => AnchorStoreError::Conflict {
            operation,
            name: name.to_string(),
        },
        kube::Error::Api(status) if status.is_not_found() => AnchorStoreError::NotFound {
            operation,
            name: name.to_string(),
        },
        source => AnchorStoreError::Kubernetes {
            operation,
            name: name.to_string(),
            source: Box::new(source),
        },
    }
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
