use super::{
    ActivationPreparation, ControllerService, ControllerServiceError, OrphanAuthority,
    OrphanAuthorityError, OrphanContainmentOutcome, PendingActivation, RegisteredWorker,
    RelayConnection, RelayConnectionLoss, RelayContainmentCompletion, RelayContainmentTicket,
    RelayPairingOutcome, RendezvousHealth, RendezvousInstallError, RendezvousRegistry,
    WorkerBootstrapAuth,
};
use crate::bridge::SessionBinding;
use crate::identity::{ScopeId, SessionId};
use crate::state::ProfileRef;
use crate::wire::{
    ActivationRequestV1, ControllerToBridgeV1, ControllerToWorkerV1, FatalCode, WireProtocolError,
    WorkerRegistrationV1,
};
use async_trait::async_trait;
use std::num::NonZeroUsize;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

/// Narrow controller facade consumed by the transport-neutral relay.
///
/// The production implementation delegates to [`ControllerService`]. The
/// trait keeps socket-state tests independent from Kubernetes API mocks and is
/// not an alternate persistence or lifecycle authority.
#[async_trait]
trait RelayController: Send + Sync {
    fn scope_id(&self) -> ScopeId;

    async fn activate(
        &self,
        request: &ActivationRequestV1,
    ) -> Result<ActivationPreparation, ControllerServiceError>;

    async fn register(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RegisteredWorker, ControllerServiceError>;

    async fn connection_lost(
        &self,
        authority: &OrphanAuthority,
    ) -> Result<OrphanContainmentOutcome, ControllerServiceError>;
}

#[async_trait]
impl RelayController for ControllerService {
    fn scope_id(&self) -> ScopeId {
        ControllerService::scope_id(self)
    }

    async fn activate(
        &self,
        request: &ActivationRequestV1,
    ) -> Result<ActivationPreparation, ControllerServiceError> {
        ControllerService::activate(self, request).await
    }

    async fn register(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RegisteredWorker, ControllerServiceError> {
        ControllerService::register(self, registration, auth).await
    }

    async fn connection_lost(
        &self,
        authority: &OrphanAuthority,
    ) -> Result<OrphanContainmentOutcome, ControllerServiceError> {
        ControllerService::connection_lost(self, authority).await
    }
}

#[derive(Debug, Error)]
pub enum RelayOpenError {
    #[error("controller operation failed")]
    Controller(#[source] ControllerServiceError),
    #[error("relay rendezvous rejected the connection")]
    Rendezvous(#[source] RendezvousInstallError),
    #[error("trusted relay authority is invalid")]
    Authority(#[source] OrphanAuthorityError),
    #[error("trusted activation response is invalid")]
    Wire(#[source] WireProtocolError),
    #[error("the peer relay lane closed during handshake")]
    PeerClosed,
    #[error("the controller-owned relay task stopped before returning a result")]
    TaskStopped,
}

impl RelayOpenError {
    /// Stable, detail-free classification suitable for a protocol fatal frame.
    pub fn fatal_code(&self) -> FatalCode {
        match self {
            Self::Controller(error) => error.fatal_code(),
            Self::Rendezvous(RendezvousInstallError::ScopeMismatch) => FatalCode::Unauthorized,
            Self::Rendezvous(
                RendezvousInstallError::AuthorityConflict
                | RendezvousInstallError::LaneOccupied
                | RendezvousInstallError::Quiescing,
            ) => FatalCode::StaleBinding,
            Self::Rendezvous(RendezvousInstallError::ProfileConflict)
            | Self::Authority(_)
            | Self::Wire(_) => FatalCode::Internal,
            Self::PeerClosed | Self::TaskStopped => FatalCode::Unavailable,
        }
    }
}

/// Successful bridge handshake result before any socket-specific work.
pub enum BridgeOpenOutcome {
    /// A durable generation exists. The outbound queue withholds `Activated`
    /// until the exact worker has registered and both queues have capacity.
    Attached(RelayAttachment<ControllerToBridgeV1>),
    /// No durable mapping exists. Send this correlated response and close;
    /// no rendezvous entry was created.
    MappingAbsent(ControllerToBridgeV1),
}

/// An exact installed lane and its bounded controller-to-peer queue.
///
/// Dropping an armed attachment synchronously changes the registry to
/// fail-closed `Quiescing`, then schedules durable containment when a Tokio
/// runtime is available. A later retry pass covers runtime shutdown or task
/// cancellation after that local transition.
pub struct RelayAttachment<M> {
    connection: Option<RelayConnection>,
    outbound: mpsc::Receiver<M>,
    quiesced: watch::Receiver<bool>,
    containment: ContainmentHandle,
}

impl<M> RelayAttachment<M> {
    pub fn connection(&self) -> &RelayConnection {
        self.connection
            .as_ref()
            .expect("an armed relay attachment always has a connection")
    }

    pub fn outbound(&mut self) -> &mut mpsc::Receiver<M> {
        &mut self.outbound
    }

    pub fn quiesced(&self) -> watch::Receiver<bool> {
        self.quiesced.clone()
    }

    fn disarm(&mut self) -> RelayConnection {
        self.connection
            .take()
            .expect("a relay attachment is disarmed at most once")
    }
}

impl<M> Drop for RelayAttachment<M> {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        self.containment.begin_and_schedule(connection);
    }
}

#[derive(Clone)]
struct ContainmentHandle {
    controller: Arc<dyn RelayController>,
    registry: RendezvousRegistry,
}

impl ContainmentHandle {
    fn begin_and_schedule(&self, connection: RelayConnection) {
        let RelayConnectionLoss::ContainmentRequired(ticket) =
            self.registry.begin_connection_loss(&connection)
        else {
            return;
        };
        let controller = Arc::clone(&self.controller);
        let registry = self.registry.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                persist_containment(controller.as_ref(), &registry, &ticket).await;
            });
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayLossOutcome {
    Contained(OrphanContainmentOutcome),
    AlreadyQuiescing,
    StaleConnection,
}

#[derive(Debug)]
pub struct RelayContainmentFailure {
    session_id: SessionId,
    fatal_code: FatalCode,
}

impl RelayContainmentFailure {
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn fatal_code(&self) -> FatalCode {
        self.fatal_code
    }
}

#[derive(Debug, Default)]
pub struct RelayContainmentReport {
    completed: usize,
    failures: Vec<RelayContainmentFailure>,
}

impl RelayContainmentReport {
    pub fn completed(&self) -> usize {
        self.completed
    }

    pub fn failures(&self) -> &[RelayContainmentFailure] {
        &self.failures
    }
}

/// Transport-neutral owner of activation, registration, and containment
/// ordering. Role-specific network endpoints must call the corresponding
/// method; peers never select their role in JSON.
#[derive(Clone)]
pub struct RelayOrchestrator {
    controller: Arc<dyn RelayController>,
    registry: RendezvousRegistry,
    queue_capacity: NonZeroUsize,
}

impl RelayOrchestrator {
    pub fn new(controller: Arc<ControllerService>, queue_capacity: NonZeroUsize) -> Self {
        Self::with_controller(controller, queue_capacity)
    }

    fn with_controller(controller: Arc<dyn RelayController>, queue_capacity: NonZeroUsize) -> Self {
        let registry = RendezvousRegistry::new(controller.scope_id());
        Self {
            controller,
            registry,
            queue_capacity,
        }
    }

    /// Health latch that the controller executable must supervise before
    /// advertising readiness.
    pub fn health(&self) -> watch::Receiver<RendezvousHealth> {
        self.registry.health()
    }

    /// Run activation in a controller-owned task so caller cancellation cannot
    /// interrupt Kubernetes mutation between durable preparation and registry
    /// installation.
    pub async fn activate_bridge(
        &self,
        request: ActivationRequestV1,
    ) -> Result<BridgeOpenOutcome, RelayOpenError> {
        let (result_sender, result_receiver) = oneshot::channel();
        let this = self.clone();
        tokio::spawn(async move {
            let result = this.activate_bridge_inner(request).await;
            let _ = result_sender.send(result);
        });
        result_receiver
            .await
            .unwrap_or(Err(RelayOpenError::TaskStopped))
    }

    /// Run single-use bootstrap consumption and lane installation in one
    /// controller-owned task for the same cancellation guarantee as activation.
    pub async fn register_worker(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RelayAttachment<ControllerToWorkerV1>, RelayOpenError> {
        let (result_sender, result_receiver) = oneshot::channel();
        let this = self.clone();
        tokio::spawn(async move {
            let result = this.register_worker_inner(registration, auth).await;
            let _ = result_sender.send(result);
        });
        result_receiver
            .await
            .unwrap_or(Err(RelayOpenError::TaskStopped))
    }

    /// Quiesce an exact attachment, persist containment, and only then remove
    /// its retry ticket. Errors leave the registry fail closed for a later pass.
    pub async fn connection_lost<M>(
        &self,
        mut attachment: RelayAttachment<M>,
    ) -> Result<RelayLossOutcome, ControllerServiceError> {
        let connection = attachment.disarm();
        match self.registry.begin_connection_loss(&connection) {
            RelayConnectionLoss::ContainmentRequired(ticket) => {
                let outcome = self.controller.connection_lost(ticket.authority()).await?;
                self.registry.complete_containment(&ticket);
                Ok(RelayLossOutcome::Contained(outcome))
            }
            RelayConnectionLoss::AlreadyQuiescing => Ok(RelayLossOutcome::AlreadyQuiescing),
            RelayConnectionLoss::StaleConnection => Ok(RelayLossOutcome::StaleConnection),
        }
    }

    /// Retry every ticket retained after a failed or cancelled persistence
    /// attempt. Each success is completed only after the controller returns Ok.
    pub async fn retry_pending_containments(&self) -> RelayContainmentReport {
        let mut report = RelayContainmentReport::default();
        for ticket in self.registry.pending_containments() {
            let detached = ticket.is_detached();
            let session_id = ticket.trigger().session_id();
            let outcome = self.persist_ticket_for_report(ticket, &mut report).await;
            if detached
                && outcome
                    .is_some_and(|outcome| outcome != OrphanContainmentOutcome::StaleObservation)
            {
                if let RelayConnectionLoss::ContainmentRequired(installed) =
                    self.registry.begin_session_containment(session_id)
                {
                    self.persist_ticket_for_report(*installed, &mut report)
                        .await;
                }
            }
        }
        report
    }

    async fn activate_bridge_inner(
        &self,
        request: ActivationRequestV1,
    ) -> Result<BridgeOpenOutcome, RelayOpenError> {
        match self
            .controller
            .activate(&request)
            .await
            .map_err(RelayOpenError::Controller)?
        {
            ActivationPreparation::MappingAbsent(response) => Ok(BridgeOpenOutcome::MappingAbsent(
                ControllerToBridgeV1::Activation(response),
            )),
            ActivationPreparation::AwaitingRegistration {
                binding,
                profile,
                pod_uid,
            } => {
                let authority =
                    OrphanAuthority::new(binding, pod_uid).map_err(RelayOpenError::Authority)?;
                let activation = match PendingActivation::new(&request, profile, &authority) {
                    Ok(activation) => activation,
                    Err(error) => {
                        self.contain_unattached(&authority)
                            .await
                            .map_err(RelayOpenError::Controller)?;
                        return Err(RelayOpenError::Wire(error));
                    }
                };
                let (outbound, receiver) = mpsc::channel(self.queue_capacity.get());
                let installation =
                    match self
                        .registry
                        .install_bridge(authority.clone(), activation, outbound)
                    {
                        Ok(installation) => installation,
                        Err(error) => {
                            self.contain_after_install_failure(&authority, &error)
                                .await
                                .map_err(RelayOpenError::Controller)?;
                            return Err(RelayOpenError::Rendezvous(error));
                        }
                    };
                self.attachment_or_contain(installation, receiver)
                    .await
                    .map(BridgeOpenOutcome::Attached)
            }
        }
    }

    async fn register_worker_inner(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
    ) -> Result<RelayAttachment<ControllerToWorkerV1>, RelayOpenError> {
        let RegisteredWorkerParts {
            binding,
            profile,
            pod_uid,
        } = registered_worker_parts(
            self.controller
                .register(registration, auth)
                .await
                .map_err(RelayOpenError::Controller)?,
        );
        let authority =
            OrphanAuthority::new(binding, pod_uid).map_err(RelayOpenError::Authority)?;
        let (outbound, receiver) = mpsc::channel(self.queue_capacity.get());
        let installation = match self
            .registry
            .install_worker(authority.clone(), profile, outbound)
        {
            Ok(installation) => installation,
            Err(error) => {
                self.contain_after_install_failure(&authority, &error)
                    .await
                    .map_err(RelayOpenError::Controller)?;
                return Err(RelayOpenError::Rendezvous(error));
            }
        };
        self.attachment_or_contain(installation, receiver).await
    }

    async fn contain_after_install_failure(
        &self,
        authority: &OrphanAuthority,
        error: &RendezvousInstallError,
    ) -> Result<(), ControllerServiceError> {
        match error {
            // These are expected connection races. The already-installed lane
            // remains authoritative, or an earlier ticket already made it
            // fail closed; the rejected duplicate must not evict either one.
            RendezvousInstallError::LaneOccupied | RendezvousInstallError::Quiescing => Ok(()),
            // Same generation plus inconsistent trusted profile metadata is
            // unsafe to route and can be retained under the exact entry.
            RendezvousInstallError::ProfileConflict => self.contain_unattached(authority).await,
            // Wiring mismatch or a different generation must be checked
            // directly against fresh durable authority. The public constructor
            // prevents scope mismatch; these branches remain defensive.
            RendezvousInstallError::ScopeMismatch => {
                self.controller.connection_lost(authority).await.map(|_| ())
            }
            RendezvousInstallError::AuthorityConflict => {
                self.contain_authority_conflict(authority).await
            }
        }
    }

    async fn contain_authority_conflict(
        &self,
        authority: &OrphanAuthority,
    ) -> Result<(), ControllerServiceError> {
        let ticket = match self
            .registry
            .begin_unattached_containment(authority.clone())
        {
            RelayConnectionLoss::ContainmentRequired(ticket) => ticket,
            RelayConnectionLoss::AlreadyQuiescing => return Ok(()),
            RelayConnectionLoss::StaleConnection => {
                return self.controller.connection_lost(authority).await.map(|_| ());
            }
        };
        let outcome = self.controller.connection_lost(ticket.authority()).await?;
        self.registry.complete_containment(&ticket);
        if outcome != OrphanContainmentOutcome::StaleObservation {
            self.contain_installed_session(authority.binding().session_id())
                .await?;
        }
        Ok(())
    }

    async fn contain_installed_session(
        &self,
        session_id: SessionId,
    ) -> Result<(), ControllerServiceError> {
        if let RelayConnectionLoss::ContainmentRequired(ticket) =
            self.registry.begin_session_containment(session_id)
        {
            self.controller.connection_lost(ticket.authority()).await?;
            self.registry.complete_containment(&ticket);
        }
        Ok(())
    }

    async fn contain_unattached(
        &self,
        authority: &OrphanAuthority,
    ) -> Result<(), ControllerServiceError> {
        match self
            .registry
            .begin_unattached_containment(authority.clone())
        {
            RelayConnectionLoss::ContainmentRequired(ticket) => {
                let detached = ticket.is_detached();
                let session_id = ticket.trigger().session_id();
                let outcome = self.controller.connection_lost(ticket.authority()).await?;
                self.registry.complete_containment(&ticket);
                if detached && outcome != OrphanContainmentOutcome::StaleObservation {
                    self.contain_installed_session(session_id).await?;
                }
                Ok(())
            }
            RelayConnectionLoss::AlreadyQuiescing => Ok(()),
            RelayConnectionLoss::StaleConnection => {
                self.controller.connection_lost(authority).await.map(|_| ())
            }
        }
    }

    async fn attachment_or_contain<M>(
        &self,
        installation: super::RelayInstallation,
        receiver: mpsc::Receiver<M>,
    ) -> Result<RelayAttachment<M>, RelayOpenError> {
        if let RelayPairingOutcome::ContainmentRequired(ticket) = installation.pairing() {
            persist_containment(self.controller.as_ref(), &self.registry, ticket).await;
            return Err(RelayOpenError::PeerClosed);
        }
        Ok(RelayAttachment {
            connection: Some(installation.connection().clone()),
            outbound: receiver,
            quiesced: installation.quiesced(),
            containment: ContainmentHandle {
                controller: Arc::clone(&self.controller),
                registry: self.registry.clone(),
            },
        })
    }

    async fn persist_ticket_for_report(
        &self,
        ticket: RelayContainmentTicket,
        report: &mut RelayContainmentReport,
    ) -> Option<OrphanContainmentOutcome> {
        match self.controller.connection_lost(ticket.authority()).await {
            Ok(outcome) => {
                if self.registry.complete_containment(&ticket)
                    == RelayContainmentCompletion::Removed
                {
                    report.completed += 1;
                }
                Some(outcome)
            }
            Err(error) => {
                report.failures.push(RelayContainmentFailure {
                    session_id: ticket.trigger().session_id(),
                    fatal_code: error.fatal_code(),
                });
                None
            }
        }
    }
}

async fn persist_containment(
    controller: &dyn RelayController,
    registry: &RendezvousRegistry,
    ticket: &RelayContainmentTicket,
) {
    if controller.connection_lost(ticket.authority()).await.is_ok() {
        registry.complete_containment(ticket);
    }
}

fn registered_worker_parts(worker: RegisteredWorker) -> RegisteredWorkerParts {
    RegisteredWorkerParts {
        binding: worker.binding().clone(),
        profile: worker.profile().clone(),
        pod_uid: worker.pod_uid().to_string(),
    }
}

struct RegisteredWorkerParts {
    binding: SessionBinding,
    profile: ProfileRef,
    pod_uid: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::RendezvousRouteError;
    use crate::identity::ScopeId;
    use crate::state::Fence;
    use crate::wire::{
        ActivationResponseV1, BrokerMappingExpectationV1, ControllerToWorkerV1, HandshakeOutcomeV1,
        ValidatedActivationOutcomeV1, MAX_PROFILE_VERSION_BYTES,
    };
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::Semaphore;
    use tokio::time::{timeout, Duration};
    use uuid::Uuid;

    const RAW_SCOPE: &str = "organization-secret-team-a";
    const PROFILE_NAME: &str = "codex-strict";
    const PROFILE_VERSION: &str = "2026-08-01";
    const POD_UID: &str = "worker-pod-uid-relay";
    const TOKEN: [u8; 32] = [0x5a; 32];

    #[derive(Clone)]
    struct Gate {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                started: Arc::new(Semaphore::new(0)),
                release: Arc::new(Semaphore::new(0)),
            }
        }

        async fn wait(&self) {
            self.started.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("test gate remains open")
                .forget();
        }
    }

    struct FakeController {
        binding: SessionBinding,
        profile: ProfileRef,
        activation_mode: AtomicU8,
        activation_profile_too_long: AtomicBool,
        registration_profile_mismatch: AtomicBool,
        registration_fails: AtomicBool,
        activation_gate: Mutex<Option<Gate>>,
        registration_gate: Mutex<Option<Gate>>,
        loss_failures: AtomicUsize,
        loss_outcome: AtomicU8,
        losses: AtomicUsize,
    }

    impl FakeController {
        fn new(binding: SessionBinding) -> Self {
            Self {
                binding,
                profile: profile(),
                activation_mode: AtomicU8::new(0),
                activation_profile_too_long: AtomicBool::new(false),
                registration_profile_mismatch: AtomicBool::new(false),
                registration_fails: AtomicBool::new(false),
                activation_gate: Mutex::new(None),
                registration_gate: Mutex::new(None),
                loss_failures: AtomicUsize::new(0),
                loss_outcome: AtomicU8::new(0),
                losses: AtomicUsize::new(0),
            }
        }

        fn set_activation_gate(&self, gate: Gate) {
            *self.activation_gate.lock().unwrap() = Some(gate);
        }

        fn set_registration_gate(&self, gate: Gate) {
            *self.registration_gate.lock().unwrap() = Some(gate);
        }

        fn fail_next_loss(&self) {
            self.loss_failures.fetch_add(1, Ordering::SeqCst);
        }

        fn should_fail_loss(&self) -> bool {
            self.loss_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        }

        fn set_loss_outcome(&self, outcome: OrphanContainmentOutcome) {
            self.loss_outcome.store(
                match outcome {
                    OrphanContainmentOutcome::ContainmentAccepted => 0,
                    OrphanContainmentOutcome::AlreadyQuiescing => 1,
                    OrphanContainmentOutcome::StaleObservation => 2,
                },
                Ordering::SeqCst,
            );
        }
    }

    #[async_trait]
    impl RelayController for FakeController {
        fn scope_id(&self) -> ScopeId {
            scope_id()
        }

        async fn activate(
            &self,
            request: &ActivationRequestV1,
        ) -> Result<ActivationPreparation, ControllerServiceError> {
            let gate = self.activation_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            match self.activation_mode.load(Ordering::SeqCst) {
                0 => Ok(ActivationPreparation::AwaitingRegistration {
                    binding: self.binding.clone(),
                    profile: if self.activation_profile_too_long.load(Ordering::SeqCst) {
                        ProfileRef::new(PROFILE_NAME, "v".repeat(MAX_PROFILE_VERSION_BYTES + 1))
                            .unwrap()
                    } else {
                        self.profile.clone()
                    },
                    pod_uid: POD_UID.to_string(),
                }),
                1 => Ok(ActivationPreparation::MappingAbsent(
                    ActivationResponseV1::mapping_absent(request)
                        .expect("test request expects a mapping"),
                )),
                _ => Err(ControllerServiceError::ScopeMismatch),
            }
        }

        async fn register(
            &self,
            _registration: WorkerRegistrationV1,
            _auth: WorkerBootstrapAuth,
        ) -> Result<RegisteredWorker, ControllerServiceError> {
            let gate = self.registration_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            if self.registration_fails.load(Ordering::SeqCst) {
                return Err(ControllerServiceError::ScopeMismatch);
            }
            Ok(RegisteredWorker::from_parts(
                self.binding.clone(),
                if self.registration_profile_mismatch.load(Ordering::SeqCst) {
                    ProfileRef::new(PROFILE_NAME, "2026-08-02").unwrap()
                } else {
                    self.profile.clone()
                },
                POD_UID.to_string(),
            ))
        }

        async fn connection_lost(
            &self,
            _authority: &OrphanAuthority,
        ) -> Result<OrphanContainmentOutcome, ControllerServiceError> {
            self.losses.fetch_add(1, Ordering::SeqCst);
            if self.should_fail_loss() {
                Err(ControllerServiceError::ScopeMismatch)
            } else {
                Ok(match self.loss_outcome.load(Ordering::SeqCst) {
                    0 => OrphanContainmentOutcome::ContainmentAccepted,
                    1 => OrphanContainmentOutcome::AlreadyQuiescing,
                    _ => OrphanContainmentOutcome::StaleObservation,
                })
            }
        }
    }

    fn scope_id() -> ScopeId {
        ScopeId::derive(RAW_SCOPE)
    }

    fn session_id() -> SessionId {
        SessionId::derive(RAW_SCOPE, "discord:relay-orchestrator")
    }

    fn binding() -> SessionBinding {
        SessionBinding::new(
            scope_id(),
            session_id(),
            Fence::new(1, Uuid::from_u128(0x101)).unwrap(),
            Uuid::from_u128(0x201),
        )
        .unwrap()
    }

    fn replacement_binding() -> SessionBinding {
        SessionBinding::new(
            scope_id(),
            session_id(),
            Fence::new(2, Uuid::from_u128(0x102)).unwrap(),
            Uuid::from_u128(0x202),
        )
        .unwrap()
    }

    fn profile() -> ProfileRef {
        ProfileRef::new(PROFILE_NAME, PROFILE_VERSION).unwrap()
    }

    fn activation_request(expectation: BrokerMappingExpectationV1) -> ActivationRequestV1 {
        ActivationRequestV1::new(
            scope_id(),
            session_id(),
            Uuid::from_u128(0x101),
            PROFILE_NAME,
            expectation,
        )
        .unwrap()
    }

    fn registration() -> WorkerRegistrationV1 {
        WorkerRegistrationV1::new(&binding())
    }

    fn auth() -> WorkerBootstrapAuth {
        WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap()
    }

    fn orchestrator(controller: Arc<FakeController>) -> (RelayOrchestrator, RendezvousRegistry) {
        let relay = RelayOrchestrator::with_controller(controller, NonZeroUsize::new(1).unwrap());
        let registry = relay.registry.clone();
        (relay, registry)
    }

    fn install_original_active_pair(
        registry: &RendezvousRegistry,
    ) -> (
        crate::controller::RelayInstallation,
        crate::controller::RelayInstallation,
        mpsc::Receiver<ControllerToBridgeV1>,
        mpsc::Receiver<ControllerToWorkerV1>,
    ) {
        let authority = OrphanAuthority::new(binding(), POD_UID).unwrap();
        let request = activation_request(BrokerMappingExpectationV1::Absent);
        let pending = PendingActivation::new(&request, profile(), &authority).unwrap();
        let (bridge_sender, bridge_receiver) = mpsc::channel(1);
        let bridge = registry
            .install_bridge(authority.clone(), pending, bridge_sender)
            .unwrap();
        let (worker_sender, worker_receiver) = mpsc::channel(1);
        let worker = registry
            .install_worker(authority, profile(), worker_sender)
            .unwrap();
        assert_eq!(worker.pairing(), &RelayPairingOutcome::Active);
        (bridge, worker, bridge_receiver, worker_receiver)
    }

    async fn wait_for_losses(controller: &FakeController, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while controller.losses.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("containment should be scheduled promptly");
    }

    #[tokio::test]
    async fn exact_pair_gets_ack_and_activation_only_after_both_domain_calls_succeed() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let request = activation_request(BrokerMappingExpectationV1::Absent);
        let BridgeOpenOutcome::Attached(mut bridge) =
            relay.activate_bridge(request.clone()).await.unwrap()
        else {
            panic!("durable generation must attach")
        };
        assert!(matches!(
            bridge.outbound().try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let mut worker = relay.register_worker(registration(), auth()).await.unwrap();
        let ControllerToWorkerV1::ProtocolResult(worker_result) =
            worker.outbound().recv().await.unwrap()
        else {
            panic!("worker must receive handshake ACK")
        };
        assert_eq!(
            worker_result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Ack
        );
        let ControllerToBridgeV1::Activation(activation) = bridge.outbound().recv().await.unwrap()
        else {
            panic!("bridge must receive Activated")
        };
        assert!(matches!(
            activation.into_validated_outcome(&request).unwrap(),
            ValidatedActivationOutcomeV1::Activated { worker_cwd, .. }
                if worker_cwd == "/session/workspace"
        ));
    }

    #[tokio::test]
    async fn mapping_absent_returns_terminal_response_without_registry_entry() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.activation_mode.store(1, Ordering::SeqCst);
        let (relay, registry) = orchestrator(controller);
        let request = activation_request(BrokerMappingExpectationV1::Present);

        let BridgeOpenOutcome::MappingAbsent(ControllerToBridgeV1::Activation(response)) =
            relay.activate_bridge(request.clone()).await.unwrap()
        else {
            panic!("missing mapping must be terminal")
        };
        assert_eq!(
            response.into_validated_outcome(&request).unwrap(),
            ValidatedActivationOutcomeV1::MappingAbsent
        );
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn cancelled_activation_caller_cannot_cancel_mutation_or_leave_routing_active() {
        let controller = Arc::new(FakeController::new(binding()));
        let gate = Gate::new();
        controller.set_activation_gate(gate.clone());
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let call = tokio::spawn({
            let relay = relay.clone();
            async move {
                relay
                    .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
                    .await
            }
        });
        gate.started.acquire().await.unwrap().forget();
        call.abort();
        gate.release.add_permits(1);

        wait_for_losses(&controller, 1).await;
        timeout(Duration::from_secs(1), async {
            while !registry.pending_containments().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successful cancellation containment removes its ticket");
    }

    #[tokio::test]
    async fn cancelled_registration_caller_contains_consumed_ready_generation() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut quiesced = bridge.quiesced();
        let gate = Gate::new();
        controller.set_registration_gate(gate.clone());
        let call = tokio::spawn({
            let relay = relay.clone();
            async move { relay.register_worker(registration(), auth()).await }
        });
        gate.started.acquire().await.unwrap().forget();
        call.abort();
        gate.release.add_permits(1);

        wait_for_losses(&controller, 1).await;
        quiesced.changed().await.unwrap();
        assert!(*quiesced.borrow_and_update());
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn failed_drop_containment_remains_pending_until_retry_succeeds() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.fail_next_loss();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        drop(bridge);

        wait_for_losses(&controller, 1).await;
        assert_eq!(registry.pending_containments().len(), 1);
        let report = relay.retry_pending_containments().await;
        assert_eq!(report.completed(), 1);
        assert!(report.failures().is_empty());
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn registration_failure_is_sanitized_and_does_not_install_worker_lane() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.registration_fails.store(true, Ordering::SeqCst);
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };

        let error = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("failed registration must not install a worker lane"),
            Err(error) => error,
        };
        assert_eq!(error.fatal_code(), FatalCode::Unauthorized);
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::AwaitingPeer)
        );
    }

    #[tokio::test]
    async fn activation_post_service_wire_failure_is_contained_immediately() {
        let controller = Arc::new(FakeController::new(binding()));
        controller
            .activation_profile_too_long
            .store(true, Ordering::SeqCst);
        let (relay, registry) = orchestrator(Arc::clone(&controller));

        let error = match relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
        {
            Ok(_) => panic!("invalid trusted activation output must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(&error, RelayOpenError::Wire(_)));
        assert_eq!(error.fatal_code(), FatalCode::Internal);
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn foreign_activation_wire_failure_contains_the_fresh_and_installed_authorities() {
        let controller = Arc::new(FakeController::new(replacement_binding()));
        controller
            .activation_profile_too_long
            .store(true, Ordering::SeqCst);
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, _bridge_receiver, _worker_receiver) =
            install_original_active_pair(&registry);

        let error = match relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
        {
            Ok(_) => panic!("invalid trusted activation output must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(&error, RelayOpenError::Wire(_)));
        assert_eq!(error.fatal_code(), FatalCode::Internal);
        assert_eq!(controller.losses.load(Ordering::SeqCst), 2);
        assert!(registry.pending_containments().is_empty());
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
    }

    #[tokio::test]
    async fn registration_post_service_profile_conflict_quiesces_existing_lane() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut quiesced = bridge.quiesced();
        controller
            .registration_profile_mismatch
            .store(true, Ordering::SeqCst);

        let error = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("conflicting trusted profile must not install a worker lane"),
            Err(error) => error,
        };
        assert!(matches!(
            &error,
            RelayOpenError::Rendezvous(RendezvousInstallError::ProfileConflict)
        ));
        assert_eq!(error.fatal_code(), FatalCode::Internal);
        quiesced.changed().await.unwrap();
        assert!(*quiesced.borrow_and_update());
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn foreign_authority_failure_is_retryable_without_quiescing_the_current_lane() {
        let controller = Arc::new(FakeController::new(replacement_binding()));
        controller.fail_next_loss();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, _bridge_receiver, _worker_receiver) =
            install_original_active_pair(&registry);
        assert_eq!(
            registry.route_target(bridge.connection()).unwrap(),
            worker.connection().connection_id()
        );

        let error = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("a foreign generation must not replace the current lane"),
            Err(error) => error,
        };
        assert!(matches!(error, RelayOpenError::Controller(_)));
        assert_eq!(
            registry.route_target(bridge.connection()).unwrap(),
            worker.connection().connection_id()
        );
        let pending = registry.pending_containments();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].is_detached());
        assert_eq!(pending[0].authority().binding(), &replacement_binding());

        let report = relay.retry_pending_containments().await;
        assert_eq!(report.completed(), 2);
        assert!(report.failures().is_empty());
        assert!(registry.pending_containments().is_empty());
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
    }

    #[tokio::test]
    async fn stale_foreign_authority_never_quiesces_the_current_lane() {
        let controller = Arc::new(FakeController::new(replacement_binding()));
        controller.set_loss_outcome(OrphanContainmentOutcome::StaleObservation);
        let (relay, registry) = orchestrator(controller);
        let (bridge, worker, _bridge_receiver, _worker_receiver) =
            install_original_active_pair(&registry);

        let error = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("a foreign generation must not replace the current lane"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            RelayOpenError::Rendezvous(RendezvousInstallError::AuthorityConflict)
        ));
        assert_eq!(
            registry.route_target(bridge.connection()).unwrap(),
            worker.connection().connection_id()
        );
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn foreign_authority_against_old_quiescing_state_keeps_its_own_ticket() {
        let controller = Arc::new(FakeController::new(replacement_binding()));
        controller.fail_next_loss();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, _bridge_receiver, _worker_receiver) =
            install_original_active_pair(&registry);
        let RelayConnectionLoss::ContainmentRequired(old_ticket) =
            registry.begin_connection_loss(bridge.connection())
        else {
            panic!("the original lane must enter quiescing")
        };

        let error = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("the new generation must remain detached"),
            Err(error) => error,
        };
        assert!(matches!(error, RelayOpenError::Controller(_)));
        let pending = registry.pending_containments();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|ticket| ticket == old_ticket.as_ref()));
        assert!(pending.iter().any(|ticket| {
            ticket.is_detached() && ticket.authority().binding() == &replacement_binding()
        }));

        let repeated = match relay.register_worker(registration(), auth()).await {
            Ok(_) => panic!("pending containment must fence a repeated install"),
            Err(error) => error,
        };
        assert!(matches!(
            repeated,
            RelayOpenError::Rendezvous(RendezvousInstallError::Quiescing)
        ));
        assert_eq!(registry.pending_containments().len(), 2);
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }
}
