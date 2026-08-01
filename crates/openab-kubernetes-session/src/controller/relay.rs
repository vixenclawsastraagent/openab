use super::{
    AcpRouteOutcome, ActivationPreparation, ControllerService, ControllerServiceError,
    LifecycleAcquireOutcome, LifecycleAdmission, LifecycleDeliveryOutcome, LifecycleServiceOutcome,
    OrphanAuthority, OrphanAuthorityError, OrphanContainmentOutcome, PendingActivation,
    RegisteredWorker, RelayBackpressure, RelayByteBudget, RelayConnection, RelayConnectionLoss,
    RelayContainmentCompletion, RelayContainmentTicket, RelayLifecycleTerminal, RelayOutboundItem,
    RelayPairingOutcome, RendezvousHealth, RendezvousInstallError, RendezvousLifecycleError,
    RendezvousRegistry, RendezvousRouteError, WorkerBootstrapAuth,
};
use crate::bridge::SessionBinding;
use crate::identity::{ScopeId, SessionId};
use crate::state::ProfileRef;
use crate::wire::{
    AcpMessageV1, ActivationRequestV1, ControllerToBridgeV1, ControllerToWorkerV1, FatalCode,
    LifecycleRequestV1, ProtocolResultV1, WireProtocolError, WorkerRegistrationV1,
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

    async fn lifecycle(
        &self,
        request: &LifecycleRequestV1,
    ) -> Result<LifecycleServiceOutcome, ControllerServiceError>;
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

    async fn lifecycle(
        &self,
        request: &LifecycleRequestV1,
    ) -> Result<LifecycleServiceOutcome, ControllerServiceError> {
        ControllerService::lifecycle(self, request).await
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

#[derive(Debug, PartialEq, Eq)]
pub enum RelayAcpDeliveryOutcome {
    Delivered,
    Backpressured {
        message: AcpMessageV1,
        reason: RelayBackpressure,
    },
    PeerContained(OrphanContainmentOutcome),
}

#[derive(Debug, Error)]
pub enum RelayDeliveryError {
    #[error("relay routing rejected the connection")]
    Rendezvous(#[source] RendezvousRouteError),
    #[error("controller containment failed")]
    Controller(#[source] ControllerServiceError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayLifecycleOutcome {
    /// An identical in-flight request already owns this session.
    Coalesced,
    /// Durable release intent exists, but absence proof is not complete yet.
    ReleasePending,
    /// Suspend intent is durable and its correlated ACK reached the bridge.
    Suspended,
    /// Full release absence is proven and its correlated ACK reached the bridge.
    Released,
}

#[derive(Debug, Error)]
pub enum RelayLifecycleError {
    #[error("relay lifecycle admission failed")]
    Rendezvous(#[source] RendezvousLifecycleError),
    #[error("controller lifecycle operation failed")]
    Controller(#[source] ControllerServiceError),
    #[error("the correlated lifecycle result did not reach the bridge")]
    ResultDeliveryLost,
    #[error("the controller-owned lifecycle task stopped before returning a result")]
    TaskStopped,
}

impl RelayLifecycleError {
    pub fn fatal_code(&self) -> FatalCode {
        match self {
            Self::Rendezvous(RendezvousLifecycleError::WrongLane)
            | Self::Rendezvous(RendezvousLifecycleError::AwaitingPeer) => FatalCode::InvalidMessage,
            Self::Rendezvous(RendezvousLifecycleError::BindingMismatch)
            | Self::Rendezvous(RendezvousLifecycleError::ConflictingRequest)
            | Self::Rendezvous(RendezvousLifecycleError::StaleConnection)
            | Self::Rendezvous(RendezvousLifecycleError::Quiescing)
            | Self::Rendezvous(RendezvousLifecycleError::ReservationLost) => {
                FatalCode::StaleBinding
            }
            Self::Controller(error) => error.fatal_code(),
            Self::ResultDeliveryLost | Self::TaskStopped => FatalCode::Unavailable,
        }
    }
}

impl RelayDeliveryError {
    pub fn fatal_code(&self) -> FatalCode {
        match self {
            Self::Rendezvous(RendezvousRouteError::AwaitingPeer) => FatalCode::InvalidMessage,
            Self::Rendezvous(
                RendezvousRouteError::StaleConnection
                | RendezvousRouteError::LifecyclePending
                | RendezvousRouteError::Quiescing,
            ) => FatalCode::StaleBinding,
            Self::Rendezvous(RendezvousRouteError::FrameExceedsByteBudget { .. }) => {
                FatalCode::Unavailable
            }
            Self::Controller(error) => error.fatal_code(),
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
    outbound: mpsc::Receiver<RelayOutboundItem<M>>,
    quiesced: watch::Receiver<bool>,
    containment: ContainmentHandle,
}

impl<M> RelayAttachment<M> {
    pub fn connection(&self) -> &RelayConnection {
        self.connection
            .as_ref()
            .expect("an armed relay attachment always has a connection")
    }

    pub fn outbound(&mut self) -> &mut mpsc::Receiver<RelayOutboundItem<M>> {
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
    LifecycleWorkerDetached,
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
    pub fn new(
        controller: Arc<ControllerService>,
        queue_capacity: NonZeroUsize,
        byte_budget: RelayByteBudget,
    ) -> Self {
        Self::with_controller(controller, queue_capacity, byte_budget)
    }

    fn with_controller(
        controller: Arc<dyn RelayController>,
        queue_capacity: NonZeroUsize,
        byte_budget: RelayByteBudget,
    ) -> Self {
        let registry = RendezvousRegistry::with_byte_budget(controller.scope_id(), byte_budget);
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

    /// Deliver one ACP message through the exact active peer lane.
    ///
    /// Backpressure returns ownership to the caller. A closed peer is changed
    /// to `Quiescing` by the registry before this method performs durable
    /// containment; transient controller errors leave the ticket retryable.
    pub async fn route_acp(
        &self,
        connection: &RelayConnection,
        message: AcpMessageV1,
    ) -> Result<RelayAcpDeliveryOutcome, RelayDeliveryError> {
        match self
            .registry
            .route_acp(connection, message)
            .map_err(RelayDeliveryError::Rendezvous)?
        {
            AcpRouteOutcome::Delivered => Ok(RelayAcpDeliveryOutcome::Delivered),
            AcpRouteOutcome::Backpressured { message, reason } => {
                Ok(RelayAcpDeliveryOutcome::Backpressured { message, reason })
            }
            AcpRouteOutcome::ContainmentRequired(ticket) => {
                let outcome = self
                    .controller
                    .connection_lost(ticket.authority())
                    .await
                    .map_err(RelayDeliveryError::Controller)?;
                self.registry.complete_containment(&ticket);
                Ok(RelayAcpDeliveryOutcome::PeerContained(outcome))
            }
        }
    }

    /// Wait until an exact backpressured ACP message should be retried.
    ///
    /// This is a readiness hint, not a capacity reservation. The transport must
    /// retain the original message and call [`Self::route_acp`] again after this
    /// method returns successfully.
    pub async fn wait_for_route_capacity(
        &self,
        connection: &RelayConnection,
        message: &AcpMessageV1,
    ) -> Result<(), RelayDeliveryError> {
        self.registry
            .wait_for_route_capacity(connection, message)
            .await
            .map_err(RelayDeliveryError::Rendezvous)
    }

    /// Fence ACP and run one exact bridge lifecycle request in a
    /// controller-owned task.
    ///
    /// The rendezvous reserves one bridge queue slot and one control-frame
    /// byte lease before Kubernetes mutation begins. Suspend and final release
    /// return only after the transport writer marks the correlated ACK as
    /// written; a pending release emits no result and retains the exact request
    /// for a later reconciliation pass.
    pub async fn request_lifecycle(
        &self,
        connection: &RelayConnection,
        request: LifecycleRequestV1,
    ) -> Result<RelayLifecycleOutcome, RelayLifecycleError> {
        let (result_sender, result_receiver) = oneshot::channel();
        let this = self.clone();
        let connection = connection.clone();
        tokio::spawn(async move {
            let result = this.request_lifecycle_inner(&connection, request).await;
            let _ = result_sender.send(result);
        });
        result_receiver
            .await
            .unwrap_or(Err(RelayLifecycleError::TaskStopped))
    }

    /// Run activation in a controller-owned task so caller cancellation cannot
    /// interrupt Kubernetes mutation between durable preparation and registry
    /// installation.
    pub async fn activate_bridge(
        &self,
        request: ActivationRequestV1,
    ) -> Result<BridgeOpenOutcome, RelayOpenError> {
        let (result_sender, result_receiver) = oneshot::channel();
        let (_caller_lifetime, caller_gone) = oneshot::channel();
        let this = self.clone();
        tokio::spawn(async move {
            let result = this.activate_bridge_inner(request, caller_gone).await;
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
        let (_caller_lifetime, caller_gone) = oneshot::channel();
        let this = self.clone();
        tokio::spawn(async move {
            let result = this
                .register_worker_inner(registration, auth, caller_gone)
                .await;
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
        let loss = self.registry.begin_connection_loss(&connection);
        // The registry is already fenced. Release every queued byte lease and
        // delivery reporter before controller I/O, which may be slow or fail.
        drop(attachment);
        match loss {
            RelayConnectionLoss::ContainmentRequired(ticket) => {
                let outcome = self.controller.connection_lost(ticket.authority()).await?;
                self.registry.complete_containment(&ticket);
                Ok(RelayLossOutcome::Contained(outcome))
            }
            RelayConnectionLoss::AlreadyQuiescing => Ok(RelayLossOutcome::AlreadyQuiescing),
            RelayConnectionLoss::StaleConnection => Ok(RelayLossOutcome::StaleConnection),
            RelayConnectionLoss::LifecycleWorkerDetached => {
                Ok(RelayLossOutcome::LifecycleWorkerDetached)
            }
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
        caller_gone: oneshot::Receiver<()>,
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
                self.attachment_or_contain(installation, receiver, caller_gone)
                    .await
                    .map(BridgeOpenOutcome::Attached)
            }
        }
    }

    async fn request_lifecycle_inner(
        &self,
        connection: &RelayConnection,
        request: LifecycleRequestV1,
    ) -> Result<RelayLifecycleOutcome, RelayLifecycleError> {
        let reservation = match self.registry.begin_lifecycle(connection, request) {
            LifecycleAdmission::Reserve(reservation) => reservation,
            LifecycleAdmission::Coalesced => return Ok(RelayLifecycleOutcome::Coalesced),
            LifecycleAdmission::Rejected(error) => {
                return Err(RelayLifecycleError::Rendezvous(error));
            }
            LifecycleAdmission::ContainmentRequired { error, ticket } => {
                self.persist_lifecycle_ticket(&ticket).await?;
                return Err(RelayLifecycleError::Rendezvous(error));
            }
        };

        let execution = match (*reservation).acquire().await {
            LifecycleAcquireOutcome::Ready(execution) => *execution,
            LifecycleAcquireOutcome::Rejected(error) => {
                return Err(RelayLifecycleError::Rendezvous(error));
            }
            LifecycleAcquireOutcome::ContainmentRequired(ticket) => {
                self.persist_lifecycle_ticket(&ticket).await?;
                return Err(RelayLifecycleError::ResultDeliveryLost);
            }
        };

        let service_outcome = match self.controller.lifecycle(execution.request()).await {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some(ticket) = execution.defer_controller_error() {
                    self.persist_lifecycle_ticket(&ticket).await?;
                }
                return Err(RelayLifecycleError::Controller(error));
            }
        };
        let terminal = match service_outcome {
            LifecycleServiceOutcome::ReleasePending => {
                execution.defer_release_pending();
                return Ok(RelayLifecycleOutcome::ReleasePending);
            }
            LifecycleServiceOutcome::SuspendAccepted => RelayLifecycleTerminal::Suspended,
            LifecycleServiceOutcome::Released => RelayLifecycleTerminal::Released,
        };

        let result = ProtocolResultV1::ack(Some(execution.request().request_id()))
            .expect("a validated lifecycle request has a non-nil request ID");
        let delivery = execution
            .queue_result(ControllerToBridgeV1::ProtocolResult(result), terminal)
            .map_err(RelayLifecycleError::Rendezvous)?;
        match delivery.wait().await {
            LifecycleDeliveryOutcome::Written(RelayLifecycleTerminal::Suspended) => {
                Ok(RelayLifecycleOutcome::Suspended)
            }
            LifecycleDeliveryOutcome::Written(RelayLifecycleTerminal::Released) => {
                Ok(RelayLifecycleOutcome::Released)
            }
            LifecycleDeliveryOutcome::ContainmentRequired(ticket) => {
                self.persist_lifecycle_ticket(&ticket).await?;
                Err(RelayLifecycleError::ResultDeliveryLost)
            }
            LifecycleDeliveryOutcome::Lost => Err(RelayLifecycleError::ResultDeliveryLost),
        }
    }

    async fn persist_lifecycle_ticket(
        &self,
        ticket: &RelayContainmentTicket,
    ) -> Result<(), RelayLifecycleError> {
        self.controller
            .connection_lost(ticket.authority())
            .await
            .map_err(RelayLifecycleError::Controller)?;
        self.registry.complete_containment(ticket);
        Ok(())
    }

    async fn register_worker_inner(
        &self,
        registration: WorkerRegistrationV1,
        auth: WorkerBootstrapAuth,
        caller_gone: oneshot::Receiver<()>,
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
        self.attachment_or_contain(installation, receiver, caller_gone)
            .await
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
            RelayConnectionLoss::LifecycleWorkerDetached => return Ok(()),
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
            RelayConnectionLoss::LifecycleWorkerDetached => Ok(()),
            RelayConnectionLoss::StaleConnection => {
                self.controller.connection_lost(authority).await.map(|_| ())
            }
        }
    }

    async fn attachment_or_contain<M>(
        &self,
        installation: super::RelayInstallation,
        receiver: mpsc::Receiver<RelayOutboundItem<M>>,
        mut caller_gone: oneshot::Receiver<()>,
    ) -> Result<RelayAttachment<M>, RelayOpenError> {
        let session_id = installation.connection().session_id();
        let mut pairing = installation.pairing().clone();
        let mut budget_releases = self.registry.byte_budget_releases();
        let mut quiesced = installation.quiesced();
        let mut pairing_waiter = None;
        loop {
            match pairing {
                RelayPairingOutcome::AwaitingPeer | RelayPairingOutcome::Active => break,
                RelayPairingOutcome::Backpressured => {
                    if pairing_waiter.is_none() {
                        pairing_waiter = Some(self.registry.begin_control_wait());
                    }
                    pairing = self.registry.retry_pairing(session_id);
                    if pairing != RelayPairingOutcome::Backpressured {
                        continue;
                    }
                    tokio::select! {
                        released = budget_releases.changed() => {
                            if released.is_err() {
                                return Err(RelayOpenError::PeerClosed);
                            }
                        }
                        changed = quiesced.changed() => {
                            if changed.is_err() || *quiesced.borrow() {
                                return Err(RelayOpenError::PeerClosed);
                            }
                        }
                        _ = &mut caller_gone => {
                            drop(pairing_waiter.take());
                            let connection = installation.connection().clone();
                            if let RelayConnectionLoss::ContainmentRequired(ticket) =
                                self.registry.begin_connection_loss(&connection)
                            {
                                persist_containment(
                                    self.controller.as_ref(),
                                    &self.registry,
                                    &ticket,
                                )
                                .await;
                            }
                            return Err(RelayOpenError::PeerClosed);
                        }
                    }
                    pairing = self.registry.retry_pairing(session_id);
                }
                RelayPairingOutcome::ContainmentRequired(ticket) => {
                    drop(pairing_waiter.take());
                    persist_containment(self.controller.as_ref(), &self.registry, &ticket).await;
                    return Err(RelayOpenError::PeerClosed);
                }
                RelayPairingOutcome::Unavailable => return Err(RelayOpenError::PeerClosed),
            }
        }
        Ok(RelayAttachment {
            connection: Some(installation.connection().clone()),
            outbound: receiver,
            quiesced,
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
    use crate::controller::{
        serve_worker_websocket, worker_websocket_config, RendezvousRouteError, WorkerWebSocketError,
    };
    use crate::identity::ScopeId;
    use crate::state::Fence;
    use crate::wire::{
        decode_frame, encode_frame, ActivationResponseV1, BrokerMappingExpectationV1,
        ControllerToWorkerV1, HandshakeOutcomeV1, SessionBindingV1, ValidatedActivationOutcomeV1,
        WireMessage, WorkerToControllerV1, MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES,
        MAX_PROFILE_VERSION_BYTES,
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::io::{duplex, DuplexStream};
    use tokio::sync::Semaphore;
    use tokio::time::{timeout, Duration};
    use tokio_tungstenite::tungstenite::{protocol::Role, Message};
    use tokio_tungstenite::WebSocketStream;
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
        lifecycle_gate: Mutex<Option<Gate>>,
        loss_gate: Mutex<Option<Gate>>,
        lifecycle_outcome: AtomicU8,
        lifecycle_calls: AtomicUsize,
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
                lifecycle_gate: Mutex::new(None),
                loss_gate: Mutex::new(None),
                lifecycle_outcome: AtomicU8::new(0),
                lifecycle_calls: AtomicUsize::new(0),
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

        fn set_lifecycle_gate(&self, gate: Gate) {
            *self.lifecycle_gate.lock().unwrap() = Some(gate);
        }

        fn set_lifecycle_outcome(&self, outcome: LifecycleServiceOutcome) {
            self.lifecycle_outcome.store(
                match outcome {
                    LifecycleServiceOutcome::SuspendAccepted => 0,
                    LifecycleServiceOutcome::ReleasePending => 1,
                    LifecycleServiceOutcome::Released => 2,
                },
                Ordering::SeqCst,
            );
        }

        fn fail_lifecycle(&self) {
            self.lifecycle_outcome.store(3, Ordering::SeqCst);
        }

        fn set_loss_gate(&self, gate: Gate) {
            *self.loss_gate.lock().unwrap() = Some(gate);
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
            let gate = self.loss_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
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

        async fn lifecycle(
            &self,
            _request: &LifecycleRequestV1,
        ) -> Result<LifecycleServiceOutcome, ControllerServiceError> {
            let gate = self.lifecycle_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            self.lifecycle_calls.fetch_add(1, Ordering::SeqCst);
            Ok(match self.lifecycle_outcome.load(Ordering::SeqCst) {
                0 => LifecycleServiceOutcome::SuspendAccepted,
                1 => LifecycleServiceOutcome::ReleasePending,
                2 => LifecycleServiceOutcome::Released,
                _ => return Err(ControllerServiceError::ScopeMismatch),
            })
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

    fn other_binding() -> SessionBinding {
        SessionBinding::new(
            scope_id(),
            SessionId::derive(RAW_SCOPE, "discord:relay-orchestrator-other"),
            Fence::new(1, Uuid::from_u128(0x103)).unwrap(),
            Uuid::from_u128(0x203),
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

    fn lifecycle_request_for(
        binding: &SessionBinding,
        kind: &str,
        request_id: u128,
    ) -> LifecycleRequestV1 {
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "requestId": Uuid::from_u128(request_id),
            "kind": kind,
            "binding": serde_json::to_value(SessionBindingV1::from(binding)).unwrap(),
            "workerSessionId": "opaque-worker-session",
        }))
        .unwrap()
    }

    fn lifecycle_request(kind: &str, request_id: u128) -> LifecycleRequestV1 {
        lifecycle_request_for(&binding(), kind, request_id)
    }

    fn auth() -> WorkerBootstrapAuth {
        WorkerBootstrapAuth::new(POD_UID, &TOKEN).unwrap()
    }

    fn decode_outbound<M: WireMessage>(queued: RelayOutboundItem<M>) -> M {
        let frame = queued.into_encoded_frame().unwrap();
        decode_frame(frame.as_bytes()).unwrap()
    }

    fn websocket_text<M: WireMessage>(message: &M) -> Message {
        Message::Text(String::from_utf8(encode_frame(message).unwrap()).unwrap())
    }

    type TestWorkerWebSocket = WebSocketStream<DuplexStream>;

    async fn worker_websocket_pair() -> (TestWorkerWebSocket, TestWorkerWebSocket) {
        let (worker_io, controller_io) = duplex(256 * 1024);
        let worker = WebSocketStream::from_raw_socket(
            worker_io,
            Role::Client,
            Some(worker_websocket_config()),
        )
        .await;
        let controller = WebSocketStream::from_raw_socket(
            controller_io,
            Role::Server,
            Some(worker_websocket_config()),
        )
        .await;
        (worker, controller)
    }

    async fn receive_websocket_text<M: WireMessage>(socket: &mut TestWorkerWebSocket) -> M {
        let frame = timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("the relay should write a WebSocket frame")
            .expect("the relay should keep the WebSocket open")
            .expect("the relay should write a valid WebSocket frame");
        let Message::Text(text) = frame else {
            panic!("the relay wire protocol must use text frames")
        };
        decode_frame(text.as_bytes()).unwrap()
    }

    fn orchestrator(controller: Arc<FakeController>) -> (RelayOrchestrator, RendezvousRegistry) {
        let budget = RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap();
        orchestrator_with_budget(controller, budget)
    }

    fn orchestrator_with_budget(
        controller: Arc<FakeController>,
        budget: RelayByteBudget,
    ) -> (RelayOrchestrator, RendezvousRegistry) {
        let relay =
            RelayOrchestrator::with_controller(controller, NonZeroUsize::new(1).unwrap(), budget);
        let registry = relay.registry.clone();
        (relay, registry)
    }

    fn install_original_active_pair(
        registry: &RendezvousRegistry,
    ) -> (
        crate::controller::RelayInstallation,
        crate::controller::RelayInstallation,
        mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
        mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>,
    ) {
        install_active_pair_for(registry, binding(), POD_UID)
    }

    fn install_active_pair_for(
        registry: &RendezvousRegistry,
        binding: SessionBinding,
        pod_uid: &str,
    ) -> (
        crate::controller::RelayInstallation,
        crate::controller::RelayInstallation,
        mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
        mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>,
    ) {
        let authority = OrphanAuthority::new(binding.clone(), pod_uid).unwrap();
        let request = ActivationRequestV1::new(
            binding.scope_id(),
            binding.session_id(),
            binding.fence().attempt_id(),
            PROFILE_NAME,
            BrokerMappingExpectationV1::Absent,
        )
        .unwrap();
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

    async fn wait_for_lifecycle_calls(controller: &FakeController, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while controller.lifecycle_calls.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the lifecycle call should be scheduled promptly");
    }

    fn drain_installed_handshake(
        bridge: &mut mpsc::Receiver<RelayOutboundItem<ControllerToBridgeV1>>,
        worker: &mut mpsc::Receiver<RelayOutboundItem<ControllerToWorkerV1>>,
    ) {
        assert!(matches!(
            decode_outbound(bridge.try_recv().unwrap()),
            ControllerToBridgeV1::Activation(_)
        ));
        assert!(matches!(
            decode_outbound(worker.try_recv().unwrap()),
            ControllerToWorkerV1::ProtocolResult(_)
        ));
    }

    async fn wait_for_pairing_pressure(registry: &RendezvousRegistry) {
        timeout(Duration::from_secs(1), async {
            loop {
                match registry.retry_pairing(session_id()) {
                    RelayPairingOutcome::AwaitingPeer => tokio::task::yield_now().await,
                    RelayPairingOutcome::Backpressured => break,
                    outcome => panic!("expected pairing pressure, got {outcome:?}"),
                }
            }
        })
        .await
        .expect("the worker lane should reach bounded pairing");
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
        let worker_item = worker.outbound().recv().await.unwrap();
        let ControllerToWorkerV1::ProtocolResult(worker_result) = decode_outbound(worker_item)
        else {
            panic!("worker must receive handshake ACK")
        };
        assert_eq!(
            worker_result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Ack
        );
        let bridge_item = bridge.outbound().recv().await.unwrap();
        let ControllerToBridgeV1::Activation(activation) = decode_outbound(bridge_item) else {
            panic!("bridge must receive Activated")
        };
        assert!(matches!(
            activation.into_validated_outcome(&request).unwrap(),
            ValidatedActivationOutcomeV1::Activated { worker_cwd, .. }
                if worker_cwd == "/session/workspace"
        ));
    }

    #[tokio::test]
    async fn lifecycle_blocks_both_acp_directions_until_the_correlated_ack_is_written() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("suspend", 0x401);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });

        wait_for_lifecycle_calls(&controller, 1).await;
        assert_eq!(
            registry.route_acp(
                bridge.connection(),
                AcpMessageV1::new(json!({"a": 1})).unwrap()
            ),
            Err(RendezvousRouteError::LifecyclePending)
        );
        assert_eq!(
            registry.route_acp(
                worker.connection(),
                AcpMessageV1::new(json!({"b": 2})).unwrap()
            ),
            Err(RendezvousRouteError::LifecyclePending)
        );

        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        let frame_pointer = frame.as_bytes().as_ptr();
        let (bytes, write_guard) = frame.into_write_parts();
        assert_eq!(bytes.as_ptr(), frame_pointer);
        let ControllerToBridgeV1::ProtocolResult(result) = decode_frame(&bytes).unwrap() else {
            panic!("suspend must emit a correlated protocol result")
        };
        assert_eq!(result.into_lifecycle_outcome(&request).unwrap(), None);
        assert!(!call.is_finished());
        write_guard.mark_written();
        assert_eq!(
            call.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Suspended
        );
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
    }

    #[tokio::test]
    async fn route_capacity_waiter_observes_lane_drain_before_and_after_wait() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.set_lifecycle_outcome(LifecycleServiceOutcome::ReleasePending);
        let (relay, registry) = orchestrator(controller);
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let first = AcpMessageV1::new(json!({"sequence": 1})).unwrap();
        let retry = AcpMessageV1::new(json!({"sequence": 2})).unwrap();
        assert_eq!(
            relay
                .route_acp(bridge.connection(), first.clone())
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert!(matches!(
            relay.route_acp(bridge.connection(), retry.clone()).await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::LaneItems,
                ..
            })
        ));

        {
            let capacity = relay.wait_for_route_capacity(bridge.connection(), &retry);
            tokio::pin!(capacity);
            assert!(timeout(Duration::from_millis(20), &mut capacity)
                .await
                .is_err());
            assert_eq!(
                decode_outbound(worker_outbound.try_recv().unwrap()),
                ControllerToWorkerV1::Acp(first)
            );
            timeout(Duration::from_secs(1), &mut capacity)
                .await
                .expect("draining the peer lane must wake the route waiter")
                .unwrap();
        }
        assert_eq!(
            relay
                .route_acp(bridge.connection(), retry.clone())
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        drop(worker_outbound.try_recv().unwrap());

        let late = AcpMessageV1::new(json!({"sequence": 3})).unwrap();
        let late_retry = AcpMessageV1::new(json!({"sequence": 4})).unwrap();
        assert_eq!(
            relay.route_acp(bridge.connection(), late).await.unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert!(matches!(
            relay
                .route_acp(bridge.connection(), late_retry.clone())
                .await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::LaneItems,
                ..
            })
        ));
        drop(worker_outbound.try_recv().unwrap());
        timeout(
            Duration::from_secs(1),
            relay.wait_for_route_capacity(bridge.connection(), &late_retry),
        )
        .await
        .expect("a lane drained before waiting must be observed")
        .unwrap();

        let fenced_first = AcpMessageV1::new(json!({"sequence": 5})).unwrap();
        let fenced_retry = AcpMessageV1::new(json!({"sequence": 6})).unwrap();
        assert_eq!(
            relay
                .route_acp(bridge.connection(), fenced_first)
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert!(matches!(
            relay
                .route_acp(bridge.connection(), fenced_retry.clone())
                .await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::LaneItems,
                ..
            })
        ));
        let lifecycle = {
            let capacity = relay.wait_for_route_capacity(bridge.connection(), &fenced_retry);
            tokio::pin!(capacity);
            assert!(timeout(Duration::from_millis(20), &mut capacity)
                .await
                .is_err());
            let lifecycle = tokio::spawn({
                let relay = relay.clone();
                let connection = bridge.connection().clone();
                async move {
                    relay
                        .request_lifecycle(&connection, lifecycle_request("release", 0x410))
                        .await
                }
            });
            timeout(Duration::from_secs(1), async {
                while registry.route_target(worker.connection())
                    != Err(RendezvousRouteError::LifecyclePending)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the lifecycle request must fence routing");
            timeout(Duration::from_secs(1), &mut capacity)
                .await
                .expect("lifecycle fencing must wake a lane capacity waiter")
                .unwrap();
            lifecycle
        };
        assert!(matches!(
            relay.route_acp(bridge.connection(), fenced_retry).await,
            Err(RelayDeliveryError::Rendezvous(
                RendezvousRouteError::LifecyclePending
            ))
        ));
        assert_eq!(
            lifecycle.await.unwrap().unwrap(),
            RelayLifecycleOutcome::ReleasePending
        );
    }

    #[tokio::test]
    async fn route_capacity_waiter_observes_byte_release_before_and_after_wait() {
        let controller = Arc::new(FakeController::new(binding()));
        let budget = RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap();
        let (relay, registry) = orchestrator_with_budget(controller, budget.clone());
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let first = AcpMessageV1::new(json!({"sequence": 1})).unwrap();
        let held_bytes = budget.hold_for_test(budget.available_bytes());
        assert!(matches!(
            relay.route_acp(bridge.connection(), first.clone()).await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::ProcessBytes,
                ..
            })
        ));

        {
            let capacity = relay.wait_for_route_capacity(bridge.connection(), &first);
            tokio::pin!(capacity);
            assert!(timeout(Duration::from_millis(20), &mut capacity)
                .await
                .is_err());
            drop(held_bytes);
            timeout(Duration::from_secs(1), &mut capacity)
                .await
                .expect("releasing byte capacity must wake the route waiter")
                .unwrap();
        }
        assert_eq!(
            relay.route_acp(bridge.connection(), first).await.unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        drop(worker_outbound.try_recv().unwrap());

        let late = AcpMessageV1::new(json!({"sequence": 2})).unwrap();
        let held_bytes = budget.hold_for_test(budget.available_bytes());
        assert!(matches!(
            relay.route_acp(bridge.connection(), late.clone()).await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::ProcessBytes,
                ..
            })
        ));
        drop(held_bytes);
        timeout(
            Duration::from_secs(1),
            relay.wait_for_route_capacity(bridge.connection(), &late),
        )
        .await
        .expect("byte capacity released before waiting must be observed")
        .unwrap();
        assert_eq!(
            relay.route_acp(bridge.connection(), late).await.unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        drop(worker_outbound.try_recv().unwrap());
    }

    #[tokio::test]
    async fn closed_peer_wakes_a_process_byte_waiter_before_budget_release() {
        let controller = Arc::new(FakeController::new(binding()));
        let budget = RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap();
        let (relay, registry) = orchestrator_with_budget(controller, budget.clone());
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let held_bytes = budget.hold_for_test(budget.available_bytes());
        let retry = AcpMessageV1::new(json!({"sequence": 1})).unwrap();
        assert!(matches!(
            relay.route_acp(bridge.connection(), retry.clone()).await,
            Ok(RelayAcpDeliveryOutcome::Backpressured {
                reason: RelayBackpressure::ProcessBytes,
                ..
            })
        ));

        {
            let capacity = relay.wait_for_route_capacity(bridge.connection(), &retry);
            tokio::pin!(capacity);
            assert!(timeout(Duration::from_millis(20), &mut capacity)
                .await
                .is_err());
            drop(worker_outbound);
            timeout(Duration::from_secs(1), &mut capacity)
                .await
                .expect("a closed peer must wake the route waiter")
                .unwrap();
        }
        assert_eq!(budget.available_bytes(), 0);
        assert_eq!(
            relay.route_acp(bridge.connection(), retry).await.unwrap(),
            RelayAcpDeliveryOutcome::PeerContained(OrphanContainmentOutcome::ContainmentAccepted)
        );
        assert_eq!(budget.available_bytes(), 0);
        drop(held_bytes);
    }

    #[tokio::test]
    async fn lifecycle_transition_wakes_a_process_byte_capacity_waiter() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.set_lifecycle_outcome(LifecycleServiceOutcome::ReleasePending);
        let budget = RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap();
        let (relay, registry) = orchestrator_with_budget(Arc::clone(&controller), budget.clone());
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let held_bytes = budget.hold_for_test(budget.available_bytes());
        let retry = AcpMessageV1::new(json!({"sequence": 1})).unwrap();
        assert_eq!(
            relay
                .route_acp(worker.connection(), retry.clone())
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Backpressured {
                message: retry.clone(),
                reason: RelayBackpressure::ProcessBytes,
            }
        );

        let lifecycle = {
            let capacity = relay.wait_for_route_capacity(worker.connection(), &retry);
            tokio::pin!(capacity);
            assert!(timeout(Duration::from_millis(20), &mut capacity)
                .await
                .is_err());
            let lifecycle = tokio::spawn({
                let relay = relay.clone();
                let connection = bridge.connection().clone();
                async move {
                    relay
                        .request_lifecycle(&connection, lifecycle_request("release", 0x40f))
                        .await
                }
            });
            timeout(Duration::from_secs(1), async {
                while registry.route_target(worker.connection())
                    != Err(RendezvousRouteError::LifecyclePending)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the lifecycle request must fence routing");
            timeout(Duration::from_secs(1), &mut capacity)
                .await
                .expect("lifecycle fencing must wake ACP capacity waiters")
                .unwrap();
            lifecycle
        };
        assert!(matches!(
            relay.route_acp(worker.connection(), retry).await,
            Err(RelayDeliveryError::Rendezvous(
                RendezvousRouteError::LifecyclePending
            ))
        ));

        drop(held_bytes);
        assert_eq!(
            lifecycle.await.unwrap().unwrap(),
            RelayLifecycleOutcome::ReleasePending
        );
    }

    #[tokio::test]
    async fn release_pending_keeps_the_bridge_and_exact_request_for_a_later_pass() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.set_lifecycle_outcome(LifecycleServiceOutcome::ReleasePending);
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("release", 0x402);

        assert_eq!(
            relay
                .request_lifecycle(bridge.connection(), request.clone())
                .await
                .unwrap(),
            RelayLifecycleOutcome::ReleasePending
        );
        assert!(matches!(
            bridge_outbound.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            registry.begin_connection_loss(worker.connection()),
            RelayConnectionLoss::LifecycleWorkerDetached
        );
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.route_acp(
                bridge.connection(),
                AcpMessageV1::new(json!({"x": 1})).unwrap()
            ),
            Err(RendezvousRouteError::LifecyclePending)
        );

        controller.set_lifecycle_outcome(LifecycleServiceOutcome::Released);
        let final_call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        wait_for_lifecycle_calls(&controller, 2).await;
        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        let ControllerToBridgeV1::ProtocolResult(result) = decode_frame(frame.as_bytes()).unwrap()
        else {
            panic!("final release must emit one correlated protocol result")
        };
        assert_eq!(result.into_lifecycle_outcome(&request).unwrap(), None);
        frame.mark_written();
        assert_eq!(
            final_call.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Released
        );
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn accepted_release_survives_retry_pressure_worker_detach_and_controller_error() {
        let budget = RelayByteBudget::new(NonZeroUsize::new(4 * 64 * 1024).unwrap()).unwrap();
        let controller = Arc::new(FakeController::new(binding()));
        controller.set_lifecycle_outcome(LifecycleServiceOutcome::ReleasePending);
        let (relay, registry) = orchestrator_with_budget(Arc::clone(&controller), budget.clone());
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("release", 0x411);
        assert_eq!(
            relay
                .request_lifecycle(bridge.connection(), request.clone())
                .await
                .unwrap(),
            RelayLifecycleOutcome::ReleasePending
        );

        let held_bytes = budget.hold_for_test(4 * 64 * 1024);
        controller.fail_lifecycle();
        let retry = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        timeout(Duration::from_secs(1), async {
            while budget.control_waiters_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the accepted release retry must wait for process bytes");
        assert_eq!(
            registry.begin_connection_loss(worker.connection()),
            RelayConnectionLoss::LifecycleWorkerDetached
        );
        drop(held_bytes);
        assert!(matches!(
            retry.await.unwrap(),
            Err(RelayLifecycleError::Controller(
                ControllerServiceError::ScopeMismatch
            ))
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::LifecyclePending)
        );

        controller.set_lifecycle_outcome(LifecycleServiceOutcome::Released);
        let final_call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        wait_for_lifecycle_calls(&controller, 3).await;
        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        frame.mark_written();
        assert_eq!(
            final_call.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Released
        );
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn lifecycle_reserves_bridge_capacity_before_controller_mutation() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        assert_eq!(
            registry.route_acp(
                worker.connection(),
                AcpMessageV1::new(json!({"queued": true})).unwrap(),
            ),
            Ok(AcpRouteOutcome::Delivered)
        );
        let request = lifecycle_request("suspend", 0x403);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        tokio::task::yield_now().await;
        assert_eq!(controller.lifecycle_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::LifecyclePending)
        );

        let queued_acp = bridge_outbound.recv().await.unwrap();
        drop(queued_acp);
        wait_for_lifecycle_calls(&controller, 1).await;
        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        frame.mark_written();
        assert_eq!(
            call.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Suspended
        );
    }

    #[tokio::test]
    async fn closed_bridge_result_queue_contains_before_controller_mutation() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        drop(bridge_outbound);

        assert!(matches!(
            relay
                .request_lifecycle(bridge.connection(), lifecycle_request("suspend", 0x40c))
                .await,
            Err(RelayLifecycleError::ResultDeliveryLost)
        ));
        assert_eq!(controller.lifecycle_calls.load(Ordering::SeqCst), 0);
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn lifecycle_control_reservation_cannot_be_starved_by_new_acp() {
        let budget = RelayByteBudget::new(
            NonZeroUsize::new(crate::controller::MIN_RELAY_BYTE_BUDGET).unwrap(),
        )
        .unwrap();
        let controller_a = Arc::new(FakeController::new(binding()));
        let controller_b = Arc::new(FakeController::new(other_binding()));
        let (relay_a, registry_a) =
            orchestrator_with_budget(Arc::clone(&controller_a), budget.clone());
        let (_relay_b, registry_b) =
            orchestrator_with_budget(Arc::clone(&controller_b), budget.clone());
        let (bridge_a, _worker_a, mut bridge_outbound_a, mut worker_outbound_a) =
            install_active_pair_for(&registry_a, binding(), POD_UID);
        drain_installed_handshake(&mut bridge_outbound_a, &mut worker_outbound_a);
        let (bridge_b, _worker_b, mut bridge_outbound_b, mut worker_outbound_b) =
            install_active_pair_for(&registry_b, other_binding(), "worker-pod-uid-other");
        drain_installed_handshake(&mut bridge_outbound_b, &mut worker_outbound_b);

        let held_message = AcpMessageV1::new(json!({"held": true})).unwrap();
        assert_eq!(
            registry_a.route_acp(bridge_a.connection(), held_message),
            Ok(AcpRouteOutcome::Delivered)
        );
        let held_item = worker_outbound_a.recv().await.unwrap();
        let request = lifecycle_request("suspend", 0x408);
        let call = tokio::spawn({
            let relay = relay_a.clone();
            let connection = bridge_a.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        timeout(Duration::from_secs(1), async {
            while budget.control_waiters_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lifecycle must register control-byte priority");
        assert_eq!(controller_a.lifecycle_calls.load(Ordering::SeqCst), 0);

        drop(held_item);
        let competing = AcpMessageV1::new(json!({"competing": true})).unwrap();
        assert_eq!(
            registry_b.route_acp(bridge_b.connection(), competing.clone()),
            Ok(AcpRouteOutcome::Backpressured {
                message: competing.clone(),
                reason: RelayBackpressure::ProcessBytes,
            })
        );
        wait_for_lifecycle_calls(&controller_a, 1).await;
        assert_eq!(budget.control_waiters_for_test(), 0);
        let frame = bridge_outbound_a
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        frame.mark_written();
        assert_eq!(
            call.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Suspended
        );
        assert_eq!(
            registry_b.route_acp(bridge_b.connection(), competing.clone()),
            Ok(AcpRouteOutcome::Delivered)
        );
        assert_eq!(
            decode_outbound(worker_outbound_b.recv().await.unwrap()),
            ControllerToWorkerV1::Acp(competing)
        );
    }

    #[tokio::test]
    async fn conflicting_lifecycle_request_contains_the_exact_pair_without_ack() {
        let controller = Arc::new(FakeController::new(binding()));
        let gate = Gate::new();
        controller.set_lifecycle_gate(gate.clone());
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("suspend", 0x404);
        let first = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        gate.started.acquire().await.unwrap().forget();

        assert_eq!(
            relay
                .request_lifecycle(bridge.connection(), request.clone())
                .await
                .unwrap(),
            RelayLifecycleOutcome::Coalesced
        );
        let mut conflicting_value = serde_json::to_value(&request).unwrap();
        conflicting_value["workerSessionId"] = json!("different-worker-session");
        let conflicting: LifecycleRequestV1 = serde_json::from_value(conflicting_value).unwrap();
        assert!(matches!(
            relay
                .request_lifecycle(bridge.connection(), conflicting)
                .await,
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::ConflictingRequest
            ))
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        gate.release.add_permits(1);
        assert!(matches!(
            first.await.unwrap(),
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::ReservationLost
            ))
        ));
        assert!(matches!(
            bridge_outbound.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn worker_lane_and_foreign_binding_are_rejected_before_controller_io() {
        let wrong_lane_controller = Arc::new(FakeController::new(binding()));
        let (wrong_lane_relay, wrong_lane_registry) =
            orchestrator(Arc::clone(&wrong_lane_controller));
        let (_bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&wrong_lane_registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        assert!(matches!(
            wrong_lane_relay
                .request_lifecycle(worker.connection(), lifecycle_request("suspend", 0x40a))
                .await,
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::WrongLane
            ))
        ));
        assert_eq!(
            wrong_lane_controller.lifecycle_calls.load(Ordering::SeqCst),
            0
        );
        assert_eq!(wrong_lane_controller.losses.load(Ordering::SeqCst), 1);

        let wrong_binding_controller = Arc::new(FakeController::new(binding()));
        let (wrong_binding_relay, wrong_binding_registry) =
            orchestrator(Arc::clone(&wrong_binding_controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&wrong_binding_registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        assert!(matches!(
            wrong_binding_relay
                .request_lifecycle(
                    bridge.connection(),
                    lifecycle_request_for(&other_binding(), "suspend", 0x40b),
                )
                .await,
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::BindingMismatch
            ))
        ));
        assert_eq!(
            wrong_binding_controller
                .lifecycle_calls
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(wrong_binding_controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ambiguous_controller_error_keeps_the_exact_request_fail_closed_for_retry() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.fail_lifecycle();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("release", 0x405);

        assert!(matches!(
            relay
                .request_lifecycle(bridge.connection(), request.clone())
                .await,
            Err(RelayLifecycleError::Controller(
                ControllerServiceError::ScopeMismatch
            ))
        ));
        assert_eq!(
            registry.route_acp(
                bridge.connection(),
                AcpMessageV1::new(json!({"a": 1})).unwrap()
            ),
            Err(RendezvousRouteError::LifecyclePending)
        );
        assert_eq!(
            registry.route_acp(
                worker.connection(),
                AcpMessageV1::new(json!({"b": 2})).unwrap()
            ),
            Err(RendezvousRouteError::LifecyclePending)
        );
        assert!(matches!(
            bridge_outbound.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        controller.set_lifecycle_outcome(LifecycleServiceOutcome::Released);
        let retry = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            let request = request.clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        wait_for_lifecycle_calls(&controller, 2).await;
        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        frame.mark_written();
        assert_eq!(
            retry.await.unwrap().unwrap(),
            RelayLifecycleOutcome::Released
        );
    }

    #[tokio::test]
    async fn worker_loss_after_controller_error_requires_containment() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.fail_lifecycle();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);

        assert!(matches!(
            relay
                .request_lifecycle(bridge.connection(), lifecycle_request("release", 0x40e))
                .await,
            Err(RelayLifecycleError::Controller(
                ControllerServiceError::ScopeMismatch
            ))
        ));
        let RelayConnectionLoss::ContainmentRequired(ticket) =
            registry.begin_connection_loss(worker.connection())
        else {
            panic!("worker loss after a failed lifecycle pass must be contained")
        };
        assert_eq!(ticket.trigger(), worker.connection());
        controller
            .connection_lost(ticket.authority())
            .await
            .unwrap();
        assert_eq!(
            registry.complete_containment(&ticket),
            RelayContainmentCompletion::Removed
        );
    }

    #[tokio::test]
    async fn worker_loss_before_controller_io_requires_containment() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        assert_eq!(
            registry.route_acp(
                worker.connection(),
                AcpMessageV1::new(json!({"occupiesBridgeQueue": true})).unwrap(),
            ),
            Ok(AcpRouteOutcome::Delivered)
        );
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move {
                relay
                    .request_lifecycle(&connection, lifecycle_request("suspend", 0x40f))
                    .await
            }
        });
        timeout(Duration::from_secs(1), async {
            while registry.route_target(bridge.connection())
                != Err(RendezvousRouteError::LifecyclePending)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the lifecycle request must fence ACP before waiting for queue space");

        let RelayConnectionLoss::ContainmentRequired(ticket) =
            registry.begin_connection_loss(worker.connection())
        else {
            panic!("pre-controller worker loss must be contained")
        };
        assert_eq!(controller.lifecycle_calls.load(Ordering::SeqCst), 0);
        controller
            .connection_lost(ticket.authority())
            .await
            .unwrap();
        assert_eq!(
            registry.complete_containment(&ticket),
            RelayContainmentCompletion::Removed
        );
        let result = timeout(Duration::from_secs(1), call)
            .await
            .expect("quiescing must cancel the lifecycle queue-capacity wait")
            .unwrap();
        assert!(matches!(
            result,
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::Quiescing
            ))
        ));
        drop(bridge_outbound.recv().await.unwrap());
    }

    #[tokio::test]
    async fn quiescing_cancels_lifecycle_byte_wait_without_waiting_for_a_release() {
        let budget = RelayByteBudget::new(
            NonZeroUsize::new(crate::controller::MIN_RELAY_BYTE_BUDGET).unwrap(),
        )
        .unwrap();
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator_with_budget(Arc::clone(&controller), budget.clone());
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let held_bytes = budget.hold_for_test(crate::controller::MIN_RELAY_BYTE_BUDGET);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move {
                relay
                    .request_lifecycle(&connection, lifecycle_request("suspend", 0x410))
                    .await
            }
        });
        timeout(Duration::from_secs(1), async {
            while budget.control_waiters_for_test() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the lifecycle request must wait for process bytes");

        let RelayConnectionLoss::ContainmentRequired(ticket) =
            registry.begin_connection_loss(worker.connection())
        else {
            panic!("worker loss before controller I/O must quiesce the byte waiter")
        };
        controller
            .connection_lost(ticket.authority())
            .await
            .unwrap();
        registry.complete_containment(&ticket);
        let result = timeout(Duration::from_secs(1), call)
            .await
            .expect("quiescing must wake the lifecycle byte wait")
            .unwrap();
        assert!(matches!(
            result,
            Err(RelayLifecycleError::Rendezvous(
                RendezvousLifecycleError::Quiescing
            ))
        ));
        assert_eq!(controller.lifecycle_calls.load(Ordering::SeqCst), 0);
        assert_eq!(budget.control_waiters_for_test(), 0);
        drop(held_bytes);
    }

    #[tokio::test]
    async fn controller_error_contains_a_worker_that_detached_during_the_lifecycle_pass() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.fail_lifecycle();
        let gate = Gate::new();
        controller.set_lifecycle_gate(gate.clone());
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move {
                relay
                    .request_lifecycle(&connection, lifecycle_request("release", 0x40d))
                    .await
            }
        });
        gate.started.acquire().await.unwrap().forget();
        assert_eq!(
            registry.begin_connection_loss(worker.connection()),
            RelayConnectionLoss::LifecycleWorkerDetached
        );
        gate.release.add_permits(1);

        assert!(matches!(
            call.await.unwrap(),
            Err(RelayLifecycleError::Controller(
                ControllerServiceError::ScopeMismatch
            ))
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
    }

    #[tokio::test]
    async fn dropping_a_lifecycle_result_frame_contains_instead_of_completing() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let request = lifecycle_request("suspend", 0x406);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move { relay.request_lifecycle(&connection, request).await }
        });
        wait_for_lifecycle_calls(&controller, 1).await;

        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        let (_bytes, write_guard) = frame.into_write_parts();
        drop(write_guard);
        assert!(matches!(
            call.await.unwrap(),
            Err(RelayLifecycleError::ResultDeliveryLost)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
        assert!(registry.pending_containments().is_empty());
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
    }

    #[tokio::test]
    async fn cancelling_the_lifecycle_caller_cannot_cancel_mutation_or_delivery_fencing() {
        let controller = Arc::new(FakeController::new(binding()));
        let gate = Gate::new();
        controller.set_lifecycle_gate(gate.clone());
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move {
                relay
                    .request_lifecycle(&connection, lifecycle_request("suspend", 0x407))
                    .await
            }
        });
        gate.started.acquire().await.unwrap().forget();
        call.abort();
        gate.release.add_permits(1);

        wait_for_lifecycle_calls(&controller, 1).await;
        let frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();
        frame.mark_written();
        timeout(Duration::from_secs(1), async {
            while registry.route_target(bridge.connection())
                != Err(RendezvousRouteError::StaleConnection)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the controller-owned task must finish after its caller is gone");
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_write_completion_cannot_remove_a_replacement_generation() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let (bridge, _worker, mut bridge_outbound, mut worker_outbound) =
            install_original_active_pair(&registry);
        drain_installed_handshake(&mut bridge_outbound, &mut worker_outbound);
        let call = tokio::spawn({
            let relay = relay.clone();
            let connection = bridge.connection().clone();
            async move {
                relay
                    .request_lifecycle(&connection, lifecycle_request("suspend", 0x409))
                    .await
            }
        });
        wait_for_lifecycle_calls(&controller, 1).await;
        let stale_frame = bridge_outbound
            .recv()
            .await
            .unwrap()
            .into_encoded_frame()
            .unwrap();

        let RelayConnectionLoss::ContainmentRequired(ticket) =
            registry.begin_connection_loss(bridge.connection())
        else {
            panic!("closing the result-owning bridge must require containment")
        };
        controller
            .connection_lost(ticket.authority())
            .await
            .unwrap();
        assert_eq!(
            registry.complete_containment(&ticket),
            RelayContainmentCompletion::Removed
        );
        let (
            replacement_bridge,
            replacement_worker,
            mut replacement_bridge_outbound,
            mut replacement_worker_outbound,
        ) = install_active_pair_for(&registry, replacement_binding(), "replacement-pod-uid");
        drain_installed_handshake(
            &mut replacement_bridge_outbound,
            &mut replacement_worker_outbound,
        );

        stale_frame.mark_written();
        assert!(matches!(
            call.await.unwrap(),
            Err(RelayLifecycleError::ResultDeliveryLost)
        ));
        assert_eq!(
            registry.route_target(replacement_bridge.connection()),
            Ok(replacement_worker.connection().connection_id())
        );
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
    async fn pairing_retries_after_transient_process_byte_pressure() {
        let controller = Arc::new(FakeController::new(binding()));
        let limit = 4 * 64 * 1024;
        let budget = RelayByteBudget::new(NonZeroUsize::new(limit).unwrap()).unwrap();
        let held = budget.hold_for_test(limit);
        let (relay, registry) = orchestrator_with_budget(controller, budget);
        let request = activation_request(BrokerMappingExpectationV1::Absent);
        let BridgeOpenOutcome::Attached(mut bridge) =
            relay.activate_bridge(request.clone()).await.unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut worker_call = tokio::spawn({
            let relay = relay.clone();
            async move { relay.register_worker(registration(), auth()).await }
        });

        wait_for_pairing_pressure(&registry).await;
        assert!(!worker_call.is_finished());
        drop(held);

        let mut worker = timeout(Duration::from_secs(1), &mut worker_call)
            .await
            .expect("budget release must wake pairing")
            .unwrap()
            .unwrap();
        let worker_item = worker.outbound().try_recv().unwrap();
        let ControllerToWorkerV1::ProtocolResult(result) = decode_outbound(worker_item) else {
            panic!("worker must receive exactly one handshake result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Ack
        );
        let bridge_item = bridge.outbound().try_recv().unwrap();
        let ControllerToBridgeV1::Activation(activation) = decode_outbound(bridge_item) else {
            panic!("bridge must receive exactly one activation result")
        };
        assert!(matches!(
            activation.into_validated_outcome(&request).unwrap(),
            ValidatedActivationOutcomeV1::Activated { .. }
        ));
    }

    #[tokio::test]
    async fn cancelled_pairing_wait_releases_global_admission_before_containment_io() {
        let controller = Arc::new(FakeController::new(binding()));
        let limit = 4 * 64 * 1024;
        let budget = RelayByteBudget::new(NonZeroUsize::new(limit).unwrap()).unwrap();
        let active_registry = RendezvousRegistry::with_byte_budget(scope_id(), budget.clone());
        let (active_bridge, _active_worker, mut active_bridge_outbound, mut active_worker_outbound) =
            install_original_active_pair(&active_registry);
        drop(active_bridge_outbound.try_recv().unwrap());
        drop(active_worker_outbound.try_recv().unwrap());
        let held = budget.hold_for_test(limit);
        let (relay, registry) = orchestrator_with_budget(Arc::clone(&controller), budget);
        let BridgeOpenOutcome::Attached(bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let loss_gate = Gate::new();
        controller.set_loss_gate(loss_gate.clone());
        let call = tokio::spawn({
            let relay = relay.clone();
            async move { relay.register_worker(registration(), auth()).await }
        });

        wait_for_pairing_pressure(&registry).await;
        call.abort();
        let _ = call.await;
        loss_gate.started.acquire().await.unwrap().forget();

        drop(held);
        let message = AcpMessageV1::new(serde_json::json!({"jsonrpc": "2.0"})).unwrap();
        assert_eq!(
            active_registry.route_acp(active_bridge.connection(), message.clone()),
            Ok(AcpRouteOutcome::Delivered)
        );
        assert_eq!(
            decode_outbound(active_worker_outbound.try_recv().unwrap()),
            ControllerToWorkerV1::Acp(message)
        );

        loss_gate.release.add_permits(1);
        wait_for_losses(&controller, 1).await;
        assert_eq!(
            registry.route_target(bridge.connection()),
            Err(RendezvousRouteError::StaleConnection)
        );
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

    #[tokio::test]
    async fn orchestrator_routes_acp_to_the_exact_active_attachment() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(controller);
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut worker = relay.register_worker(registration(), auth()).await.unwrap();
        drop(bridge.outbound().recv().await.unwrap());
        drop(worker.outbound().recv().await.unwrap());
        let message = AcpMessageV1::new(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/prompt"
        }))
        .unwrap();

        assert!(matches!(
            relay.route_acp(bridge.connection(), message.clone()).await,
            Ok(RelayAcpDeliveryOutcome::Delivered)
        ));
        let queued = worker.outbound().recv().await.unwrap();
        assert_eq!(decode_outbound(queued), ControllerToWorkerV1::Acp(message));
    }

    #[test]
    fn worker_websocket_limits_cover_the_wire_ceiling() {
        let config = worker_websocket_config();
        assert_eq!(config.max_message_size, Some(MAX_ACP_FRAME_BYTES));
        assert_eq!(config.max_frame_size, Some(MAX_ACP_FRAME_BYTES));
        assert!(!config.accept_unmasked_frames);
        assert_eq!(config.write_buffer_size, 0);
        assert_eq!(
            config.max_write_buffer_size,
            MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES
        );
    }

    #[tokio::test]
    async fn worker_websocket_registers_once_and_routes_both_directions() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay.clone(),
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));

        worker_socket
            .send(Message::Ping(vec![0x14, 0x61]))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), worker_socket.next())
                .await
                .expect("the worker ping should be flushed")
                .expect("the relay should keep the WebSocket open")
                .expect("the relay should send a valid pong"),
            Message::Pong(vec![0x14, 0x61])
        );
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let ControllerToWorkerV1::ProtocolResult(result) =
            receive_websocket_text(&mut worker_socket).await
        else {
            panic!("the first controller frame must be the pairing result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Ack
        );
        worker_socket
            .send(Message::Ping(vec![0x14, 0x62]))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), worker_socket.next())
                .await
                .expect("the registered worker ping should be flushed")
                .expect("the relay should keep the registered socket open")
                .expect("the relay should send a valid registered pong"),
            Message::Pong(vec![0x14, 0x62])
        );
        assert!(matches!(
            decode_outbound(bridge.outbound().recv().await.unwrap()),
            ControllerToBridgeV1::Activation(_)
        ));

        let from_worker = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/prompt",
            "params": {"prompt": "from worker"}
        }))
        .unwrap();
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Acp(
                from_worker.clone(),
            )))
            .await
            .unwrap();
        assert_eq!(
            decode_outbound(
                timeout(Duration::from_secs(1), bridge.outbound().recv())
                    .await
                    .expect("worker ACP should reach the bridge")
                    .unwrap()
            ),
            ControllerToBridgeV1::Acp(from_worker)
        );

        let to_worker = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "id": 7,
            "result": {"stopReason": "end_turn"}
        }))
        .unwrap();
        assert_eq!(
            relay
                .route_acp(bridge.connection(), to_worker.clone())
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert_eq!(
            receive_websocket_text::<ControllerToWorkerV1>(&mut worker_socket).await,
            ControllerToWorkerV1::Acp(to_worker)
        );

        worker_socket.close(None).await.unwrap();
        let outcome = timeout(Duration::from_secs(1), driver)
            .await
            .expect("worker close should complete containment")
            .expect("worker driver task should not panic")
            .expect("worker close should be contained");
        assert_eq!(
            outcome,
            RelayLossOutcome::Contained(OrphanContainmentOutcome::ContainmentAccepted)
        );
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_websocket_rejects_acp_before_registration() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        let acp = AcpMessageV1::new(json!({"jsonrpc": "2.0", "method": "initialize"})).unwrap();

        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Acp(acp)))
            .await
            .unwrap();
        let ControllerToWorkerV1::ProtocolResult(result) =
            receive_websocket_text(&mut worker_socket).await
        else {
            panic!("a rejected handshake must return a sanitized result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Fatal(FatalCode::InvalidMessage)
        );
        assert!(matches!(
            driver.await.unwrap(),
            Err(WorkerWebSocketError::ExpectedRegistration)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn binary_acp_after_registration_is_contained_without_routing() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let _: ControllerToWorkerV1 = receive_websocket_text(&mut worker_socket).await;
        drop(bridge.outbound().recv().await.unwrap());

        let acp = WorkerToControllerV1::Acp(
            AcpMessageV1::new(json!({"jsonrpc": "2.0", "method": "session/prompt"})).unwrap(),
        );
        worker_socket
            .send(Message::Binary(encode_frame(&acp).unwrap()))
            .await
            .unwrap();

        assert!(matches!(
            timeout(Duration::from_secs(1), driver)
                .await
                .expect("binary ACP should terminate the worker driver")
                .unwrap(),
            Err(WorkerWebSocketError::ExpectedTextAcp)
        ));
        assert!(matches!(
            bridge.outbound().try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_websocket_rejects_binary_registration_without_decoding_it() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));

        worker_socket
            .send(Message::Binary(
                encode_frame(&WorkerToControllerV1::Registration(registration())).unwrap(),
            ))
            .await
            .unwrap();
        let ControllerToWorkerV1::ProtocolResult(result) =
            receive_websocket_text(&mut worker_socket).await
        else {
            panic!("a rejected binary frame must return a sanitized result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Fatal(FatalCode::InvalidMessage)
        );
        assert!(matches!(
            driver.await.unwrap(),
            Err(WorkerWebSocketError::ExpectedTextRegistration)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn worker_websocket_registration_timeout_is_one_fixed_deadline() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_millis(20),
            Duration::from_secs(1),
        ));

        let ControllerToWorkerV1::ProtocolResult(result) =
            receive_websocket_text(&mut worker_socket).await
        else {
            panic!("a timed-out handshake must return a sanitized result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Fatal(FatalCode::Unavailable)
        );
        assert!(matches!(
            driver.await.unwrap(),
            Err(WorkerWebSocketError::RegistrationTimedOut)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn worker_websocket_retains_backpressured_frames_in_order() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let ControllerToWorkerV1::ProtocolResult(result) =
            receive_websocket_text(&mut worker_socket).await
        else {
            panic!("the worker must receive its pairing result")
        };
        assert_eq!(
            result.into_handshake_outcome().unwrap(),
            HandshakeOutcomeV1::Ack
        );
        worker_socket
            .send(Message::Ping(vec![0x14, 0x70]))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), worker_socket.next())
                .await
                .expect("the backpressure test reader should be active")
                .unwrap()
                .unwrap(),
            Message::Pong(vec![0x14, 0x70])
        );

        let first = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sequence": 1}
        }))
        .unwrap();
        let second = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sequence": 2}
        }))
        .unwrap();
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Acp(first.clone())))
            .await
            .unwrap();
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Acp(second.clone())))
            .await
            .unwrap();
        worker_socket
            .send(Message::Ping(vec![0x14, 0x71]))
            .await
            .unwrap();

        assert!(
            timeout(Duration::from_millis(100), worker_socket.next())
                .await
                .is_err(),
            "the reader must stop at the first backpressured ACP frame"
        );

        assert!(matches!(
            decode_outbound(bridge.outbound().recv().await.unwrap()),
            ControllerToBridgeV1::Activation(_)
        ));
        assert_eq!(
            decode_outbound(
                timeout(Duration::from_secs(1), bridge.outbound().recv())
                    .await
                    .expect("the first retained ACP frame should be retried")
                    .unwrap()
            ),
            ControllerToBridgeV1::Acp(first)
        );
        assert_eq!(
            decode_outbound(
                timeout(Duration::from_secs(1), bridge.outbound().recv())
                    .await
                    .expect("the second ACP frame should follow the first")
                    .unwrap()
            ),
            ControllerToBridgeV1::Acp(second)
        );
        assert_eq!(
            timeout(Duration::from_secs(1), worker_socket.next())
                .await
                .expect("the reader should resume after exact ACP retries")
                .unwrap()
                .unwrap(),
            Message::Pong(vec![0x14, 0x71])
        );

        worker_socket.close(None).await.unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(1), driver)
                .await
                .expect("worker close should complete containment")
                .unwrap()
                .unwrap(),
            RelayLossOutcome::Contained(OrphanContainmentOutcome::ContainmentAccepted)
        ));
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_worker_websocket_fails_closed_through_attachment_drop() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay,
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let _: ControllerToWorkerV1 = receive_websocket_text(&mut worker_socket).await;
        drop(bridge.outbound().recv().await.unwrap());

        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
        wait_for_losses(&controller, 1).await;
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stalled_worker_write_times_out_and_releases_the_global_budget() {
        let controller = Arc::new(FakeController::new(binding()));
        let large = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"chunk": "x".repeat(512 * 1024)}
        }))
        .unwrap();
        let limit = large.encoded_payload_bytes() + MAX_CONTROL_FRAME_BYTES;
        let budget = RelayByteBudget::new(NonZeroUsize::new(limit).unwrap()).unwrap();
        let (relay, _registry) = orchestrator_with_budget(Arc::clone(&controller), budget.clone());
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay.clone(),
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_millis(20),
        ));
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let _: ControllerToWorkerV1 = receive_websocket_text(&mut worker_socket).await;
        drop(bridge.outbound().recv().await.unwrap());
        assert_eq!(budget.available_bytes(), limit);

        assert_eq!(
            relay.route_acp(bridge.connection(), large).await.unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert_eq!(budget.available_bytes(), 0);
        assert!(matches!(
            timeout(Duration::from_secs(1), driver)
                .await
                .expect("a stalled worker write must reach its deadline")
                .unwrap(),
            Err(WorkerWebSocketError::WriteTimedOut)
        ));
        assert_eq!(budget.available_bytes(), limit);
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_ping_flood_cannot_starve_outbound_acp() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, _registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let (mut worker_socket, controller_socket) = worker_websocket_pair().await;
        let driver = tokio::spawn(serve_worker_websocket(
            relay.clone(),
            controller_socket,
            auth(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        worker_socket
            .send(websocket_text(&WorkerToControllerV1::Registration(
                registration(),
            )))
            .await
            .unwrap();
        let _: ControllerToWorkerV1 = receive_websocket_text(&mut worker_socket).await;
        drop(bridge.outbound().recv().await.unwrap());

        let (mut ping_sink, mut response_stream) = worker_socket.split();
        let flood = tokio::spawn(async move {
            while ping_sink
                .send(Message::Ping(vec![0x14, 0x63]))
                .await
                .is_ok()
            {}
        });
        tokio::task::yield_now().await;
        let expected = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"mustNotStarve": true}
        }))
        .unwrap();
        assert_eq!(
            relay
                .route_acp(bridge.connection(), expected.clone())
                .await
                .unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );

        timeout(Duration::from_secs(1), async {
            loop {
                match response_stream.next().await.unwrap().unwrap() {
                    Message::Text(text) => {
                        let frame: ControllerToWorkerV1 = decode_frame(text.as_bytes()).unwrap();
                        if frame == ControllerToWorkerV1::Acp(expected.clone()) {
                            break;
                        }
                    }
                    Message::Pong(_) => {}
                    frame => panic!("unexpected worker flood response: {frame:?}"),
                }
            }
        })
        .await
        .expect("Ping traffic must not starve queued ACP");

        flood.abort();
        let _ = flood.await;
        drop(response_stream);
        driver.abort();
        let _ = driver.await;
        wait_for_losses(&controller, 1).await;
        assert_eq!(controller.losses.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_loss_releases_queued_bytes_before_controller_io() {
        let controller = Arc::new(FakeController::new(binding()));
        let limit = 4 * 64 * 1024;
        let budget = RelayByteBudget::new(NonZeroUsize::new(limit).unwrap()).unwrap();
        let (relay, _registry) = orchestrator_with_budget(Arc::clone(&controller), budget.clone());
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut worker = relay.register_worker(registration(), auth()).await.unwrap();
        drop(bridge.outbound().recv().await.unwrap());
        drop(worker.outbound().recv().await.unwrap());
        let queued = AcpMessageV1::new(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"queued": true}
        }))
        .unwrap();
        assert_eq!(
            relay.route_acp(bridge.connection(), queued).await.unwrap(),
            RelayAcpDeliveryOutcome::Delivered
        );
        assert!(budget.available_bytes() < limit);

        let gate = Gate::new();
        controller.set_loss_gate(gate.clone());
        let loss = tokio::spawn({
            let relay = relay.clone();
            async move { relay.connection_lost(worker).await }
        });
        gate.started.acquire().await.unwrap().forget();

        assert_eq!(budget.available_bytes(), limit);
        let late = AcpMessageV1::new(json!({"jsonrpc": "2.0"})).unwrap();
        assert!(matches!(
            relay.route_acp(bridge.connection(), late).await,
            Err(RelayDeliveryError::Rendezvous(
                RendezvousRouteError::Quiescing
            ))
        ));
        gate.release.add_permits(1);
        assert_eq!(
            loss.await.unwrap().unwrap(),
            RelayLossOutcome::Contained(OrphanContainmentOutcome::ContainmentAccepted)
        );
    }

    #[tokio::test]
    async fn cancelled_explicit_loss_retains_its_containment_ticket() {
        let controller = Arc::new(FakeController::new(binding()));
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut worker = relay.register_worker(registration(), auth()).await.unwrap();
        drop(bridge.outbound().recv().await.unwrap());
        drop(worker.outbound().recv().await.unwrap());
        let gate = Gate::new();
        controller.set_loss_gate(gate.clone());
        let loss = tokio::spawn({
            let relay = relay.clone();
            async move { relay.connection_lost(worker).await }
        });
        gate.started.acquire().await.unwrap().forget();

        loss.abort();
        assert!(loss.await.unwrap_err().is_cancelled());
        assert_eq!(registry.pending_containments().len(), 1);
        assert!(matches!(
            relay
                .route_acp(
                    bridge.connection(),
                    AcpMessageV1::new(json!({"jsonrpc": "2.0"})).unwrap(),
                )
                .await,
            Err(RelayDeliveryError::Rendezvous(
                RendezvousRouteError::Quiescing
            ))
        ));

        gate.release.add_permits(1);
        let report = relay.retry_pending_containments().await;
        assert_eq!(report.completed(), 1);
        assert!(report.failures().is_empty());
        assert!(registry.pending_containments().is_empty());
    }

    #[tokio::test]
    async fn closed_peer_delivery_failure_remains_retryable_after_controller_error() {
        let controller = Arc::new(FakeController::new(binding()));
        controller.fail_next_loss();
        let (relay, registry) = orchestrator(Arc::clone(&controller));
        let BridgeOpenOutcome::Attached(mut bridge) = relay
            .activate_bridge(activation_request(BrokerMappingExpectationV1::Absent))
            .await
            .unwrap()
        else {
            panic!("durable generation must attach")
        };
        let mut worker = relay.register_worker(registration(), auth()).await.unwrap();
        drop(bridge.outbound().recv().await.unwrap());
        drop(worker.outbound().recv().await.unwrap());
        worker.outbound().close();

        let error = relay
            .route_acp(
                bridge.connection(),
                AcpMessageV1::new(serde_json::json!({"jsonrpc": "2.0"})).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, RelayDeliveryError::Controller(_)));
        assert_eq!(registry.pending_containments().len(), 1);
        let report = relay.retry_pending_containments().await;
        assert_eq!(report.completed(), 1);
        assert!(report.failures().is_empty());
        assert!(registry.pending_containments().is_empty());
    }
}
