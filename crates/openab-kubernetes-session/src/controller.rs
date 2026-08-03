mod activation;
mod activity;
#[cfg(any(feature = "controller-runtime", test))]
mod bridge_websocket;
mod cleanup;
mod composition;
mod generation;
mod lifecycle;
#[cfg(feature = "controller-runtime")]
mod listener;
mod locks;
mod registration;
mod relay;
mod release;
mod rendezvous;
#[cfg(feature = "controller-runtime")]
mod runtime;
mod service;
#[cfg(feature = "controller-runtime")]
mod supervisor;
#[cfg(feature = "controller-runtime")]
mod tls;
#[cfg(feature = "controller-runtime")]
mod upgrade;
#[cfg(any(feature = "controller-runtime", test))]
mod websocket;
#[cfg(any(feature = "controller-runtime", test))]
mod worker_websocket;

pub use activation::{
    ActivationCoordinator, ActivationError, ActivationPreparation, ActivationTiming,
    GenerationProvisioner, GenerationProvisionerError, GenerationResource, ObservedWorker,
    ProvisionerOperation, ScopeCapacityAdmission,
};
pub use activity::{
    ActivityCoordinator, ActivityError, ActivityEvent, ActivityOutcome, ActivityTurnId,
    ActivityTurnIdError,
};
#[cfg(feature = "controller-runtime")]
pub use bridge_websocket::{BridgeWebSocketOutcome, ControllerBridgeWebSocketError};
pub use cleanup::{
    CleanupProgress, LifecycleProvisioner, ReleaseCleanupProgress, ReleaseProvisioner,
};
pub use composition::{
    ControllerCoordinatorConfigError, ControllerCoordinators, DurableIntentError,
    DurableIntentOutcome, DurableIntentReport, DurableIntentResult, LifecycleDeadlineOutcome,
    LifecycleDeadlineReport, LifecycleDeadlineResult, StartupOrphanOutcome, StartupOrphanReport,
    StartupOrphanResult,
};
pub use generation::{
    AllChildrenAbsentProof, ComputeAbsentProof, KubernetesGenerationProvisioner,
    ReleasedChildrenAbsentProof,
};
pub use lifecycle::{
    LifecycleCoordinator, LifecycleError, LifecycleReconcileOutcome, OrphanAuthority,
    OrphanAuthorityError, OrphanContainmentOutcome,
};
#[cfg(feature = "controller-runtime")]
pub use listener::{ControllerListener, ControllerListenerError};
pub use locks::{SessionLockGuard, SessionLocks};
pub use registration::{
    BootstrapPresence, ConsumedBootstrap, RegisteredWorker, RegistrationCoordinator,
    RegistrationError, RegistrationOperation, RegistrationProvisioner,
    RegistrationProvisionerError, RegistrationRecovery, VerifiedBootstrap, WorkerBootstrapAuth,
    WorkerBootstrapAuthError,
};
pub use relay::{
    BridgeOpenOutcome, RelayAcpDeliveryOutcome, RelayActivityError, RelayAttachment,
    RelayContainmentFailure, RelayContainmentReport, RelayDeliveryError, RelayLifecycleError,
    RelayLifecycleOutcome, RelayLossOutcome, RelayOpenError, RelayOrchestrator,
};
pub use release::{ReleaseCoordinator, ReleaseError, ReleaseOutcome};
pub use rendezvous::{
    AcpRouteOutcome, PendingActivation, RelayBackpressure, RelayByteBudget, RelayByteBudgetError,
    RelayConnection, RelayConnectionId, RelayConnectionLoss, RelayContainmentCompletion,
    RelayContainmentTicket, RelayInstallation, RelayLane, RelayOutboundFrame, RelayOutboundItem,
    RelayOutboundWriteGuard, RelayPairingOutcome, RendezvousFatalError, RendezvousHealth,
    RendezvousInstallError, RendezvousLifecycleError, RendezvousRegistry, RendezvousRouteError,
    MIN_RELAY_BYTE_BUDGET,
};
pub(crate) use rendezvous::{
    LifecycleAcquireOutcome, LifecycleAdmission, LifecycleDeliveryOutcome, RelayLifecycleTerminal,
};
#[cfg(feature = "controller-runtime")]
pub use runtime::{
    ControllerRuntimeBuildError, ControllerStartup, ControllerStartupError, PreparedController,
};
pub use service::{
    ControllerService, ControllerServiceConfigError, ControllerServiceError,
    LifecycleServiceOutcome,
};
#[cfg(feature = "controller-runtime")]
pub use supervisor::{
    ControllerReadiness, ControllerReadinessState, ControllerRuntimeServeError,
    ControllerSupervisor, ControllerSupervisorConfig, ControllerSupervisorConfigError,
};
#[cfg(feature = "controller-runtime")]
pub use tls::{ControllerTlsAcceptor, ControllerTlsConfigError, ControllerTlsConnectionError};
#[cfg(feature = "controller-runtime")]
pub use upgrade::{
    ControllerAdmissionError, ControllerConnection, ControllerConnectionError,
    ControllerConnectionOutcome, ControllerEndpoint, ControllerEndpointBuildError,
    ControllerEndpointConfig, ControllerEndpointConfigError, BRIDGE_WEBSOCKET_PATH,
    WORKER_POD_UID_HEADER, WORKER_WEBSOCKET_PATH,
};
#[cfg(feature = "controller-runtime")]
pub use worker_websocket::WorkerWebSocketError;
