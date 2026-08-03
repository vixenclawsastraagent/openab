//! Concrete startup gate for the opt-in Kubernetes session controller.

use super::{
    ControllerCoordinatorConfigError, ControllerCoordinators, ControllerEndpoint,
    ControllerEndpointBuildError, ControllerEndpointConfig, ControllerListener, ControllerService,
    ControllerServiceConfigError, ControllerServiceError, ControllerSupervisor,
    ControllerSupervisorConfig, ControllerTlsAcceptor, GenerationProvisioner,
    GenerationProvisionerError, KubernetesGenerationProvisioner, LifecycleProvisioner,
    RegistrationProvisioner, RelayByteBudget, RelayOrchestrator, ReleaseProvisioner,
    RendezvousFatalError, RendezvousHealth, StartupOrphanReport,
};
use crate::identity::ScopeId;
use crate::profile_config::ControllerPolicy;
use crate::resources::MvpWorkerProfile;
use crate::state::ProfileRef;
use crate::store::{AnchorStoreError, ConfigMapAnchorStore};
use kube::Client;
use std::num::NonZeroUsize;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

/// Fully configured controller that has not passed startup containment.
///
/// This type deliberately exposes no service, relay, endpoint, or listener.
/// Consuming [`Self::prepare`] is the only path to the built-in serving
/// capability. Custom transports built from lower-level controller APIs must
/// enforce the same startup-containment contract themselves.
pub struct ControllerStartup {
    service: Arc<ControllerService>,
    relay: RelayOrchestrator,
    health: watch::Receiver<RendezvousHealth>,
    listener: ControllerListener,
}

impl ControllerStartup {
    /// Wire one fixed-scope Kubernetes controller from already resolved worker
    /// profiles without performing Kubernetes I/O.
    ///
    /// All historical profile revisions required by durable anchors belong in
    /// `profiles`; `current_profiles` independently selects the revision used
    /// for new sessions. Cluster-reference resolution remains a caller-owned
    /// startup step and cannot be influenced by a relay peer.
    #[allow(clippy::too_many_arguments)]
    pub fn from_resolved_profiles(
        client: Client,
        namespace: impl Into<String>,
        scope_id: ScopeId,
        profiles: impl IntoIterator<Item = MvpWorkerProfile>,
        current_profiles: impl IntoIterator<Item = ProfileRef>,
        policy: ControllerPolicy,
        relay_queue_capacity: NonZeroUsize,
        relay_byte_budget: RelayByteBudget,
        bridge_credential: &[u8],
        endpoint_config: ControllerEndpointConfig,
        tls: ControllerTlsAcceptor,
    ) -> Result<Self, ControllerRuntimeBuildError> {
        let namespace = namespace.into();
        let store = ConfigMapAnchorStore::new(client.clone(), namespace.clone(), scope_id)
            .map_err(ControllerRuntimeBuildError::Store)?;
        let provisioner = Arc::new(
            KubernetesGenerationProvisioner::new(client, namespace, scope_id)
                .map_err(ControllerRuntimeBuildError::Provisioner)?,
        );
        let generation: Arc<dyn GenerationProvisioner> = provisioner.clone();
        let lifecycle: Arc<dyn LifecycleProvisioner> = provisioner.clone();
        let registration: Arc<dyn RegistrationProvisioner> = provisioner.clone();
        let release: Arc<dyn ReleaseProvisioner> = provisioner;
        let coordinators = ControllerCoordinators::new(
            store,
            profiles,
            policy,
            generation,
            lifecycle,
            registration,
            release,
        )
        .map_err(ControllerRuntimeBuildError::Coordinator)?;
        let service = Arc::new(
            ControllerService::new(coordinators, current_profiles)
                .map_err(ControllerRuntimeBuildError::Service)?,
        );
        let relay = RelayOrchestrator::new(
            Arc::clone(&service),
            relay_queue_capacity,
            relay_byte_budget,
        );
        let health = relay.health();
        let endpoint = ControllerEndpoint::new(relay.clone(), bridge_credential, endpoint_config)
            .map_err(ControllerRuntimeBuildError::Endpoint)?;
        let listener = ControllerListener::new(endpoint, tls);
        Ok(Self {
            service,
            relay,
            health,
            listener,
        })
    }

    /// Contain every pre-restart live generation before enabling transport.
    ///
    /// Cancelling this future drops the configured runtime and yields no
    /// serving capability. A caller must restart the complete startup gate;
    /// it must not infer success from partially completed Kubernetes writes.
    pub async fn prepare(self) -> Result<PreparedController, ControllerStartupError> {
        require_healthy(&self.health).map_err(ControllerStartupError::FatalHealth)?;
        let startup_orphans = self
            .service
            .quiesce_startup_orphans()
            .await
            .map_err(ControllerStartupError::StartupContainment)?;
        if !startup_orphans.containment_complete() {
            return Err(ControllerStartupError::StartupContainmentIncomplete {
                report: startup_orphans,
            });
        }
        require_healthy(&self.health).map_err(ControllerStartupError::FatalHealth)?;

        Ok(PreparedController {
            service: self.service,
            relay: self.relay,
            health: self.health,
            listener: self.listener,
            startup_orphans,
        })
    }
}

/// Controller that passed restart containment and may now own one listener.
pub struct PreparedController {
    pub(super) service: Arc<ControllerService>,
    pub(super) relay: RelayOrchestrator,
    pub(super) health: watch::Receiver<RendezvousHealth>,
    pub(super) listener: ControllerListener,
    pub(super) startup_orphans: StartupOrphanReport,
}

impl PreparedController {
    /// Return the complete report that proved startup containment.
    pub fn startup_orphans(&self) -> &StartupOrphanReport {
        &self.startup_orphans
    }

    /// Consume the prepared startup gate into the only built-in serving path.
    ///
    /// The supervisor owns readiness, periodic maintenance, and bounded clean
    /// shutdown. Lower-level controller transports remain responsible for
    /// enforcing the same contracts themselves.
    pub fn into_supervisor(self, config: ControllerSupervisorConfig) -> ControllerSupervisor {
        ControllerSupervisor::new(self, config)
    }
}

pub(super) fn require_healthy(
    health: &watch::Receiver<RendezvousHealth>,
) -> Result<(), RendezvousFatalError> {
    match *health.borrow() {
        RendezvousHealth::Healthy => Ok(()),
        RendezvousHealth::Fatal(source) => Err(source),
    }
}

/// Failure while wiring the fixed-scope built-in controller runtime.
#[derive(Debug, Error)]
pub enum ControllerRuntimeBuildError {
    /// The durable anchor store rejected its static configuration.
    #[error("controller anchor store configuration is invalid")]
    Store(#[source] AnchorStoreError),
    /// The Kubernetes generation provisioner rejected its configuration.
    #[error("controller Kubernetes provisioner configuration is invalid")]
    Provisioner(#[source] GenerationProvisionerError),
    /// The controller composition root rejected its profiles or policy.
    #[error("controller coordinator configuration is invalid")]
    Coordinator(#[source] ControllerCoordinatorConfigError),
    /// The service rejected its current-profile selection.
    #[error("controller service configuration is invalid")]
    Service(#[source] ControllerServiceConfigError),
    /// The authenticated endpoint rejected its transport configuration.
    #[error("controller endpoint configuration is invalid")]
    Endpoint(#[source] ControllerEndpointBuildError),
}

/// Failure before the controller is allowed to admit relay traffic.
#[derive(Debug, Error)]
pub enum ControllerStartupError {
    /// The in-memory relay had already entered a process-fatal state.
    #[error("controller relay health is fatal during startup")]
    FatalHealth(#[source] RendezvousFatalError),
    /// The startup containment inventory could not be completed.
    #[error("controller startup containment failed")]
    StartupContainment(#[source] ControllerServiceError),
    /// One or more observed live generations could not be durably contained.
    #[error("controller startup containment is incomplete")]
    StartupContainmentIncomplete { report: StartupOrphanReport },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_gate_accepts_only_the_healthy_state() {
        let (_healthy_tx, healthy) = watch::channel(RendezvousHealth::Healthy);
        assert!(require_healthy(&healthy).is_ok());
        let (_fatal_tx, fatal) =
            watch::channel(RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned));
        assert_eq!(
            require_healthy(&fatal),
            Err(RendezvousFatalError::StatePoisoned)
        );
    }
}
