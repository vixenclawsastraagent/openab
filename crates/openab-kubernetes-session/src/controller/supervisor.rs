//! Readiness, maintenance, and bounded shutdown for the built-in controller.

use super::runtime::require_healthy;
use super::{
    ControllerListenerError, ControllerService, PreparedController, RelayOrchestrator,
    RendezvousFatalError, RendezvousHealth, StartupOrphanReport,
};
use futures_util::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};

/// Static scheduling and termination policy for one controller process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerSupervisorConfig {
    maintenance_interval: Duration,
    shutdown_grace: Duration,
}

impl ControllerSupervisorConfig {
    pub fn new(
        maintenance_interval: Duration,
        shutdown_grace: Duration,
    ) -> Result<Self, ControllerSupervisorConfigError> {
        if maintenance_interval.is_zero() {
            return Err(ControllerSupervisorConfigError::ZeroMaintenanceInterval);
        }
        if shutdown_grace.is_zero() {
            return Err(ControllerSupervisorConfigError::ZeroShutdownGrace);
        }
        Ok(Self {
            maintenance_interval,
            shutdown_grace,
        })
    }

    pub fn maintenance_interval(self) -> Duration {
        self.maintenance_interval
    }

    pub fn shutdown_grace(self) -> Duration {
        self.shutdown_grace
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ControllerSupervisorConfigError {
    #[error("controller maintenance interval must be non-zero")]
    ZeroMaintenanceInterval,
    #[error("controller shutdown grace must be non-zero")]
    ZeroShutdownGrace,
}

/// Probe state published by the built-in controller supervisor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControllerReadinessState {
    NotReady,
    Ready,
}

/// Read-only readiness subscription.
///
/// A closed publisher is always reported as [`ControllerReadinessState::NotReady`],
/// even if its last observed value was ready.
#[derive(Clone)]
pub struct ControllerReadiness {
    receiver: watch::Receiver<ControllerReadinessState>,
}

impl ControllerReadiness {
    /// Return the current probe state; publisher loss is fail-closed.
    pub fn state(&self) -> ControllerReadinessState {
        if self.receiver.has_changed().is_err() {
            ControllerReadinessState::NotReady
        } else {
            *self.receiver.borrow()
        }
    }

    /// Wait for the next state transition.
    ///
    /// `None` is terminal publisher loss. Probe adapters should publish one
    /// final not-ready result and stop watching instead of polling again.
    pub async fn wait_for_change(&mut self) -> Option<ControllerReadinessState> {
        if self.receiver.changed().await.is_err() {
            return None;
        }
        Some(self.state())
    }
}

/// The only built-in serving path after startup containment succeeds.
pub struct ControllerSupervisor {
    prepared: PreparedController,
    config: ControllerSupervisorConfig,
    readiness: watch::Sender<ControllerReadinessState>,
}

impl ControllerSupervisor {
    pub(super) fn new(prepared: PreparedController, config: ControllerSupervisorConfig) -> Self {
        let (readiness, _) = watch::channel(ControllerReadinessState::NotReady);
        Self {
            prepared,
            config,
            readiness,
        }
    }

    pub fn startup_orphans(&self) -> &StartupOrphanReport {
        self.prepared.startup_orphans()
    }

    pub fn readiness(&self) -> ControllerReadiness {
        ControllerReadiness {
            receiver: self.readiness.subscribe(),
        }
    }

    /// Serve until external shutdown or a terminal controller condition.
    ///
    /// Clean shutdown uses one grace deadline for listener and maintenance
    /// drain, relay-owned task settlement, and final durable containment.
    /// Process-fatal relay health cancels every other future immediately and
    /// deliberately performs no further rendezvous or Kubernetes operation.
    pub async fn serve_until<F>(
        self,
        tcp_listener: TcpListener,
        shutdown: F,
    ) -> Result<(), ControllerRuntimeServeError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            prepared,
            config,
            readiness,
        } = self;
        let PreparedController {
            service,
            relay,
            mut health,
            listener,
            startup_orphans,
        } = prepared;
        let observed_unavailable_profile_sessions =
            startup_orphans.unavailable_profile_session_count();
        if observed_unavailable_profile_sessions > 0 {
            tracing::warn!(
                observed_unavailable_profile_sessions,
                "startup inventory observed sessions with unavailable worker profile revisions"
            );
        }
        require_live_healthy(&health)?;

        let (listener_stop_tx, listener_stop_rx) = oneshot::channel();
        let (maintenance_stop_tx, maintenance_stop_rx) = oneshot::channel();
        let (listener_started_tx, listener_started_rx) = oneshot::channel();
        let serving = async move {
            let _ = listener_started_tx.send(());
            listener
                .serve_until(tcp_listener, async move {
                    let _ = listener_stop_rx.await;
                })
                .await
        };
        let maintenance = AssertUnwindSafe(maintenance_loop(
            Arc::clone(&service),
            relay.clone(),
            health.clone(),
            config.maintenance_interval,
            maintenance_stop_rx,
        ))
        .catch_unwind();
        tokio::pin!(serving);
        tokio::pin!(maintenance);
        tokio::pin!(shutdown);
        tokio::pin!(listener_started_rx);
        let mut listener_stop_tx = Some(listener_stop_tx);
        let mut maintenance_stop_tx = Some(maintenance_stop_tx);
        let mut readiness_published = false;
        // Declared after every future that can own an attachment or controller
        // task so cancellation/unwind drops this fail-stop fence first.
        let mut terminal = TerminalGuard::new(readiness, relay.clone());

        loop {
            tokio::select! {
                biased;

                changed = health.changed() => {
                    if let Some(error) = health_change_error(changed, &health) {
                        terminal.fail();
                        return Err(error);
                    }
                }
                result = &mut serving => {
                    terminal.fail();
                    return Err(listener_terminal_error(result));
                }
                result = &mut maintenance => {
                    terminal.fail();
                    return Err(maintenance_terminal_error(result));
                }
                _ = &mut shutdown => {
                    terminal.not_ready();
                    signal_stop(&mut listener_stop_tx);
                    signal_stop(&mut maintenance_stop_tx);
                    let outcome = AssertUnwindSafe(finish_normal_shutdown(
                        &mut health,
                        config.shutdown_grace,
                        serving.as_mut(),
                        maintenance.as_mut(),
                        &relay,
                    ))
                    .catch_unwind()
                    .await;
                    let result = match outcome {
                        Ok(result) => result,
                        Err(_) => match require_live_healthy(&health) {
                            Err(error) => Err(error),
                            Ok(()) => Err(ControllerRuntimeServeError::ShutdownPanicked),
                        },
                    };
                    if result.is_ok() {
                        terminal.clean();
                    } else {
                        terminal.fail();
                    }
                    return result;
                }
                _ = &mut listener_started_rx, if !readiness_published => {
                    if let Err(error) = require_live_healthy(&health) {
                        terminal.fail();
                        return Err(error);
                    }
                    terminal.ready();
                    readiness_published = true;
                }
            }
        }
    }
}

struct TerminalGuard {
    sender: watch::Sender<ControllerReadinessState>,
    relay: RelayOrchestrator,
    armed: bool,
}

impl TerminalGuard {
    fn new(sender: watch::Sender<ControllerReadinessState>, relay: RelayOrchestrator) -> Self {
        Self {
            sender,
            relay,
            armed: true,
        }
    }

    fn ready(&mut self) {
        self.sender.send_if_modified(|state| {
            if *state == ControllerReadinessState::Ready {
                false
            } else {
                *state = ControllerReadinessState::Ready;
                true
            }
        });
    }

    fn not_ready(&mut self) {
        self.sender.send_if_modified(|state| {
            if *state == ControllerReadinessState::NotReady {
                false
            } else {
                *state = ControllerReadinessState::NotReady;
                true
            }
        });
    }

    fn fail(&mut self) {
        if !self.armed {
            return;
        }
        self.not_ready();
        self.relay.abort_owned_tasks();
        self.armed = false;
    }

    fn clean(&mut self) {
        if !self.armed {
            return;
        }
        self.not_ready();
        self.armed = false;
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.armed {
            self.fail();
        }
    }
}

async fn maintenance_loop(
    service: Arc<ControllerService>,
    relay: RelayOrchestrator,
    health: watch::Receiver<RendezvousHealth>,
    period: Duration,
    mut stop: oneshot::Receiver<()>,
) -> Result<(), ControllerRuntimeServeError> {
    let first_tick = Instant::now()
        .checked_add(period)
        .ok_or(ControllerRuntimeServeError::MaintenanceDeadlineOverflow)?;
    let mut interval = tokio::time::interval_at(first_tick, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;

            _ = &mut stop => return Ok(()),
            _ = interval.tick() => {
                run_maintenance_pass(&service, &relay, &health).await?;
            }
        }
    }
}

async fn run_maintenance_pass(
    service: &ControllerService,
    relay: &RelayOrchestrator,
    health: &watch::Receiver<RendezvousHealth>,
) -> Result<(), ControllerRuntimeServeError> {
    require_live_healthy(health)?;
    let containment = relay.retry_pending_containments().await;
    if containment.failures().is_empty() {
        tracing::debug!(
            completed = containment.completed(),
            "controller relay containment maintenance completed"
        );
    } else {
        tracing::warn!(
            completed = containment.completed(),
            failures = containment.failures().len(),
            "controller relay containment maintenance remains incomplete"
        );
    }

    require_live_healthy(health)?;
    match service.scan_lifecycle_deadlines().await {
        Ok(report) => {
            let failures = report
                .results()
                .iter()
                .filter(|result| result.error().is_some())
                .count();
            if failures == 0 {
                tracing::debug!(
                    sessions = report.results().len(),
                    "controller lifecycle deadline maintenance completed"
                );
            } else {
                tracing::warn!(
                    sessions = report.results().len(),
                    failures,
                    "controller lifecycle deadline maintenance has session failures"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "controller lifecycle deadline inventory failed"
            );
        }
    }

    require_live_healthy(health)?;
    match service.reconcile_durable_intents().await {
        Ok(report) => {
            let failures = report
                .results()
                .iter()
                .filter(|result| result.error().is_some())
                .count();
            if failures == 0 {
                tracing::debug!(
                    sessions = report.results().len(),
                    "controller durable intent maintenance completed"
                );
            } else {
                tracing::warn!(
                    sessions = report.results().len(),
                    failures,
                    "controller durable intent maintenance has session failures"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "controller durable intent inventory failed"
            );
        }
    }
    require_live_healthy(health)
}

async fn finish_normal_shutdown<S, M>(
    health: &mut watch::Receiver<RendezvousHealth>,
    grace: Duration,
    serving: Pin<&mut S>,
    maintenance: Pin<&mut M>,
    relay: &RelayOrchestrator,
) -> Result<(), ControllerRuntimeServeError>
where
    S: Future<Output = Result<(), ControllerListenerError>>,
    M: Future<
        Output = Result<Result<(), ControllerRuntimeServeError>, Box<dyn std::any::Any + Send>>,
    >,
{
    let deadline = Instant::now()
        .checked_add(grace)
        .ok_or(ControllerRuntimeServeError::ShutdownDeadlineOverflow)?;
    let drain = async {
        let (listener_result, maintenance_result) = tokio::join!(serving, maintenance);
        listener_result.map_err(ControllerRuntimeServeError::Listener)?;
        match maintenance_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(ControllerRuntimeServeError::MaintenancePanicked),
        }

        relay
            .wait_for_owned_tasks()
            .await
            .map_err(|_| ControllerRuntimeServeError::OwnedTaskFailed)?;
        let containment = relay.retry_pending_containments().await;
        require_final_containment(containment.failures().len())
    };
    await_healthy_until(health, deadline, drain).await
}

fn require_final_containment(failures: usize) -> Result<(), ControllerRuntimeServeError> {
    if failures == 0 {
        Ok(())
    } else {
        Err(ControllerRuntimeServeError::FinalContainmentIncomplete { failures })
    }
}

async fn await_healthy_until<T, F>(
    health: &mut watch::Receiver<RendezvousHealth>,
    deadline: Instant,
    future: F,
) -> Result<T, ControllerRuntimeServeError>
where
    F: Future<Output = Result<T, ControllerRuntimeServeError>>,
{
    tokio::pin!(future);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            biased;

            changed = health.changed() => {
                if let Some(error) = health_change_error(changed, health) {
                    return Err(error);
                }
            }
            _ = &mut timeout => {
                return Err(ControllerRuntimeServeError::ShutdownTimedOut);
            }
            result = &mut future => {
                require_live_healthy(health)?;
                return result;
            }
        }
    }
}

fn require_live_healthy(
    health: &watch::Receiver<RendezvousHealth>,
) -> Result<(), ControllerRuntimeServeError> {
    if health.has_changed().is_err() {
        return Err(ControllerRuntimeServeError::HealthChannelClosed);
    }
    require_healthy(health).map_err(ControllerRuntimeServeError::FatalHealth)
}

fn health_change_error(
    changed: Result<(), watch::error::RecvError>,
    health: &watch::Receiver<RendezvousHealth>,
) -> Option<ControllerRuntimeServeError> {
    match changed {
        Err(_) => Some(ControllerRuntimeServeError::HealthChannelClosed),
        Ok(()) => match *health.borrow() {
            RendezvousHealth::Healthy => None,
            RendezvousHealth::Fatal(source) => {
                Some(ControllerRuntimeServeError::FatalHealth(source))
            }
        },
    }
}

fn listener_terminal_error(
    result: Result<(), ControllerListenerError>,
) -> ControllerRuntimeServeError {
    match result {
        Ok(()) => ControllerRuntimeServeError::ListenerStopped,
        Err(source) => ControllerRuntimeServeError::Listener(source),
    }
}

fn maintenance_terminal_error(
    result: Result<Result<(), ControllerRuntimeServeError>, Box<dyn std::any::Any + Send>>,
) -> ControllerRuntimeServeError {
    match result {
        Ok(Ok(())) => ControllerRuntimeServeError::MaintenanceStopped,
        Ok(Err(error)) => error,
        Err(_) => ControllerRuntimeServeError::MaintenancePanicked,
    }
}

fn signal_stop(sender: &mut Option<oneshot::Sender<()>>) {
    if let Some(sender) = sender.take() {
        let _ = sender.send(());
    }
}

#[cfg(test)]
pub(super) fn terminal_guard_for_test(
    sender: watch::Sender<ControllerReadinessState>,
    relay: RelayOrchestrator,
) -> impl Drop {
    TerminalGuard::new(sender, relay)
}

#[cfg(test)]
pub(super) async fn finish_normal_shutdown_for_test(
    health: &mut watch::Receiver<RendezvousHealth>,
    grace: Duration,
    relay: &RelayOrchestrator,
) -> Result<(), ControllerRuntimeServeError> {
    let serving = std::future::ready(Ok::<(), ControllerListenerError>(()));
    let maintenance = std::future::ready(Ok::<
        Result<(), ControllerRuntimeServeError>,
        Box<dyn std::any::Any + Send>,
    >(Ok(())));
    tokio::pin!(serving);
    tokio::pin!(maintenance);
    finish_normal_shutdown(health, grace, serving.as_mut(), maintenance.as_mut(), relay).await
}

/// Terminal failure while supervising the built-in controller runtime.
#[derive(Debug, Error)]
pub enum ControllerRuntimeServeError {
    #[error("controller relay health became fatal")]
    FatalHealth(#[source] RendezvousFatalError),
    #[error("controller relay health channel closed")]
    HealthChannelClosed,
    #[error("controller listener failed")]
    Listener(#[source] ControllerListenerError),
    #[error("controller listener stopped without a terminal signal")]
    ListenerStopped,
    #[error("controller maintenance stopped without a terminal signal")]
    MaintenanceStopped,
    #[error("controller maintenance panicked")]
    MaintenancePanicked,
    #[error("a controller-owned relay task failed")]
    OwnedTaskFailed,
    #[error("controller shutdown exceeded its grace period")]
    ShutdownTimedOut,
    #[error("controller shutdown encountered an internal panic")]
    ShutdownPanicked,
    #[error("controller shutdown deadline cannot be represented")]
    ShutdownDeadlineOverflow,
    #[error("controller maintenance deadline cannot be represented")]
    MaintenanceDeadlineOverflow,
    #[error("final controller containment is incomplete for {failures} session(s)")]
    FinalContainmentIncomplete { failures: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_closed_readiness_publisher_is_never_ready() {
        let (sender, receiver) = watch::channel(ControllerReadinessState::Ready);
        let readiness = ControllerReadiness { receiver };
        assert_eq!(readiness.state(), ControllerReadinessState::Ready);
        drop(sender);
        assert_eq!(readiness.state(), ControllerReadinessState::NotReady);
    }

    #[tokio::test]
    async fn fatal_health_preempts_shutdown_work_without_polling_it_again() {
        let (health_tx, mut health) = watch::channel(RendezvousHealth::Healthy);
        let polls = Arc::new(AtomicUsize::new(0));
        let future_polls = Arc::clone(&polls);
        let shutdown_work = std::future::poll_fn(move |_context| {
            future_polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::<Result<(), ControllerRuntimeServeError>>::Pending
        });
        let deadline = Instant::now() + Duration::from_secs(60);
        let task =
            tokio::spawn(
                async move { await_healthy_until(&mut health, deadline, shutdown_work).await },
            );
        tokio::task::yield_now().await;
        let observed_polls = polls.load(Ordering::SeqCst);
        assert!(observed_polls > 0);

        health_tx.send_replace(RendezvousHealth::Fatal(RendezvousFatalError::StatePoisoned));
        assert!(matches!(
            task.await.unwrap(),
            Err(ControllerRuntimeServeError::FatalHealth(
                RendezvousFatalError::StatePoisoned
            ))
        ));
        assert_eq!(polls.load(Ordering::SeqCst), observed_polls);
    }

    #[tokio::test(start_paused = true)]
    async fn one_deadline_bounds_the_entire_shutdown_future() {
        let (_health_tx, mut health) = watch::channel(RendezvousHealth::Healthy);
        let deadline = Instant::now() + Duration::from_secs(5);
        let shutdown_work = async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        };

        assert!(matches!(
            await_healthy_until(&mut health, deadline, shutdown_work).await,
            Err(ControllerRuntimeServeError::ShutdownTimedOut)
        ));
        assert_eq!(Instant::now(), deadline);
    }

    #[test]
    fn failed_final_containment_is_never_clean_shutdown() {
        assert!(require_final_containment(0).is_ok());
        assert!(matches!(
            require_final_containment(2),
            Err(ControllerRuntimeServeError::FinalContainmentIncomplete { failures: 2 })
        ));
    }
}
