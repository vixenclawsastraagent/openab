use futures_util::FutureExt;
use kube::Client;
use openab_kubernetes_session::bridge::{
    is_valid_controller_bearer_credential, MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES,
    MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES,
};
use openab_kubernetes_session::controller::{
    resolve_profile_revisions, ControllerProbeServer, ControllerStartup, ControllerTlsAcceptor,
    RelayByteBudget,
};
use openab_kubernetes_session::controller_process_config::ControllerProcessConfigV1;
use openab_kubernetes_session::profile_config::TrustedControllerConfigV1;
use std::any::Any;
use std::ffi::OsString;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, Read};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::prelude::*;
use zeroize::Zeroizing;

#[derive(Debug, PartialEq, Eq)]
struct ControllerCommand {
    config_file: PathBuf,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum CommandError {
    #[error("command option is unknown")]
    UnknownOption,
    #[error("command option is missing a value")]
    MissingOptionValue,
    #[error("command option was specified more than once")]
    DuplicateOption,
    #[error("the required --config-file option is missing")]
    MissingConfigFile,
    #[error("controller configuration file must be an absolute path")]
    RelativeConfigFile,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum CredentialLoadError {
    #[error("controller bridge credential could not be read")]
    Read,
    #[error("controller bridge credential is shorter than the required security floor")]
    TooShort,
    #[error("controller bridge credential exceeds its size limit")]
    TooLarge,
    #[error("controller bridge credential contains a disallowed byte")]
    InvalidByte,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum RuntimePairError {
    #[error("controller shutdown signal stream failed")]
    SignalFailed,
    #[error("controller supervisor stopped unexpectedly")]
    SupervisorStopped,
    #[error("controller supervisor failed")]
    SupervisorFailed,
    #[error("controller supervisor panicked")]
    SupervisorPanicked,
    #[error("controller probe server stopped unexpectedly")]
    ProbeStopped,
    #[error("controller probe server failed")]
    ProbeFailed,
    #[error("controller probe server panicked")]
    ProbePanicked,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum ApplicationError {
    #[error("controller command is invalid")]
    Command,
    #[error("controller shutdown signal handling could not be initialized")]
    SignalInitialization,
    #[error("controller shutdown signal stream failed")]
    SignalWait,
    #[error("controller process configuration file could not be opened")]
    OpenProcessConfig,
    #[error("controller process configuration is invalid")]
    ProcessConfig,
    #[error("controller worker-profile configuration file could not be opened")]
    OpenProfileConfig,
    #[error("controller worker-profile configuration is invalid")]
    ProfileConfig,
    #[error("controller TLS certificate file could not be opened")]
    OpenTlsCertificate,
    #[error("controller TLS private-key file could not be opened")]
    OpenTlsPrivateKey,
    #[error("controller TLS identity is invalid")]
    TlsIdentity,
    #[error("controller bridge credential file could not be opened")]
    OpenCredential,
    #[error("controller bridge credential is invalid")]
    Credential,
    #[error("Kubernetes client initialization failed")]
    KubernetesClient,
    #[error("controller worker-profile reference resolution failed")]
    ProfileResolution,
    #[error("controller relay byte budget is invalid")]
    RelayByteBudget,
    #[error("controller runtime composition failed")]
    RuntimeBuild,
    #[error("controller startup containment failed")]
    StartupContainment,
    #[error("controller relay listener could not be bound")]
    RelayBind,
    #[error("controller probe listener could not be bound")]
    ProbeBind,
    #[error(transparent)]
    Runtime(RuntimePairError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("controller shutdown signal stream failed")]
struct SignalWaitError;

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> Result<Self, ApplicationError> {
        use tokio::signal::unix::{signal, SignalKind};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())
                .map_err(|_| ApplicationError::SignalInitialization)?,
            terminate: signal(SignalKind::terminate())
                .map_err(|_| ApplicationError::SignalInitialization)?,
        })
    }

    async fn wait(&mut self) -> Result<(), SignalWaitError> {
        tokio::select! {
            received = self.interrupt.recv() => received.map(|_| ()).ok_or(SignalWaitError),
            received = self.terminate.recv() => received.map(|_| ()).ok_or(SignalWaitError),
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn install() -> Result<Self, ApplicationError> {
        Ok(Self)
    }

    async fn wait(&mut self) -> Result<(), SignalWaitError> {
        tokio::signal::ctrl_c().await.map_err(|_| SignalWaitError)
    }
}

enum StartupPhase<T> {
    Completed(T),
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChildCompletion {
    Clean,
    Failed,
    Panicked,
}

fn parse_args<I>(args: I) -> Result<ControllerCommand, CommandError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut config_file = None;
    let mut args = args.into_iter();
    while let Some(option) = args.next() {
        if option.to_str() != Some("--config-file") {
            return Err(CommandError::UnknownOption);
        }
        let value = args.next().ok_or(CommandError::MissingOptionValue)?;
        if value.to_str().is_some_and(|value| value.starts_with("--")) {
            return Err(CommandError::MissingOptionValue);
        }
        let value = PathBuf::from(value);
        if !is_mounted_absolute_path(&value) {
            return Err(CommandError::RelativeConfigFile);
        }
        if config_file.replace(value).is_some() {
            return Err(CommandError::DuplicateOption);
        }
    }

    Ok(ControllerCommand {
        config_file: config_file.ok_or(CommandError::MissingConfigFile)?,
    })
}

fn is_mounted_absolute_path(path: &Path) -> bool {
    // Controller containers are Linux-based. Keep POSIX mounted paths valid
    // when configuration parsing is tested from a non-Linux build host.
    path.as_os_str().to_string_lossy().starts_with('/') || path.is_absolute()
}

fn read_bridge_credential<R>(reader: R) -> Result<Zeroizing<Vec<u8>>, CredentialLoadError>
where
    R: Read,
{
    let mut credential = Zeroizing::new(Vec::new());
    reader
        .take((MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut credential)
        .map_err(|_| CredentialLoadError::Read)?;
    if credential.len() < MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES {
        return Err(CredentialLoadError::TooShort);
    }
    if credential.len() > MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES {
        return Err(CredentialLoadError::TooLarge);
    }
    if !is_valid_controller_bearer_credential(&credential) {
        return Err(CredentialLoadError::InvalidByte);
    }
    Ok(credential)
}

async fn startup_phase_or_shutdown<F, S, T, E>(phase: F, shutdown: S) -> Result<StartupPhase<T>, E>
where
    F: Future<Output = T>,
    S: Future<Output = Result<(), E>>,
{
    tokio::pin!(phase);
    tokio::pin!(shutdown);
    tokio::select! {
        biased;

        result = &mut shutdown => result.map(|()| StartupPhase::Shutdown),
        result = &mut phase => Ok(StartupPhase::Completed(result)),
    }
}

fn child_completion<E>(result: Result<Result<(), E>, Box<dyn Any + Send>>) -> ChildCompletion {
    match result {
        Ok(Ok(())) => ChildCompletion::Clean,
        Ok(Err(_)) => ChildCompletion::Failed,
        Err(_) => ChildCompletion::Panicked,
    }
}

fn supervisor_error(completion: ChildCompletion) -> RuntimePairError {
    match completion {
        ChildCompletion::Clean => RuntimePairError::SupervisorStopped,
        ChildCompletion::Failed => RuntimePairError::SupervisorFailed,
        ChildCompletion::Panicked => RuntimePairError::SupervisorPanicked,
    }
}

fn probe_error(completion: ChildCompletion) -> RuntimePairError {
    match completion {
        ChildCompletion::Clean => RuntimePairError::ProbeStopped,
        ChildCompletion::Failed => RuntimePairError::ProbeFailed,
        ChildCompletion::Panicked => RuntimePairError::ProbePanicked,
    }
}

fn request_stop(sender: &mut Option<oneshot::Sender<()>>) {
    if let Some(sender) = sender.take() {
        let _ = sender.send(());
    }
}

async fn supervise_pair<S, SF, SE, P, PF, PE, X, XE>(
    supervisor: S,
    probe: P,
    external_shutdown: X,
) -> Result<(), RuntimePairError>
where
    S: FnOnce(oneshot::Receiver<()>) -> SF,
    SF: Future<Output = Result<(), SE>>,
    P: FnOnce(oneshot::Receiver<()>) -> PF,
    PF: Future<Output = Result<(), PE>>,
    X: Future<Output = Result<(), XE>>,
{
    let (supervisor_stop_tx, supervisor_stop_rx) = oneshot::channel();
    let (probe_stop_tx, probe_stop_rx) = oneshot::channel();
    let mut supervisor_stop_tx = Some(supervisor_stop_tx);
    let mut probe_stop_tx = Some(probe_stop_tx);
    let supervisor_polled = AtomicBool::new(false);
    let probe_polled = AtomicBool::new(false);
    let supervisor = async {
        supervisor_polled.store(true, Ordering::Relaxed);
        AssertUnwindSafe(async move { supervisor(supervisor_stop_rx).await })
            .catch_unwind()
            .await
    };
    let probe = async {
        probe_polled.store(true, Ordering::Relaxed);
        AssertUnwindSafe(async move { probe(probe_stop_rx).await })
            .catch_unwind()
            .await
    };
    tokio::pin!(supervisor);
    tokio::pin!(probe);
    tokio::pin!(external_shutdown);

    tokio::select! {
        biased;

        signal = &mut external_shutdown => {
            if !supervisor_polled.load(Ordering::Relaxed)
                && !probe_polled.load(Ordering::Relaxed)
            {
                return signal.map_err(|_| RuntimePairError::SignalFailed);
            }

            request_stop(&mut supervisor_stop_tx);
            if signal.is_err() {
                let _ = supervisor.await;
                request_stop(&mut probe_stop_tx);
                let _ = probe.await;
                return Err(RuntimePairError::SignalFailed);
            }

            tokio::select! {
                biased;

                result = &mut probe => {
                    let error = probe_error(child_completion(result));
                    let _ = supervisor.await;
                    Err(error)
                }
                result = &mut supervisor => {
                    match child_completion(result) {
                        ChildCompletion::Clean => {
                            request_stop(&mut probe_stop_tx);
                            match child_completion(probe.await) {
                                ChildCompletion::Clean => Ok(()),
                                completion => Err(probe_error(completion)),
                            }
                        }
                        completion => {
                            let error = supervisor_error(completion);
                            request_stop(&mut probe_stop_tx);
                            let _ = probe.await;
                            Err(error)
                        }
                    }
                }
            }
        }
        result = &mut supervisor => {
            let error = supervisor_error(child_completion(result));
            request_stop(&mut probe_stop_tx);
            let _ = probe.await;
            Err(error)
        }
        result = &mut probe => {
            let error = probe_error(child_completion(result));
            request_stop(&mut supervisor_stop_tx);
            let _ = supervisor.await;
            Err(error)
        }
    }
}

async fn run<I>(args: I) -> Result<(), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
{
    let command = parse_args(args).map_err(|_| ApplicationError::Command)?;
    let mut shutdown = ShutdownSignals::install()?;

    let process_file =
        File::open(command.config_file).map_err(|_| ApplicationError::OpenProcessConfig)?;
    let process = ControllerProcessConfigV1::from_reader(BufReader::new(process_file))
        .map_err(|_| ApplicationError::ProcessConfig)?;
    let profile_file =
        File::open(process.profiles_file()).map_err(|_| ApplicationError::OpenProfileConfig)?;
    let profile_config = TrustedControllerConfigV1::from_reader(BufReader::new(profile_file))
        .map_err(|_| ApplicationError::ProfileConfig)?;
    let policy = *profile_config.policy();

    let certificate_file = File::open(process.tls_certificate_file())
        .map_err(|_| ApplicationError::OpenTlsCertificate)?;
    let private_key_file = File::open(process.tls_private_key_file())
        .map_err(|_| ApplicationError::OpenTlsPrivateKey)?;
    let tls = ControllerTlsAcceptor::from_pem(
        BufReader::new(certificate_file),
        BufReader::new(private_key_file),
        process.tls_handshake_timeout(),
    )
    .map_err(|_| ApplicationError::TlsIdentity)?;
    let client = match startup_phase_or_shutdown(Client::try_default(), shutdown.wait())
        .await
        .map_err(|_| ApplicationError::SignalWait)?
    {
        StartupPhase::Completed(result) => {
            result.map_err(|_| ApplicationError::KubernetesClient)?
        }
        StartupPhase::Shutdown => return Ok(()),
    };
    let resolved = match startup_phase_or_shutdown(
        resolve_profile_revisions(client.clone(), process.worker_namespace(), &profile_config),
        shutdown.wait(),
    )
    .await
    .map_err(|_| ApplicationError::SignalWait)?
    {
        StartupPhase::Completed(result) => {
            result.map_err(|_| ApplicationError::ProfileResolution)?
        }
        StartupPhase::Shutdown => return Ok(()),
    };
    let (profiles, current_profiles, unavailable_historical_revisions) = resolved.into_parts();
    if unavailable_historical_revisions > 0 {
        tracing::warn!(
            unavailable_historical_revisions,
            "historical worker-profile revisions could not be resolved"
        );
    }
    let byte_budget = RelayByteBudget::new(process.relay_byte_budget_bytes())
        .map_err(|_| ApplicationError::RelayByteBudget)?;
    let credential_file = File::open(process.bridge_credential_file())
        .map_err(|_| ApplicationError::OpenCredential)?;
    let credential = read_bridge_credential(BufReader::new(credential_file))
        .map_err(|_| ApplicationError::Credential)?;
    let startup = ControllerStartup::from_resolved_profiles(
        client,
        process.worker_namespace().to_string(),
        process.scope_id(),
        profiles,
        current_profiles,
        policy,
        process.relay_queue_capacity(),
        byte_budget,
        &credential,
        process.endpoint_config(),
        tls,
    )
    .map_err(|_| ApplicationError::RuntimeBuild)?;
    drop(credential);
    drop(profile_config);

    let prepared = match startup_phase_or_shutdown(startup.prepare(), shutdown.wait())
        .await
        .map_err(|_| ApplicationError::SignalWait)?
    {
        StartupPhase::Completed(result) => {
            result.map_err(|_| ApplicationError::StartupContainment)?
        }
        StartupPhase::Shutdown => return Ok(()),
    };
    tracing::info!("controller startup containment completed");

    let relay_listener = match startup_phase_or_shutdown(
        TcpListener::bind(process.relay_address()),
        shutdown.wait(),
    )
    .await
    .map_err(|_| ApplicationError::SignalWait)?
    {
        StartupPhase::Completed(result) => result.map_err(|_| ApplicationError::RelayBind)?,
        StartupPhase::Shutdown => return Ok(()),
    };
    let probe_listener = match startup_phase_or_shutdown(
        TcpListener::bind(process.probe_address()),
        shutdown.wait(),
    )
    .await
    .map_err(|_| ApplicationError::SignalWait)?
    {
        StartupPhase::Completed(result) => result.map_err(|_| ApplicationError::ProbeBind)?,
        StartupPhase::Shutdown => return Ok(()),
    };

    let supervisor = prepared.into_supervisor(process.supervisor_config());
    let probe = ControllerProbeServer::new(supervisor.readiness());
    tracing::info!("controller relay and probe listeners are bound");
    supervise_pair(
        move |stop| {
            supervisor.serve_until(relay_listener, async move {
                let _ = stop.await;
            })
        },
        move |stop| {
            probe.serve_until(probe_listener, async move {
                let _ = stop.await;
            })
        },
        shutdown.wait(),
    )
    .await
    .map_err(ApplicationError::Runtime)?;
    tracing::info!("controller shutdown completed");
    Ok(())
}

fn install_tracing() -> Result<(), ()> {
    let formatter = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_ansi(false)
        .compact();
    tracing_subscriber::registry()
        .with(controller_trace_targets())
        .with(formatter)
        .try_init()
        .map_err(|_| ())
}

fn controller_trace_targets() -> Targets {
    Targets::new()
        .with_target("openab_kubernetes_session", LevelFilter::INFO)
        .with_target("openab_kubernetes_session_controller", LevelFilter::INFO)
        .with_default(LevelFilter::OFF)
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        eprintln!("openab-kubernetes-session-controller: internal panic");
    }));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    install_sanitized_panic_hook();
    if install_tracing().is_err() {
        eprintln!("openab-kubernetes-session-controller: tracing initialization failed");
        std::process::exit(1);
    }
    if let Err(error) = run(std::env::args_os().skip(1)).await {
        eprintln!("openab-kubernetes-session-controller: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor};
    use std::sync::Arc;

    const TEST_CREDENTIAL: &[u8] = b"abc_DEF-123.~+/abc_DEF-123.~+/==";

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn accepts_only_one_absolute_config_file() {
        assert_eq!(
            parse_args(args(&["--config-file", "/etc/openab/controller.toml"])),
            Ok(ControllerCommand {
                config_file: PathBuf::from("/etc/openab/controller.toml"),
            })
        );
        assert_eq!(parse_args(args(&[])), Err(CommandError::MissingConfigFile));
        assert_eq!(
            parse_args(args(&["--config-file"])),
            Err(CommandError::MissingOptionValue)
        );
        assert_eq!(
            parse_args(args(&["--unknown", "secret-value"])),
            Err(CommandError::UnknownOption)
        );
        assert_eq!(
            parse_args(args(&["--config-file", "relative.toml"])),
            Err(CommandError::RelativeConfigFile)
        );
        assert_eq!(
            parse_args(args(&[
                "--config-file",
                "/first.toml",
                "--config-file",
                "/second.toml",
            ])),
            Err(CommandError::DuplicateOption)
        );
    }

    #[test]
    fn reads_one_bounded_exact_bridge_credential() {
        assert_eq!(
            read_bridge_credential(Cursor::new(TEST_CREDENTIAL))
                .expect("valid credential")
                .as_slice(),
            TEST_CREDENTIAL
        );
        assert_eq!(
            read_bridge_credential(Cursor::new(b"short")),
            Err(CredentialLoadError::TooShort)
        );
        assert_eq!(
            read_bridge_credential(Cursor::new(vec![
                b'a';
                MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES + 1
            ])),
            Err(CredentialLoadError::TooLarge)
        );
        let mut invalid = TEST_CREDENTIAL.to_vec();
        invalid[0] = b'\n';
        assert_eq!(
            read_bridge_credential(Cursor::new(invalid)),
            Err(CredentialLoadError::InvalidByte)
        );
        assert_eq!(
            read_bridge_credential(FailingReader),
            Err(CredentialLoadError::Read)
        );
    }

    #[test]
    fn tracing_is_closed_to_dependency_targets_and_debug_events() {
        let targets = controller_trace_targets();

        assert!(targets.would_enable(
            "openab_kubernetes_session::controller::supervisor",
            &tracing::Level::INFO
        ));
        assert!(targets.would_enable(
            "openab_kubernetes_session_controller",
            &tracing::Level::WARN
        ));
        assert!(!targets.would_enable(
            "openab_kubernetes_session_controller",
            &tracing::Level::DEBUG
        ));
        for dependency in ["kube", "kube_client", "hyper", "tokio_tungstenite"] {
            assert!(!targets.would_enable(dependency, &tracing::Level::ERROR));
        }
    }

    #[tokio::test]
    async fn latched_shutdown_wins_a_simultaneously_ready_startup_phase() {
        let result = startup_phase_or_shutdown(async { 42_u8 }, async { Ok::<(), ()>(()) })
            .await
            .expect("shutdown stream remains healthy");
        assert!(matches!(result, StartupPhase::Shutdown));
    }

    #[tokio::test]
    async fn latched_runtime_shutdown_never_polls_either_server() {
        let supervisor_polled = Arc::new(AtomicBool::new(false));
        let probe_polled = Arc::new(AtomicBool::new(false));
        let supervisor_polled_in_task = Arc::clone(&supervisor_polled);
        let probe_polled_in_task = Arc::clone(&probe_polled);
        let result = supervise_pair(
            move |_stop| async move {
                supervisor_polled_in_task.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            move |_stop| async move {
                probe_polled_in_task.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            async { Ok::<(), TestError>(()) },
        )
        .await;

        assert_eq!(result, Ok(()));
        assert!(!supervisor_polled.load(Ordering::SeqCst));
        assert!(!probe_polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn latched_signal_failure_never_polls_either_server() {
        let supervisor_polled = Arc::new(AtomicBool::new(false));
        let probe_polled = Arc::new(AtomicBool::new(false));
        let supervisor_polled_in_task = Arc::clone(&supervisor_polled);
        let probe_polled_in_task = Arc::clone(&probe_polled);
        let result = supervise_pair(
            move |_stop| async move {
                supervisor_polled_in_task.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            move |_stop| async move {
                probe_polled_in_task.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            async { Err::<(), _>(TestError) },
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::SignalFailed));
        assert!(!supervisor_polled.load(Ordering::SeqCst));
        assert!(!probe_polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn normal_shutdown_keeps_probe_until_supervisor_drain_finishes() {
        let probe_stopped = Arc::new(AtomicBool::new(false));
        let probe_stopped_in_task = Arc::clone(&probe_stopped);
        let (supervisor_started_tx, supervisor_started_rx) = oneshot::channel();
        let (probe_started_tx, probe_started_rx) = oneshot::channel();
        let (supervisor_stopping_tx, supervisor_stopping_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(supervise_pair(
            move |stop| async move {
                let _ = supervisor_started_tx.send(());
                let _ = stop.await;
                let _ = supervisor_stopping_tx.send(());
                let _ = drain_rx.await;
                Ok::<(), TestError>(())
            },
            move |stop| async move {
                let _ = probe_started_tx.send(());
                let _ = stop.await;
                probe_stopped_in_task.store(true, Ordering::SeqCst);
                Ok::<(), TestError>(())
            },
            async move { shutdown_rx.await.map_err(|_| TestError) },
        ));

        supervisor_started_rx.await.expect("supervisor started");
        probe_started_rx.await.expect("probe started");
        shutdown_tx.send(()).expect("request shutdown");
        supervisor_stopping_rx
            .await
            .expect("supervisor began its bounded drain");
        assert!(!probe_stopped.load(Ordering::SeqCst));
        drain_tx.send(()).expect("finish supervisor drain");
        assert_eq!(task.await.expect("orchestrator task joined"), Ok(()));
        assert!(probe_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_probe_failure_stops_supervisor_and_keeps_probe_provenance() {
        let supervisor_stopped = Arc::new(AtomicBool::new(false));
        let supervisor_stopped_in_task = Arc::clone(&supervisor_stopped);
        let result = supervise_pair(
            move |stop| async move {
                let _ = stop.await;
                supervisor_stopped_in_task.store(true, Ordering::SeqCst);
                Ok::<(), TestError>(())
            },
            |_stop| async { Err::<(), _>(TestError) },
            std::future::pending::<Result<(), TestError>>(),
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::ProbeFailed));
        assert!(supervisor_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_supervisor_panic_stops_probe_without_exposing_payload() {
        let probe_stopped = Arc::new(AtomicBool::new(false));
        let probe_stopped_in_task = Arc::clone(&probe_stopped);
        let result = supervise_pair(
            |_stop| async {
                panic!("sensitive panic payload");
                #[allow(unreachable_code)]
                Ok::<(), TestError>(())
            },
            move |stop| async move {
                let _ = stop.await;
                probe_stopped_in_task.store(true, Ordering::SeqCst);
                Ok::<(), TestError>(())
            },
            std::future::pending::<Result<(), TestError>>(),
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::SupervisorPanicked));
        assert!(probe_stopped.load(Ordering::SeqCst));
        assert!(!result
            .expect_err("supervisor panic is fatal")
            .to_string()
            .contains("sensitive"));
    }

    #[tokio::test]
    async fn supervisor_factory_panic_is_categorized_and_stops_probe() {
        let probe_stopped = Arc::new(AtomicBool::new(false));
        let probe_stopped_in_task = Arc::clone(&probe_stopped);
        let result = supervise_pair(
            |_stop| {
                panic!("sensitive factory panic payload");
                #[allow(unreachable_code)]
                std::future::ready(Ok::<(), TestError>(()))
            },
            move |stop| async move {
                let _ = stop.await;
                probe_stopped_in_task.store(true, Ordering::SeqCst);
                Ok::<(), TestError>(())
            },
            std::future::pending::<Result<(), TestError>>(),
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::SupervisorPanicked));
        assert!(probe_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn first_probe_failure_survives_supervisor_cleanup_panic() {
        let result = supervise_pair(
            |stop| async move {
                let _ = stop.await;
                panic!("secondary supervisor panic");
                #[allow(unreachable_code)]
                Ok::<(), TestError>(())
            },
            |_stop| async { Err::<(), _>(TestError) },
            std::future::pending::<Result<(), TestError>>(),
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::ProbeFailed));
    }

    #[tokio::test]
    async fn probe_failure_during_normal_drain_remains_fatal() {
        let (supervisor_started_tx, supervisor_started_rx) = oneshot::channel();
        let (probe_started_tx, probe_started_rx) = oneshot::channel();
        let (supervisor_stopping_tx, supervisor_stopping_rx) = oneshot::channel();
        let (drain_tx, drain_rx) = oneshot::channel();
        let (probe_failure_tx, probe_failure_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(supervise_pair(
            move |stop| async move {
                let _ = supervisor_started_tx.send(());
                let _ = stop.await;
                let _ = supervisor_stopping_tx.send(());
                let _ = drain_rx.await;
                Ok::<(), TestError>(())
            },
            move |stop| async move {
                let _ = probe_started_tx.send(());
                tokio::select! {
                    _ = stop => Ok::<(), TestError>(()),
                    _ = probe_failure_rx => Err(TestError),
                }
            },
            async move { shutdown_rx.await.map_err(|_| TestError) },
        ));

        supervisor_started_rx.await.expect("supervisor started");
        probe_started_rx.await.expect("probe started");
        shutdown_tx.send(()).expect("request shutdown");
        supervisor_stopping_rx
            .await
            .expect("supervisor began its bounded drain");
        probe_failure_tx.send(()).expect("fail probe during drain");
        drain_tx
            .send(())
            .expect("settle supervisor after probe failure");
        assert_eq!(
            task.await.expect("orchestrator task joined"),
            Err(RuntimePairError::ProbeFailed)
        );
    }

    #[tokio::test]
    async fn signal_stream_failure_still_settles_both_children() {
        let supervisor_stopped = Arc::new(AtomicBool::new(false));
        let probe_stopped = Arc::new(AtomicBool::new(false));
        let supervisor_stopped_in_task = Arc::clone(&supervisor_stopped);
        let probe_stopped_in_task = Arc::clone(&probe_stopped);
        let result = supervise_pair(
            move |stop| async move {
                let _ = stop.await;
                supervisor_stopped_in_task.store(true, Ordering::SeqCst);
                Err::<(), _>(TestError)
            },
            move |stop| async move {
                let _ = stop.await;
                probe_stopped_in_task.store(true, Ordering::SeqCst);
                Err::<(), _>(TestError)
            },
            async {
                tokio::task::yield_now().await;
                Err::<(), _>(TestError)
            },
        )
        .await;

        assert_eq!(result, Err(RuntimePairError::SignalFailed));
        assert!(supervisor_stopped.load(Ordering::SeqCst));
        assert!(probe_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelling_orchestration_drops_both_child_futures() {
        let supervisor_dropped = Arc::new(AtomicBool::new(false));
        let probe_dropped = Arc::new(AtomicBool::new(false));
        let supervisor_dropped_in_task = Arc::clone(&supervisor_dropped);
        let probe_dropped_in_task = Arc::clone(&probe_dropped);
        let task = tokio::spawn(supervise_pair(
            move |_stop| async move {
                let _guard = DropFlag(supervisor_dropped_in_task);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            move |_stop| async move {
                let _guard = DropFlag(probe_dropped_in_task);
                std::future::pending::<()>().await;
                Ok::<(), TestError>(())
            },
            std::future::pending::<Result<(), TestError>>(),
        ));

        tokio::task::yield_now().await;
        task.abort();
        assert!(task
            .await
            .expect_err("orchestration was cancelled")
            .is_cancelled());
        assert!(supervisor_dropped.load(Ordering::SeqCst));
        assert!(probe_dropped.load(Ordering::SeqCst));
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("sensitive reader detail"))
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct TestError;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
}
