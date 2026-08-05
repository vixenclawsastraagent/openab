//! Direct, one-shot ACP child process ownership.

use super::bootstrap::{
    WorkerCommand, HOME, HOME_ENV, SESSION_ROOT, SESSION_ROOT_ENV, WORKSPACE, WORKSPACE_ENV,
};
use super::workspace::{PreparedWorkspace, WorkspacePreparationError};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::Instant;

#[cfg(unix)]
use rustix::fd::{AsRawFd, BorrowedFd};
#[cfg(unix)]
use rustix::process::{Pid, Signal};

pub(super) const WORKER_CHILD_TERM_GRACE: Duration = Duration::from_secs(10);
const PROCESS_GROUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const POST_KILL_GROUP_EXIT_TIMEOUT: Duration = Duration::from_secs(2);
const CHILD_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const CHILD_USER: &str = "agent";

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum WorkerProcessError {
    #[error(transparent)]
    Workspace(#[from] WorkspacePreparationError),
    #[error("the ACP child could not be started")]
    Spawn,
    #[error("the ACP child standard streams are unavailable")]
    ChildStdio,
    #[error("the ACP child could not be reaped")]
    ChildWait,
    #[error("the ACP child process group could not receive TERM")]
    TerminateGroup,
    #[error("the ACP child process group could not be inspected")]
    InspectGroup,
    #[error("the ACP child process group could not receive KILL")]
    KillGroup,
    #[error("the ACP child leader could not receive fallback KILL")]
    KillLeader,
    #[error("the ACP child process group survived KILL")]
    GroupSurvived,
}

#[cfg(unix)]
pub(super) struct WorkerProcess {
    child: Child,
    process_group: Pid,
    child_stdin: Option<ChildStdin>,
    child_stdout: Option<ChildStdout>,
    exit_success: Option<bool>,
    armed: bool,
}

#[cfg(unix)]
impl WorkerProcess {
    pub(super) fn take_stdio(&mut self) -> Result<(ChildStdout, ChildStdin), WorkerProcessError> {
        let stdout = self
            .child_stdout
            .take()
            .ok_or(WorkerProcessError::ChildStdio)?;
        let stdin = self
            .child_stdin
            .take()
            .ok_or(WorkerProcessError::ChildStdio)?;
        Ok((stdout, stdin))
    }

    pub(super) async fn wait_for_exit(&mut self) -> Result<bool, WorkerProcessError> {
        if let Some(success) = self.exit_success {
            return Ok(success);
        }
        let status = self
            .child
            .wait()
            .await
            .map_err(|_| WorkerProcessError::ChildWait)?;
        let success = status.success();
        self.exit_success = Some(success);
        Ok(success)
    }

    pub(super) async fn terminate_and_reap(&mut self) -> Result<(), WorkerProcessError> {
        let result = terminate_process_tree(
            &mut self.child,
            &mut self.exit_success,
            self.process_group,
            &SystemProcessGroups,
            &TokioClock,
        )
        .await;
        if result.is_ok() {
            self.armed = false;
        }
        result
    }

    pub(super) fn exit_success(&self) -> Option<bool> {
        self.exit_success
    }
}

#[cfg(unix)]
impl Drop for WorkerProcess {
    fn drop(&mut self) {
        if self.armed {
            let _ = SystemProcessGroups.kill(self.process_group);
            let _ = self.child.start_kill();
        }
    }
}

pub(super) fn spawn_child(
    command: &WorkerCommand,
    workspace: &PreparedWorkspace,
) -> Result<WorkerProcess, WorkerProcessError> {
    spawn_child_with(command, workspace, |configured| configured.spawn())
}

#[cfg(unix)]
fn spawn_child_with<F>(
    child_command: &WorkerCommand,
    workspace: &PreparedWorkspace,
    spawn: F,
) -> Result<WorkerProcess, WorkerProcessError>
where
    F: FnOnce(&mut Command) -> std::io::Result<Child>,
{
    let workspace_fd = workspace.workspace_fd().as_raw_fd();
    let mut command = Command::new(child_command.executable());
    command
        .args(child_command.arguments())
        .env_clear()
        .env(HOME_ENV, HOME)
        .env(SESSION_ROOT_ENV, SESSION_ROOT)
        .env(WORKSPACE_ENV, WORKSPACE)
        .env("PATH", CHILD_PATH)
        .env("USER", CHILD_USER)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .process_group(0);

    // SAFETY: the retained descriptor remains alive until `spawn` returns.
    // `fchdir` is async-signal-safe and this closure performs no allocation.
    unsafe {
        command.pre_exec(move || {
            let workspace = BorrowedFd::borrow_raw(workspace_fd);
            rustix::process::fchdir(workspace).map_err(std::io::Error::from)
        });
    }

    // This is deliberately the final parent-side operation before spawn. The
    // child then enters the already-retained directory capability in pre-exec.
    workspace.revalidate_bindings()?;
    let mut child = spawn(&mut command).map_err(|_| WorkerProcessError::Spawn)?;
    let process_group = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
        .filter(|pid| pid.as_raw_pid() > 1)
        .ok_or_else(|| {
            let _ = child.start_kill();
            WorkerProcessError::Spawn
        })?;
    let child_stdin = child.stdin.take();
    let child_stdout = child.stdout.take();

    Ok(WorkerProcess {
        child,
        process_group,
        child_stdin,
        child_stdout,
        exit_success: None,
        armed: true,
    })
}

#[async_trait]
trait DirectChild: Send {
    fn try_wait_sanitized(&mut self) -> Result<Option<bool>, WorkerProcessError>;
    fn start_kill_sanitized(&mut self) -> Result<(), WorkerProcessError>;
    async fn wait_sanitized(&mut self) -> Result<bool, WorkerProcessError>;
}

#[async_trait]
impl DirectChild for Child {
    fn try_wait_sanitized(&mut self) -> Result<Option<bool>, WorkerProcessError> {
        self.try_wait()
            .map(|status| status.map(|status| status.success()))
            .map_err(|_| WorkerProcessError::ChildWait)
    }

    fn start_kill_sanitized(&mut self) -> Result<(), WorkerProcessError> {
        self.start_kill()
            .map_err(|_| WorkerProcessError::KillLeader)
    }

    async fn wait_sanitized(&mut self) -> Result<bool, WorkerProcessError> {
        self.wait()
            .await
            .map(|status| status.success())
            .map_err(|_| WorkerProcessError::ChildWait)
    }
}

#[cfg(unix)]
trait ProcessGroups: Send + Sync {
    fn terminate(&self, process_group: Pid) -> Result<(), WorkerProcessError>;
    fn kill(&self, process_group: Pid) -> Result<(), WorkerProcessError>;
    fn exists(&self, process_group: Pid) -> Result<bool, WorkerProcessError>;
}

#[cfg(unix)]
struct SystemProcessGroups;

#[cfg(unix)]
impl SystemProcessGroups {
    fn signal(
        process_group: Pid,
        signal: Signal,
        failure: WorkerProcessError,
    ) -> Result<(), WorkerProcessError> {
        match rustix::process::kill_process_group(process_group, signal) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            #[cfg(all(target_os = "macos", test))]
            Err(rustix::io::Errno::PERM) => Ok(()),
            Err(_) => Err(failure),
        }
    }
}

#[cfg(unix)]
impl ProcessGroups for SystemProcessGroups {
    fn terminate(&self, process_group: Pid) -> Result<(), WorkerProcessError> {
        Self::signal(
            process_group,
            Signal::TERM,
            WorkerProcessError::TerminateGroup,
        )
    }

    fn kill(&self, process_group: Pid) -> Result<(), WorkerProcessError> {
        Self::signal(process_group, Signal::KILL, WorkerProcessError::KillGroup)
    }

    fn exists(&self, process_group: Pid) -> Result<bool, WorkerProcessError> {
        match rustix::process::test_kill_process_group(process_group) {
            Ok(()) => Ok(true),
            Err(rustix::io::Errno::SRCH) => Ok(false),
            // Darwin reports EPERM for a just-reaped, now-empty process
            // group. Production workers are Linux-only; this compatibility
            // branch keeps the Unix capability contract testable on macOS.
            #[cfg(all(target_os = "macos", test))]
            Err(rustix::io::Errno::PERM) => Ok(false),
            Err(_) => Err(WorkerProcessError::InspectGroup),
        }
    }
}

trait TerminationClock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

struct TokioClock;

impl TerminationClock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep_until(deadline))
    }
}

#[cfg(unix)]
async fn terminate_process_tree<C, G, K>(
    child: &mut C,
    exit_success: &mut Option<bool>,
    process_group: Pid,
    groups: &G,
    clock: &K,
) -> Result<(), WorkerProcessError>
where
    C: DirectChild,
    G: ProcessGroups,
    K: TerminationClock,
{
    let mut first_error = None;
    if exit_success.is_none() {
        match child.try_wait_sanitized() {
            Ok(status) => *exit_success = status,
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    let mut group_present = match groups.exists(process_group) {
        Ok(present) => present,
        Err(error) => {
            first_error.get_or_insert(error);
            true
        }
    };
    let deadline = clock.now() + WORKER_CHILD_TERM_GRACE;
    if group_present {
        if let Err(error) = groups.terminate(process_group) {
            first_error.get_or_insert(error);
        }
    }

    while group_present {
        if exit_success.is_none() {
            match child.try_wait_sanitized() {
                Ok(status) => *exit_success = status,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        match groups.exists(process_group) {
            Ok(false) => {
                group_present = false;
                break;
            }
            Ok(true) => {}
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }

        let now = clock.now();
        if now >= deadline {
            break;
        }
        clock
            .sleep_until(std::cmp::min(deadline, now + PROCESS_GROUP_POLL_INTERVAL))
            .await;
    }

    let killed_group = group_present;
    if killed_group {
        if let Err(error) = groups.kill(process_group) {
            first_error.get_or_insert(error);
        }
    }
    if exit_success.is_none() {
        match child.try_wait_sanitized() {
            Ok(status) => *exit_success = status,
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if exit_success.is_none() {
        let leader_kill_error = child.start_kill_sanitized().err();
        match child.wait_sanitized().await {
            Ok(success) => *exit_success = Some(success),
            Err(error) => {
                if let Some(kill_error) = leader_kill_error {
                    first_error.get_or_insert(kill_error);
                }
                first_error.get_or_insert(error);
            }
        }
    }

    if killed_group {
        let absence_deadline = clock.now() + POST_KILL_GROUP_EXIT_TIMEOUT;
        loop {
            match groups.exists(process_group) {
                Ok(false) => break,
                Ok(true) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                    break;
                }
            }
            let now = clock.now();
            if now >= absence_deadline {
                first_error.get_or_insert(WorkerProcessError::GroupSurvived);
                break;
            }
            clock
                .sleep_until(std::cmp::min(
                    absence_deadline,
                    now + PROCESS_GROUP_POLL_INTERVAL,
                ))
                .await;
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::worker::bootstrap::WorkerCommand;
    use crate::worker::workspace::{prepare_workspace_beneath, WorkspaceIdentity};
    use rustix::fd::OwnedFd;
    use rustix::fs::{Mode, OFlags};
    use std::collections::{BTreeSet, VecDeque};
    use std::ffi::OsString;
    use std::fs;
    use std::future;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncReadExt;
    use uuid::Uuid;

    struct TemporaryWorkspace {
        parent: PathBuf,
    }

    impl TemporaryWorkspace {
        fn prepare() -> (Self, PreparedWorkspace) {
            let parent =
                std::env::temp_dir().join(format!("openab-worker-process-{}", Uuid::new_v4()));
            fs::create_dir(&parent).unwrap();
            fs::create_dir(parent.join("session")).unwrap();
            let parent_fd = open_directory(&parent);
            let workspace =
                prepare_workspace_beneath(parent_fd, WorkspaceIdentity::current()).unwrap();
            (Self { parent }, workspace)
        }

        fn workspace(&self) -> PathBuf {
            self.parent.join("session/workspace")
        }
    }

    impl Drop for TemporaryWorkspace {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.parent).unwrap();
        }
    }

    fn open_directory(path: &Path) -> OwnedFd {
        rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .unwrap()
    }

    fn worker_command(executable: &str, arguments: &[&str]) -> WorkerCommand {
        let values = ["serve", "--", executable]
            .into_iter()
            .chain(arguments.iter().copied())
            .map(OsString::from);
        WorkerCommand::parse(values).unwrap()
    }

    async fn read_stdout_and_reap(process: &mut WorkerProcess) -> String {
        let (mut stdout, stdin) = process.take_stdio().unwrap();
        drop(stdin);
        let mut output = String::new();
        stdout.read_to_string(&mut output).await.unwrap();
        assert!(process.wait_for_exit().await.unwrap());
        process.terminate_and_reap().await.unwrap();
        output
    }

    #[tokio::test]
    async fn child_cwd_uses_retained_capability_after_path_replacement() {
        let (temporary, workspace) = TemporaryWorkspace::prepare();
        let original = temporary.workspace();
        let retained = original.with_file_name("workspace-retained");
        let command = worker_command("/bin/pwd", &[]);

        let mut process = spawn_child_with(&command, &workspace, |configured| {
            fs::rename(&original, &retained).unwrap();
            fs::create_dir(&original).unwrap();
            configured.spawn()
        })
        .unwrap();
        let output = read_stdout_and_reap(&mut process).await;

        assert_eq!(
            PathBuf::from(output.trim()),
            fs::canonicalize(retained).unwrap()
        );
    }

    #[tokio::test]
    async fn child_environment_is_an_exact_fixed_allowlist() {
        let (_temporary, workspace) = TemporaryWorkspace::prepare();
        let command = worker_command("/usr/bin/env", &[]);
        let mut process = spawn_child(&command, &workspace).unwrap();

        let actual = read_stdout_and_reap(&mut process)
            .await
            .lines()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let expected = [
            format!("HOME={HOME}"),
            format!("OPENAB_SESSION_ROOT={SESSION_ROOT}"),
            format!("OPENAB_WORKSPACE={WORKSPACE}"),
            format!("PATH={CHILD_PATH}"),
            format!("USER={CHILD_USER}"),
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn child_process_group_is_created_before_exec() {
        let (_temporary, workspace) = TemporaryWorkspace::prepare();
        let command = worker_command("/bin/sleep", &["30"]);
        let mut process = spawn_child(&command, &workspace).unwrap();

        assert_eq!(
            rustix::process::getpgid(Some(process.process_group)).unwrap(),
            process.process_group
        );
        process.terminate_and_reap().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_reaps_a_child_observed_through_stdout_eof() {
        let (_temporary, workspace) = TemporaryWorkspace::prepare();
        let command = worker_command("/bin/echo", &["{}"]);
        let mut process = spawn_child(&command, &workspace).unwrap();
        let (mut stdout, stdin) = process.take_stdio().unwrap();
        drop(stdin);
        let mut output = String::new();
        stdout.read_to_string(&mut output).await.unwrap();
        assert_eq!(output, "{}\n");
        process.terminate_and_reap().await.unwrap();
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Event {
        Terminate,
        TryWait,
        Exists,
        Sleep(Duration),
        Kill,
        StartKill,
        Wait,
    }

    type Events = Arc<Mutex<Vec<Event>>>;

    struct FakeChild {
        events: Events,
        try_results: VecDeque<Result<Option<bool>, WorkerProcessError>>,
        default_try_result: Result<Option<bool>, WorkerProcessError>,
        wait_result: Result<bool, WorkerProcessError>,
    }

    #[async_trait]
    impl DirectChild for FakeChild {
        fn try_wait_sanitized(&mut self) -> Result<Option<bool>, WorkerProcessError> {
            self.events.lock().unwrap().push(Event::TryWait);
            self.try_results
                .pop_front()
                .unwrap_or(self.default_try_result)
        }

        fn start_kill_sanitized(&mut self) -> Result<(), WorkerProcessError> {
            self.events.lock().unwrap().push(Event::StartKill);
            Ok(())
        }

        async fn wait_sanitized(&mut self) -> Result<bool, WorkerProcessError> {
            self.events.lock().unwrap().push(Event::Wait);
            self.wait_result
        }
    }

    struct FakeGroups {
        events: Events,
        existence: Mutex<VecDeque<Result<bool, WorkerProcessError>>>,
        default_exists: Result<bool, WorkerProcessError>,
        killed: Mutex<bool>,
        post_kill_existence: Mutex<VecDeque<Result<bool, WorkerProcessError>>>,
        default_post_kill_exists: Result<bool, WorkerProcessError>,
    }

    impl ProcessGroups for FakeGroups {
        fn terminate(&self, _process_group: Pid) -> Result<(), WorkerProcessError> {
            self.events.lock().unwrap().push(Event::Terminate);
            Ok(())
        }

        fn kill(&self, _process_group: Pid) -> Result<(), WorkerProcessError> {
            self.events.lock().unwrap().push(Event::Kill);
            *self.killed.lock().unwrap() = true;
            Ok(())
        }

        fn exists(&self, _process_group: Pid) -> Result<bool, WorkerProcessError> {
            self.events.lock().unwrap().push(Event::Exists);
            if *self.killed.lock().unwrap() {
                self.post_kill_existence
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(self.default_post_kill_exists)
            } else {
                self.existence
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(self.default_exists)
            }
        }
    }

    struct StepClock {
        start: Instant,
        now: Mutex<Instant>,
        events: Events,
    }

    impl StepClock {
        fn new(events: Events) -> Self {
            let start = Instant::now();
            Self {
                start,
                now: Mutex::new(start),
                events,
            }
        }
    }

    impl TerminationClock for StepClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }

        fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            *self.now.lock().unwrap() = deadline;
            self.events
                .lock()
                .unwrap()
                .push(Event::Sleep(deadline - self.start));
            Box::pin(future::ready(()))
        }
    }

    fn test_pid() -> Pid {
        Pid::from_raw(42).unwrap()
    }

    #[tokio::test]
    async fn graceful_group_exit_skips_kill_and_reaps_once() {
        let events = Events::default();
        let mut child = FakeChild {
            events: Arc::clone(&events),
            try_results: VecDeque::from([Ok(None), Ok(Some(true))]),
            default_try_result: Ok(Some(true)),
            wait_result: Ok(true),
        };
        let groups = FakeGroups {
            events: Arc::clone(&events),
            existence: Mutex::new(VecDeque::from([Ok(true), Ok(false)])),
            default_exists: Ok(false),
            killed: Mutex::new(false),
            post_kill_existence: Mutex::new(VecDeque::new()),
            default_post_kill_exists: Ok(false),
        };
        let clock = StepClock::new(Arc::clone(&events));
        let mut exit_success = None;

        terminate_process_tree(&mut child, &mut exit_success, test_pid(), &groups, &clock)
            .await
            .unwrap();

        assert_eq!(exit_success, Some(true));
        assert!(!events.lock().unwrap().contains(&Event::Kill));
        assert!(!events.lock().unwrap().contains(&Event::Wait));
    }

    #[tokio::test]
    async fn persistent_group_is_killed_only_at_the_single_ten_second_deadline() {
        let events = Events::default();
        let mut child = FakeChild {
            events: Arc::clone(&events),
            try_results: VecDeque::new(),
            default_try_result: Ok(None),
            wait_result: Ok(false),
        };
        let groups = FakeGroups {
            events: Arc::clone(&events),
            existence: Mutex::new(VecDeque::new()),
            default_exists: Ok(true),
            killed: Mutex::new(false),
            post_kill_existence: Mutex::new(VecDeque::new()),
            default_post_kill_exists: Ok(false),
        };
        let clock = StepClock::new(Arc::clone(&events));
        let mut exit_success = None;

        terminate_process_tree(&mut child, &mut exit_success, test_pid(), &groups, &clock)
            .await
            .unwrap();

        let events = events.lock().unwrap();
        let kill = events
            .iter()
            .position(|event| *event == Event::Kill)
            .unwrap();
        assert!(events[..kill].iter().all(|event| *event != Event::Kill));
        assert_eq!(
            events[..kill]
                .iter()
                .filter_map(|event| match event {
                    Event::Sleep(elapsed) => Some(*elapsed),
                    _ => None,
                })
                .next_back(),
            Some(WORKER_CHILD_TERM_GRACE)
        );
        assert_eq!(
            events[kill + 1..],
            [Event::TryWait, Event::StartKill, Event::Wait, Event::Exists,]
        );
        assert_eq!(exit_success, Some(false));
    }

    #[tokio::test]
    async fn reaped_leader_does_not_hide_a_persistent_descendant_group() {
        let events = Events::default();
        let mut child = FakeChild {
            events: Arc::clone(&events),
            try_results: VecDeque::from([Ok(Some(true))]),
            default_try_result: Ok(Some(true)),
            wait_result: Ok(true),
        };
        let groups = FakeGroups {
            events: Arc::clone(&events),
            existence: Mutex::new(VecDeque::new()),
            default_exists: Ok(true),
            killed: Mutex::new(false),
            post_kill_existence: Mutex::new(VecDeque::new()),
            default_post_kill_exists: Ok(false),
        };
        let clock = StepClock::new(Arc::clone(&events));
        let mut exit_success = None;

        terminate_process_tree(&mut child, &mut exit_success, test_pid(), &groups, &clock)
            .await
            .unwrap();

        let events = events.lock().unwrap();
        assert!(events.contains(&Event::Kill));
        assert!(!events.contains(&Event::StartKill));
        assert!(!events.contains(&Event::Wait));
        assert_eq!(exit_success, Some(true));
    }

    #[tokio::test]
    async fn cleanup_waits_for_group_absence_after_kill() {
        let events = Events::default();
        let mut child = FakeChild {
            events: Arc::clone(&events),
            try_results: VecDeque::new(),
            default_try_result: Ok(None),
            wait_result: Ok(true),
        };
        let groups = FakeGroups {
            events: Arc::clone(&events),
            existence: Mutex::new(VecDeque::new()),
            default_exists: Ok(true),
            killed: Mutex::new(false),
            post_kill_existence: Mutex::new(VecDeque::from([Ok(true), Ok(false)])),
            default_post_kill_exists: Ok(false),
        };
        let clock = StepClock::new(Arc::clone(&events));
        let mut exit_success = None;

        terminate_process_tree(&mut child, &mut exit_success, test_pid(), &groups, &clock)
            .await
            .unwrap();

        let events = events.lock().unwrap();
        let kill = events
            .iter()
            .position(|event| *event == Event::Kill)
            .unwrap();
        assert!(events[kill + 1..].iter().any(|event| {
            matches!(event, Event::Sleep(elapsed) if *elapsed > WORKER_CHILD_TERM_GRACE)
        }));
        assert_eq!(events.last(), Some(&Event::Exists));
    }

    #[tokio::test]
    async fn cleanup_fails_if_the_group_survives_kill() {
        let events = Events::default();
        let mut child = FakeChild {
            events: Arc::clone(&events),
            try_results: VecDeque::new(),
            default_try_result: Ok(None),
            wait_result: Ok(true),
        };
        let groups = FakeGroups {
            events: Arc::clone(&events),
            existence: Mutex::new(VecDeque::new()),
            default_exists: Ok(true),
            killed: Mutex::new(false),
            post_kill_existence: Mutex::new(VecDeque::new()),
            default_post_kill_exists: Ok(true),
        };
        let clock = StepClock::new(Arc::clone(&events));
        let mut exit_success = None;

        assert_eq!(
            terminate_process_tree(&mut child, &mut exit_success, test_pid(), &groups, &clock,)
                .await,
            Err(WorkerProcessError::GroupSurvived)
        );
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    Event::Sleep(elapsed) => Some(*elapsed),
                    _ => None,
                })
                .next_back(),
            Some(WORKER_CHILD_TERM_GRACE + POST_KILL_GROUP_EXIT_TIMEOUT)
        );
    }
}
