#![cfg(all(feature = "worker-runtime", unix))]

#[path = "support/registered_worker.rs"]
mod registered_worker;

use futures_util::StreamExt;
#[cfg(target_os = "linux")]
use openab_kubernetes_session::wire::{decode_frame, WorkerToControllerV1};
use openab_kubernetes_session::worker::bootstrap::WorkerCommand;
use openab_kubernetes_session::worker::supervisor::{
    supervise_registered_worker, WorkerSupervisionError,
};
use openab_kubernetes_session::worker::workspace::{
    prepare_workspace_beneath, PreparedWorkspace, WorkspaceIdentity,
};
#[cfg(target_os = "linux")]
use openab_kubernetes_session::worker::workspace::{WorkspaceDirectory, WorkspacePreparationError};
use registered_worker::registered_worker_pair_with_command;
use rustix::fd::OwnedFd;
use rustix::fs::{Mode, OFlags};
use std::ffi::OsString;
use std::fs;
use std::future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

struct TemporaryWorkspace {
    parent: PathBuf,
}

impl TemporaryWorkspace {
    fn prepare() -> (Self, PreparedWorkspace) {
        let parent =
            std::env::temp_dir().join(format!("openab-worker-supervision-{}", Uuid::new_v4()));
        fs::create_dir(&parent).unwrap();
        fs::create_dir(parent.join("session")).unwrap();
        let parent_fd = open_directory(&parent);
        let workspace = prepare_workspace_beneath(parent_fd, WorkspaceIdentity::current()).unwrap();
        (Self { parent }, workspace)
    }

    #[cfg(target_os = "linux")]
    fn session(&self) -> PathBuf {
        self.parent.join("session")
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

fn command<I>(executable: &str, arguments: I) -> WorkerCommand
where
    I: IntoIterator<Item = OsString>,
{
    let mut values = vec![
        OsString::from("serve"),
        OsString::from("--"),
        OsString::from(executable),
    ];
    values.extend(arguments);
    WorkerCommand::parse(values).unwrap()
}

#[cfg(target_os = "linux")]
async fn next_worker_acp(
    controller: &mut registered_worker::ControllerSocket,
) -> serde_json::Value {
    let frame = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("worker ACP frame timed out")
        .expect("worker socket ended before ACP")
        .expect("worker socket failed before ACP");
    let Message::Text(text) = frame else {
        panic!("worker supervisor must emit a text application frame");
    };
    let WorkerToControllerV1::Acp(message) =
        decode_frame::<WorkerToControllerV1>(text.as_bytes()).unwrap()
    else {
        panic!("registered worker emitted a non-ACP application frame");
    };
    message.into_payload()
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn non_linux_supervisor_fails_closed_without_spawning() {
    let (temporary, workspace) = TemporaryWorkspace::prepare();
    let marker = temporary.parent.join("child-started");
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/usr/bin/touch",
        [marker.clone().into_os_string()],
    ))
    .await;

    assert_eq!(
        supervise_registered_worker(registered, workspace, future::pending()).await,
        Err(WorkerSupervisionError::UnsupportedRuntime)
    );
    assert!(!marker.exists());
    let closed = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("controller lane remained open after unsupported supervision");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn post_ack_workspace_replacement_prevents_spawn_and_closes_socket() {
    let (temporary, workspace) = TemporaryWorkspace::prepare();
    let marker = temporary.parent.join("child-started");
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/usr/bin/touch",
        [marker.clone().into_os_string()],
    ))
    .await;

    fs::rename(
        temporary.session().join("workspace"),
        temporary.session().join("workspace-retained"),
    )
    .unwrap();
    fs::create_dir(temporary.session().join("workspace")).unwrap();

    let error = supervise_registered_worker(registered, workspace, future::pending())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        WorkerSupervisionError::Workspace(WorkspacePreparationError::DirectoryReplaced {
            directory: WorkspaceDirectory::Workspace,
        })
    );
    assert!(!marker.exists());

    let closed = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("controller lane remained open after rejected workspace");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn latched_shutdown_closes_the_acknowledged_lane_without_spawning() {
    let (temporary, workspace) = TemporaryWorkspace::prepare();
    let marker = temporary.parent.join("child-started");
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/usr/bin/touch",
        [marker.clone().into_os_string()],
    ))
    .await;

    assert_eq!(
        supervise_registered_worker(registered, workspace, future::ready(Ok(()))).await,
        Ok(())
    );
    assert!(!marker.exists());
    let closed = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("controller lane remained open after latched shutdown");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn direct_exec_preserves_literal_shell_metacharacters() {
    let (temporary, workspace) = TemporaryWorkspace::prepare();
    let marker = temporary.parent.join("shell-must-not-run");
    let arguments = [
        OsString::from("{}"),
        OsString::from(";"),
        OsString::from("/usr/bin/touch"),
        marker.clone().into_os_string(),
    ];
    let (registered, _controller) =
        registered_worker_pair_with_command(command("/bin/echo", arguments)).await;

    assert_eq!(
        supervise_registered_worker(registered, workspace, future::pending())
            .await
            .unwrap_err(),
        WorkerSupervisionError::Relay(
            openab_kubernetes_session::worker::relay::WorkerRelayError::InvalidChildMessage
        )
    );
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn clean_stdout_eof_does_not_hide_a_nonzero_child_exit() {
    let (_temporary, workspace) = TemporaryWorkspace::prepare();
    let (registered, _controller) = registered_worker_pair_with_command(command(
        "/bin/sh",
        [OsString::from("-c"), OsString::from("exit 17")],
    ))
    .await;

    assert_eq!(
        supervise_registered_worker(registered, workspace, future::pending()).await,
        Err(WorkerSupervisionError::ChildFailed)
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn acknowledged_child_uses_private_cwd_fixed_env_and_own_process_group() {
    let (temporary, workspace) = TemporaryWorkspace::prepare();
    let script = r#"
pgid=$(ps -o pgid= -p $$ | tr -d ' ')
printf '{"cwd":"%s","home":"%s","root":"%s","workspace":"%s","path":"%s","user":"%s","transport":"%s","pid":%s,"pgid":%s}\n' \
  "$PWD" "$HOME" "$OPENAB_SESSION_ROOT" "$OPENAB_WORKSPACE" "$PATH" "$USER" \
  "${OPENAB_SESSION_CONTROLLER_URL}${OPENAB_SESSION_CONTROLLER_CA_FILE}${OPENAB_REGISTRATION_TOKEN_FILE}${OPENAB_REGISTRATION_BINDING_FILE}${OPENAB_WORKER_POD_UID}" \
  "$$" "$pgid"
"#;
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/bin/sh",
        [OsString::from("-c"), OsString::from(script)],
    ))
    .await;
    let supervisor = tokio::spawn(supervise_registered_worker(
        registered,
        workspace,
        future::pending(),
    ));

    let report = next_worker_acp(&mut controller).await;
    assert_eq!(
        report["cwd"],
        fs::canonicalize(temporary.session().join("workspace"))
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    assert_eq!(report["home"], "/session/home");
    assert_eq!(report["root"], "/session");
    assert_eq!(report["workspace"], "/session/workspace");
    assert_eq!(
        report["path"],
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    );
    assert_eq!(report["user"], "agent");
    assert_eq!(report["transport"], "");
    assert_eq!(report["pid"], report["pgid"]);

    assert_eq!(
        timeout(TEST_TIMEOUT, supervisor)
            .await
            .expect("clean child did not stop supervision")
            .unwrap(),
        Ok(())
    );
}

#[cfg(target_os = "linux")]
async fn wait_until_process_is_absent(raw_pid: u64) {
    let raw_pid = i32::try_from(raw_pid).unwrap();
    let pid = rustix::process::Pid::from_raw(raw_pid).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match rustix::process::test_kill_process(pid) {
            Err(rustix::io::Errno::SRCH) => return,
            _ if tokio::time::Instant::now() >= deadline => {
                panic!("supervisor left a process alive")
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

#[cfg(target_os = "linux")]
fn assert_process_is_absent(raw_pid: u64) {
    let raw_pid = i32::try_from(raw_pid).unwrap();
    let pid = rustix::process::Pid::from_raw(raw_pid).unwrap();
    assert_eq!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancelled_supervision_closes_socket_and_kills_the_process_group() {
    let (_temporary, workspace) = TemporaryWorkspace::prepare();
    let script = r#"
sleep 60 &
descendant=$!
printf '{"pid":%s,"descendant":%s}\n' "$$" "$descendant"
wait
"#;
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/bin/sh",
        [OsString::from("-c"), OsString::from(script)],
    ))
    .await;
    let supervisor = tokio::spawn(supervise_registered_worker(
        registered,
        workspace,
        future::pending(),
    ));

    let report = next_worker_acp(&mut controller).await;
    let leader = report["pid"].as_u64().unwrap();
    let descendant = report["descendant"].as_u64().unwrap();
    supervisor.abort();
    assert!(supervisor.await.unwrap_err().is_cancelled());

    let closed = timeout(TEST_TIMEOUT, controller.next())
        .await
        .expect("controller lane remained open after supervisor cancellation");
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
    wait_until_process_is_absent(leader).await;
    wait_until_process_is_absent(descendant).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn controller_loss_kills_term_ignoring_leader_and_descendant() {
    let (_temporary, workspace) = TemporaryWorkspace::prepare();
    let script = r#"
trap '' TERM
sleep 60 &
descendant=$!
printf '{"pid":%s,"descendant":%s}\n' "$$" "$descendant"
wait
"#;
    let (registered, mut controller) = registered_worker_pair_with_command(command(
        "/bin/sh",
        [OsString::from("-c"), OsString::from(script)],
    ))
    .await;
    let supervisor = tokio::spawn(supervise_registered_worker(
        registered,
        workspace,
        future::pending(),
    ));

    let report = next_worker_acp(&mut controller).await;
    let leader = report["pid"].as_u64().unwrap();
    let descendant = report["descendant"].as_u64().unwrap();
    controller.close(None).await.unwrap();
    drop(controller);

    let result = timeout(Duration::from_secs(15), supervisor)
        .await
        .expect("TERM/KILL cleanup exceeded its fixed grace")
        .unwrap();
    assert!(matches!(
        result,
        Err(WorkerSupervisionError::Relay(
            openab_kubernetes_session::worker::relay::WorkerRelayError::ControllerClosed
                | openab_kubernetes_session::worker::relay::WorkerRelayError::WebSocketTransport
        ))
    ));
    assert_process_is_absent(leader);
    assert_process_is_absent(descendant);
}
