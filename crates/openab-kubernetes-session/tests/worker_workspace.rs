#![cfg(all(feature = "worker-runtime", unix))]

use openab_kubernetes_session::worker::workspace::{
    prepare_workspace, prepare_workspace_beneath, PreparedWorkspace, WorkspaceDirectory,
    WorkspaceIdentity, WorkspacePreparationError,
};
use std::fs::{self, File};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[test]
fn creates_only_the_fixed_private_directories_and_is_idempotent() {
    let tree = TestTree::with_root();

    let prepared = prepare(&tree);
    assert_eq!(prepared.home(), Path::new("/session/home"));
    assert_eq!(prepared.workspace(), Path::new("/session/workspace"));
    assert_real_directory(&tree.home());
    assert_real_directory(&tree.workspace());
    assert_eq!(mode(&tree.home()) & 0o777, 0o700);
    assert_eq!(mode(&tree.workspace()) & 0o777, 0o700);
    assert_eq!(entries(tree.root()), ["home", "workspace"]);

    let repeated = prepare(&tree);
    assert_eq!(repeated.home(), prepared.home());
    assert_eq!(repeated.workspace(), prepared.workspace());
    assert_eq!(entries(tree.root()), ["home", "workspace"]);
    assert!(prepared.revalidate_bindings().is_ok());

    let debug = format!("{prepared:?}");
    assert!(!debug.contains(tree.base().to_string_lossy().as_ref()));
}

#[test]
fn preserves_existing_private_directories_modes_and_contents() {
    let tree = TestTree::with_root();
    fs::create_dir(tree.home()).unwrap();
    fs::create_dir(tree.workspace()).unwrap();
    set_mode(&tree.home(), 0o2770);
    set_mode(&tree.workspace(), 0o2770);
    let home_marker = tree.home().join("retained-home");
    let workspace_marker = tree.workspace().join("retained-workspace");
    fs::write(&home_marker, b"home-state").unwrap();
    fs::write(&workspace_marker, b"workspace-state").unwrap();
    let home_mode = mode(&tree.home()) & 0o7777;
    let workspace_mode = mode(&tree.workspace()) & 0o7777;

    let prepared = prepare(&tree);

    assert_eq!(fs::read(home_marker).unwrap(), b"home-state");
    assert_eq!(fs::read(workspace_marker).unwrap(), b"workspace-state");
    assert_eq!(mode(&tree.home()) & 0o7777, home_mode);
    assert_eq!(mode(&tree.workspace()) & 0o7777, workspace_mode);
    assert!(prepared.revalidate_bindings().is_ok());
}

#[test]
fn rejects_missing_non_directory_and_symlinked_roots() {
    let missing = TestTree::without_root();
    assert_eq!(
        workspace_error(prepare_with_identity(
            &missing,
            WorkspaceIdentity::current()
        )),
        WorkspacePreparationError::RootUnavailable
    );

    let file = TestTree::without_root();
    fs::write(file.root(), b"not a directory").unwrap();
    assert_eq!(
        workspace_error(prepare_with_identity(&file, WorkspaceIdentity::current())),
        WorkspacePreparationError::RootNotDirectory
    );

    let linked = TestTree::without_root();
    let actual = linked.base().join("actual");
    fs::create_dir(&actual).unwrap();
    symlink(&actual, linked.root()).unwrap();
    assert_eq!(
        workspace_error(prepare_with_identity(&linked, WorkspaceIdentity::current())),
        WorkspacePreparationError::RootSymlink
    );
}

#[test]
fn rejects_symlink_escapes_for_each_private_directory() {
    for directory in [WorkspaceDirectory::Home, WorkspaceDirectory::Workspace] {
        let tree = TestTree::with_root();
        if directory == WorkspaceDirectory::Workspace {
            fs::create_dir(tree.home()).unwrap();
        }
        let outside = tree.base().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, tree.root().join(directory.name())).unwrap();

        assert_eq!(
            workspace_error(prepare_with_identity(&tree, WorkspaceIdentity::current())),
            WorkspacePreparationError::DirectorySymlink { directory }
        );
    }
}

#[test]
fn rejects_non_directories_for_each_private_directory() {
    for directory in [WorkspaceDirectory::Home, WorkspaceDirectory::Workspace] {
        let tree = TestTree::with_root();
        if directory == WorkspaceDirectory::Workspace {
            fs::create_dir(tree.home()).unwrap();
        }
        fs::write(tree.root().join(directory.name()), b"not a directory").unwrap();

        assert_eq!(
            workspace_error(prepare_with_identity(&tree, WorkspaceIdentity::current())),
            WorkspacePreparationError::DirectoryNotDirectory { directory }
        );
    }
}

#[test]
fn rejects_private_directories_owned_by_another_identity() {
    let tree = TestTree::with_root();
    let prepared = prepare(&tree);
    set_mode(tree.root(), 0o777);
    set_mode(&tree.home(), 0o777);
    set_mode(&tree.workspace(), 0o777);

    let current = WorkspaceIdentity::current();
    for foreign in [
        WorkspaceIdentity::from_raw(different(current.uid()), current.gid()),
        WorkspaceIdentity::from_raw(current.uid(), different(current.gid())),
    ] {
        assert_eq!(
            workspace_error(prepare_with_identity(&tree, foreign)),
            WorkspacePreparationError::DirectoryOwnership {
                directory: WorkspaceDirectory::Home,
            }
        );
    }
    drop(prepared);
}

#[test]
fn rejects_non_writable_root_home_and_workspace() {
    let root = TestTree::with_root();
    set_mode(root.root(), 0o500);
    assert_eq!(
        workspace_error(prepare_with_identity(&root, WorkspaceIdentity::current())),
        WorkspacePreparationError::RootNotWritable
    );

    for directory in [WorkspaceDirectory::Home, WorkspaceDirectory::Workspace] {
        let tree = TestTree::with_root();
        let prepared = prepare(&tree);
        let path = match directory {
            WorkspaceDirectory::Home => tree.home(),
            WorkspaceDirectory::Workspace => tree.workspace(),
        };
        set_mode(&path, 0o500);
        assert_eq!(
            workspace_error(prepare_with_identity(&tree, WorkspaceIdentity::current())),
            WorkspacePreparationError::DirectoryNotWritable { directory }
        );
        drop(prepared);
    }
}

#[test]
fn retained_capabilities_detect_private_directory_replacement() {
    for directory in [WorkspaceDirectory::Home, WorkspaceDirectory::Workspace] {
        let tree = TestTree::with_root();
        let prepared = prepare(&tree);
        let path = match directory {
            WorkspaceDirectory::Home => tree.home(),
            WorkspaceDirectory::Workspace => tree.workspace(),
        };
        let retained = tree.root().join(format!("retained-{}", directory.name()));
        let before = match directory {
            WorkspaceDirectory::Home => rustix::fs::fstat(prepared.home_fd()).unwrap(),
            WorkspaceDirectory::Workspace => rustix::fs::fstat(prepared.workspace_fd()).unwrap(),
        };
        fs::rename(&path, retained).unwrap();
        fs::create_dir(&path).unwrap();

        let after = match directory {
            WorkspaceDirectory::Home => rustix::fs::fstat(prepared.home_fd()).unwrap(),
            WorkspaceDirectory::Workspace => rustix::fs::fstat(prepared.workspace_fd()).unwrap(),
        };
        assert_eq!((after.st_dev, after.st_ino), (before.st_dev, before.st_ino));
        assert_eq!(
            prepared.revalidate_bindings().unwrap_err(),
            WorkspacePreparationError::DirectoryReplaced { directory }
        );
    }
}

#[test]
fn retained_capabilities_detect_root_path_replacement() {
    let tree = TestTree::with_root();
    let prepared = prepare(&tree);
    let retained = tree.base().join("retained-session");
    fs::rename(tree.root(), &retained).unwrap();
    fs::create_dir(tree.root()).unwrap();

    assert_eq!(
        prepared.revalidate_bindings().unwrap_err(),
        WorkspacePreparationError::RootReplaced
    );
    assert!(rustix::fs::fstat(prepared.workspace_fd()).is_ok());
}

#[test]
fn revalidation_rejects_same_inode_permission_drift() {
    let root = TestTree::with_root();
    let prepared = prepare(&root);
    set_mode(root.root(), 0o500);
    assert_eq!(
        prepared.revalidate_bindings().unwrap_err(),
        WorkspacePreparationError::RootNotWritable
    );

    for directory in [WorkspaceDirectory::Home, WorkspaceDirectory::Workspace] {
        let tree = TestTree::with_root();
        let prepared = prepare(&tree);
        let path = match directory {
            WorkspaceDirectory::Home => tree.home(),
            WorkspaceDirectory::Workspace => tree.workspace(),
        };
        set_mode(&path, 0o500);
        assert_eq!(
            prepared.revalidate_bindings().unwrap_err(),
            WorkspacePreparationError::DirectoryNotWritable { directory }
        );
    }
}

#[test]
fn errors_and_results_do_not_disclose_the_test_root() {
    let tree = TestTree::without_root();
    let error = workspace_error(prepare_with_identity(&tree, WorkspaceIdentity::current()));
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains(tree.base().to_string_lossy().as_ref()));

    let tree = TestTree::with_root();
    let prepared = prepare(&tree);
    let rendered = format!("{prepared:?}");
    assert!(!rendered.contains(tree.base().to_string_lossy().as_ref()));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn production_entrypoint_fails_closed_off_linux() {
    assert_eq!(
        workspace_error(prepare_workspace()),
        WorkspacePreparationError::UnsupportedRuntime
    );
}

fn prepare(tree: &TestTree) -> PreparedWorkspace {
    prepare_with_identity(tree, WorkspaceIdentity::current()).unwrap()
}

fn prepare_with_identity(
    tree: &TestTree,
    identity: WorkspaceIdentity,
) -> Result<PreparedWorkspace, WorkspacePreparationError> {
    prepare_workspace_beneath(tree.parent_fd(), identity)
}

fn workspace_error(
    result: Result<PreparedWorkspace, WorkspacePreparationError>,
) -> WorkspacePreparationError {
    match result {
        Ok(_) => panic!("workspace preparation unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn assert_real_directory(path: &Path) {
    let metadata = fs::symlink_metadata(path).unwrap();
    assert!(metadata.is_dir());
    assert!(!metadata.file_type().is_symlink());
}

fn entries(path: &Path) -> Vec<String> {
    let mut entries = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode()
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn different(value: u32) -> u32 {
    if value == u32::MAX {
        value - 1
    } else {
        value + 1
    }
}

struct TestTree {
    base: PathBuf,
    root: PathBuf,
}

impl TestTree {
    fn with_root() -> Self {
        let tree = Self::without_root();
        fs::create_dir(&tree.root).unwrap();
        tree
    }

    fn without_root() -> Self {
        let temp = fs::canonicalize(std::env::temp_dir()).unwrap();
        let base = temp.join(format!(
            "openab-worker-workspace-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        fs::create_dir(&base).unwrap();
        let root = base.join("session");
        Self { base, root }
    }

    fn base(&self) -> &Path {
        &self.base
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn home(&self) -> PathBuf {
        self.root.join("home")
    }

    fn workspace(&self) -> PathBuf {
        self.root.join("workspace")
    }

    fn parent_fd(&self) -> OwnedFd {
        File::open(&self.base).unwrap().into()
    }
}

impl Drop for TestTree {
    fn drop(&mut self) {
        restore_directories(&self.base);
        let _ = fs::remove_dir_all(&self.base);
    }
}

fn restore_directories(root: &Path) {
    let Ok(metadata) = fs::symlink_metadata(root) else {
        return;
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return;
    }
    let _ = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        restore_directories(&entry.path());
    }
}
