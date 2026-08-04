//! Private filesystem preparation for one Kubernetes session worker.
//!
//! Production always uses the fixed `/session` mount. Unix tests may replace
//! only its trusted parent directory capability; callers never provide a path.

use std::fmt;
use std::path::Path;

#[cfg(unix)]
use std::path::PathBuf;

use thiserror::Error;

use super::bootstrap::{HOME as HOME_PATH, WORKSPACE as WORKSPACE_PATH};

#[cfg(unix)]
use rustix::fd::{AsFd, BorrowedFd, OwnedFd};
#[cfg(unix)]
use rustix::fs::{AtFlags, FileType, Mode, OFlags, Stat};

#[cfg(unix)]
const ROOT_NAME: &str = "session";
const HOME_NAME: &str = "home";
const WORKSPACE_NAME: &str = "workspace";

/// One of the two worker-private directories beneath `/session`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceDirectory {
    Home,
    Workspace,
}

impl WorkspaceDirectory {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Home => HOME_NAME,
            Self::Workspace => WORKSPACE_NAME,
        }
    }
}

impl fmt::Display for WorkspaceDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Effective identity expected to own worker-private directories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceIdentity {
    uid: u32,
    gid: u32,
}

impl WorkspaceIdentity {
    #[cfg(unix)]
    pub fn current() -> Self {
        Self {
            uid: rustix::process::geteuid().as_raw(),
            gid: rustix::process::getegid().as_raw(),
        }
    }

    /// Construct an identity for deterministic contract tests.
    #[doc(hidden)]
    pub const fn from_raw(uid: u32, gid: u32) -> Self {
        Self { uid, gid }
    }

    pub const fn uid(self) -> u32 {
        self.uid
    }

    pub const fn gid(self) -> u32 {
        self.gid
    }
}

/// Closed, path-redacted failures for private workspace preparation.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkspacePreparationError {
    #[error("the Kubernetes session worker requires Linux")]
    UnsupportedRuntime,
    #[error("the private session root is unavailable")]
    RootUnavailable,
    #[error("the private session root must not be a symbolic link")]
    RootSymlink,
    #[error("the private session root is not a directory")]
    RootNotDirectory,
    #[error("the private session root is not writable")]
    RootNotWritable,
    #[error("the private session root was replaced")]
    RootReplaced,
    #[error("the private {directory} directory is unavailable")]
    DirectoryUnavailable { directory: WorkspaceDirectory },
    #[error("the private {directory} directory must not be a symbolic link")]
    DirectorySymlink { directory: WorkspaceDirectory },
    #[error("the private {directory} path is not a directory")]
    DirectoryNotDirectory { directory: WorkspaceDirectory },
    #[error("the private {directory} directory has an unexpected owner")]
    DirectoryOwnership { directory: WorkspaceDirectory },
    #[error("the private {directory} directory is not writable")]
    DirectoryNotWritable { directory: WorkspaceDirectory },
    #[error("the private {directory} directory was replaced")]
    DirectoryReplaced { directory: WorkspaceDirectory },
}

/// Retained capabilities for the worker-private filesystem.
///
/// The custom `Debug` intentionally reveals neither host paths nor file
/// descriptor numbers.
pub struct PreparedWorkspace {
    #[cfg(unix)]
    parent: OwnedFd,
    #[cfg(unix)]
    root: OwnedFd,
    #[cfg(unix)]
    home: OwnedFd,
    #[cfg(unix)]
    workspace: OwnedFd,
    #[cfg(unix)]
    identity: WorkspaceIdentity,
    _private: (),
}

impl fmt::Debug for PreparedWorkspace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedWorkspace")
            .field("home", &HOME_PATH)
            .field("workspace", &WORKSPACE_PATH)
            .finish_non_exhaustive()
    }
}

impl PreparedWorkspace {
    pub fn home(&self) -> &Path {
        Path::new(HOME_PATH)
    }

    pub fn workspace(&self) -> &Path {
        Path::new(WORKSPACE_PATH)
    }

    #[cfg(unix)]
    pub fn home_fd(&self) -> BorrowedFd<'_> {
        self.home.as_fd()
    }

    #[cfg(unix)]
    pub fn workspace_fd(&self) -> BorrowedFd<'_> {
        self.workspace.as_fd()
    }

    /// Confirm that every fixed pathname still names its retained capability.
    #[cfg(unix)]
    pub fn revalidate_bindings(&self) -> Result<(), WorkspacePreparationError> {
        let reopened_root = open_directory(&self.parent, ROOT_NAME)
            .map_err(|_| WorkspacePreparationError::RootReplaced)?;
        require_same_identity(&reopened_root, &self.root)
            .map_err(|_| WorkspacePreparationError::RootReplaced)?;
        validate_root_capability(&reopened_root, self.identity)?;

        for (directory, retained) in [
            (WorkspaceDirectory::Home, &self.home),
            (WorkspaceDirectory::Workspace, &self.workspace),
        ] {
            let reopened = open_directory(&reopened_root, directory.name())
                .map_err(|_| WorkspacePreparationError::DirectoryReplaced { directory })?;
            require_same_identity(&reopened, retained)
                .map_err(|_| WorkspacePreparationError::DirectoryReplaced { directory })?;
            validate_private_capability(&reopened, directory, self.identity)?;
        }

        Ok(())
    }
}

/// Prepare the fixed `/session` mount used by production Linux workers.
#[cfg(target_os = "linux")]
pub fn prepare_workspace() -> Result<PreparedWorkspace, WorkspacePreparationError> {
    let parent = rustix::fs::open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| WorkspacePreparationError::RootUnavailable)?;
    prepare_workspace_beneath(parent, WorkspaceIdentity::current())
}

/// Fail closed before touching a filesystem on unsupported worker platforms.
#[cfg(not(target_os = "linux"))]
pub fn prepare_workspace() -> Result<PreparedWorkspace, WorkspacePreparationError> {
    Err(WorkspacePreparationError::UnsupportedRuntime)
}

/// Prepare the fixed `session` child beneath a trusted parent capability.
///
/// This is public only so the production contract can be exercised on Unix
/// development hosts without granting tests access to `/session`.
#[cfg(unix)]
#[doc(hidden)]
pub fn prepare_workspace_beneath(
    parent: OwnedFd,
    identity: WorkspaceIdentity,
) -> Result<PreparedWorkspace, WorkspacePreparationError> {
    let root = prepare_root(&parent, identity)?;
    let home = prepare_private_directory(&root, WorkspaceDirectory::Home, identity)?;
    let workspace = prepare_private_directory(&root, WorkspaceDirectory::Workspace, identity)?;

    let prepared = PreparedWorkspace {
        parent,
        root,
        home,
        workspace,
        identity,
        _private: (),
    };
    prepared.revalidate_bindings()?;
    Ok(prepared)
}

#[cfg(unix)]
fn prepare_root(
    parent: &OwnedFd,
    identity: WorkspaceIdentity,
) -> Result<OwnedFd, WorkspacePreparationError> {
    let inspected = rustix::fs::statat(parent, ROOT_NAME, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| WorkspacePreparationError::RootUnavailable)?;
    match FileType::from_raw_mode(inspected.st_mode) {
        FileType::Symlink => return Err(WorkspacePreparationError::RootSymlink),
        FileType::Directory => {}
        _ => return Err(WorkspacePreparationError::RootNotDirectory),
    }

    let root = open_directory(parent, ROOT_NAME)
        .map_err(|_| classify_root(parent).unwrap_or(WorkspacePreparationError::RootUnavailable))?;
    validate_root_capability(&root, identity)?;
    Ok(root)
}

#[cfg(unix)]
fn validate_root_capability(
    root: &OwnedFd,
    identity: WorkspaceIdentity,
) -> Result<(), WorkspacePreparationError> {
    let stat = rustix::fs::fstat(root).map_err(|_| WorkspacePreparationError::RootUnavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        return Err(WorkspacePreparationError::RootNotDirectory);
    }
    if !is_writable_directory(&stat, identity) || probe_writable(root).is_err() {
        return Err(WorkspacePreparationError::RootNotWritable);
    }
    Ok(())
}

#[cfg(unix)]
fn classify_root(parent: &OwnedFd) -> Option<WorkspacePreparationError> {
    match rustix::fs::statat(parent, ROOT_NAME, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Some(match FileType::from_raw_mode(stat.st_mode) {
            FileType::Symlink => WorkspacePreparationError::RootSymlink,
            FileType::Directory => return None,
            _ => WorkspacePreparationError::RootNotDirectory,
        }),
        Err(_) => Some(WorkspacePreparationError::RootUnavailable),
    }
}

#[cfg(unix)]
fn prepare_private_directory(
    root: &OwnedFd,
    directory: WorkspaceDirectory,
    identity: WorkspaceIdentity,
) -> Result<OwnedFd, WorkspacePreparationError> {
    let name = directory.name();
    let created = match inspect_private_directory(root, directory)? {
        Some(()) => false,
        None => match rustix::fs::mkdirat(root, name, Mode::RWXU) {
            Ok(()) => true,
            Err(error) if error == rustix::io::Errno::EXIST => false,
            Err(_) => {
                return Err(WorkspacePreparationError::DirectoryUnavailable { directory });
            }
        },
    };

    inspect_private_directory(root, directory)?
        .ok_or(WorkspacePreparationError::DirectoryUnavailable { directory })?;
    let capability = open_directory(root, name).map_err(|_| {
        classify_private_directory(root, directory)
            .unwrap_or(WorkspacePreparationError::DirectoryUnavailable { directory })
    })?;
    if created {
        rustix::fs::fchmod(&capability, Mode::RWXU)
            .map_err(|_| WorkspacePreparationError::DirectoryNotWritable { directory })?;
    }
    validate_private_capability(&capability, directory, identity)?;
    Ok(capability)
}

#[cfg(unix)]
fn validate_private_capability(
    capability: &OwnedFd,
    directory: WorkspaceDirectory,
    identity: WorkspaceIdentity,
) -> Result<(), WorkspacePreparationError> {
    let stat = rustix::fs::fstat(capability)
        .map_err(|_| WorkspacePreparationError::DirectoryUnavailable { directory })?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        return Err(WorkspacePreparationError::DirectoryNotDirectory { directory });
    }
    if stat.st_uid != identity.uid || stat.st_gid != identity.gid {
        return Err(WorkspacePreparationError::DirectoryOwnership { directory });
    }
    if !is_writable_directory(&stat, identity) || probe_writable(capability).is_err() {
        return Err(WorkspacePreparationError::DirectoryNotWritable { directory });
    }
    Ok(())
}

#[cfg(unix)]
fn inspect_private_directory(
    root: &OwnedFd,
    directory: WorkspaceDirectory,
) -> Result<Option<()>, WorkspacePreparationError> {
    match rustix::fs::statat(root, directory.name(), AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => match FileType::from_raw_mode(stat.st_mode) {
            FileType::Symlink => Err(WorkspacePreparationError::DirectorySymlink { directory }),
            FileType::Directory => Ok(Some(())),
            _ => Err(WorkspacePreparationError::DirectoryNotDirectory { directory }),
        },
        Err(error) if error == rustix::io::Errno::NOENT => Ok(None),
        Err(_) => Err(WorkspacePreparationError::DirectoryUnavailable { directory }),
    }
}

#[cfg(unix)]
fn classify_private_directory(
    root: &OwnedFd,
    directory: WorkspaceDirectory,
) -> Option<WorkspacePreparationError> {
    inspect_private_directory(root, directory).err()
}

#[cfg(unix)]
fn open_directory(parent: impl AsFd, name: &str) -> rustix::io::Result<OwnedFd> {
    rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
}

#[cfg(unix)]
fn require_same_identity(left: &OwnedFd, right: &OwnedFd) -> Result<(), ()> {
    let left = rustix::fs::fstat(left).map_err(|_| ())?;
    let right = rustix::fs::fstat(right).map_err(|_| ())?;
    if left.st_dev == right.st_dev && left.st_ino == right.st_ino {
        Ok(())
    } else {
        Err(())
    }
}

#[cfg(unix)]
fn is_writable_directory(stat: &Stat, identity: WorkspaceIdentity) -> bool {
    let required = if stat.st_uid == identity.uid {
        Mode::WUSR | Mode::XUSR
    } else if stat.st_gid == identity.gid {
        Mode::WGRP | Mode::XGRP
    } else {
        Mode::WOTH | Mode::XOTH
    };
    Mode::from_bits_retain(stat.st_mode).contains(required)
}

#[cfg(unix)]
fn probe_writable(directory: impl AsFd) -> Result<(), ()> {
    let probe_name = PathBuf::from(format!(".openab-write-probe-{}", uuid::Uuid::new_v4()));
    let probe = rustix::fs::openat(
        &directory,
        &probe_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| ())?;
    drop(probe);
    rustix::fs::unlinkat(directory, &probe_name, AtFlags::empty()).map_err(|_| ())
}
