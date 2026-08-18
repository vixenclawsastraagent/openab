use crate::client_transport::{
    validate_client_ca_pem, validate_worker_controller_url, MAX_CLIENT_CA_PEM_BYTES,
};
use crate::wire::{decode_frame, WorkerRegistrationV1};
use std::env;
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

pub const MAX_WORKER_REGISTRATION_BINDING_BYTES: usize = 4 * 1024;
pub const WORKER_REGISTRATION_TOKEN_BYTES: usize = 32;

pub const SESSION_CONTROLLER_URL_ENV: &str = "OPENAB_SESSION_CONTROLLER_URL";
pub const SESSION_CONTROLLER_CA_FILE_ENV: &str = "OPENAB_SESSION_CONTROLLER_CA_FILE";
pub const REGISTRATION_TOKEN_FILE_ENV: &str = "OPENAB_REGISTRATION_TOKEN_FILE";
pub const REGISTRATION_BINDING_FILE_ENV: &str = "OPENAB_REGISTRATION_BINDING_FILE";
pub const WORKER_POD_UID_ENV: &str = "OPENAB_WORKER_POD_UID";
pub const SESSION_ROOT_ENV: &str = "OPENAB_SESSION_ROOT";
pub const WORKSPACE_ENV: &str = "OPENAB_WORKSPACE";
pub const HOME_ENV: &str = "HOME";

/// Variables captured from the controller-owned Pod contract, in read order.
pub const WORKER_BOOTSTRAP_ENV_NAMES: [&str; 8] = [
    SESSION_CONTROLLER_URL_ENV,
    SESSION_CONTROLLER_CA_FILE_ENV,
    REGISTRATION_TOKEN_FILE_ENV,
    REGISTRATION_BINDING_FILE_ENV,
    WORKER_POD_UID_ENV,
    SESSION_ROOT_ENV,
    WORKSPACE_ENV,
    HOME_ENV,
];

/// Transport and bootstrap variables that must not reach the ACP child.
pub const WORKER_CHILD_SCRUB_ENV_NAMES: [&str; 5] = [
    SESSION_CONTROLLER_URL_ENV,
    SESSION_CONTROLLER_CA_FILE_ENV,
    REGISTRATION_TOKEN_FILE_ENV,
    REGISTRATION_BINDING_FILE_ENV,
    WORKER_POD_UID_ENV,
];

const CONTROLLER_CA_FILE: &str = "/var/run/openab-controller-ca/ca.crt";
const REGISTRATION_TOKEN_FILE: &str = "/var/run/openab-registration/token";
const REGISTRATION_BINDING_FILE: &str = "/var/run/openab-registration/binding.json";
pub(crate) const SESSION_ROOT: &str = "/session";
pub(crate) const WORKSPACE: &str = "/session/workspace";
pub(crate) const HOME: &str = "/session/home";

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerCommandError {
    #[error("worker subcommand is missing")]
    MissingSubcommand,
    #[error("worker subcommand is unknown")]
    UnknownSubcommand,
    #[error("worker child separator is missing")]
    MissingSeparator,
    #[error("worker child separator must be exactly --")]
    InvalidSeparator,
    #[error("worker ACP executable is missing")]
    MissingExecutable,
    #[error("worker ACP executable must be an absolute Linux path")]
    RelativeExecutable,
}

/// Literal child command retained without touching the executable.
pub struct WorkerCommand {
    executable: PathBuf,
    arguments: Vec<OsString>,
}

impl WorkerCommand {
    pub fn parse<I>(args: I) -> Result<Self, WorkerCommandError>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut args = args.into_iter();
        let subcommand = args.next().ok_or(WorkerCommandError::MissingSubcommand)?;
        if subcommand.to_str() != Some("serve") {
            return Err(WorkerCommandError::UnknownSubcommand);
        }
        let separator = args.next().ok_or(WorkerCommandError::MissingSeparator)?;
        if separator.to_str() != Some("--") {
            return Err(WorkerCommandError::InvalidSeparator);
        }
        let executable = PathBuf::from(args.next().ok_or(WorkerCommandError::MissingExecutable)?);
        if !is_literal_linux_absolute(&executable) {
            return Err(WorkerCommandError::RelativeExecutable);
        }
        Ok(Self {
            executable,
            arguments: args.collect(),
        })
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

impl fmt::Debug for WorkerCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerCommand")
            .field("executable", &"<redacted>")
            .field("arguments", &"<redacted>")
            .finish()
    }
}

fn is_literal_linux_absolute(path: &Path) -> bool {
    let value = path.as_os_str().to_string_lossy();
    !value.is_empty() && value.starts_with('/') && !value.as_bytes().contains(&0)
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerEnvironmentError {
    #[error("required worker environment variable {name} is unavailable")]
    Unavailable { name: &'static str },
    #[error("worker environment variable {name} is invalid")]
    Invalid { name: &'static str },
}

/// Captured controller-owned launch contract. Values are deliberately absent
/// from Debug output and only the eight known variables are inspected.
pub struct WorkerBootstrapEnvironment {
    controller_url: String,
    controller_ca_file: PathBuf,
    registration_token_file: PathBuf,
    registration_binding_file: PathBuf,
    pod_uid: String,
}

impl WorkerBootstrapEnvironment {
    pub fn from_environment() -> Result<Self, WorkerEnvironmentError> {
        Self::from_lookup(env::var_os)
    }

    pub fn from_lookup<F>(mut lookup: F) -> Result<Self, WorkerEnvironmentError>
    where
        F: FnMut(&'static str) -> Option<OsString>,
    {
        let controller_url = required_string(&mut lookup, SESSION_CONTROLLER_URL_ENV)?;
        let controller_ca_file = required_string(&mut lookup, SESSION_CONTROLLER_CA_FILE_ENV)?;
        let registration_token_file = required_string(&mut lookup, REGISTRATION_TOKEN_FILE_ENV)?;
        let registration_binding_file =
            required_string(&mut lookup, REGISTRATION_BINDING_FILE_ENV)?;
        let pod_uid = required_string(&mut lookup, WORKER_POD_UID_ENV)?;
        let session_root = required_string(&mut lookup, SESSION_ROOT_ENV)?;
        let workspace = required_string(&mut lookup, WORKSPACE_ENV)?;
        let home = required_string(&mut lookup, HOME_ENV)?;

        if validate_worker_controller_url(&controller_url).is_err() {
            return Err(WorkerEnvironmentError::Invalid {
                name: SESSION_CONTROLLER_URL_ENV,
            });
        }
        for (name, actual, expected) in [
            (
                SESSION_CONTROLLER_CA_FILE_ENV,
                controller_ca_file.as_str(),
                CONTROLLER_CA_FILE,
            ),
            (
                REGISTRATION_TOKEN_FILE_ENV,
                registration_token_file.as_str(),
                REGISTRATION_TOKEN_FILE,
            ),
            (
                REGISTRATION_BINDING_FILE_ENV,
                registration_binding_file.as_str(),
                REGISTRATION_BINDING_FILE,
            ),
            (SESSION_ROOT_ENV, session_root.as_str(), SESSION_ROOT),
            (WORKSPACE_ENV, workspace.as_str(), WORKSPACE),
            (HOME_ENV, home.as_str(), HOME),
        ] {
            if actual != expected {
                return Err(WorkerEnvironmentError::Invalid { name });
            }
        }
        if !crate::state::is_valid_observation_identifier(&pod_uid) {
            return Err(WorkerEnvironmentError::Invalid {
                name: WORKER_POD_UID_ENV,
            });
        }

        Ok(Self {
            controller_url,
            controller_ca_file: controller_ca_file.into(),
            registration_token_file: registration_token_file.into(),
            registration_binding_file: registration_binding_file.into(),
            pod_uid,
        })
    }

    pub fn controller_url(&self) -> &str {
        &self.controller_url
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

impl fmt::Debug for WorkerBootstrapEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerBootstrapEnvironment")
            .field("controller_url", &"<redacted>")
            .field("controller_ca_file", &"<redacted>")
            .field("registration_token_file", &"<redacted>")
            .field("registration_binding_file", &"<redacted>")
            .field("pod_uid", &"<redacted>")
            .finish()
    }
}

fn required_string<F>(lookup: &mut F, name: &'static str) -> Result<String, WorkerEnvironmentError>
where
    F: FnMut(&'static str) -> Option<OsString>,
{
    lookup(name)
        .ok_or(WorkerEnvironmentError::Unavailable { name })?
        .into_string()
        .map_err(|_| WorkerEnvironmentError::Invalid { name })
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum WorkerBootstrapError {
    #[error("worker registration token file could not be opened")]
    OpenRegistrationToken,
    #[error("worker registration binding file could not be opened")]
    OpenRegistrationBinding,
    #[error("worker controller CA file could not be opened")]
    OpenControllerCa,
    #[error("worker registration token could not be read")]
    RegistrationTokenRead,
    #[error("worker registration token must contain exactly 32 raw bytes")]
    InvalidRegistrationTokenLength,
    #[error("worker registration binding could not be read")]
    RegistrationBindingRead,
    #[error("worker registration binding exceeds its size limit")]
    RegistrationBindingTooLarge,
    #[error("worker registration binding is invalid")]
    InvalidRegistrationBinding,
    #[error("worker controller CA bundle could not be read")]
    ControllerCaRead,
    #[error("worker controller CA bundle exceeds its size limit")]
    ControllerCaTooLarge,
    #[error("worker controller CA bundle is invalid")]
    InvalidControllerCa,
}

/// Fully validated bootstrap material loaded before network or child access.
pub struct WorkerBootstrap {
    command: WorkerCommand,
    controller_url: String,
    controller_ca_pem: Vec<u8>,
    registration_token: RegistrationToken,
    registration: WorkerRegistrationV1,
    pod_uid: String,
}

impl WorkerBootstrap {
    pub fn load_from_files(
        command: WorkerCommand,
        environment: WorkerBootstrapEnvironment,
    ) -> Result<Self, WorkerBootstrapError> {
        let registration_token = read_registration_token(
            File::open(&environment.registration_token_file)
                .map_err(|_| WorkerBootstrapError::OpenRegistrationToken)?,
        )?;
        let registration = read_registration_binding(
            File::open(&environment.registration_binding_file)
                .map_err(|_| WorkerBootstrapError::OpenRegistrationBinding)?,
        )?;
        let controller_ca_pem = read_controller_ca(
            File::open(&environment.controller_ca_file)
                .map_err(|_| WorkerBootstrapError::OpenControllerCa)?,
        )?;
        Ok(Self::from_parts(
            command,
            environment,
            registration_token,
            registration,
            controller_ca_pem,
        ))
    }

    pub fn load_from_readers<T, B, C>(
        command: WorkerCommand,
        environment: WorkerBootstrapEnvironment,
        token: T,
        registration: B,
        ca: C,
    ) -> Result<Self, WorkerBootstrapError>
    where
        T: Read,
        B: Read,
        C: Read,
    {
        let registration_token = read_registration_token(token)?;
        let registration = read_registration_binding(registration)?;
        let controller_ca_pem = read_controller_ca(ca)?;
        Ok(Self::from_parts(
            command,
            environment,
            registration_token,
            registration,
            controller_ca_pem,
        ))
    }

    fn from_parts(
        command: WorkerCommand,
        environment: WorkerBootstrapEnvironment,
        registration_token: RegistrationToken,
        registration: WorkerRegistrationV1,
        controller_ca_pem: Vec<u8>,
    ) -> Self {
        Self {
            command,
            controller_url: environment.controller_url,
            controller_ca_pem,
            registration_token,
            registration,
            pod_uid: environment.pod_uid,
        }
    }

    pub fn controller_url(&self) -> &str {
        &self.controller_url
    }

    pub fn controller_ca_pem(&self) -> &[u8] {
        &self.controller_ca_pem
    }

    pub fn registration(&self) -> &WorkerRegistrationV1 {
        &self.registration
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    /// Consume the bootstrap while constructing the only authorized request
    /// that may observe the raw one-shot credential. The credential is erased
    /// before any request bytes can be written to the network.
    pub(super) fn into_registration_request<R, E>(
        mut self,
        encode: impl FnOnce(&[u8; WORKER_REGISTRATION_TOKEN_BYTES], &str) -> Result<R, E>,
    ) -> Result<(R, WorkerRegistrationV1, WorkerCommand), E> {
        let request = encode(&self.registration_token.0, &self.pod_uid)?;
        self.registration_token.0.zeroize();
        let Self {
            command,
            registration,
            ..
        } = self;
        Ok((request, registration, command))
    }
}

impl fmt::Debug for WorkerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerBootstrap")
            .field("command", &"<redacted>")
            .field("controller_url", &"<redacted>")
            .field("controller_ca_pem", &"<redacted>")
            .field("registration_token", &self.registration_token)
            .field("registration", &"<redacted>")
            .field("pod_uid", &"<redacted>")
            .finish()
    }
}

struct RegistrationToken(Zeroizing<[u8; WORKER_REGISTRATION_TOKEN_BYTES]>);

impl fmt::Debug for RegistrationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn read_registration_token<R>(reader: R) -> Result<RegistrationToken, WorkerBootstrapError>
where
    R: Read,
{
    let mut reader = reader.take((WORKER_REGISTRATION_TOKEN_BYTES + 1) as u64);
    let mut bytes = Zeroizing::new([0_u8; WORKER_REGISTRATION_TOKEN_BYTES + 1]);
    let mut length = 0;
    while length < bytes.len() {
        match reader.read(&mut bytes[length..]) {
            Ok(0) => break,
            Ok(read) => length += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err(WorkerBootstrapError::RegistrationTokenRead),
        }
    }
    if length != WORKER_REGISTRATION_TOKEN_BYTES {
        return Err(WorkerBootstrapError::InvalidRegistrationTokenLength);
    }
    let mut token = Zeroizing::new([0_u8; WORKER_REGISTRATION_TOKEN_BYTES]);
    token.copy_from_slice(&bytes[..WORKER_REGISTRATION_TOKEN_BYTES]);
    Ok(RegistrationToken(token))
}

fn read_registration_binding<R>(reader: R) -> Result<WorkerRegistrationV1, WorkerBootstrapError>
where
    R: Read,
{
    let mut bytes = Vec::new();
    reader
        .take((MAX_WORKER_REGISTRATION_BINDING_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| WorkerBootstrapError::RegistrationBindingRead)?;
    if bytes.len() > MAX_WORKER_REGISTRATION_BINDING_BYTES {
        return Err(WorkerBootstrapError::RegistrationBindingTooLarge);
    }
    decode_frame(&bytes).map_err(|_| WorkerBootstrapError::InvalidRegistrationBinding)
}

fn read_controller_ca<R>(reader: R) -> Result<Vec<u8>, WorkerBootstrapError>
where
    R: Read,
{
    let mut bytes = Vec::new();
    reader
        .take((MAX_CLIENT_CA_PEM_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| WorkerBootstrapError::ControllerCaRead)?;
    if bytes.len() > MAX_CLIENT_CA_PEM_BYTES {
        return Err(WorkerBootstrapError::ControllerCaTooLarge);
    }
    validate_client_ca_pem(&bytes).map_err(|_| WorkerBootstrapError::InvalidControllerCa)?;
    Ok(bytes)
}
