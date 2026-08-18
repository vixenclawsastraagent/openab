use openab_kubernetes_session::bridge::runtime::{BridgeEnvironment, BridgeEnvironmentError};
use openab_kubernetes_session::bridge::websocket::{
    bridge_websocket_config, run_bridge_websocket, BridgeWebSocketError,
};
use openab_kubernetes_session::bridge::{
    is_valid_controller_bearer_credential, MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES,
    MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES,
};
use openab_kubernetes_session::client_transport::{
    build_client_request, client_connector, ClientEndpoint, ClientRequestError, PrivateCaError,
};
use std::ffi::OsString;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
struct BridgeCommand {
    controller_url: String,
    profile: String,
    scope: String,
    credential_file: PathBuf,
    controller_ca_file: Option<PathBuf>,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum CommandError {
    #[error("expected the bridge subcommand")]
    ExpectedBridgeSubcommand,
    #[error("command option is unknown")]
    UnknownOption,
    #[error("command option is missing a value")]
    MissingOptionValue,
    #[error("command option was specified more than once")]
    DuplicateOption,
    #[error("a required command option is missing")]
    MissingRequiredOption,
    #[error("command option value must be valid UTF-8")]
    InvalidOptionEncoding,
    #[error("command option value must not be empty")]
    EmptyOptionValue,
    #[error("controller credential file must be an absolute path")]
    RelativeCredentialFile,
    #[error("controller CA file must be an absolute path")]
    RelativeControllerCaFile,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum CredentialError {
    #[error("controller bearer credential could not be read")]
    Read,
    #[error("controller bearer credential is shorter than the required security floor")]
    TooShort,
    #[error("controller bearer credential exceeds its size limit")]
    TooLarge,
    #[error("controller bearer credential contains a disallowed byte")]
    InvalidByte,
}

#[derive(Debug, Error)]
enum ApplicationError {
    #[error(transparent)]
    Command(#[from] CommandError),
    #[error("controller bearer credential file could not be opened")]
    OpenCredentialFile(#[source] std::io::Error),
    #[error("controller CA file could not be opened")]
    OpenControllerCaFile,
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    ControllerCa(#[from] PrivateCaError),
    #[error("broker-owned bridge environment is invalid")]
    Environment(#[source] BridgeEnvironmentError),
    #[error(transparent)]
    Request(#[from] ClientRequestError),
    #[error("controller WebSocket connection timed out")]
    ConnectionTimedOut,
    #[error("controller WebSocket connection failed")]
    Connection(#[source] tungstenite::Error),
    #[error("bridge relay terminated with an error")]
    Relay(#[source] BridgeWebSocketError),
}

fn parse_args<I>(args: I) -> Result<BridgeCommand, CommandError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let subcommand = args.next().ok_or(CommandError::ExpectedBridgeSubcommand)?;
    if subcommand.to_str() != Some("bridge") {
        return Err(CommandError::ExpectedBridgeSubcommand);
    }

    let mut controller_url = None;
    let mut profile = None;
    let mut scope = None;
    let mut credential_file = None;
    let mut controller_ca_file = None;

    while let Some(option) = args.next() {
        let option = option.to_str().ok_or(CommandError::UnknownOption)?;
        let value = args.next().ok_or(CommandError::MissingOptionValue)?;
        if value.to_str().is_some_and(|value| value.starts_with("--")) {
            return Err(CommandError::MissingOptionValue);
        }

        match option {
            "--controller-url" => {
                set_once(&mut controller_url, string_option(value)?)?;
            }
            "--profile" => {
                set_once(&mut profile, string_option(value)?)?;
            }
            "--scope" => {
                set_once(&mut scope, string_option(value)?)?;
            }
            "--credential-file" => {
                if value.is_empty() {
                    return Err(CommandError::EmptyOptionValue);
                }
                let value = PathBuf::from(value);
                if !value.is_absolute() {
                    return Err(CommandError::RelativeCredentialFile);
                }
                set_once(&mut credential_file, value)?;
            }
            "--controller-ca-file" => {
                if value.is_empty() {
                    return Err(CommandError::EmptyOptionValue);
                }
                let value = PathBuf::from(value);
                if !value.is_absolute() {
                    return Err(CommandError::RelativeControllerCaFile);
                }
                set_once(&mut controller_ca_file, value)?;
            }
            _ => return Err(CommandError::UnknownOption),
        }
    }

    Ok(BridgeCommand {
        controller_url: controller_url.ok_or(CommandError::MissingRequiredOption)?,
        profile: profile.ok_or(CommandError::MissingRequiredOption)?,
        scope: scope.ok_or(CommandError::MissingRequiredOption)?,
        credential_file: credential_file.ok_or(CommandError::MissingRequiredOption)?,
        controller_ca_file,
    })
}

fn string_option(value: OsString) -> Result<String, CommandError> {
    let value = value
        .into_string()
        .map_err(|_| CommandError::InvalidOptionEncoding)?;
    if value.is_empty() {
        return Err(CommandError::EmptyOptionValue);
    }
    Ok(value)
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), CommandError> {
    if slot.replace(value).is_some() {
        return Err(CommandError::DuplicateOption);
    }
    Ok(())
}

fn read_bearer_credential<R>(reader: &mut R) -> Result<Vec<u8>, CredentialError>
where
    R: Read,
{
    let mut credential = Vec::new();
    if reader
        .take((MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut credential)
        .is_err()
    {
        credential.fill(0);
        return Err(CredentialError::Read);
    }

    if credential.len() < MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES {
        credential.fill(0);
        return Err(CredentialError::TooShort);
    }
    if credential.len() > MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES {
        credential.fill(0);
        return Err(CredentialError::TooLarge);
    }
    if !is_valid_controller_bearer_credential(&credential) {
        credential.fill(0);
        return Err(CredentialError::InvalidByte);
    }
    Ok(credential)
}

async fn run<I>(args: I) -> Result<(), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
{
    let command = parse_args(args)?;
    let environment = BridgeEnvironment::from_environment(&command.scope, &command.profile)
        .map_err(ApplicationError::Environment)?;

    let mut controller_ca_file = if let Some(path) = &command.controller_ca_file {
        Some(File::open(path).map_err(|_| ApplicationError::OpenControllerCaFile)?)
    } else {
        None
    };
    let connector = client_connector(
        controller_ca_file
            .as_mut()
            .map(|file| file as &mut dyn Read),
    )?;
    drop(controller_ca_file);

    let mut credential_file =
        File::open(&command.credential_file).map_err(ApplicationError::OpenCredentialFile)?;
    let mut credential = read_bearer_credential(&mut credential_file)?;
    drop(credential_file);
    let request = build_client_request(
        ClientEndpoint::Bridge {
            bearer: &credential,
        },
        &command.controller_url,
    );
    credential.fill(0);
    let request = request?;

    let connection =
        connect_async_tls_with_config(request, Some(bridge_websocket_config()), false, connector);
    let (socket, _) = timeout(CONNECT_TIMEOUT, connection)
        .await
        .map_err(|_| ApplicationError::ConnectionTimedOut)?
        .map_err(ApplicationError::Connection)?;

    let (identity, mapping_expectation) = environment.into_parts();
    run_bridge_websocket(
        socket,
        tokio::io::stdin(),
        tokio::io::stdout(),
        identity,
        mapping_expectation,
        STARTUP_TIMEOUT,
        WRITE_TIMEOUT,
    )
    .await
    .map_err(ApplicationError::Relay)?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run(std::env::args_os().skip(1)).await {
        eprintln!("openab-kubernetes-session: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor};

    const TEST_CREDENTIAL: &[u8] = b"abc_DEF-123.~+/abc_DEF-123.~+/==";

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_bridge_options_in_any_order() {
        let command = parse_args(args(&[
            "bridge",
            "--scope",
            "team-a",
            "--controller-url",
            "wss://controller.example.test/v1/bridge",
            "--credential-file",
            "/run/secrets/controller-token",
            "--controller-ca-file",
            "/run/config/controller-ca.pem",
            "--profile",
            "isolated",
        ]))
        .expect("valid command");

        assert_eq!(
            command,
            BridgeCommand {
                controller_url: "wss://controller.example.test/v1/bridge".to_string(),
                profile: "isolated".to_string(),
                scope: "team-a".to_string(),
                credential_file: PathBuf::from("/run/secrets/controller-token"),
                controller_ca_file: Some(PathBuf::from("/run/config/controller-ca.pem")),
            }
        );
    }

    #[test]
    fn leaves_the_controller_ca_unset_by_default() {
        let command = parse_args(args(&[
            "bridge",
            "--controller-url",
            "wss://controller.example.test/v1/bridge",
            "--profile",
            "isolated",
            "--scope",
            "team-a",
            "--credential-file",
            "/run/secrets/controller-token",
        ]))
        .expect("valid command");

        assert_eq!(command.controller_ca_file, None);
    }

    #[test]
    fn rejects_missing_duplicate_and_unknown_options_without_echoing_input() {
        assert_eq!(
            parse_args(args(&["bridge", "--profile"])),
            Err(CommandError::MissingOptionValue)
        );
        assert_eq!(
            parse_args(args(&["bridge", "--profile", "one", "--profile", "two"])),
            Err(CommandError::DuplicateOption)
        );
        let error = parse_args(args(&["bridge", "--secret-option", "do-not-echo"]))
            .expect_err("unknown option");
        assert_eq!(error, CommandError::UnknownOption);
        assert!(!error.to_string().contains("do-not-echo"));
        assert_eq!(
            parse_args(args(&["bridge", "--profile", "--scope", "team-a"])),
            Err(CommandError::MissingOptionValue)
        );
    }

    #[test]
    fn rejects_missing_subcommand_and_empty_or_missing_required_values() {
        assert_eq!(
            parse_args(args(&["not-bridge"])),
            Err(CommandError::ExpectedBridgeSubcommand)
        );
        assert_eq!(
            parse_args(args(&[
                "bridge",
                "--controller-url",
                "",
                "--profile",
                "isolated",
                "--scope",
                "team-a",
                "--credential-file",
                "/token"
            ])),
            Err(CommandError::EmptyOptionValue)
        );
        assert_eq!(
            parse_args(args(&["bridge"])),
            Err(CommandError::MissingRequiredOption)
        );
        assert_eq!(
            parse_args(args(&[
                "bridge",
                "--credential-file",
                "relative/controller-token"
            ])),
            Err(CommandError::RelativeCredentialFile)
        );
        assert_eq!(
            parse_args(args(&[
                "bridge",
                "--controller-ca-file",
                "relative/controller-ca.pem"
            ])),
            Err(CommandError::RelativeControllerCaFile)
        );
    }

    #[test]
    fn reads_an_exact_unmodified_bearer_credential() {
        let mut input = Cursor::new(TEST_CREDENTIAL.to_vec());

        assert_eq!(
            read_bearer_credential(&mut input).expect("safe token"),
            TEST_CREDENTIAL
        );
    }

    #[test]
    fn rejects_short_oversized_or_non_token_credentials() {
        let mut empty = Cursor::new(Vec::<u8>::new());
        assert_eq!(
            read_bearer_credential(&mut empty),
            Err(CredentialError::TooShort)
        );
        let mut short = Cursor::new(vec![b'a'; MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES - 1]);
        assert_eq!(
            read_bearer_credential(&mut short),
            Err(CredentialError::TooShort)
        );

        let mut oversized = Cursor::new(vec![b'a'; MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES + 1]);
        assert_eq!(
            read_bearer_credential(&mut oversized),
            Err(CredentialError::TooLarge)
        );

        for &invalid_byte in b"\n\r !:" {
            let mut unsafe_value = vec![b'a'; MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES];
            unsafe_value[MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES / 2] = invalid_byte;
            let mut input = Cursor::new(unsafe_value);
            let error = read_bearer_credential(&mut input).expect_err("unsafe token");
            assert_eq!(error, CredentialError::InvalidByte);
        }
        for padding_index in [0, MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES / 2] {
            let mut unsafe_value = vec![b'a'; MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES];
            unsafe_value[padding_index] = b'=';
            let mut input = Cursor::new(unsafe_value);
            assert_eq!(
                read_bearer_credential(&mut input),
                Err(CredentialError::InvalidByte)
            );
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("sensitive source"))
        }
    }

    struct PartialFailingReader {
        delivered_secret: bool,
    }

    impl Read for PartialFailingReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.delivered_secret {
                return Err(io::Error::other("failure after partial secret"));
            }
            self.delivered_secret = true;
            buffer[..6].copy_from_slice(b"secret");
            Ok(6)
        }
    }

    #[test]
    fn hides_credential_read_error_details() {
        let error = read_bearer_credential(&mut FailingReader).expect_err("read failure");

        assert_eq!(error, CredentialError::Read);
        assert!(!error.to_string().contains("sensitive source"));

        let mut partial = PartialFailingReader {
            delivered_secret: false,
        };
        let error = read_bearer_credential(&mut partial).expect_err("partial read failure");
        assert_eq!(error, CredentialError::Read);
        assert!(!error.to_string().contains("secret"));
    }
}
