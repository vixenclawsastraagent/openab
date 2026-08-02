use openab_kubernetes_session::bridge::runtime::{BridgeEnvironment, BridgeEnvironmentError};
use openab_kubernetes_session::bridge::websocket::{
    bridge_websocket_config, run_bridge_websocket, BridgeWebSocketError,
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
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Uri};

const MAX_BEARER_CREDENTIAL_BYTES: usize = 4 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
struct BridgeCommand {
    controller_url: String,
    profile: String,
    scope: String,
    credential_file: PathBuf,
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
}

#[derive(Debug, Error, PartialEq, Eq)]
enum CredentialError {
    #[error("controller bearer credential could not be read")]
    Read,
    #[error("controller bearer credential must not be empty")]
    Empty,
    #[error("controller bearer credential exceeds its size limit")]
    TooLarge,
    #[error("controller bearer credential contains a disallowed byte")]
    InvalidByte,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum RequestError {
    #[error("controller URL must be an absolute wss URL without user information")]
    InvalidControllerUrl,
    #[error("controller WebSocket request could not be constructed")]
    InvalidRequest,
}

#[derive(Debug, Error)]
enum ApplicationError {
    #[error(transparent)]
    Command(#[from] CommandError),
    #[error("controller bearer credential file could not be opened")]
    OpenCredentialFile(#[source] std::io::Error),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error("broker-owned bridge environment is invalid")]
    Environment(#[source] BridgeEnvironmentError),
    #[error(transparent)]
    Request(#[from] RequestError),
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
            _ => return Err(CommandError::UnknownOption),
        }
    }

    Ok(BridgeCommand {
        controller_url: controller_url.ok_or(CommandError::MissingRequiredOption)?,
        profile: profile.ok_or(CommandError::MissingRequiredOption)?,
        scope: scope.ok_or(CommandError::MissingRequiredOption)?,
        credential_file: credential_file.ok_or(CommandError::MissingRequiredOption)?,
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
        .take((MAX_BEARER_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut credential)
        .is_err()
    {
        credential.fill(0);
        return Err(CredentialError::Read);
    }

    if credential.is_empty() {
        return Err(CredentialError::Empty);
    }
    if credential.len() > MAX_BEARER_CREDENTIAL_BYTES {
        credential.fill(0);
        return Err(CredentialError::TooLarge);
    }
    if !is_b64token(&credential) {
        credential.fill(0);
        return Err(CredentialError::InvalidByte);
    }
    Ok(credential)
}

/// Validate the closed `b64token` credential grammar from RFC 6750 section 2.1.
fn is_b64token(value: &[u8]) -> bool {
    let padding_start = value
        .iter()
        .position(|byte| *byte == b'=')
        .unwrap_or(value.len());
    padding_start > 0
        && value[..padding_start].iter().copied().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
        && value[padding_start..].iter().all(|byte| *byte == b'=')
}

fn build_controller_request(
    controller_url: &str,
    credential: &[u8],
) -> Result<Request, RequestError> {
    let uri = controller_url
        .parse::<Uri>()
        .map_err(|_| RequestError::InvalidControllerUrl)?;
    let authority = uri.authority().ok_or(RequestError::InvalidControllerUrl)?;
    if uri.scheme_str() != Some("wss")
        || authority.as_str().is_empty()
        || authority.host().is_empty()
        || authority.as_str().contains('@')
    {
        return Err(RequestError::InvalidControllerUrl);
    }
    if credential.is_empty()
        || credential.len() > MAX_BEARER_CREDENTIAL_BYTES
        || !is_b64token(credential)
    {
        return Err(RequestError::InvalidRequest);
    }

    let mut request = uri
        .into_client_request()
        .map_err(|_| RequestError::InvalidRequest)?;
    let mut authorization = Vec::with_capacity("Bearer ".len() + credential.len());
    authorization.extend_from_slice(b"Bearer ");
    authorization.extend_from_slice(credential);
    let header = HeaderValue::from_bytes(&authorization).map_err(|_| RequestError::InvalidRequest);
    authorization.fill(0);
    let mut header = header?;
    header.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, header);
    Ok(request)
}

async fn run<I>(args: I) -> Result<(), ApplicationError>
where
    I: IntoIterator<Item = OsString>,
{
    let command = parse_args(args)?;
    let environment = BridgeEnvironment::from_environment(&command.scope, &command.profile)
        .map_err(ApplicationError::Environment)?;

    let mut credential_file =
        File::open(&command.credential_file).map_err(ApplicationError::OpenCredentialFile)?;
    let mut credential = read_bearer_credential(&mut credential_file)?;
    drop(credential_file);
    let request = build_controller_request(&command.controller_url, &credential);
    credential.fill(0);
    let request = request?;

    let connection =
        connect_async_tls_with_config(request, Some(bridge_websocket_config()), false, None);
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
    use tokio_tungstenite::tungstenite::http::header::{
        AUTHORIZATION, CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
    };

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
            }
        );
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
    }

    #[test]
    fn reads_an_exact_unmodified_bearer_credential() {
        let mut input = Cursor::new(b"abc_DEF-123.~+/==".to_vec());

        assert_eq!(
            read_bearer_credential(&mut input).expect("safe token"),
            b"abc_DEF-123.~+/=="
        );
    }

    #[test]
    fn rejects_empty_oversized_or_non_token_credentials() {
        let mut empty = Cursor::new(Vec::<u8>::new());
        assert_eq!(
            read_bearer_credential(&mut empty),
            Err(CredentialError::Empty)
        );

        let mut oversized = Cursor::new(vec![b'a'; MAX_BEARER_CREDENTIAL_BYTES + 1]);
        assert_eq!(
            read_bearer_credential(&mut oversized),
            Err(CredentialError::TooLarge)
        );

        for unsafe_value in [
            b"token\n".as_slice(),
            b"token\r".as_slice(),
            b" token".as_slice(),
            b"=token".as_slice(),
            b"token=value".as_slice(),
            b"token!".as_slice(),
        ] {
            let mut input = Cursor::new(unsafe_value);
            let error = read_bearer_credential(&mut input).expect_err("unsafe token");
            assert_eq!(error, CredentialError::InvalidByte);
            assert!(!error.to_string().contains("token"));
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

    #[test]
    fn constructs_a_wss_upgrade_request_with_bearer_authorization() {
        let request = build_controller_request(
            "wss://controller.example.test/v1/bridge",
            b"abc_DEF-123.~+/==",
        )
        .expect("valid request");

        assert_eq!(request.method(), "GET");
        assert_eq!(request.uri(), "wss://controller.example.test/v1/bridge");
        assert_eq!(request.headers()[HOST], "controller.example.test");
        assert_eq!(request.headers()[CONNECTION], "Upgrade");
        assert_eq!(request.headers()[UPGRADE], "websocket");
        assert_eq!(request.headers()[SEC_WEBSOCKET_VERSION], "13");
        assert!(!request.headers()[SEC_WEBSOCKET_KEY].is_empty());
        assert_eq!(request.headers()[AUTHORIZATION], "Bearer abc_DEF-123.~+/==");
        assert!(request.headers()[AUTHORIZATION].is_sensitive());
        assert!(!format!("{request:?}").contains("abc_DEF-123.~+/=="));
    }

    #[test]
    fn rejects_non_tls_relative_and_userinfo_controller_urls() {
        for value in [
            "ws://controller.example.test/bridge",
            "https://controller.example.test/bridge",
            "/relative/bridge",
            "wss://user@controller.example.test/bridge",
            "wss://:443/bridge",
        ] {
            assert!(matches!(
                build_controller_request(value, b"safe-token"),
                Err(RequestError::InvalidControllerUrl)
            ));
        }
    }

    #[test]
    fn rejects_unsafe_credentials_when_constructing_a_request_directly() {
        for value in [
            b"".as_slice(),
            b"token\n".as_slice(),
            b"=token".as_slice(),
            b"token=value".as_slice(),
        ] {
            assert!(matches!(
                build_controller_request("wss://controller.example.test/bridge", value),
                Err(RequestError::InvalidRequest)
            ));
        }
    }
}
