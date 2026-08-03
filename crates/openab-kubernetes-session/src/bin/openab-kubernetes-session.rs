use openab_kubernetes_session::bridge::runtime::{BridgeEnvironment, BridgeEnvironmentError};
use openab_kubernetes_session::bridge::websocket::{
    bridge_websocket_config, run_bridge_websocket, BridgeWebSocketError,
};
use openab_kubernetes_session::bridge::{
    is_valid_controller_bearer_credential, MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES,
    MIN_CONTROLLER_BEARER_CREDENTIAL_BYTES,
};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::Item;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Uri};
use tokio_tungstenite::Connector;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONTROLLER_CA_PEM_BYTES: usize = 256 * 1024;
const CERTIFICATE_BEGIN: &[u8] = b"-----BEGIN CERTIFICATE-----";
const CERTIFICATE_END: &[u8] = b"-----END CERTIFICATE-----";

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

#[derive(Debug, Error, PartialEq, Eq)]
enum ControllerCaError {
    #[error("controller CA bundle could not be read")]
    Read,
    #[error("controller CA bundle exceeds its size limit")]
    TooLarge,
    #[error("controller CA bundle must contain at least one certificate")]
    MissingCertificate,
    #[error("controller CA bundle is not valid certificate PEM")]
    InvalidPem,
    #[error("controller CA bundle contains a non-certificate PEM block")]
    NonCertificateBlock,
    #[error("controller CA bundle contains an invalid certificate")]
    InvalidCertificate,
    #[error("native trust roots could not be loaded")]
    NativeRoots,
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
    #[error("controller CA file could not be opened")]
    OpenControllerCaFile,
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    ControllerCa(#[from] ControllerCaError),
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

fn read_controller_ca_certificates<R>(
    reader: &mut R,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, ControllerCaError>
where
    R: Read + ?Sized,
{
    let mut pem = Vec::new();
    reader
        .take((MAX_CONTROLLER_CA_PEM_BYTES + 1) as u64)
        .read_to_end(&mut pem)
        .map_err(|_| ControllerCaError::Read)?;
    if pem.len() > MAX_CONTROLLER_CA_PEM_BYTES {
        return Err(ControllerCaError::TooLarge);
    }

    validate_certificate_only_envelope(&pem)?;

    let mut certificates = Vec::new();
    let mut validator = RootCertStore::empty();
    for item in rustls_pemfile::read_all(&mut Cursor::new(pem)) {
        let item = item.map_err(|_| ControllerCaError::InvalidPem)?;
        let Item::X509Certificate(certificate) = item else {
            return Err(ControllerCaError::NonCertificateBlock);
        };
        validator
            .add(certificate.clone())
            .map_err(|_| ControllerCaError::InvalidCertificate)?;
        certificates.push(certificate);
    }

    if certificates.is_empty() {
        return Err(ControllerCaError::MissingCertificate);
    }
    Ok(certificates)
}

fn validate_certificate_only_envelope(pem: &[u8]) -> Result<(), ControllerCaError> {
    let mut inside_certificate = false;
    let mut body_line_seen = false;
    let mut certificate_count = 0usize;

    for raw_line in pem.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if !inside_certificate {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            if line == CERTIFICATE_BEGIN {
                inside_certificate = true;
                body_line_seen = false;
                continue;
            }
            if line.starts_with(b"-----BEGIN ") {
                return Err(ControllerCaError::NonCertificateBlock);
            }
            return Err(ControllerCaError::InvalidPem);
        }

        if line == CERTIFICATE_END {
            if !body_line_seen {
                return Err(ControllerCaError::InvalidPem);
            }
            inside_certificate = false;
            certificate_count += 1;
            continue;
        }
        if line.is_empty()
            || line.starts_with(b"-----")
            || !line
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
        {
            return Err(ControllerCaError::InvalidPem);
        }
        body_line_seen = true;
    }

    if inside_certificate {
        return Err(ControllerCaError::InvalidPem);
    }
    if certificate_count == 0 {
        return Err(ControllerCaError::MissingCertificate);
    }
    Ok(())
}

fn controller_connector(
    controller_ca: Option<&mut dyn Read>,
) -> Result<Option<Connector>, ControllerCaError> {
    let Some(controller_ca) = controller_ca else {
        return Ok(None);
    };

    let private_roots = read_controller_ca_certificates(controller_ca)?;
    let native_roots =
        rustls_native_certs::load_native_certs().map_err(|_| ControllerCaError::NativeRoots)?;
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(native_roots);
    for certificate in private_roots {
        roots
            .add(certificate)
            .map_err(|_| ControllerCaError::InvalidCertificate)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Some(Connector::Rustls(Arc::new(config))))
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
    if !is_valid_controller_bearer_credential(credential) {
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

    let mut controller_ca_file = if let Some(path) = &command.controller_ca_file {
        Some(File::open(path).map_err(|_| ApplicationError::OpenControllerCaFile)?)
    } else {
        None
    };
    let connector = controller_connector(
        controller_ca_file
            .as_mut()
            .map(|file| file as &mut dyn Read),
    )?;
    drop(controller_ca_file);

    let mut credential_file =
        File::open(&command.credential_file).map_err(ApplicationError::OpenCredentialFile)?;
    let mut credential = read_bearer_credential(&mut credential_file)?;
    drop(credential_file);
    let request = build_controller_request(&command.controller_url, &credential);
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
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use std::io::{self, Cursor};
    use tokio_tungstenite::tungstenite::http::header::{
        AUTHORIZATION, CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
    };

    const TEST_CREDENTIAL: &[u8] = b"abc_DEF-123.~+/abc_DEF-123.~+/==";
    const TEST_AUTHORIZATION: &str = "Bearer abc_DEF-123.~+/abc_DEF-123.~+/==";

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    fn test_ca_pem() -> String {
        let CertifiedKey { cert, .. } =
            generate_simple_self_signed(vec!["controller.example.test".to_owned()])
                .expect("test CA");
        cert.pem()
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
    fn reads_a_strict_certificate_only_ca_bundle() {
        let bundle = format!("\n{}\n{}\n", test_ca_pem(), test_ca_pem());
        let mut input = Cursor::new(bundle.into_bytes());

        let certificates =
            read_controller_ca_certificates(&mut input).expect("valid private CA bundle");

        assert_eq!(certificates.len(), 2);
    }

    #[test]
    fn rejects_empty_truncated_garbage_and_key_ca_bundles() {
        let CertifiedKey { signing_key, .. } =
            generate_simple_self_signed(vec!["controller.example.test".to_owned()])
                .expect("test key");
        let cases = [
            (Vec::new(), ControllerCaError::MissingCertificate),
            (
                b"-----BEGIN CERTIFICATE-----\nAQID\n".to_vec(),
                ControllerCaError::InvalidPem,
            ),
            (
                b"not a PEM bundle\n".to_vec(),
                ControllerCaError::InvalidPem,
            ),
            (
                signing_key.serialize_pem().into_bytes(),
                ControllerCaError::NonCertificateBlock,
            ),
        ];

        for (bundle, expected) in cases {
            let mut input = Cursor::new(bundle);
            assert_eq!(read_controller_ca_certificates(&mut input), Err(expected));
        }

        let mut prefixed = Cursor::new(format!("garbage\n{}", test_ca_pem()).into_bytes());
        assert_eq!(
            read_controller_ca_certificates(&mut prefixed),
            Err(ControllerCaError::InvalidPem)
        );
    }

    #[test]
    fn rejects_invalid_der_in_a_certificate_envelope() {
        let mut input =
            Cursor::new(b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n".to_vec());

        assert_eq!(
            read_controller_ca_certificates(&mut input),
            Err(ControllerCaError::InvalidCertificate)
        );
    }

    #[test]
    fn bounds_and_sanitizes_controller_ca_reads() {
        let mut exact_limit = test_ca_pem().into_bytes();
        exact_limit.resize(MAX_CONTROLLER_CA_PEM_BYTES, b' ');
        let mut exact_limit_reader = Cursor::new(exact_limit.clone());
        assert_eq!(
            read_controller_ca_certificates(&mut exact_limit_reader)
                .expect("an exact-limit valid bundle")
                .len(),
            1
        );

        exact_limit.push(b' ');
        let mut oversized = Cursor::new(exact_limit);
        assert_eq!(
            read_controller_ca_certificates(&mut oversized),
            Err(ControllerCaError::TooLarge)
        );

        let mut failing = FailingReader;
        let error = read_controller_ca_certificates(&mut failing).expect_err("read failure");
        assert_eq!(error, ControllerCaError::Read);
        assert!(!error.to_string().contains("sensitive source"));
    }

    #[test]
    fn selects_a_custom_connector_only_when_a_ca_is_present() {
        assert!(controller_connector(None)
            .expect("default connector")
            .is_none());

        let mut input = Cursor::new(test_ca_pem().into_bytes());
        assert!(matches!(
            controller_connector(Some(&mut input)).expect("private CA connector"),
            Some(tokio_tungstenite::Connector::Rustls(_))
        ));
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

        for invalid_byte in [b'\n', b'\r', b' ', b'!', b':'] {
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

    #[test]
    fn constructs_a_wss_upgrade_request_with_bearer_authorization() {
        let request =
            build_controller_request("wss://controller.example.test/v1/bridge", TEST_CREDENTIAL)
                .expect("valid request");

        assert_eq!(request.method(), "GET");
        assert_eq!(request.uri(), "wss://controller.example.test/v1/bridge");
        assert_eq!(request.headers()[HOST], "controller.example.test");
        assert_eq!(request.headers()[CONNECTION], "Upgrade");
        assert_eq!(request.headers()[UPGRADE], "websocket");
        assert_eq!(request.headers()[SEC_WEBSOCKET_VERSION], "13");
        assert!(!request.headers()[SEC_WEBSOCKET_KEY].is_empty());
        assert_eq!(request.headers()[AUTHORIZATION], TEST_AUTHORIZATION);
        assert!(request.headers()[AUTHORIZATION].is_sensitive());
        assert!(!format!("{request:?}").contains(std::str::from_utf8(TEST_CREDENTIAL).unwrap()));
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
