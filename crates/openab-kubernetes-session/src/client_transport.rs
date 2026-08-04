use crate::bridge::{
    is_valid_controller_bearer_credential, MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES,
};
use crate::wire::{MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::Item;
use std::fmt;
use std::io::{Cursor, Read};
use std::sync::Arc;
use thiserror::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue, Uri};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::Connector;
use zeroize::Zeroizing;

pub const MAX_CLIENT_CA_PEM_BYTES: usize = 256 * 1024;
pub const MAX_CLIENT_URL_BYTES: usize = 2_048;

const CERTIFICATE_BEGIN: &[u8] = b"-----BEGIN CERTIFICATE-----";
const CERTIFICATE_END: &[u8] = b"-----END CERTIFICATE-----";
const WORKER_POD_UID_HEADER: HeaderName = HeaderName::from_static("x-openab-pod-uid");
const WORKER_TOKEN_HEX_BYTES: usize = 64;
const MAX_WORKER_POD_UID_BYTES: usize = 256;

/// The only outbound WebSocket endpoints understood by this add-on.
#[derive(Clone, Copy)]
pub enum ClientEndpoint<'a> {
    Bridge {
        bearer: &'a [u8],
    },
    Worker {
        token: &'a [u8; 32],
        pod_uid: &'a str,
    },
}

impl ClientEndpoint<'_> {
    const fn path(self) -> &'static str {
        match self {
            Self::Bridge { .. } => "/v1/bridge",
            Self::Worker { .. } => "/v1/worker",
        }
    }
}

impl fmt::Debug for ClientEndpoint<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bridge { .. } => formatter
                .debug_struct("Bridge")
                .field("bearer", &"<redacted>")
                .finish(),
            Self::Worker { .. } => formatter
                .debug_struct("Worker")
                .field("token", &"<redacted>")
                .field("pod_uid", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PrivateCaError {
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
pub enum ClientRequestError {
    #[error("controller URL exceeds its size limit")]
    UrlTooLarge,
    #[error(
        "controller URL must be an exact absolute wss endpoint without user information, query, or fragment"
    )]
    InvalidUrl,
    #[error("controller bearer authorization is invalid")]
    InvalidAuthorization,
    #[error("worker Pod UID is invalid")]
    InvalidWorkerPodUid,
    #[error("controller WebSocket request could not be constructed")]
    InvalidRequest,
}

/// Build finite transport limits shared by bridge and worker clients.
pub fn client_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: 0,
        max_write_buffer_size: MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES,
        max_message_size: Some(MAX_ACP_FRAME_BYTES),
        max_frame_size: Some(MAX_ACP_FRAME_BYTES),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

/// Select native roots by default, or native plus one bounded private bundle.
pub fn client_connector(
    private_ca: Option<&mut dyn Read>,
) -> Result<Option<Connector>, PrivateCaError> {
    let Some(private_ca) = private_ca else {
        return Ok(None);
    };

    let private_roots = read_private_ca_certificates(private_ca)?;
    let native_roots =
        rustls_native_certs::load_native_certs().map_err(|_| PrivateCaError::NativeRoots)?;
    build_client_connector(native_roots, private_roots).map(Some)
}

/// Validate one bounded certificate-only private CA bundle without loading
/// platform roots or constructing a connector.
pub fn validate_client_ca_pem(pem: &[u8]) -> Result<(), PrivateCaError> {
    read_private_ca_certificates(&mut Cursor::new(pem)).map(drop)
}

/// Validate the one worker relay URL without constructing a secret header.
pub fn validate_worker_controller_url(controller_url: &str) -> Result<(), ClientRequestError> {
    validate_client_uri(controller_url, "/v1/worker").map(drop)
}

fn build_client_connector(
    native_roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    private_roots: Vec<rustls::pki_types::CertificateDer<'static>>,
) -> Result<Connector, PrivateCaError> {
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(native_roots);
    for certificate in private_roots {
        roots
            .add(certificate)
            .map_err(|_| PrivateCaError::InvalidCertificate)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Connector::Rustls(Arc::new(config)))
}

/// Construct one sensitive bearer-authenticated request for a closed endpoint.
pub fn build_client_request(
    endpoint: ClientEndpoint<'_>,
    controller_url: &str,
) -> Result<Request, ClientRequestError> {
    let uri = validate_client_uri(controller_url, endpoint.path())?;
    let mut request = uri
        .into_client_request()
        .map_err(|_| ClientRequestError::InvalidRequest)?;
    match endpoint {
        ClientEndpoint::Bridge { bearer } => {
            if !is_valid_controller_bearer_credential(bearer) {
                return Err(ClientRequestError::InvalidAuthorization);
            }
            insert_sensitive_bearer(&mut request, bearer)?;
        }
        ClientEndpoint::Worker { token, pod_uid } => {
            if !is_valid_worker_pod_uid(pod_uid) {
                return Err(ClientRequestError::InvalidWorkerPodUid);
            }
            let mut encoded_token = Zeroizing::new([0_u8; WORKER_TOKEN_HEX_BYTES]);
            hex::encode_to_slice(token, encoded_token.as_mut())
                .map_err(|_| ClientRequestError::InvalidAuthorization)?;
            insert_sensitive_bearer(&mut request, encoded_token.as_slice())?;

            let mut pod_uid_header = HeaderValue::from_str(pod_uid)
                .map_err(|_| ClientRequestError::InvalidWorkerPodUid)?;
            pod_uid_header.set_sensitive(true);
            request
                .headers_mut()
                .insert(WORKER_POD_UID_HEADER, pod_uid_header);
        }
    }
    Ok(request)
}

fn validate_client_uri(
    controller_url: &str,
    expected_path: &'static str,
) -> Result<Uri, ClientRequestError> {
    if controller_url.len() > MAX_CLIENT_URL_BYTES {
        return Err(ClientRequestError::UrlTooLarge);
    }
    if controller_url.contains('#') {
        return Err(ClientRequestError::InvalidUrl);
    }
    let uri = controller_url
        .parse::<Uri>()
        .map_err(|_| ClientRequestError::InvalidUrl)?;
    let authority = uri.authority().ok_or(ClientRequestError::InvalidUrl)?;
    let explicit_port = authority
        .as_str()
        .strip_prefix(authority.host())
        .ok_or(ClientRequestError::InvalidUrl)?;
    let port_is_valid = explicit_port.is_empty()
        || (explicit_port.starts_with(':') && authority.port_u16().is_some());
    if uri.scheme_str() != Some("wss")
        || authority.host().is_empty()
        || authority.as_str().contains('@')
        || !port_is_valid
        || uri.query().is_some()
        || uri.path() != expected_path
    {
        return Err(ClientRequestError::InvalidUrl);
    }
    Ok(uri)
}

fn insert_sensitive_bearer(
    request: &mut Request,
    credential: &[u8],
) -> Result<(), ClientRequestError> {
    let mut authorization = Zeroizing::new(Vec::with_capacity(
        "Bearer ".len() + credential.len().min(MAX_CONTROLLER_BEARER_CREDENTIAL_BYTES),
    ));
    authorization.extend_from_slice(b"Bearer ");
    authorization.extend_from_slice(credential);
    let header = HeaderValue::from_bytes(authorization.as_slice())
        .map_err(|_| ClientRequestError::InvalidRequest);
    let mut header = header?;
    header.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, header);
    Ok(())
}

fn is_valid_worker_pod_uid(value: &str) -> bool {
    (1..=MAX_WORKER_POD_UID_BYTES).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/' && byte != b'\\')
}

fn read_private_ca_certificates<R>(
    reader: &mut R,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, PrivateCaError>
where
    R: Read + ?Sized,
{
    let mut pem = Vec::new();
    reader
        .take((MAX_CLIENT_CA_PEM_BYTES + 1) as u64)
        .read_to_end(&mut pem)
        .map_err(|_| PrivateCaError::Read)?;
    if pem.len() > MAX_CLIENT_CA_PEM_BYTES {
        return Err(PrivateCaError::TooLarge);
    }

    validate_certificate_only_envelope(&pem)?;

    let mut certificates = Vec::new();
    let mut validator = RootCertStore::empty();
    for item in rustls_pemfile::read_all(&mut Cursor::new(pem)) {
        let item = item.map_err(|_| PrivateCaError::InvalidPem)?;
        let Item::X509Certificate(certificate) = item else {
            return Err(PrivateCaError::NonCertificateBlock);
        };
        validator
            .add(certificate.clone())
            .map_err(|_| PrivateCaError::InvalidCertificate)?;
        certificates.push(certificate);
    }

    if certificates.is_empty() {
        return Err(PrivateCaError::MissingCertificate);
    }
    Ok(certificates)
}

fn validate_certificate_only_envelope(pem: &[u8]) -> Result<(), PrivateCaError> {
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
                return Err(PrivateCaError::NonCertificateBlock);
            }
            return Err(PrivateCaError::InvalidPem);
        }

        if line == CERTIFICATE_END {
            if !body_line_seen {
                return Err(PrivateCaError::InvalidPem);
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
            return Err(PrivateCaError::InvalidPem);
        }
        body_line_seen = true;
    }

    if inside_certificate {
        return Err(PrivateCaError::InvalidPem);
    }
    if certificate_count == 0 {
        return Err(PrivateCaError::MissingCertificate);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConnection, ServerConfig, ServerConnection};

    fn identity(hostname: &str) -> CertifiedKey<KeyPair> {
        generate_simple_self_signed(vec![hostname.to_owned()]).expect("test TLS identity")
    }

    #[test]
    fn private_ca_parser_accepts_multiple_certificates_and_the_exact_limit() {
        let first = identity("first.example.test");
        let second = identity("second.example.test");
        let bundle = format!("\n{}\n{}\n", first.cert.pem(), second.cert.pem());
        assert_eq!(
            read_private_ca_certificates(&mut Cursor::new(bundle))
                .expect("multi-certificate private roots")
                .len(),
            2
        );

        let mut exact = first.cert.pem().into_bytes();
        exact.resize(MAX_CLIENT_CA_PEM_BYTES, b' ');
        assert_eq!(
            read_private_ca_certificates(&mut Cursor::new(exact))
                .expect("exact-limit private roots")
                .len(),
            1
        );
    }

    #[test]
    fn deterministic_native_and_private_roots_keep_standard_verification() {
        let native_identity = identity("native.example.test");
        let private_identity = identity("controller.example.test");
        let private_roots =
            read_private_ca_certificates(&mut Cursor::new(private_identity.cert.pem()))
                .expect("private roots");
        let connector =
            build_client_connector(vec![native_identity.cert.der().clone()], private_roots)
                .expect("deterministic connector");
        let Connector::Rustls(client_config) = connector else {
            panic!("root builder must select rustls");
        };

        assert_eq!(
            complete_handshake(
                Arc::clone(&client_config),
                server_config(&private_identity),
                "controller.example.test",
            )
            .unwrap(),
            Some("controller.example.test".to_owned())
        );
        assert_eq!(
            complete_handshake(
                Arc::clone(&client_config),
                server_config(&native_identity),
                "native.example.test",
            )
            .unwrap(),
            Some("native.example.test".to_owned())
        );
        assert!(complete_handshake(
            Arc::clone(&client_config),
            server_config(&private_identity),
            "wrong.example.test",
        )
        .is_err());

        let untrusted_identity = identity("controller.example.test");
        assert!(complete_handshake(
            client_config,
            server_config(&untrusted_identity),
            "controller.example.test",
        )
        .is_err());
    }

    fn server_config(identity: &CertifiedKey<KeyPair>) -> Arc<ServerConfig> {
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            identity.signing_key.serialize_der(),
        ));
        Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![identity.cert.der().clone()], key)
                .expect("test server config"),
        )
    }

    fn complete_handshake(
        client_config: Arc<ClientConfig>,
        server_config: Arc<ServerConfig>,
        server_name: &str,
    ) -> Result<Option<String>, rustls::Error> {
        let name = ServerName::try_from(server_name.to_owned()).expect("test server name");
        let mut client = ClientConnection::new(client_config, name).expect("test client");
        let mut server = ServerConnection::new(server_config).expect("test server");

        for _ in 0..16 {
            let mut client_bytes = Vec::new();
            client
                .write_tls(&mut client_bytes)
                .expect("serialize client TLS");
            if !client_bytes.is_empty() {
                server
                    .read_tls(&mut Cursor::new(client_bytes))
                    .expect("read client TLS");
                server.process_new_packets()?;
            }

            let mut server_bytes = Vec::new();
            server
                .write_tls(&mut server_bytes)
                .expect("serialize server TLS");
            if !server_bytes.is_empty() {
                client
                    .read_tls(&mut Cursor::new(server_bytes))
                    .expect("read server TLS");
                client.process_new_packets()?;
            }

            if !client.is_handshaking() && !server.is_handshaking() {
                return Ok(server.server_name().map(str::to_owned));
            }
        }
        panic!("TLS handshake did not settle");
    }
}
