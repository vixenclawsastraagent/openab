//! TLS termination for the trusted controller endpoint.
//!
//! Authentication still happens during the subsequent WebSocket upgrade, but
//! callers cannot dispatch an admitted controller connection over plaintext.

use super::{ControllerConnection, ControllerConnectionError, ControllerConnectionOutcome};
use rustls_pemfile::{read_all, Item};
use std::fmt;
use std::io::{BufRead, Cursor, Read};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

const MAX_CERTIFICATE_PEM_BYTES: usize = 256 * 1024;
const MAX_PRIVATE_KEY_PEM_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct ControllerTlsAcceptor {
    acceptor: TlsAcceptor,
    tls_handshake_timeout: Duration,
}

impl ControllerTlsAcceptor {
    /// Build a controller TLS endpoint from bounded PEM readers.
    pub fn from_pem<C, K>(
        certificate_pem: C,
        private_key_pem: K,
        tls_handshake_timeout: Duration,
    ) -> Result<Self, ControllerTlsConfigError>
    where
        C: BufRead,
        K: BufRead,
    {
        if tls_handshake_timeout.is_zero() {
            return Err(ControllerTlsConfigError::ZeroHandshakeTimeout);
        }

        let certificates = read_certificates(certificate_pem)?;
        let private_key = read_private_key(private_key_pem)?;
        let provider = Arc::new(ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| ControllerTlsConfigError::InvalidIdentity)?
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|_| ControllerTlsConfigError::InvalidIdentity)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config.max_early_data_size = 0;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            tls_handshake_timeout,
        })
    }

    /// Terminate TLS before serving one already-admitted controller connection.
    pub async fn serve<S>(
        &self,
        connection: ControllerConnection,
        stream: S,
    ) -> Result<ControllerConnectionOutcome, ControllerTlsConnectionError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let stream = self.accept_tls(stream).await?;
        connection
            .serve(stream)
            .await
            .map_err(|source| ControllerTlsConnectionError::Connection(Box::new(source)))
    }

    async fn accept_tls<S>(&self, stream: S) -> Result<TlsStream<S>, ControllerTlsConnectionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match timeout(self.tls_handshake_timeout, self.acceptor.accept(stream)).await {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(source)) => Err(ControllerTlsConnectionError::Handshake(source)),
            Err(_) => Err(ControllerTlsConnectionError::TimedOut),
        }
    }
}

impl fmt::Debug for ControllerTlsAcceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControllerTlsAcceptor")
            .field("tls_handshake_timeout", &self.tls_handshake_timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ControllerTlsConfigError {
    #[error("controller TLS certificate PEM exceeds its size limit")]
    CertificatePemTooLarge,
    #[error("controller TLS private key PEM exceeds its size limit")]
    PrivateKeyPemTooLarge,
    #[error("controller TLS certificate PEM is invalid")]
    InvalidCertificatePem,
    #[error("controller TLS private key PEM is invalid")]
    InvalidPrivateKeyPem,
    #[error("controller TLS certificate PEM contains a non-certificate item")]
    UnexpectedCertificatePemItem,
    #[error("controller TLS private key PEM contains a non-private-key item")]
    UnexpectedPrivateKeyPemItem,
    #[error("controller TLS certificate PEM contains no certificate")]
    MissingCertificate,
    #[error("controller TLS private key PEM contains no private key")]
    MissingPrivateKey,
    #[error("controller TLS private key PEM contains multiple private keys")]
    MultiplePrivateKeys,
    #[error("controller TLS certificate and private key do not form a valid identity")]
    InvalidIdentity,
    #[error("controller TLS handshake timeout must be greater than zero")]
    ZeroHandshakeTimeout,
}

#[derive(Debug, Error)]
pub enum ControllerTlsConnectionError {
    #[error("controller TLS handshake timed out")]
    TimedOut,
    #[error("controller TLS handshake failed")]
    Handshake(#[source] std::io::Error),
    #[error("controller connection failed after TLS negotiation")]
    Connection(#[source] Box<ControllerConnectionError>),
}

fn read_certificates<R>(reader: R) -> Result<Vec<CertificateDer<'static>>, ControllerTlsConfigError>
where
    R: BufRead,
{
    let bytes = read_bounded(
        reader,
        MAX_CERTIFICATE_PEM_BYTES,
        ControllerTlsConfigError::CertificatePemTooLarge,
        ControllerTlsConfigError::InvalidCertificatePem,
    )?;
    validate_pem_envelope(&bytes, PemRole::Certificate)?;
    let mut reader = Cursor::new(bytes.as_slice());
    let mut certificates = Vec::new();
    for item in read_all(&mut reader) {
        match item.map_err(|_| ControllerTlsConfigError::InvalidCertificatePem)? {
            Item::X509Certificate(certificate) => certificates.push(certificate),
            _ => return Err(ControllerTlsConfigError::UnexpectedCertificatePemItem),
        }
    }
    if certificates.is_empty() {
        return Err(ControllerTlsConfigError::MissingCertificate);
    }
    Ok(certificates)
}

fn read_private_key<R>(reader: R) -> Result<PrivateKeyDer<'static>, ControllerTlsConfigError>
where
    R: BufRead,
{
    let bytes = read_bounded(
        reader,
        MAX_PRIVATE_KEY_PEM_BYTES,
        ControllerTlsConfigError::PrivateKeyPemTooLarge,
        ControllerTlsConfigError::InvalidPrivateKeyPem,
    )?;
    validate_pem_envelope(bytes.as_slice(), PemRole::PrivateKey)?;
    let mut reader = Cursor::new(bytes.as_slice());
    let mut private_key = None;
    for item in read_all(&mut reader) {
        let candidate = match item.map_err(|_| ControllerTlsConfigError::InvalidPrivateKeyPem)? {
            Item::Pkcs1Key(key) => PrivateKeyDer::Pkcs1(key),
            Item::Pkcs8Key(key) => PrivateKeyDer::Pkcs8(key),
            Item::Sec1Key(key) => PrivateKeyDer::Sec1(key),
            _ => return Err(ControllerTlsConfigError::UnexpectedPrivateKeyPemItem),
        };
        if private_key.replace(candidate).is_some() {
            return Err(ControllerTlsConfigError::MultiplePrivateKeys);
        }
    }
    private_key.ok_or(ControllerTlsConfigError::MissingPrivateKey)
}

fn read_bounded<R>(
    reader: R,
    limit: usize,
    too_large: ControllerTlsConfigError,
    invalid: ControllerTlsConfigError,
) -> Result<Zeroizing<Vec<u8>>, ControllerTlsConfigError>
where
    R: BufRead,
{
    let mut bytes = Zeroizing::new(Vec::new());
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid)?;
    if bytes.len() > limit {
        return Err(too_large);
    }
    Ok(bytes)
}

#[derive(Clone, Copy)]
enum PemRole {
    Certificate,
    PrivateKey,
}

impl PemRole {
    fn accepts(self, label: &[u8]) -> bool {
        match self {
            Self::Certificate => label == b"CERTIFICATE",
            Self::PrivateKey => matches!(
                label,
                b"RSA PRIVATE KEY" | b"PRIVATE KEY" | b"EC PRIVATE KEY"
            ),
        }
    }

    fn invalid(self) -> ControllerTlsConfigError {
        match self {
            Self::Certificate => ControllerTlsConfigError::InvalidCertificatePem,
            Self::PrivateKey => ControllerTlsConfigError::InvalidPrivateKeyPem,
        }
    }

    fn unexpected(self) -> ControllerTlsConfigError {
        match self {
            Self::Certificate => ControllerTlsConfigError::UnexpectedCertificatePemItem,
            Self::PrivateKey => ControllerTlsConfigError::UnexpectedPrivateKeyPemItem,
        }
    }
}

fn validate_pem_envelope(bytes: &[u8], role: PemRole) -> Result<(), ControllerTlsConfigError> {
    let mut active_label = None;
    for raw_line in bytes.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        match active_label {
            None if line.iter().all(|byte| byte.is_ascii_whitespace()) => {}
            None => {
                let label = pem_marker_label(line, b"-----BEGIN ").ok_or_else(|| role.invalid())?;
                if !role.accepts(label) {
                    return Err(role.unexpected());
                }
                active_label = Some(label);
            }
            Some(label) => {
                if let Some(end_label) = pem_marker_label(line, b"-----END ") {
                    if end_label != label {
                        return Err(role.invalid());
                    }
                    active_label = None;
                } else if pem_marker_label(line, b"-----BEGIN ").is_some() {
                    return Err(role.invalid());
                }
            }
        }
    }
    if active_label.is_some() {
        return Err(role.invalid());
    }
    Ok(())
}

fn pem_marker_label<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let label = line.strip_prefix(prefix)?.strip_suffix(b"-----")?;
    (!label.is_empty()).then_some(label)
}

#[cfg(test)]
mod tests;
