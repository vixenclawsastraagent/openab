use super::*;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{duplex, AsyncWriteExt};
use tokio_rustls::rustls::client::ClientConfig;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::TlsConnector;

fn identity() -> (
    String,
    String,
    tokio_rustls::rustls::pki_types::CertificateDer<'static>,
) {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test identity");
    (cert.pem(), signing_key.serialize_pem(), cert.der().clone())
}

fn tls_acceptor(cert_pem: &str, key_pem: &str) -> ControllerTlsAcceptor {
    ControllerTlsAcceptor::from_pem(
        Cursor::new(cert_pem.as_bytes()),
        Cursor::new(key_pem.as_bytes()),
        Duration::from_secs(1),
    )
    .expect("TLS acceptor")
}

fn client_config(
    certificate: Option<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    if let Some(certificate) = certificate {
        roots.add(certificate).expect("test root");
    }
    ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

#[test]
fn pem_identity_is_bounded_strict_and_key_matched() {
    let (cert_pem, key_pem, _) = identity();
    tls_acceptor(&cert_pem, &key_pem);

    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(Vec::<u8>::new()),
            Cursor::new(key_pem.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::MissingCertificate)
    ));
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(Vec::<u8>::new()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::MissingPrivateKey)
    ));

    let cert_with_key = format!("{cert_pem}{key_pem}");
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_with_key.as_bytes()),
            Cursor::new(key_pem.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::UnexpectedCertificatePemItem)
    ));
    let key_with_cert = format!("{key_pem}{cert_pem}");
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(key_with_cert.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::UnexpectedPrivateKeyPemItem)
    ));
    let duplicate_key = format!("{key_pem}{key_pem}");
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(duplicate_key.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::MultiplePrivateKeys)
    ));

    let (_, other_key, _) = identity();
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(other_key.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::InvalidIdentity)
    ));
}

#[test]
fn pem_errors_are_static_and_oversized_input_fails_closed() {
    let (cert_pem, key_pem, _) = identity();
    let malformed = b"-----BEGIN CERTIFICATE-----\nAAAA\n";
    let error = ControllerTlsAcceptor::from_pem(
        Cursor::new(malformed),
        Cursor::new(key_pem.as_bytes()),
        Duration::from_secs(1),
    )
    .expect_err("malformed certificate");
    assert_eq!(error, ControllerTlsConfigError::InvalidCertificatePem);
    assert!(!error.to_string().contains("AAAA"));

    for certificate in [format!("junk\n{cert_pem}"), format!("{cert_pem}junk\n")] {
        assert_eq!(
            ControllerTlsAcceptor::from_pem(
                Cursor::new(certificate.as_bytes()),
                Cursor::new(key_pem.as_bytes()),
                Duration::from_secs(1),
            )
            .expect_err("certificate junk must fail closed"),
            ControllerTlsConfigError::InvalidCertificatePem
        );
    }
    let unknown = b"-----BEGIN UNKNOWN-----\nAAAA\n-----END UNKNOWN-----\n";
    assert_eq!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(unknown),
            Cursor::new(key_pem.as_bytes()),
            Duration::from_secs(1),
        )
        .expect_err("unknown certificate item must fail closed"),
        ControllerTlsConfigError::UnexpectedCertificatePemItem
    );

    let key_with_junk = format!("{key_pem}junk\n");
    assert_eq!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(key_with_junk.as_bytes()),
            Duration::from_secs(1),
        )
        .expect_err("private key junk must fail closed"),
        ControllerTlsConfigError::InvalidPrivateKeyPem
    );
    assert_eq!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(cert_pem.as_bytes()),
            Cursor::new(unknown),
            Duration::from_secs(1),
        )
        .expect_err("unknown private key item must fail closed"),
        ControllerTlsConfigError::UnexpectedPrivateKeyPemItem
    );

    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(vec![b'x'; MAX_CERTIFICATE_PEM_BYTES + 1]),
            Cursor::new(key_pem.as_bytes()),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::CertificatePemTooLarge)
    ));
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(identity().0.into_bytes()),
            Cursor::new(vec![b'x'; MAX_PRIVATE_KEY_PEM_BYTES + 1]),
            Duration::from_secs(1),
        ),
        Err(ControllerTlsConfigError::PrivateKeyPemTooLarge)
    ));
    assert!(matches!(
        ControllerTlsAcceptor::from_pem(
            Cursor::new(identity().0.into_bytes()),
            Cursor::new(key_pem.as_bytes()),
            Duration::ZERO,
        ),
        Err(ControllerTlsConfigError::ZeroHandshakeTimeout)
    ));
}

#[tokio::test]
async fn trusted_hostname_completes_tls_and_unknown_ca_fails() {
    let (cert_pem, key_pem, certificate) = identity();
    let acceptor = tls_acceptor(&cert_pem, &key_pem);
    let (client_io, server_io) = duplex(16 * 1024);
    let connector = TlsConnector::from(Arc::new(client_config(Some(certificate))));
    let name = ServerName::try_from("localhost").expect("server name");

    let (server, client) = tokio::join!(
        acceptor.accept_tls(server_io),
        connector.connect(name, client_io)
    );
    server.expect("server TLS");
    client.expect("client TLS");

    let acceptor = tls_acceptor(&cert_pem, &key_pem);
    let (client_io, server_io) = duplex(16 * 1024);
    let connector = TlsConnector::from(Arc::new(client_config(None)));
    let name = ServerName::try_from("localhost").expect("server name");
    let (server, client) = tokio::join!(
        acceptor.accept_tls(server_io),
        connector.connect(name, client_io)
    );
    assert!(server.is_err());
    assert!(client.is_err());
}

#[tokio::test]
async fn plaintext_http_is_never_accepted_as_tls() {
    let (cert_pem, key_pem, _) = identity();
    let acceptor = tls_acceptor(&cert_pem, &key_pem);
    let (mut client_io, server_io) = duplex(8 * 1024);

    let client = async move {
        client_io
            .write_all(b"GET /v1/bridge HTTP/1.1\r\n\r\n")
            .await
            .expect("write plaintext");
    };
    let (server, ()) = tokio::join!(acceptor.accept_tls(server_io), client);
    assert!(matches!(
        server,
        Err(ControllerTlsConnectionError::Handshake(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn tls_handshake_has_one_fixed_deadline() {
    let (cert_pem, key_pem, _) = identity();
    let acceptor = tls_acceptor(&cert_pem, &key_pem);
    let (_client_io, server_io) = duplex(8 * 1024);
    let server = tokio::spawn(async move { acceptor.accept_tls(server_io).await });

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        server.await.expect("server task"),
        Err(ControllerTlsConnectionError::TimedOut)
    ));
}
