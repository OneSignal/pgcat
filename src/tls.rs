// Stream wrapper.

use std::path::Path;
use std::sync::Arc;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio_rustls::rustls::{
    self,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    DigitallySignedStruct, SignatureScheme,
};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

// TLS
pub fn load_certs(path: &Path) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let invalid = |_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert");
    CertificateDer::pem_file_iter(path)
        .map_err(invalid)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)
}

pub fn load_key(path: &Path) -> std::io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid key"))
}

/// The acceptor built from the last (certificate, key) pair seen in the config.
/// Building a `ServerConfig` reads both files from disk and parses them, so it
/// is done once per config change rather than once per client connection.
struct CachedAcceptor {
    certificate: String,
    private_key: String,
    acceptor: TlsAcceptor,
}

static ACCEPTOR: Lazy<Mutex<Option<CachedAcceptor>>> = Lazy::new(|| Mutex::new(None));

pub struct Tls {
    pub acceptor: TlsAcceptor,
}

impl Tls {
    pub fn new() -> Result<Self, Error> {
        let config = get_config();

        let (certificate, private_key) = match (
            config.general.tls_certificate,
            config.general.tls_private_key,
        ) {
            (Some(certificate), Some(private_key)) => (certificate, private_key),
            _ => return Err(Error::TlsError),
        };

        let mut cached = ACCEPTOR.lock();

        if let Some(entry) = cached.as_ref() {
            if entry.certificate == certificate && entry.private_key == private_key {
                return Ok(Tls {
                    acceptor: entry.acceptor.clone(),
                });
            }
        }

        let acceptor = Self::build_acceptor(&certificate, &private_key)?;

        *cached = Some(CachedAcceptor {
            certificate,
            private_key,
            acceptor: acceptor.clone(),
        });

        Ok(Tls { acceptor })
    }

    fn build_acceptor(certificate: &str, private_key: &str) -> Result<TlsAcceptor, Error> {
        let certs = load_certs(Path::new(certificate)).map_err(|_| Error::TlsError)?;
        let key = load_key(Path::new(private_key)).map_err(|_| Error::TlsError)?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|_| Error::TlsError)?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

/// Accepts any server certificate. Used for `verify_server_certificate = false`,
/// which behaves like libpq's `sslmode=prefer`: the link is encrypted but the
/// peer is not authenticated.
#[derive(Debug)]
pub struct NoCertificateVerification(pub Arc<CryptoProvider>);

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
