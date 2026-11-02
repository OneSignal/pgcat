// Stream wrapper.

use std::path::Path;
use std::sync::Arc;

use log::error;
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
    let key = PrivateKeyDer::from_pem_file(path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid key"))?;

    Ok(match key {
        // rustls 0.21 tried every key parser regardless of the PEM label, so a
        // "BEGIN RSA PRIVATE KEY" file that actually holds a PKCS#8 body (which is
        // what some tooling writes) used to load fine. rustls 0.23 trusts the
        // label, so sniff the DER and keep accepting those files.
        PrivateKeyDer::Pkcs1(der) if looks_like_pkcs8(der.secret_pkcs1_der()) => {
            PrivateKeyDer::Pkcs8(der.secret_pkcs1_der().to_vec().into())
        }
        key => key,
    })
}

/// True when `der` is `SEQUENCE { INTEGER 0, SEQUENCE { ... } ... }`, the shape of a
/// PKCS#8 PrivateKeyInfo; a PKCS#1 RSAPrivateKey has an INTEGER (the modulus) after
/// the version instead.
fn looks_like_pkcs8(der: &[u8]) -> bool {
    // Outer SEQUENCE header: tag, then a short-form or long-form length.
    let mut i = match der {
        [0x30, len, ..] if *len < 0x80 => 2,
        [0x30, len, ..] if *len & 0x80 != 0 => 2 + (len & 0x7f) as usize,
        _ => return false,
    };
    // version INTEGER 0
    if der.get(i..i + 3) != Some(&[0x02, 0x01, 0x00]) {
        return false;
    }
    i += 3;
    der.get(i) == Some(&0x30)
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
        let certs = load_certs(Path::new(certificate)).map_err(|err| {
            error!("Could not load tls_certificate {}: {}", certificate, err);
            Error::TlsError
        })?;
        let key = load_key(Path::new(private_key)).map_err(|err| {
            error!("Could not load tls_private_key {}: {}", private_key, err);
            Error::TlsError
        })?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| {
                error!("Could not build the TLS server config: {}", err);
                Error::TlsError
            })?;

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

#[cfg(test)]
mod tests {
    use super::*;

    // The certificate and key CircleCI runs the TLS integration tests with.
    const CERT: &str = ".circleci/server.cert";
    const KEY: &str = ".circleci/server.key";

    #[test]
    fn loads_ci_certificate_and_key() {
        let certs = load_certs(Path::new(CERT)).expect("certificate");
        assert_eq!(certs.len(), 1);
        load_key(Path::new(KEY)).expect("private key");
    }

    #[test]
    fn sniffs_pkcs8_bodies() {
        // PKCS#8: SEQUENCE { INTEGER 0, SEQUENCE { OID ... } ... }
        assert!(looks_like_pkcs8(&[
            0x30, 0x82, 0x04, 0xbf, 0x02, 0x01, 0x00, 0x30, 0x0d, 0x06
        ]));
        // PKCS#1: SEQUENCE { INTEGER 0, INTEGER modulus ... }
        assert!(!looks_like_pkcs8(&[
            0x30, 0x82, 0x04, 0xa2, 0x02, 0x01, 0x00, 0x02, 0x82, 0x01
        ]));
        assert!(!looks_like_pkcs8(&[]));
        assert!(!looks_like_pkcs8(&[0x30, 0x03, 0x02, 0x01]));
    }

    #[test]
    fn builds_acceptor_from_ci_certificate_and_key() {
        // .circleci/server.key is a PKCS#8 body under a "RSA PRIVATE KEY" label;
        // keep it that way, it is the regression fixture for load_key's sniffing.
        let certs = load_certs(Path::new(CERT)).expect("certificate");
        let key = load_key(Path::new(KEY)).expect("private key");
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key);
        assert!(config.is_ok(), "{:?}", config.err());
        Tls::build_acceptor(CERT, KEY).expect("acceptor");
    }
}
