//! TLS with the same modes as libpq's `sslmode` (see [`TlsMode`]).
//!
//! ClickHouse serves HTTP and HTTPS on different ports, so nothing is negotiated inside the
//! connection: `prefer` tries a TLS handshake first and speaks plain HTTP only when the server
//! does not answer one (the port is a plain one), as libpq falls back.

use std::path::Path;
use std::sync::Arc;

use rustls::client::WantsClientCert;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, ConfigBuilder, DigitallySignedStruct, RootCertStore,
    SignatureScheme,
};
use spelaion_driver_api::{DriverError, Result, TlsMode};

/// The certificate a client presents to log in without a password (ADR-022), with its key.
pub struct ClientIdentity<'a> {
    pub cert: &'a Path,
    pub key: &'a Path,
}

/// How a connection to the server is made.
#[derive(Clone)]
pub enum Tls {
    /// Plain HTTP.
    Off,
    /// HTTPS, and nothing else.
    Required(Arc<ClientConfig>),
    /// HTTPS when the server answers a handshake, plain HTTP when it does not.
    Preferred(Arc<ClientConfig>),
}

/// The TLS of a mode.
pub fn configure(
    mode: TlsMode,
    ca: Option<&Path>,
    identity: Option<ClientIdentity<'_>>,
) -> Result<Tls> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?;

    Ok(match mode {
        TlsMode::Disable => Tls::Off,
        TlsMode::Prefer | TlsMode::Require => {
            let verifier = Arc::new(NoVerification(provider.signature_verification_algorithms));
            let config = Arc::new(client_auth(
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(verifier),
                identity,
            )?);
            if mode == TlsMode::Prefer {
                Tls::Preferred(config)
            } else {
                Tls::Required(config)
            }
        }
        TlsMode::VerifyCa => {
            let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots(ca)?), provider)
                .build()
                .map_err(tls_error)?;
            Tls::Required(Arc::new(client_auth(
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(IgnoreHostname(inner))),
                identity,
            )?))
        }
        TlsMode::VerifyFull => Tls::Required(Arc::new(client_auth(
            builder.with_root_certificates(roots(ca)?),
            identity,
        )?)),
    })
}

/// Finishes the configuration with the client's certificate, if any.
fn client_auth(
    builder: ConfigBuilder<ClientConfig, WantsClientCert>,
    identity: Option<ClientIdentity<'_>>,
) -> Result<ClientConfig> {
    let Some(identity) = identity else {
        return Ok(builder.with_no_client_auth());
    };
    let certs = CertificateDer::pem_file_iter(identity.cert)
        .and_then(|iter| iter.collect::<std::result::Result<Vec<_>, _>>())
        .map_err(|e| {
            DriverError::Connection(format!(
                "reading the client certificate {}: {e}",
                identity.cert.display()
            ))
        })?;
    if certs.is_empty() {
        return Err(DriverError::Connection(format!(
            "{} contains no certificates",
            identity.cert.display()
        )));
    }
    // The key's content never goes into a message: only its path.
    let key = PrivateKeyDer::from_pem_file(identity.key).map_err(|e| {
        DriverError::Connection(format!(
            "reading the client key {}: {e}",
            identity.key.display()
        ))
    })?;
    builder
        .with_client_auth_cert(certs, key)
        .map_err(|e| DriverError::Connection(format!("client certificate: {e}")))
}

/// Trusted authorities: the given file or, if none, the system store.
fn roots(ca: Option<&Path>) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    match ca {
        Some(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .and_then(|iter| iter.collect::<std::result::Result<Vec<_>, _>>())
                .map_err(|e| {
                    DriverError::Connection(format!("reading certificate {}: {e}", path.display()))
                })?;
            if certs.is_empty() {
                return Err(DriverError::Connection(format!(
                    "{} contains no certificates",
                    path.display()
                )));
            }
            for cert in certs {
                store.add(cert).map_err(tls_error)?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            store.add_parsable_certificates(native.certs);
            if store.is_empty() {
                return Err(DriverError::Connection(
                    "no trusted certificates found in the system store".into(),
                ));
            }
        }
    }
    Ok(store)
}

fn tls_error(e: impl std::fmt::Display) -> DriverError {
    DriverError::Connection(format!("TLS: {e}"))
}

/// `prefer`/`require`: encrypt without checking who is on the other side, like libpq. The
/// handshake signatures are still verified, so the encryption is real.
#[derive(Debug)]
struct NoVerification(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// `verify-ca`: the chain must be valid, but the name may not match the host.
#[derive(Debug)]
struct IgnoreHostname(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for IgnoreHostname {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}
