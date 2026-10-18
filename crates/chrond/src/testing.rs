//! Certificates for tests (feature `test-util`): a CA, a server
//! certificate for `localhost` and `127.0.0.1`, a client certificate, and
//! a client certificate from an unrelated CA.

use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};

pub struct Pki {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
    pub other_ca_client_cert_pem: String,
    pub other_ca_client_key_pem: String,
}

pub struct PkiPaths {
    pub ca: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

fn ca() -> (rcgen::Certificate, KeyPair) {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    (params.self_signed(&key).expect("ca"), key)
}

fn leaf(
    sans: &[&str],
    purpose: ExtendedKeyUsagePurpose,
    ca: &(rcgen::Certificate, KeyPair),
) -> (String, String) {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .expect("params");
    params.extended_key_usages = vec![purpose];
    let cert = params.signed_by(&key, &ca.0, &ca.1).expect("leaf");
    (cert.pem(), key.serialize_pem())
}

/// Fresh certificates every call.
pub fn pki() -> Pki {
    let root = ca();
    let other = ca();
    let (server_cert_pem, server_key_pem) = leaf(
        &["localhost", "127.0.0.1"],
        ExtendedKeyUsagePurpose::ServerAuth,
        &root,
    );
    let (client_cert_pem, client_key_pem) =
        leaf(&["client"], ExtendedKeyUsagePurpose::ClientAuth, &root);
    let (other_ca_client_cert_pem, other_ca_client_key_pem) =
        leaf(&["client"], ExtendedKeyUsagePurpose::ClientAuth, &other);
    Pki {
        ca_pem: root.0.pem(),
        server_cert_pem,
        server_key_pem,
        client_cert_pem,
        client_key_pem,
        other_ca_client_cert_pem,
        other_ca_client_key_pem,
    }
}

impl Pki {
    /// Writes `ca.pem`, `server.pem`, `server.key`, `client.pem`,
    /// `client.key` into `dir`.
    pub fn write_to(&self, dir: &Path) -> PkiPaths {
        let paths = PkiPaths {
            ca: dir.join("ca.pem"),
            server_cert: dir.join("server.pem"),
            server_key: dir.join("server.key"),
            client_cert: dir.join("client.pem"),
            client_key: dir.join("client.key"),
        };
        std::fs::write(&paths.ca, &self.ca_pem).expect("write");
        std::fs::write(&paths.server_cert, &self.server_cert_pem).expect("write");
        std::fs::write(&paths.server_key, &self.server_key_pem).expect("write");
        std::fs::write(&paths.client_cert, &self.client_cert_pem).expect("write");
        std::fs::write(&paths.client_key, &self.client_key_pem).expect("write");
        paths
    }
}
