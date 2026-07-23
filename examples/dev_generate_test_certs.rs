//! Regenerates the mTLS fixtures under `tests/fixtures/tls/`: a CA, a server cert (SAN
//! `127.0.0.1`, for the embedded test broker) and a client cert (for `AuthConfig::Mtls`), both
//! signed by that CA. Pure-Rust (rcgen/rustls), no `openssl` involved. These certs are
//! test-only fixtures committed to version control -- never used for anything production:
//!
//!   cargo run --example dev_generate_test_certs

use std::path::Path;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};

const OUT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls");

fn main() {
    let (ca_cert, issuer) = new_ca();
    let (server_cert, server_key) = new_leaf(
        &issuer,
        "127.0.0.1",
        "rosetta-mq-test-server",
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (client_cert, client_key) = new_leaf(
        &issuer,
        "rosetta-mq-test-client",
        "rosetta-mq-test-client",
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    let out_dir = Path::new(OUT_DIR);
    std::fs::create_dir_all(out_dir).unwrap();
    write(out_dir, "ca.pem", &ca_cert.pem());
    write(out_dir, "server-cert.pem", &server_cert.pem());
    write(out_dir, "server-key.pem", &server_key.serialize_pem());
    write(out_dir, "client-cert.pem", &client_cert.pem());
    write(out_dir, "client-key.pem", &client_key.serialize_pem());

    println!("wrote CA + server + client fixtures to {OUT_DIR}");
}

fn new_ca() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut params =
        CertificateParams::new(Vec::default()).expect("empty subject alt name can't produce error");
    let (yesterday, tomorrow) = validity_period();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "rosetta-mq test CA");
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::CrlSign);
    params.not_before = yesterday;
    params.not_after = tomorrow;

    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert, Issuer::new(params, key_pair))
}

/// `san` is the subject alt name the peer validates the cert against (an IP for the server,
/// since the test broker is dialed at `127.0.0.1`; an arbitrary name for the client, which
/// `rumqttc`/`rumqttd` don't hostname-check).
fn new_leaf(
    issuer: &Issuer<'static, KeyPair>,
    san: &str,
    common_name: &str,
    eku: ExtendedKeyUsagePurpose,
) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::new(vec![san.into()]).expect("valid SAN");
    let (yesterday, tomorrow) = validity_period();
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.use_authority_key_identifier_extension = true;
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.extended_key_usages.push(eku);
    params.not_before = yesterday;
    params.not_after = tomorrow;

    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, issuer).unwrap();
    (cert, key_pair)
}

fn validity_period() -> (OffsetDateTime, OffsetDateTime) {
    // Ten years -- these are committed fixtures, not rotated certs, so they should outlive
    // however long this repo goes between regenerations.
    let span = Duration::new(10 * 365 * 86400, 0);
    let yesterday = OffsetDateTime::now_utc().checked_sub(Duration::new(86400, 0)).unwrap();
    let far_future = OffsetDateTime::now_utc().checked_add(span).unwrap();
    (yesterday, far_future)
}

fn write(dir: &Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).unwrap();
}
