//! TLS for `wss://`: configured PEM files, or a persisted self-signed
//! certificate whose subject alternative names follow the relay's settings.
//!
//! The generated certificate's host set and fingerprint are stored alongside
//! the pair so interrupted writes or changed hosts cannot silently reuse the
//! wrong certificate.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{Datelike, Days, Months};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, pki_types::pem::PemObject};

use crate::config::NostrConfig;

/// Details shown to the operator so clients can trust the relay certificate.
#[derive(Debug, Clone)]
pub struct CertInfo {
    /// `"pem"` or `"self-signed"`.
    pub source: &'static str,
    /// PEM file containing the certificate chain.
    pub cert_path: PathBuf,
    /// Uppercase colon-separated SHA-256 of the leaf certificate (DER).
    pub sha256_fingerprint: String,
}

#[derive(Serialize, Deserialize)]
struct SavedHosts {
    hosts: BTreeSet<String>,
    sha256_fingerprint: String,
}

/// `dir` is `<data_dir>/nostr`; the self-signed pair lives in `dir/tls/`.
pub fn acceptor(cfg: &NostrConfig, dir: &Path) -> Result<(TlsAcceptor, CertInfo)> {
    let cert_configured = !cfg.cert_path.trim().is_empty();
    let key_configured = !cfg.key_path.trim().is_empty();
    match (cert_configured, key_configured) {
        (true, true) => {
            let cert_path = Path::new(&cfg.cert_path);
            let key_path = Path::new(&cfg.key_path);
            let (chain, key) = load_pem(cert_path, key_path)?;
            build_acceptor(chain, key, cert_path, key_path, "pem")
        }
        (true, false) | (false, true) => {
            bail!("nostr TLS: cert_path and key_path must be set together")
        }
        (false, false) => self_signed(cfg, dir),
    }
}

fn load_pem(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let chain = CertificateDer::pem_file_iter(cert_path)
        .with_context(|| format!("opening certificate {}", cert_path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certificate {}", cert_path.display()))?;
    if chain.is_empty() {
        bail!("no certificates in {}", cert_path.display());
    }
    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("loading private key {}", key_path.display()))?;
    Ok((chain, key))
}

fn fingerprint(der: &CertificateDer<'_>) -> String {
    Sha256::digest(der.as_ref())
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn build_acceptor(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    cert_path: &Path,
    key_path: &Path,
    source: &'static str,
) -> Result<(TlsAcceptor, CertInfo)> {
    let leaf = chain
        .first()
        .context("nostr TLS: empty certificate chain")?;
    let sha256_fingerprint = fingerprint(leaf);
    let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("nostr TLS: enabling protocol versions")?
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .with_context(|| {
        format!(
            "nostr TLS: invalid certificate {} or key {}",
            cert_path.display(),
            key_path.display()
        )
    })?;
    server.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok((
        TlsAcceptor::from(Arc::new(server)),
        CertInfo {
            source,
            cert_path: cert_path.to_path_buf(),
            sha256_fingerprint,
        },
    ))
}

fn san_hosts(cfg: &NostrConfig) -> Result<(Vec<String>, BTreeSet<String>)> {
    let mut hosts = Vec::new();
    let mut set = BTreeSet::new();
    let mut add = |name: &str| -> Result<()> {
        let name = name.trim();
        if name.is_empty() {
            bail!("nostr TLS: an empty TLS host is not valid");
        }
        let normalized = name
            .parse::<IpAddr>()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| name.to_ascii_lowercase());
        if set.insert(normalized.clone()) {
            hosts.push(normalized);
        }
        Ok(())
    };
    for host in ["localhost", "127.0.0.1", "::1"] {
        add(host)?;
    }
    for host in &cfg.tls_hosts {
        add(host)?;
    }
    let listen_host = cfg
        .listen
        .parse::<SocketAddr>()
        .ok()
        .map(|addr| addr.ip().to_string())
        .or_else(|| {
            cfg.listen.rsplit_once(':').map(|(host, _)| {
                host.trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_string()
            })
        });
    if let Some(host) = listen_host
        && !host.is_empty()
        && !host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified())
    {
        add(&host)?;
    }
    Ok((hosts, set))
}

fn self_signed(cfg: &NostrConfig, dir: &Path) -> Result<(TlsAcceptor, CertInfo)> {
    let tls_dir = dir.join("tls");
    let cert_path = tls_dir.join("cert.pem");
    let key_path = tls_dir.join("key.pem");
    let hosts_path = tls_dir.join("hosts.json");
    let (sans, hosts) = san_hosts(cfg)?;

    let saved = std::fs::read(&hosts_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<SavedHosts>(&bytes).ok());
    if let Some(saved) = saved
        && saved.hosts == hosts
        && let Ok((chain, key)) = load_pem(&cert_path, &key_path)
        && let Ok(result) = build_acceptor(chain, key, &cert_path, &key_path, "self-signed")
        && result.1.sha256_fingerprint == saved.sha256_fingerprint
    {
        return Ok(result);
    }

    // rcgen 0.13 recognizes IP strings and emits iPAddress SANs for them.
    let mut params = rcgen::CertificateParams::new(sans).context("nostr TLS: invalid SAN")?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "mistl nostr relay");
    let start = chrono::Utc::now()
        .date_naive()
        .checked_sub_days(Days::new(1))
        .context("nostr TLS: invalid certificate start date")?;
    let end = start
        .checked_add_months(Months::new(120))
        .context("nostr TLS: invalid certificate end date")?;
    params.not_before = rcgen::date_time_ymd(start.year(), start.month() as u8, start.day() as u8);
    params.not_after = rcgen::date_time_ymd(end.year(), end.month() as u8, end.day() as u8);
    let key = rcgen::KeyPair::generate().context("nostr TLS: generating key")?;
    let cert = params
        .self_signed(&key)
        .context("nostr TLS: generating certificate")?;
    let saved = SavedHosts {
        hosts,
        sha256_fingerprint: fingerprint(cert.der()),
    };
    crate::statefile::write_private(&key_path, key.serialize_pem().as_bytes())
        .with_context(|| format!("writing private key {}", key_path.display()))?;
    crate::statefile::write_bytes(&cert_path, cert.pem().as_bytes())
        .with_context(|| format!("writing certificate {}", cert_path.display()))?;
    crate::statefile::write_bytes(&hosts_path, &serde_json::to_vec_pretty(&saved)?)
        .with_context(|| format!("writing certificate hosts {}", hosts_path.display()))?;
    let (chain, key) = load_pem(&cert_path, &key_path)?;
    build_acceptor(chain, key, &cert_path, &key_path, "self-signed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::ServerName;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            let dir = std::env::temp_dir()
                .join(format!("mistl-nostr-tls-{:016x}", rand::random::<u64>()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn fingerprint_is_uppercase_colon_separated_sha256() {
        let der = CertificateDer::from(b"abc".to_vec());
        assert_eq!(
            fingerprint(&der),
            "BA:78:16:BF:8F:01:CF:EA:41:41:40:DE:5D:AE:22:23:\
             B0:03:61:A3:96:17:7A:9C:B4:10:FF:61:F2:00:15:AD"
        );
    }

    #[test]
    fn generates_reuses_and_regenerates_when_hosts_change() {
        let dir = Scratch::new();
        let mut cfg = NostrConfig::default();
        let (_, first) = acceptor(&cfg, &dir.0).unwrap();
        assert_eq!(first.source, "self-signed");
        assert!(first.cert_path.exists());
        assert_eq!(first.sha256_fingerprint.len(), 95);
        assert_eq!(
            first.sha256_fingerprint,
            acceptor(&cfg, &dir.0).unwrap().1.sha256_fingerprint
        );

        cfg.tls_hosts = vec!["example.com".into(), "192.0.2.1".into()];
        let (_, changed) = acceptor(&cfg, &dir.0).unwrap();
        assert_ne!(first.sha256_fingerprint, changed.sha256_fingerprint);
        cfg.tls_hosts = vec!["192.0.2.1".into(), "EXAMPLE.COM".into(), "localhost".into()];
        assert_eq!(
            changed.sha256_fingerprint,
            acceptor(&cfg, &dir.0).unwrap().1.sha256_fingerprint
        );

        cfg.cert_path = changed.cert_path.to_string_lossy().into_owned();
        cfg.key_path = dir.0.join("tls/key.pem").to_string_lossy().into_owned();
        let (_, loaded) = acceptor(&cfg, &dir.0).unwrap();
        assert_eq!(loaded.source, "pem");
        assert_eq!(loaded.sha256_fingerprint, changed.sha256_fingerprint);
    }

    #[test]
    fn incomplete_or_missing_pem_paths_report_errors() {
        let dir = Scratch::new();
        let mut cfg = NostrConfig::default();
        cfg.cert_path = "missing-cert.pem".into();
        assert!(
            acceptor(&cfg, &dir.0)
                .err()
                .unwrap()
                .to_string()
                .contains("set together")
        );
        cfg.cert_path.clear();
        cfg.key_path = "missing-key.pem".into();
        assert!(
            acceptor(&cfg, &dir.0)
                .err()
                .unwrap()
                .to_string()
                .contains("set together")
        );
        cfg.cert_path = "missing-cert.pem".into();
        assert!(
            format!("{:#}", acceptor(&cfg, &dir.0).err().unwrap()).contains("missing-cert.pem")
        );
    }

    #[test]
    fn corrupt_or_mismatched_pair_is_replaced() {
        let dir = Scratch::new();
        let other = Scratch::new();
        let cfg = NostrConfig::default();
        let first = acceptor(&cfg, &dir.0).unwrap().1;
        acceptor(&cfg, &other.0).unwrap();
        std::fs::copy(other.0.join("tls/key.pem"), dir.0.join("tls/key.pem")).unwrap();
        let second = acceptor(&cfg, &dir.0).unwrap().1;
        assert_ne!(first.sha256_fingerprint, second.sha256_fingerprint);
        std::fs::write(dir.0.join("tls/cert.pem"), "not a certificate").unwrap();
        assert_ne!(
            second.sha256_fingerprint,
            acceptor(&cfg, &dir.0).unwrap().1.sha256_fingerprint
        );
    }

    #[test]
    fn san_hosts_are_deduplicated_and_ignore_unspecified_listeners() {
        let mut cfg = NostrConfig::default();
        cfg.listen = "[::]:7447".into();
        cfg.tls_hosts = vec!["::1".into(), "Example.COM".into(), "example.com".into()];
        let (hosts, _) = san_hosts(&cfg).unwrap();
        assert_eq!(hosts, ["localhost", "127.0.0.1", "::1", "example.com"]);
        cfg.listen = "[2001:db8::1]:7447".into();
        assert!(san_hosts(&cfg).unwrap().0.contains(&"2001:db8::1".into()));
    }

    #[test]
    fn listener_host_changes_regenerate_only_when_sans_change() {
        let dir = Scratch::new();
        let mut cfg = NostrConfig::default();
        cfg.listen = "0.0.0.0:7447".into();
        let original = acceptor(&cfg, &dir.0).unwrap().1;
        cfg.listen = "[::]:7447".into();
        assert_eq!(
            original.sha256_fingerprint,
            acceptor(&cfg, &dir.0).unwrap().1.sha256_fingerprint
        );
        cfg.listen = "192.0.2.2:7447".into();
        assert_ne!(
            original.sha256_fingerprint,
            acceptor(&cfg, &dir.0).unwrap().1.sha256_fingerprint
        );
    }

    #[tokio::test]
    async fn handshake_trusts_generated_cert_and_ip_sans() {
        let dir = Scratch::new();
        let mut cfg = NostrConfig::default();
        cfg.listen = "192.0.2.2:7447".into();
        cfg.tls_hosts = vec!["relay.example".into(), "192.0.2.1".into()];
        let (acceptor, info) = acceptor(&cfg, &dir.0).unwrap();
        let chain = CertificateDer::pem_file_iter(&info.cert_path)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(chain[0].clone()).unwrap();
        let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        let connector = TlsConnector::from(Arc::new(client));
        for name in [
            "localhost",
            "127.0.0.1",
            "::1",
            "relay.example",
            "192.0.2.1",
            "192.0.2.2",
        ] {
            let (client_io, server_io) = tokio::io::duplex(16 * 1024);
            let name = ServerName::try_from(name.to_string()).unwrap();
            let (client, server) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(
                    connector.connect(name, client_io),
                    acceptor.accept(server_io)
                )
            })
            .await
            .unwrap();
            let mut client = client.unwrap();
            let mut server = server.unwrap();
            assert_eq!(
                client.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            client.write_all(b"ping").await.unwrap();
            client.flush().await.unwrap();
            let mut buf = [0; 4];
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
        }
    }
}
