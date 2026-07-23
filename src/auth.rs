use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// Per-broker auth configuration, discriminated by the `method` field in TOML (e.g.
/// `method = "mtls"`, plus that variant's own fields as siblings -- same "internally tagged"
/// convention as [`crate::decoder::DecoderConfig`]). A single enum field makes the two methods
/// mutually exclusive by construction: TOML can't set both at once.
#[derive(Debug, Deserialize)]
#[serde(tag = "method")]
pub enum AuthConfig {
    #[serde(rename = "mtls")]
    Mtls(MtlsConfig),
    #[serde(rename = "userpass")]
    UserPass(UserPassConfig),
}

#[derive(Debug, Deserialize)]
pub struct MtlsConfig {
    pub ca_file: PathBuf,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct UserPassConfig {
    pub username: String,
    pub password: PasswordSource,
}

/// Untagged so a plain TOML string picks the literal password, and a `{ env = "..." }` inline
/// table picks the env-var indirection -- mutually exclusive by construction (a single field),
/// not by hand-validating two optional fields. The env-var form exists so a real password
/// doesn't have to live in a config file that might get committed.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum PasswordSource {
    Env {
        env: String,
    },
    Literal(String),
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("failed to read {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read env var {name:?} for password: {source}")]
    EnvVar {
        name: String,
        #[source]
        source: std::env::VarError,
    },
}

/// The resolved, runtime form of [`AuthConfig`]: certs/keys already read off disk, env vars
/// already resolved. `Client::connect` consumes this directly and never touches the filesystem
/// or environment itself -- all fallible I/O happens once, up front, via [`AuthConfig::build`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedAuth {
    None,
    Mtls {
        ca: Vec<u8>,
        cert: Vec<u8>,
        key: Vec<u8>,
    },
    UserPass {
        username: String,
        password: String,
    },
}

impl AuthConfig {
    pub fn build(&self, base_dir: &Path) -> Result<ResolvedAuth, AuthError> {
        match self {
            AuthConfig::Mtls(cfg) => Ok(ResolvedAuth::Mtls {
                ca: read(base_dir, &cfg.ca_file)?,
                cert: read(base_dir, &cfg.cert_file)?,
                key: read(base_dir, &cfg.key_file)?,
            }),
            AuthConfig::UserPass(cfg) => {
                let password = match &cfg.password {
                    PasswordSource::Literal(password) => password.clone(),
                    PasswordSource::Env { env: name } => {
                        std::env::var(name).map_err(|source| AuthError::EnvVar {
                            name: name.clone(),
                            source,
                        })?
                    }
                };
                Ok(ResolvedAuth::UserPass {
                    username: cfg.username.clone(),
                    password,
                })
            }
        }
    }
}

fn read(base_dir: &Path, file: &Path) -> Result<Vec<u8>, AuthError> {
    let path = crate::util::resolve_path(base_dir, file);
    std::fs::read(&path).map_err(|source| AuthError::Io { path, source })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> AuthConfig {
        toml::from_str(raw).unwrap()
    }

    #[test]
    fn parses_valid_mtls_config() {
        let cfg = parse(
            r#"
            method = "mtls"
            ca_file = "ca.pem"
            cert_file = "client.pem"
            key_file = "client.key"
        "#,
        );
        match cfg {
            AuthConfig::Mtls(cfg) => {
                assert_eq!(cfg.ca_file, Path::new("ca.pem"));
                assert_eq!(cfg.cert_file, Path::new("client.pem"));
                assert_eq!(cfg.key_file, Path::new("client.key"));
            }
            other => panic!("expected mtls config, got {other:?}"),
        }
    }

    #[test]
    fn parses_userpass_with_literal_password() {
        let cfg = parse(
            r#"
            method = "userpass"
            username = "device-reader"
            password = "supersecret"
        "#,
        );
        match cfg {
            AuthConfig::UserPass(cfg) => {
                assert_eq!(cfg.username, "device-reader");
                assert!(matches!(cfg.password, PasswordSource::Literal(p) if p == "supersecret"));
            }
            other => panic!("expected userpass config, got {other:?}"),
        }
    }

    #[test]
    fn parses_userpass_with_env_password() {
        let cfg = parse(
            r#"
            method = "userpass"
            username = "device-reader"
            password = { env = "MQTT_PASSWORD" }
        "#,
        );
        match cfg {
            AuthConfig::UserPass(cfg) => {
                assert!(
                    matches!(cfg.password, PasswordSource::Env { env } if env == "MQTT_PASSWORD")
                );
            }
            other => panic!("expected userpass config, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_method() {
        let err: Result<AuthConfig, _> = toml::from_str(
            r#"
            method = "oauth"
        "#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn rejects_mtls_missing_required_field() {
        let err: Result<AuthConfig, _> = toml::from_str(
            r#"
            method = "mtls"
            ca_file = "ca.pem"
            cert_file = "client.pem"
        "#,
        );
        assert!(err.is_err());
    }

    #[test]
    fn build_mtls_reads_cert_bytes_relative_to_base_dir() {
        let dir = tempdir();
        std::fs::write(dir.join("ca.pem"), b"ca-bytes").unwrap();
        std::fs::write(dir.join("client.pem"), b"cert-bytes").unwrap();
        std::fs::write(dir.join("client.key"), b"key-bytes").unwrap();

        let cfg = AuthConfig::Mtls(MtlsConfig {
            ca_file: PathBuf::from("ca.pem"),
            cert_file: PathBuf::from("client.pem"),
            key_file: PathBuf::from("client.key"),
        });

        let resolved = cfg.build(&dir).unwrap();
        assert_eq!(
            resolved,
            ResolvedAuth::Mtls {
                ca: b"ca-bytes".to_vec(),
                cert: b"cert-bytes".to_vec(),
                key: b"key-bytes".to_vec(),
            }
        );
    }

    #[test]
    fn build_mtls_fails_on_missing_file() {
        let dir = tempdir();
        let cfg = AuthConfig::Mtls(MtlsConfig {
            ca_file: PathBuf::from("does-not-exist.pem"),
            cert_file: PathBuf::from("client.pem"),
            key_file: PathBuf::from("client.key"),
        });

        let err = cfg.build(&dir).unwrap_err();
        assert!(matches!(err, AuthError::Io { .. }));
    }

    #[test]
    fn build_userpass_resolves_literal_password() {
        let cfg = AuthConfig::UserPass(UserPassConfig {
            username: "device-reader".to_string(),
            password: PasswordSource::Literal("supersecret".to_string()),
        });

        let resolved = cfg.build(Path::new(".")).unwrap();
        assert_eq!(
            resolved,
            ResolvedAuth::UserPass {
                username: "device-reader".to_string(),
                password: "supersecret".to_string(),
            }
        );
    }

    #[test]
    fn build_userpass_resolves_password_from_env() {
        // Distinctively-named var: cargo test runs tests in parallel within one process, so a
        // generic name here could collide with another test's env mutation.
        let name = "ROSETTA_MQ_TEST_BUILD_USERPASS_RESOLVES_PASSWORD_FROM_ENV";
        // SAFETY: single-threaded with respect to this specific env var -- no other test reads
        // or writes this distinctively-named key.
        unsafe {
            std::env::set_var(name, "env-secret");
        }

        let cfg = AuthConfig::UserPass(UserPassConfig {
            username: "device-reader".to_string(),
            password: PasswordSource::Env {
                env: name.to_string(),
            },
        });

        let resolved = cfg.build(Path::new(".")).unwrap();

        unsafe {
            std::env::remove_var(name);
        }

        assert_eq!(
            resolved,
            ResolvedAuth::UserPass {
                username: "device-reader".to_string(),
                password: "env-secret".to_string(),
            }
        );
    }

    #[test]
    fn build_userpass_fails_on_missing_env_var() {
        let name = "ROSETTA_MQ_TEST_BUILD_USERPASS_FAILS_ON_MISSING_ENV_VAR";
        // SAFETY: distinctively-named var, not read/written by any other test.
        unsafe {
            std::env::remove_var(name);
        }

        let cfg = AuthConfig::UserPass(UserPassConfig {
            username: "device-reader".to_string(),
            password: PasswordSource::Env {
                env: name.to_string(),
            },
        });

        let err = cfg.build(Path::new(".")).unwrap_err();
        assert!(matches!(err, AuthError::EnvVar { .. }));
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rosetta-mq-auth-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
