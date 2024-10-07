use std::env::{self, VarError};
use std::ffi::OsStr;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Attempts to get the current Unix timestamp in seconds.
///
/// # Returns
///
/// - `Ok(u64)`: The current Unix timestamp in seconds.
/// - `Err(std::time::SystemTimeError)`: If the system time is before UNIX_EPOCH.
pub fn try_unix_timestamp() -> Result<u64, std::time::SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
}

/// Initializes tracing with a pretty print format for the console.
pub fn init_tracing() {
    let subscriber = tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(false);

    let filter_layer = EnvFilter::from_default_env();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(subscriber)
        .init();
}

#[derive(Debug)]
pub enum EnvVarError {
    Parse {
        value: String,
        expected_type: String,
    },
    VarError(VarError),
}

impl std::fmt::Display for EnvVarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvVarError::Parse {
                value,
                expected_type,
            } => {
                write!(f, "Could not parse {} as {}", value, expected_type)
            }
            EnvVarError::VarError(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for EnvVarError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EnvVarError::Parse { .. } => None,
            EnvVarError::VarError(err) => Some(err),
        }
    }
}

pub fn env_or_else<K, V, F>(key: K, default: F) -> Result<V, EnvVarError>
where
    K: AsRef<OsStr>,
    V: FromStr,
    V::Err: std::fmt::Debug,
    F: FnOnce() -> V,
{
    match env::var(&key) {
        Ok(val) => val.parse().map_err(|_| EnvVarError::Parse {
            value: key.as_ref().to_string_lossy().into_owned(),
            expected_type: std::any::type_name::<V>().to_string(),
        }),
        Err(VarError::NotPresent) => Ok(default()),
        Err(e) => Err(EnvVarError::VarError(e)),
    }
}

pub fn env_or<K, V>(key: K, default: V) -> Result<V, EnvVarError>
where
    K: AsRef<OsStr>,
    V: FromStr,
    V::Err: std::fmt::Debug,
{
    env_or_else(key, || default)
}
