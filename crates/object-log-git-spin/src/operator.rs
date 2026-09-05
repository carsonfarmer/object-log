//! Private command adapter; durable semantics stay in object-log.

use std::{
    ffi::OsString,
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use clap::{Arg, Command};
use object_log::{Log, LogId, Options, Resolution, ValidatedBackend, View};
use object_log_git::{ObjectFormat, Repository};
use object_store::{
    RetryConfig, aws::AmazonS3Builder, client::ClientOptions, path::Path as StorePath,
};
use serde::{Deserialize, Serialize};

const CONFIG_BYTES: usize = 16 * 1024;
const TOKEN_BYTES: usize = 1024 * 1024;
const OUTPUT_BYTES: usize = 2048;
const DEADLINE: Duration = Duration::from_mins(1);
const USAGE: &str = "object-log-git-maintain --config FILE status | resume-commit --token-file FILE | checkpoint --retain-packs";

#[derive(Clone, Copy, Debug)]
struct Failure(&'static str, u8);

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Failure {}

#[derive(Serialize)]
pub(super) struct Report {
    operation: &'static str,
    outcome: &'static str,
    #[serde(skip)]
    exit: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tail_entries: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_through: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<&'static str>,
}

impl Report {
    fn new(operation: &'static str, outcome: &'static str, exit: u8) -> Self {
        Self {
            operation,
            outcome,
            exit,
            generation: None,
            tail_entries: None,
            checkpoint_through: None,
            collection_epoch: None,
            collection_active: None,
            usage: None,
        }
    }

    fn failed(operation: &'static str, failure: Failure) -> Self {
        Self::new(operation, failure.0, failure.1)
    }

    fn observed(mut self, view: &View) -> Self {
        self.generation = Some(view.generation());
        self.tail_entries = Some(view.tail().len());
        self.checkpoint_through = view
            .checkpoint()
            .map(object_log::CheckpointRef::through_sequence);
        self.collection_epoch = Some(view.collection_epoch());
        self.collection_active = Some(view.collection_plan_bytes().is_some());
        self
    }

    pub(super) const fn exit(&self) -> u8 {
        self.exit
    }

    pub(super) fn write(&self, mut output: impl Write) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(self)?;
        if bytes.len() >= OUTPUT_BYTES {
            return Err(std::io::Error::other("operator output limit"));
        }
        bytes.push(b'\n');
        output.write_all(&bytes)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    #[serde(default = "region")]
    region: String,
    #[serde(default = "prefix")]
    prefix: String,
    #[serde(default = "log_id")]
    log_id: String,
    #[serde(default = "object_format")]
    object_format: String,
    #[serde(default = "read_only")]
    read_only: String,
    #[serde(default = "read_only")]
    allow_non_fast_forward: String,
    auth_mode: Option<String>,
    auth_read_token: Option<String>,
    auth_write_token: Option<String>,
}
fn region() -> String {
    "us-east-1".into()
}
fn prefix() -> String {
    "object-log-git".into()
}
fn log_id() -> String {
    "repository".into()
}
fn object_format() -> String {
    "sha1".into()
}
fn read_only() -> String {
    "false".into()
}

impl Config {
    // Config::load rejects every other format before opening storage.
    fn format(&self) -> ObjectFormat {
        if self.object_format == "sha256" {
            ObjectFormat::Sha256
        } else {
            ObjectFormat::Sha1
        }
    }

    fn load(path: &Path) -> Result<Self, Failure> {
        let bytes = read_file(path, CONFIG_BYTES)?;
        let config: Self =
            toml::from_str(std::str::from_utf8(&bytes).map_err(|_| Failure("invalid_config", 2))?)
                .map_err(|_| Failure("invalid_config", 2))?;
        let endpoint =
            url::Url::parse(&config.endpoint).map_err(|_| Failure("invalid_config", 2))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || config.bucket.is_empty()
            || config.access_key.is_empty()
            || config.secret_key.is_empty()
            || config.region.is_empty()
            || config.prefix.is_empty()
            || !matches!(config.object_format.as_str(), "sha1" | "sha256")
            || !matches!(config.read_only.as_str(), "true" | "false")
            || !matches!(config.allow_non_fast_forward.as_str(), "true" | "false")
            || LogId::new(&config.log_id).is_err()
            || StorePath::parse(&config.prefix).is_err()
        {
            return Err(Failure("invalid_config", 2));
        }
        // Storage-only operator configs need no HTTP credentials. Explicit HTTP
        // settings use exactly the serving parser and cannot be silently ignored.
        if config.auth_mode.is_some()
            || config.auth_read_token.is_some()
            || config.auth_write_token.is_some()
        {
            super::auth::AuthConfig::parse(
                config.auth_mode.as_deref().unwrap_or("basic"),
                config.auth_read_token.as_deref().unwrap_or(""),
                config.auth_write_token.as_deref().unwrap_or(""),
            )
            .map_err(|_| Failure("invalid_config", 2))?;
        }
        Ok(config)
    }

    async fn open(&self) -> Result<Log, Failure> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_endpoint(&self.endpoint)
            .with_virtual_hosted_style_request(false)
            .with_disable_bulk_delete(false)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(self.endpoint.starts_with("http://"))
                    .with_connect_timeout(Duration::from_secs(5))
                    .with_timeout(Duration::from_secs(30)),
            )
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .build()
            .map_err(|_| Failure("invalid_config", 2))?;
        let backend = ValidatedBackend::new(Arc::new(store), StorePath::from(self.prefix.clone()))
            .await
            .map_err(|error| classify(&error))?;
        let id = LogId::new(&self.log_id).map_err(|error| classify(&error))?;
        Log::open_existing(&backend, &id, Options::default())
            .await
            .map_err(|error| classify(&error))
    }
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>, Failure> {
    // Opening a FIFO must not hang before we can inspect the opened handle.
    // Reject final symlinks as well; only private, regular input files are supported.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| Failure("input_unavailable", 2))?;
    let metadata = file
        .metadata()
        .map_err(|_| Failure("input_unavailable", 2))?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(Failure("input_limit", 2));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Failure("input_not_private", 2));
    }
    // Read at most limit+1 even if the regular file grows after metadata().
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure("input_unavailable", 2))?;
    if bytes.len() > limit {
        return Err(Failure("input_limit", 2));
    }
    Ok(bytes)
}

struct Request {
    config: PathBuf,
    action: Action,
}
enum Action {
    Status,
    Resume(Vec<u8>),
    Checkpoint,
}
impl Action {
    fn name(&self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Resume(_) => "resume-commit",
            Self::Checkpoint => "checkpoint",
        }
    }
}

fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Request, Failure> {
    let parsed = Command::new("object-log-git-maintain")
        .subcommand_required(true)
        .arg(
            Arg::new("config")
                .long("config")
                .required(true)
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .subcommand(Command::new("status"))
        .subcommand(
            Command::new("checkpoint").arg(
                Arg::new("retain-packs")
                    .long("retain-packs")
                    .action(clap::ArgAction::SetTrue)
                    .required(true),
            ),
        )
        .subcommand(
            Command::new("resume-commit").arg(
                Arg::new("token-file")
                    .long("token-file")
                    .required(true)
                    .value_parser(clap::value_parser!(PathBuf)),
            ),
        )
        .try_get_matches_from(arguments)
        .map_err(|error| {
            if error.kind() == clap::error::ErrorKind::DisplayHelp {
                Failure("usage", 0)
            } else {
                Failure("invalid_arguments", 2)
            }
        })?;
    let config = parsed
        .get_one::<PathBuf>("config")
        .cloned()
        .ok_or(Failure("invalid_arguments", 2))?;
    let action = match parsed.subcommand() {
        Some(("status", _)) => Action::Status,
        Some(("checkpoint", _)) => Action::Checkpoint,
        Some(("resume-commit", subcommand)) => {
            let path = subcommand
                .get_one::<PathBuf>("token-file")
                .ok_or(Failure("invalid_arguments", 2))?;
            Action::Resume(read_file(path, TOKEN_BYTES)?)
        }
        _ => return Err(Failure("invalid_arguments", 2)),
    };
    Ok(Request { config, action })
}

fn classify(error: &object_log::Error) -> Failure {
    match error {
        object_log::Error::Store(_) => Failure("backend_unavailable", 4),
        object_log::Error::ConfigurationMismatch(_) | object_log::Error::InvalidLogId => {
            Failure("configuration_mismatch", 2)
        }
        object_log::Error::LimitExceeded(_) => Failure("resource_limit", 5),
        object_log::Error::CollectionFence => Failure("collection_fenced", 5),
        object_log::Error::ViewExpired => Failure("view_expired", 5),
        object_log::Error::UnsupportedBackend(_) => Failure("unsupported_backend", 5),
        _ => Failure("invalid_evidence", 5),
    }
}

async fn execute(log: &Log, action: &Action, format: ObjectFormat) -> Report {
    match action {
        Action::Checkpoint => match Repository::checkpoint_retaining_packs(log, format).await {
            Ok(object_log::CheckpointStatus::Published(view)) => {
                Report::new(action.name(), "checkpointed", 0).observed(&view)
            }
            Ok(object_log::CheckpointStatus::Conflict(view)) => {
                Report::new(action.name(), "conflict", 3).observed(&view)
            }
            // Do not resolve after the shared helper's cumulative budget is dropped.
            Ok(object_log::CheckpointStatus::Pending(_)) => {
                Report::new(action.name(), "pending", 4)
            }
            Err(object_log_git::Error::ObjectLog(error)) => {
                Report::failed(action.name(), classify(&error))
            }
            Err(object_log_git::Error::Busy) => Report::new(action.name(), "busy", 3),
            Err(_) => Report::new(action.name(), "invalid_git_state_or_limit", 5),
        },
        Action::Status => match log.load().await {
            Ok(view) => Report::new(action.name(), "observed", 0).observed(&view),
            Err(error) => Report::failed(action.name(), classify(&error)),
        },
        Action::Resume(token) => match log.resume(token).await {
            Ok(Resolution::Committed(view)) => {
                Report::new(action.name(), "committed", 0).observed(&view)
            }
            Ok(Resolution::NotCommitted(view)) => {
                Report::new(action.name(), "not_committed", 0).observed(&view)
            }
            Ok(Resolution::StillPending(_)) | Err(object_log::Error::Store(_)) => {
                Report::new(action.name(), "pending", 4)
            }
            Ok(Resolution::Expired(view)) => {
                Report::new(action.name(), "expired", 4).observed(&view)
            }
            Err(error) => Report::failed(action.name(), classify(&error)),
        },
    }
}

pub(super) fn run(arguments: impl IntoIterator<Item = OsString>) -> Report {
    let request = match parse(arguments) {
        Ok(request) => request,
        Err(failure) => {
            let mut report = Report::failed("input", failure);
            report.usage = Some(USAGE);
            return report;
        }
    };
    let operation = request.action.name();
    let config = match Config::load(&request.config) {
        Ok(config) => config,
        Err(failure) => return Report::failed(operation, failure),
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return Report::new(operation, "runtime_unavailable", 5);
    };
    runtime.block_on(async {
        let work = async {
            match config.open().await {
                Ok(log) => execute(&log, &request.action, config.format()).await,
                Err(failure) => Report::failed(operation, failure),
            }
        };
        bounded(operation, DEADLINE, work).await
    })
}

async fn bounded(
    operation: &'static str,
    deadline: Duration,
    work: impl std::future::Future<Output = Report>,
) -> Report {
    tokio::time::timeout(deadline, work)
        .await
        .unwrap_or_else(|_| {
            Report::new(
                operation,
                if matches!(operation, "resume-commit" | "checkpoint") {
                    "pending"
                } else {
                    "backend_unavailable"
                },
                4,
            )
        })
}

#[cfg(test)]
#[path = "operator_tests.rs"]
mod tests;
