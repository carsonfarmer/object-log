use std::{env, error::Error, net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc};

use object_log::{Log, LogId, Options, ValidatedBackend};
use object_log_git::ObjectFormat;
use object_log_git_http::{GitHttpServer, SharedGitHttpServer, SmartHttp};
use object_store::parse_url_opts;
use tokio::net::TcpListener;
use url::Url;

const DEFAULT_LISTEN: &str = "127.0.0.1:3000";
const DEFAULT_CONCURRENCY: &str = "4";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "object_log_git_http=info".into()),
        )
        .init();

    let store_url = required("OBJECT_LOG_STORE_URL")?.parse::<Url>()?;
    let listen = env::var("OBJECT_LOG_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN.into())
        .parse::<SocketAddr>()?;
    let concurrency = env::var("OBJECT_LOG_CONCURRENCY")
        .unwrap_or_else(|_| DEFAULT_CONCURRENCY.into())
        .parse::<NonZeroUsize>()?;
    let scratch = env::var_os("OBJECT_LOG_SCRATCH")
        .map_or_else(|| env::temp_dir().join("object-log-git"), PathBuf::from);

    let (store, prefix) = parse_url_opts(&store_url, env::vars())?;
    let backend = ValidatedBackend::new(Arc::from(store), prefix).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let format = match env::var("OBJECT_LOG_GIT_FORMAT")
        .as_deref()
        .unwrap_or("sha1")
    {
        "sha1" => ObjectFormat::Sha1,
        "sha256" => ObjectFormat::Sha256,
        _ => return Err("OBJECT_LOG_GIT_FORMAT must be sha1 or sha256".into()),
    };
    let engine = env::var("OBJECT_LOG_GIT_ENGINE").unwrap_or_else(|_| "shared".into());
    let (shared, oracle) = match engine.as_str() {
        "shared" => (Some(SharedGitHttpServer::new(log, format)), None),
        "oracle" if format == ObjectFormat::Sha1 => (
            None,
            Some(GitHttpServer::new(
                SmartHttp::new(log, &scratch),
                scratch,
                concurrency,
            )),
        ),
        _ => return Err("OBJECT_LOG_GIT_ENGINE must be shared, or oracle with sha1".into()),
    };
    let app = if let Some(host) = &shared {
        host.clone().router()
    } else if let Some(host) = &oracle {
        host.clone().router()
    } else {
        return Err("no Git host".into());
    };
    let listener = TcpListener::bind(listen).await?;
    tracing::info!(address = %listener.local_addr()?, "Git HTTP server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    if let Some(host) = shared {
        host.shutdown().await;
    }
    if let Some(host) = oracle {
        host.shutdown().await;
    }
    Ok(())
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("{name} is required").into())
}

async fn shutdown() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install termination handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
