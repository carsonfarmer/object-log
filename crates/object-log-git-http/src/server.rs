//! Native HTTP host for one fixed Git repository.

use std::task::{Context, Poll};
use std::{error::Error as StdError, io, path::PathBuf, pin::Pin, sync::Arc};
use std::{num::NonZeroUsize, time::Duration};

use async_compression::tokio::bufread::GzipDecoder;
use axum::{
    Router,
    body::Body,
    extract::{RawQuery, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, EXPIRES, PRAGMA},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::TryStreamExt;
use tokio::{
    io::{AsyncRead, AsyncSeekExt, BufReader, ReadBuf, SeekFrom},
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
};
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::task::TaskTracker;
use tower_http::{
    limit::RequestBodyLimitLayer,
    timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer},
};

use crate::{Error, ReceiveOutcome, Service, SmartHttp};

const MAX_ENCODED_BODY_BYTES: usize = 513 * 1024 * 1024;
const REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const RESPONSE_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const PRAGMA_NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");
const EXPIRES_PAST: HeaderValue = HeaderValue::from_static("Fri, 01 Jan 1980 00:00:00 GMT");

pub(super) type RequestReader = Pin<Box<dyn AsyncRead + Send>>;

/// A native HTTP host for one object-log-backed repository at `/repo`.
///
/// The host owns no durable state. Its scratch directory can be empty at
/// startup and discarded at any time.
#[derive(Clone, Debug)]
pub struct GitHttpServer {
    endpoint: SmartHttp,
    scratch: PathBuf,
    permits: Arc<Semaphore>,
    tasks: TaskTracker,
}

impl GitHttpServer {
    /// Creates a host with a fixed limit on active Git operations.
    ///
    #[must_use]
    pub fn new(
        endpoint: SmartHttp,
        scratch: impl Into<PathBuf>,
        max_concurrency: NonZeroUsize,
    ) -> Self {
        Self {
            endpoint,
            scratch: scratch.into(),
            permits: Arc::new(Semaphore::new(max_concurrency.get())),
            tasks: TaskTracker::new(),
        }
    }

    /// Returns the complete router for the fixed `/repo` mapping.
    pub fn router(self) -> Router {
        Router::new()
            .route("/repo/info/refs", get(info_refs))
            .route("/repo/git-upload-pack", post(upload_pack))
            .route("/repo/git-receive-pack", post(receive_pack))
            .layer(ResponseBodyTimeoutLayer::new(RESPONSE_IDLE_TIMEOUT))
            .layer(RequestBodyTimeoutLayer::new(REQUEST_BODY_IDLE_TIMEOUT))
            .layer(RequestBodyLimitLayer::new(MAX_ENCODED_BODY_BYTES))
            .with_state(self)
    }

    /// Waits for detached Git operations after the router has stopped.
    pub async fn shutdown(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    fn permit(&self) -> Result<OwnedSemaphorePermit, Failure> {
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|error| match error {
                TryAcquireError::NoPermits => Failure::busy(),
                TryAcquireError::Closed => Failure::internal(),
            })
    }
}

async fn info_refs(
    State(host): State<GitHttpServer>,
    RawQuery(query): RawQuery,
) -> Result<Response, Failure> {
    let service = parse_service(query.as_deref())?;
    let permit = host.permit()?;
    let endpoint = host.endpoint.clone();
    let output = host
        .tasks
        .spawn(async move {
            let _permit = permit;
            let mut output = Vec::new();
            endpoint.advertise(service, &mut output).await?;
            Ok::<_, Error>(output)
        })
        .await
        .map_err(|_| Failure::internal())??;
    Ok(response(service, true, StatusCode::OK, Body::from(output)))
}

async fn upload_pack(
    State(host): State<GitHttpServer>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Failure> {
    let encoding = require_request_headers(&headers, Service::UploadPack)?;
    let mut input = request_reader(body, encoding);
    let permit = host.permit()?;
    let endpoint = host.endpoint.clone();
    let scratch = host.scratch.clone();
    let (output, permit) = host
        .tasks
        .spawn(async move {
            tokio::fs::create_dir_all(&scratch).await?;
            let output = tempfile::tempfile_in(scratch)?;
            let mut output = tokio::fs::File::from_std(output);
            endpoint.upload_pack(&mut input, &mut output).await?;
            output.seek(SeekFrom::Start(0)).await?;
            Ok::<_, Error>((output, permit))
        })
        .await
        .map_err(|_| Failure::internal())??;
    let body = Body::from_stream(ReaderStream::new(PermittedReader {
        inner: output,
        _permit: permit,
    }));
    Ok(response(Service::UploadPack, false, StatusCode::OK, body))
}

struct PermittedReader<R> {
    inner: R,
    _permit: OwnedSemaphorePermit,
}

impl<R: AsyncRead + Unpin> AsyncRead for PermittedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

async fn receive_pack(
    State(host): State<GitHttpServer>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Failure> {
    let encoding = require_request_headers(&headers, Service::ReceivePack)?;
    let mut input = request_reader(body, encoding);
    let permit = host.permit()?;
    let endpoint = host.endpoint.clone();
    let (outcome, output) = host
        .tasks
        .spawn(async move {
            let _permit = permit;
            let mut output = Vec::new();
            let outcome = endpoint.receive_pack(&mut input, &mut output).await?;
            Ok::<_, Error>((outcome, output))
        })
        .await
        .map_err(|_| Failure::internal())??;
    let status = match &outcome {
        ReceiveOutcome::Committed | ReceiveOutcome::Rejected => StatusCode::OK,
        ReceiveOutcome::Pending(_) => {
            tracing::warn!("Git publication remains uncertain; client must refresh refs");
            StatusCode::SERVICE_UNAVAILABLE
        }
        ReceiveOutcome::Expired => {
            tracing::warn!("Git publication evidence expired; client must refresh refs");
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    Ok(response(
        Service::ReceivePack,
        false,
        status,
        Body::from(output),
    ))
}

pub(super) fn request_reader(body: Body, encoding: Encoding) -> RequestReader {
    let stream = body.into_data_stream().map_err(body_error);
    let reader = BufReader::new(StreamReader::new(stream));
    match encoding {
        Encoding::Identity => Box::pin(reader),
        Encoding::Gzip => {
            let mut decoder = GzipDecoder::new(reader);
            decoder.multiple_members(true);
            Box::pin(decoder)
        }
    }
}

fn body_error(error: axum::Error) -> io::Error {
    let mut source = error.source();
    let mut kind = io::ErrorKind::InvalidData;
    while let Some(current) = source {
        if current.is::<http_body_util::LengthLimitError>() {
            kind = io::ErrorKind::FileTooLarge;
            break;
        }
        if current.is::<tower_http::timeout::TimeoutError>() {
            kind = io::ErrorKind::TimedOut;
            break;
        }
        source = current.source();
    }
    io::Error::new(kind, error)
}

pub(super) fn parse_service(query: Option<&str>) -> Result<Service, Failure> {
    match query {
        Some("service=git-upload-pack") => Ok(Service::UploadPack),
        Some("service=git-receive-pack") => Ok(Service::ReceivePack),
        _ => Err(Failure::new(StatusCode::BAD_REQUEST, "invalid Git service")),
    }
}

pub(super) fn require_request_headers(
    headers: &HeaderMap,
    service: Service,
) -> Result<Encoding, Failure> {
    let expected = match service {
        Service::UploadPack => "application/x-git-upload-pack-request",
        Service::ReceivePack => "application/x-git-receive-pack-request",
    };
    let mut content_types = headers.get_all(CONTENT_TYPE).iter();
    let content_type = content_types
        .next()
        .ok_or_else(|| Failure::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "missing content type"))?;
    if content_types.next().is_some() {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "repeated content type",
        ));
    }
    if !content_type
        .as_bytes()
        .eq_ignore_ascii_case(expected.as_bytes())
    {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported Git content type",
        ));
    }
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Ok(Encoding::Identity);
    };
    if values.next().is_some() {
        return Err(Failure::unsupported_encoding());
    }
    if value.as_bytes().eq_ignore_ascii_case(b"identity") {
        Ok(Encoding::Identity)
    } else if value.as_bytes().eq_ignore_ascii_case(b"gzip") {
        Ok(Encoding::Gzip)
    } else {
        Err(Failure::unsupported_encoding())
    }
}

#[derive(Clone, Copy)]
pub(super) enum Encoding {
    Identity,
    Gzip,
}

pub(super) fn response(
    service: Service,
    advertisement: bool,
    status: StatusCode,
    body: Body,
) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    let content_type = if advertisement {
        service.advertisement_content_type()
    } else {
        service.result_content_type()
    };
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(service.cache_control()),
    );
    headers.insert(PRAGMA, PRAGMA_NO_CACHE);
    headers.insert(EXPIRES, EXPIRES_PAST);
    response
}

#[derive(Debug)]
pub(super) struct Failure {
    status: StatusCode,
    message: &'static str,
}

impl Failure {
    pub(super) const fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    pub(super) const fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal Git service error",
        )
    }

    const fn unsupported_encoding() -> Self {
        Self::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported content encoding",
        )
    }

    pub(super) const fn busy() -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "Git service is busy")
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::Git(object_log_git::Error::Busy) => Self::busy(),
            Error::Git(object_log_git::Error::InvalidProtocol(_)) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid Git protocol")
            }
            Error::Protocol(_) => Self::new(StatusCode::BAD_REQUEST, "invalid Git protocol"),
            Error::RequestTooLarge(_) => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large")
            }
            Error::Io(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                Self::new(StatusCode::BAD_REQUEST, "truncated request body")
            }
            Error::Io(error) if error.kind() == io::ErrorKind::TimedOut => {
                Self::new(StatusCode::REQUEST_TIMEOUT, "request body timed out")
            }
            Error::Io(error) if error.kind() == io::ErrorKind::InvalidData => {
                Self::new(StatusCode::BAD_REQUEST, "invalid request body")
            }
            Error::Io(error) if error.kind() == io::ErrorKind::FileTooLarge => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large")
            }
            error => {
                tracing::error!(error = %error, "Git service failed");
                Self::internal()
            }
        }
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (
            self.status,
            [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            self.message,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use axum::http::Request;
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use object_log::{Log, LogId, Options, ValidatedBackend};
    use object_store::{memory::InMemory, path::Path as StorePath};
    use tokio::io::AsyncReadExt;
    use tower::ServiceExt;

    use super::*;
    use crate::MAX_COMMANDS;

    #[tokio::test]
    async fn response_reader_owns_its_concurrency_permit() -> Result<(), Box<dyn StdError>> {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).acquire_owned().await?;
        let mut reader = PermittedReader {
            inner: tokio::io::empty(),
            _permit: permit,
        };
        assert_eq!(permits.available_permits(), 0);

        assert_eq!(reader.read(&mut [0]).await?, 0);
        assert_eq!(permits.available_permits(), 0);
        drop(reader);
        assert_eq!(permits.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn routes_apply_git_metadata_and_client_error_statuses() -> Result<(), Box<dyn StdError>>
    {
        let (server, _scratch) = test_server().await?;
        let app = server.router();
        let response = app
            .clone()
            .oneshot(Request::get("/repo/info/refs?service=git-upload-pack").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE),
            Some(&HeaderValue::from_static(
                "application/x-git-upload-pack-advertisement"
            ))
        );
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&HeaderValue::from_static(
                "no-cache, max-age=0, must-revalidate"
            ))
        );
        assert_eq!(response.headers().get(PRAGMA), Some(&PRAGMA_NO_CACHE));
        assert_eq!(response.headers().get(EXPIRES), Some(&EXPIRES_PAST));

        let response = app
            .clone()
            .oneshot(Request::get("/repo/info/refs?service=bad").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(Request::post("/repo/git-upload-pack").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let response = app
            .oneshot(
                Request::post("/repo/git-upload-pack")
                    .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                    .body(Body::from("0008"))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn streamed_request_and_protocol_limits_are_enforced() -> Result<(), Box<dyn StdError>> {
        let (server, _scratch) = test_server().await?;
        let app = server.router();
        let id = "11".repeat(20);
        let mut negotiation = packet(format!("want {id}\n").as_bytes());
        negotiation.extend_from_slice(b"00000000");
        let split = negotiation.len() / 2;
        let body = Body::from_stream(futures::stream::iter([
            Ok::<_, Infallible>(Bytes::copy_from_slice(&negotiation[..split])),
            Ok(Bytes::copy_from_slice(&negotiation[split..])),
        ]));
        let response = app
            .clone()
            .oneshot(
                Request::post("/repo/git-upload-pack")
                    .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                    .body(body)?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await?.to_bytes(),
            "0008NAK\n"
        );

        let want = packet(format!("want {id}\n").as_bytes());
        let mut too_many = Vec::with_capacity(want.len() * (MAX_COMMANDS + 1));
        for _ in 0..=MAX_COMMANDS {
            too_many.extend_from_slice(&want);
        }
        let response = app
            .clone()
            .oneshot(
                Request::post("/repo/git-upload-pack")
                    .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                    .body(Body::from(too_many))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        Ok(())
    }

    async fn test_server() -> Result<(GitHttpServer, tempfile::TempDir), Box<dyn StdError>> {
        let backend = ValidatedBackend::new(
            Arc::new(InMemory::new()),
            StorePath::from("git-http-server-tests"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
        let scratch = tempfile::tempdir()?;
        let concurrency = "2".parse()?;
        Ok((
            GitHttpServer::new(
                SmartHttp::new(log, scratch.path()),
                scratch.path(),
                concurrency,
            ),
            scratch,
        ))
    }

    fn packet(data: &[u8]) -> Vec<u8> {
        let mut packet = format!("{:04x}", data.len() + 4).into_bytes();
        packet.extend_from_slice(data);
        packet
    }
}
