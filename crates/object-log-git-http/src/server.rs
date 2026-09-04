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
    middleware::{self, Next},
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
const MAX_HEADER_BYTES: usize = 16 * 1024;
const REQUEST_BODY_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const RESPONSE_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const PRAGMA_NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");
const EXPIRES_PAST: HeaderValue = HeaderValue::from_static("Fri, 01 Jan 1980 00:00:00 GMT");

type RequestReader = Pin<Box<dyn AsyncRead + Send>>;

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
            .layer(middleware::from_fn(limit_headers))
            .with_state(self)
    }

    /// Stops task admission and waits for active Git operations to finish.
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

async fn limit_headers(request: axum::extract::Request, next: Next) -> Response {
    let uri = request.uri();
    let mut bytes = request.method().as_str().len() + uri.path().len();
    bytes = bytes.saturating_add(uri.query().map_or(0, str::len));
    for (name, value) in request.headers() {
        bytes = bytes.saturating_add(name.as_str().len() + value.as_bytes().len());
    }
    if bytes > MAX_HEADER_BYTES {
        return Failure::new(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request headers are too large",
        )
        .into_response();
    }
    next.run(request).await
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
    require_request_headers(&headers, Service::UploadPack)?;
    let mut input = request_reader(body, &headers)?;
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
    require_request_headers(&headers, Service::ReceivePack)?;
    let mut input = request_reader(body, &headers)?;
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

fn request_reader(body: Body, headers: &HeaderMap) -> Result<RequestReader, Failure> {
    let stream = body.into_data_stream().map_err(body_error);
    let reader = BufReader::new(StreamReader::new(stream));
    match content_encoding(headers)? {
        Encoding::Identity => Ok(Box::pin(reader)),
        Encoding::Gzip => {
            let mut decoder = GzipDecoder::new(reader);
            decoder.multiple_members(true);
            Ok(Box::pin(decoder))
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

fn parse_service(query: Option<&str>) -> Result<Service, Failure> {
    match query {
        Some("service=git-upload-pack") => Ok(Service::UploadPack),
        Some("service=git-receive-pack") => Ok(Service::ReceivePack),
        _ => Err(Failure::new(StatusCode::BAD_REQUEST, "invalid Git service")),
    }
}

fn require_request_headers(headers: &HeaderMap, service: Service) -> Result<(), Failure> {
    let expected = match service {
        Service::UploadPack => "application/x-git-upload-pack-request",
        Service::ReceivePack => "application/x-git-receive-pack-request",
    };
    let content_type = single_header(headers, CONTENT_TYPE)?;
    if !content_type.eq_ignore_ascii_case(expected) {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported Git content type",
        ));
    }
    content_encoding(headers)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Encoding {
    Identity,
    Gzip,
}

fn content_encoding(headers: &HeaderMap) -> Result<Encoding, Failure> {
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Ok(Encoding::Identity);
    };
    if values.next().is_some() {
        return Err(Failure::unsupported_encoding());
    }
    let value = value
        .to_str()
        .map_err(|_| Failure::unsupported_encoding())?;
    if value.eq_ignore_ascii_case("identity") {
        Ok(Encoding::Identity)
    } else if value.eq_ignore_ascii_case("gzip") {
        Ok(Encoding::Gzip)
    } else {
        Err(Failure::unsupported_encoding())
    }
}

fn single_header(
    headers: &HeaderMap,
    name: axum::http::header::HeaderName,
) -> Result<&str, Failure> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .ok_or_else(|| Failure::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "missing content type"))?;
    if values.next().is_some() {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "repeated content type",
        ));
    }
    value
        .to_str()
        .map_err(|_| Failure::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "invalid content type"))
}

fn response(service: Service, advertisement: bool, status: StatusCode, body: Body) -> Response {
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
struct Failure {
    status: StatusCode,
    message: &'static str,
}

impl Failure {
    const fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    const fn internal() -> Self {
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

    const fn busy() -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "Git service is busy")
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
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
    use tokio::io::AsyncReadExt;

    use super::*;

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
}
