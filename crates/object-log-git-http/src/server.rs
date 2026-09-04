//! Native HTTP host for one fixed Git repository.

use std::{error::Error as StdError, io, path::PathBuf, pin::Pin, sync::Arc};

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
    io::{AsyncRead, AsyncSeekExt, BufReader, SeekFrom},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::io::{ReaderStream, StreamReader};
use tower_http::limit::RequestBodyLimitLayer;

use crate::{Error, ReceiveOutcome, Service, SmartHttp};

const MAX_ENCODED_BODY_BYTES: usize = 513 * 1024 * 1024;
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
}

impl GitHttpServer {
    /// Creates a host with a fixed limit on active Git operations.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrency` is zero.
    #[must_use]
    pub fn new(endpoint: SmartHttp, scratch: impl Into<PathBuf>, max_concurrency: usize) -> Self {
        assert!(max_concurrency != 0, "Git HTTP concurrency must be nonzero");
        Self {
            endpoint,
            scratch: scratch.into(),
            permits: Arc::new(Semaphore::new(max_concurrency)),
        }
    }

    /// Returns the complete router for the fixed `/repo` mapping.
    pub fn router(self) -> Router {
        Router::new()
            .route("/repo/info/refs", get(info_refs))
            .route("/repo/git-upload-pack", post(upload_pack))
            .route("/repo/git-receive-pack", post(receive_pack))
            .layer(RequestBodyLimitLayer::new(MAX_ENCODED_BODY_BYTES))
            .with_state(self)
    }

    async fn permit(&self) -> Result<OwnedSemaphorePermit, Failure> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Failure::internal())
    }
}

async fn info_refs(
    State(host): State<GitHttpServer>,
    RawQuery(query): RawQuery,
) -> Result<Response, Failure> {
    let service = parse_service(query.as_deref())?;
    let permit = host.permit().await?;
    let endpoint = host.endpoint.clone();
    let output = tokio::spawn(async move {
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
    let permit = host.permit().await?;
    let endpoint = host.endpoint.clone();
    let scratch = host.scratch.clone();
    let output = tokio::spawn(async move {
        let _permit = permit;
        tokio::fs::create_dir_all(&scratch).await?;
        let output = tempfile::tempfile_in(scratch)?;
        let mut output = tokio::fs::File::from_std(output);
        endpoint.upload_pack(&mut input, &mut output).await?;
        output.seek(SeekFrom::Start(0)).await?;
        Ok::<_, Error>(output)
    })
    .await
    .map_err(|_| Failure::internal())??;
    let body = Body::from_stream(ReaderStream::new(output));
    Ok(response(Service::UploadPack, false, StatusCode::OK, body))
}

async fn receive_pack(
    State(host): State<GitHttpServer>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Failure> {
    require_request_headers(&headers, Service::ReceivePack)?;
    let mut input = request_reader(body, &headers)?;
    let permit = host.permit().await?;
    let endpoint = host.endpoint.clone();
    let (outcome, output) = tokio::spawn(async move {
        let _permit = permit;
        let mut output = Vec::new();
        let outcome = endpoint.receive_pack(&mut input, &mut output).await?;
        Ok::<_, Error>((outcome, output))
    })
    .await
    .map_err(|_| Failure::internal())??;
    let status = match outcome {
        ReceiveOutcome::Committed | ReceiveOutcome::Rejected => StatusCode::OK,
        ReceiveOutcome::Pending(_) | ReceiveOutcome::Expired => StatusCode::SERVICE_UNAVAILABLE,
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
    let mut too_large = false;
    while let Some(current) = source {
        if current.is::<http_body_util::LengthLimitError>() {
            too_large = true;
            break;
        }
        source = current.source();
    }
    if too_large {
        io::Error::new(io::ErrorKind::FileTooLarge, error)
    } else {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
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
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::Protocol(_) => Self::new(StatusCode::BAD_REQUEST, "invalid Git protocol"),
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
