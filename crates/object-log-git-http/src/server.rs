//! Native HTTP media, body, and error handling.
use crate::{Error, Service};
use async_compression::tokio::bufread::GzipDecoder;
use axum::{
    body::Body,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, EXPIRES, PRAGMA},
    },
    response::{IntoResponse, Response},
};
use futures::TryStreamExt;
use std::{error::Error as StdError, io, pin::Pin};
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::StreamReader;
const PRAGMA_NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");
const EXPIRES_PAST: HeaderValue = HeaderValue::from_static("Fri, 01 Jan 1980 00:00:00 GMT");
pub(super) type RequestReader = Pin<Box<dyn AsyncRead + Send>>;
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
        HeaderValue::from_static(crate::CACHE_CONTROL),
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

    use axum::http::Request;
    use object_log::{Log, LogId, Options, ValidatedBackend};
    use object_store::{memory::InMemory, path::Path as StorePath};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn routes_apply_git_metadata_and_client_error_statuses() -> Result<(), Box<dyn StdError>>
    {
        let server = test_server().await?;
        let app = server.router();
        let response = app
            .clone()
            .oneshot(
                Request::get("/repo/info/refs?service=git-upload-pack")
                    .header("git-protocol", "version=2")
                    .body(Body::empty())?,
            )
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
            .oneshot(
                Request::get("/repo/info/refs?service=bad")
                    .header("git-protocol", "version=2")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::post("/repo/git-upload-pack")
                    .header("git-protocol", "version=2")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let response = app
            .oneshot(
                Request::post("/repo/git-upload-pack")
                    .header("git-protocol", "version=2")
                    .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
                    .body(Body::from("0008"))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    async fn test_server() -> Result<crate::SharedGitHttpServer, Box<dyn StdError>> {
        let backend = ValidatedBackend::new(
            std::sync::Arc::new(InMemory::new()),
            StorePath::from("git-http-server-tests"),
        )
        .await?;
        let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
        Ok(crate::SharedGitHttpServer::new(
            log,
            object_log_git::ObjectFormat::Sha1,
        ))
    }
}
