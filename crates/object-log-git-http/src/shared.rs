//! Native transport for the common, WASI-compatible Git engine.

use std::convert::Infallible;

use axum::{
    Router,
    body::Body,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
};
use bytes::Bytes;
use object_log::{Log, Resolution, TransactionId};
use object_log_git::{ObjectFormat, Repository};
use tokio::io::AsyncReadExt;
use tokio_util::task::TaskTracker;
use tower_http::{
    limit::RequestBodyLimitLayer,
    timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer},
};

use crate::{
    Error, Service,
    server::{Failure, parse_service, request_reader, require_request_headers, response},
};

const MAX_BODY: usize = 10 * 1024 * 1024;

/// A thin native host for one repository using the shared Git engine.
#[derive(Clone, Debug)]
pub struct SharedGitHttpServer {
    log: Log,
    format: ObjectFormat,
    tasks: TaskTracker,
}

impl SharedGitHttpServer {
    /// Creates a host. The engine admits one operation process-wide.
    #[must_use]
    pub fn new(log: Log, format: ObjectFormat) -> Self {
        Self {
            log,
            format,
            tasks: TaskTracker::new(),
        }
    }

    /// Returns the fixed `/repo` smart HTTP routes.
    pub fn router(self) -> Router {
        Router::new()
            .route("/repo/info/refs", get(advertise))
            .route("/repo/git-upload-pack", post(upload))
            .route("/repo/git-receive-pack", post(receive))
            .layer(ResponseBodyTimeoutLayer::new(
                std::time::Duration::from_mins(1),
            ))
            .layer(RequestBodyTimeoutLayer::new(
                std::time::Duration::from_mins(1),
            ))
            .layer(RequestBodyLimitLayer::new(MAX_BODY))
            .with_state(self)
    }

    /// Waits for publication tasks after the router has stopped.
    pub async fn shutdown(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

async fn advertise(
    State(host): State<SharedGitHttpServer>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, Failure> {
    let service = parse_service(query.as_deref())?;
    let bytes = match service {
        Service::UploadPack => {
            require_v2(&headers)?;
            Repository::upload_advertisement(host.format)
        }
        Service::ReceivePack => Repository::open(&host.log, host.format)
            .await
            .map_err(Error::from)?
            .receive_advertisement()
            .await
            .map_err(Error::from)?,
    };
    Ok(response(
        service,
        true,
        StatusCode::OK,
        retained_body(bytes),
    ))
}

async fn upload(
    State(host): State<SharedGitHttpServer>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Failure> {
    require_v2(&headers)?;
    let encoding = require_request_headers(&headers, Service::UploadPack)?;
    // Admit before collecting input so concurrent transports cannot accumulate
    // uncharged request buffers while waiting for the engine.
    let repository = Repository::open(&host.log, host.format)
        .await
        .map_err(Error::from)?;
    let mut input = request_reader(body, encoding);
    let bytes = collect(&mut input, 9 * 1024 * 1024).await?;
    let output = repository.upload_pack(bytes).await.map_err(Error::from)?;
    Ok(response(
        Service::UploadPack,
        false,
        StatusCode::OK,
        retained_body(output),
    ))
}

async fn receive(
    State(host): State<SharedGitHttpServer>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Failure> {
    let encoding = require_request_headers(&headers, Service::ReceivePack)?;
    let repository = Repository::open(&host.log, host.format)
        .await
        .map_err(Error::from)?;
    let mut input = request_reader(body, encoding);
    let input = collect(&mut input, MAX_BODY).await?;
    // A client disconnect must not cancel an in-flight durable publication.
    let (status, output) = host
        .tasks
        .spawn(async move {
            let prepared = match repository
                .prepare_receive(TransactionId::new(), input)
                .await
            {
                Ok(prepared) => prepared,
                Err(object_log_git::Error::ReceiveRejected { response, .. }) => {
                    return Ok::<_, Error>((StatusCode::OK, response));
                }
                Err(error) => return Err(error.into()),
            };
            let token = prepared.recovery_token().clone();
            let (resolution, output) = prepared.publish_receive().await?;
            let status = match resolution {
                Resolution::Committed(_) | Resolution::NotCommitted(_) => StatusCode::OK,
                Resolution::StillPending(_) | Resolution::Expired(_) => {
                    tracing::warn!(
                        "Git publication remains uncertain; a connected caller receives the recovery token, otherwise refresh refs"
                    );
                    return Ok((StatusCode::SERVICE_UNAVAILABLE, token));
                }
            };
            Ok((status, output))
        })
        .await
        .map_err(|_| Failure::internal())??;
    let mut result = response(Service::ReceivePack, false, status, retained_body(output));
    if status == StatusCode::SERVICE_UNAVAILABLE {
        result.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/octet-stream"),
        );
    }
    Ok(result)
}

fn require_v2(headers: &HeaderMap) -> Result<(), Failure> {
    let mut values = headers.get_all("git-protocol").iter();
    if values
        .next()
        .is_none_or(|value| value.as_bytes() != b"version=2")
        || values.next().is_some()
    {
        return Err(Failure::new(
            StatusCode::BAD_REQUEST,
            "Git protocol version 2 is required",
        ));
    }
    Ok(())
}

async fn collect(input: &mut crate::server::RequestReader, limit: usize) -> Result<Bytes, Error> {
    let mut output = Vec::with_capacity(limit);
    let mut chunk = [0; 8192];
    loop {
        let count = input.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        if count > limit - output.len() {
            return Err(Error::RequestTooLarge("decoded body bytes"));
        }
        output.extend_from_slice(&chunk[..count]);
    }
    Ok(Bytes::from(output.into_boxed_slice()))
}

fn retained_body(bytes: Bytes) -> Body {
    // Keep the engine's accounted Bytes owner through the final HTTP chunk.
    Body::from_stream(futures::stream::unfold(bytes, |mut bytes| async move {
        if bytes.is_empty() {
            None
        } else {
            let chunk = bytes.split_to(bytes.len().min(64 * 1024));
            Some((Ok::<_, Infallible>(chunk), bytes))
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::Encoding;
    use axum::http::HeaderValue;

    #[test]
    fn protocol_header_requires_one_exact_version() {
        let mut headers = HeaderMap::new();
        assert!(require_v2(&headers).is_err());
        headers.insert("git-protocol", HeaderValue::from_static("version=1"));
        assert!(require_v2(&headers).is_err());
        headers.insert("git-protocol", HeaderValue::from_static("version=2"));
        assert!(require_v2(&headers).is_ok());
        headers.append("git-protocol", HeaderValue::from_static("version=2"));
        assert!(require_v2(&headers).is_err());
    }

    #[tokio::test]
    async fn decoded_body_has_an_exact_bound() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = request_reader(Body::from(vec![1; 8192]), Encoding::Identity);
        assert_eq!(collect(&mut input, 8192).await?.len(), 8192);
        let mut input = request_reader(Body::from(vec![1; 8193]), Encoding::Identity);
        assert!(matches!(
            collect(&mut input, 8192).await,
            Err(Error::RequestTooLarge(_))
        ));
        Ok(())
    }
}
