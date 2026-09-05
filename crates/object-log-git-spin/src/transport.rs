use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use http_body::{Body as _, Frame};
use http_body_util::{BodyExt, StreamBody};
use object_store::client::{
    ClientConfigKey, ClientOptions, CryptoProvider, DigestAlgorithm, DigestContext, HmacContext,
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
    HttpResponseBody, HttpService, Signer, SigningAlgorithm,
};
use sha2::{Digest, Sha256};
use spin_executor::CancelOnDropToken;
use spin_sdk::http::conversions::TryIntoOutgoingRequest;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

const HTTP_CALLS: usize = 512;
const HTTP_BYTES: usize = 96 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct QuotaExceeded;

impl std::fmt::Display for QuotaExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Git HTTP storage quota exceeded")
    }
}

impl std::error::Error for QuotaExceeded {}

// One budget per incoming Git handler, including bootstrap and engine retries.
#[derive(Debug, Default)]
struct Budget {
    calls: AtomicUsize,
    bytes: AtomicUsize,
}
impl Budget {
    fn charge(counter: &AtomicUsize, amount: usize, limit: usize) -> Result<(), HttpError> {
        counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(amount).filter(|&next| next <= limit)
            })
            .map(|_| ())
            .map_err(|_| http_error(QuotaExceeded))
    }
    fn call(&self) -> Result<(), HttpError> {
        Self::charge(&self.calls, 1, HTTP_CALLS)
    }
    fn transfer(&self, bytes: usize) -> Result<(), HttpError> {
        Self::charge(&self.bytes, bytes, HTTP_BYTES)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Transport(Arc<Budget>);
#[derive(Debug, Clone)]
struct Service {
    budget: Arc<Budget>,
    connect: Option<u64>,
    read: Option<u64>,
}
impl HttpConnector for Transport {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        if options
            .get_config_value(&ClientConfigKey::Timeout)
            .is_some()
        {
            return Err(unsupported());
        }
        let duration = |key| -> object_store::Result<Option<u64>> {
            options
                .get_config_value(&key)
                .map(|value| {
                    let value = humantime::parse_duration(&value).map_err(|_| unsupported())?;
                    u64::try_from(value.as_nanos()).map_err(|_| unsupported())
                })
                .transpose()
        };
        Ok(HttpClient::new(Service {
            budget: Arc::clone(&self.0),
            connect: duration(ClientConfigKey::ConnectTimeout)?,
            read: duration(ClientConfigKey::ReadTimeout)?,
        }))
    }
}

fn http_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> HttpError {
    HttpError::new_boxed(HttpErrorKind::Unknown, error.into())
}

#[async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        retry_read(request, |request| self.call_once(request)).await
    }
}

#[path = "read_retry.rs"]
mod read_retry;

async fn retry_read<F, Fut>(request: HttpRequest, attempt: F) -> Result<HttpResponse, HttpError>
where
    F: FnMut(HttpRequest) -> Fut,
    Fut: std::future::Future<Output = Result<HttpResponse, HttpError>>,
{
    read_retry::retry_read(request, attempt, |error| {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<spin_sdk::http::ErrorCode>())
            .is_some_and(|code| {
                matches!(
                    code,
                    spin_sdk::http::ErrorCode::ConnectionTerminated
                        | spin_sdk::http::ErrorCode::HttpResponseIncomplete
                        | spin_sdk::http::ErrorCode::HttpProtocolError
                )
            })
    })
    .await
}

impl Service {
    async fn call_once(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.budget.call()?;
        let (mut parts, mut body) = request.into_parts();
        // Preserve exact framing supplied by object_store's body. In particular,
        // S3 bulk deletion requires Content-Length rather than chunked encoding.
        if !parts.headers.contains_key(http::header::CONTENT_LENGTH)
            && let Some(length) = body.size_hint().exact()
        {
            parts
                .headers
                .insert(http::header::CONTENT_LENGTH, length.into());
        }
        let (outgoing, _) = http::Request::from_parts(parts, ())
            .try_into_outgoing_request()
            .map_err(http_error)?;
        let outgoing_body = outgoing
            .body()
            .map_err(|()| http_error(std::io::Error::other("outgoing body unavailable")))?;
        let output = outgoing_body
            .write()
            .map_err(|()| http_error(std::io::Error::other("output stream unavailable")))?;
        let options = spin_sdk::wit::wasi::http0_2_0::types::RequestOptions::new();
        options
            .set_connect_timeout(self.connect)
            .map_err(|()| http_error(std::io::Error::other("unsupported connect timeout")))?;
        options
            .set_first_byte_timeout(self.read)
            .map_err(|()| http_error(std::io::Error::other("unsupported first-byte timeout")))?;
        options
            .set_between_bytes_timeout(self.read)
            .map_err(|()| http_error(std::io::Error::other("unsupported read timeout")))?;
        let pending =
            spin_sdk::wit::wasi::http0_2_0::outgoing_handler::handle(outgoing, Some(options))
                .map_err(http_error)?;
        let upload_budget = Arc::clone(&self.budget);
        let upload = async move {
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame.map_err(http_error)?.into_data() {
                    upload_budget.transfer(data.len())?;
                    write_chunk(&output, &data).await?;
                }
            }
            drop(output);
            spin_sdk::http::OutgoingBody::finish(outgoing_body, None).map_err(http_error)?;
            Ok::<_, HttpError>(())
        };
        let response = async move {
            // Drop the poll subscription before the future-response resource.
            let mut token: Option<CancelOnDropToken> = None;
            futures::future::poll_fn(|cx| {
                drop(token.take());
                if let Some(result) = pending.get() {
                    Poll::Ready(
                        result
                            .map_err(|()| {
                                http_error(std::io::Error::other("response already consumed"))
                            })?
                            .map_err(http_error),
                    )
                } else {
                    token = Some(
                        spin_executor::push_waker_and_get_token(
                            pending.subscribe(),
                            cx.waker().clone(),
                        )
                        .into(),
                    );
                    Poll::Pending
                }
            })
            .await
        };
        let response = match futures::future::select(Box::pin(upload), Box::pin(response)).await {
            futures::future::Either::Left((uploaded, response)) => {
                uploaded?;
                response.await?
            }
            futures::future::Either::Right((response, upload)) => {
                let response = response?;
                // Early rejections must not wait for the server to consume the upload.
                if (200..300).contains(&response.status()) {
                    upload.await?;
                }
                response
            }
        };
        let mut builder = http::Response::builder().status(response.status());
        for (name, value) in response.headers().entries() {
            builder = builder.header(name, value);
        }
        // Keep the response resource alive until its streaming body is dropped.
        let response_budget = Arc::clone(&self.budget);
        let stream = response.take_body_stream().map(move |chunk| {
            let _keep_response_alive = &response;
            let chunk = chunk.map_err(http_error)?;
            response_budget.transfer(chunk.len())?;
            Ok(Frame::data(Bytes::from(chunk)))
        });
        builder
            .body(HttpResponseBody::new(StreamBody::new(stream)))
            .map_err(http_error)
    }
}

pub(crate) async fn write_chunk(
    output: &wasi::io::streams::OutputStream,
    bytes: &[u8],
) -> Result<(), HttpError> {
    let mut offset = 0;
    let mut token: Option<CancelOnDropToken> = None;
    futures::future::poll_fn(|cx| {
        drop(token.take());
        while offset < bytes.len() {
            let available = output.check_write().map_err(http_error)?;
            if available == 0 {
                token = Some(
                    spin_executor::push_waker_and_get_token(output.subscribe(), cx.waker().clone())
                        .into(),
                );
                return Poll::Pending;
            }
            let count = usize::try_from(available)
                .unwrap_or(usize::MAX)
                .min(bytes.len() - offset);
            output
                .write(&bytes[offset..offset + count])
                .map_err(http_error)?;
            offset += count;
        }
        Poll::Ready(Ok(()))
    })
    .await
}

#[derive(Debug)]
pub(crate) struct Crypto;
struct Hash(Sha256, [u8; 32]);
struct Authentication(Hmac<Sha256>, [u8; 32]);
impl DigestContext for Hash {
    fn update(&mut self, data: &[u8]) {
        Digest::update(&mut self.0, data);
    }
    fn finish(&mut self) -> object_store::Result<&[u8]> {
        self.1 = self.0.clone().finalize().into();
        Ok(&self.1)
    }
}
impl HmacContext for Authentication {
    fn update(&mut self, data: &[u8]) {
        Mac::update(&mut self.0, data);
    }
    fn finish(&mut self) -> object_store::Result<&[u8]> {
        self.1 = self.0.clone().finalize().into_bytes().into();
        Ok(&self.1)
    }
}
fn unsupported() -> object_store::Error {
    object_store::Error::NotSupported {
        source: "unsupported S3 transport option or crypto algorithm".into(),
    }
}
impl CryptoProvider for Crypto {
    fn digest(&self, algorithm: DigestAlgorithm) -> object_store::Result<Box<dyn DigestContext>> {
        match algorithm {
            DigestAlgorithm::Sha256 => Ok(Box::new(Hash(Sha256::new(), [0; 32]))),
            _ => Err(unsupported()),
        }
    }
    fn hmac(
        &self,
        algorithm: DigestAlgorithm,
        secret: &[u8],
    ) -> object_store::Result<Box<dyn HmacContext>> {
        match algorithm {
            DigestAlgorithm::Sha256 => Ok(Box::new(Authentication(
                Hmac::<Sha256>::new_from_slice(secret).map_err(|_| unsupported())?,
                [0; 32],
            ))),
            _ => Err(unsupported()),
        }
    }
    fn sign(&self, _: SigningAlgorithm, _: &[u8]) -> object_store::Result<Box<dyn Signer>> {
        Err(unsupported())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::client::HttpRequestBody;

    #[test]
    fn safe_read_retry_is_bounded_and_preserves_request_and_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        for (method, body, expected) in [
            (http::Method::GET, Bytes::new(), 2),
            (http::Method::HEAD, Bytes::new(), 2),
            (http::Method::GET, Bytes::from_static(b"body"), 1),
            (http::Method::PUT, Bytes::new(), 1),
            (http::Method::POST, Bytes::new(), 1),
            (http::Method::DELETE, Bytes::new(), 1),
        ] {
            let budget = Budget::default();
            let request = http::Request::builder()
                .method(method.clone())
                .uri("http://example.invalid/object?versionId=7")
                .header("range", "bytes=3-5")
                .header("if-match", "version")
                .body(HttpRequestBody::from(body))?;
            let result =
                futures::executor::block_on(retry_read(request, |request| {
                    assert_eq!(request.method(), method);
                    assert_eq!(request.uri(), "http://example.invalid/object?versionId=7");
                    assert_eq!(request.headers()["range"], "bytes=3-5");
                    assert_eq!(request.headers()["if-match"], "version");
                    futures::future::ready(budget.call().and_then(|()| {
                        Err(http_error(spin_sdk::http::ErrorCode::HttpProtocolError))
                    }))
                }));
            assert!(result.is_err());
            assert_eq!(budget.calls.load(Ordering::Relaxed), expected);
        }
        let budget = Budget::default();
        budget.calls.store(HTTP_CALLS - 1, Ordering::Relaxed);
        let request = http::Request::new(HttpRequestBody::empty());
        let result =
            futures::executor::block_on(retry_read(request, |_| {
                futures::future::ready(budget.call().and_then(|()| {
                    Err(http_error(spin_sdk::http::ErrorCode::ConnectionTerminated))
                }))
            }));
        assert!(
            std::error::Error::source(&result.err().ok_or("read unexpectedly succeeded")?)
                .is_some_and(<dyn std::error::Error + 'static>::is::<QuotaExceeded>)
        );
        assert_eq!(budget.calls.load(Ordering::Relaxed), HTTP_CALLS);
        Ok(())
    }

    #[test]
    fn read_retry_does_not_match_text_or_nontransport_errors() {
        for error in [
            http_error(QuotaExceeded),
            http_error(std::io::Error::other("HttpProtocolError")),
            http_error(spin_sdk::http::ErrorCode::HttpRequestDenied),
        ] {
            let mut error = Some(error);
            let mut calls = 0;
            let result = futures::executor::block_on(retry_read(
                http::Request::new(HttpRequestBody::empty()),
                |_| {
                    calls += 1;
                    futures::future::ready(Err(error
                        .take()
                        .unwrap_or_else(|| http_error(QuotaExceeded))))
                },
            ));
            assert!(result.is_err());
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn quota_marker_survives_storage_and_git_error_wrappers() -> Result<(), HttpError> {
        let wrap = |source: HttpError| {
            anyhow::Error::new(object_log_git::Error::ObjectLog(object_log::Error::Store(
                object_store::Error::Generic {
                    store: "test",
                    source: Box::new(source),
                },
            )))
        };
        let budget = Budget::default();
        let denied = budget
            .transfer(HTTP_BYTES + 1)
            .err()
            .ok_or_else(|| http_error(std::io::Error::other("quota unexpectedly admitted")))?;
        assert!(
            wrap(denied)
                .chain()
                .any(<dyn std::error::Error + 'static>::is::<QuotaExceeded>)
        );
        let ordinary = http_error(std::io::Error::other("Git HTTP storage quota exceeded"));
        assert!(
            !wrap(ordinary)
                .chain()
                .any(<dyn std::error::Error + 'static>::is::<QuotaExceeded>)
        );
        Ok(())
    }

    #[test]
    fn quota_accepts_exact_limits_and_rejects_overflow_without_wrapping() -> Result<(), HttpError> {
        let budget = Budget::default();
        for _ in 0..HTTP_CALLS {
            budget.call()?;
        }
        assert!(budget.call().is_err());
        assert_eq!(budget.calls.load(Ordering::Relaxed), HTTP_CALLS);
        budget.transfer(HTTP_BYTES - 1)?;
        budget.transfer(1)?;
        assert!(budget.transfer(1).is_err());
        assert!(budget.transfer(usize::MAX).is_err());
        assert_eq!(budget.bytes.load(Ordering::Relaxed), HTTP_BYTES);
        Ok(())
    }

    #[test]
    fn connector_clones_share_bootstrap_and_command_quota() -> Result<(), HttpError> {
        let bootstrap = Transport::default();
        let command = bootstrap.clone();
        bootstrap.0.transfer(HTTP_BYTES / 2)?;
        command.0.transfer(HTTP_BYTES / 2)?;
        assert!(bootstrap.0.transfer(1).is_err());
        let mut threads = Vec::new();
        for _ in 0..32 {
            let retry = command.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..16 {
                    retry.0.call()?;
                }
                Ok::<_, HttpError>(())
            }));
        }
        for thread in threads {
            thread
                .join()
                .map_err(|_| http_error(std::io::Error::other("quota test thread failed")))??;
        }
        assert!(bootstrap.0.call().is_err());
        // A new handler receives a distinct budget.
        Transport::default().0.call()?;
        Ok(())
    }

    #[test]
    fn exhausted_calls_fail_before_entering_wasi_http() -> Result<(), HttpError> {
        let budget = Arc::new(Budget::default());
        for _ in 0..HTTP_CALLS {
            budget.call()?;
        }
        let service = Service {
            budget,
            connect: None,
            read: None,
        };
        let request = HttpRequest::new(object_store::client::HttpRequestBody::empty());
        // Native WASI imports trap, so this also proves the boundary precedes I/O.
        assert!(futures::executor::block_on(service.call(request)).is_err());
        Ok(())
    }

    #[test]
    fn overall_deadline_cannot_be_silently_ignored() {
        assert!(Transport::default().connect(&ClientOptions::new()).is_err());
        assert!(
            Transport::default()
                .connect(&ClientOptions::new().with_timeout_disabled())
                .is_ok()
        );
    }
}
