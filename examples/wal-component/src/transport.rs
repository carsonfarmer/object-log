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
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};

const QUOTA_EXCEEDED: &str = "object storage transport limit exceeded";

// One budget per component session, including bootstrap and transport retries.
#[derive(Debug)]
struct Budget {
    max_calls: u64,
    max_bytes: u64,
    calls: AtomicU64,
    bytes: AtomicU64,
}
impl Budget {
    fn call(&self) -> Result<(), HttpError> {
        self.calls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|&next| next <= self.max_calls)
            })
            .map(|_| ())
            .map_err(|_| http_error(QUOTA_EXCEEDED))
    }
    fn transfer(&self, bytes: impl TryInto<u64>) -> Result<(), HttpError> {
        let bytes = bytes.try_into().map_err(|_| http_error(QUOTA_EXCEEDED))?;
        self.bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(bytes)
                    .filter(|&next| next <= self.max_bytes)
            })
            .map(|_| ())
            .map_err(|_| http_error(QUOTA_EXCEEDED))
    }
}

#[derive(Debug, Clone)]
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

async fn finish_exchange<U, R, T, E, F>(upload: U, response: R, is_success: F) -> Result<T, E>
where
    U: std::future::Future<Output = Result<(), E>>,
    R: std::future::Future<Output = Result<T, E>>,
    F: Fn(&T) -> bool,
{
    futures::pin_mut!(upload, response);
    match futures::future::select(upload, response).await {
        futures::future::Either::Left((uploaded, response)) => match uploaded {
            Ok(()) => response.await,
            Err(upload_error) => match response.await {
                Ok(response) if !is_success(&response) => Ok(response),
                _ => Err(upload_error),
            },
        },
        futures::future::Either::Right((response, upload)) => match response {
            Ok(response) if !is_success(&response) => Ok(response),
            response => {
                upload.await?;
                response
            }
        },
    }
}

#[path = "request_retry.rs"]
mod request_retry;

#[async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Retry before exposing a response; failed conditional replays remain uncertain.
        // Each attempt retains the same transport budget and configured deadlines.
        request_retry::retry_request(
            request,
            |request| self.call_once(request),
            |error| {
                std::error::Error::source(error)
                    .and_then(|source| source.downcast_ref::<spin_sdk::http::ErrorCode>())
                    .is_some_and(|code| {
                        matches!(
                            code,
                            spin_sdk::http::ErrorCode::ConnectionTerminated
                                | spin_sdk::http::ErrorCode::ConnectionReadTimeout
                                | spin_sdk::http::ErrorCode::HttpResponseIncomplete
                                | spin_sdk::http::ErrorCode::HttpProtocolError
                        )
                    })
            },
        )
        .await
    }
}

impl Service {
    async fn call_once(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.budget.call()?;
        let (mut parts, mut body) = request.into_parts();
        // A rejected conditional upload may leave its body unread. Expect lets
        // the server close that HTTP/1 connection explicitly instead of pooling it.
        if parts.method == http::Method::PUT
            && (parts.headers.contains_key(http::header::IF_MATCH)
                || parts.headers.contains_key(http::header::IF_NONE_MATCH))
            && body.size_hint().exact().is_some_and(|length| length > 0)
        {
            parts.headers.insert(
                http::header::EXPECT,
                http::HeaderValue::from_static("100-continue"),
            );
        }
        // Preserve exact framing supplied by object_store's body. In particular,
        // S3 bulk deletion requires Content-Length rather than chunked encoding.
        if !parts.headers.contains_key(http::header::CONTENT_LENGTH)
            && let Some(length) = body.size_hint().exact()
        {
            parts
                .headers
                .insert(http::header::CONTENT_LENGTH, length.into());
        }
        // object_store bounds metadata connects to one second but inherits the
        // S3 read deadline. Metadata is local and small; bound its waits too.
        let metadata = parts.uri.authority().is_some_and(|authority| {
            authority.as_str() == super::INSTANCE_METADATA_ENDPOINT.trim_start_matches("http://")
        }) && parts.uri.path().starts_with("/latest/");
        let read_timeout = if metadata {
            Some(1_000_000_000)
        } else {
            self.read
        };
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
            .set_first_byte_timeout(read_timeout)
            .map_err(|()| http_error(std::io::Error::other("unsupported first-byte timeout")))?;
        options
            .set_between_bytes_timeout(read_timeout)
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
        // A server can reject a conditional request while closing its upload stream.
        // Its definite non-success response takes precedence over that closure.
        let response = finish_exchange(upload, response, |response| {
            (200..300).contains(&response.status())
        })
        .await?;
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

impl Transport {
    pub(crate) fn new(max_calls: u64, max_bytes: u64) -> Result<Self, &'static str> {
        if max_calls == 0 || max_bytes == 0 {
            return Err("transport limits must be positive");
        }
        Ok(Self(Arc::new(Budget {
            max_calls,
            max_bytes,
            calls: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })))
    }

    pub(crate) fn usage(&self) -> (u64, u64) {
        (
            self.0.calls.load(Ordering::Relaxed),
            self.0.bytes.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_once<T>(value: T) -> impl std::future::Future<Output = T> {
        let mut value = Some(value);
        let mut pending = true;
        std::future::poll_fn(move |cx| {
            if std::mem::take(&mut pending) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(value.take().unwrap())
            }
        })
    }

    async fn exchange(
        upload: Result<(), &'static str>,
        response: Result<u16, &'static str>,
        response_first: bool,
    ) -> Result<u16, &'static str> {
        if response_first {
            finish_exchange(
                pending_once(upload),
                std::future::ready(response),
                |status| (200..300).contains(status),
            )
            .await
        } else {
            finish_exchange(
                std::future::ready(upload),
                std::future::ready(response),
                |status| (200..300).contains(status),
            )
            .await
        }
    }

    #[tokio::test]
    async fn definite_rejection_wins_in_both_completion_orders() {
        for response_first in [false, true] {
            assert_eq!(
                exchange(Err("upload closed"), Ok(412), response_first).await,
                Ok(412)
            );
        }
    }

    #[tokio::test]
    async fn all_other_outcomes_wait_for_both_sides() {
        for response_first in [false, true] {
            for response in [Ok(200), Err("response failed")] {
                assert_eq!(
                    exchange(Err("upload closed"), response, response_first).await,
                    Err("upload closed")
                );
            }
            for response in [Ok(200), Ok(412), Err("response failed")] {
                assert_eq!(exchange(Ok(()), response, response_first).await, response);
            }
        }
    }

    #[test]
    fn rejected_transfers_preserve_the_shared_budget() {
        let transport = Transport::new(3, 5).unwrap();
        let shared = transport.clone();
        shared.0.transfer(5_u64).unwrap();
        let error = transport.0.transfer(1_u64).unwrap_err();
        assert_eq!(transport.usage(), (0, 5));
        assert!(
            error
                .to_string()
                .contains("object storage transport limit exceeded")
        );
        assert!(
            !std::error::Error::source(&error)
                .unwrap()
                .is::<spin_sdk::http::ErrorCode>()
        );
        assert!(shared.0.transfer(-1_i32).is_err());
        transport.0.calls.store(2, Ordering::Relaxed);
        shared.0.call().unwrap();
        assert!(transport.0.call().is_err());
        assert_eq!(shared.usage(), (3, 5));
        assert!(Transport::new(0, 1).is_err());
        assert!(Transport::new(1, 0).is_err());
    }
}
