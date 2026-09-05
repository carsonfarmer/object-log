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
use std::task::Poll;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Transport;
#[derive(Debug, Clone, Copy)]
struct Service {
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
        let upload = async move {
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame.map_err(http_error)?.into_data() {
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
        let stream = response.take_body_stream().map(move |chunk| {
            let _keep_response_alive = &response;
            chunk
                .map(|chunk| Frame::data(Bytes::from(chunk)))
                .map_err(http_error)
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

    #[test]
    fn overall_deadline_cannot_be_silently_ignored() {
        assert!(Transport.connect(&ClientOptions::new()).is_err());
        assert!(
            Transport
                .connect(&ClientOptions::new().with_timeout_disabled())
                .is_ok()
        );
    }
}
