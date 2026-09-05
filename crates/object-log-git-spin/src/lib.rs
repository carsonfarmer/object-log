//! Spin HTTP and S3 transport adapters for the shared Git repository engine.
#![cfg_attr(
    not(target_arch = "wasm32"),
    allow(
        dead_code,
        reason = "Native builds test helpers; the HTTP export requires WASI."
    )
)]

mod transport;

use std::{io::Read, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use object_log::{Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_git::{Error, ObjectFormat, Repository};
use object_store::{RetryConfig, aws::AmazonS3Builder, client::ClientOptions, path::Path};
use spin_sdk::http::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

const BODY_LIMIT: usize = 10 * 1024 * 1024;
const UPLOAD_RESULT: &str = "application/x-git-upload-pack-result";
const RECEIVE_RESULT: &str = "application/x-git-receive-pack-result";

struct Reply(u16, &'static str, Bytes);

async fn repository() -> anyhow::Result<Repository> {
    let variable = spin_sdk::variables::get;
    let endpoint = variable("endpoint")?;
    let store = AmazonS3Builder::new()
        .with_bucket_name(variable("bucket")?)
        .with_region(variable("region")?)
        .with_access_key_id(variable("access_key")?)
        .with_secret_access_key(variable("secret_key")?)
        .with_endpoint(&endpoint)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(endpoint.starts_with("http://"))
                .with_timeout_disabled()
                .with_connect_timeout(Duration::from_secs(5))
                .with_read_timeout(Duration::from_secs(30)),
        )
        .with_http_connector(transport::Transport)
        .with_crypto_provider(Arc::new(transport::Crypto))
        // object_store retries require Tokio timers; the engine handles uncertain publication.
        .with_retry(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
        .build()?;
    let backend = ValidatedBackend::new(Arc::new(store), Path::from(variable("prefix")?)).await?;
    let log = Log::open(
        &backend,
        &LogId::new(variable("log_id")?)?,
        Options::default(),
    )
    .await?;
    let format = object_format()?;
    Ok(Repository::open(&log, format).await?)
}

fn object_format() -> anyhow::Result<ObjectFormat> {
    match spin_sdk::variables::get("object_format")?.as_str() {
        "sha1" => Ok(ObjectFormat::Sha1),
        "sha256" => Ok(ObjectFormat::Sha256),
        _ => anyhow::bail!("object_format must be sha1 or sha256"),
    }
}

fn header(request: &IncomingRequest, name: &str) -> anyhow::Result<Option<String>> {
    let values = request.headers().get(name);
    anyhow::ensure!(values.len() <= 1, "duplicate {name} header");
    values
        .into_iter()
        .next()
        .map(String::from_utf8)
        .transpose()
        .map_err(Into::into)
}

fn validate_headers(
    request: &IncomingRequest,
    upload: bool,
    upload_advert: bool,
    post: bool,
) -> anyhow::Result<Option<String>> {
    if upload || upload_advert {
        anyhow::ensure!(
            header(request, "git-protocol")?.as_deref() == Some("version=2"),
            "Git protocol v2 required"
        );
    }
    if post {
        let expected = if upload {
            "application/x-git-upload-pack-request"
        } else {
            "application/x-git-receive-pack-request"
        };
        anyhow::ensure!(
            header(request, "content-type")?
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(expected)),
            "unsupported Git content type"
        );
    }
    let encoding = header(request, "content-encoding")?;
    anyhow::ensure!(
        encoding
            .as_deref()
            .is_none_or(|value| value == "identity" || value == "gzip"),
        "unsupported content encoding"
    );
    if let Some(length) = header(request, "content-length")? {
        anyhow::ensure!(
            length.parse::<usize>()? <= BODY_LIMIT,
            "request exceeds byte limit"
        );
    }
    Ok(encoding)
}

async fn dispatch(request: IncomingRequest) -> anyhow::Result<Reply> {
    let path = request.path_with_query().unwrap_or_default();
    let upload = path == "/repo/git-upload-pack";
    let receive = path == "/repo/git-receive-pack";
    let upload_advert = path == "/repo/info/refs?service=git-upload-pack";
    let receive_advert = path == "/repo/info/refs?service=git-receive-pack";
    let get = request.method() == Method::Get;
    let post = request.method() == Method::Post;
    if !(get && (upload_advert || receive_advert) || post && (upload || receive)) {
        return Ok(Reply(404, "text/plain", Bytes::from_static(b"not found\n")));
    }
    let Ok(encoding) = validate_headers(&request, upload, upload_advert, post) else {
        return Ok(Reply(
            400,
            "text/plain",
            Bytes::from_static(b"invalid Git HTTP request\n"),
        ));
    };
    if upload_advert {
        return Ok(Reply(
            200,
            "application/x-git-upload-pack-advertisement",
            Repository::upload_advertisement(object_format()?),
        ));
    }
    // Opening holds engine admission before host body collection. The bounded
    // host buffer lives in the runtime allowance until the command charges it.
    let repository = repository().await?;
    if receive_advert {
        return Ok(Reply(
            200,
            "application/x-git-receive-pack-advertisement",
            repository.receive_advertisement().await?,
        ));
    }
    let Ok(body) = body(request, encoding.as_deref() == Some("gzip")).await else {
        return Ok(Reply(
            400,
            "text/plain",
            Bytes::from_static(b"invalid or oversized request body\n"),
        ));
    };
    if upload {
        return Ok(Reply(
            200,
            UPLOAD_RESULT,
            repository.upload_pack(body).await?,
        ));
    }
    match repository.prepare_receive(TransactionId::new(), body).await {
        Ok(prepared) => {
            let token = prepared.recovery_token().clone();
            match prepared.publish_receive().await {
                Ok((
                    object_log::Resolution::Committed(_) | object_log::Resolution::NotCommitted(_),
                    response,
                )) => Ok(Reply(200, RECEIVE_RESULT, response)),
                Ok((
                    object_log::Resolution::StillPending(_) | object_log::Resolution::Expired(_),
                    _,
                ))
                | Err(_) => {
                    // Return the opaque exact-candidate token without allocating an
                    // encoded copy. A 503 never claims successful publication.
                    Ok(Reply(503, "application/octet-stream", token))
                }
            }
        }
        Err(Error::ReceiveRejected { response, .. }) => Ok(Reply(200, RECEIVE_RESULT, response)),
        Err(error) => Err(error.into()),
    }
}

async fn body(request: IncomingRequest, gzip: bool) -> anyhow::Result<Bytes> {
    let mut stream = request.into_body_stream();
    let mut input = Vec::with_capacity(BODY_LIMIT);
    while let Some(chunk) = stream.next().await {
        append(&mut input, &chunk?)?;
    }
    decode_body(input, gzip)
}

fn decode_body(input: Vec<u8>, gzip: bool) -> anyhow::Result<Bytes> {
    if gzip {
        let mut output = Vec::with_capacity(BODY_LIMIT);
        let mut decoder = flate2::read::MultiGzDecoder::new(input.as_slice());
        let mut chunk = [0; 8192];
        loop {
            let length = decoder.read(&mut chunk)?;
            if length == 0 {
                break;
            }
            append(&mut output, &chunk[..length])?;
        }
        drop(decoder);
        drop(input);
        return Ok(Bytes::from(output.into_boxed_slice()));
    }
    Ok(Bytes::from(input.into_boxed_slice()))
}

fn append(output: &mut Vec<u8>, chunk: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        chunk.len() <= BODY_LIMIT - output.len(),
        "request exceeds byte limit"
    );
    output.extend_from_slice(chunk);
    Ok(())
}

async fn respond(
    out: ResponseOutparam,
    Reply(status, content_type, bytes): Reply,
) -> anyhow::Result<()> {
    let fields = Fields::from_list(&[
        ("content-type".into(), content_type.as_bytes().to_vec()),
        (
            "content-length".into(),
            bytes.len().to_string().into_bytes(),
        ),
        ("cache-control".into(), b"no-cache".to_vec()),
    ])?;
    let response = OutgoingResponse::new(fields);
    response
        .set_status_code(status)
        .map_err(|()| anyhow::anyhow!("invalid response status"))?;
    let body = response
        .body()
        .map_err(|()| anyhow::anyhow!("response body unavailable"))?;
    let output = body
        .write()
        .map_err(|()| anyhow::anyhow!("response stream unavailable"))?;
    out.set(response);
    // Keep the original Bytes owner (and engine reservation) until all writes finish.
    transport::write_chunk(&output, &bytes).await?;
    drop(output);
    OutgoingBody::finish(body, None)?;
    drop(bytes);
    Ok(())
}

// The SDK generates unsafe ABI exports; handwritten adapter code remains safe.
#[allow(unsafe_code, clippy::same_length_and_capacity)]
mod entry {
    use super::{
        Bytes, Error, IncomingRequest, RECEIVE_RESULT, Reply, ResponseOutparam, dispatch, respond,
    };
    #[cfg_attr(target_arch = "wasm32", spin_sdk::http_component)]
    async fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let reply = match dispatch(request).await {
            Ok(reply) => reply,
            Err(error) => {
                eprintln!("Git request failed: {error:#}");
                match error.downcast::<Error>() {
                    Ok(Error::Busy) => Reply(
                        503,
                        "text/plain",
                        Bytes::from_static(b"Git operation busy\n"),
                    ),
                    Ok(
                        Error::InvalidProtocol(_) | Error::InvalidReference | Error::InvalidPack(_),
                    ) => Reply(
                        400,
                        "text/plain",
                        Bytes::from_static(b"invalid Git request\n"),
                    ),
                    Ok(Error::ReceiveRejected { response, .. }) => {
                        Reply(200, RECEIVE_RESULT, response)
                    }
                    _ => Reply(
                        500,
                        "text/plain",
                        Bytes::from_static(b"Git request failed\n"),
                    ),
                }
            }
        };
        if let Err(error) = respond(out, reply).await {
            eprintln!("Git response failed: {error:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn input_limit_is_checked_before_append() -> anyhow::Result<()> {
        let mut input = vec![0; BODY_LIMIT - 1];
        append(&mut input, b"x")?;
        assert!(append(&mut input, b"y").is_err());
        assert_eq!(input.len(), BODY_LIMIT);
        Ok(())
    }

    fn gzip(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(bytes)?;
        Ok(encoder.finish()?)
    }

    #[test]
    fn compressed_input_is_bounded_after_expansion() -> anyhow::Result<()> {
        let compressed = gzip(&vec![0; BODY_LIMIT + 1])?;
        assert!(compressed.len() < BODY_LIMIT);
        assert!(decode_body(compressed, true).is_err());
        assert_eq!(decode_body(gzip(b"request")?, true)?, b"request"[..]);
        assert!(decode_body(b"bad gzip".to_vec(), true).is_err());
        Ok(())
    }

    #[test]
    fn concatenated_gzip_members_are_preserved() -> anyhow::Result<()> {
        let mut compressed = gzip(b"first")?;
        compressed.extend(gzip(b"second")?);
        assert_eq!(decode_body(compressed, true)?, b"firstsecond"[..]);
        Ok(())
    }
}
