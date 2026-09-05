//! Spin HTTP and S3 transport adapters for the shared Git repository engine.
#![cfg_attr(
    not(target_arch = "wasm32"),
    allow(
        dead_code,
        reason = "Native builds test helpers; the HTTP export requires WASI."
    )
)]

mod auth;
mod packfiles;
mod receive_body;
mod transport;

use std::{io::Read, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use object_log::{Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_git::{Error, ObjectFormat, ReceivePolicy, Repository};
use object_store::{RetryConfig, aws::AmazonS3Builder, client::ClientOptions, path::Path};
use spin_sdk::http::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

const BODY_LIMIT: usize = 10 * 1024 * 1024;
const UPLOAD_RESULT: &str = "application/x-git-upload-pack-result";
const RECEIVE_RESULT: &str = "application/x-git-receive-pack-result";

enum Reply {
    Normal(u16, &'static str, Bytes),
    Pack(packfiles::PackReply),
}

async fn repository(format: ObjectFormat) -> anyhow::Result<Repository> {
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
        .with_http_connector(transport::Transport::default())
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
            .is_none_or(|value| value.eq_ignore_ascii_case("identity")
                || value.eq_ignore_ascii_case("gzip")),
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

fn http_policy() -> anyhow::Result<(auth::AuthConfig, bool, ReceivePolicy, ObjectFormat)> {
    // Validate the entire HTTP policy before credentials, storage, or body access.
    let variable = spin_sdk::variables::get;
    let config = auth::AuthConfig::parse(
        &variable("auth_mode")?,
        &variable("auth_read_token")?,
        &variable("auth_write_token")?,
    )
    .map_err(anyhow::Error::msg)?;
    let read_only = variable("read_only")?.parse::<bool>()?;
    let policy = if variable("allow_non_fast_forward")?.parse::<bool>()? {
        ReceivePolicy::AllowNonFastForward
    } else {
        ReceivePolicy::FastForwardOnly
    };
    let format = object_format()?;
    Ok((config, read_only, policy, format))
}

fn auth_rejection(
    request: &IncomingRequest,
    config: &auth::AuthConfig,
    write: bool,
    read_only: bool,
) -> Option<Reply> {
    let authorization = request.headers().get("authorization");
    if let Err(denied) = config.authorize(authorization.iter().map(Vec::as_slice), write, read_only)
    {
        return Some(match denied {
            auth::Denied::Unauthorized => Reply::Normal(
                401,
                "text/plain",
                Bytes::from_static(b"authentication required\n"),
            ),
            auth::Denied::Forbidden => {
                Reply::Normal(403, "text/plain", Bytes::from_static(b"access forbidden\n"))
            }
        });
    }
    None
}

async fn dispatch(request: IncomingRequest) -> anyhow::Result<Reply> {
    let path = request.path_with_query().unwrap_or_default();
    let upload = path == "/repo/git-upload-pack";
    let receive = path == "/repo/git-receive-pack";
    let upload_advert = path == "/repo/info/refs?service=git-upload-pack";
    let receive_advert = path == "/repo/info/refs?service=git-receive-pack";
    let packfile = path.starts_with("/repo/packfiles/");
    let get = request.method() == Method::Get;
    let post = request.method() == Method::Post;
    if !(get && (upload_advert || receive_advert || packfile) || post && (upload || receive)) {
        return Ok(Reply::Normal(
            404,
            "text/plain",
            Bytes::from_static(b"not found\n"),
        ));
    }
    let (config, read_only, policy, format) = http_policy()?;
    if let Some(reply) = auth_rejection(&request, &config, receive || receive_advert, read_only) {
        return Ok(reply);
    }
    let uri_base = packfiles::configured(
        spin_sdk::variables::get("packfile_uri_base")?.as_str(),
        request.authority().as_deref(),
    )?;
    if packfile {
        if uri_base.is_none() {
            return Ok(Reply::Normal(404, "text/plain", Bytes::new()));
        }
        return packfiles::download(&request, &path, format).await;
    }
    let Ok(encoding) = validate_headers(&request, upload, upload_advert, post) else {
        return Ok(Reply::Normal(
            400,
            "text/plain",
            Bytes::from_static(b"invalid Git HTTP request\n"),
        ));
    };
    if upload_advert {
        return Ok(Reply::Normal(
            200,
            "application/x-git-upload-pack-advertisement",
            uri_base.as_ref().map_or_else(
                || Repository::upload_advertisement(format),
                |base| base.advertisement(format),
            ),
        ));
    }
    // Acquire engine admission before reading a body. Receive decoding retains
    // only fixed host buffers before each frame transfers to engine accounting.
    let repository = repository(format).await?;
    if receive_advert {
        return Ok(Reply::Normal(
            200,
            "application/x-git-receive-pack-advertisement",
            repository.receive_advertisement().await?,
        ));
    }
    let gzip = encoding
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    if upload {
        let Ok(body) = body(request, gzip).await else {
            return Ok(Reply::Normal(
                400,
                "text/plain",
                Bytes::from_static(b"invalid or oversized request body\n"),
            ));
        };
        return Ok(Reply::Normal(
            200,
            UPLOAD_RESULT,
            if let Some(base) = &uri_base {
                repository.upload_pack_with_uris(body, base).await?
            } else {
                repository.upload_pack(body).await?
            },
        ));
    }
    let source = request
        .into_body_stream()
        .map(|chunk| chunk.map_err(|_| std::io::Error::other("HTTP request body failed")));
    let Some(frames) = receive_body::frames(source, gzip).await? else {
        return Ok(Reply::Normal(200, RECEIVE_RESULT, Bytes::new()));
    };
    receive_stream(repository, frames, policy).await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
async fn receive_command(
    repository: Repository,
    body: Bytes,
    policy: ReceivePolicy,
) -> anyhow::Result<Reply> {
    let source = futures::stream::iter(
        body.chunks(16 * 1024)
            .map(|chunk| Ok(chunk.to_vec()))
            .collect::<Vec<_>>(),
    );
    let Some(frames) = receive_body::frames(source, false).await? else {
        return Ok(Reply::Normal(200, RECEIVE_RESULT, Bytes::new()));
    };
    receive_stream(repository, frames, policy).await
}

async fn receive_stream<S>(
    repository: Repository,
    frames: S,
    policy: ReceivePolicy,
) -> anyhow::Result<Reply>
where
    S: futures::Stream<Item = Result<Bytes, Error>> + Unpin,
{
    match repository
        .prepare_receive_stream_with_policy(TransactionId::new(), frames, policy)
        .await
    {
        Ok(prepared) => {
            let token = prepared.recovery_token().clone();
            match prepared.publish_receive().await {
                Ok((
                    object_log::Resolution::Committed(_) | object_log::Resolution::NotCommitted(_),
                    response,
                )) => Ok(Reply::Normal(200, RECEIVE_RESULT, response)),
                Ok((
                    object_log::Resolution::StillPending(_) | object_log::Resolution::Expired(_),
                    _,
                ))
                | Err(_) => {
                    // Return the opaque exact-candidate token without allocating an
                    // encoded copy. A 503 never claims successful publication.
                    Ok(Reply::Normal(503, "application/octet-stream", token))
                }
            }
        }
        Err(Error::ReceiveRejected { response, .. }) => {
            Ok(Reply::Normal(200, RECEIVE_RESULT, response))
        }
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

async fn respond(out: ResponseOutparam, reply: Reply) -> anyhow::Result<()> {
    let (status, content_type, bytes, extra) = match reply {
        Reply::Normal(status, content_type, bytes) => (status, content_type, bytes, Vec::new()),
        Reply::Pack(pack) => (
            pack.status,
            "application/x-git-packed-objects",
            pack.bytes,
            pack.headers,
        ),
    };
    let fields = Fields::from_list(&[
        ("content-type".into(), content_type.as_bytes().to_vec()),
        (
            "content-length".into(),
            bytes.len().to_string().into_bytes(),
        ),
        ("cache-control".into(), b"no-cache".to_vec()),
    ])?;
    for (name, value) in extra {
        fields.set(&name, &[value.into_bytes()])?;
    }
    if status == 401 {
        fields.set(
            "www-authenticate",
            &[b"Basic realm=\"object-log Git\"".to_vec()],
        )?;
    }
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
                eprintln!("Git request failed");
                let transport_limit = error
                    .chain()
                    .any(<dyn std::error::Error + 'static>::is::<super::transport::QuotaExceeded>);
                match error.downcast::<Error>() {
                    Ok(Error::Busy) => Reply::Normal(
                        503,
                        "text/plain",
                        Bytes::from_static(b"Git operation busy\n"),
                    ),
                    Ok(Error::ObjectLog(object_log::Error::RequestDenied)) => Reply::Normal(
                        503,
                        "text/plain",
                        Bytes::from_static(b"Git operation limit reached\n"),
                    ),
                    Ok(
                        Error::InvalidProtocol(_)
                        | Error::InvalidObjectId
                        | Error::InvalidReference
                        | Error::InvalidPack(_),
                    ) => Reply::Normal(
                        400,
                        "text/plain",
                        Bytes::from_static(b"invalid Git request\n"),
                    ),
                    Ok(Error::ReceiveRejected { response, .. }) => {
                        Reply::Normal(200, RECEIVE_RESULT, response)
                    }
                    _ if transport_limit => Reply::Normal(
                        503,
                        "text/plain",
                        Bytes::from_static(b"Git operation limit reached\n"),
                    ),
                    _ => Reply::Normal(
                        500,
                        "text/plain",
                        Bytes::from_static(b"Git request failed\n"),
                    ),
                }
            }
        };
        if respond(out, reply).await.is_err() {
            eprintln!("Git response failed");
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
