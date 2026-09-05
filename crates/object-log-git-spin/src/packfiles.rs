//! Authenticated pack downloads. The configured URL is deployment metadata, not
//! a signing authority; forwarded URL headers never choose the emitted origin.
use bytes::Bytes;
use object_log_git::{ObjectFormat, ObjectId, PackfileUris};

pub(crate) fn configured(
    base: &str,
    authority: Option<&str>,
) -> anyhow::Result<Option<PackfileUris>> {
    if base.is_empty() {
        return Ok(None);
    }
    let base = PackfileUris::new(base)?;
    let uri: http::Uri = base.as_str().parse()?;
    if uri.authority().map(http::uri::Authority::as_str) != authority {
        return Err(object_log_git::Error::InvalidProtocol("URI deployment authority").into());
    }
    Ok(Some(base))
}

pub(crate) async fn download(
    request: &super::IncomingRequest,
    path: &str,
    format: ObjectFormat,
) -> anyhow::Result<super::Reply> {
    let (blob, checksum) = parse_path(path, format)?;
    let range = super::header(request, "range")?;
    let if_range = super::header(request, "if-range")?;
    let range = Range::parse(&checksum.to_string(), range.as_deref(), if_range.as_deref())?;
    let bytes = super::repository(format)
        .await?
        .fetch_uri_pack(blob, checksum)
        .await?;
    Ok(super::Reply::Pack(reply(
        bytes,
        &checksum.to_string(),
        range,
    )))
}

pub(crate) fn parse_path(path: &str, format: ObjectFormat) -> anyhow::Result<(ObjectId, ObjectId)> {
    let algorithm = match format {
        ObjectFormat::Sha1 => "sha1",
        ObjectFormat::Sha256 => "sha256",
    };
    let path = path
        .strip_prefix("/repo/packfiles/v1/")
        .and_then(|p| p.strip_prefix(algorithm))
        .and_then(|p| p.strip_prefix('/'))
        .and_then(|p| p.strip_suffix(".pack"))
        .ok_or(object_log_git::Error::InvalidProtocol("packfile URI path"))?;
    let (blob, checksum) = path
        .split_once('/')
        .ok_or(object_log_git::Error::InvalidProtocol("packfile URI path"))?;
    if !path
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'/')
    {
        return Err(object_log_git::Error::InvalidProtocol("URI identity").into());
    }
    Ok((
        ObjectId::parse(format, blob)?,
        ObjectId::parse(format, checksum)?,
    ))
}

pub(crate) struct PackReply {
    pub(crate) status: u16,
    pub(crate) bytes: Bytes,
    pub(crate) headers: Vec<(String, String)>,
}

#[derive(Clone, Copy)]
enum Range {
    Full,
    Suffix(u64),
    Bytes(u64, u64),
}

impl Range {
    fn parse(checksum: &str, range: Option<&str>, if_range: Option<&str>) -> anyhow::Result<Self> {
        use object_log_git::Error::InvalidProtocol;
        if range.is_some_and(|v| v.len() > 128) || if_range.is_some_and(|v| v.len() > 128) {
            return Err(InvalidProtocol("range bytes").into());
        }
        let Some(range) = range.filter(|_| if_range.is_none_or(|v| v == format!("\"{checksum}\"")))
        else {
            return Ok(Self::Full);
        };
        let (unit, value) = range
            .split_once('=')
            .ok_or(InvalidProtocol("packfile range"))?;
        if unit != "bytes" {
            return Ok(Self::Full);
        }
        let (start, end) = value
            .split_once('-')
            .ok_or(InvalidProtocol("packfile range"))?;
        let number = |text: &str| -> anyhow::Result<u64> {
            if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
                return Err(InvalidProtocol("range number").into());
            }
            // Values beyond any supported pack length saturate identically on native/WASI.
            Ok(text.bytes().fold(0u64, |value, digit| {
                value
                    .saturating_mul(10)
                    .saturating_add(u64::from(digit - b'0'))
            }))
        };
        if start.is_empty() {
            Ok(Self::Suffix(number(end)?))
        } else {
            Ok(Self::Bytes(
                number(start)?,
                if end.is_empty() {
                    u64::MAX
                } else {
                    number(end)?.saturating_add(1)
                },
            ))
        }
    }
}

fn reply(bytes: Bytes, checksum: &str, range: Range) -> PackReply {
    let length = bytes.len();
    let mut headers = vec![
        ("etag".into(), format!("\"{checksum}\"")),
        ("accept-ranges".into(), "bytes".into()),
        ("cache-control".into(), "private, no-store".into()),
    ];
    let clip = |value: u64| usize::try_from(value).unwrap_or(usize::MAX).min(length);
    let (start, end) = match range {
        Range::Full => {
            return PackReply {
                status: 200,
                bytes,
                headers,
            };
        }
        Range::Suffix(suffix) => (length - clip(suffix), length),
        Range::Bytes(start, end) => (clip(start), clip(end)),
    };
    if start >= end || start >= length {
        headers.push(("content-range".into(), format!("bytes */{length}")));
        return PackReply {
            status: 416,
            bytes: Bytes::new(),
            headers,
        };
    }
    headers.push((
        "content-range".into(),
        format!("bytes {start}-{}/{length}", end - 1),
    ));
    PackReply {
        status: 206,
        bytes: bytes.slice(start..end),
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reply(
        bytes: Bytes,
        checksum: &str,
        range: Option<&str>,
        if_range: Option<&str>,
    ) -> anyhow::Result<PackReply> {
        Ok(super::reply(
            bytes,
            checksum,
            Range::parse(checksum, range, if_range)?,
        ))
    }
    #[test]
    fn ranges_and_authority() -> anyhow::Result<()> {
        assert!(configured("https://example.com/repo", Some("evil.invalid")).is_err());
        assert!(configured("https://example.com/repo", Some("example.com"))?.is_some());
        assert!(configured("", None)?.is_none());
        for (range, status, bytes) in [
            ("bytes=2-", 206, "2345"),
            ("bytes=0-2", 206, "012"),
            ("bytes=-2", 206, "45"),
            ("bytes=6-", 416, ""),
            ("bytes=-0", 416, ""),
            ("bytes=4-2", 416, ""),
            ("items=0-1", 200, "012345"),
            ("bytes=0-4294967296", 206, "012345"),
            ("bytes=-9999999999999999999999999999999", 206, "012345"),
        ] {
            let output = reply(Bytes::from_static(b"012345"), "hash", Some(range), None)?;
            assert_eq!(output.status, status);
            assert_eq!(output.bytes.as_ref(), bytes.as_bytes());
        }
        for range in [
            "bytes=0-1,3-4",
            "bytes=+1-",
            "bytes=a-",
            "bytes=-",
            "garbage",
        ] {
            assert!(reply(Bytes::from_static(b"abc"), "hash", Some(range), None).is_err());
        }
        assert_eq!(
            reply(
                Bytes::from_static(b"abc"),
                "hash",
                Some("bytes=2-"),
                Some("\"other\"")
            )?
            .status,
            200
        );
        Ok(())
    }
}
