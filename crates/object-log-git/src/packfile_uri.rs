//! Versioned, reconstructible pack representations; URLs grant no authority.
use std::io::Write;

use bytes::Bytes;

use crate::{
    Error, ObjectFormat, ObjectId,
    pack::{
        self,
        budget::{Operation, hold},
    },
};

pub(crate) const MAX_URIS: usize = 8;
const MIN_BLOB_BYTES: usize = 64 * 1024;

/// Opt-in canonical repository URL for authenticated packfile URI downloads.
///
/// Private Git clients need proactive HTTP authentication for URI requests.
/// URLs retain no objects and remain available only while the blob is reachable.
#[derive(Clone, Debug)]
pub struct PackfileUris(url::Url);

impl PackfileUris {
    /// Validates an absolute canonical repository URL without credentials.
    /// HTTPS is required except for loopback HTTP fixtures.
    ///
    /// # Errors
    /// Rejects noncanonical URLs, credentials, query/fragment, or unsafe schemes.
    pub fn new(base: &str) -> Result<Self, Error> {
        let url = url::Url::parse(base).map_err(|_| Error::InvalidProtocol("URI base URL"))?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain("localhost")) => true,
            _ => false,
        };
        if base.len() > 1024
            || url.as_str() != base
            || base.ends_with('/')
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
            || url.path().contains('%')
            || url.path() != "/repo"
        {
            return Err(Error::InvalidProtocol("URI base URL"));
        }
        Ok(Self(url))
    }

    /// Canonical configured URL, including repository path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns discovery with opt-in packfile URI support.
    #[must_use]
    pub fn advertisement(&self, format: ObjectFormat) -> Bytes {
        Bytes::from_static(crate::wire::uri_advertisement(format))
    }

    pub(crate) fn accepts(&self, protocols: &[u8]) -> bool {
        protocols
            .split(|b| *b == b',')
            .any(|scheme| scheme == self.0.scheme().as_bytes())
    }

    pub(crate) fn uri(&self, blob: ObjectId, checksum: ObjectId) -> String {
        let format = match blob.format() {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        };
        format!(
            "{}/packfiles/v1/{format}/{blob}/{checksum}.pack",
            self.as_str()
        )
    }

    pub(crate) const fn eligible(bytes: usize) -> bool {
        bytes >= MIN_BLOB_BYTES
    }
}

/// v1 always encodes a full blob with the pinned gix-zlib recipe. Never copy
/// storage compression: moving the blob between full/delta packs must not change
/// this representation. Golden tests are an encoder compatibility obligation.
pub(crate) fn canonical(
    operation: &Operation,
    format: ObjectFormat,
    data: &[u8],
) -> Result<(ObjectId, Bytes), Error> {
    if data.len() > pack::MAX_OBJECT_BYTES {
        return Err(Error::InvalidProtocol("URI blob bytes"));
    }
    let capacity = data.len() + data.len() / 16 + 1024;
    let memory = operation.reserve(capacity)?;
    let _compress = operation.reserve(pack::COMPRESS_BYTES)?;
    operation.work(data.len() + capacity)?;
    let mut writer = gix_hash::io::Write::new(
        Output(Vec::with_capacity(capacity)),
        pack::object_hash(format),
    );
    writer
        .write_all(&gix_pack::data::header::encode(
            gix_pack::data::Version::V2,
            1,
        ))
        .map_err(pack::pack_error)?;
    gix_pack::data::entry::Header::Blob
        .write_to(data.len() as u64, &mut writer)
        .map_err(pack::pack_error)?;
    let mut compressor =
        gix_zlib::stream::deflate::Write::new(&mut writer, gix_zlib::Compression::DEFAULT);
    compressor
        .write_all(data)
        .and_then(|()| compressor.flush())
        .map_err(pack::pack_error)?;
    drop(compressor);
    let gix_hash::io::Write { hash, mut inner } = writer;
    let digest = hash.try_finalize().map_err(pack::pack_error)?;
    let id = ObjectId::from_bytes(format, digest.as_slice())?;
    inner
        .write_all(digest.as_slice())
        .map_err(pack::pack_error)?;
    Ok((id, hold(Bytes::from(inner.0), memory)))
}

struct Output(Vec<u8>);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0.capacity() - self.0.len() {
            return Err(std::io::Error::other("URI encoding memory"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uri_base_is_canonical_and_safe() {
        for base in [
            "http://127.0.0.1:9000/repo",
            "https://example.com/repo",
            "http://[::1]:9000/repo",
        ] {
            assert!(PackfileUris::new(base).is_ok());
        }
        for base in [
            "http://example.com/repo",
            "https://user:secret@example.com/repo",
            "https://example.com/repo?token=x",
            "https://example.com/repo#x",
            "https://EXAMPLE.com/repo",
            "https://example.com/repo/",
            "https://example.com/%2frepo",
            "https://example.com/other",
            "https://example.com/prefix/repo",
        ] {
            assert!(PackfileUris::new(base).is_err(), "{base}");
        }
    }
    #[test]
    fn canonical_golden_both_formats() -> Result<(), Box<dyn std::error::Error>> {
        let data = vec![b'x'; 65536];
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let operation =
                crate::pack::budget::Pool::new(crate::pack::budget::LIVE_BYTES).admit()?;
            let (checksum, pack) = canonical(&operation, format, &data)?;
            assert_eq!(
                checksum.to_string(),
                match format {
                    ObjectFormat::Sha1 => "65a6a6777e4a8a3e4c3213fc9035542b03598d3a",
                    ObjectFormat::Sha256 =>
                        "a894426233f065e1789eeedf854ba8ff847a23c01e55b62336bf5080b0010524",
                }
            );
            eprintln!("URI golden {format:?}: {checksum}, {} bytes", pack.len());
            assert_eq!(
                pack.len(),
                match format {
                    ObjectFormat::Sha1 => 119,
                    ObjectFormat::Sha256 => 131,
                }
            );
            assert_eq!(pack, canonical(&operation, format, &data)?.1);
            assert_eq!(&pack[..12], b"PACK\0\0\0\x02\0\0\0\x01");
        }
        Ok(())
    }
}
