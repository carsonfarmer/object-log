use std::mem::size_of;

use bytes::Bytes;
use object_log::{Log, ObjectRef, StagedObject, View};

use super::{Cursor, Input};
use crate::{
    Error, ObjectFormat, ObjectId,
    durable::publication_plan,
    format::PackDescriptor,
    pack::{
        INFLATE_BYTES, MAX_INDEX_BYTES, MAX_OBJECTS, MAX_STREAM_OBJECT_BYTES, SCAN_WINDOW_BYTES,
        budget::{Reservation, hold},
        delta_integer, invalid, object_hash, pack_error,
    },
};

pub(crate) struct Entry {
    pub(crate) header: gix_pack::data::Entry,
    pub(super) end: u64,
    pub(super) crc: u32,
    pub(super) id: Option<ObjectId>,
    pub(crate) result_size: usize,
}

pub(crate) struct Scanned<'a, 'log> {
    pub(super) input: &'a Input<'log>,
    pub(super) entries: Vec<Entry>,
    pub(super) id: ObjectId,
    pub(super) bytes: u64,
    pub(super) _memory: Reservation,
}

impl Scanned<'_, '_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub(super) async fn scan<'a, 'log>(
    input: &'a Input<'log>,
    format: ObjectFormat,
) -> Result<Scanned<'a, 'log>, Error> {
    let hash = object_hash(format);
    let end = input
        .bytes
        .checked_sub(format.digest_len() as u64)
        .filter(|end| *end >= 12)
        .ok_or_else(|| pack_error("input pack is truncated"))?;
    let mut cursor = Cursor::new(input);
    let mut header = [0; 12];
    input.operation.work(header.len())?;
    cursor.read_exact(&mut header).await?;
    let (version, count) = gix_pack::data::header::decode(&header).map_err(pack_error)?;
    if version != gix_pack::data::Version::V2 || count > MAX_OBJECTS {
        return invalid("pack version or object count exceeds policy");
    }
    let memory = input
        .operation
        .reserve(count as usize * size_of::<Entry>())?;
    let mut entries: Vec<Entry> = Vec::with_capacity(count as usize);
    let _codec_memory = input.operation.reserve(INFLATE_BYTES + SCAN_WINDOW_BYTES)?;
    let mut scanner = Scanner {
        window: vec![0; SCAN_WINDOW_BYTES],
        codec: gix_zlib::Decompress::new(),
        pack_hash: gix_hash::hasher(hash),
    };
    scanner.pack_hash.update(&header);
    for _ in 0..count {
        entries.push(scanner.entry(&mut cursor, end, format, &entries).await?);
    }
    if cursor.position != end {
        return invalid("pack object count does not match its entries");
    }
    let mut trailer = [0; 32];
    cursor
        .read_exact(&mut trailer[..format.digest_len()])
        .await?;
    let digest = scanner.pack_hash.try_finalize().map_err(pack_error)?;
    if digest.as_slice() != &trailer[..format.digest_len()] {
        return invalid("pack checksum does not match");
    }
    Ok(Scanned {
        input,
        entries,
        id: ObjectId::from_bytes(format, digest.as_slice())?,
        bytes: input.bytes,
        _memory: memory,
    })
}

struct Scanner {
    pack_hash: gix_hash::Hasher,
    codec: gix_zlib::Decompress,
    window: Vec<u8>,
}

impl Scanner {
    async fn entry(
        &mut self,
        cursor: &mut Cursor<'_, '_>,
        end: u64,
        format: ObjectFormat,
        entries: &[Entry],
    ) -> Result<Entry, Error> {
        let (entry, encoded) = read_header(cursor, end, format).await?;
        let encoded = &encoded[..entry.header_size()];
        self.pack_hash.update(encoded);
        let mut crc = gix_features::hash::crc32(encoded);
        if let gix_pack::data::entry::Header::OfsDelta { base_distance } = entry.header {
            let base = entry
                .checked_base_pack_offset(base_distance)
                .ok_or_else(|| pack_error("invalid OFS_DELTA base"))?;
            if entries
                .binary_search_by_key(&base, |entry| entry.header.pack_offset())
                .is_err()
            {
                return invalid("OFS_DELTA base is not an earlier entry");
            }
        }
        let size = usize::try_from(entry.decompressed_size).map_err(pack_error)?;
        let kind = entry.header.as_kind();
        cursor
            .input
            .operation
            .work((size + 1) * (1 + usize::from(kind.is_some())))?;
        let mut object_hash = kind.map(|kind| {
            let mut hash = gix_hash::hasher(object_hash(format));
            hash.update(&gix_object::encode::loose_header(
                kind,
                entry.decompressed_size,
            ));
            hash
        });
        self.codec.reset();
        let mut prefix = [0; 20];
        let mut captured = 0;
        loop {
            let bytes = cursor.window().await?;
            let available = bytes
                .len()
                .min(usize::try_from(end - cursor.position).map_err(pack_error)?);
            let produced = usize::try_from(self.codec.total_out()).map_err(pack_error)?;
            let capacity = self.window.len().min(size - produced + 1);
            let before = self.codec.total_in();
            let status = self
                .codec
                .decompress(
                    &bytes[..available],
                    &mut self.window[..capacity],
                    gix_zlib::FlushDecompress::None,
                )
                .map_err(pack_error)?;
            let consumed = usize::try_from(self.codec.total_in() - before).map_err(pack_error)?;
            let written = usize::try_from(self.codec.total_out()).map_err(pack_error)? - produced;
            if written > size - produced {
                return invalid("entry exceeds its declared size");
            }
            cursor.input.operation.work(consumed * 2)?;
            self.pack_hash.update(&bytes[..consumed]);
            crc = gix_features::hash::crc32_update(crc, &bytes[..consumed]);
            cursor.position += consumed as u64;
            if let Some(hash) = &mut object_hash {
                hash.update(&self.window[..written]);
            }
            let keep = written.min(prefix.len() - captured);
            prefix[captured..captured + keep].copy_from_slice(&self.window[..keep]);
            captured += keep;
            if status == gix_zlib::Status::StreamEnd {
                if self.codec.total_out() != entry.decompressed_size {
                    return invalid("entry decoded size does not match");
                }
                break;
            }
            if consumed == 0 && written == 0 {
                return invalid("entry zlib stream is truncated or made no progress");
            }
        }
        let result_size = if entry.header.is_delta() {
            let (_, offset) = delta_integer(&prefix[..captured])?;
            delta_integer(&prefix[offset..captured])?.0
        } else {
            size
        };
        if result_size > MAX_STREAM_OBJECT_BYTES {
            return invalid("delta result exceeds object byte limit");
        }
        let id = object_hash
            .map(|hash| {
                let digest = hash.try_finalize().map_err(pack_error)?;
                ObjectId::from_bytes(format, digest.as_slice())
            })
            .transpose()?;
        Ok(Entry {
            header: entry,
            end: cursor.position,
            crc,
            id,
            result_size,
        })
    }
}

pub(super) async fn read_header(
    cursor: &mut Cursor<'_, '_>,
    end: u64,
    format: ObjectFormat,
) -> Result<(gix_pack::data::Entry, [u8; 42]), Error> {
    let start = cursor.position;
    let mut encoded = [0; 42];
    for position in 0..42 {
        if cursor.position >= end {
            return invalid("pack entry header is truncated");
        }
        cursor.input.operation.work(2)?;
        let mut byte = [0];
        cursor.read_exact(&mut byte).await?;
        encoded[position] = byte[0];
        match gix_pack::data::Entry::from_bytes(&encoded[..=position], start, object_hash(format)) {
            Ok(entry) => {
                if entry.header_size() != entry.header.size(entry.decompressed_size)
                    || entry.decompressed_size > MAX_STREAM_OBJECT_BYTES as u64
                    || (entry
                        .header
                        .as_kind()
                        .is_some_and(|kind| kind != gix_object::Kind::Blob)
                        && entry.decompressed_size > crate::pack::MAX_OBJECT_BYTES as u64)
                {
                    return invalid("pack entry header is noncanonical or oversized");
                }
                return Ok((entry, encoded));
            }
            Err(gix_pack::data::entry::decode::Error::Corrupt { .. }) => {}
            Err(error) => return Err(pack_error(error)),
        }
    }
    invalid("pack entry header is too long")
}

impl<'log> Scanned<'_, 'log> {
    /// Only full-object packs can finish until bounded delta resolution exists.
    pub(super) async fn finish(self) -> Result<(PackDescriptor, StagedObject), Error> {
        let (staged, _) = self.finish_inner(false).await?;
        Ok(staged)
    }

    pub(super) async fn finish_certified(self) -> Result<CertifiedPack<'log>, Error> {
        self.finish_inner(true).await
    }

    async fn finish_inner(mut self, certify: bool) -> Result<CertifiedPack<'log>, Error> {
        let input = self.input;
        if self.entries.iter().any(|entry| entry.id.is_none()) {
            return invalid("input requires delta resolution before publication");
        }
        let index = self.index(input)?;
        let root_bytes = input.log.node_size(
            index.len(),
            input.chunks.iter().map(|chunk| chunk.reference().len()),
        )?;
        let _root_memory = input.operation.reserve(root_bytes)?;
        input.operation.work(root_bytes)?;

        let _plan_memory = publication_plan(&input.operation, input.view)?;
        let _proof_memory = input
            .operation
            .reserve(input.chunks.len() * size_of::<StagedObject>())?;
        let root = input
            .log
            .put_node(input.view, index, input.chunks.clone())
            .await?;
        let descriptor = PackDescriptor {
            id: self.id,
            bytes: self.bytes,
        };
        let certificate = if certify {
            let memory = input
                .operation
                .reserve(self.entries.len() * size_of::<(ObjectId, gix_object::Kind)>())?;
            input
                .operation
                .work(self.entries.len() * size_of::<Entry>())?;
            let mut entries = Vec::with_capacity(self.entries.len());
            for entry in &self.entries {
                entries.push((
                    entry
                        .id
                        .ok_or_else(|| pack_error("uncertified object ID"))?,
                    entry
                        .header
                        .header
                        .as_kind()
                        .ok_or_else(|| pack_error("cannot certify delta entry"))?,
                ));
            }
            Some(ScanCertificate {
                log: input.log,
                view: input.view,
                operation: input.operation.clone(),
                root: root.reference().clone(),
                descriptor: descriptor.clone(),
                entries,
                _memory: memory,
            })
        } else {
            None
        };
        Ok(((descriptor, root), certificate))
    }

    fn index(&mut self, input: &Input<'_>) -> Result<Bytes, Error> {
        let count = self.entries.len();
        let width = self.id.format().digest_len();
        let size = 1032 + count * (width + 8) + width * 2;
        if size > MAX_INDEX_BYTES
            || self
                .entries
                .iter()
                .any(|entry| entry.header.pack_offset() >= 0x8000_0000)
        {
            return invalid("prototype index size or offset exceeds policy");
        }
        let memory = input.operation.reserve(size)?;
        input
            .operation
            .work(size * 2 + count * (count.max(1).ilog2() as usize + 1) * width)?;
        self.entries.sort_unstable_by_key(|entry| entry.id);
        if self.entries.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return invalid("pack contains duplicate object IDs");
        }
        let mut fan = [0_u32; 256];
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(b"\xfftOc\0\0\0\x02");
        for entry in &self.entries {
            let id = entry
                .id
                .ok_or_else(|| pack_error("unresolved index object"))?;
            fan[usize::from(id.as_bytes()[0])] += 1;
        }
        let mut total = 0;
        for count in fan {
            total += count;
            bytes.extend_from_slice(&total.to_be_bytes());
        }
        for entry in &self.entries {
            bytes.extend_from_slice(
                entry
                    .id
                    .ok_or_else(|| pack_error("unresolved index object"))?
                    .as_bytes(),
            );
        }
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.crc.to_be_bytes());
        }
        for entry in &self.entries {
            bytes.extend_from_slice(
                &u32::try_from(entry.header.pack_offset())
                    .map_err(pack_error)?
                    .to_be_bytes(),
            );
        }
        bytes.extend_from_slice(self.id.as_bytes());
        let mut hash = gix_hash::hasher(object_hash(self.id.format()));
        hash.update(&bytes);
        bytes.extend_from_slice(hash.try_finalize().map_err(pack_error)?.as_slice());
        Ok(hold(bytes.into(), memory))
    }
}

/// A move-only proof from a complete full-entry scan and its authenticated index.
/// It lives only inside the receive attempt; replay never retains it.
pub(crate) struct ScanCertificate<'a> {
    log: &'a Log,
    view: &'a View,
    operation: crate::pack::budget::Operation,
    root: ObjectRef,
    descriptor: PackDescriptor,
    entries: Vec<(ObjectId, gix_object::Kind)>,
    _memory: Reservation,
}

pub(super) type CertifiedPack<'a> = ((PackDescriptor, StagedObject), Option<ScanCertificate<'a>>);

impl ScanCertificate<'_> {
    pub(crate) fn matches_context(
        &self,
        operation: &crate::pack::budget::Operation,
        log: &Log,
        view: &View,
    ) -> bool {
        self.operation.same_as(operation)
            && std::ptr::eq(self.log, log)
            && std::ptr::eq(self.view, view)
    }

    pub(crate) fn verifies_blob(
        &self,
        root: &ObjectRef,
        descriptor: &PackDescriptor,
        position: u32,
        id: ObjectId,
    ) -> bool {
        self.root == *root
            && self.descriptor == *descriptor
            && self.entries.get(position as usize) == Some(&(id, gix_object::Kind::Blob))
    }
}
