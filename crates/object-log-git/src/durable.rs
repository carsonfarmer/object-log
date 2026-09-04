use std::io::{self, Write};
use std::{collections::VecDeque, mem::size_of, path::PathBuf};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream};
use object_log::{Log, ObjectKind, ObjectRef, ReferenceNode, StagedObject, View};

use crate::{
    Error, ObjectFormat, ObjectId,
    format::PackDescriptor,
    pack::{
        COMPRESS_BYTES, INFLATE_BYTES, MAX_DELTA_DEPTH, MAX_FETCH_PACK_BYTES, MAX_INDEX_BYTES,
        MAX_OBJECT_BYTES, MAX_OBJECTS, MAX_PACK_BYTES, Normalized,
        budget::{Operation, Reservation, hold},
        delta_integer, invalid, object_hash, pack_error,
    },
};

const CHUNK_BYTES: usize = 1024 * 1024;
// Canonical CBOR adds 1,045 bytes for the envelope and 16 chunk references.
const MAX_PACK_ROOT_BYTES: usize = MAX_INDEX_BYTES + 1_045;
const MAX_CATALOG_BYTES: usize = 24 * CHUNK_BYTES;
const MAX_CACHE_BYTES: usize = 8 * CHUNK_BYTES;
const MAX_TRANSFERS: usize = 8;

type PackIndex = gix_pack::index::File<Bytes>;
type PackEntry = gix_pack::data::Entry;
type EntryHeader = gix_pack::data::entry::Header;

pub(crate) async fn stage(
    operation: &Operation,
    log: &Log,
    view: &View,
    normalized: Normalized,
) -> Result<(PackDescriptor, StagedObject), Error> {
    let bytes = Bytes::from(normalized.bytes);
    let count = bytes.len().div_ceil(CHUNK_BYTES);
    if count > log.options().max_object_refs {
        return invalid("pack needs too many chunks");
    }
    let staging_bytes = count
        .checked_mul(size_of::<StagedObject>() + size_of::<ObjectRef>())
        .ok_or_else(|| Error::InvalidPack("Git staging size overflowed".into()))?;
    let _staging_memory = operation.reserve(staging_bytes)?;
    let _root_memory = operation.reserve(MAX_PACK_ROOT_BYTES)?;
    operation.work(MAX_PACK_ROOT_BYTES)?;
    operation.io(MAX_PACK_ROOT_BYTES)?;
    let children = stream::iter((0..count).map(|index| {
        let chunk = bytes.slice(index * CHUNK_BYTES..bytes.len().min((index + 1) * CHUNK_BYTES));
        async move {
            operation.io(chunk.len())?;
            Ok::<_, Error>(log.put_object(view, chunk).await?)
        }
    }))
    .buffered(MAX_TRANSFERS)
    .try_collect()
    .await?;
    let root = log
        .put_node(view, Bytes::from(normalized.index), children)
        .await?;
    Ok((
        PackDescriptor {
            id: normalized.id,
            bytes: bytes.len() as u64,
        },
        root,
    ))
}

pub(crate) struct Catalog {
    format: ObjectFormat,
    packs: Box<[Pack]>,
    directory: Vec<Location>,
    operation: Operation,
    _memory: Reservation,
}

impl Catalog {
    fn location(&self, id: ObjectId) -> Option<Location> {
        self.directory
            .binary_search_by(|location| oid(&self.packs, *location).cmp(id.as_bytes()))
            .ok()
            .map(|position| self.directory[position])
    }
}

struct Pack {
    id: ObjectId,
    bytes: u32,
    index: PackIndex,
    offsets: Box<[OffsetEntry]>,
    node: ReferenceNode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Location {
    pack: u16,
    index: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OffsetEntry {
    offset: u32,
    index: u32,
}

pub(crate) async fn load(
    operation: &Operation,
    log: &Log,
    view: &View,
    format: ObjectFormat,
    roots: &[(PackDescriptor, ObjectRef)],
) -> Result<Catalog, Error> {
    if roots.len() > usize::from(u16::MAX) {
        return invalid("catalog has too many packs");
    }
    let memory = operation.reserve(catalog_bytes(format, roots)?)?;
    for (_, root) in roots {
        let root_bytes = usize::try_from(root.len())
            .map_err(|_| Error::InvalidPack("pack root exceeds memory".into()))?;
        operation.work(root_bytes)?;
        operation.io(root_bytes)?;
    }
    let loads = stream::iter(roots.iter().map(|(descriptor, root)| async move {
        load_pack(log, view, format, descriptor, root).await
    }))
    .buffered(MAX_TRANSFERS);
    futures::pin_mut!(loads);
    let mut packs = Vec::with_capacity(roots.len());
    let mut entries = 0;
    while let Some(pack) = loads.try_next().await? {
        entries += pack.index.num_objects() as usize;
        packs.push(pack);
    }
    let mut directory = Vec::with_capacity(entries);
    for (pack, stored) in packs.iter().enumerate() {
        let pack = u16::try_from(pack)
            .map_err(|_| Error::InvalidPack("catalog has too many packs".into()))?;
        directory.extend((0..stored.index.num_objects()).map(|index| Location { pack, index }));
    }
    directory.sort_unstable_by(|a, b| {
        oid(&packs, *a)
            .cmp(oid(&packs, *b))
            .then_with(|| {
                packs[usize::from(a.pack)]
                    .id
                    .cmp(&packs[usize::from(b.pack)].id)
            })
            .then_with(|| a.index.cmp(&b.index))
    });
    directory.dedup_by(|a, b| oid(&packs, *a) == oid(&packs, *b));
    Ok(Catalog {
        format,
        packs: packs.into_boxed_slice(),
        directory,
        operation: operation.clone(),
        _memory: memory,
    })
}

fn catalog_bytes(
    format: ObjectFormat,
    roots: &[(PackDescriptor, ObjectRef)],
) -> Result<usize, Error> {
    let hash = format.digest_len();
    let index_fixed = 1_032 + 2 * hash;
    let index_entry = hash + 8;
    roots.iter().try_fold(
        roots.len() * size_of::<Pack>(),
        |total, (descriptor, root)| {
            validate_pack_ref(format, descriptor, root)?;
            let root = usize::try_from(root.len())
                .map_err(|_| Error::InvalidPack("pack root exceeds memory".into()))?;
            let entries =
                ((root.saturating_sub(index_fixed)) / index_entry).min(MAX_OBJECTS as usize);
            let dynamic = root
                + MAX_PACK_BYTES.div_ceil(CHUNK_BYTES) * size_of::<ObjectRef>()
                + entries * (size_of::<OffsetEntry>() + size_of::<Location>());
            total
                .checked_add(dynamic)
                .filter(|total| *total <= MAX_CATALOG_BYTES)
                .ok_or_else(|| Error::InvalidPack("catalog exceeds byte limit".into()))
        },
    )
}

async fn load_pack(
    log: &Log,
    view: &View,
    format: ObjectFormat,
    descriptor: &PackDescriptor,
    root: &ObjectRef,
) -> Result<Pack, Error> {
    let bytes = usize::try_from(descriptor.bytes)
        .map_err(|_| Error::InvalidPack("pack length exceeds memory".into()))?;
    let node = log.read_node(view, root).await?;
    let chunks = node.children();
    if chunks.len() != bytes.div_ceil(CHUNK_BYTES) {
        return invalid("pack chunk count does not match");
    }
    for (index, child) in chunks.iter().enumerate() {
        let expected = if index + 1 == chunks.len() {
            bytes - index * CHUNK_BYTES
        } else {
            CHUNK_BYTES
        };
        if child.kind() != ObjectKind::Blob || child.len() != expected as u64 {
            return invalid("pack chunk is invalid");
        }
    }
    let (index, offsets) = validate_index(node.payload(), format, descriptor)?;
    Ok(Pack {
        id: descriptor.id,
        bytes: u32::try_from(bytes)
            .map_err(|_| Error::InvalidPack("pack length exceeds u32".into()))?,
        index,
        offsets,
        node,
    })
}

fn validate_pack_ref(
    format: ObjectFormat,
    descriptor: &PackDescriptor,
    root: &ObjectRef,
) -> Result<(), Error> {
    if root.kind() != ObjectKind::Node || descriptor.id.format() != format {
        return invalid("pack root or object format is invalid");
    }
    if root.len() > MAX_PACK_ROOT_BYTES as u64 {
        return invalid("pack root exceeds byte limit");
    }
    let hash_len = descriptor.id.as_bytes().len() as u64;
    if descriptor.bytes > MAX_PACK_BYTES as u64 || descriptor.bytes < 12 + hash_len {
        return invalid("pack byte length is out of range");
    }
    Ok(())
}

fn validate_index(
    bytes: &Bytes,
    format: ObjectFormat,
    descriptor: &PackDescriptor,
) -> Result<(PackIndex, Box<[OffsetEntry]>), Error> {
    if bytes.len() > MAX_INDEX_BYTES {
        return invalid("pack index exceeds byte limit");
    }
    let hash = object_hash(format);
    let index = gix_pack::index::File::from_data(bytes.clone(), PathBuf::new(), hash)
        .map_err(pack_error)?;
    if index.version() != gix_pack::index::Version::V2 || index.num_objects() > MAX_OBJECTS {
        return invalid("pack index version or object count is unsupported");
    }
    let hash_len = descriptor.id.as_bytes().len();
    let footer = bytes
        .len()
        .checked_sub(hash_len * 2)
        .ok_or_else(|| Error::InvalidPack("pack index footer is truncated".into()))?;
    if &bytes[footer..footer + hash_len] != descriptor.id.as_bytes() {
        return invalid("index pack checksum does not match descriptor");
    }
    let mut hasher = gix_hash::hasher(hash);
    hasher.update(&bytes[..bytes.len() - hash_len]);
    if hasher.try_finalize().map_err(pack_error)?.as_slice() != &bytes[bytes.len() - hash_len..] {
        return invalid("pack index checksum does not match");
    }

    let count = index.num_objects();
    let mut exact_fan = [0_u32; 256];
    let mut previous = None;
    let pack_end = descriptor.bytes - hash_len as u64;
    let mut offsets = Vec::with_capacity(count as usize);
    for (position, entry) in index.iter().enumerate() {
        if entry.oid.as_slice().iter().all(|byte| *byte == 0)
            || previous.is_some_and(|oid: gix_hash::ObjectId| oid >= entry.oid)
        {
            return invalid("pack index IDs are not strictly ordered");
        }
        exact_fan[usize::from(entry.oid.as_slice()[0])] += 1;
        previous = Some(entry.oid);
        if !(12..pack_end).contains(&entry.pack_offset) {
            return invalid("pack index offset is outside the pack");
        }
        offsets.push(OffsetEntry {
            offset: u32::try_from(entry.pack_offset)
                .map_err(|_| Error::InvalidPack("pack offset exceeds u32".into()))?,
            index: u32::try_from(position)
                .map_err(|_| Error::InvalidPack("pack index position exceeds u32".into()))?,
        });
    }
    let mut cumulative = 0_u32;
    for item in &mut exact_fan {
        cumulative += *item;
        *item = cumulative;
    }
    if bytes[8..8 + 256 * 4]
        .chunks_exact(4)
        .zip(exact_fan)
        .any(|(stored, expected)| stored != expected.to_be_bytes())
    {
        return invalid("pack fan table is not exact");
    }
    offsets.sort_unstable_by_key(|entry| entry.offset);
    if offsets
        .windows(2)
        .any(|pair| pair[0].offset == pair[1].offset)
    {
        return invalid("pack index offsets are not unique");
    }
    Ok((index, offsets.into_boxed_slice()))
}

fn oid(packs: &[Pack], location: Location) -> &[u8] {
    packs[usize::from(location.pack)].oid(location.index)
}

impl Pack {
    fn oid(&self, index: u32) -> &[u8] {
        self.index.oid_at_index(index).as_bytes()
    }

    #[allow(clippy::expect_used, reason = "validate_index requires v2")]
    fn crc(&self, index: u32) -> u32 {
        self.index
            .crc32_at_index(index)
            .expect("validated v2 index has CRCs")
    }

    #[allow(clippy::expect_used, reason = "validate_index bounds every offset")]
    fn offset(&self, index: u32) -> u32 {
        u32::try_from(self.index.pack_offset_at_index(index))
            .expect("validated pack offsets fit u32")
    }

    #[allow(clippy::expect_used, reason = "validate_index proves the range")]
    fn entry_range(&self, index: u32) -> std::ops::Range<u32> {
        let start = self.offset(index);
        let position = self
            .offsets
            .binary_search_by_key(&start, |entry| entry.offset)
            .expect("validated pack offset is indexed");
        let trailer = u32::try_from(self.id.as_bytes().len()).expect("Git digest length fits u32");
        let end = self
            .offsets
            .get(position + 1)
            .map_or(self.bytes - trailer, |entry| entry.offset);
        start..end
    }

    fn base(&self, entry: &PackEntry) -> Result<Option<u32>, Error> {
        match entry.header {
            EntryHeader::OfsDelta { base_distance } => {
                let base = entry
                    .checked_base_pack_offset(base_distance)
                    .and_then(|offset| u32::try_from(offset).ok())
                    .and_then(|offset| {
                        self.offsets
                            .binary_search_by_key(&offset, |entry| entry.offset)
                            .ok()
                    })
                    .map(|position| self.offsets[position].index)
                    .ok_or_else(|| Error::InvalidPack("OFS_DELTA base is missing".into()))?;
                Ok(Some(base))
            }
            EntryHeader::RefDelta { base_id } => {
                Ok(Some(self.index.lookup(base_id).ok_or_else(|| {
                    Error::InvalidPack("REF_DELTA base is missing".into())
                })?))
            }
            _ => Ok(None),
        }
    }
}

pub(crate) struct Object {
    pub(crate) kind: gix_object::Kind,
    pub(crate) data: Bytes,
}

pub(crate) struct Reader<'a> {
    log: &'a Log,
    view: &'a View,
    catalog: &'a Catalog,
    cache: VecDeque<((u16, u16), Bytes)>,
    cache_bytes: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(log: &'a Log, view: &'a View, catalog: &'a Catalog) -> Self {
        Self {
            log,
            view,
            catalog,
            cache: VecDeque::new(),
            cache_bytes: 0,
        }
    }

    pub(crate) async fn find(&mut self, id: ObjectId) -> Result<Option<Object>, Error> {
        let Some(location) = self.catalog.location(id) else {
            return Ok(None);
        };
        let pack = &self.catalog.packs[usize::from(location.pack)];
        let mut current = location.index;
        let delta_capacity = MAX_DELTA_DEPTH.min(pack.index.num_objects() as usize);
        let _read_memory = self
            .catalog
            .operation
            .reserve(pack.index.num_objects() as usize + delta_capacity * size_of::<Bytes>())?;
        let mut visited = vec![false; pack.index.num_objects() as usize];
        let mut deltas = Vec::with_capacity(delta_capacity);
        let (kind, mut data) = loop {
            let slot = visited
                .get_mut(current as usize)
                .ok_or_else(|| Error::InvalidPack("delta base index is invalid".into()))?;
            if *slot {
                return invalid("delta graph contains a cycle");
            }
            *slot = true;
            let (entry, data) = self
                .entry(location.pack, current, deltas.is_empty())
                .await?;
            if let Some(base) = pack.base(&entry)? {
                if deltas.len() == MAX_DELTA_DEPTH {
                    return invalid("delta graph is too deep");
                }
                current = base;
                deltas.push(data);
            } else {
                let kind = entry
                    .header
                    .as_kind()
                    .ok_or_else(|| Error::InvalidPack("pack object kind is invalid".into()))?;
                break (kind, data);
            }
        };
        while let Some(delta) = deltas.pop() {
            data = apply_delta(&self.catalog.operation, &data, &delta, deltas.is_empty())?;
        }
        let actual = gix_object::compute_hash(object_hash(pack.id.format()), kind, &data)
            .map_err(pack_error)?;
        if actual.as_slice() != id.as_bytes() {
            return invalid("decoded object ID does not match");
        }
        Ok(Some(Object { kind, data }))
    }

    #[allow(clippy::expect_used, reason = "fetch count is bounded by MAX_OBJECTS")]
    pub(crate) async fn fetch_pack(&mut self, ids: &[ObjectId]) -> Result<Bytes, Error> {
        let format = self.catalog.format;
        if ids.len() > MAX_OBJECTS as usize || ids.iter().any(|id| id.format() != format) {
            return invalid("fetch selection is invalid");
        }
        let selected_bytes =
            ids.len() * (size_of::<ObjectId>() + size_of::<(Location, ObjectId, u32)>());
        let _selected_memory = self.catalog.operation.reserve(selected_bytes)?;
        let mut selected = ids.to_vec();
        selected.sort_unstable();
        selected.dedup();
        let mut entries = Vec::with_capacity(selected.len());
        for id in &selected {
            let location = self
                .catalog
                .location(*id)
                .ok_or_else(|| Error::InvalidPack("fetch object is missing".into()))?;
            let pack = &self.catalog.packs[usize::from(location.pack)];
            entries.push((location, *id, pack.offset(location.index)));
        }
        entries.sort_unstable_by_key(|(location, id, offset)| {
            let pack = self.catalog.packs[usize::from(location.pack)].id;
            (pack, *offset, *id)
        });

        let hash = object_hash(format);
        let hash_len = hash.len_in_bytes();
        let count = u32::try_from(entries.len()).expect("MAX_OBJECTS fits u32");
        let output_memory = self.catalog.operation.reserve(MAX_FETCH_PACK_BYTES)?;
        let mut writer = gix_hash::io::Write::new(
            PackOutput {
                bytes: Vec::with_capacity(MAX_FETCH_PACK_BYTES),
                limit: MAX_FETCH_PACK_BYTES - hash_len,
                operation: &self.catalog.operation,
            },
            hash,
        );
        writer
            .write_all(&gix_pack::data::header::encode(
                gix_pack::data::Version::V2,
                count,
            ))
            .map_err(output_error)?;
        for (location, id, _) in entries {
            let (entry, compressed) = self.stored_entry(location.pack, location.index).await?;
            let pack = &self.catalog.packs[usize::from(location.pack)];
            let base = pack
                .base(&entry)?
                .map(|index| ObjectId::from_bytes(format, pack.oid(index)))
                .transpose()?;
            if base.is_some_and(|base| selected.binary_search(&base).is_err()) {
                drop((entry, compressed));
                let object = self
                    .find(id)
                    .await?
                    .ok_or_else(|| Error::InvalidPack("fetch object is missing".into()))?;
                let header = match object.kind {
                    gix_object::Kind::Tree => EntryHeader::Tree,
                    gix_object::Kind::Blob => EntryHeader::Blob,
                    gix_object::Kind::Commit => EntryHeader::Commit,
                    gix_object::Kind::Tag => EntryHeader::Tag,
                };
                header
                    .write_to(object.data.len() as u64, &mut writer)
                    .map_err(output_error)?;
                self.catalog.operation.work(object.data.len())?;
                let _compress_memory = self.catalog.operation.reserve(COMPRESS_BYTES)?;
                let mut compressor = gix_zlib::stream::deflate::Write::new(
                    &mut writer,
                    gix_zlib::Compression::DEFAULT,
                );
                compressor
                    .write_all(&object.data)
                    .and_then(|()| compressor.flush())
                    .map_err(output_error)?;
                continue;
            }
            let header = base.map_or(entry.header, |base| EntryHeader::RefDelta {
                base_id: gix_hash::ObjectId::from_bytes_or_panic(base.as_bytes()),
            });
            header
                .write_to(entry.decompressed_size, &mut writer)
                .map_err(output_error)?;
            writer.write_all(&compressed).map_err(output_error)?;
        }
        let gix_hash::io::Write { hash, mut inner } = writer;
        let digest = hash.try_finalize().map_err(pack_error)?;
        inner.operation.work(hash_len)?;
        inner.bytes.extend_from_slice(digest.as_slice());
        Ok(hold(Bytes::from(inner.bytes), output_memory))
    }

    async fn entry(
        &mut self,
        pack: u16,
        index: u32,
        hash: bool,
    ) -> Result<(PackEntry, Bytes), Error> {
        let (entry, compressed) = self.stored_entry(pack, index).await?;
        let size = usize::try_from(entry.decompressed_size)
            .map_err(|_| Error::InvalidPack("pack entry size exceeds memory".into()))?;
        self.catalog
            .operation
            .work(size * (1 + usize::from(hash && !entry.header.is_delta())))?;
        let memory = self.catalog.operation.reserve(size)?;
        let mut data = vec![0; size];
        let _inflate_memory = self.catalog.operation.reserve(INFLATE_BYTES)?;
        let (status, consumed, written) = gix_zlib::Inflate::default()
            .once(&compressed, &mut data)
            .map_err(pack_error)?;
        if status != gix_zlib::Status::StreamEnd || consumed != compressed.len() || written != size
        {
            return invalid("pack entry zlib stream is not exact");
        }
        Ok((entry, hold(Bytes::from(data), memory)))
    }

    async fn stored_entry(&mut self, pack: u16, index: u32) -> Result<(PackEntry, Bytes), Error> {
        let stored = &self.catalog.packs[usize::from(pack)];
        let range = stored.entry_range(index);
        let offset = u64::from(range.start);
        self.catalog
            .operation
            .work((range.end - range.start) as usize)?;
        let bytes = self.read_range(pack, range).await?;
        if gix_features::hash::crc32(&bytes) != stored.crc(index) {
            return invalid("pack entry CRC does not match");
        }
        let entry =
            gix_pack::data::Entry::from_bytes(&bytes, offset, object_hash(stored.id.format()))
                .map_err(pack_error)?;
        if entry.header_size() != entry.header.size(entry.decompressed_size) {
            return invalid("pack entry header is not canonical");
        }
        let size = usize::try_from(entry.decompressed_size)
            .map_err(|_| Error::InvalidPack("pack entry size exceeds memory".into()))?;
        if size > MAX_OBJECT_BYTES {
            return invalid("pack entry exceeds object byte limit");
        }
        let compressed = bytes.slice(entry.header_size()..);
        Ok((entry, compressed))
    }

    async fn read_range(&mut self, pack: u16, range: std::ops::Range<u32>) -> Result<Bytes, Error> {
        let first = range.start as usize / CHUNK_BYTES;
        let last = (range.end as usize - 1) / CHUNK_BYTES;
        if first == last {
            let chunk = self.chunk(pack, first).await?;
            let end = range.end as usize % CHUNK_BYTES;
            return Ok(chunk.slice(
                range.start as usize % CHUNK_BYTES..if end == 0 { chunk.len() } else { end },
            ));
        }
        let length = (range.end - range.start) as usize;
        let memory = self.catalog.operation.reserve(length)?;
        let mut bytes = BytesMut::with_capacity(length);
        for chunk_index in first..=last {
            let chunk = self.chunk(pack, chunk_index).await?;
            let start = if chunk_index == first {
                range.start as usize % CHUNK_BYTES
            } else {
                0
            };
            let end = if chunk_index == last {
                let end = range.end as usize % CHUNK_BYTES;
                if end == 0 { chunk.len() } else { end }
            } else {
                chunk.len()
            };
            bytes.extend_from_slice(&chunk[start..end]);
        }
        Ok(hold(bytes.freeze(), memory))
    }

    async fn chunk(&mut self, pack: u16, index: usize) -> Result<Bytes, Error> {
        let index = u16::try_from(index)
            .map_err(|_| Error::InvalidPack("pack chunk index exceeds u16".into()))?;
        if let Some((_, bytes)) = self.cache.iter().find(|(key, _)| *key == (pack, index)) {
            return Ok(bytes.clone());
        }
        let object = self.catalog.packs[usize::from(pack)]
            .node
            .children()
            .get(usize::from(index))
            .ok_or_else(|| Error::InvalidPack("pack chunk is missing".into()))?
            .clone();
        let bytes = usize::try_from(object.len())
            .map_err(|_| Error::InvalidPack("pack chunk exceeds memory".into()))?;
        while self.cache_bytes + bytes > MAX_CACHE_BYTES {
            let Some((_, removed)) = self.cache.pop_front() else {
                break;
            };
            self.cache_bytes -= removed.len();
        }
        self.catalog.operation.io(bytes)?;
        self.catalog.operation.work(bytes)?;
        let memory = self.catalog.operation.reserve(bytes)?;
        let value = self.log.read_object(self.view, &object).await?;
        if value.len() != bytes {
            return invalid("pack chunk byte length does not match");
        }
        let value = hold(value, memory);
        self.cache_bytes += bytes;
        self.cache.push_back(((pack, index), value.clone()));
        Ok(value)
    }
}

struct PackOutput<'a> {
    bytes: Vec<u8>,
    limit: usize,
    operation: &'a Operation,
}

fn output_error(error: io::Error) -> Error {
    error.downcast::<Error>().unwrap_or_else(pack_error)
}

impl Write for PackOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other(pack_error(
                "fetch pack exceeds byte limit",
            )));
        }
        self.operation
            .work(bytes.len() * 2)
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn apply_delta(op: &Operation, base: &[u8], delta: &[u8], hash: bool) -> Result<Bytes, Error> {
    let (base_size, mut position) = delta_integer(delta)?;
    let (result_size, consumed) = delta_integer(&delta[position..])?;
    position += consumed;
    if base_size != base.len() || result_size > MAX_OBJECT_BYTES {
        return invalid("delta base or result size is invalid");
    }
    op.work(result_size * (1 + usize::from(hash)))?;
    let memory = op.reserve(result_size)?;
    let mut result = Vec::with_capacity(result_size);
    while position < delta.len() {
        let command = delta[position];
        position += 1;
        if command & 0x80 == 0 {
            let length = usize::from(command);
            if length == 0 || position + length > delta.len() || result.len() + length > result_size
            {
                return invalid("delta insert is invalid");
            }
            result.extend_from_slice(&delta[position..position + length]);
            position += length;
            continue;
        }
        let mut offset = 0_usize;
        let mut length = 0_usize;
        for bit in 0..4 {
            if command & (1 << bit) != 0 {
                let byte = *delta
                    .get(position)
                    .ok_or_else(|| Error::InvalidPack("delta copy is truncated".into()))?;
                position += 1;
                offset |= usize::from(byte) << (bit * 8);
            }
        }
        for bit in 0..3 {
            if command & (0x10 << bit) != 0 {
                let byte = *delta
                    .get(position)
                    .ok_or_else(|| Error::InvalidPack("delta copy is truncated".into()))?;
                position += 1;
                length |= usize::from(byte) << (bit * 8);
            }
        }
        if length == 0 {
            length = 0x1_0000;
        }
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= base.len())
            .ok_or_else(|| Error::InvalidPack("delta copy is outside its base".into()))?;
        if result
            .len()
            .checked_add(length)
            .is_none_or(|end| end > result_size)
        {
            return invalid("delta result exceeds its declared size");
        }
        result.extend_from_slice(&base[offset..end]);
    }
    if result.len() != result_size {
        return invalid("delta result size does not match");
    }
    Ok(hold(Bytes::from(result), memory))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        error::Error as StdError,
        path::Path,
        process::{Command, Stdio},
        sync::Arc,
    };

    use object_log::{
        CollectionFinish, CollectionStart, CommitStatus, LogId, Options, TransactionId,
        ValidatedBackend,
        sim::{FailurePhase, FaultStore, Operation as StoreOperation},
    };
    use object_store::{memory::InMemory, path::Path as StorePath};

    use super::*;
    use crate::pack::{
        ExternalBase,
        budget::{LIVE_BYTES, Pool},
    };

    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

    fn test_operation() -> Operation {
        let Ok(operation) = Pool::new(LIVE_BYTES).admit() else {
            unreachable!("new test pool must admit its first operation")
        };
        operation
    }

    fn normalize(
        format: ObjectFormat,
        input: &[u8],
        bases: &[ExternalBase<'_>],
    ) -> Result<Normalized, Error> {
        crate::pack::normalize(&test_operation(), format, input, bases)
    }

    fn maximal_normalized(operation: &Operation) -> Result<Normalized, Error> {
        let memory = [
            operation.reserve(MAX_PACK_BYTES)?,
            operation.reserve(MAX_INDEX_BYTES)?,
        ];
        Ok(Normalized {
            bytes: vec![0; MAX_PACK_BYTES],
            index: vec![0; MAX_INDEX_BYTES],
            id: ObjectId::from_bytes(ObjectFormat::Sha1, &[1; 20])?,
            _memory: memory,
        })
    }

    struct Fixture {
        normalized: Normalized,
        objects: Vec<(ObjectId, Vec<u8>)>,
    }

    async fn open(store: FaultStore, name: &str) -> TestResult<(Log, View)> {
        let backend =
            ValidatedBackend::new(Arc::new(store), StorePath::from(format!("durable-{name}")))
                .await?;
        let log = Log::open(&backend, &LogId::new(name)?, Options::default()).await?;
        let view = log.load().await?;
        Ok((log, view))
    }

    fn fixture(
        format: ObjectFormat,
        count: usize,
        ofs_delta: bool,
        large: bool,
    ) -> TestResult<Fixture> {
        let mut data = Vec::with_capacity(count);
        for marker in 0..count {
            let size = if large { 1_200_000 } else { 50_000 };
            let split = size - marker.min(40) * 1_000;
            let mut object = vec![b'a'; split];
            object.resize(size, b'b');
            object.extend_from_slice(marker.to_string().as_bytes());
            object.push(b'\n');
            data.push(object);
        }
        pack_fixture(format, data, ofs_delta, large)
    }

    fn pack_fixture(
        format: ObjectFormat,
        data: Vec<Vec<u8>>,
        ofs_delta: bool,
        uncompressed: bool,
    ) -> TestResult<Fixture> {
        let directory = tempfile::tempdir()?;
        let mut init = vec!["init", "--bare", "--quiet"];
        if format == ObjectFormat::Sha256 {
            init.push("--object-format=sha256");
        }
        git(directory.path(), init, &[])?;
        let mut objects = Vec::with_capacity(data.len());
        for data in data {
            let output = git(directory.path(), ["hash-object", "-w", "--stdin"], &data)?;
            let id = ObjectId::parse(format, std::str::from_utf8(&output)?.trim())?;
            objects.push((id, data));
        }
        let mut ids = objects
            .iter()
            .map(|(id, _)| id.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        if !ids.is_empty() {
            ids.push('\n');
        }
        let mut arguments = vec![
            "-c",
            if uncompressed {
                "pack.compression=0"
            } else {
                "pack.compression=6"
            },
            "pack-objects",
            "--stdout",
            "--window=2",
            "--depth=10",
        ];
        if ofs_delta {
            arguments.push("--delta-base-offset");
        }
        let input = git(directory.path(), arguments, ids.as_bytes())?;
        let normalized = normalize(format, &input, &[] as &[ExternalBase<'_>])?;
        Ok(Fixture {
            normalized,
            objects,
        })
    }

    fn git<I, S>(directory: &Path, arguments: I, input: &[u8]) -> TestResult<Vec<u8>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut child = Command::new("git")
            .current_dir(directory)
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        std::io::Write::write_all(
            &mut child.stdin.take().ok_or("Git stdin is unavailable")?,
            input,
        )?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
        }
        Ok(output.stdout)
    }

    async fn load_one(
        log: &Log,
        view: &View,
        format: ObjectFormat,
        descriptor: PackDescriptor,
        root: &StagedObject,
    ) -> Result<Catalog, Error> {
        load(
            &test_operation(),
            log,
            view,
            format,
            &[(descriptor, root.reference().clone())],
        )
        .await
    }

    async fn stage_load_find(format: ObjectFormat) -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "round-trip").await?;
        let empty = pack_fixture(format, Vec::new(), false, false)?;
        assert_eq!(empty.normalized.bytes.len(), 12 + format.digest_len());
        let (descriptor, root) = stage(&test_operation(), &log, &view, empty.normalized).await?;
        let empty = load_one(&log, &view, format, descriptor, &root).await?;
        assert!(empty.directory.is_empty());

        let fixture = fixture(format, 10, true, false)?;
        let chunks = fixture.normalized.bytes.len().div_ceil(CHUNK_BYTES);
        let first_id = fixture.objects[0].0;
        let Fixture {
            normalized,
            objects,
        } = fixture;

        store.reset();
        let (descriptor, root) = stage(&test_operation(), &log, &view, normalized).await?;
        let puts = store.metrics().operation(StoreOperation::Put);
        assert_eq!(puts.requests, (chunks + 1) as u64);
        assert_eq!(
            puts.uploaded_bytes,
            descriptor.bytes + root.reference().len()
        );

        store.reset();
        let pool = Pool::new(usize::try_from(root.reference().len())? + size_of::<Pack>() - 1);
        assert!(
            load(
                &pool.admit()?,
                &log,
                &view,
                format,
                &[(descriptor.clone(), root.reference().clone())]
            )
            .await
            .is_err()
        );
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);

        store.reset();
        let blocked = test_operation();
        blocked.io(crate::pack::budget::TRANSFER_BYTES)?;
        assert!(
            load(
                &blocked,
                &log,
                &view,
                format,
                &[(descriptor.clone(), root.reference().clone())]
            )
            .await
            .is_err()
        );
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);

        store.reset();
        let blocked = test_operation();
        let root_bytes = usize::try_from(root.reference().len())?;
        blocked.work(crate::pack::budget::WORK_BYTES - root_bytes + 1)?;
        assert!(
            load(
                &blocked,
                &log,
                &view,
                format,
                &[(descriptor.clone(), root.reference().clone())]
            )
            .await
            .is_err()
        );
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);

        let catalog = load_one(&log, &view, format, descriptor, &root).await?;
        let gets = store.metrics().operation(StoreOperation::Get);
        assert_eq!(gets.requests, 1);
        assert_eq!(gets.downloaded_bytes, root.reference().len());

        let mut reader = Reader::new(&log, &view, &catalog);
        store.reset();
        let missing = ObjectId::from_bytes(format, &vec![0xfe; format.digest_len()])?;
        assert!(reader.find(missing).await?.is_none());
        let gets = store.metrics().operation(StoreOperation::Get);
        assert_eq!(gets.requests, 0);
        assert_eq!(gets.downloaded_bytes, 0);
        for (id, expected) in objects {
            let object = reader.find(id).await?.ok_or("stored object is missing")?;
            assert_eq!(object.kind, gix_object::Kind::Blob);
            assert_eq!(&object.data[..], expected);
        }
        let mut shared = Reader::new(&log, &view, &catalog);
        assert!(shared.find(first_id).await?.is_some());
        while catalog.operation.calls() < crate::pack::budget::CALLS {
            catalog.operation.io(0)?;
        }
        let mut second = Reader::new(&log, &view, &catalog);
        store.reset();
        assert!(second.find(first_id).await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn sha1_and_sha256_stage_load_and_sparse_find_match_git() -> TestResult {
        stage_load_find(ObjectFormat::Sha1).await?;
        stage_load_find(ObjectFormat::Sha256).await
    }

    #[tokio::test]
    async fn stage_reserves_exact_maximum_before_writes_and_through_root_put() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "stage-memory").await?;
        let vectors = MAX_PACK_BYTES.div_ceil(CHUNK_BYTES)
            * (size_of::<StagedObject>() + size_of::<ObjectRef>());
        let required = MAX_PACK_BYTES + MAX_INDEX_BYTES + MAX_PACK_ROOT_BYTES + vectors;
        let pool = Pool::new(required);
        let operation = pool.admit()?;
        let normalized = maximal_normalized(&operation)?;
        store.reset();
        let mut pause = store.pause_put_at(17, FailurePhase::Before);
        let worker = operation.clone();
        let worker_log = log.clone();
        let worker_view = view.clone();
        let task =
            tokio::spawn(
                async move { stage(&worker, &worker_log, &worker_view, normalized).await },
            );
        assert!(pause.wait_until_entered().await);
        assert_eq!(operation.live_bytes(), required);
        assert!(pause.release());
        let (_, root) = task.await??;
        assert_eq!(root.reference().len(), MAX_PACK_ROOT_BYTES as u64);
        assert_eq!(operation.live_bytes(), 0);

        let operation = Pool::new(required - 1).admit()?;
        let normalized = maximal_normalized(&operation)?;
        store.reset();
        assert!(stage(&operation, &log, &view, normalized).await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Put).requests, 0);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn catalog_preflight_rejects_derived_limit_without_gets() -> TestResult {
        for (format, name) in [
            (ObjectFormat::Sha1, "catalog-sha1"),
            (ObjectFormat::Sha256, "catalog-sha256"),
        ] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (log, view) = open(store.clone(), name).await?;
            let root = log
                .put_node(&view, Bytes::from(vec![0; MAX_INDEX_BYTES]), Vec::new())
                .await?;
            let descriptor = PackDescriptor {
                id: ObjectId::from_bytes(format, &vec![1; format.digest_len()])?,
                bytes: (12 + format.digest_len()) as u64,
            };
            let pair = (descriptor, root.reference().clone());
            let mut roots = Vec::new();
            while catalog_bytes(format, &roots).is_ok() {
                roots.push(pair.clone());
            }
            assert!(catalog_bytes(format, &roots[..roots.len() - 1]).is_ok());
            store.reset();
            assert!(
                load(&test_operation(), &log, &view, format, &roots)
                    .await
                    .is_err()
            );
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn catalog_reservation_releases_on_drop_and_cancel() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "catalog-release").await?;
        let fixture = fixture(ObjectFormat::Sha1, 2, true, false)?;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let roots = vec![(descriptor, root.reference().clone())];
        let expected = catalog_bytes(ObjectFormat::Sha1, &roots)?;
        let bounded = Pool::new(expected - 1);
        store.reset();
        assert!(
            load(&bounded.admit()?, &log, &view, ObjectFormat::Sha1, &roots,)
                .await
                .is_err()
        );
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);

        let pool = Pool::new(expected);
        let operation = pool.admit()?;
        let catalog = load(&operation, &log, &view, ObjectFormat::Sha1, &roots).await?;
        assert_eq!(operation.live_bytes(), expected);
        drop(catalog);
        assert_eq!(operation.live_bytes(), 0);

        store.reset();
        let mut pause = store.pause_next_get(FailurePhase::Before);
        {
            let loading = load(&operation, &log, &view, ObjectFormat::Sha1, &roots);
            tokio::pin!(loading);
            let entered = tokio::select! {
                entered = pause.wait_until_entered() => entered,
                _ = &mut loading => return Err("catalog load completed before its pause".into()),
            };
            assert!(entered);
            assert_eq!(operation.live_bytes(), expected);
        }
        assert!(!pause.release());
        assert_eq!(operation.live_bytes(), 0);
        drop(operation);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_ids_choose_the_lowest_pack_id_for_every_root_order() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "duplicates").await?;
        let shared = vec![b'x'; 50_000];
        let left = pack_fixture(
            ObjectFormat::Sha1,
            vec![shared.clone(), vec![0]],
            true,
            false,
        )?;
        let right = pack_fixture(ObjectFormat::Sha1, vec![shared, vec![1]], true, false)?;
        let shared_id = left.objects[0].0;
        let (left, left_root) = stage(&test_operation(), &log, &view, left.normalized).await?;
        let (right, right_root) = stage(&test_operation(), &log, &view, right.normalized).await?;
        let expected = left.id.min(right.id);
        let roots = vec![
            (left, left_root.reference().clone()),
            (right, right_root.reference().clone()),
        ];
        for roots in [roots.clone(), roots.into_iter().rev().collect()] {
            let catalog = load(&test_operation(), &log, &view, ObjectFormat::Sha1, &roots).await?;
            let position = catalog
                .directory
                .binary_search_by(|location| {
                    oid(&catalog.packs, *location).cmp(shared_id.as_bytes())
                })
                .map_err(|_| "shared object is absent")?;
            let selected = catalog.directory[position];
            assert_eq!(catalog.packs[usize::from(selected.pack)].id, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn chunk_boundaries_are_exact_and_verified_chunks_are_cached() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "boundaries").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, true, true)?;
        let range = entry_range(
            &fixture.normalized.index,
            fixture.normalized.bytes.len(),
            ObjectFormat::Sha1,
            0,
        )?;
        assert!(range.start < CHUNK_BYTES && range.end > CHUNK_BYTES);
        let expected = fixture.normalized.bytes.clone();
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let pack_bytes = usize::try_from(descriptor.bytes)?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        let boundary = u32::try_from(CHUNK_BYTES)?;
        store.reset();
        assert_eq!(
            &reader.read_range(0, 12..boundary).await?[..],
            &expected[12..CHUNK_BYTES]
        );
        let gets = store.metrics().operation(StoreOperation::Get);
        assert_eq!(gets.requests, 1);
        assert_eq!(gets.downloaded_bytes, CHUNK_BYTES as u64);
        store.reset();
        assert_eq!(
            &reader.read_range(0, boundary - 1..boundary + 1).await?[..],
            &expected[(CHUNK_BYTES - 1)..=CHUNK_BYTES]
        );
        let gets = store.metrics().operation(StoreOperation::Get);
        assert_eq!(gets.requests, 1);
        assert_eq!(gets.downloaded_bytes, (pack_bytes - CHUNK_BYTES) as u64);
        store.reset();
        reader.find(fixture.objects[0].0).await?;
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn cache_evicts_at_eight_mib_while_slices_keep_their_reservation() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "cache-eviction").await?;
        let data = (1_u64..=8)
            .map(|mut state| {
                (0..1_100_000)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state.to_le_bytes()[0]
                    })
                    .collect()
            })
            .collect();
        let fixture = pack_fixture(ObjectFormat::Sha1, data, false, true)?;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        assert!(catalog.packs[0].node.children().len() > MAX_CACHE_BYTES / CHUNK_BYTES);
        for index in 0..MAX_CACHE_BYTES / CHUNK_BYTES {
            drop(reader.chunk(0, index).await?);
        }
        let pressure = catalog
            .operation
            .reserve(LIVE_BYTES - catalog.operation.live_bytes())?;
        drop(reader.chunk(0, MAX_CACHE_BYTES / CHUNK_BYTES).await?);
        drop(pressure);
        let first = reader.chunk(0, 0).await?.slice(..16);
        for index in 1..catalog.packs[0].node.children().len() {
            drop(reader.chunk(0, index).await?);
        }
        assert!(reader.cache_bytes <= MAX_CACHE_BYTES);
        assert!(reader.cache.iter().all(|(key, _)| *key != (0, 0)));
        let retained = catalog.operation.live_bytes();
        drop(first);
        assert_eq!(retained - catalog.operation.live_bytes(), CHUNK_BYTES);
        Ok(())
    }

    async fn store_raw(
        log: &Log,
        view: &View,
        pack: Vec<u8>,
        index: Vec<u8>,
    ) -> TestResult<(PackDescriptor, StagedObject)> {
        let id = ObjectId::from_bytes(ObjectFormat::Sha1, &pack[pack.len() - 20..])?;
        let operation = test_operation();
        let memory = [
            operation.reserve(pack.len())?,
            operation.reserve(index.len())?,
        ];
        Ok(stage(
            &operation,
            log,
            view,
            Normalized {
                bytes: pack,
                index,
                id,
                _memory: memory,
            },
        )
        .await?)
    }

    async fn assert_load_fails(
        log: &Log,
        view: &View,
        format: ObjectFormat,
        descriptor: PackDescriptor,
        root: StagedObject,
    ) -> TestResult {
        assert!(
            load(
                &test_operation(),
                log,
                view,
                format,
                &[(descriptor, root.reference().clone())]
            )
            .await
            .is_err()
        );
        Ok(())
    }

    async fn assert_raw_load_fails(
        log: &Log,
        view: &View,
        pack: Vec<u8>,
        index: Vec<u8>,
    ) -> TestResult {
        let (descriptor, root) = store_raw(log, view, pack, index).await?;
        assert_load_fails(log, view, ObjectFormat::Sha1, descriptor, root).await
    }

    async fn assert_find_raw_fails(
        log: &Log,
        view: &View,
        pack: Vec<u8>,
        index: Vec<u8>,
        id: ObjectId,
    ) -> TestResult {
        let (descriptor, root) = store_raw(log, view, pack, index).await?;
        let catalog = load_one(log, view, ObjectFormat::Sha1, descriptor, &root).await?;
        assert!(Reader::new(log, view, &catalog).find(id).await.is_err());
        Ok(())
    }

    fn index_count(index: &[u8]) -> usize {
        u32::from_be_bytes(
            index[8 + 255 * 4..8 + 256 * 4]
                .try_into()
                .unwrap_or_default(),
        ) as usize
    }

    fn index_oid_range(format: ObjectFormat, position: usize) -> std::ops::Range<usize> {
        let start = 8 + 256 * 4 + position * format.digest_len();
        start..start + format.digest_len()
    }

    fn index_crc_offset(index: &[u8], format: ObjectFormat, position: usize) -> usize {
        8 + 256 * 4 + index_count(index) * format.digest_len() + position * 4
    }

    fn index_offset_offset(index: &[u8], format: ObjectFormat, position: usize) -> usize {
        index_crc_offset(index, format, 0) + index_count(index) * 4 + position * 4
    }

    fn exact_fan(index: &mut [u8], format: ObjectFormat) {
        let count = index_count(index);
        let mut fan = [0_u32; 256];
        for position in 0..count {
            fan[usize::from(index[index_oid_range(format, position).start])] += 1;
        }
        let mut cumulative = 0;
        for (position, value) in fan.into_iter().enumerate() {
            cumulative += value;
            index[8 + position * 4..12 + position * 4].copy_from_slice(&cumulative.to_be_bytes());
        }
    }

    fn rehash_index(index: &mut [u8], format: ObjectFormat) -> TestResult {
        let hash_len = format.digest_len();
        let mut hasher = gix_hash::hasher(object_hash(format));
        hasher.update(&index[..index.len() - hash_len]);
        let digest = hasher.try_finalize()?;
        let trailer = index.len() - hash_len;
        index[trailer..].copy_from_slice(digest.as_slice());
        Ok(())
    }

    fn rehash_pack(pack: &mut [u8], format: ObjectFormat) -> TestResult<ObjectId> {
        let hash_len = format.digest_len();
        let mut hasher = gix_hash::hasher(object_hash(format));
        hasher.update(&pack[..pack.len() - hash_len]);
        let digest = hasher.try_finalize()?;
        let trailer = pack.len() - hash_len;
        pack[trailer..].copy_from_slice(digest.as_slice());
        Ok(ObjectId::from_bytes(format, digest.as_slice())?)
    }

    fn set_pack_id(index: &mut [u8], format: ObjectFormat, id: ObjectId) {
        let hash_len = format.digest_len();
        let start = index.len() - hash_len * 2;
        index[start..start + hash_len].copy_from_slice(id.as_bytes());
    }

    fn set_crc(index: &mut [u8], format: ObjectFormat, position: usize, crc: u32) {
        let start = index_crc_offset(index, format, position);
        index[start..start + 4].copy_from_slice(&crc.to_be_bytes());
    }

    fn entry_range(
        index: &[u8],
        pack_len: usize,
        format: ObjectFormat,
        position: u32,
    ) -> TestResult<std::ops::Range<usize>> {
        let file = gix_pack::index::File::from_data(index, PathBuf::new(), object_hash(format))?;
        let start = usize::try_from(file.pack_offset_at_index(position))?;
        let next = file
            .sorted_offsets()
            .into_iter()
            .find(|offset| *offset > start as u64);
        let end = match next {
            Some(offset) => usize::try_from(offset)?,
            None => pack_len - format.digest_len(),
        };
        Ok(start..end)
    }

    fn refresh_entry(
        pack: &mut [u8],
        index: &mut [u8],
        format: ObjectFormat,
        position: u32,
    ) -> TestResult<ObjectId> {
        let id = rehash_pack(pack, format)?;
        set_pack_id(index, format, id);
        let range = entry_range(index, pack.len(), format, position)?;
        set_crc(
            index,
            format,
            position as usize,
            gix_features::hash::crc32(&pack[range]),
        );
        rehash_index(index, format)?;
        Ok(id)
    }

    fn inspect_pack(
        pack: &[u8],
        format: ObjectFormat,
    ) -> TestResult<Vec<(gix_pack::data::entry::Header, Vec<u8>)>> {
        let hash = object_hash(format);
        let trailer = pack.len() - format.digest_len();
        let mut hasher = gix_hash::hasher(hash);
        hasher.update(&pack[..trailer]);
        assert_eq!(hasher.try_finalize()?.as_slice(), &pack[trailer..]);
        assert_eq!(&pack[..8], b"PACK\0\0\0\x02");
        let count = u32::from_be_bytes(pack[8..12].try_into()?);
        let mut offset = 12;
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let entry =
                gix_pack::data::Entry::from_bytes(&pack[offset..trailer], offset as u64, hash)?;
            let size = usize::try_from(entry.decompressed_size)?;
            let mut decoded = vec![0; size];
            let compressed = &pack[offset + entry.header_size()..trailer];
            let (status, consumed, written) =
                gix_zlib::Inflate::default().once(compressed, &mut decoded)?;
            assert_eq!(status, gix_zlib::Status::StreamEnd);
            assert_eq!(written, size);
            entries.push((entry.header, compressed[..consumed].to_vec()));
            offset += entry.header_size() + consumed;
        }
        assert_eq!(offset, trailer);
        Ok(entries)
    }

    fn verify_fetch_pack(pack: &[u8], format: ObjectFormat, expected: &[ObjectId]) -> TestResult {
        let entries = inspect_pack(pack, format)?;
        let mut expected = expected.to_vec();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(entries.len(), expected.len());
        let directory = tempfile::tempdir()?;
        let mut init = vec!["init", "--bare", "--quiet"];
        if format == ObjectFormat::Sha256 {
            init.push("--object-format=sha256");
        }
        git(directory.path(), init, &[])?;
        git(
            directory.path(),
            [
                "index-pack",
                "--stdin",
                "--strict",
                "--check-self-contained-and-connected",
            ],
            pack,
        )?;
        for id in expected {
            let name = format!("{id}^{{object}}");
            git(directory.path(), ["cat-file", "-e", &name], &[])?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn fetch_pack_reuses_full_and_ref_delta_streams_for_both_formats() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for ofs in [false, true] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let name = format!("fetch-{format:?}-{ofs}");
                let (log, view) = open(store, &name).await?;
                let fixture = fixture(format, 10, ofs, false)?;
                let source = inspect_pack(&fixture.normalized.bytes, format)?;
                assert!(source.iter().any(|entry| entry.0.is_delta()));
                let ids = fixture
                    .objects
                    .iter()
                    .map(|item| item.0)
                    .collect::<Vec<_>>();
                let entries = indexed_entries(&fixture.normalized, format)?;
                assert!(entries.iter().any(|entry| entry.2 >= 2));
                let delta = entries
                    .iter()
                    .find(|entry| entry.1.is_delta())
                    .ok_or("fixture has no delta")?
                    .0;
                let (descriptor, root) =
                    stage(&test_operation(), &log, &view, fixture.normalized).await?;
                let catalog = load_one(&log, &view, format, descriptor, &root).await?;
                let mut reader = Reader::new(&log, &view, &catalog);
                let output = reader.fetch_pack(&ids).await?;
                verify_fetch_pack(&output, format, &ids)?;
                let output_entries = inspect_pack(&output, format)?;
                assert_eq!(source.len(), output_entries.len());
                for (source, output) in source.iter().zip(&output_entries) {
                    assert_eq!(source.1, output.1);
                    if source.0.is_delta() {
                        assert!(matches!(
                            output.0,
                            gix_pack::data::entry::Header::RefDelta { .. }
                        ));
                    }
                }
                assert!(!output_entries.iter().any(|entry| matches!(
                    entry.0,
                    gix_pack::data::entry::Header::OfsDelta { .. }
                )));

                let mut reordered = ids.clone();
                reordered.reverse();
                reordered.extend_from_slice(&ids);
                assert_eq!(output, reader.fetch_pack(&reordered).await?);
                let fallback = reader.fetch_pack(&[delta]).await?;
                verify_fetch_pack(&fallback, format, &[delta])?;
                assert!(!inspect_pack(&fallback, format)?[0].0.is_delta());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn fetch_pack_writes_empty_sha1_and_sha256_packs() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let name = format!("empty-fetch-{format:?}");
            let (log, view) = open(store, &name).await?;
            let fixture = pack_fixture(format, Vec::new(), false, false)?;
            let (descriptor, root) =
                stage(&test_operation(), &log, &view, fixture.normalized).await?;
            let catalog = load_one(&log, &view, format, descriptor, &root).await?;
            let output = Reader::new(&log, &view, &catalog).fetch_pack(&[]).await?;
            verify_fetch_pack(&output, format, &[])?;
        }
        Ok(())
    }

    #[test]
    fn fetch_output_accepts_the_raw_maximum_and_rejects_the_next_byte() -> TestResult {
        let operation = test_operation();
        let hash_len = ObjectFormat::Sha1.digest_len();
        let mut output = PackOutput {
            bytes: Vec::with_capacity(MAX_FETCH_PACK_BYTES),
            limit: MAX_FETCH_PACK_BYTES - hash_len,
            operation: &operation,
        };
        let chunk = vec![0; 64 * 1024];
        let mut remaining = output.limit;
        while remaining != 0 {
            let bytes = remaining.min(chunk.len());
            output.write_all(&chunk[..bytes])?;
            remaining -= bytes;
        }
        assert_eq!(output.bytes.len() + hash_len, MAX_FETCH_PACK_BYTES);
        assert!(output.write_all(&[0]).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn fetch_pack_rejects_a_missing_immediate_delta_base() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "fetch-missing-base").await?;
        let fixture = fixture(ObjectFormat::Sha1, 10, false, false)?;
        let delta = indexed_entries(&fixture.normalized, ObjectFormat::Sha1)?
            .into_iter()
            .find(|entry| matches!(entry.1, EntryHeader::RefDelta { .. }))
            .ok_or("fixture has no REF_DELTA")?
            .0;
        let mut pack = fixture.normalized.bytes;
        let mut index = fixture.normalized.index;
        let file = gix_pack::index::File::from_data(
            index.as_slice(),
            PathBuf::new(),
            object_hash(ObjectFormat::Sha1),
        )?;
        let position = file
            .lookup(gix_hash::ObjectId::try_from(delta.as_bytes())?)
            .ok_or("delta is absent")?;
        let range = entry_range(&index, pack.len(), ObjectFormat::Sha1, position)?;
        let entry = gix_pack::data::Entry::from_bytes(
            &pack[range],
            file.pack_offset_at_index(position),
            object_hash(ObjectFormat::Sha1),
        )?;
        let data = usize::try_from(entry.data_offset)?;
        pack[data - 20..data].fill(0xfe);
        refresh_entry(&mut pack, &mut index, ObjectFormat::Sha1, position)?;
        let (descriptor, root) = store_raw(&log, &view, pack, index).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        assert!(
            Reader::new(&log, &view, &catalog)
                .fetch_pack(&[delta])
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn fetch_pack_preflight_and_work_fail_before_object_gets() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "fetch-preflight").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, false)?;
        let id = fixture.objects[0].0;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let operation = test_operation();
        let catalog = load(
            &operation,
            &log,
            &view,
            ObjectFormat::Sha1,
            &[(descriptor, root.reference().clone())],
        )
        .await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        let baseline = operation.live_bytes();
        store.reset();
        let missing = ObjectId::from_bytes(ObjectFormat::Sha1, &[0xfe; 20])?;
        assert!(reader.fetch_pack(&[missing]).await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        assert_eq!(operation.live_bytes(), baseline);

        let range = catalog.packs[0].entry_range(0);
        let entry_work = usize::try_from(range.end - range.start)?;
        let used = operation.work_bytes();
        operation.work(crate::pack::budget::WORK_BYTES - used - entry_work - 23)?;
        store.reset();
        assert!(matches!(
            reader.fetch_pack(&[id]).await,
            Err(Error::InvalidPack(message)) if message == "Git work limit exceeded"
        ));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        assert_eq!(operation.live_bytes(), baseline);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_output_reservation_lives_with_bytes_and_releases_on_cancel() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "fetch-resources").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, false)?;
        let id = fixture.objects[0].0;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let operation = test_operation();
        let catalog = load(
            &operation,
            &log,
            &view,
            ObjectFormat::Sha1,
            &[(descriptor.clone(), root.reference().clone())],
        )
        .await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let baseline = operation.live_bytes();
        let output = reader.fetch_pack(&[id]).await?;
        assert_eq!(operation.live_bytes() - baseline, MAX_FETCH_PACK_BYTES);
        drop(output);
        assert_eq!(operation.live_bytes(), baseline);
        drop(reader);
        drop(catalog);
        drop(operation);

        let pool = Pool::new(LIVE_BYTES);
        let operation = pool.admit()?;
        let catalog = load(
            &operation,
            &log,
            &view,
            ObjectFormat::Sha1,
            &[(descriptor, root.reference().clone())],
        )
        .await?;
        store.reset();
        let mut pause = store.pause_next_get(FailurePhase::Before);
        let worker_log = log.clone();
        let worker_view = view.clone();
        let task = tokio::spawn(async move {
            Reader::new(&worker_log, &worker_view, &catalog)
                .fetch_pack(&[id])
                .await
        });
        assert!(pause.wait_until_entered().await);
        assert!(operation.live_bytes() >= MAX_FETCH_PACK_BYTES);
        task.abort();
        let _ = task.await;
        assert!(!pause.release());
        assert_eq!(operation.live_bytes(), 0);
        drop(operation);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn empty_object_has_one_exact_zlib_stream() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "empty").await?;
        let fixture = pack_fixture(ObjectFormat::Sha1, vec![Vec::new()], true, false)?;
        let id = fixture.objects[0].0;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        let object = reader.find(id).await?.ok_or("empty object is missing")?;
        assert!(object.data.is_empty());
        store.reset();
        assert!(reader.find(id).await?.is_some());
        let used = catalog.operation.work_bytes();
        catalog
            .operation
            .work(crate::pack::budget::WORK_BYTES - used)?;
        assert!(matches!(
            reader.find(id).await,
            Err(Error::InvalidPack(message)) if message == "Git work limit exceeded"
        ));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn inflate_memory_and_hash_work_fail_before_cached_decode() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "hash-work").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, true, false)?;
        let id = fixture.objects[0].0;
        let size = fixture.objects[0].1.len();
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let operation = test_operation();
        let catalog = load(
            &operation,
            &log,
            &view,
            ObjectFormat::Sha1,
            &[(descriptor.clone(), root.reference().clone())],
        )
        .await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let allowance = size + INFLATE_BYTES - 1;
        let pressure = operation.reserve(LIVE_BYTES - operation.live_bytes() - allowance)?;
        store.reset();
        assert!(matches!(
            reader.find(id).await,
            Err(Error::InvalidPack(message)) if message == "Git live-memory limit exceeded"
        ));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        drop(reader);
        drop(catalog);
        drop(pressure);
        drop(operation);

        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let range = catalog.packs[0].entry_range(0);
        let mut reader = Reader::new(&log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let required = (range.end - range.start) as usize + size * 2;
        let used = catalog.operation.work_bytes();
        catalog
            .operation
            .work(crate::pack::budget::WORK_BYTES - used - required + 1)?;
        store.reset();
        assert!(matches!(
            reader.find(id).await,
            Err(Error::InvalidPack(message)) if message == "Git work limit exceeded"
        ));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn indexes_reject_corrupt_checksums_fans_ids_and_offsets() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "index-corruption").await?;
        let fixture = fixture(ObjectFormat::Sha1, 2, true, false)?;
        let pack = fixture.normalized.bytes;
        let index = fixture.normalized.index;
        let descriptor = PackDescriptor {
            id: fixture.normalized.id,
            bytes: pack.len() as u64,
        };
        let oversized = Bytes::from(vec![0; MAX_INDEX_BYTES + 1]);
        assert!(validate_index(&oversized, ObjectFormat::Sha1, &descriptor).is_err());

        let mut corrupt = index.clone();
        corrupt[7] = 1;
        rehash_index(&mut corrupt, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;

        let mut corrupt = index.clone();
        corrupt[8 + 255 * 4..8 + 256 * 4].copy_from_slice(&(MAX_OBJECTS + 1).to_be_bytes());
        rehash_index(&mut corrupt, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;

        let mut corrupt = index.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;

        let mut corrupt = index.clone();
        corrupt[8 + 255 * 4..8 + 256 * 4].copy_from_slice(&0_u32.to_be_bytes());
        rehash_index(&mut corrupt, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;

        let mut corrupt = index.clone();
        let oid = index_oid_range(ObjectFormat::Sha1, 0);
        corrupt[oid].fill(0);
        exact_fan(&mut corrupt, ObjectFormat::Sha1);
        rehash_index(&mut corrupt, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;

        let mut duplicate = index.clone();
        let first = duplicate[index_oid_range(ObjectFormat::Sha1, 0)].to_vec();
        let second = index_oid_range(ObjectFormat::Sha1, 1);
        duplicate[second].copy_from_slice(&first);
        exact_fan(&mut duplicate, ObjectFormat::Sha1);
        rehash_index(&mut duplicate, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), duplicate).await?;

        for offset in [11_u32, u32::try_from(pack.len())?] {
            let mut corrupt = index.clone();
            let start = index_offset_offset(&corrupt, ObjectFormat::Sha1, 0);
            corrupt[start..start + 4].copy_from_slice(&offset.to_be_bytes());
            rehash_index(&mut corrupt, ObjectFormat::Sha1)?;
            assert_raw_load_fails(&log, &view, pack.clone(), corrupt).await?;
        }

        let mut duplicate = index.clone();
        let first =
            duplicate[index_offset_offset(&duplicate, ObjectFormat::Sha1, 0)..][..4].to_vec();
        let second = index_offset_offset(&duplicate, ObjectFormat::Sha1, 1);
        duplicate[second..second + 4].copy_from_slice(&first);
        rehash_index(&mut duplicate, ObjectFormat::Sha1)?;
        assert_raw_load_fails(&log, &view, pack.clone(), duplicate).await?;

        let (mut descriptor, root) = store_raw(&log, &view, pack, index).await?;
        descriptor.id = ObjectId::from_bytes(ObjectFormat::Sha1, &[0x42; 20])?;
        assert_load_fails(&log, &view, ObjectFormat::Sha1, descriptor, root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn nodes_reject_wrong_child_kind_lengths_and_descriptor_bytes() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "node-corruption").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, true, true)?;
        let pack = Bytes::from(fixture.normalized.bytes.clone());
        let descriptor = PackDescriptor {
            id: fixture.normalized.id,
            bytes: pack.len() as u64,
        };

        let child = log
            .put_object(&view, Bytes::from(vec![0; CHUNK_BYTES]))
            .await?;
        let oversized = log
            .put_node(
                &view,
                Bytes::from(vec![0; MAX_INDEX_BYTES + 1]),
                vec![child; MAX_PACK_BYTES / CHUNK_BYTES],
            )
            .await?;
        assert_eq!(oversized.reference().len(), MAX_PACK_ROOT_BYTES as u64 + 1);
        store.reset();
        assert_load_fails(
            &log,
            &view,
            ObjectFormat::Sha1,
            descriptor.clone(),
            oversized,
        )
        .await?;
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);

        let blob = log.put_object(&view, pack.clone()).await?;
        let child_node = log.put_node(&view, Bytes::new(), vec![blob]).await?;
        let root = log
            .put_node(
                &view,
                Bytes::from(fixture.normalized.index.clone()),
                vec![child_node],
            )
            .await?;
        assert_load_fails(&log, &view, ObjectFormat::Sha1, descriptor.clone(), root).await?;

        let first = log.put_object(&view, pack.slice(..CHUNK_BYTES - 1)).await?;
        let second = log.put_object(&view, pack.slice(CHUNK_BYTES - 1..)).await?;
        let root = log
            .put_node(
                &view,
                Bytes::from(fixture.normalized.index.clone()),
                vec![first, second],
            )
            .await?;
        assert_load_fails(&log, &view, ObjectFormat::Sha1, descriptor.clone(), root).await?;

        let (mut wrong, root) = store_raw(
            &log,
            &view,
            fixture.normalized.bytes,
            fixture.normalized.index,
        )
        .await?;
        wrong.bytes += 1;
        assert_load_fails(&log, &view, ObjectFormat::Sha1, wrong, root).await?;
        Ok(())
    }

    #[tokio::test]
    async fn entries_reject_crc_zlib_trailing_bytes_and_wrong_oid() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "entry-corruption").await?;
        let initial = fixture(ObjectFormat::Sha1, 1, true, false)?;
        let original_id = initial.objects[0].0;
        let original_pack = initial.normalized.bytes;
        let original_index = initial.normalized.index;
        let range = entry_range(&original_index, original_pack.len(), ObjectFormat::Sha1, 0)?;
        let entry = gix_pack::data::Entry::from_bytes(
            &original_pack[range.clone()],
            range.start as u64,
            object_hash(ObjectFormat::Sha1),
        )?;
        let compressed = usize::try_from(entry.data_offset)?;
        assert!(delta_integer(&[0xff; 16]).is_err());
        assert!(apply_delta(&test_operation(), &[], &[0, 1, 1], false).is_err());

        let mut pack = original_pack.clone();
        pack[compressed] ^= 1;
        assert_find_raw_fails(&log, &view, pack, original_index.clone(), original_id).await?;

        let mut pack = original_pack.clone();
        let mut index = original_index.clone();
        pack[compressed] = 0;
        refresh_entry(&mut pack, &mut index, ObjectFormat::Sha1, 0)?;
        assert_find_raw_fails(&log, &view, pack, index, original_id).await?;

        let mut pack = original_pack.clone();
        let mut index = original_index.clone();
        pack.insert(pack.len() - 20, 0);
        refresh_entry(&mut pack, &mut index, ObjectFormat::Sha1, 0)?;
        assert_find_raw_fails(&log, &view, pack, index, original_id).await?;

        let mut pack = original_pack;
        let mut index = original_index;
        pack.remove(pack.len() - 21);
        refresh_entry(&mut pack, &mut index, ObjectFormat::Sha1, 0)?;
        assert_find_raw_fails(&log, &view, pack, index, original_id).await?;

        let fixture = fixture(ObjectFormat::Sha1, 1, true, false)?;
        let mut index = fixture.normalized.index;
        let fake = ObjectId::from_bytes(ObjectFormat::Sha1, &[0x42; 20])?;
        let oid = index_oid_range(ObjectFormat::Sha1, 0);
        index[oid].copy_from_slice(fake.as_bytes());
        exact_fan(&mut index, ObjectFormat::Sha1);
        rehash_index(&mut index, ObjectFormat::Sha1)?;
        assert_find_raw_fails(&log, &view, fixture.normalized.bytes, index, fake).await?;
        Ok(())
    }

    fn indexed_entries(
        normalized: &Normalized,
        format: ObjectFormat,
    ) -> TestResult<Vec<(ObjectId, gix_pack::data::entry::Header, usize)>> {
        let index = gix_pack::index::File::from_data(
            normalized.index.as_slice(),
            PathBuf::new(),
            object_hash(format),
        )?;
        let by_offset = index
            .iter()
            .map(|entry| (entry.pack_offset, entry.oid))
            .collect::<BTreeMap<_, _>>();
        let mut entries = Vec::new();
        for position in 0..index.num_objects() {
            let offset = index.pack_offset_at_index(position);
            let range = entry_range(&normalized.index, normalized.bytes.len(), format, position)?;
            let entry = gix_pack::data::Entry::from_bytes(
                &normalized.bytes[range],
                offset,
                object_hash(format),
            )?;
            let mut depth = 0;
            let mut header = entry.header;
            let mut current = offset;
            while header.is_delta() {
                current = match header {
                    gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                        current - base_distance
                    }
                    gix_pack::data::entry::Header::RefDelta { base_id } => index
                        .lookup(base_id)
                        .map(|base| index.pack_offset_at_index(base))
                        .ok_or("REF_DELTA base is missing")?,
                    _ => current,
                };
                let base_id = by_offset
                    .get(&current)
                    .ok_or("delta base offset is missing")?;
                let base = index.lookup(base_id).ok_or("delta base ID is missing")?;
                let range = entry_range(&normalized.index, normalized.bytes.len(), format, base)?;
                header = gix_pack::data::Entry::from_bytes(
                    &normalized.bytes[range],
                    current,
                    object_hash(format),
                )?
                .header;
                depth += 1;
            }
            entries.push((
                ObjectId::from_bytes(format, index.oid_at_index(position).as_bytes())?,
                entry.header,
                depth,
            ));
        }
        Ok(entries)
    }

    async fn delta_style_round_trips(ofs: bool) -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), if ofs { "ofs" } else { "ref" }).await?;
        let fixture = fixture(ObjectFormat::Sha1, 10, ofs, false)?;
        let entries = indexed_entries(&fixture.normalized, ObjectFormat::Sha1)?;
        let deepest = entries
            .iter()
            .max_by_key(|entry| entry.2)
            .ok_or("pack is empty")?;
        let (deep_id, deep_header, depth) = *deepest;
        assert!(depth >= 2);
        assert_eq!(
            entries
                .iter()
                .any(|entry| matches!(entry.1, gix_pack::data::entry::Header::OfsDelta { .. })),
            ofs
        );
        let mut corrupt_pack = fixture.normalized.bytes.clone();
        let mut corrupt_index = fixture.normalized.index.clone();
        let index = gix_pack::index::File::from_data(
            corrupt_index.as_slice(),
            PathBuf::new(),
            object_hash(ObjectFormat::Sha1),
        )?;
        let position = index
            .lookup(gix_hash::ObjectId::try_from(deep_id.as_bytes())?)
            .ok_or("deep delta is absent")?;
        let range = entry_range(
            &corrupt_index,
            corrupt_pack.len(),
            ObjectFormat::Sha1,
            position,
        )?;
        let entry = gix_pack::data::Entry::from_bytes(
            &corrupt_pack[range],
            index.pack_offset_at_index(position),
            object_hash(ObjectFormat::Sha1),
        )?;
        let data_offset = usize::try_from(entry.data_offset)?;
        match deep_header {
            gix_pack::data::entry::Header::RefDelta { .. } => {
                corrupt_pack[data_offset - 20..data_offset].copy_from_slice(deep_id.as_bytes());
            }
            gix_pack::data::entry::Header::OfsDelta { .. } => {
                corrupt_pack[data_offset - 1] ^= 1;
            }
            _ => return Err("deepest entry is not a delta".into()),
        }
        refresh_entry(
            &mut corrupt_pack,
            &mut corrupt_index,
            ObjectFormat::Sha1,
            position,
        )?;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let mut reader = Reader::new(&log, &view, &catalog);
        for (id, expected) in fixture.objects {
            let found = reader.find(id).await?.ok_or("object is missing")?;
            assert_eq!(&found.data[..], expected);
        }
        assert_find_raw_fails(&log, &view, corrupt_pack, corrupt_index, deep_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn multi_level_ofs_and_ref_delta_chains_round_trip_with_limits() -> TestResult {
        delta_style_round_trips(true).await?;
        delta_style_round_trips(false).await
    }

    #[tokio::test]
    async fn collection_keeps_published_pack_tree_and_removes_unpublished_tree() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store, "collection").await?;
        let live = fixture(ObjectFormat::Sha1, 1, true, false)?;
        let live_id = live.objects[0].0;
        let (live_descriptor, live_root) =
            stage(&test_operation(), &log, &view, live.normalized).await?;
        let live_ref = live_root.reference().clone();
        let dead = fixture(ObjectFormat::Sha1, 1, true, false)?;
        let (dead_descriptor, dead_root) =
            stage(&test_operation(), &log, &view, dead.normalized).await?;
        let dead_ref = dead_root.reference().clone();
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"publish Git pack"),
            Bytes::new(),
            vec![live_root],
        )?;
        let CommitStatus::Committed(current) = log.commit(prepared).await? else {
            return Err("pack publication did not commit".into());
        };
        let CollectionStart::Installed(fenced, _) = log.start_collection(&current).await? else {
            return Err("collection did not install".into());
        };
        let CollectionFinish::Complete(current, _) = log.resume_collection(&fenced).await? else {
            return Err("collection did not complete".into());
        };
        let live = load(
            &test_operation(),
            &log,
            &current,
            ObjectFormat::Sha1,
            &[(live_descriptor, live_ref)],
        )
        .await?;
        assert!(
            Reader::new(&log, &current, &live)
                .find(live_id)
                .await?
                .is_some()
        );
        assert!(
            load(
                &test_operation(),
                &log,
                &current,
                ObjectFormat::Sha1,
                &[(dead_descriptor, dead_ref)],
            )
            .await
            .is_err()
        );
        Ok(())
    }
}
