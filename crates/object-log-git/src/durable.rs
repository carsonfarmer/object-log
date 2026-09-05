use std::io::{self, Write};
use std::{collections::VecDeque, mem::size_of, path::PathBuf};

#[path = "durable_fetch.rs"]
mod fetch_writer;
#[path = "durable_catalog.rs"]
mod tree_reader;
use tree_reader::SelectedPacks;

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
const MAX_CHUNKS: usize = u16::MAX as usize + 1;
// A child reference uses at most 62 CBOR bytes for a <= 1 MiB blob.
const MAX_PACK_ROOT_BYTES: usize = MAX_INDEX_BYTES + 64 + 62 * MAX_CHUNKS;
const MAX_CATALOG_BYTES: usize = 24 * CHUNK_BYTES;
const MAX_CACHE_BYTES: usize = 8 * CHUNK_BYTES;
const MAX_TRANSFERS: usize = 8;

type PackIndex = gix_pack::index::File<Bytes>;
type PackEntry = gix_pack::data::Entry;
type EntryHeader = gix_pack::data::entry::Header;

// Core writes decode an authenticated active collection plan. The factor
// covers candidate structs, decoded byte vectors, and canonical re-encoding,
// including malformed plans whose short arrays expand into large structs.
pub(crate) fn publication_plan(
    operation: &Operation,
    view: &View,
) -> Result<crate::pack::budget::Reservation, Error> {
    let bytes = usize::try_from(view.collection_plan_bytes().unwrap_or(0))
        .map_err(|_| Error::InvalidPack("collection plan exceeds memory".into()))?;
    if bytes != 0 {
        operation.work(bytes)?;
    }
    operation.reserve(
        bytes
            .checked_mul(128)
            .ok_or_else(|| Error::InvalidPack("collection plan exceeds memory".into()))?,
    )
}

pub(crate) async fn stage(
    operation: &Operation,
    log: &Log,
    view: &View,
    normalized: Normalized,
) -> Result<(PackDescriptor, StagedObject), Error> {
    if normalized.bytes.len() > MAX_PACK_BYTES || normalized.index.len() > MAX_INDEX_BYTES {
        return invalid("staged pack or index exceeds byte limit");
    }
    let bytes = Bytes::from(normalized.bytes);
    let width = CHUNK_BYTES.min(log.options().max_object_bytes);
    if width == 0 || bytes.is_empty() {
        return invalid("pack chunk width is zero");
    }
    let count = bytes.len().div_ceil(width);
    if count > log.options().max_object_refs.min(MAX_CHUNKS) {
        return invalid("pack needs too many chunks");
    }
    let staging_bytes = count
        .checked_mul(size_of::<StagedObject>() + size_of::<ObjectRef>())
        .ok_or_else(|| Error::InvalidPack("Git staging size overflowed".into()))?;
    let _staging_memory = operation.reserve(staging_bytes)?;
    let root_bytes = log.node_size(
        normalized.index.len(),
        bytes.chunks(width).map(|chunk| chunk.len() as u64),
    )?;
    let _root_memory = operation.reserve(root_bytes)?;
    operation.work(root_bytes)?;

    let children = stream::iter((0..count).map(|index| {
        let chunk = bytes.slice(index * width..bytes.len().min((index + 1) * width));
        async move {
            let _plan_memory = publication_plan(operation, view)?;

            Ok::<_, Error>(log.put_object(view, chunk).await?)
        }
    }))
    .buffered(MAX_TRANSFERS)
    .try_collect()
    .await?;
    let _plan_memory = publication_plan(operation, view)?;
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
    tree: Option<crate::catalog_tree::CatalogTree>,
    format: ObjectFormat,
    packs: Box<[Pack]>,
    directory: Vec<Location>,
    operation: Operation,
    _memory: Reservation,
}

impl Catalog {
    pub(crate) fn containing_pack(&self, id: ObjectId) -> Option<ObjectId> {
        self.location(id)
            .map(|location| self.packs[usize::from(location.pack)].id)
    }

    fn location(&self, id: ObjectId) -> Option<Location> {
        self.directory
            .binary_search_by(|location| oid(&self.packs, *location).cmp(id.as_bytes()))
            .ok()
            .map(|position| self.directory[position])
    }
}

/// One authenticated selected index, tied to the caller's exact read context.
/// Loading and enumerating it never reads pack blob chunks or unrelated roots.
pub(crate) struct SelectedIndex<'a> {
    pack: Pack,
    root: StagedObject,
    operation: &'a Operation,
    log: &'a Log,
    view: &'a View,
    _memory: Reservation,
}

impl<'a> SelectedIndex<'a> {
    pub(crate) async fn load(
        operation: &'a Operation,
        log: &'a Log,
        view: &'a View,
        descriptor: &PackDescriptor,
        root: &StagedObject,
    ) -> Result<Self, Error> {
        let format = descriptor.id.format();
        let memory = operation.reserve_state(selected_index_bytes(descriptor, root)?)?;
        let bytes = usize::try_from(root.reference().len()).map_err(pack_error)?;
        let entries = (bytes / (format.digest_len() + 8)).min(MAX_OBJECTS as usize);
        operation.work(
            bytes + entries * (entries.max(1).ilog2() as usize + 1) * size_of::<OffsetEntry>(),
        )?;
        let pack = load_pack(log, view, format, descriptor, root.reference()).await?;
        Ok(Self {
            pack,
            root: root.clone(),
            operation,
            log,
            view,
            _memory: memory,
        })
    }

    /// Stream one indexed object and its same-pack dependency chain to verified
    /// scratch for the receive attempt. No whole decoded object is retained.
    pub(crate) async fn stage_base<'source>(
        &self,
        source: &crate::pack::ingest::Input<'source>,
        id: ObjectId,
        position: u32,
    ) -> Result<crate::pack::ingest::Decoded<'source>, Error> {
        if !source.matches_context(self.operation, self.log, self.view) {
            return invalid("selected index belongs to another receive context");
        }
        self.verify_position(id, position)?;
        let input = source
            .stored_pack(
                &self.root,
                u64::from(self.pack.bytes),
                self.pack.chunk_bytes,
            )
            .await?;
        let count = self.num_objects() as usize;
        let capacity = count.min(MAX_DELTA_DEPTH + 1);
        let _memory = self
            .operation
            .reserve(count + capacity * size_of::<crate::pack::ingest::IndexedEntry>())?;
        let mut visited = vec![false; count];
        let mut chain = Vec::with_capacity(capacity);
        let mut current = position;
        loop {
            if visited[current as usize] {
                return invalid("selected delta graph cycles");
            }
            visited[current as usize] = true;
            let expected = self.object_id_at(current)?;
            let range = self.pack.entry_range(current);
            let entry = input
                .indexed_entry(
                    u64::from(range.start),
                    u64::from(range.end),
                    expected,
                    self.pack.crc(current),
                )
                .await?;
            self.operation.work(count.max(1).ilog2() as usize + 1)?;
            let base = self.pack.base(&entry.header)?;
            chain.push(entry);
            let Some(base) = base else {
                break;
            };
            if chain.len() > MAX_DELTA_DEPTH {
                return invalid("selected delta graph is too deep");
            }
            current = base;
        }
        source.decode_chain(&input, &chain).await
    }

    pub(crate) fn num_objects(&self) -> u32 {
        self.pack.index.num_objects()
    }

    pub(crate) fn object_id_at(&self, position: u32) -> Result<ObjectId, Error> {
        self.operation.work(self.pack.id.format().digest_len())?;
        if position >= self.num_objects() {
            return invalid("catalog index position is out of range");
        }
        ObjectId::from_bytes(
            self.pack.id.format(),
            self.pack.index.oid_at_index(position).as_bytes(),
        )
    }

    pub(crate) fn position_of(&self, id: ObjectId) -> Result<Option<u32>, Error> {
        if id.format() != self.pack.id.format() {
            return invalid("selected index object format differs");
        }
        self.operation
            .work((self.num_objects().max(1).ilog2() as usize + 1) * id.as_bytes().len())?;
        Ok(self
            .pack
            .index
            .lookup(gix_hash::ObjectId::from_bytes_or_panic(id.as_bytes())))
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = Result<(ObjectId, u32), Error>> + '_ {
        (0..self.num_objects()).map(|position| self.object_id_at(position).map(|id| (id, position)))
    }

    pub(crate) fn verify_position(&self, id: ObjectId, position: u32) -> Result<(), Error> {
        if self.object_id_at(position)? != id {
            return invalid("catalog OID does not match selected index position");
        }
        Ok(())
    }
}

// Includes the wrapper and Arc counters even for stack-only migration callers.
fn selected_index_bytes(descriptor: &PackDescriptor, root: &StagedObject) -> Result<usize, Error> {
    Ok(catalog_bytes(
        descriptor.id.format(),
        &[(descriptor.clone(), root.reference().clone())],
    )? + size_of::<SelectedIndex<'_>>()
        - size_of::<Pack>()
        + 2 * size_of::<usize>())
}

struct Pack {
    id: ObjectId,
    bytes: u32,
    chunk_bytes: usize,
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
    let memory = operation.reserve_state(catalog_bytes(format, roots)?)?;
    for (_, root) in roots {
        let root_bytes = usize::try_from(root.len())
            .map_err(|_| Error::InvalidPack("pack root exceeds memory".into()))?;
        operation.work(root_bytes)?;
    }
    let loads = stream::iter(roots.iter().cloned().map(|(descriptor, root)| async move {
        load_pack(log, view, format, &descriptor, &root).await
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
        tree: None,
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
                + (root / 58) * size_of::<ObjectRef>()
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
    let width = chunks.first().map_or(0, ObjectRef::len);
    if width == 0 || width > CHUNK_BYTES as u64 || chunks.len() > MAX_CHUNKS {
        return invalid("pack chunk geometry is invalid");
    }
    let width = usize::try_from(width)
        .map_err(|_| Error::InvalidPack("pack chunk exceeds memory".into()))?;
    if chunks.len() != bytes.div_ceil(width) {
        return invalid("pack chunk count does not match");
    }
    for (index, child) in chunks.iter().enumerate() {
        let expected = if index + 1 == chunks.len() {
            bytes - index * width
        } else {
            width
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
        chunk_bytes: width,
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
    if descriptor.bytes > crate::pack::MAX_STORED_PACK_BYTES as u64
        || descriptor.bytes < 12 + hash_len
    {
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
    tree_cache: Option<crate::catalog_tree::CatalogCache<'a>>,
    selected_packs: Option<SelectedPacks<'a>>,
    cache: VecDeque<((u16, u16), Bytes)>,
    cache_bytes: usize,
    cache_memory: Option<Reservation>,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(log: &'a Log, view: &'a View, catalog: &'a Catalog) -> Self {
        Self {
            log,
            view,
            catalog,
            tree_cache: None,
            selected_packs: None,
            cache: VecDeque::new(),
            cache_bytes: 0,
            cache_memory: None,
        }
    }

    pub(crate) async fn contains(&mut self, id: ObjectId) -> Result<bool, Error> {
        Ok(self.location(id).await?.is_some())
    }

    /// Authenticated stored entry extent, including its pack entry header.
    /// This is sizing metadata; selected content still needs verification.
    pub(crate) async fn packed_entry_bytes(
        &mut self,
        id: ObjectId,
    ) -> Result<Option<usize>, Error> {
        let Some(location) = self.location(id).await? else {
            return Ok(None);
        };
        self.catalog.operation.work(size_of::<Location>())?;
        let range = self.pack(location.pack).entry_range(location.index);
        Ok(Some(
            usize::try_from(range.end - range.start).map_err(pack_error)?,
        ))
    }

    /// Size for filtering only: full blobs use authenticated, canonical pack
    /// metadata without checking the decoded size or object ID. Selected content
    /// must still pass `verify`. Deltas are verified through bounded replay, so
    /// their size is the decoded result, never the instruction-stream size.
    pub(crate) async fn object_size(&mut self, id: ObjectId) -> Result<Option<usize>, Error> {
        let Some(location) = self.location(id).await? else {
            return Ok(None);
        };
        self.catalog.operation.work(42)?;
        let entry = self.entry_header(location).await?;
        if entry.header == EntryHeader::Blob {
            return Ok(Some(
                usize::try_from(entry.decompressed_size).map_err(pack_error)?,
            ));
        }
        if entry.header.is_delta() {
            return Ok(Some(self.verify_decoded(location).await?.1));
        }
        Ok(self.find(id).await?.map(|object| object.data.len()))
    }

    // Verify blobs and deltas without retaining the whole decoded object.
    // Full structural objects retain their existing bounded parser path.
    pub(crate) async fn verify(&mut self, id: ObjectId) -> Result<Option<gix_object::Kind>, Error> {
        let Some(location) = self.location(id).await? else {
            return Ok(None);
        };
        let pack = self.pack(location.pack);
        let range = pack.entry_range(location.index);
        self.catalog
            .operation
            .work((range.end - range.start) as usize)?;
        let entry = self.entry_header(location).await?;
        if entry.header.is_delta() {
            return Ok(Some(self.verify_decoded(location).await?.0));
        }
        if entry.header != EntryHeader::Blob {
            return Ok(self.find(id).await?.map(|object| object.kind));
        }
        let size = usize::try_from(entry.decompressed_size).map_err(pack_error)?;
        // Include one byte of expansion probing to reject undersized headers.
        self.catalog.operation.work((size + 1) * 2)?;
        let _memory = self
            .catalog
            .operation
            .reserve(INFLATE_BYTES + VERIFY_WINDOW_BYTES)?;
        let mut decoder = BlobVerifier::new(id.format(), entry.decompressed_size);
        self.copy_compressed_entry(location, &entry, &mut decoder)
            .await?;
        decoder.finish(id)?;
        Ok(Some(gix_object::Kind::Blob))
    }

    async fn entry_header(&mut self, location: Location) -> Result<PackEntry, Error> {
        let pack = self.pack(location.pack);
        let range = pack.entry_range(location.index);
        // A canonical u64 size takes at most ten bytes, followed by at
        // most a SHA-256 base ID. Never gather the compressed entry.
        let prefix = self
            .read_range(location.pack, range.start..range.end.min(range.start + 42))
            .await?;
        parse_entry(&prefix, u64::from(range.start), pack.id.format())
    }

    pub(crate) async fn find(&mut self, id: ObjectId) -> Result<Option<Object>, Error> {
        let Some(location) = self.location(id).await? else {
            return Ok(None);
        };
        let pack = self.pack(location.pack);
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

    pub(crate) async fn fetch_pack(&mut self, ids: &[ObjectId]) -> Result<Bytes, Error> {
        let memory = self.catalog.operation.reserve(MAX_FETCH_PACK_BYTES)?;
        let mut bytes = Vec::with_capacity(MAX_FETCH_PACK_BYTES);
        let operation = &self.catalog.operation;
        {
            let sink = futures::sink::unfold(&mut bytes, |bytes, frame: Bytes| async move {
                if frame.len() > MAX_FETCH_PACK_BYTES - bytes.len() {
                    return Err(io::Error::other(pack_error(
                        "fetch pack exceeds byte limit",
                    )));
                }
                operation.work(frame.len()).map_err(io::Error::other)?;
                bytes.extend_from_slice(&frame);
                Ok(bytes)
            });
            self.write_fetch(ids, &mut Box::pin(sink)).await?;
        }
        Ok(hold(Bytes::from(bytes), memory))
    }

    // The synchronous consumer verifies bounded full-object inflation.
    async fn copy_compressed_entry(
        &mut self,
        location: Location,
        entry: &PackEntry,
        writer: &mut impl Write,
    ) -> Result<(), Error> {
        let pack = self.pack(location.pack);
        let range = pack.entry_range(location.index);
        let mut crc = 0;
        let mut skip = entry.header_size();
        self.visit_range(location.pack, range, |bytes| {
            crc = gix_features::hash::crc32_update(crc, bytes);
            let header_bytes = skip.min(bytes.len());
            skip -= header_bytes;
            writer
                .write_all(&bytes[header_bytes..])
                .map_err(output_error)
        })
        .await?;
        // The output is private until every entry validates. Failure drops
        // it, so corrupt entries never become a successful fetch response.
        if crc != pack.crc(location.index) {
            return invalid("pack entry CRC does not match");
        }
        Ok(())
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
        let stored = self.pack(pack);
        let range = stored.entry_range(index);
        let offset = u64::from(range.start);
        self.catalog
            .operation
            .work((range.end - range.start) as usize)?;
        let bytes = self.read_range(pack, range).await?;
        if gix_features::hash::crc32(&bytes) != stored.crc(index) {
            return invalid("pack entry CRC does not match");
        }
        let entry = parse_entry(&bytes, offset, stored.id.format())?;
        let compressed = bytes.slice(entry.header_size()..);
        Ok((entry, compressed))
    }

    async fn read_range(&mut self, pack: u16, range: std::ops::Range<u32>) -> Result<Bytes, Error> {
        let stored = self.pack(pack);
        if range.start > range.end || range.end > stored.bytes {
            return invalid("pack range is invalid");
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let width = stored.chunk_bytes;
        let first = range.start as usize / width;
        let last = (range.end as usize - 1) / width;
        if first == last {
            let chunk = self.chunk(pack, first).await?;
            let end = range.end as usize % width;
            return Ok(
                chunk.slice(range.start as usize % width..if end == 0 { chunk.len() } else { end })
            );
        }
        let length = (range.end - range.start) as usize;
        let memory = self.catalog.operation.reserve(length)?;
        let mut bytes = BytesMut::with_capacity(length);
        self.visit_range(pack, range, |chunk| {
            bytes.extend_from_slice(chunk);
            Ok(())
        })
        .await?;
        Ok(hold(bytes.freeze(), memory))
    }

    // Each callback borrows one authenticated chunk slice. A consumer cannot
    // retain cache-owned Bytes; this method never gathers the full range.
    async fn visit_range(
        &mut self,
        pack: u16,
        range: std::ops::Range<u32>,
        mut visit: impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let stored = self.pack(pack);
        if range.start > range.end || range.end > stored.bytes {
            return invalid("pack range is invalid");
        }
        let width = stored.chunk_bytes;
        let mut position = range.start as usize;
        while position < range.end as usize {
            let chunk = self.chunk(pack, position / width).await?;
            let start = position % width;
            let count = (range.end as usize - position).min(chunk.len() - start);
            visit(&chunk[start..start + count])?;
            position += count;
        }
        Ok(())
    }

    async fn chunk(&mut self, pack: u16, index: usize) -> Result<Bytes, Error> {
        let index = u16::try_from(index)
            .map_err(|_| Error::InvalidPack("pack chunk index exceeds u16".into()))?;
        if let Some((_, bytes)) = self.cache.iter().find(|(key, _)| *key == (pack, index)) {
            return Ok(bytes.clone());
        }
        if self.cache_memory.is_none() {
            // Every inserted entry costs one cumulative I/O call. Reserving the
            // call limit bounds metadata even with very small stored chunks.
            let capacity = self.catalog.operation.call_limit();
            self.cache_memory = Some(
                self.catalog
                    .operation
                    .reserve(capacity * size_of::<((u16, u16), Bytes)>())?,
            );
            self.cache = VecDeque::with_capacity(capacity);
        }
        let object = self
            .pack(pack)
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

const VERIFY_WINDOW_BYTES: usize = 32 * 1024;

struct BlobVerifier {
    inflate: gix_zlib::Decompress,
    hash: gix_hash::Hasher,
    output: Vec<u8>,
    size: u64,
    ended: bool,
}

impl BlobVerifier {
    fn new(format: ObjectFormat, size: u64) -> Self {
        let mut hash = gix_hash::hasher(object_hash(format));
        hash.update(&gix_object::encode::loose_header(
            gix_object::Kind::Blob,
            size,
        ));
        Self {
            inflate: gix_zlib::Decompress::new(),
            hash,
            output: vec![0; VERIFY_WINDOW_BYTES],
            size,
            ended: false,
        }
    }

    fn consume(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        loop {
            if self.ended {
                return if bytes.is_empty() {
                    Ok(())
                } else {
                    invalid("pack entry zlib stream is not exact")
                };
            }
            let remaining =
                usize::try_from(self.size - self.inflate.total_out()).map_err(pack_error)?;
            let capacity = (remaining + 1).min(self.output.len());
            let before_in = self.inflate.total_in();
            let before_out = self.inflate.total_out();
            let status = self
                .inflate
                .decompress(
                    bytes,
                    &mut self.output[..capacity],
                    gix_zlib::FlushDecompress::None,
                )
                .map_err(pack_error)?;
            let consumed =
                usize::try_from(self.inflate.total_in() - before_in).map_err(pack_error)?;
            let written =
                usize::try_from(self.inflate.total_out() - before_out).map_err(pack_error)?;
            if written > remaining {
                return invalid("pack entry exceeds its declared size");
            }
            self.hash.update(&self.output[..written]);
            bytes = &bytes[consumed..];
            self.ended = status == gix_zlib::Status::StreamEnd;
            if self.ended {
                continue;
            }
            if consumed == 0 && written == 0 {
                return if bytes.is_empty() {
                    Ok(())
                } else {
                    invalid("pack entry zlib stream made no progress")
                };
            }
            // Drain a full output window even if this chunk's input is spent.
            if bytes.is_empty() && written < capacity {
                return Ok(());
            }
        }
    }

    fn finish(self, id: ObjectId) -> Result<(), Error> {
        if !self.ended || self.inflate.total_out() != self.size {
            return invalid("pack entry zlib stream is not exact");
        }
        if self.hash.try_finalize().map_err(pack_error)?.as_slice() != id.as_bytes() {
            return invalid("decoded object ID does not match");
        }
        Ok(())
    }
}

impl Write for BlobVerifier {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.consume(bytes).map_err(io::Error::other)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn parse_entry(bytes: &[u8], offset: u64, format: ObjectFormat) -> Result<PackEntry, Error> {
    let entry = PackEntry::from_bytes(bytes, offset, object_hash(format)).map_err(pack_error)?;
    if entry.header_size() != entry.header.size(entry.decompressed_size) {
        return invalid("pack entry header is not canonical");
    }
    if entry.decompressed_size > crate::pack::MAX_STREAM_OBJECT_BYTES as u64
        || (entry
            .header
            .as_kind()
            .is_some_and(|kind| kind != gix_object::Kind::Blob)
            && entry.decompressed_size > MAX_OBJECT_BYTES as u64)
    {
        return invalid("pack entry exceeds object byte limit");
    }
    Ok(entry)
}

pub(crate) fn output_error(error: io::Error) -> Error {
    error.downcast::<Error>().unwrap_or_else(pack_error)
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

    include!("durable_catalog_tests.rs");

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

    async fn stage(
        operation: &crate::pack::budget::Operation,
        log: &Log,
        view: &View,
        normalized: Normalized,
    ) -> Result<(PackDescriptor, StagedObject), Error> {
        let guarded = log.with_request_guard(Arc::new(operation.clone()));
        super::stage(operation, &guarded, view, normalized).await
    }

    async fn load(
        operation: &crate::pack::budget::Operation,
        log: &Log,
        view: &View,
        format: ObjectFormat,
        roots: &[(PackDescriptor, ObjectRef)],
    ) -> Result<Catalog, Error> {
        let guarded = log.with_request_guard(Arc::new(operation.clone()));
        super::load(operation, &guarded, view, format, roots).await
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
        blocked.work(crate::pack::budget::WORK_BYTES - root_bytes as u64 + 1)?;
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

        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));

        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut shared = Reader::new(&guarded_log, &view, &catalog);
        assert!(shared.find(first_id).await?.is_some());
        while catalog.operation.calls() < crate::pack::budget::CALLS {
            catalog.operation.io(0)?;
        }
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut second = Reader::new(&guarded_log, &view, &catalog);
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
    async fn variable_chunks_authenticate_geometry_and_keep_reads_sparse() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for width in [8_240, 16 * 1024, CHUNK_BYTES] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let backend =
                    ValidatedBackend::new(Arc::new(store.clone()), StorePath::from("variable"))
                        .await?;
                let log = Log::open(
                    &backend,
                    &LogId::new("chunks")?,
                    Options {
                        max_object_bytes: width,
                        ..Options::default()
                    },
                )
                .await?;
                let view = log.load().await?;
                let fixture = pack_fixture(format, vec![vec![b'x'; width * 2 + 100]], false, true)?;
                let expected = fixture.normalized.bytes.clone();
                let root_bytes = log.node_size(
                    fixture.normalized.index.len(),
                    expected.chunks(width).map(|chunk| chunk.len() as u64),
                )?;
                let (descriptor, root) =
                    stage(&test_operation(), &log, &view, fixture.normalized).await?;
                assert_eq!(root.reference().len(), root_bytes as u64);
                let catalog = load_one(&log, &view, format, descriptor, &root).await?;
                assert_eq!(catalog.packs[0].chunk_bytes, width);
                let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
                let mut reader = Reader::new(&guarded_log, &view, &catalog);
                store.reset();
                let boundary = u32::try_from(width)?;
                assert_eq!(
                    &reader.read_range(0, boundary - 1..boundary + 1).await?[..],
                    &expected[(width - 1)..=width]
                );
                let gets = store.metrics().operation(StoreOperation::Get);
                assert_eq!(gets.requests, 2);
                assert_eq!(gets.downloaded_bytes, 2 * width as u64);
                assert!(gets.downloaded_bytes < expected.len() as u64);
                assert_eq!(
                    &reader.read_range(0, 0..boundary).await?[..],
                    &expected[..width]
                );
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 2);
                let end = u32::try_from(expected.len())?;
                assert_eq!(
                    &reader.read_range(0, end - 1..end).await?[..],
                    &expected[expected.len() - 1..]
                );
                let (id, data) = &fixture.objects[0];
                assert_eq!(
                    &reader.find(*id).await?.ok_or("missing object")?.data[..],
                    data
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn cache_metadata_is_reserved_before_the_first_chunk_read() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "cache-metadata").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, false)?;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let metadata = crate::pack::budget::CALLS * size_of::<((u16, u16), Bytes)>();
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        let pressure = catalog
            .operation
            .reserve(LIVE_BYTES - catalog.operation.live_bytes() - metadata + 1)?;
        store.reset();
        assert!(reader.read_range(0, 0..1).await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        assert_eq!(reader.cache.capacity(), 0);
        drop(pressure);
        let before = catalog.operation.live_bytes();
        drop(reader.read_range(0, 0..1).await?);
        assert_eq!(
            catalog.operation.live_bytes() - before,
            metadata + reader.cache_bytes
        );
        drop(reader);
        assert_eq!(catalog.operation.live_bytes(), before);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_variable_geometry_is_rejected_before_blob_reads() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "geometry").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, false)?;
        let size = fixture.normalized.bytes.len();
        for lengths in [
            vec![],
            vec![0],
            vec![CHUNK_BYTES + 1],
            vec![1, size - 1],
            vec![size, 1],
            vec![size - 1],
            vec![size / 3, size / 3 - 1, size - 2 * (size / 3) + 1],
        ] {
            let mut children = Vec::new();
            for length in lengths {
                children.push(log.put_object(&view, Bytes::from(vec![0; length])).await?);
            }
            let root = log
                .put_node(
                    &view,
                    Bytes::from(fixture.normalized.index.clone()),
                    children,
                )
                .await?;
            let descriptor = PackDescriptor {
                id: fixture.normalized.id,
                bytes: size as u64,
            };
            store.reset();
            assert_load_fails(&log, &view, ObjectFormat::Sha1, descriptor, root).await?;
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn stage_reserves_exact_maximum_before_writes_and_through_root_put() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "stage-memory").await?;
        let vectors = MAX_PACK_BYTES.div_ceil(CHUNK_BYTES)
            * (size_of::<StagedObject>() + size_of::<ObjectRef>());
        let root_bytes = log.node_size(
            MAX_INDEX_BYTES,
            std::iter::repeat_n(CHUNK_BYTES as u64, MAX_PACK_BYTES / CHUNK_BYTES),
        )?;
        let required = MAX_PACK_BYTES + MAX_INDEX_BYTES + root_bytes + vectors;
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
        assert_eq!(root.reference().len(), root_bytes as u64);
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
    async fn oversized_staged_root_is_rejected_before_any_put() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let backend =
            ValidatedBackend::new(Arc::new(store.clone()), StorePath::from("root-limit")).await?;
        let log = Log::open(
            &backend,
            &LogId::new("oversized-root")?,
            Options {
                max_object_bytes: MAX_INDEX_BYTES,
                ..Options::default()
            },
        )
        .await?;
        let view = log.load().await?;
        let operation = test_operation();
        let normalized = maximal_normalized(&operation)?;
        store.reset();
        assert!(matches!(
            stage(&operation, &log, &view, normalized).await,
            Err(Error::ObjectLog(object_log::Error::LimitExceeded(
                "object bytes"
            )))
        ));
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
        assert!(
            Reader::new(
                &log.with_request_guard(Arc::new(catalog.operation.clone())),
                view,
                &catalog
            )
            .find(id)
            .await
            .is_err()
        );
        assert!(
            Reader::new(
                &log.with_request_guard(Arc::new(catalog.operation.clone())),
                view,
                &catalog
            )
            .verify(id)
            .await
            .is_err()
        );
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
                let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
                let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
                assert_eq!(reader.verify(delta).await?, Some(gix_object::Kind::Blob));
                let fallback = reader.fetch_pack(&[delta]).await?;
                verify_fetch_pack(&fallback, format, &[delta])?;
                assert!(!inspect_pack(&fallback, format)?[0].0.is_delta());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn selected_base_decodes_full_and_delta_objects_without_materializing_them() -> TestResult
    {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for deltas in [false, true] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let (base_log, view) = open(store.clone(), "selected-base").await?;
                let fixture = if deltas {
                    fixture(format, 10, true, false)?
                } else {
                    pack_fixture(format, vec![vec![b'x'; MAX_OBJECT_BYTES]], false, false)?
                };
                let expected = fixture
                    .objects
                    .iter()
                    .map(|(id, bytes)| (*id, bytes.len()))
                    .collect::<Vec<_>>();
                let (descriptor, root) =
                    stage(&test_operation(), &base_log, &view, fixture.normalized).await?;
                let operation = crate::pack::budget::Pool::new(3 * CHUNK_BYTES).admit()?;
                let log = base_log.with_request_guard(Arc::new(operation.clone()));
                let source =
                    crate::pack::ingest::Input::receive(&operation, &log, &view, stream::empty())
                        .await?;
                let selected =
                    SelectedIndex::load(&operation, &log, &view, &descriptor, &root).await?;
                assert!(operation.reserve(MAX_OBJECT_BYTES).is_err());
                let baseline = operation.live_bytes();
                for (id, position) in selected.entries().collect::<Result<Vec<_>, _>>()? {
                    let decoded = selected.stage_base(&source, id, position).await?;
                    assert_eq!(decoded.id(), id);
                    assert_eq!(
                        decoded.len(),
                        expected
                            .iter()
                            .find(|(expected, _)| *expected == id)
                            .ok_or("missing expected base")?
                            .1 as u64
                    );
                    drop(decoded);
                    assert_eq!(operation.live_bytes(), baseline);
                }
                store.reset();
                let id = expected[0].0;
                assert!(selected.stage_base(&source, id, u32::MAX).await.is_err());
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
                let other_operation = test_operation();
                let foreign = crate::pack::ingest::Input::receive(
                    &other_operation,
                    &log,
                    &view,
                    stream::empty(),
                )
                .await?;
                assert!(selected.stage_base(&foreign, id, 0).await.is_err());
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
                drop(foreign);
                drop(selected);
                drop(source);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn selected_base_rejects_authenticated_wrong_crc_and_delta_result_oid() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for wrong_oid in [false, true] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let (base_log, view) = open(store, "selected-base-corrupt").await?;
                let mut fixture = fixture(format, 3, true, false)?;
                let (id, _, _) = indexed_entries(&fixture.normalized, format)?
                    .into_iter()
                    .find(|(_, header, _)| header.is_delta())
                    .ok_or("missing delta")?;
                let index = gix_pack::index::File::from_data(
                    fixture.normalized.index.as_slice(),
                    PathBuf::new(),
                    object_hash(format),
                )?;
                let position = index
                    .lookup(gix_hash::ObjectId::try_from(id.as_bytes())?)
                    .ok_or("missing index ID")?;
                let mut expected = id;
                if wrong_oid {
                    let mut fake = id.as_bytes().to_vec();
                    let last = fake.len() - 1;
                    fake[last] ^= 1;
                    expected = ObjectId::from_bytes(format, &fake)?;
                    fixture.normalized.index[index_oid_range(format, position as usize)]
                        .copy_from_slice(&fake);
                } else {
                    let offset =
                        index_crc_offset(&fixture.normalized.index, format, position as usize);
                    fixture.normalized.index[offset] ^= 1;
                }
                rehash_index(&mut fixture.normalized.index, format)?;
                let (descriptor, root) =
                    stage(&test_operation(), &base_log, &view, fixture.normalized).await?;
                let operation = test_operation();
                let log = base_log.with_request_guard(Arc::new(operation.clone()));
                let source =
                    crate::pack::ingest::Input::receive(&operation, &log, &view, stream::empty())
                        .await?;
                let selected =
                    SelectedIndex::load(&operation, &log, &view, &descriptor, &root).await?;
                let error = selected
                    .stage_base(&source, expected, position)
                    .await
                    .err()
                    .ok_or("corrupt metadata accepted")?;
                assert!(error.to_string().contains(if wrong_oid {
                    "OID mismatch"
                } else {
                    "CRC mismatch"
                }));
                drop(selected);
                drop(source);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_selected_base_releases_all_attempt_scratch() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (base_log, view) = open(store.clone(), "selected-base-cancel").await?;
        let fixture = fixture(ObjectFormat::Sha1, 3, true, false)?;
        let (descriptor, root) =
            stage(&test_operation(), &base_log, &view, fixture.normalized).await?;
        let operation = test_operation();
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let source =
            crate::pack::ingest::Input::receive(&operation, &log, &view, stream::empty()).await?;
        let selected = SelectedIndex::load(&operation, &log, &view, &descriptor, &root).await?;
        let id = selected.object_id_at(0)?;
        let baseline = operation.live_bytes();
        store.reset();
        let mut pause = store.pause_next_put(FailurePhase::Before);
        let mut decode = Box::pin(selected.stage_base(&source, id, 0));
        tokio::select! { entered = pause.wait_until_entered() => assert!(entered), result = &mut decode => { result?; return Err("selected decode did not pause".into()); } }
        drop(decode);
        assert!(!pause.release());
        assert_eq!(operation.live_bytes(), baseline);
        assert_eq!(base_log.load().await?.generation(), view.generation());
        drop(selected);
        drop(source);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn selected_index_is_sparse_authenticated_and_checks_catalog_positions() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (base_log, view) = open(store.clone(), "selected-index").await?;
            let fixture = fixture(format, 3, true, false)?;
            let expected = fixture
                .objects
                .iter()
                .map(|(id, _)| *id)
                .collect::<std::collections::BTreeSet<_>>();
            let (descriptor, root) =
                stage(&test_operation(), &base_log, &view, fixture.normalized).await?;
            let operation = test_operation();
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            store.reset();
            let selected = SelectedIndex::load(&operation, &log, &view, &descriptor, &root).await?;
            let entries = selected.entries().collect::<Result<Vec<_>, _>>()?;
            assert_eq!(
                entries
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<std::collections::BTreeSet<_>>(),
                expected
            );
            for (id, position) in &entries {
                selected.verify_position(*id, *position)?;
            }
            assert!(
                selected
                    .verify_position(entries[0].0, selected.num_objects())
                    .is_err()
            );
            assert!(selected.verify_position(entries[0].0, u32::MAX).is_err());
            assert!(
                selected
                    .verify_position(entries[0].0, entries[1].1)
                    .is_err()
            );
            let other = if format == ObjectFormat::Sha1 {
                ObjectFormat::Sha256
            } else {
                ObjectFormat::Sha1
            };
            let foreign = ObjectId::from_bytes(other, &vec![0x42; other.digest_len()])?;
            assert!(selected.verify_position(foreign, 0).is_err());
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
            assert_eq!(store.metrics().downloaded_bytes(), root.reference().len());
            operation.work(crate::pack::budget::WORK_BYTES - operation.work_bytes())?;
            assert!(selected.object_id_at(0).is_err());
            drop(selected);
            assert_eq!(operation.live_bytes(), 0);
            let operation = test_operation();
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let pressure = operation.reserve_state(crate::pack::budget::STATE_BYTES)?;
            store.reset();
            assert!(
                SelectedIndex::load(&operation, &log, &view, &descriptor, &root)
                    .await
                    .is_err()
            );
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
            drop(pressure);
            let mut wrong = descriptor.clone();
            wrong.id = ObjectId::from_bytes(format, &vec![0x42; format.digest_len()])?;
            assert!(
                SelectedIndex::load(&operation, &log, &view, &wrong, &root)
                    .await
                    .is_err()
            );
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn object_size_uses_blob_metadata_and_decoded_delta_results() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for ofs in [false, true] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let (log, view) = open(store, "object-size").await?;
                let fixture = fixture(format, 10, ofs, false)?;
                assert!(
                    indexed_entries(&fixture.normalized, format)?
                        .iter()
                        .any(|entry| entry.1.is_delta())
                );
                let (descriptor, root) =
                    stage(&test_operation(), &log, &view, fixture.normalized).await?;
                let catalog = load_one(&log, &view, format, descriptor, &root).await?;
                let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
                let mut reader = Reader::new(&guarded_log, &view, &catalog);
                for (id, data) in fixture.objects {
                    assert_eq!(reader.object_size(id).await?, Some(data.len()));
                }
                let missing = ObjectId::from_bytes(format, &vec![0x42; format.digest_len()])?;
                assert_eq!(reader.object_size(missing).await?, None);
            }
            for size in [0, 15, 16, 127, 128, 2 * CHUNK_BYTES] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let (log, view) = open(store.clone(), "blob-size").await?;
                let fixture = pack_fixture(format, vec![vec![b'x'; size]], false, false)?;
                let id = fixture.objects[0].0;
                let (descriptor, root) =
                    stage(&test_operation(), &log, &view, fixture.normalized).await?;
                let catalog = load_one(&log, &view, format, descriptor, &root).await?;
                let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
                let mut reader = Reader::new(&guarded_log, &view, &catalog);
                reader
                    .entry_header(catalog.location(id).ok_or("missing location")?)
                    .await?;
                // Only the bounded prefix allocation fits; no inflater or object fits.
                let pressure = catalog
                    .operation
                    .reserve(LIVE_BYTES - catalog.operation.live_bytes() - 42)?;
                store.reset();
                assert_eq!(reader.object_size(id).await?, Some(size));
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
                assert!(reader.verify(id).await.is_err());
                drop(pressure);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn object_size_is_filter_metadata_not_content_verification() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (log, view) = open(store, "wrong-size").await?;
            let mut fixture = pack_fixture(format, vec![vec![b'x'; 128]], false, false)?;
            let id = fixture.objects[0].0;
            // Re-authenticate a canonical but false declared size (129 vs 128).
            fixture.normalized.bytes[12] ^= 1;
            fixture.normalized.id = refresh_entry(
                &mut fixture.normalized.bytes,
                &mut fixture.normalized.index,
                format,
                0,
            )?;
            let (descriptor, root) =
                stage(&test_operation(), &log, &view, fixture.normalized).await?;
            let catalog = load_one(&log, &view, format, descriptor, &root).await?;
            let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
            let mut reader = Reader::new(&guarded_log, &view, &catalog);
            assert_eq!(reader.object_size(id).await?, Some(129));
            assert!(reader.verify(id).await.is_err());
            assert!(reader.find(id).await.is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn verifies_full_blobs_with_fixed_memory_and_installed_git_ids() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for uncompressed in [false, true] {
                let store = FaultStore::from_arc(Arc::new(InMemory::new()));
                let (log, view) = open(store.clone(), "verify-blob").await?;
                let fixture = pack_fixture(
                    format,
                    vec![vec![b'x'; 2 * CHUNK_BYTES]],
                    false,
                    uncompressed,
                )?;
                let id = fixture.objects[0].0;
                let (descriptor, root) =
                    stage(&test_operation(), &log, &view, fixture.normalized).await?;
                let catalog = load_one(&log, &view, format, descriptor, &root).await?;
                let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
                let mut reader = Reader::new(&guarded_log, &view, &catalog);
                // Cache compressed chunks before isolating decoder allocation.
                let range = catalog.packs[0].entry_range(0);
                reader.visit_range(0, range, |_| Ok(())).await?;
                let allowance = INFLATE_BYTES + VERIFY_WINDOW_BYTES;
                let pressure = catalog
                    .operation
                    .reserve(LIVE_BYTES - catalog.operation.live_bytes() - allowance)?;
                store.reset();
                let baseline = catalog.operation.live_bytes();
                assert_eq!(reader.verify(id).await?, Some(gix_object::Kind::Blob));
                assert_eq!(catalog.operation.live_bytes(), baseline);
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
                assert!(reader.find(id).await.is_err());
                drop(pressure);
                assert_eq!(
                    reader.find(id).await?.ok_or("missing blob")?.data.len(),
                    2 * CHUNK_BYTES
                );
            }
        }
        Ok(())
    }

    #[test]
    fn streaming_blob_verification_checks_zlib_boundaries_size_and_oid() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for size in [
                0,
                1,
                VERIFY_WINDOW_BYTES - 1,
                VERIFY_WINDOW_BYTES,
                VERIFY_WINDOW_BYTES + 1,
            ] {
                let data = vec![b'x'; size];
                let hash =
                    gix_object::compute_hash(object_hash(format), gix_object::Kind::Blob, &data)?;
                let id = ObjectId::from_bytes(format, hash.as_slice())?;
                let mut compressed = Vec::new();
                let mut writer = gix_zlib::stream::deflate::Write::new(
                    &mut compressed,
                    gix_zlib::Compression::DEFAULT,
                );
                writer.write_all(&data)?;
                writer.flush()?;
                drop(writer);
                for width in [1, 7, compressed.len()] {
                    let mut verifier = BlobVerifier::new(format, size as u64);
                    for chunk in compressed.chunks(width) {
                        verifier.consume(chunk)?;
                    }
                    verifier.finish(id)?;
                }
                let mut verifier = BlobVerifier::new(format, size as u64);
                verifier.consume(&compressed)?;
                assert!(verifier.consume(&[0]).is_err());
                let mut verifier = BlobVerifier::new(format, size as u64);
                let mut trailing = compressed.clone();
                trailing.push(0);
                assert!(verifier.consume(&trailing).is_err());
                let mut verifier = BlobVerifier::new(format, size as u64);
                verifier.consume(&compressed[..compressed.len() - 1])?;
                assert!(verifier.finish(id).is_err());
                let mut verifier = BlobVerifier::new(format, size as u64 + 1);
                verifier.consume(&compressed)?;
                assert!(verifier.finish(id).is_err());
                if size > 0 {
                    let mut verifier = BlobVerifier::new(format, size as u64 - 1);
                    assert!(verifier.consume(&compressed).is_err());
                }
                let wrong = ObjectId::from_bytes(format, &vec![0x42; format.digest_len()])?;
                let mut verifier = BlobVerifier::new(format, size as u64);
                verifier.consume(&compressed)?;
                assert!(verifier.finish(wrong).is_err());
                let mut verifier = BlobVerifier::new(format, size as u64);
                let mut corrupt = compressed;
                let last = corrupt.len() - 1;
                corrupt[last] ^= 1;
                assert!(verifier.consume(&corrupt).is_err());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_blob_verification_releases_decoder_and_admission() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "verify-cancel").await?;
        let fixture = fixture(ObjectFormat::Sha256, 1, false, true)?;
        let id = fixture.objects[0].0;
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let pool = Pool::new(LIVE_BYTES);
        let operation = pool.admit()?;
        let catalog = load(
            &operation,
            &log,
            &view,
            ObjectFormat::Sha256,
            &[(descriptor, root.reference().clone())],
        )
        .await?;
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let baseline = operation.live_bytes();
        let mut pause = store.pause_next_get(FailurePhase::Before);
        let mut pending = Box::pin(reader.verify(id));
        tokio::select! {
            result = &mut pending => { result?; return Err("verification completed before pause".into()); },
            entered = pause.wait_until_entered() => assert!(entered),
        }
        assert!(operation.live_bytes() >= baseline + INFLATE_BYTES + VERIFY_WINDOW_BYTES);
        drop(pending);
        assert_eq!(operation.live_bytes(), baseline);
        assert!(!pause.release());
        drop(reader);
        drop(catalog);
        assert_eq!(operation.live_bytes(), 0);
        drop(operation);
        drop(guarded_log);
        assert!(pool.admit().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn blob_verification_checks_work_and_memory_before_cached_decode() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "verify-budget").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, false)?;
        let id = fixture.objects[0].0;
        let size = fixture.objects[0].1.len();
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let baseline = catalog.operation.live_bytes();
        let pressure = catalog
            .operation
            .reserve(LIVE_BYTES - baseline - INFLATE_BYTES - VERIFY_WINDOW_BYTES + 1)?;
        store.reset();
        assert!(matches!(reader.verify(id).await,
            Err(Error::InvalidPack(message)) if message == "Git live-memory limit exceeded"));
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        drop(pressure);
        let range = catalog.packs[0].entry_range(0);
        let required = (range.end - range.start) as usize + (size + 1) * 2;
        let used = catalog.operation.work_bytes();
        catalog
            .operation
            .work(crate::pack::budget::WORK_BYTES - used - required as u64 + 1)?;
        assert!(matches!(reader.verify(id).await,
            Err(Error::InvalidPack(message)) if message == "Git work limit exceeded"));
        assert_eq!(catalog.operation.live_bytes(), baseline);
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn range_fetch_reuses_large_entries_without_a_contiguous_copy() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (log, view) = open(store.clone(), "range-fetch").await?;
            let fixture = fixture(format, 1, false, true)?;
            let id = fixture.objects[0].0;
            let expected = fixture.normalized.bytes.clone();
            let (descriptor, root) =
                stage(&test_operation(), &log, &view, fixture.normalized).await?;
            let catalog = load_one(&log, &view, format, descriptor, &root).await?;
            let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
            let mut reader = Reader::new(&guarded_log, &view, &catalog);
            let range = catalog.packs[0].entry_range(0);
            assert!(range.end - range.start > u32::try_from(CHUNK_BYTES)?);
            // Warm the two authenticated chunks, then leave enough space for
            // the output and selection but less than one compressed entry.
            let mut length = 0;
            reader
                .visit_range(0, range.clone(), |bytes| {
                    assert!(bytes.len() <= CHUNK_BYTES);
                    length += bytes.len();
                    Ok(())
                })
                .await?;
            assert_eq!(length, (range.end - range.start) as usize);
            let allowance = MAX_FETCH_PACK_BYTES + 65536 + 1024;
            let pressure = catalog
                .operation
                .reserve(LIVE_BYTES - catalog.operation.live_bytes() - allowance)?;
            store.reset();
            let output = reader.fetch_pack(&[id]).await?;
            assert_eq!(&output[..], expected);
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
            // With the fetch output alive, gathering the old contiguous range
            // exceeds the same pool. Streaming reuse succeeded in that pool.
            assert!(matches!(reader.read_range(0, range).await,
                Err(Error::InvalidPack(message)) if message == "Git live-memory limit exceeded"));
            verify_fetch_pack(&output, format, &[id])?;
            drop(output);
            drop(pressure);
        }
        Ok(())
    }

    #[tokio::test]
    async fn range_visits_boundaries_and_stop_on_consumer_failure() -> TestResult {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let (log, view) = open(store.clone(), "range-boundaries").await?;
        let fixture = fixture(ObjectFormat::Sha1, 1, false, true)?;
        let expected = fixture.normalized.bytes.clone();
        let (descriptor, root) = stage(&test_operation(), &log, &view, fixture.normalized).await?;
        let catalog = load_one(&log, &view, ObjectFormat::Sha1, descriptor, &root).await?;
        let end = u32::try_from(expected.len())?;
        let boundary = u32::try_from(CHUNK_BYTES)?;
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        for range in [
            0..0,
            end..end,
            0..boundary,
            boundary - 1..boundary + 1,
            boundary..end,
        ] {
            let mut actual = Vec::new();
            reader
                .visit_range(0, range.clone(), |bytes| {
                    actual.extend_from_slice(bytes);
                    Ok(())
                })
                .await?;
            assert_eq!(actual, expected[range.start as usize..range.end as usize]);
        }
        assert!(
            reader
                .visit_range(0, end..end + 1, |_| Ok(()))
                .await
                .is_err()
        );
        let reversed = std::ops::Range { start: 1, end: 0 };
        assert!(reader.visit_range(0, reversed, |_| Ok(())).await.is_err());
        drop(reader);
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        store.reset();
        assert!(
            reader
                .visit_range(0, 0..end, |_| invalid("consumer stopped"))
                .await
                .is_err()
        );
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn range_fetch_rejects_crc_mismatch_without_retaining_output() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let store = FaultStore::from_arc(Arc::new(InMemory::new()));
            let (log, view) = open(store, "range-crc").await?;
            let fixture = fixture(format, 1, false, true)?;
            let id = fixture.objects[0].0;
            let mut normalized = fixture.normalized;
            // One-object v2 index: header/fanout, ID, then CRC table.
            normalized.index[8 + 1024 + format.digest_len()] ^= 1;
            rehash_index(&mut normalized.index, format)?;
            let (descriptor, root) = stage(&test_operation(), &log, &view, normalized).await?;
            let catalog = load_one(&log, &view, format, descriptor, &root).await?;
            let baseline = catalog.operation.live_bytes();
            let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
            let mut reader = Reader::new(&guarded_log, &view, &catalog);
            assert!(matches!(reader.fetch_pack(&[id]).await,
                Err(Error::InvalidPack(message)) if message == "pack entry CRC does not match"));
            assert!(matches!(reader.verify(id).await,
                Err(Error::InvalidPack(message)) if message == "pack entry CRC does not match"));
            drop(reader);
            assert_eq!(catalog.operation.live_bytes(), baseline);
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
            let output = Reader::new(
                &log.with_request_guard(Arc::new(catalog.operation.clone())),
                &view,
                &catalog,
            )
            .fetch_pack(&[])
            .await?;
            verify_fetch_pack(&output, format, &[])?;
        }
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
            Reader::new(
                &log.with_request_guard(Arc::new(catalog.operation.clone())),
                &view,
                &catalog
            )
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        let baseline = operation.live_bytes();
        store.reset();
        let missing = ObjectId::from_bytes(ObjectFormat::Sha1, &[0xfe; 20])?;
        assert!(reader.fetch_pack(&[missing]).await.is_err());
        assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
        assert_eq!(operation.live_bytes(), baseline);

        let range = catalog.packs[0].entry_range(0);
        let entry_work = usize::try_from(range.end - range.start)?;
        let used = operation.work_bytes();
        operation.work(crate::pack::budget::WORK_BYTES - used - entry_work as u64 - 23)?;
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
            Reader::new(
                &worker_log.with_request_guard(Arc::new(catalog.operation.clone())),
                &worker_view,
                &catalog,
            )
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
        drop(reader.chunk(0, 0).await?);
        let required = (range.end - range.start) as usize + size * 2;
        let used = catalog.operation.work_bytes();
        catalog
            .operation
            .work(crate::pack::budget::WORK_BYTES - used - required as u64 + 1)?;
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
                Bytes::from(vec![0; MAX_PACK_ROOT_BYTES + 1]),
                vec![child; MAX_PACK_BYTES / CHUNK_BYTES],
            )
            .await?;
        assert!(oversized.reference().len() > MAX_PACK_ROOT_BYTES as u64);
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

        let first = log.put_object(&view, pack.slice(..1)).await?;
        let second = log.put_object(&view, pack.slice(1..)).await?;
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
        let guarded_log = log.with_request_guard(Arc::new(catalog.operation.clone()));
        let mut reader = Reader::new(&guarded_log, &view, &catalog);
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
            Reader::new(
                &log.with_request_guard(Arc::new(live.operation.clone())),
                &current,
                &live
            )
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
    include!("durable_fetch_tests.rs");
}
