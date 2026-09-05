use std::{io::Cursor, mem::size_of, path::PathBuf, sync::atomic::AtomicBool};

use gix_pack::data::{Version, input::EntriesToBytesIter};

use crate::{Error, ObjectFormat, ObjectId};

pub(crate) mod ingest;

#[path = "budget.rs"]
pub(super) mod budget;
pub(super) const MAX_RECEIVE_PACK_BYTES: usize = 9 * 1024 * 1024;
pub(super) const MAX_FETCH_PACK_BYTES: usize = 9_437_184;
pub(super) const MAX_PACK_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_INDEX_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_OBJECTS: u32 = 32_768;
pub(super) const MAX_OBJECT_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_DELTA_DEPTH: usize = 256;
pub(super) const INFLATE_BYTES: usize = 48 * 1024;
pub(crate) const SCAN_WINDOW_BYTES: usize = 32 * 1024;
pub(super) const COMPRESS_BYTES: usize = 416 * 1024;
const DEFAULT_LIMITS: Limits = Limits {
    input_bytes: MAX_RECEIVE_PACK_BYTES,
    output_bytes: MAX_PACK_BYTES,
    object_bytes: MAX_OBJECT_BYTES,
    objects: MAX_OBJECTS,
    index_bytes: MAX_INDEX_BYTES,
};

#[derive(Clone, Copy)]
struct Limits {
    input_bytes: usize,
    output_bytes: usize,
    object_bytes: usize,
    objects: u32,
    index_bytes: usize,
}

#[derive(Default)]
struct Stats {
    normalized: usize,
    deltas: usize,
    resolved: usize,
    largest: usize,
    instructions: usize,
    has_ref: bool,
}
impl Stats {
    fn record(&mut self, inflated: usize, result: usize, delta: bool) -> Result<(), Error> {
        self.normalized = total([self.normalized, inflated])?;
        self.resolved = total([self.resolved, result])?;
        self.largest = self.largest.max(result);
        if delta {
            self.deltas = total([self.deltas, result])?;
            self.instructions = self.instructions.max(inflated);
        }
        Ok(())
    }
}

type InputEntry = gix_pack::data::input::Entry;
type Base<'a> = (gix_hash::ObjectId, gix_object::Kind, &'a [u8]);
type Memory = budget::Reservation;
type Resolved = (Vec<InputEntry>, usize, Memory, Memory);
fn checked<T>(value: Option<T>, message: &'static str) -> Result<T, Error> {
    value.ok_or_else(|| Error::InvalidPack(message.into()))
}
fn total<const N: usize>(parts: [usize; N]) -> Result<usize, Error> {
    parts.into_iter().try_fold(0_usize, |sum, part| {
        checked(sum.checked_add(part), "pack size overflowed")
    })
}
fn product(left: usize, right: usize) -> Result<usize, Error> {
    checked(left.checked_mul(right), "pack size overflowed")
}

pub(crate) struct ExternalBase<'a> {
    pub(crate) id: ObjectId,
    pub(crate) kind: gix_object::Kind,
    pub(crate) data: &'a [u8],
}
pub(crate) struct Normalized {
    pub(crate) bytes: Vec<u8>,
    pub(crate) index: Vec<u8>,
    pub(crate) id: ObjectId,
    pub(crate) _memory: [budget::Reservation; 2],
}
pub(crate) enum NormalizeError {
    MissingBase {
        id: ObjectId,
        candidates: Vec<ObjectId>,
        _memory: Memory,
        message: String,
    },
    DuplicateObject(ObjectId),
    Invalid(Error),
}

impl From<Error> for NormalizeError {
    fn from(error: Error) -> Self {
        Self::Invalid(error)
    }
}

#[cfg(test)]
impl NormalizeError {
    fn into_error(self) -> Error {
        match self {
            Self::MissingBase { message, .. } => Error::InvalidPack(message),
            Self::DuplicateObject(_) => {
                Error::InvalidPack("pack contains duplicate object IDs".into())
            }
            Self::Invalid(error) => error,
        }
    }
}

pub(crate) fn normalize_attempt(
    operation: &budget::Operation,
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
) -> Result<Normalized, NormalizeError> {
    normalize_attempt_with(operation, format, input, external_bases, DEFAULT_LIMITS)
}

#[cfg(test)]
pub(crate) fn normalize(
    operation: &budget::Operation,
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
) -> Result<Normalized, Error> {
    normalize_with(operation, format, input, external_bases, DEFAULT_LIMITS)
}
#[cfg(test)]
fn normalize_with(
    operation: &budget::Operation,
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
    limits: Limits,
) -> Result<Normalized, Error> {
    normalize_attempt_with(operation, format, input, external_bases, limits)
        .map_err(NormalizeError::into_error)
}

fn normalize_attempt_with(
    operation: &budget::Operation,
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
    limits: Limits,
) -> Result<Normalized, NormalizeError> {
    if input.len() > limits.input_bytes {
        return Err(Error::InvalidPack("input exceeds byte limit".into()).into());
    }
    let hash = object_hash(format);
    let (entries, mut stats, mut scan_memory) = scan(operation, input, hash, limits)?;
    let input_metadata = product(entries.len(), size_of::<InputEntry>())?;

    let (bases, base_memory) = bases(operation, hash, external_bases, limits, &mut stats)?;
    let (entries, output_bytes, resolve_memory, compressed_memory) = resolve_external(
        operation,
        entries,
        bases,
        limits,
        hash.len_in_bytes(),
        &mut stats,
    )?;
    scan_memory.shrink(input_metadata)?;
    drop(base_memory);
    let object_count = entries.len();
    let _indexed_memory = operation.reserve(product(object_count, size_of::<InputEntry>())?)?;
    let entries = entries.into_iter().map(Ok);
    operation.work(checked(
        product(output_bytes, 2)?.checked_sub(hash.len_in_bytes()),
        "pack rewrite work overflowed",
    )?)?;
    let output_memory = operation.reserve(output_bytes)?;
    let mut output = Cursor::new(Vec::with_capacity(output_bytes));
    let indexed_entries = {
        let mut writer = EntriesToBytesIter::new(entries, &mut output, Version::V2, hash);
        let mut indexed = Vec::with_capacity(object_count);
        for entry in &mut writer {
            indexed.push(entry.map_err(pack_error)?);
        }
        indexed
    };
    let bytes = output.into_inner();
    let (id, index, index_memory) = index(
        operation,
        &bytes,
        format,
        hash,
        indexed_entries,
        limits,
        &stats,
    )?;
    drop((scan_memory, resolve_memory, compressed_memory));
    Ok(Normalized {
        bytes,
        index,
        id,
        _memory: [output_memory, index_memory],
    })
}
fn bases<'a>(
    operation: &budget::Operation,
    hash: gix_hash::Kind,
    source: &[ExternalBase<'a>],
    limits: Limits,
    stats: &mut Stats,
) -> Result<(Vec<Base<'a>>, budget::Reservation), Error> {
    if source.len() > limits.objects as usize {
        return invalid("external base count exceeds limit");
    }
    let memory = operation.reserve(product(source.len(), size_of::<Base<'_>>())?)?;
    let mut bases = Vec::with_capacity(source.len());
    for base in source {
        if base.data.len() > limits.object_bytes {
            return invalid("external base exceeds object byte limit");
        }
        operation.work(base.data.len())?;
        let id = gix_object::compute_hash(hash, base.kind, base.data).map_err(pack_error)?;
        if id.as_slice() != base.id.as_bytes() {
            return invalid("external base ID does not match its data");
        }
        stats.record(base.data.len(), base.data.len(), false)?;
        bases.push((id, base.kind, base.data));
    }
    bases.sort_unstable_by_key(|(id, _, _)| *id);
    if bases.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return invalid("external base is duplicated");
    }
    Ok((bases, memory))
}
fn scan(
    operation: &budget::Operation,
    input: &[u8],
    hash: gix_hash::Kind,
    limits: Limits,
) -> Result<(Vec<InputEntry>, Stats, budget::Reservation), Error> {
    let pack = gix_pack::data::File::from_data(input, PathBuf::new(), hash)
        .map_err(pack_error)?
        .with_alloc_limit_bytes(Some(limits.object_bytes));
    if pack.version() != Version::V2 {
        return invalid("pack version is unsupported");
    }
    let count = pack.num_objects();
    if count > limits.objects {
        return invalid("object count exceeds limit");
    }
    operation.work(input.len())?;
    pack.verify_checksum(
        &mut gix_features::progress::Discard,
        &AtomicBool::new(false),
    )
    .map_err(pack_error)?;

    let mut offset = 12_u64;
    let mut memory = operation.reserve(product(count as usize, size_of::<InputEntry>())?)?;
    let _offset_memory = operation.reserve(product(count as usize, size_of::<u64>())?)?;
    let mut offsets = Vec::with_capacity(count as usize);
    let _inflate_memory = operation.reserve(INFLATE_BYTES + SCAN_WINDOW_BYTES)?;
    let mut inflated = vec![0; SCAN_WINDOW_BYTES];
    let mut inflate = gix_zlib::Decompress::new();
    let mut entries = Vec::with_capacity(count as usize);
    let mut stats = Stats::default();
    for position in 0..count {
        let entry = pack.entry(offset).map_err(pack_error)?;
        if entry.header_size() != entry.header.size(entry.decompressed_size) {
            return invalid("pack entry header is not canonical");
        }
        offsets.push(offset);
        if let gix_pack::data::entry::Header::OfsDelta { base_distance } = entry.header {
            let base = checked(
                entry.checked_base_pack_offset(base_distance),
                "invalid OFS_DELTA base offset",
            )?;
            if offsets.binary_search(&base).is_err() {
                return invalid("OFS_DELTA base is not an earlier entry");
            }
        }

        let inflated_size = usize::try_from(entry.decompressed_size)
            .map_err(|_| Error::InvalidPack("entry size does not fit in memory".into()))?;
        if inflated_size > limits.object_bytes {
            return invalid("entry exceeds object byte limit");
        }
        // One extra output byte detects a falsely small declared size.
        operation.work(total([inflated_size, 1])?)?;
        let start = usize::try_from(entry.data_offset)
            .map_err(|_| Error::InvalidPack("pack entry offset does not fit in memory".into()))?;
        let compressed_input = input
            .get(start..pack.pack_end())
            .ok_or_else(|| Error::InvalidPack("pack entry extends beyond the trailer".into()))?;
        let (compressed, result_size) = scan_inflate(
            &mut inflate,
            compressed_input,
            inflated_size,
            entry.header.is_delta(),
            &mut inflated,
        )?;
        if result_size > limits.object_bytes {
            return invalid("decoded object exceeds byte limit");
        }
        stats.record(inflated_size, result_size, entry.header.is_delta())?;
        let compressed = u64::try_from(compressed)
            .map_err(|_| Error::InvalidPack("compressed entry is too large".into()))?;
        let end = checked(
            entry.data_offset.checked_add(compressed),
            "pack entry offset overflowed",
        )?;
        let end_in_memory = usize::try_from(end)
            .map_err(|_| Error::InvalidPack("pack entry end does not fit in memory".into()))?;
        if end_in_memory > pack.pack_end() {
            return invalid("pack entry extends beyond the trailer");
        }
        let pack_offset = usize::try_from(offset)
            .map_err(|_| Error::InvalidPack("pack entry offset does not fit in memory".into()))?;
        operation.work(end_in_memory - pack_offset)?;
        memory.grow(end_in_memory - start)?;
        entries.push(InputEntry {
            header: entry.header,
            header_size: u16::try_from(entry.header_size())
                .map_err(|_| Error::InvalidPack("pack entry header is too large".into()))?,
            pack_offset: offset,
            compressed: Some(input[start..end_in_memory].to_vec()),
            compressed_size: compressed,
            crc32: Some(pack.entry_crc32(offset, end_in_memory - pack_offset)),
            decompressed_size: entry.decompressed_size,
            trailer: (position + 1 == count).then(|| pack.checksum()),
        });
        offset = end;
    }
    if usize::try_from(offset).ok() != Some(pack.pack_end()) {
        return invalid("pack object count does not match its entries");
    }
    Ok((entries, stats, memory))
}

// Stop at the first exact zlib end: remaining input belongs to later entries.
// Only delta's two size integers need decoded bytes after this scan.
fn scan_inflate(
    inflate: &mut gix_zlib::Decompress,
    input: &[u8],
    declared: usize,
    delta: bool,
    window: &mut [u8],
) -> Result<(usize, usize), Error> {
    inflate.reset();
    let mut prefix = [0_u8; 20]; // Two maximal u64 delta integers, also enough on WASIp2.
    let mut captured = 0;
    loop {
        let written = usize::try_from(inflate.total_out()).map_err(pack_error)?;
        let consumed = usize::try_from(inflate.total_in()).map_err(pack_error)?;
        let capacity = window.len().min(declared - written + 1);
        let status = inflate
            .decompress(
                &input[consumed..],
                &mut window[..capacity],
                gix_zlib::FlushDecompress::None,
            )
            .map_err(pack_error)?;
        let produced = usize::try_from(inflate.total_out()).map_err(pack_error)?;
        if produced > declared {
            return invalid("entry exceeds its declared size");
        }
        let keep = (produced - written).min(prefix.len() - captured);
        prefix[captured..captured + keep].copy_from_slice(&window[..keep]);
        captured += keep;
        if status == gix_zlib::Status::StreamEnd {
            if produced != declared {
                return invalid("entry decoded size does not match");
            }
            let result_size = if delta {
                let (_, offset) = delta_integer(&prefix[..captured])?;
                delta_integer(&prefix[offset..captured])?.0
            } else {
                declared
            };
            return Ok((
                usize::try_from(inflate.total_in()).map_err(pack_error)?,
                result_size,
            ));
        }
        if produced == written && inflate.total_in() == consumed as u64 {
            return invalid("entry zlib stream is truncated or made no progress");
        }
    }
}

pub(super) fn delta_integer(bytes: &[u8]) -> Result<(usize, usize), Error> {
    let mut size = 0_usize;
    let mut shift = 0_u32;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let part = usize::from(byte & 0x7f);
        if part > usize::MAX >> shift {
            return invalid("delta size overflowed");
        }
        size |= part << shift;
        if byte & 0x80 == 0 {
            return Ok((size, index + 1));
        }
        shift = checked(
            shift.checked_add(7).filter(|shift| *shift < usize::BITS),
            "delta size overflowed",
        )?;
    }
    invalid("delta size is truncated")
}

fn resolve_external(
    operation: &budget::Operation,
    entries: Vec<InputEntry>,
    bases: Vec<Base<'_>>,
    limits: Limits,
    trailer_bytes: usize,
    stats: &mut Stats,
) -> Result<Resolved, Error> {
    let input_count = entries.len();
    let count = total([input_count, bases.len()])?;
    if count > limits.objects as usize {
        return invalid("object count exceeds limit after thin-pack resolution");
    }
    let retained = product(count, size_of::<InputEntry>())?;
    let temporary = total([
        product(input_count, size_of::<(u64, u64)>())?,
        product(bases.len(), size_of::<Option<u64>>())?,
    ])?;
    let metadata = total([retained, temporary])?;
    let mut memory = operation.reserve(metadata)?;
    let mut compression_memory = operation.reserve(0)?;
    let mut output = Vec::with_capacity(count);
    let mut translated = Vec::with_capacity(input_count);
    let mut inserted = vec![None; bases.len()];
    let mut next_offset = 12_u64;
    for mut entry in entries {
        let original_offset = entry.pack_offset;
        match entry.header {
            gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                let original_base = checked(
                    original_offset.checked_sub(base_distance),
                    "invalid OFS_DELTA base offset",
                )?;
                let new_base = translated
                    .binary_search_by_key(&original_base, |(old, _)| *old)
                    .ok()
                    .map(|index| translated[index].1)
                    .ok_or_else(|| Error::InvalidPack("OFS_DELTA base is missing".into()))?;
                entry.header = gix_pack::data::entry::Header::OfsDelta {
                    base_distance: next_offset - new_base,
                };
            }
            gix_pack::data::entry::Header::RefDelta { base_id } => {
                if let Ok(base_index) = bases.binary_search_by_key(&base_id, |(id, _, _)| *id) {
                    let (_, kind, data) = bases[base_index];
                    let base_offset = if let Some(offset) = inserted[base_index] {
                        offset
                    } else {
                        let (reserved, crc) = compression_bounds(data.len())?;
                        operation.work(total([data.len(), crc])?)?;
                        compression_memory.grow(reserved)?;
                        let base = InputEntry::from_data_obj(
                            &gix_object::Data {
                                kind,
                                object_hash: base_id.kind(),
                                data,
                            },
                            next_offset,
                            gix_zlib::Compression::default(),
                        )
                        .map_err(pack_error)?;
                        let retained = base.compressed.as_ref().map_or(0, Vec::capacity);
                        compression_memory.shrink(checked(
                            reserved.checked_sub(retained),
                            "compressed base exceeded memory bound",
                        )?)?;
                        let offset = next_offset;
                        next_offset =
                            push_entry(&mut output, base, next_offset, limits, trailer_bytes)?;
                        inserted[base_index] = Some(offset);
                        offset
                    };
                    entry.header = gix_pack::data::entry::Header::OfsDelta {
                        base_distance: next_offset - base_offset,
                    };
                }
            }
            _ => {}
        }
        translated.push((original_offset, next_offset));
        entry.pack_offset = next_offset;
        entry.header_size = u16::try_from(entry.header.size(entry.decompressed_size))
            .map_err(|_| Error::InvalidPack("pack entry header is too large".into()))?;
        operation.work(usize::try_from(entry.bytes_in_pack()).map_err(pack_error)?)?;
        entry.crc32 = Some(entry.compute_crc32());
        stats.has_ref |= matches!(entry.header, gix_pack::data::entry::Header::RefDelta { .. });
        next_offset = push_entry(&mut output, entry, next_offset, limits, trailer_bytes)?;
    }
    if inserted.iter().any(Option::is_none) {
        return invalid("external base set is not exact");
    }
    let output_bytes = checked(
        usize::try_from(next_offset)
            .ok()
            .and_then(|bytes| bytes.checked_add(trailer_bytes)),
        "normalized pack size overflowed",
    )?;
    if output_bytes > limits.output_bytes {
        return invalid("normalized pack exceeds byte limit");
    }
    drop((translated, inserted, bases));
    memory.shrink(temporary)?;
    Ok((output, output_bytes, memory, compression_memory))
}

fn compression_bounds(bytes: usize) -> Result<(usize, usize), Error> {
    let quick = total([bytes, 7])? / 8;
    let bound = total([
        bytes,
        quick,
        9,
        usize::from(bytes == 0),
        usize::from(bytes < 9),
    ])?;
    let capacity = checked(
        bound.checked_next_power_of_two(),
        "compression size overflowed",
    )?
    .max(8);
    Ok((
        total([product(capacity, 2)?, COMPRESS_BYTES])?,
        total([bound, 12])?,
    ))
}

fn push_entry(
    output: &mut Vec<InputEntry>,
    entry: InputEntry,
    offset: u64,
    limits: Limits,
    trailer_bytes: usize,
) -> Result<u64, Error> {
    let next = checked(
        offset.checked_add(entry.bytes_in_pack()),
        "normalized pack offset overflowed",
    )?;
    if usize::try_from(next)
        .ok()
        .is_none_or(|bytes| bytes > limits.output_bytes.saturating_sub(trailer_bytes))
    {
        return invalid("normalized pack exceeds byte limit");
    }
    output.push(entry);
    Ok(next)
}

fn index(
    operation: &budget::Operation,
    pack: &[u8],
    format: ObjectFormat,
    hash: gix_hash::Kind,
    entries: Vec<InputEntry>,
    limits: Limits,
    stats: &Stats,
) -> Result<(ObjectId, Vec<u8>, budget::Reservation), NormalizeError> {
    let object_count = entries.len();
    let graph_item_bytes = size_of::<(u64, gix_pack::data::entry::Header)>()
        + size_of::<Option<usize>>()
        + size_of::<Option<u16>>()
        + size_of::<usize>();
    let _graph_memory = operation.reserve(product(object_count, graph_item_bytes)?)?;
    let relationships = entries
        .iter()
        .map(|entry| (entry.pack_offset, entry.header))
        .collect::<Vec<_>>();
    let index_bytes = 8_usize
        .checked_add(256 * 4)
        .and_then(|bytes| bytes.checked_add((hash.len_in_bytes() + 8).checked_mul(object_count)?))
        .and_then(|bytes| bytes.checked_add(hash.len_in_bytes() * 2))
        .ok_or_else(|| Error::InvalidPack("pack index size overflowed".into()))?;
    if index_bytes > limits.index_bytes {
        return Err(Error::InvalidPack("pack index exceeds byte limit".into()).into());
    }
    operation.work(total([
        stats.normalized,
        stats.deltas,
        stats.resolved,
        usize::from(stats.has_ref) * stats.resolved,
        checked(
            index_bytes.checked_sub(hash.len_in_bytes()),
            "pack index size underflowed",
        )?,
    ])?)?;
    let _gix_memory = operation.reserve(total([
        product(2, stats.resolved)?,
        stats.largest,
        product(3, stats.instructions)?,
        4_096,
        product(1_536, object_count)?,
        INFLATE_BYTES,
        32 * 1024,
    ])?)?;
    let index_memory = operation.reserve(index_bytes)?;
    let mut progress = gix_features::progress::Discard;
    let mut index = Vec::with_capacity(index_bytes);
    let interrupt = AtomicBool::new(false);
    let mut entries = entries.into_iter().map(Ok);
    let outcome = gix_pack::index::write_data_iter_to_stream(
        gix_pack::index::Version::V2,
        || Ok((entry_bytes, pack)),
        &mut entries,
        Some(1),
        &mut progress,
        &mut index,
        &interrupt,
        hash,
        Some(limits.object_bytes),
        Version::V2,
    )
    .map_err(|error| {
        index_error(operation, format, &relationships, error)
            .unwrap_or_else(NormalizeError::Invalid)
    })?;
    let file = gix_pack::index::File::from_data(index.as_slice(), PathBuf::new(), hash)
        .map_err(pack_error)?;
    let mut previous = None;
    for entry in file.iter() {
        if previous == Some(entry.oid) {
            return Err(NormalizeError::DuplicateObject(ObjectId::from_bytes(
                format,
                entry.oid.as_slice(),
            )?));
        }
        previous = Some(entry.oid);
    }
    validate_delta_depth(&relationships, &file)?;
    let id = ObjectId::from_bytes(format, outcome.data_hash.as_slice())?;
    Ok((id, index, index_memory))
}

fn index_error(
    operation: &budget::Operation,
    format: ObjectFormat,
    relationships: &[(u64, gix_pack::data::entry::Header)],
    error: gix_pack::index::write::Error,
) -> Result<NormalizeError, Error> {
    let gix_pack::index::write::Error::TreeTraversal(
        gix_pack::cache::delta::traverse::Error::UnresolvedRefDelta { base_id },
    ) = &error
    else {
        return Ok(NormalizeError::Invalid(pack_error(error)));
    };
    let id = ObjectId::from_bytes(format, base_id.as_slice())?;
    let bytes = relationships.len() * size_of::<ObjectId>();
    let memory = operation.reserve(bytes)?;
    operation.work(bytes)?;
    let mut candidates = Vec::with_capacity(relationships.len());
    for (_, header) in relationships {
        if let gix_pack::data::entry::Header::RefDelta { base_id } = header {
            candidates.push(ObjectId::from_bytes(format, base_id.as_slice())?);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    Ok(NormalizeError::MissingBase {
        id,
        candidates,
        _memory: memory,
        message: error.to_string(),
    })
}

fn validate_delta_depth(
    entries: &[(u64, gix_pack::data::entry::Header)],
    index: &gix_pack::index::File<&[u8]>,
) -> Result<(), Error> {
    let mut parents = Vec::with_capacity(entries.len());
    for (offset, header) in entries {
        let parent = match header {
            gix_pack::data::entry::Header::OfsDelta { base_distance } => Some(
                offset
                    .checked_sub(*base_distance)
                    .ok_or_else(|| Error::InvalidPack("invalid OFS_DELTA base offset".into()))?,
            ),
            gix_pack::data::entry::Header::RefDelta { base_id } => Some(
                index
                    .lookup(base_id)
                    .map(|position| index.pack_offset_at_index(position))
                    .ok_or_else(|| Error::InvalidPack("REF_DELTA base is missing".into()))?,
            ),
            _ => None,
        };
        parents.push(
            parent
                .map(|offset| {
                    entries
                        .binary_search_by_key(&offset, |(offset, _)| *offset)
                        .map_err(|_| Error::InvalidPack("delta base is missing".into()))
                })
                .transpose()?,
        );
    }
    validate_parent_depth(&parents)
}

fn validate_parent_depth(parents: &[Option<usize>]) -> Result<(), Error> {
    let mut depths = vec![None::<u16>; parents.len()];
    let mut path = Vec::with_capacity(parents.len());
    for start in 0..parents.len() {
        path.clear();
        let mut current = start;
        let mut depth = loop {
            if let Some(depth) = depths[current] {
                if depth == u16::MAX {
                    return invalid("delta graph contains a cycle");
                }
                break depth;
            }
            depths[current] = Some(u16::MAX);
            path.push(current);
            match parents[current] {
                Some(parent) => current = parent,
                None => break 0_u16,
            }
        };
        while let Some(node) = path.pop() {
            depth = parents[node].map_or(0, |_| depth + 1);
            if usize::from(depth) > MAX_DELTA_DEPTH {
                return invalid("delta depth exceeds limit");
            }
            depths[node] = Some(depth);
        }
    }
    Ok(())
}

fn entry_bytes<'a>(range: gix_pack::data::EntryRange, pack: &'a &[u8]) -> Option<&'a [u8]> {
    pack.get(usize::try_from(range.start).ok()?..usize::try_from(range.end).ok()?)
}

pub(crate) const fn object_hash(format: ObjectFormat) -> gix_hash::Kind {
    match format {
        ObjectFormat::Sha1 => gix_hash::Kind::Sha1,
        ObjectFormat::Sha256 => gix_hash::Kind::Sha256,
    }
}

pub(super) fn pack_error(error: impl std::fmt::Display) -> Error {
    Error::InvalidPack(error.to_string())
}

pub(super) fn invalid<T>(message: &'static str) -> Result<T, Error> {
    Err(Error::InvalidPack(message.into()))
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write as _,
        ops::Range,
        process::{Command, Stdio},
    };

    use super::*;

    fn operation() -> budget::Operation {
        let Ok(operation) = budget::Pool::new(budget::LIVE_BYTES).admit() else {
            unreachable!("new test pool must admit its first operation")
        };
        operation
    }

    fn normalize(
        format: ObjectFormat,
        input: &[u8],
        external_bases: &[ExternalBase<'_>],
    ) -> Result<Normalized, Error> {
        super::normalize(&operation(), format, input, external_bases)
    }

    fn normalize_with(
        format: ObjectFormat,
        input: &[u8],
        external_bases: &[ExternalBase<'_>],
        limits: Limits,
    ) -> Result<Normalized, Error> {
        super::normalize_with(&operation(), format, input, external_bases, limits)
    }

    struct Fixture {
        dir: tempfile::TempDir,
        blobs: Vec<(ObjectId, Vec<u8>)>,
    }

    impl Fixture {
        fn new(format: ObjectFormat) -> Result<Self, Box<dyn std::error::Error>> {
            let dir = tempfile::tempdir()?;
            let mut init = vec!["init", "--bare", "--quiet"];
            if format == ObjectFormat::Sha256 {
                init.push("--object-format=sha256");
            }
            git(dir.path(), init, &[])?;
            let mut blobs = Vec::new();
            for marker in b'1'..=b'3' {
                let mut data = vec![b'a'; 50_000];
                data.push(marker);
                data.push(b'\n');
                let output = git(dir.path(), ["hash-object", "-w", "--stdin"], &data)?;
                let id = ObjectId::parse(format, std::str::from_utf8(&output)?.trim())?;
                blobs.push((id, data));
            }
            Ok(Self { dir, blobs })
        }

        fn pack(&self, ofs_delta: bool) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            let mut ids = self
                .blobs
                .iter()
                .map(|(id, _)| id.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            ids.push('\n');
            let mut arguments = vec!["pack-objects", "--stdout", "--window=10", "--depth=10"];
            if ofs_delta {
                arguments.push("--delta-base-offset");
            }
            git(self.dir.path(), arguments, ids.as_bytes())
        }
    }

    fn assert_delta_error(bytes: &[u8], expected: &str) {
        assert!(matches!(
            delta_integer(bytes),
            Err(Error::InvalidPack(message)) if message == expected
        ));
    }

    #[test]
    fn delta_integer_rejects_target_width_overflow_and_truncation() {
        let bits = std::mem::size_of::<usize>() * 8;
        let encoded = bits.div_ceil(7);
        let mut terminating_overflow = vec![0x80; encoded];
        terminating_overflow[encoded - 1] = 1 << (bits % 7);
        assert_delta_error(&terminating_overflow, "delta size overflowed");
        assert_delta_error(&vec![0x80; encoded], "delta size overflowed");
        assert_delta_error(&[0x80], "delta size is truncated");
    }

    fn compress_for_scan(data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut bytes = Vec::new();
        let mut writer =
            gix_zlib::stream::deflate::Write::new(&mut bytes, gix_zlib::Compression::DEFAULT);
        writer.write_all(data)?;
        writer.flush()?;
        drop(writer);
        Ok(bytes)
    }

    #[test]
    fn scan_inflation_checks_exact_sizes_and_adjacent_streams()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut decoder = gix_zlib::Decompress::new();
        for size in [
            0,
            1,
            SCAN_WINDOW_BYTES - 1,
            SCAN_WINDOW_BYTES,
            SCAN_WINDOW_BYTES + 1,
        ] {
            let compressed = compress_for_scan(&vec![b'x'; size])?;
            for width in [1, 17, SCAN_WINDOW_BYTES] {
                let mut window = vec![0; width];
                let mut adjacent = compressed.clone();
                adjacent.extend_from_slice(&compressed);
                assert_eq!(
                    scan_inflate(&mut decoder, &adjacent, size, false, &mut window)?,
                    (compressed.len(), size)
                );
                assert!(
                    scan_inflate(&mut decoder, &compressed, size + 1, false, &mut window).is_err()
                );
                if size > 0 {
                    assert!(
                        scan_inflate(&mut decoder, &compressed, size - 1, false, &mut window)
                            .is_err()
                    );
                }
                assert!(
                    scan_inflate(
                        &mut decoder,
                        &compressed[..compressed.len() - 1],
                        size,
                        false,
                        &mut window
                    )
                    .is_err()
                );
            }
            let mut corrupt = compressed;
            let last = corrupt.len() - 1;
            corrupt[last] ^= 1;
            assert!(scan_inflate(&mut decoder, &corrupt, size, false, &mut [0; 17]).is_err());
        }
        Ok(())
    }

    #[test]
    fn scan_inflation_retains_only_delta_size_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut decoder = gix_zlib::Decompress::new();
        let mut data = vec![0x80, 0x01, 0x81, 0x01]; // base 128, result 129
        data.resize(SCAN_WINDOW_BYTES * 2, 0);
        let compressed = compress_for_scan(&data)?;
        assert_eq!(
            scan_inflate(&mut decoder, &compressed, data.len(), true, &mut [0; 1])?,
            (compressed.len(), 129)
        );
        for data in [vec![], vec![0], vec![0x80; 20], vec![0, 0x80]] {
            let compressed = compress_for_scan(&data)?;
            assert!(
                scan_inflate(&mut decoder, &compressed, data.len(), true, &mut [0; 17]).is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn pack_scan_uses_fixed_decode_memory_for_both_hashes() -> Result<(), Box<dyn std::error::Error>>
    {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let mut fixture = Fixture::new(format)?;
            let data = vec![b'x'; 2 * 1024 * 1024];
            let output = git(fixture.dir.path(), ["hash-object", "-w", "--stdin"], &data)?;
            let id = ObjectId::parse(format, std::str::from_utf8(&output)?.trim())?;
            fixture.blobs = vec![(id, data)];
            let input = fixture.pack(false)?;
            let operation = budget::Pool::new(128 * 1024).admit()?;
            let input_memory = operation.reserve(input.len())?;
            let (entries, stats, memory) =
                scan(&operation, &input, object_hash(format), DEFAULT_LIMITS)?;
            assert_eq!(entries.len(), 1);
            assert_eq!(stats.largest, 2 * 1024 * 1024);
            assert_eq!(stats.normalized, 2 * 1024 * 1024);
            assert!(operation.reserve(stats.largest).is_err());
            drop((entries, memory, input_memory));
            assert_eq!(operation.live_bytes(), 0);
            let normalized = normalize(format, &input, &[])?;
            verify_with_git(&normalized.bytes, format)?;

            let metadata = size_of::<InputEntry>() + size_of::<u64>();
            let operation =
                budget::Pool::new(metadata + INFLATE_BYTES + SCAN_WINDOW_BYTES - 1).admit()?;
            assert!(
                matches!(scan(&operation, &input, object_hash(format), DEFAULT_LIMITS),
                Err(Error::InvalidPack(message)) if message == "Git live-memory limit exceeded")
            );
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn normalizes_base_ofs_and_in_pack_ref_objects() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(ObjectFormat::Sha1)?;
        for (pack, expected_delta) in [(fixture.pack(true)?, "ofs"), (fixture.pack(false)?, "ref")]
        {
            let entries = entries(&pack, ObjectFormat::Sha1)?;
            assert!(entries.iter().any(|(entry, _)| match entry.header {
                gix_pack::data::entry::Header::OfsDelta { .. } => expected_delta == "ofs",
                gix_pack::data::entry::Header::RefDelta { .. } => expected_delta == "ref",
                _ => false,
            }));
            let normalized = normalize(ObjectFormat::Sha1, &pack, &[])?;
            assert_eq!(object_count(&normalized, ObjectFormat::Sha1)?, 3);
            assert_eq!(
                normalized.id.as_bytes(),
                &normalized.bytes[normalized.bytes.len() - 20..]
            );
            verify_with_git(&normalized.bytes, ObjectFormat::Sha1)?;
        }
        Ok(())
    }

    #[test]
    fn normalizes_a_ref_delta_before_its_in_pack_base() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(ObjectFormat::Sha1)?;
        let pack = ref_delta_first(&fixture.pack(false)?)?;
        let first = entries(&pack, ObjectFormat::Sha1)?
            .into_iter()
            .next()
            .ok_or("reordered pack is empty")?
            .0;
        assert!(matches!(
            first.header,
            gix_pack::data::entry::Header::RefDelta { .. }
        ));

        let normalized = normalize(ObjectFormat::Sha1, &pack, &[])?;
        assert_eq!(object_count(&normalized, ObjectFormat::Sha1)?, 3);
        verify_with_git(&normalized.bytes, ObjectFormat::Sha1)?;
        Ok(())
    }

    #[test]
    fn resolves_a_thin_ref_delta_from_a_supplied_base() -> Result<(), Box<dyn std::error::Error>> {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let fixture = Fixture::new(format)?;
            let full = fixture.pack(false)?;
            let (thin, base_id) = thin_ref_deltas(&full, format)?;
            let (_, data) = fixture
                .blobs
                .iter()
                .find(|(id, _)| id.as_bytes() == base_id.as_slice())
                .ok_or("generated REF_DELTA did not name a fixture blob")?;
            assert!(normalize(format, &thin, &[]).is_err());

            let normalized = normalize(
                format,
                &thin,
                &[ExternalBase {
                    id: ObjectId::from_bytes(format, base_id.as_slice())?,
                    kind: gix_object::Kind::Blob,
                    data,
                }],
            )?;
            assert_eq!(object_count(&normalized, format)?, 3);
            assert!(
                entries(&normalized.bytes, format)?
                    .iter()
                    .all(|(entry, _)| {
                        !matches!(entry.header, gix_pack::data::entry::Header::RefDelta { .. })
                    })
            );
            verify_with_git(&normalized.bytes, format)?;
            assert!(
                normalize_with(
                    format,
                    &thin,
                    &[ExternalBase {
                        id: ObjectId::from_bytes(format, base_id.as_slice())?,
                        kind: gix_object::Kind::Blob,
                        data,
                    }],
                    Limits {
                        objects: 2,
                        ..DEFAULT_LIMITS
                    },
                )
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn thin_pack_work_has_an_exact_operation_boundary() -> Result<(), Box<dyn std::error::Error>> {
        let format = ObjectFormat::Sha1;
        let fixture = Fixture::new(format)?;
        let (thin, base_id) = thin_ref_deltas(&fixture.pack(false)?, format)?;
        let (id, data) = fixture
            .blobs
            .iter()
            .find(|(id, _)| id.as_bytes() == base_id.as_slice())
            .ok_or("generated REF_DELTA did not name a fixture blob")?;
        let base = ExternalBase {
            id: *id,
            kind: gix_object::Kind::Blob,
            data,
        };
        let stored = entries(&thin, format)?;
        let inflated = stored
            .iter()
            .map(|(entry, _)| usize::try_from(entry.decompressed_size))
            .sum::<Result<usize, _>>()?;
        let input_crc = stored.iter().map(|(_, range)| range.len()).sum::<usize>();
        let resolved = fixture
            .blobs
            .iter()
            .map(|(_, data)| data.len())
            .sum::<usize>();
        let deltas = resolved - data.len();
        let hash_bytes = object_hash(format).len_in_bytes();
        let index_bytes = 8 + 256 * 4 + fixture.blobs.len() * (hash_bytes + 8) + 2 * hash_bytes;

        let probe = operation();
        let normalized = super::normalize(&probe, format, &thin, std::slice::from_ref(&base))?;
        let rewritten_crc = entries(&normalized.bytes, format)?
            .into_iter()
            .filter(|(entry, _)| entry.header.is_delta())
            .map(|(_, range)| range.len())
            .sum::<usize>();
        let expected = thin.len()
            + stored.len() // One bounded expansion probe per scanned entry.
            + inflated
            + input_crc
            + 2 * data.len()
            + compression_bounds(data.len())?.1
            + rewritten_crc
            + (2 * normalized.bytes.len() - hash_bytes)
            + inflated
            + data.len()
            + deltas
            + resolved
            + index_bytes
            - hash_bytes;
        assert_eq!(probe.work_bytes(), expected);
        drop(normalized);

        let exact = operation();
        exact.work(budget::WORK_BYTES - expected)?;
        drop(super::normalize(
            &exact,
            format,
            &thin,
            std::slice::from_ref(&base),
        )?);
        assert_eq!(exact.work_bytes(), budget::WORK_BYTES);

        let over = operation();
        over.work(budget::WORK_BYTES - expected + 1)?;
        assert!(matches!(
            super::normalize(&over, format, &thin, std::slice::from_ref(&base)),
            Err(Error::InvalidPack(message)) if message == "Git work limit exceeded"
        ));
        Ok(())
    }

    #[test]
    fn rejects_corrupt_or_ambiguous_pack_structure() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(ObjectFormat::Sha1)?;
        let pack = fixture.pack(true)?;

        let mut checksum = pack.clone();
        let last = checksum.len() - 1;
        checksum[last] ^= 1;
        assert!(normalize(ObjectFormat::Sha1, &checksum, &[]).is_err());
        assert!(normalize(ObjectFormat::Sha1, &pack[..pack.len() - 1], &[]).is_err());

        let mut invalid_offset = pack.clone();
        let entry = entries(&pack, ObjectFormat::Sha1)?
            .into_iter()
            .find(|(entry, _)| {
                matches!(entry.header, gix_pack::data::entry::Header::OfsDelta { .. })
            })
            .ok_or("Git did not produce an OFS_DELTA")?
            .0;
        invalid_offset[usize::try_from(entry.data_offset)? - 1] = 0;
        rehash(&mut invalid_offset)?;
        assert!(normalize(ObjectFormat::Sha1, &invalid_offset, &[]).is_err());

        let mut overlong = pack.clone();
        let base = entries(&pack, ObjectFormat::Sha1)?
            .into_iter()
            .find(|(entry, _)| entry.header.as_kind().is_some())
            .ok_or("Git did not produce a base object")?
            .0;
        let last_header_byte = usize::try_from(base.data_offset)? - 1;
        overlong[last_header_byte] |= 0x80;
        overlong.insert(last_header_byte + 1, 0);
        rehash(&mut overlong)?;
        assert!(normalize(ObjectFormat::Sha1, &overlong, &[]).is_err());

        let mut understated = pack.clone();
        let mut header = Vec::new();
        base.header.write_to(16_384, &mut header)?;
        assert_eq!(header.len(), base.header_size());
        let start = usize::try_from(base.pack_offset())?;
        understated[start..usize::try_from(base.data_offset)?].copy_from_slice(&header);
        rehash(&mut understated)?;
        assert!(normalize(ObjectFormat::Sha1, &understated, &[]).is_err());

        let duplicated = duplicate_base_entry(&fixture.pack(false)?)?;
        assert!(normalize(ObjectFormat::Sha1, &duplicated, &[]).is_err());

        let mut trailing = pack.clone();
        trailing.insert(trailing.len() - 20, 0);
        rehash(&mut trailing)?;
        assert!(normalize(ObjectFormat::Sha1, &trailing, &[]).is_err());

        let mut wrong_count = pack.clone();
        wrong_count[8..12].copy_from_slice(&2_u32.to_be_bytes());
        rehash(&mut wrong_count)?;
        assert!(normalize(ObjectFormat::Sha1, &wrong_count, &[]).is_err());
        Ok(())
    }

    #[test]
    fn rejects_each_resource_limit_before_returning_a_pack()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(ObjectFormat::Sha1)?;
        let pack = fixture.pack(false)?;
        let operation = budget::Pool::new(0).admit()?;
        assert!(matches!(
            super::normalize(&operation, ObjectFormat::Sha1, &pack, &[]),
            Err(Error::InvalidPack(message)) if message == "Git live-memory limit exceeded"
        ));
        let limits = Limits {
            input_bytes: pack.len() - 1,
            ..DEFAULT_LIMITS
        };
        assert!(normalize_with(ObjectFormat::Sha1, &pack, &[], limits).is_err());
        assert!(
            normalize_with(
                ObjectFormat::Sha1,
                &pack,
                &[],
                Limits {
                    input_bytes: pack.len(),
                    ..DEFAULT_LIMITS
                },
            )
            .is_ok()
        );

        let mut count = pack.clone();
        count[8..12].copy_from_slice(&(DEFAULT_LIMITS.objects + 1).to_be_bytes());
        assert!(normalize(ObjectFormat::Sha1, &count, &[]).is_err());

        for limits in [
            Limits {
                object_bytes: 8,
                ..DEFAULT_LIMITS
            },
            Limits {
                output_bytes: 8,
                ..DEFAULT_LIMITS
            },
            Limits {
                index_bytes: 8,
                ..DEFAULT_LIMITS
            },
        ] {
            assert!(normalize_with(ObjectFormat::Sha1, &pack, &[], limits).is_err());
        }

        for limits in [
            Limits {
                object_bytes: fixture.blobs[0].1.len(),
                objects: u32::try_from(fixture.blobs.len())?,
                ..DEFAULT_LIMITS
            },
            Limits {
                output_bytes: normalize(ObjectFormat::Sha1, &pack, &[])?.bytes.len(),
                ..DEFAULT_LIMITS
            },
        ] {
            assert!(normalize_with(ObjectFormat::Sha1, &pack, &[], limits).is_ok());
        }
        let index_bytes = 8 + 256 * 4 + fixture.blobs.len() * (20 + 8) + 40;
        assert!(
            normalize_with(
                ObjectFormat::Sha1,
                &pack,
                &[],
                Limits {
                    index_bytes,
                    ..DEFAULT_LIMITS
                },
            )
            .is_ok()
        );
        assert!(
            normalize_with(
                ObjectFormat::Sha1,
                &pack,
                &[],
                Limits {
                    index_bytes: index_bytes - 1,
                    ..DEFAULT_LIMITS
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn accepts_empty_sha1_and_sha256_packs() -> Result<(), Box<dyn std::error::Error>> {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let hash = object_hash(format);
            let mut pack = gix_pack::data::header::encode(Version::V2, 0).to_vec();
            let mut hasher = gix_hash::hasher(hash);
            hasher.update(&pack);
            pack.extend_from_slice(hasher.try_finalize()?.as_slice());
            let normalized = normalize(format, &pack, &[])?;
            assert_eq!(object_count(&normalized, format)?, 0);
            assert_eq!(normalized.bytes, pack);
            verify_with_git(&normalized.bytes, format)?;
        }
        Ok(())
    }

    #[test]
    fn normalizes_sha256_objects() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        git(
            dir.path(),
            ["init", "--bare", "--quiet", "--object-format=sha256"],
            &[],
        )?;
        let id = git(dir.path(), ["hash-object", "-w", "--stdin"], b"sha256\n")?;
        let pack = git(dir.path(), ["pack-objects", "--stdout"], &id)?;
        let normalized = normalize(ObjectFormat::Sha256, &pack, &[])?;
        assert_eq!(object_count(&normalized, ObjectFormat::Sha256)?, 1);
        verify_with_git(&normalized.bytes, ObjectFormat::Sha256)?;
        Ok(())
    }

    #[test]
    fn rejects_bad_external_base_evidence() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(ObjectFormat::Sha1)?;
        let full = fixture.pack(false)?;
        let (id, data) = &fixture.blobs[0];
        let bad = ExternalBase {
            id: *id,
            kind: gix_object::Kind::Blob,
            data: &data[..data.len() - 1],
        };
        assert!(normalize(ObjectFormat::Sha1, &full, &[bad]).is_err());

        let base_id = entries(&full, ObjectFormat::Sha1)?
            .into_iter()
            .find_map(|(entry, _)| match entry.header {
                gix_pack::data::entry::Header::RefDelta { base_id } => Some(base_id),
                _ => None,
            })
            .ok_or("Git did not produce a REF_DELTA")?;
        let (_, data) = fixture
            .blobs
            .iter()
            .find(|(id, _)| id.as_bytes() == base_id.as_slice())
            .ok_or("generated REF_DELTA did not name a fixture blob")?;
        let present = ExternalBase {
            id: ObjectId::from_bytes(ObjectFormat::Sha1, base_id.as_slice())?,
            kind: gix_object::Kind::Blob,
            data,
        };
        assert!(matches!(
            normalize(ObjectFormat::Sha1, &full, &[present]),
            Err(Error::InvalidPack(message)) if message == "pack contains duplicate object IDs"
        ));

        let data = b"unused external base";
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, data)?;
        let unused = ExternalBase {
            id: ObjectId::from_bytes(ObjectFormat::Sha1, id.as_slice())?,
            kind: gix_object::Kind::Blob,
            data,
        };
        assert!(matches!(
            normalize(ObjectFormat::Sha1, &full, &[unused]),
            Err(Error::InvalidPack(message)) if message == "external base set is not exact"
        ));
        Ok(())
    }

    #[test]
    fn enforces_the_git_generator_delta_depth_limit() {
        let mut parents = Vec::with_capacity(MAX_DELTA_DEPTH + 2);
        parents.push(None);
        for position in 1..=MAX_DELTA_DEPTH {
            parents.push(Some(position - 1));
        }
        assert!(validate_parent_depth(&parents).is_ok());
        parents.push(Some(parents.len() - 1));
        assert!(validate_parent_depth(&parents).is_err());
        assert!(validate_parent_depth(&[Some(1), Some(0)]).is_err());
    }

    type ParsedEntries = Vec<(gix_pack::data::Entry, Range<usize>)>;

    fn object_count(
        normalized: &Normalized,
        format: ObjectFormat,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        Ok(gix_pack::index::File::from_data(
            normalized.index.as_slice(),
            PathBuf::new(),
            object_hash(format),
        )?
        .num_objects())
    }

    fn entries(
        bytes: &[u8],
        format: ObjectFormat,
    ) -> Result<ParsedEntries, Box<dyn std::error::Error>> {
        let file = gix_pack::data::File::from_data(bytes, PathBuf::new(), object_hash(format))?;
        let mut inflate = gix_zlib::Inflate::default();
        let mut data = Vec::new();
        let mut offset = 12_u64;
        let mut result = Vec::new();
        for _ in 0..file.num_objects() {
            let entry = file.entry(offset)?;
            data.resize(usize::try_from(entry.decompressed_size)?, 0);
            let compressed = file.decompress_entry(&entry, &mut inflate, &mut data)?;
            let end = entry.data_offset + u64::try_from(compressed)?;
            result.push((entry, usize::try_from(offset)?..usize::try_from(end)?));
            offset = end;
        }
        Ok(result)
    }

    fn thin_ref_deltas(
        full: &[u8],
        format: ObjectFormat,
    ) -> Result<(Vec<u8>, gix_hash::ObjectId), Box<dyn std::error::Error>> {
        let entries = entries(full, format)?;
        let base_id = entries
            .iter()
            .find_map(|(entry, _)| {
                if let gix_pack::data::entry::Header::RefDelta { base_id } = entry.header {
                    Some(base_id)
                } else {
                    None
                }
            })
            .ok_or("Git did not produce a REF_DELTA")?;
        let deltas = entries
            .into_iter()
            .filter(|(entry, _)| {
                matches!(
                    entry.header,
                    gix_pack::data::entry::Header::RefDelta { base_id: id } if id == base_id
                )
            })
            .collect::<Vec<_>>();
        if deltas.len() < 2 {
            return Err("Git did not produce two REF_DELTA entries for one base".into());
        }
        let mut thin =
            gix_pack::data::header::encode(Version::V2, u32::try_from(deltas.len())?).to_vec();
        for (_, range) in deltas {
            thin.extend_from_slice(&full[range]);
        }
        append_hash(&mut thin, format)?;
        Ok((thin, base_id))
    }

    fn duplicate_base_entry(full: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let (_, range) = entries(full, ObjectFormat::Sha1)?
            .into_iter()
            .find(|(entry, _)| entry.header.as_kind().is_some())
            .ok_or("Git did not produce a base object")?;
        let mut duplicated = gix_pack::data::header::encode(Version::V2, 2).to_vec();
        duplicated.extend_from_slice(&full[range.clone()]);
        duplicated.extend_from_slice(&full[range]);
        append_hash(&mut duplicated, ObjectFormat::Sha1)?;
        Ok(duplicated)
    }

    fn ref_delta_first(full: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut entries = entries(full, ObjectFormat::Sha1)?;
        let position = entries
            .iter()
            .position(|(entry, _)| {
                matches!(entry.header, gix_pack::data::entry::Header::RefDelta { .. })
            })
            .ok_or("Git did not produce a REF_DELTA")?;
        let (_, first) = entries.remove(position);
        let mut reordered =
            gix_pack::data::header::encode(Version::V2, u32::try_from(entries.len() + 1)?).to_vec();
        reordered.extend_from_slice(&full[first]);
        for (_, range) in entries {
            reordered.extend_from_slice(&full[range]);
        }
        append_hash(&mut reordered, ObjectFormat::Sha1)?;
        Ok(reordered)
    }

    fn rehash(pack: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        pack.truncate(pack.len() - 20);
        append_hash(pack, ObjectFormat::Sha1)
    }

    fn append_hash(
        pack: &mut Vec<u8>,
        format: ObjectFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut hasher = gix_hash::hasher(object_hash(format));
        hasher.update(pack);
        let id = hasher.try_finalize()?;
        pack.extend_from_slice(id.as_slice());
        Ok(())
    }

    fn verify_with_git(
        pack: &[u8],
        format: ObjectFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let mut init = vec!["init", "--bare", "--quiet"];
        if format == ObjectFormat::Sha256 {
            init.push("--object-format=sha256");
        }
        git(dir.path(), init, &[])?;
        git(dir.path(), ["index-pack", "--strict", "--stdin"], pack)?;
        git(dir.path(), ["fsck", "--strict", "--no-progress"], &[])?;
        Ok(())
    }

    fn git<I, S>(
        dir: &std::path::Path,
        arguments: I,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut child = Command::new("git")
            .current_dir(dir)
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("Git stdin is unavailable")?
            .write_all(input)?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(format!("Git failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        Ok(output.stdout)
    }
}
