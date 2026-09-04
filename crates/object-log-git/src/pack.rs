use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    path::PathBuf,
    sync::atomic::AtomicBool,
};

use gix_pack::data::{Version, input::EntriesToBytesIter};

use crate::{Error, ObjectFormat, ObjectId};

pub(super) const MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;

const DEFAULT_LIMITS: Limits = Limits {
    input_bytes: MAX_INPUT_BYTES,
    output_bytes: 64 * 1024 * 1024,
    object_bytes: 16 * 1024 * 1024,
    work_bytes: 256 * 1024 * 1024,
    objects: 65_535,
    index_bytes: 4 * 1024 * 1024,
};
// Git's pack generator documents 4095 as its maximum delta depth.
const MAX_DELTA_DEPTH: u16 = 4095;

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    input_bytes: usize,
    output_bytes: usize,
    object_bytes: usize,
    work_bytes: usize,
    objects: u32,
    index_bytes: usize,
}

pub(crate) struct ExternalBase<'a> {
    pub(crate) id: ObjectId,
    pub(crate) kind: gix_object::Kind,
    pub(crate) data: &'a [u8],
}

#[cfg_attr(test, derive(Clone))]
pub(crate) struct Normalized {
    pub(crate) bytes: Vec<u8>,
    pub(crate) index: Vec<u8>,
    pub(crate) id: ObjectId,
}

pub(crate) fn normalize(
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
) -> Result<Normalized, Error> {
    normalize_with(format, input, external_bases, DEFAULT_LIMITS)
}

fn normalize_with(
    format: ObjectFormat,
    input: &[u8],
    external_bases: &[ExternalBase<'_>],
    limits: Limits,
) -> Result<Normalized, Error> {
    if input.len() > limits.input_bytes {
        return invalid("input exceeds byte limit");
    }
    let hash = object_hash(format);
    let (entries, input_work) = scan(input, hash, limits)?;

    let bases = ExternalBases::new(hash, external_bases, limits, input_work)?;
    let (entries, output_bytes) = resolve_external(entries, &bases, limits, hash.len_in_bytes())?;
    let object_count = entries.len();
    let entries = entries.into_iter().map(Ok);
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
    let (id, index) = index(&bytes, format, hash, indexed_entries, limits)?;
    Ok(Normalized { bytes, index, id })
}

struct ExternalBases<'a> {
    values: BTreeMap<gix_hash::ObjectId, (gix_object::Kind, &'a [u8])>,
}

impl<'a> ExternalBases<'a> {
    fn new(
        hash: gix_hash::Kind,
        bases: &[ExternalBase<'a>],
        limits: Limits,
        mut work: usize,
    ) -> Result<Self, Error> {
        let mut values = BTreeMap::new();
        for base in bases {
            if base.data.len() > limits.object_bytes {
                return invalid("external base exceeds object byte limit");
            }
            let id = gix_object::compute_hash(hash, base.kind, base.data).map_err(pack_error)?;
            if id.as_slice() != base.id.as_bytes() {
                return invalid("external base ID does not match its data");
            }
            if values.insert(id, (base.kind, base.data)).is_some() {
                return invalid("external base is duplicated");
            }
            work = work
                .checked_add(base.data.len())
                .ok_or_else(|| Error::InvalidPack("decoded work overflowed".into()))?;
            if work > limits.work_bytes {
                return invalid("decoded work exceeds limit");
            }
            if values.len() > limits.objects as usize {
                return invalid("external base count exceeds limit");
            }
        }
        Ok(Self { values })
    }
}

fn scan(
    input: &[u8],
    hash: gix_hash::Kind,
    limits: Limits,
) -> Result<(Vec<gix_pack::data::input::Entry>, usize), Error> {
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
    pack.verify_checksum(
        &mut gix_features::progress::Discard,
        &AtomicBool::new(false),
    )
    .map_err(pack_error)?;

    let mut offset = 12_u64;
    let mut offsets = BTreeSet::new();
    let mut inflated = Vec::new();
    let mut inflate = gix_zlib::Inflate::default();
    let mut work = 0_usize;
    let mut entries = Vec::with_capacity(count as usize);
    for position in 0..count {
        let entry = pack.entry(offset).map_err(pack_error)?;
        if entry.header_size() != entry.header.size(entry.decompressed_size) {
            return invalid("pack entry header is not canonical");
        }
        offsets.insert(offset);
        if let gix_pack::data::entry::Header::OfsDelta { base_distance } = entry.header {
            let base = entry
                .checked_base_pack_offset(base_distance)
                .ok_or_else(|| Error::InvalidPack("invalid OFS_DELTA base offset".into()))?;
            if !offsets.contains(&base) {
                return invalid("OFS_DELTA base is not an earlier entry");
            }
        }

        let inflated_size = usize::try_from(entry.decompressed_size)
            .map_err(|_| Error::InvalidPack("entry size does not fit in memory".into()))?;
        if inflated_size > limits.object_bytes {
            return invalid("entry exceeds object byte limit");
        }
        inflated.resize(inflated_size, 0);
        let compressed = pack
            .decompress_entry(&entry, &mut inflate, &mut inflated)
            .map_err(pack_error)?;
        let result_size = if entry.header.is_delta() {
            delta_result_size(&inflated)?
        } else {
            inflated_size
        };
        if result_size > limits.object_bytes {
            return invalid("decoded object exceeds byte limit");
        }
        let inflated_work = inflated_size
            .checked_mul(1 + usize::from(entry.header.is_delta()))
            .ok_or_else(|| Error::InvalidPack("decoded work overflowed".into()))?;
        work = work
            .checked_add(result_size)
            .and_then(|value| value.checked_add(inflated_work))
            .ok_or_else(|| Error::InvalidPack("decoded work overflowed".into()))?;
        if work > limits.work_bytes {
            return invalid("decoded work exceeds limit");
        }
        let end = entry
            .data_offset
            .checked_add(u64::try_from(compressed).map_err(|_| {
                Error::InvalidPack("compressed entry length does not fit in a pack".into())
            })?)
            .ok_or_else(|| Error::InvalidPack("pack entry offset overflowed".into()))?;
        let end_in_memory = usize::try_from(end)
            .map_err(|_| Error::InvalidPack("pack entry end does not fit in memory".into()))?;
        if end_in_memory > pack.pack_end() {
            return invalid("pack entry extends beyond the trailer");
        }
        let start = usize::try_from(entry.data_offset)
            .map_err(|_| Error::InvalidPack("pack entry offset does not fit in memory".into()))?;
        let pack_offset = usize::try_from(offset)
            .map_err(|_| Error::InvalidPack("pack entry offset does not fit in memory".into()))?;
        entries.push(gix_pack::data::input::Entry {
            header: entry.header,
            header_size: u16::try_from(entry.header_size())
                .map_err(|_| Error::InvalidPack("pack entry header is too large".into()))?,
            pack_offset: offset,
            compressed: Some(input[start..end_in_memory].to_vec()),
            compressed_size: u64::try_from(compressed)
                .map_err(|_| Error::InvalidPack("compressed entry is too large".into()))?,
            crc32: Some(pack.entry_crc32(offset, end_in_memory - pack_offset)),
            decompressed_size: entry.decompressed_size,
            trailer: (position + 1 == count).then(|| pack.checksum()),
        });
        offset = end;
    }
    if usize::try_from(offset).ok() != Some(pack.pack_end()) {
        return invalid("pack object count does not match its entries");
    }
    Ok((entries, work))
}

fn delta_result_size(delta: &[u8]) -> Result<usize, Error> {
    let (_, consumed) = delta_size(delta)?;
    let (size, _) = delta_size(&delta[consumed..])?;
    usize::try_from(size)
        .map_err(|_| Error::InvalidPack("delta result does not fit in memory".into()))
}

fn delta_size(bytes: &[u8]) -> Result<(u64, usize), Error> {
    let mut size = 0_u64;
    for (index, byte) in bytes.iter().copied().enumerate() {
        let shift = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_mul(7))
            .filter(|value| *value < u64::BITS)
            .ok_or_else(|| Error::InvalidPack("delta size overflowed".into()))?;
        size |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((size, index + 1));
        }
    }
    invalid("delta size is truncated")
}

fn resolve_external(
    entries: Vec<gix_pack::data::input::Entry>,
    bases: &ExternalBases<'_>,
    limits: Limits,
    trailer_bytes: usize,
) -> Result<(Vec<gix_pack::data::input::Entry>, usize), Error> {
    if 12_usize
        .checked_add(trailer_bytes)
        .is_none_or(|bytes| bytes > limits.output_bytes)
    {
        return invalid("normalized pack exceeds byte limit");
    }
    let mut output = Vec::with_capacity(entries.len());
    let mut translated = BTreeMap::new();
    let mut inserted = BTreeMap::new();
    let mut next_offset = 12_u64;
    for mut entry in entries {
        let original_offset = entry.pack_offset;
        match entry.header {
            gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                let original_base = original_offset
                    .checked_sub(base_distance)
                    .ok_or_else(|| Error::InvalidPack("invalid OFS_DELTA base offset".into()))?;
                let new_base = translated
                    .get(&original_base)
                    .copied()
                    .ok_or_else(|| Error::InvalidPack("OFS_DELTA base is missing".into()))?;
                entry.header = gix_pack::data::entry::Header::OfsDelta {
                    base_distance: next_offset - new_base,
                };
            }
            gix_pack::data::entry::Header::RefDelta { base_id } => {
                if let Some(&(kind, data)) = bases.values.get(&base_id) {
                    let base_offset = if let Some(offset) = inserted.get(&base_id).copied() {
                        offset
                    } else {
                        let base = gix_pack::data::input::Entry::from_data_obj(
                            &gix_object::Data {
                                kind,
                                object_hash: base_id.kind(),
                                data,
                            },
                            next_offset,
                            gix_zlib::Compression::default(),
                        )
                        .map_err(pack_error)?;
                        let offset = next_offset;
                        next_offset =
                            push_entry(&mut output, base, next_offset, limits, trailer_bytes)?;
                        inserted.insert(base_id, offset);
                        offset
                    };
                    entry.header = gix_pack::data::entry::Header::OfsDelta {
                        base_distance: next_offset - base_offset,
                    };
                }
            }
            _ => {}
        }
        translated.insert(original_offset, next_offset);
        entry.pack_offset = next_offset;
        entry.header_size = u16::try_from(entry.header.size(entry.decompressed_size))
            .map_err(|_| Error::InvalidPack("pack entry header is too large".into()))?;
        entry.crc32 = Some(entry.compute_crc32());
        next_offset = push_entry(&mut output, entry, next_offset, limits, trailer_bytes)?;
    }
    if inserted.len() != bases.values.len() {
        return invalid("external base set is not exact");
    }
    let output_bytes = usize::try_from(next_offset)
        .ok()
        .and_then(|bytes| bytes.checked_add(trailer_bytes))
        .ok_or_else(|| Error::InvalidPack("normalized pack size overflowed".into()))?;
    Ok((output, output_bytes))
}

fn push_entry(
    output: &mut Vec<gix_pack::data::input::Entry>,
    entry: gix_pack::data::input::Entry,
    offset: u64,
    limits: Limits,
    trailer_bytes: usize,
) -> Result<u64, Error> {
    if output.len() >= limits.objects as usize {
        return invalid("object count exceeds limit after thin-pack resolution");
    }
    let next = offset
        .checked_add(entry.bytes_in_pack())
        .ok_or_else(|| Error::InvalidPack("normalized pack offset overflowed".into()))?;
    if usize::try_from(next)
        .ok()
        .and_then(|bytes| bytes.checked_add(trailer_bytes))
        .is_none_or(|bytes| bytes > limits.output_bytes)
    {
        return invalid("normalized pack exceeds byte limit");
    }
    output.push(entry);
    Ok(next)
}

fn index(
    pack: &[u8],
    format: ObjectFormat,
    hash: gix_hash::Kind,
    entries: Vec<gix_pack::data::input::Entry>,
    limits: Limits,
) -> Result<(ObjectId, Vec<u8>), Error> {
    let object_count = entries.len();
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
        return invalid("pack index exceeds byte limit");
    }
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
    .map_err(pack_error)?;
    let file = gix_pack::index::File::from_data(index.as_slice(), PathBuf::new(), hash)
        .map_err(pack_error)?;
    let mut previous = None;
    for entry in file.iter() {
        if previous == Some(entry.oid) {
            return invalid("pack contains duplicate object IDs");
        }
        previous = Some(entry.oid);
    }
    validate_delta_depth(&relationships, &file)?;
    let id = ObjectId::from_bytes(format, outcome.data_hash.as_slice())?;
    Ok((id, index))
}

fn validate_delta_depth(
    entries: &[(u64, gix_pack::data::entry::Header)],
    index: &gix_pack::index::File<&[u8]>,
) -> Result<(), Error> {
    let parents = entries
        .iter()
        .map(|(offset, header)| {
            let parent = match header {
                gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                    Some(offset.checked_sub(*base_distance).ok_or_else(|| {
                        Error::InvalidPack("invalid OFS_DELTA base offset".into())
                    })?)
                }
                gix_pack::data::entry::Header::RefDelta { base_id } => Some(
                    index
                        .lookup(base_id)
                        .map(|position| index.pack_offset_at_index(position))
                        .ok_or_else(|| Error::InvalidPack("REF_DELTA base is missing".into()))?,
                ),
                _ => None,
            };
            parent
                .map(|offset| {
                    entries
                        .binary_search_by_key(&offset, |(offset, _)| *offset)
                        .map_err(|_| Error::InvalidPack("delta base is missing".into()))
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, Error>>()?;
    validate_parent_depth(&parents)
}

fn validate_parent_depth(parents: &[Option<usize>]) -> Result<(), Error> {
    let mut depths = vec![None::<u16>; parents.len()];
    let mut visiting = vec![false; parents.len()];
    let mut path = Vec::new();
    for start in 0..parents.len() {
        path.clear();
        let mut current = start;
        let known_depth = loop {
            if let Some(depth) = depths[current] {
                break depth;
            }
            if visiting[current] {
                return invalid("delta graph contains a cycle");
            }
            visiting[current] = true;
            path.push(current);
            match parents[current] {
                Some(parent) => current = parent,
                None => break 0_u16,
            }
        };
        let mut depth = known_depth;
        while let Some(node) = path.pop() {
            depth = if parents[node].is_some() {
                depth + 1
            } else {
                0
            };
            if depth > MAX_DELTA_DEPTH {
                return invalid("delta depth exceeds limit");
            }
            depths[node] = Some(depth);
            visiting[node] = false;
        }
    }
    Ok(())
}

fn entry_bytes<'a>(range: gix_pack::data::EntryRange, pack: &'a &[u8]) -> Option<&'a [u8]> {
    let start = usize::try_from(range.start).ok()?;
    let end = usize::try_from(range.end).ok()?;
    pack.get(start..end)
}

pub(crate) const fn object_hash(format: ObjectFormat) -> gix_hash::Kind {
    match format {
        ObjectFormat::Sha1 => gix_hash::Kind::Sha1,
        ObjectFormat::Sha256 => gix_hash::Kind::Sha256,
    }
}

fn pack_error(error: impl std::fmt::Display) -> Error {
    Error::InvalidPack(error.to_string())
}

fn invalid<T>(message: &'static str) -> Result<T, Error> {
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
                work_bytes: 8,
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

        let (_, exact_work) = scan(&pack, gix_hash::Kind::Sha1, DEFAULT_LIMITS)?;
        assert!(
            normalize_with(
                ObjectFormat::Sha1,
                &pack,
                &[],
                Limits {
                    work_bytes: exact_work - 1,
                    ..DEFAULT_LIMITS
                },
            )
            .is_err()
        );
        for limits in [
            Limits {
                object_bytes: fixture.blobs[0].1.len(),
                objects: u32::try_from(fixture.blobs.len())?,
                work_bytes: exact_work,
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
        let mut parents = Vec::with_capacity(usize::from(MAX_DELTA_DEPTH) + 2);
        parents.push(None);
        for position in 1..=usize::from(MAX_DELTA_DEPTH) {
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
