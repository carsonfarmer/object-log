//! Backpressured pack output; the final digest is emitted only after every CRC.

use super::{
    Bytes, COMPRESS_BYTES, EntryHeader, Error, Location, MAX_DELTA_DEPTH, MAX_OBJECTS, ObjectId,
    Operation, Reader, Reservation, hold, invalid, io, object_hash, output_error, pack_error,
    size_of,
};
use futures::{Sink, SinkExt};
use gix_zlib::stream::deflate::{Compress, FlushCompress};

const FRAME: usize = 64 * 1024;

struct Output<'a, S> {
    sink: &'a mut S,
    operation: &'a Operation,
    hash: gix_hash::Hasher,
    bytes: usize,
    limit: usize,
}

impl<S: Sink<Bytes, Error = io::Error> + Unpin> Output<'_, S> {
    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > self.limit - self.bytes {
            return invalid("fetch pack exceeds byte limit");
        }
        for chunk in bytes.chunks(FRAME) {
            self.operation.work(chunk.len() * 2)?;
            self.hash.update(chunk);
            let memory = self.operation.reserve(chunk.len())?;
            self.sink
                .send(hold(Bytes::copy_from_slice(chunk), memory))
                .await
                .map_err(output_error)?;
            self.bytes += chunk.len();
        }
        Ok(())
    }
    async fn finish(self) -> Result<(), Error> {
        let digest = self.hash.try_finalize().map_err(pack_error)?;
        self.operation.work(digest.as_slice().len())?;
        let memory = self.operation.reserve(digest.as_slice().len())?;
        self.sink
            .send(hold(Bytes::copy_from_slice(digest.as_slice()), memory))
            .await
            .map_err(output_error)?;
        Ok(())
    }

    async fn compress(
        &mut self,
        object: &mut crate::pack::ingest::read_only_delta::ReadOnlyDelta<'_, '_>,
    ) -> Result<(), Error> {
        let header = match object.kind() {
            gix_object::Kind::Tree => EntryHeader::Tree,
            gix_object::Kind::Blob => EntryHeader::Blob,
            gix_object::Kind::Commit => EntryHeader::Commit,
            gix_object::Kind::Tag => EntryHeader::Tag,
        };
        let mut bytes = [0; 64];
        let length = header
            .write_to(object.len() as u64, &mut bytes.as_mut_slice())
            .map_err(output_error)?;
        self.write(&bytes[..length]).await?;
        let _memory = self
            .operation
            .reserve(COMPRESS_BYTES + FRAME + crate::pack::SCAN_WINDOW_BYTES)?;
        let mut compressor = Compress::new(gix_zlib::Compression::DEFAULT);
        let mut window = vec![0; FRAME];
        let mut decoded = vec![0; crate::pack::SCAN_WINDOW_BYTES];
        loop {
            let length = object.next_into(&mut decoded).await?;
            let flush = if length == 0 {
                FlushCompress::Finish
            } else {
                FlushCompress::None
            };
            let mut position = 0;
            loop {
                let before_in = compressor.total_in();
                let before_out = compressor.total_out();
                let status = compressor
                    .compress(&decoded[position..length], &mut window, flush)
                    .map_err(pack_error)?;
                let consumed =
                    usize::try_from(compressor.total_in() - before_in).map_err(pack_error)?;
                let written =
                    usize::try_from(compressor.total_out() - before_out).map_err(pack_error)?;
                position += consumed;
                self.operation.work(consumed)?;
                self.write(&window[..written]).await?;
                if status == gix_zlib::Status::StreamEnd || (length != 0 && position == length) {
                    break;
                }
                if consumed == 0 && written == 0 {
                    return invalid("compression made no progress");
                }
            }
            if length == 0 {
                break;
            }
        }
        Ok(())
    }
}

impl<'a> Reader<'a> {
    pub(super) async fn verify_decoded(
        &self,
        location: Location,
    ) -> Result<(gix_object::Kind, usize), Error> {
        let plan = self.decoded_plan(location).await?;
        let mut object =
            crate::pack::ingest::read_only_delta::ReadOnlyDelta::new(&plan.input, &plan.chain)
                .await?;
        let _memory = self
            .catalog
            .operation
            .reserve(crate::pack::SCAN_WINDOW_BYTES)?;
        let mut window = vec![0; crate::pack::SCAN_WINDOW_BYTES];
        while object.next_into(&mut window).await? != 0 {}
        Ok((object.kind(), object.len()))
    }

    pub(super) async fn decoded_plan(&self, location: Location) -> Result<DecodedPlan<'a>, Error> {
        let pack = self.pack(location.pack);
        let input = crate::pack::ingest::Input::read_only(
            &self.catalog.operation,
            self.log,
            self.view,
            &pack.node,
            u64::from(pack.bytes),
            pack.chunk_bytes,
        )?
        .with_encoded_cache(self.encoded_cache()?)?;
        let count = pack.index.num_objects() as usize;
        let capacity = count.min(MAX_DELTA_DEPTH + 1);
        let memory = self
            .catalog
            .operation
            .reserve(count + capacity * size_of::<crate::pack::ingest::IndexedEntry>())?;
        let mut visited = vec![false; count];
        let mut chain = Vec::with_capacity(capacity);
        let mut current = location.index;
        loop {
            if visited[current as usize] {
                return invalid("selected delta graph cycles");
            }
            visited[current as usize] = true;
            let id = ObjectId::from_bytes(pack.id.format(), pack.oid(current))?;
            let range = pack.entry_range(current);
            let entry = input
                .indexed_entry(
                    u64::from(range.start),
                    u64::from(range.end),
                    id,
                    pack.crc(current),
                )
                .await?;
            self.catalog
                .operation
                .work(count.max(1).ilog2() as usize + 1)?;
            let base = pack.base(&entry.header)?;
            chain.push(entry);
            let Some(base) = base else {
                break;
            };
            if chain.len() > MAX_DELTA_DEPTH {
                return invalid("selected delta graph is too deep");
            }
            current = base;
        }
        if chain.last().and_then(|entry| entry.header.header.as_kind())
            != Some(gix_object::Kind::Blob)
            && chain
                .iter()
                .any(|entry| entry.result_size > crate::pack::MAX_OBJECT_BYTES)
        {
            return invalid("structural delta exceeds object byte limit");
        }
        Ok(DecodedPlan {
            input,
            chain,
            _memory: memory,
        })
    }

    /// A failed write may follow partial output. Callers must abort the response
    /// without its pack digest or protocol flush; they must never retry it.
    pub(crate) async fn write_fetch<S>(
        &mut self,
        ids: &[ObjectId],
        sink: &mut S,
    ) -> Result<(), Error>
    where
        S: Sink<Bytes, Error = io::Error> + Unpin,
    {
        let format = self.catalog.format;
        if ids.len() > MAX_OBJECTS as usize || ids.iter().any(|id| id.format() != format) {
            return invalid("fetch selection is invalid");
        }
        let _selected_memory = self.catalog.operation.reserve(
            ids.len() * (size_of::<ObjectId>() + size_of::<(ObjectId, ObjectId, u32)>()),
        )?;
        let mut selected = ids.to_vec();
        selected.sort_unstable();
        selected.dedup();
        let mut entries = Vec::with_capacity(selected.len());
        for id in &selected {
            let location = self
                .location(*id)
                .await?
                .ok_or_else(|| Error::InvalidPack("fetch object is missing".into()))?;
            let pack = self.pack(location.pack);
            entries.push((pack.id, *id, pack.offset(location.index)));
        }
        entries.sort_unstable_by_key(|(pack, id, offset)| (*pack, *offset, *id));
        let hash = object_hash(format);
        let hash_len = hash.len_in_bytes();
        let count = u32::try_from(entries.len()).map_err(pack_error)?;
        let mut output = Output {
            sink,
            operation: &self.catalog.operation,
            hash: gix_hash::hasher(hash),
            bytes: 0,
            limit: crate::pack::MAX_STORED_PACK_BYTES - hash_len,
        };
        output
            .write(&gix_pack::data::header::encode(
                gix_pack::data::Version::V2,
                count,
            ))
            .await?;
        for (_, id, _) in entries {
            let location = self
                .location(id)
                .await?
                .ok_or_else(|| Error::InvalidPack("fetch object is missing".into()))?;
            let range = self.pack(location.pack).entry_range(location.index);
            self.catalog
                .operation
                .work((range.end - range.start) as usize)?;
            let entry = self.entry_header(location).await?;
            let pack = self.pack(location.pack);
            let base = pack
                .base(&entry)?
                .map(|index| ObjectId::from_bytes(format, pack.oid(index)))
                .transpose()?;
            if base.is_some_and(|base| selected.binary_search(&base).is_err()) {
                let plan = self.decoded_plan(location).await?;
                let mut object = crate::pack::ingest::read_only_delta::ReadOnlyDelta::new(
                    &plan.input,
                    &plan.chain,
                )
                .await?;
                output.compress(&mut object).await?;
                continue;
            }
            let header = base.map_or(entry.header, |base| EntryHeader::RefDelta {
                base_id: gix_hash::ObjectId::from_bytes_or_panic(base.as_bytes()),
            });
            let mut bytes = [0; 64];
            let length = header
                .write_to(entry.decompressed_size, &mut bytes.as_mut_slice())
                .map_err(output_error)?;
            output.write(&bytes[..length]).await?;
            let range = pack.entry_range(location.index);
            let width = pack.chunk_bytes;
            let expected_crc = pack.crc(location.index);
            let mut position = range.start as usize;
            let mut skip = entry.header_size();
            let mut crc = 0;
            while position < range.end as usize {
                let chunk = self.chunk(location.pack, position / width).await?;
                let start = position % width;
                let count = (range.end as usize - position).min(chunk.len() - start);
                let bytes = &chunk[start..start + count];
                crc = gix_features::hash::crc32_update(crc, bytes);
                let header_bytes = skip.min(bytes.len());
                skip -= header_bytes;
                output.write(&bytes[header_bytes..]).await?;
                position += count;
            }
            if crc != expected_crc {
                return invalid("pack entry CRC does not match");
            }
        }
        output.finish().await
    }
}

pub(super) struct DecodedPlan<'a> {
    pub(super) input: crate::pack::ingest::Input<'a>,
    pub(super) chain: Vec<crate::pack::ingest::IndexedEntry>,
    _memory: Reservation,
}
