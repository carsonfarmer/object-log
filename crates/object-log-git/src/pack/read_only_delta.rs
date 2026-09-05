//! Read-only delta replay. Backward copies spend work instead of staging scratch.

use super::{Cursor, IndexedEntry, Input};
use crate::{
    Error,
    pack::{
        INFLATE_BYTES, MAX_DELTA_DEPTH, SCAN_WINDOW_BYTES, budget::Reservation, invalid,
        object_hash, pack_error,
    },
};
use gix_pack::data::entry::Header;

/// The caller admits object sizes and supplies one authenticated pack's chain,
/// target first and full base last. No selected-base requirement or durable PUT.
/// Returned bytes are provisional until EOF verifies the target OID; an error
/// after output starts requires aborting the response without a pack trailer.
pub(crate) struct ReadOnlyDelta<'a, 'log> {
    input: &'a Input<'log>,
    chain: &'a [IndexedEntry],
    cursor: Cursor<'a, 'log>,
    layers: Vec<Layer>,
    kind: gix_object::Kind,
    emitted: usize,
    hash: gix_hash::Hasher,
    finished: bool,
    _memory: Reservation,
}

impl<'a, 'log> ReadOnlyDelta<'a, 'log> {
    pub(crate) async fn new(
        input: &'a Input<'log>,
        chain: &'a [IndexedEntry],
    ) -> Result<Self, Error> {
        let kind = validate_chain(input, chain)?;
        let target = chain[0]
            .id
            .ok_or_else(|| pack_error("delta target ID is missing"))?;
        let memory = input
            .operation
            .reserve(chain.len() * std::mem::size_of::<Layer>())?;
        let mut cursor = Cursor::new(input);
        // Drain all encoded entries: a requested range may never reach its base's
        // trailer. Verify command bounds and exact CRC/zlib before yielding data.
        for (position, entry) in chain.iter().enumerate() {
            if position + 1 == chain.len() {
                verify_full_base(input, entry, &mut cursor).await?;
                continue;
            }
            let mut layer = Layer::new(
                input,
                entry,
                chain.get(position + 1).map(|base| base.result_size),
            )?;
            layer.seek(entry.result_size, &mut cursor).await?;
            layer.finish(&mut cursor).await?;
        }
        let mut layers = Vec::with_capacity(chain.len());
        for (position, entry) in chain.iter().enumerate() {
            layers.push(Layer::new(
                input,
                entry,
                chain.get(position + 1).map(|base| base.result_size),
            )?);
        }
        let mut hash = gix_hash::hasher(object_hash(target.format()));
        hash.update(&gix_object::encode::loose_header(
            kind,
            chain[0].result_size as u64,
        ));
        let mut result = Self {
            input,
            chain,
            cursor,
            layers,
            kind,
            emitted: 0,
            hash,
            finished: false,
            _memory: memory,
        };
        result.verify_dependencies().await?;
        Ok(result)
    }

    async fn verify_dependencies(&mut self) -> Result<(), Error> {
        let _memory = self.input.operation.reserve(SCAN_WINDOW_BYTES)?;
        let mut window = vec![0; SCAN_WINDOW_BYTES];
        for first in (1..self.chain.len().saturating_sub(1)).rev() {
            let entry = &self.chain[first];
            let expected = entry
                .id
                .ok_or_else(|| pack_error("delta dependency ID is missing"))?;
            let mut hash = gix_hash::hasher(object_hash(expected.format()));
            hash.update(&gix_object::encode::loose_header(
                self.kind,
                entry.result_size as u64,
            ));
            let mut offset = 0;
            while offset < entry.result_size {
                let limit = window.len().min(entry.result_size - offset);
                let count = self.fill(first, offset, &mut window[..limit]).await?;
                hash.update(&window[..count]);
                offset += count;
            }
            if hash.try_finalize().map_err(pack_error)?.as_slice() != expected.as_bytes() {
                return invalid("decoded dependency ID does not match");
            }
        }
        Ok(())
    }

    pub(crate) fn kind(&self) -> gix_object::Kind {
        self.kind
    }
    pub(crate) fn len(&self) -> usize {
        self.chain[0].result_size
    }

    /// Pull at most one fixed window. No reads occur until the next pull.
    pub(crate) async fn next_into(&mut self, output: &mut [u8]) -> Result<usize, Error> {
        if self.finished {
            return Ok(0);
        }
        if self.emitted == self.len() {
            let expected = self.chain[0]
                .id
                .ok_or_else(|| pack_error("delta target ID is missing"))?;
            // Finalization consumes the hasher; preserve an inert replacement.
            let hash = std::mem::replace(
                &mut self.hash,
                gix_hash::hasher(object_hash(expected.format())),
            );
            if hash.try_finalize().map_err(pack_error)?.as_slice() != expected.as_bytes() {
                return invalid("decoded object ID does not match");
            }
            self.finished = true;
            return Ok(0);
        }
        if output.is_empty() {
            return invalid("delta output window is empty");
        }
        let limit = output
            .len()
            .min(SCAN_WINDOW_BYTES)
            .min(self.len() - self.emitted);
        let written = self.fill(0, self.emitted, &mut output[..limit]).await?;
        self.hash.update(&output[..written]);
        self.emitted += written;
        Ok(written)
    }
    async fn fill(
        &mut self,
        first: usize,
        start: usize,
        output: &mut [u8],
    ) -> Result<usize, Error> {
        let limit = output.len();
        let mut written = 0;
        while written < limit {
            let mut offset = start + written;
            let mut count = limit - written;
            let mut copied = false;
            // A copy descends exactly one chain level. This explicit stack has
            // no recursive futures and cannot retain one input chunk per layer.
            for layer in &mut self.layers[first..] {
                layer.seek(offset, &mut self.cursor).await?;
                layer.command(&mut self.cursor).await?;
                match layer.pending {
                    Command::Copy { start, remaining } => {
                        offset = start;
                        count = count.min(remaining);
                    }
                    Command::Literal { remaining } => {
                        let bytes = layer.decoder.window(&mut self.cursor).await?;
                        count = count.min(remaining).min(bytes.len());
                        if count == 0 {
                            return invalid("delta literal is truncated");
                        }
                        output[written..written + count].copy_from_slice(&bytes[..count]);
                        layer.consume(count);
                        copied = true;
                        break;
                    }
                    Command::None => return invalid("delta result is truncated"),
                }
            }
            if !copied {
                return invalid("delta chain has no full base");
            }
            written += count;
        }
        self.input.operation.work(written)?;
        Ok(written)
    }
}

async fn verify_full_base(
    input: &Input<'_>,
    entry: &IndexedEntry,
    cursor: &mut Cursor<'_, '_>,
) -> Result<(), Error> {
    let expected = entry
        .id
        .ok_or_else(|| pack_error("delta base ID is missing"))?;
    let kind = entry
        .header
        .header
        .as_kind()
        .ok_or_else(|| pack_error("delta base kind is invalid"))?;
    let mut hash = gix_hash::hasher(object_hash(expected.format()));
    hash.update(&gix_object::encode::loose_header(
        kind,
        entry.result_size as u64,
    ));
    let mut decoder = Decoder::new(input, entry)?;
    loop {
        let bytes = decoder.window(cursor).await?;
        if bytes.is_empty() {
            break;
        }
        input.operation.work(bytes.len())?;
        hash.update(bytes);
        decoder.offset = decoder.available;
    }
    if hash.try_finalize().map_err(pack_error)?.as_slice() != expected.as_bytes() {
        return invalid("decoded base ID does not match");
    }
    Ok(())
}

fn validate_chain(input: &Input<'_>, chain: &[IndexedEntry]) -> Result<gix_object::Kind, Error> {
    if chain.is_empty() || chain.len() > MAX_DELTA_DEPTH + 1 {
        return invalid("delta graph is too deep or empty");
    }
    let first = chain[0]
        .id
        .ok_or_else(|| pack_error("delta target ID is missing"))?;
    for (position, entry) in chain.iter().enumerate() {
        let id = entry
            .id
            .ok_or_else(|| pack_error("delta chain ID is missing"))?;
        if entry.header.data_offset < entry.header.header_size() as u64
            || entry.header.header_size()
                != entry.header.header.size(entry.header.decompressed_size)
            || entry.header.decompressed_size == u64::MAX
        {
            return invalid("delta chain header is invalid");
        }
        if id.format() != first.format()
            || entry.header.pack_offset() < 12
            || entry.header.data_offset >= entry.end
            || entry.end
                > input
                    .bytes
                    .saturating_sub(first.format().digest_len() as u64)
        {
            return invalid("delta chain range is invalid");
        }
        input
            .operation
            .work(position * std::mem::size_of::<u64>())?;
        if chain[..position]
            .iter()
            .any(|previous| previous.header.pack_offset() == entry.header.pack_offset())
        {
            return invalid("delta graph contains a cycle");
        }
        match (entry.header.header, chain.get(position + 1)) {
            (Header::OfsDelta { base_distance }, Some(base))
                if entry.header.checked_base_pack_offset(base_distance)
                    == Some(base.header.pack_offset()) => {}
            (Header::RefDelta { base_id }, Some(base))
                if base
                    .id
                    .is_some_and(|id| base_id.as_slice() == id.as_bytes()) => {}
            (header, None)
                if header.as_kind().is_some()
                    && entry.header.decompressed_size == entry.result_size as u64 => {}
            _ => return invalid("delta chain base mismatch"),
        }
    }
    chain
        .last()
        .and_then(|entry| entry.header.header.as_kind())
        .ok_or_else(|| pack_error("delta chain lacks full base"))
}

#[derive(Clone, Copy)]
enum Command {
    None,
    Literal { remaining: usize },
    Copy { start: usize, remaining: usize },
}

struct Layer {
    decoder: Decoder,
    base: Option<usize>,
    size: usize,
    position: usize,
    initialized: bool,
    pending: Command,
}
impl Layer {
    fn new(input: &Input<'_>, entry: &IndexedEntry, base: Option<usize>) -> Result<Self, Error> {
        Ok(Self {
            decoder: Decoder::new(input, entry)?,
            base,
            size: entry.result_size,
            position: 0,
            initialized: false,
            pending: Command::None,
        })
    }
    async fn command(&mut self, cursor: &mut Cursor<'_, '_>) -> Result<(), Error> {
        if !self.initialized {
            if let Some(base) = self.base {
                if self.decoder.integer(cursor).await? != base
                    || self.decoder.integer(cursor).await? != self.size
                {
                    return invalid("delta base or result size mismatch");
                }
            } else {
                self.pending = Command::Literal {
                    remaining: self.size,
                };
            }
            self.initialized = true;
        }
        if !matches!(self.pending, Command::None) {
            return Ok(());
        }
        if self.decoder.window(cursor).await?.is_empty() {
            return Ok(());
        }
        if self.base.is_none() {
            return invalid("full object exceeds declared size");
        }
        let code = self.decoder.byte(cursor).await?;
        if code == 0 {
            return invalid("delta opcode zero is invalid");
        }
        let (start, length) = if code & 0x80 == 0 {
            (None, usize::from(code))
        } else {
            let mut offset = 0_u64;
            let mut length = 0_u64;
            for bit in 0..4 {
                if code & (1 << bit) != 0 {
                    offset |= u64::from(self.decoder.byte(cursor).await?) << (bit * 8);
                }
            }
            for bit in 0..3 {
                if code & (0x10 << bit) != 0 {
                    length |= u64::from(self.decoder.byte(cursor).await?) << (bit * 8);
                }
            }
            if length == 0 {
                length = 0x1_0000;
            }
            let start = usize::try_from(offset).map_err(pack_error)?;
            let length = usize::try_from(length).map_err(pack_error)?;
            if start
                .checked_add(length)
                .is_none_or(|end| end > self.base.unwrap_or(0))
            {
                return invalid("delta copy exceeds base");
            }
            (Some(start), length)
        };
        if length > self.size - self.position {
            return invalid("delta output exceeds declared size");
        }
        self.pending = start.map_or(Command::Literal { remaining: length }, |start| {
            Command::Copy {
                start,
                remaining: length,
            }
        });
        Ok(())
    }
    fn consume(&mut self, count: usize) {
        self.pending = match self.pending {
            Command::Literal { remaining } => {
                self.decoder.offset += count;
                if remaining == count {
                    Command::None
                } else {
                    Command::Literal {
                        remaining: remaining - count,
                    }
                }
            }
            Command::Copy { start, remaining } => {
                if remaining == count {
                    Command::None
                } else {
                    Command::Copy {
                        start: start + count,
                        remaining: remaining - count,
                    }
                }
            }
            Command::None => Command::None,
        };
        self.position += count;
    }
    async fn seek(&mut self, offset: usize, cursor: &mut Cursor<'_, '_>) -> Result<(), Error> {
        if offset > self.size {
            return invalid("delta range exceeds result");
        }
        if offset < self.position {
            self.decoder.reset();
            self.position = 0;
            self.initialized = false;
            self.pending = Command::None;
        }
        while self.position < offset {
            self.command(cursor).await?;
            let count = match self.pending {
                Command::Copy { remaining, .. } => remaining.min(offset - self.position),
                Command::Literal { remaining } => remaining
                    .min(offset - self.position)
                    .min(self.decoder.window(cursor).await?.len()),
                Command::None => 0,
            };
            if count == 0 {
                return invalid("delta result is truncated");
            }
            self.consume(count);
        }
        Ok(())
    }
    async fn finish(&mut self, cursor: &mut Cursor<'_, '_>) -> Result<(), Error> {
        self.command(cursor).await?;
        if self.position != self.size
            || !self.decoder.window(cursor).await?.is_empty()
            || matches!(
                self.pending,
                Command::Copy { .. } | Command::Literal { remaining: 1.. }
            )
        {
            return invalid("delta result size mismatch");
        }
        Ok(())
    }
}

/// Decoder state is per layer; the authenticated compressed chunk cache is shared.
struct Decoder {
    start: u64,
    end: u64,
    size: u64,
    position: u64,
    initial_crc: u32,
    crc: u32,
    expected_crc: u32,
    codec: gix_zlib::Decompress,
    window: Vec<u8>,
    offset: usize,
    available: usize,
    finished: bool,
    _memory: Reservation,
}
impl Decoder {
    fn new(input: &Input<'_>, entry: &IndexedEntry) -> Result<Self, Error> {
        let memory = input.operation.reserve(INFLATE_BYTES + SCAN_WINDOW_BYTES)?;
        let mut header = [0; 42];
        let count = entry
            .header
            .header
            .write_to(entry.header.decompressed_size, &mut &mut header[..])
            .map_err(pack_error)?;
        let crc = gix_features::hash::crc32(&header[..count]);
        Ok(Self {
            start: entry.header.data_offset,
            end: entry.end,
            size: entry.header.decompressed_size,
            position: entry.header.data_offset,
            initial_crc: crc,
            crc,
            expected_crc: entry.crc,
            codec: gix_zlib::Decompress::new(),
            window: vec![0; SCAN_WINDOW_BYTES],
            offset: 0,
            available: 0,
            finished: false,
            _memory: memory,
        })
    }
    fn reset(&mut self) {
        self.codec.reset();
        self.position = self.start;
        self.crc = self.initial_crc;
        self.offset = 0;
        self.available = 0;
        self.finished = false;
    }
    async fn window(&mut self, cursor: &mut Cursor<'_, '_>) -> Result<&[u8], Error> {
        while self.offset == self.available && !self.finished {
            cursor.position = self.position;
            let bytes = cursor.window().await?;
            let available = bytes
                .len()
                .min(usize::try_from(self.end - self.position).map_err(pack_error)?);
            let before_in = self.codec.total_in();
            let before_out = self.codec.total_out();
            let capacity = self.window.len().min(
                usize::try_from(self.size.saturating_sub(before_out).saturating_add(1))
                    .map_err(pack_error)?,
            );
            let status = self
                .codec
                .decompress(
                    &bytes[..available],
                    &mut self.window[..capacity],
                    gix_zlib::FlushDecompress::None,
                )
                .map_err(pack_error)?;
            let consumed =
                usize::try_from(self.codec.total_in() - before_in).map_err(pack_error)?;
            self.available =
                usize::try_from(self.codec.total_out() - before_out).map_err(pack_error)?;
            cursor.input.operation.work(consumed * 2 + self.available)?;
            self.crc = gix_features::hash::crc32_update(self.crc, &bytes[..consumed]);
            self.position += consumed as u64;
            self.offset = 0;
            if self.codec.total_out() > self.size {
                return invalid("delta inflation exceeds declared size");
            }
            if status == gix_zlib::Status::StreamEnd {
                if self.codec.total_out() != self.size || self.position != self.end {
                    return invalid("delta zlib boundary mismatch");
                }
                if self.crc != self.expected_crc {
                    return invalid("delta entry CRC mismatch");
                }
                self.finished = true;
            } else if consumed == 0 && self.available == 0 {
                return invalid("delta zlib made no progress");
            }
        }
        Ok(&self.window[self.offset..self.available])
    }
    async fn byte(&mut self, cursor: &mut Cursor<'_, '_>) -> Result<u8, Error> {
        let byte = self
            .window(cursor)
            .await?
            .first()
            .copied()
            .ok_or_else(|| pack_error("delta instruction is truncated"))?;
        self.offset += 1;
        Ok(byte)
    }
    async fn integer(&mut self, cursor: &mut Cursor<'_, '_>) -> Result<usize, Error> {
        let mut bytes = [0; 10];
        for position in 0..bytes.len() {
            bytes[position] = self.byte(cursor).await?;
            if bytes[position] & 0x80 == 0 {
                return crate::pack::delta_integer(&bytes[..=position]).map(|(value, _)| value);
            }
        }
        invalid("delta integer overflows")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ObjectFormat, ObjectId,
        pack::budget::{LIVE_BYTES, Pool},
    };
    use bytes::Bytes;
    use futures::stream;
    use object_log::{
        Log, LogId, Options, ValidatedBackend,
        sim::{FaultStore, Operation as StoreOperation},
    };
    use object_store::{memory::InMemory, path::Path};
    use std::{error::Error as StdError, io::Write as _, sync::Arc};
    type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

    fn integer(mut value: usize, bytes: &mut Vec<u8>) {
        loop {
            let rest = value >> 7;
            bytes.push(value.to_le_bytes()[0] & 0x7f | if rest == 0 { 0 } else { 0x80 });
            if rest == 0 {
                break;
            }
            value = rest;
        }
    }
    fn copy(mut offset: u32, mut length: u32, bytes: &mut Vec<u8>) {
        while length > 0 {
            let count = length.min(0xff_ffff);
            bytes.push(0xff);
            bytes.extend_from_slice(&offset.to_le_bytes());
            bytes.extend_from_slice(&count.to_le_bytes()[..3]);
            offset += count;
            length -= count;
        }
    }
    fn append(
        pack: &mut Vec<u8>,
        header: Header,
        body: &[u8],
        result_size: usize,
        id: ObjectId,
    ) -> TestResult<IndexedEntry> {
        let start = pack.len();
        let header_len = header.write_to(body.len() as u64, pack)?;
        let mut compressor =
            gix_zlib::stream::deflate::Write::new(&mut *pack, gix_zlib::Compression::DEFAULT);
        compressor.write_all(body)?;
        compressor.flush()?;
        drop(compressor);
        Ok(IndexedEntry {
            header: gix_pack::data::Entry {
                header,
                encoded_header_size: u16::try_from(header_len)?,
                decompressed_size: body.len() as u64,
                data_offset: (start + header_len) as u64,
            },
            end: pack.len() as u64,
            crc: gix_features::hash::crc32(&pack[start..]),
            id: Some(id),
            result_size,
        })
    }
    fn oid(format: ObjectFormat, data: &[u8]) -> TestResult<ObjectId> {
        Ok(ObjectId::from_bytes(
            format,
            gix_object::compute_hash(object_hash(format), gix_object::Kind::Blob, data)?.as_slice(),
        )?)
    }
    fn seal(pack: &mut Vec<u8>, format: ObjectFormat) -> TestResult {
        let mut hash = gix_hash::hasher(object_hash(format));
        hash.update(pack);
        pack.extend_from_slice(hash.try_finalize()?.as_slice());
        Ok(())
    }
    async fn open() -> TestResult<(Log, object_log::View, FaultStore)> {
        let faults = FaultStore::new(InMemory::new());
        let backend =
            ValidatedBackend::new(Arc::new(faults.clone()), Path::from("readonly-delta")).await?;
        let log = Log::open(&backend, &LogId::new("readonly-delta")?, Options::default()).await?;
        let view = log.load().await?;
        Ok((log, view, faults))
    }
    async fn read_all(reader: &mut ReadOnlyDelta<'_, '_>) -> TestResult<Vec<u8>> {
        let mut output = Vec::new();
        let mut window = vec![0; 17003];
        loop {
            let count = reader.next_into(&mut window).await?;
            if count == 0 {
                break;
            }
            output.extend_from_slice(&window[..count]);
        }
        Ok(output)
    }

    #[tokio::test]
    async fn read_only_delta_large_backward_copy_is_bounded_and_never_stages() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let mut base = vec![b'a'; 50 * 1024 * 1024];
            base[25 * 1024 * 1024..].fill(b'b');
            let base_id = oid(format, &base)?;
            let mut hash = gix_hash::hasher(object_hash(format));
            hash.update(&gix_object::encode::loose_header(
                gix_object::Kind::Blob,
                base.len() as u64,
            ));
            hash.update(&base[25 * 1024 * 1024..]);
            hash.update(&base[..25 * 1024 * 1024]);
            let target = ObjectId::from_bytes(format, hash.try_finalize()?.as_slice())?;
            let mut pack = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
            let base_entry = append(&mut pack, Header::Blob, &base, base.len(), base_id)?;
            let mut commands = Vec::new();
            integer(base.len(), &mut commands);
            integer(base.len(), &mut commands);
            copy(25 * 1024 * 1024, 25 * 1024 * 1024, &mut commands);
            copy(0, 25 * 1024 * 1024, &mut commands);
            let delta = append(
                &mut pack,
                Header::RefDelta {
                    base_id: gix_hash::ObjectId::from_bytes_or_panic(base_id.as_bytes()),
                },
                &commands,
                base.len(),
                target,
            )?;
            seal(&mut pack, format)?;
            drop(base);
            let (base_log, view, faults) = open().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter([Ok(Bytes::from(pack))]),
            )
            .await?;
            let chain = [delta, base_entry];
            faults.reset();
            let mut reader = ReadOnlyDelta::new(&input, &chain).await?;
            assert_eq!(reader.kind(), gix_object::Kind::Blob);
            assert_eq!(reader.len(), 50 * 1024 * 1024);
            let mut window = vec![0; SCAN_WINDOW_BYTES];
            let mut position = 0;
            loop {
                let count = reader.next_into(&mut window).await?;
                if count == 0 {
                    break;
                }
                for (offset, byte) in window[..count].iter().enumerate() {
                    assert_eq!(
                        *byte,
                        if position + offset < 25 * 1024 * 1024 {
                            b'b'
                        } else {
                            b'a'
                        }
                    );
                }
                position += count;
                assert!(operation.live_bytes() < 2 * 1024 * 1024);
            }
            assert_eq!(position, 50 * 1024 * 1024);
            assert_eq!(faults.metrics().operation(StoreOperation::Put).requests, 0);
            drop(reader);
            drop(input);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_only_delta_ofs_ref_chain_and_pull_stop() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let base = vec![b'x'; 70_000];
            let middle = [base.as_slice(), b"middle"].concat();
            let target = [b"target".as_slice(), &middle[30_000..65_536]].concat();
            let base_id = oid(format, &base)?;
            let middle_id = oid(format, &middle)?;
            let target_id = oid(format, &target)?;
            let mut pack = b"PACK\0\0\0\x02\0\0\0\x03".to_vec();
            let base_entry = append(&mut pack, Header::Blob, &base, base.len(), base_id)?;
            let mut commands = Vec::new();
            integer(base.len(), &mut commands);
            integer(middle.len(), &mut commands);
            copy(0, 70_000, &mut commands);
            commands.extend_from_slice(b"\x06middle");
            let header = Header::OfsDelta {
                base_distance: pack.len() as u64 - base_entry.header.pack_offset(),
            };
            let middle_entry = append(&mut pack, header, &commands, middle.len(), middle_id)?;
            commands.clear();
            integer(middle.len(), &mut commands);
            integer(target.len(), &mut commands);
            commands.extend_from_slice(b"\x06target");
            copy(30_000, 35_536, &mut commands);
            let target_entry = append(
                &mut pack,
                Header::RefDelta {
                    base_id: gix_hash::ObjectId::from_bytes_or_panic(middle_id.as_bytes()),
                },
                &commands,
                target.len(),
                target_id,
            )?;
            seal(&mut pack, format)?;
            let (base_log, view, faults) = open().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = base_log.with_request_guard(Arc::new(operation.clone()));
            let input = Input::receive(
                &operation,
                &log,
                &view,
                stream::iter([Ok(Bytes::from(pack))]),
            )
            .await?;
            let chain = [target_entry, middle_entry, base_entry];
            faults.reset();
            let mut reader = ReadOnlyDelta::new(&input, &chain).await?;
            assert_eq!(read_all(&mut reader).await?, target);
            drop(reader);
            let mut reader = ReadOnlyDelta::new(&input, &chain).await?;
            reader.next_into(&mut [0; 17]).await?;
            let reads = faults.metrics().operation(StoreOperation::Get).requests;
            drop(reader);
            assert_eq!(
                faults.metrics().operation(StoreOperation::Get).requests,
                reads
            );
            assert_eq!(faults.metrics().operation(StoreOperation::Put).requests, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_only_delta_rejects_crc_commands_and_final_identity() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for fault in 0..6 {
                let base = b"abcd";
                let base_id = oid(format, base)?;
                let mut pack = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
                let base_entry = append(&mut pack, Header::Blob, base, 4, base_id)?;
                let commands: &[u8] = match fault {
                    1 => &[4, 4, 0],
                    2 => &[4, 4, 4, b'a'],
                    _ => &[4, 4, 0x90, 4],
                };
                let mut delta = append(
                    &mut pack,
                    Header::RefDelta {
                        base_id: gix_hash::ObjectId::from_bytes_or_panic(base_id.as_bytes()),
                    },
                    commands,
                    4,
                    if fault == 3 {
                        oid(format, b"else")?
                    } else {
                        base_id
                    },
                )?;
                if fault == 0 {
                    delta.crc ^= 1;
                }
                if fault == 4 {
                    delta.end -= 1;
                }
                if fault == 5 {
                    pack.push(0);
                    delta.end += 1;
                    delta.crc = gix_features::hash::crc32(
                        &pack[usize::try_from(delta.header.pack_offset())?..],
                    );
                }
                seal(&mut pack, format)?;
                let (base_log, view, faults) = open().await?;
                let operation = Pool::new(LIVE_BYTES).admit()?;
                let log = base_log.with_request_guard(Arc::new(operation.clone()));
                let input = Input::receive(
                    &operation,
                    &log,
                    &view,
                    stream::iter([Ok(Bytes::from(pack))]),
                )
                .await?;
                let chain = [delta, base_entry];
                faults.reset();
                let result = ReadOnlyDelta::new(&input, &chain).await;
                if fault == 3 {
                    assert!(read_all(&mut result?).await.is_err());
                } else {
                    assert!(result.is_err());
                }
                assert_eq!(faults.metrics().operation(StoreOperation::Put).requests, 0);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn read_only_delta_checks_unused_base_and_intermediate_identity() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for corrupt_base in [true, false] {
                let base_id = oid(format, if corrupt_base { b"abce" } else { b"abcd" })?;
                let middle_id = oid(format, if corrupt_base { b"abcdX" } else { b"abcdY" })?;
                let target = oid(format, b"abc")?;
                let mut pack = b"PACK\0\0\0\x02\0\0\0\x03".to_vec();
                let base = append(&mut pack, Header::Blob, b"abcd", 4, base_id)?;
                let middle = append(
                    &mut pack,
                    Header::RefDelta {
                        base_id: gix_hash::ObjectId::from_bytes_or_panic(base_id.as_bytes()),
                    },
                    &[4, 5, 0x90, 4, 1, b'X'],
                    5,
                    middle_id,
                )?;
                let last = append(
                    &mut pack,
                    Header::RefDelta {
                        base_id: gix_hash::ObjectId::from_bytes_or_panic(middle_id.as_bytes()),
                    },
                    &[5, 3, 0x90, 3],
                    3,
                    target,
                )?;
                seal(&mut pack, format)?;
                let (base_log, view, faults) = open().await?;
                let operation = Pool::new(LIVE_BYTES).admit()?;
                let log = base_log.with_request_guard(Arc::new(operation.clone()));
                let input = Input::receive(
                    &operation,
                    &log,
                    &view,
                    stream::iter([Ok(Bytes::from(pack))]),
                )
                .await?;
                let chain = [last, middle, base];
                faults.reset();
                assert!(ReadOnlyDelta::new(&input, &chain).await.is_err());
                assert_eq!(faults.metrics().operation(StoreOperation::Put).requests, 0);
                drop(input);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn read_only_delta_repeated_backward_copies_spend_cumulative_work() -> TestResult {
        let format = ObjectFormat::Sha1;
        let base = vec![b'x'; 1024 * 1024];
        let base_id = oid(format, &base)?;
        let target = oid(format, &vec![b'x'; 512])?;
        let mut commands = Vec::new();
        integer(base.len(), &mut commands);
        integer(512, &mut commands);
        for _ in 0..256 {
            copy(1024 * 1024 - 1, 1, &mut commands);
            copy(0, 1, &mut commands);
        }
        let mut pack = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
        let base_entry = append(&mut pack, Header::Blob, &base, base.len(), base_id)?;
        let delta = append(
            &mut pack,
            Header::RefDelta {
                base_id: gix_hash::ObjectId::from_bytes_or_panic(base_id.as_bytes()),
            },
            &commands,
            512,
            target,
        )?;
        seal(&mut pack, format)?;
        let (base_log, view, faults) = open().await?;
        let operation = Pool::new(LIVE_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(pack))]),
        )
        .await?;
        let chain = [delta, base_entry];
        faults.reset();
        let mut reader = ReadOnlyDelta::new(&input, &chain).await?;
        // Leave a fixed replay allowance; supported payload size must not change
        // whether each backward seek is charged cumulatively.
        operation
            .work(crate::pack::budget::WORK_BYTES - operation.work_bytes() - 256 * 1024 * 1024)?;
        let error = reader
            .next_into(&mut [0; 512])
            .await
            .err()
            .ok_or("replay should exhaust work")?;
        assert!(error.to_string().contains("work limit"));
        assert!(operation.work_bytes() > 250 * 1024 * 1024);
        assert_eq!(faults.metrics().operation(StoreOperation::Put).requests, 0);
        drop(reader);
        drop(input);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }
    #[tokio::test]
    async fn read_only_delta_rejects_chain_and_header_geometry_before_reads() -> TestResult {
        let format = ObjectFormat::Sha1;
        let id = oid(format, b"abc")?;
        let mut pack = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
        let entry = append(&mut pack, Header::Blob, b"abc", 3, id)?;
        seal(&mut pack, format)?;
        let (base_log, view, faults) = open().await?;
        let operation = Pool::new(LIVE_BYTES).admit()?;
        let log = base_log.with_request_guard(Arc::new(operation.clone()));
        let input = Input::receive(
            &operation,
            &log,
            &view,
            stream::iter([Ok(Bytes::from(pack))]),
        )
        .await?;
        for fault in 0..4 {
            let count = if fault == 0 { MAX_DELTA_DEPTH + 2 } else { 1 };
            let mut chain = (0..count)
                .map(|_| IndexedEntry {
                    header: entry.header.clone(),
                    end: entry.end,
                    crc: entry.crc,
                    id: entry.id,
                    result_size: entry.result_size,
                })
                .collect::<Vec<_>>();
            match fault {
                1 => chain[0].header.data_offset = 0,
                2 => chain[0].header.decompressed_size = u64::MAX,
                3 => {
                    chain[0].header.header = Header::RefDelta {
                        base_id: gix_hash::ObjectId::from_bytes_or_panic(id.as_bytes()),
                    }
                }
                _ => {}
            }
            faults.reset();
            assert!(ReadOnlyDelta::new(&input, &chain).await.is_err());
            assert_eq!(faults.metrics().total_requests(), 0);
        }
        Ok(())
    }
}
