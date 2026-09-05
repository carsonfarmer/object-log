//! Unpublished bounded decoded scratch. Its view is not a retention lease.

use bytes::BytesMut;
use object_log::{Log, View};

use super::{Cursor, Input};
use crate::{
    Error, ObjectId,
    pack::{
        INFLATE_BYTES, MAX_STREAM_OBJECT_BYTES, SCAN_WINDOW_BYTES,
        budget::{Operation, Reservation},
        invalid, object_hash, pack_error,
    },
};

pub(super) struct Sink<'a> {
    input: Input<'a>,
    pending: BytesMut,
    memory: Reservation,
    limit: usize,
    inline: bool,
}

impl<'a> Sink<'a> {
    pub(super) fn new(
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
        limit: usize,
    ) -> Result<Self, Error> {
        Self::with_inline(operation, log, view, limit, false)
    }

    fn with_inline(
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
        limit: usize,
        inline: bool,
    ) -> Result<Self, Error> {
        let input = Input::empty(operation, log, view, limit)?;
        let capacity = if inline { limit } else { input.width };
        let memory = operation.reserve(capacity)?;
        let pending = BytesMut::with_capacity(capacity);
        Ok(Self {
            input,
            pending,
            memory,
            limit,
            inline,
        })
    }

    pub(super) fn len(&self) -> u64 {
        self.input.bytes
    }

    pub(super) async fn write(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() as u64 > self.limit as u64 - self.input.bytes {
            return invalid("scratch or output exceeds byte limit");
        }
        self.input.operation.work(bytes.len())?;
        self.input.bytes += bytes.len() as u64;
        while !bytes.is_empty() {
            let width = if self.inline {
                self.limit
            } else {
                self.input.width
            };
            let count = bytes.len().min(width - self.pending.len());
            self.pending.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if !self.inline && self.pending.len() == self.input.width {
                self.flush(true).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self, refill: bool) -> Result<(), Error> {
        let bytes = std::mem::take(&mut self.pending).freeze();
        // Keep the outgoing allocation charged across the await without putting
        // a reservation inside Bytes retained indefinitely by memory backends.
        let memory = std::mem::replace(&mut self.memory, self.input.operation.reserve(0)?);
        self.input.put(bytes, memory).await?;
        if refill {
            self.memory = self.input.operation.reserve(self.input.width)?;
            self.pending = BytesMut::with_capacity(self.input.width);
        }
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<Input<'a>, Error> {
        if self.inline {
            self.input.inline = Some(crate::pack::budget::hold(
                self.pending.freeze(),
                self.memory,
            ));
        } else if !self.pending.is_empty() {
            self.flush(false).await?;
        }
        Ok(self.input)
    }
}

/// A fully hash-verified decoded object, readable only under its original view.
pub(crate) struct Decoded<'a> {
    pub(super) input: Input<'a>,
    pub(super) kind: gix_object::Kind,
    pub(super) id: ObjectId,
    pub(super) depth: usize,
    pub(super) context: std::sync::Arc<()>,
    _context_memory: Reservation,
}

#[cfg(test)]
impl Decoded<'_> {
    pub(crate) fn id(&self) -> ObjectId {
        self.id
    }
    pub(crate) fn len(&self) -> u64 {
        self.input.bytes
    }
}

pub(super) struct ObjectSink<'a> {
    sink: Sink<'a>,
    hash: gix_hash::Hasher,
    kind: gix_object::Kind,
    size: usize,
    format: crate::ObjectFormat,
    context: std::sync::Arc<()>,
    context_memory: Reservation,
}

impl<'a> ObjectSink<'a> {
    pub(super) fn new(
        source: &Input<'a>,
        kind: gix_object::Kind,
        size: usize,
        format: crate::ObjectFormat,
    ) -> Result<Self, Error> {
        if size > MAX_STREAM_OBJECT_BYTES
            || (kind != gix_object::Kind::Blob && size > crate::pack::MAX_OBJECT_BYTES)
        {
            return invalid("decoded scratch exceeds object limit");
        }
        // Tiny decoded objects are transient computation, not publication
        // roots. Keep them charged to this request; the durable encoded input
        // can reconstruct them after a retry. Larger objects spill in chunks.
        let sink = Sink::with_inline(
            &source.operation,
            source.log,
            source.view,
            size,
            size <= SCAN_WINDOW_BYTES,
        )?;
        let mut hash = gix_hash::hasher(object_hash(format));
        hash.update(&gix_object::encode::loose_header(kind, size as u64));
        Ok(Self {
            sink,
            hash,
            kind,
            size,
            format,
            context: source.context.clone(),
            context_memory: source.operation.reserve(2 * std::mem::size_of::<usize>())?,
        })
    }

    pub(super) async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.sink.input.operation.work(bytes.len())?;
        self.hash.update(bytes);
        self.sink.write(bytes).await
    }

    pub(super) async fn finish(
        self,
        expected: Option<ObjectId>,
        depth: usize,
    ) -> Result<Decoded<'a>, Error> {
        if self.sink.len() != self.size as u64 {
            return invalid("decoded scratch size mismatch");
        }
        let id = ObjectId::from_bytes(
            self.format,
            self.hash.try_finalize().map_err(pack_error)?.as_slice(),
        )?;
        if expected.is_some_and(|expected| expected != id) {
            return invalid("decoded scratch OID mismatch");
        }
        Ok(Decoded {
            input: self.sink.finish().await?,
            kind: self.kind,
            id,
            depth,
            context: self.context,
            _context_memory: self.context_memory,
        })
    }
}

pub(super) struct Inflated<'a, 'log> {
    cursor: Cursor<'a, 'log>,
    end: u64,
    size: u64,
    codec: gix_zlib::Decompress,
    window: Vec<u8>,
    offset: usize,
    available: usize,
    finished: bool,
    crc: u32,
    expected_crc: u32,
    _memory: Reservation,
}

impl<'a, 'log> Inflated<'a, 'log> {
    pub(super) fn new(input: &'a Input<'log>, entry: &super::scan::Entry) -> Result<Self, Error> {
        input
            .operation
            .work(usize::try_from(entry.header.decompressed_size).map_err(pack_error)? + 1)?;
        let memory = input.operation.reserve(INFLATE_BYTES + SCAN_WINDOW_BYTES)?;
        let mut header = [0; 42];
        let count = entry
            .header
            .header
            .write_to(entry.header.decompressed_size, &mut &mut header[..])
            .map_err(pack_error)?;
        input.operation.work(count)?;
        let crc = gix_features::hash::crc32(&header[..count]);
        let mut cursor = Cursor::new(input);
        cursor.position = entry.header.data_offset;
        Ok(Self {
            cursor,
            end: entry.end,
            size: entry.header.decompressed_size,
            codec: gix_zlib::Decompress::new(),
            window: vec![0; SCAN_WINDOW_BYTES],
            offset: 0,
            available: 0,
            finished: false,
            crc,
            expected_crc: entry.crc,
            _memory: memory,
        })
    }

    pub(super) async fn window(&mut self) -> Result<&[u8], Error> {
        while self.offset == self.available && !self.finished {
            let bytes = self.cursor.window().await?;
            let available = bytes
                .len()
                .min(usize::try_from(self.end - self.cursor.position).map_err(pack_error)?);
            let before_in = self.codec.total_in();
            let before_out = self.codec.total_out();
            let capacity = self
                .window
                .len()
                .min(usize::try_from(self.size - before_out + 1).map_err(pack_error)?);
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
            self.cursor.input.operation.work(consumed * 2)?;
            self.crc = gix_features::hash::crc32_update(self.crc, &bytes[..consumed]);
            self.cursor.position += consumed as u64;
            self.offset = 0;
            self.available =
                usize::try_from(self.codec.total_out() - before_out).map_err(pack_error)?;
            if self.codec.total_out() > self.size {
                return invalid("scratch inflation exceeds declared size");
            }
            if status == gix_zlib::Status::StreamEnd {
                if self.codec.total_out() != self.size || self.cursor.position != self.end {
                    return invalid("scratch zlib boundary mismatch");
                }
                if self.crc != self.expected_crc {
                    return invalid("scratch entry CRC mismatch");
                }
                self.finished = true;
            } else if consumed == 0 && self.available == 0 {
                return invalid("scratch zlib made no progress");
            }
        }
        Ok(&self.window[self.offset..self.available])
    }

    pub(super) fn consume(&mut self, count: usize) {
        self.offset += count;
    }

    pub(super) async fn byte(&mut self) -> Result<u8, Error> {
        let byte = *self
            .window()
            .await?
            .first()
            .ok_or_else(|| pack_error("delta instruction is truncated"))?;
        self.consume(1);
        Ok(byte)
    }

    pub(super) async fn integer(&mut self) -> Result<usize, Error> {
        let mut encoded = [0; 10];
        for position in 0..encoded.len() {
            encoded[position] = self.byte().await?;
            if encoded[position] & 0x80 == 0 {
                return Ok(crate::pack::delta_integer(&encoded[..=position])?.0);
            }
        }
        invalid("delta integer overflows")
    }
}

pub(super) async fn decode<'a>(
    source: &Input<'a>,
    entry: &super::scan::Entry,
    format: crate::ObjectFormat,
    base: Option<&Decoded<'a>>,
) -> Result<Decoded<'a>, Error> {
    decode_from(source, source, entry, format, base).await
}

pub(super) async fn decode_from<'a>(
    source: &Input<'a>,
    encoded: &Input<'_>,
    entry: &super::scan::Entry,
    format: crate::ObjectFormat,
    base: Option<&Decoded<'a>>,
) -> Result<Decoded<'a>, Error> {
    let mut inflated = Inflated::new(encoded, entry)?;
    if let Some(kind) = entry.header.header.as_kind() {
        let mut output = ObjectSink::new(source, kind, entry.result_size, format)?;
        loop {
            let bytes = inflated.window().await?;
            if bytes.is_empty() {
                break;
            }
            let count = bytes.len();
            output.write(bytes).await?;
            inflated.consume(count);
        }
        return output.finish(entry.id, 0).await;
    }
    let base = base.ok_or_else(|| pack_error("delta base is unresolved"))?;
    if base.depth >= crate::pack::MAX_DELTA_DEPTH {
        return invalid("delta graph is too deep");
    }
    if inflated.integer().await? as u64 != base.input.bytes
        || inflated.integer().await? != entry.result_size
    {
        return invalid("delta base or result size mismatch");
    }
    let mut output = ObjectSink::new(source, base.kind, entry.result_size, format)?;
    let mut cursor = Cursor::new(&base.input);
    while !inflated.window().await?.is_empty() {
        let command = inflated.byte().await?;
        if command & 0x80 == 0 {
            if command == 0 {
                return invalid("delta opcode zero is invalid");
            }
            let mut remaining = usize::from(command);
            while remaining > 0 {
                let bytes = inflated.window().await?;
                if bytes.is_empty() {
                    return invalid("delta literal is truncated");
                }
                let count = remaining.min(bytes.len());
                output.write(&bytes[..count]).await?;
                inflated.consume(count);
                remaining -= count;
            }
        } else {
            let mut offset = 0_u64;
            let mut length = 0_u64;
            for bit in 0..4 {
                if command & (1 << bit) != 0 {
                    offset |= u64::from(inflated.byte().await?) << (bit * 8);
                }
            }
            for bit in 0..3 {
                if command & (0x10 << bit) != 0 {
                    length |= u64::from(inflated.byte().await?) << (bit * 8);
                }
            }
            if length == 0 {
                length = 0x1_0000;
            }
            let end = offset
                .checked_add(length)
                .filter(|end| *end <= base.input.bytes)
                .ok_or_else(|| pack_error("delta copy exceeds base"))?;
            cursor.position = offset;
            while cursor.position < end {
                let bytes = cursor.window().await?;
                let count = bytes
                    .len()
                    .min(usize::try_from(end - cursor.position).map_err(pack_error)?);
                output.write(&bytes[..count]).await?;
                cursor.position += count as u64;
            }
        }
    }
    output.finish(entry.id, base.depth + 1).await
}
