//! Stream normalized pack bytes while retaining only standard index metadata.

use super::{
    BaseProvider, Cursor, Input,
    scan::{Entry, Scanned},
    scratch::{Decoded, Sink},
};
use crate::{
    Error,
    format::PackDescriptor,
    pack::{COMPRESS_BYTES, MAX_PACK_BYTES, SCAN_WINDOW_BYTES, invalid, object_hash, pack_error},
};
use gix_pack::data::entry::Header;
use object_log::StagedObject;
use std::mem::size_of;

struct Output<'a> {
    sink: Sink<'a>,
    operation: crate::pack::budget::Operation,
    hash: gix_hash::Hasher,
    crc: u32,
}

impl<'a> Output<'a> {
    fn new(input: &Input<'a>, format: crate::ObjectFormat) -> Result<Self, Error> {
        Ok(Self {
            sink: Sink::new(&input.operation, input.log, input.view, MAX_PACK_BYTES)?,
            operation: input.operation.clone(),
            hash: gix_hash::hasher(object_hash(format)),
            crc: 0,
        })
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.operation.work(bytes.len() * 2)?;
        self.hash.update(bytes);
        self.crc = gix_features::hash::crc32_update(self.crc, bytes);
        self.sink.write(bytes).await
    }

    async fn header(
        &mut self,
        header: Header,
        size: u64,
        format: crate::ObjectFormat,
    ) -> Result<gix_pack::data::Entry, Error> {
        let offset = self.sink.len();
        let mut bytes = [0; 42];
        let count = header
            .write_to(size, &mut &mut bytes[..])
            .map_err(pack_error)?;
        self.crc = 0;
        self.write(&bytes[..count]).await?;
        gix_pack::data::Entry::from_bytes(&bytes[..count], offset, object_hash(format))
            .map_err(pack_error)
    }

    async fn copy(&mut self, input: &Input<'_>, start: u64, end: u64) -> Result<(), Error> {
        let mut cursor = Cursor::new(input);
        cursor.position = start;
        while cursor.position < end {
            let bytes = cursor.window().await?;
            let count = bytes
                .len()
                .min(usize::try_from(end - cursor.position).map_err(pack_error)?);
            self.write(&bytes[..count]).await?;
            cursor.position += count as u64;
        }
        Ok(())
    }

    async fn compress(&mut self, object: &Decoded<'_>) -> Result<(), Error> {
        let _memory = self.operation.reserve(COMPRESS_BYTES + SCAN_WINDOW_BYTES)?;
        let mut codec = gix_zlib::stream::deflate::Compress::new(gix_zlib::Compression::DEFAULT);
        let mut window = vec![0; SCAN_WINDOW_BYTES];
        let mut cursor = Cursor::new(&object.input);
        loop {
            let bytes = cursor.window().await?;
            let finish = cursor.position == object.input.bytes;
            let before_in = codec.total_in();
            let before_out = codec.total_out();
            let status = codec
                .compress(
                    &bytes,
                    &mut window,
                    if finish {
                        gix_zlib::stream::deflate::FlushCompress::Finish
                    } else {
                        gix_zlib::stream::deflate::FlushCompress::None
                    },
                )
                .map_err(pack_error)?;
            let consumed = usize::try_from(codec.total_in() - before_in).map_err(pack_error)?;
            let written = usize::try_from(codec.total_out() - before_out).map_err(pack_error)?;
            self.operation.work(consumed)?;
            cursor.position += consumed as u64;
            self.write(&window[..written]).await?;
            if status == gix_zlib::Status::StreamEnd {
                break;
            }
            if consumed == 0 && written == 0 {
                return invalid("scratch compressor made no progress");
            }
        }
        Ok(())
    }
}

impl Scanned<'_, '_> {
    pub(crate) async fn normalize(
        self,
        provider: &mut impl BaseProvider,
    ) -> Result<(PackDescriptor, StagedObject), Error> {
        if self.entries.iter().all(|entry| entry.id.is_some()) {
            return self.finish().await;
        }
        let resolved = self.resolve(provider).await?;
        let count = self.entries.len() + resolved.external.len();
        let memory = self.input.operation.reserve(count * size_of::<Entry>())?;
        let mut entries = Vec::with_capacity(count);
        let format = self.id.format();
        let mut output = Output::new(self.input, format)?;
        output
            .write(&gix_pack::data::header::encode(
                gix_pack::data::Version::V2,
                u32::try_from(count).map_err(pack_error)?,
            ))
            .await?;
        for (position, entry) in self.entries.iter().enumerate() {
            let object = resolved.objects[position]
                .as_ref()
                .ok_or_else(|| pack_error("unresolved output object"))?;
            let header = match resolved.bases[position] {
                Some(id) => Header::RefDelta {
                    base_id: gix_hash::ObjectId::try_from(id.as_bytes()).map_err(pack_error)?,
                },
                None => entry.header.header,
            };
            let header = output
                .header(header, entry.header.decompressed_size, format)
                .await?;
            output
                .copy(self.input, entry.header.data_offset, entry.end)
                .await?;
            entries.push(Entry {
                header,
                end: output.sink.len(),
                crc: output.crc,
                id: Some(object.id),
                result_size: usize::try_from(object.input.bytes).map_err(pack_error)?,
            });
        }
        for object in &resolved.external {
            let kind = match object.kind {
                gix_object::Kind::Blob => Header::Blob,
                gix_object::Kind::Tree => Header::Tree,
                gix_object::Kind::Commit => Header::Commit,
                gix_object::Kind::Tag => Header::Tag,
            };
            let header = output.header(kind, object.input.bytes, format).await?;
            output.compress(object).await?;
            entries.push(Entry {
                header,
                end: output.sink.len(),
                crc: output.crc,
                id: Some(object.id),
                result_size: usize::try_from(object.input.bytes).map_err(pack_error)?,
            });
        }
        let id = crate::ObjectId::from_bytes(
            format,
            output.hash.try_finalize().map_err(pack_error)?.as_slice(),
        )?;
        output.sink.write(id.as_bytes()).await?;
        let input = output.sink.finish().await?;
        Scanned {
            input: &input,
            entries,
            id,
            bytes: input.bytes,
            _memory: memory,
        }
        .finish()
        .await
    }
}
