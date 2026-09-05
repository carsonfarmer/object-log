//! Selected authenticated pack ranges feeding the same bounded scratch decoder.

use super::{Cursor, Decoded, IndexedEntry, Input, scan, scratch};
use crate::{
    Error, ObjectId,
    pack::{MAX_OBJECT_BYTES, budget::Operation, invalid, pack_error},
};
use object_log::{Log, ObjectRef, StagedObject, View};
use std::{mem::size_of, sync::Arc};

impl<'a> Input<'a> {
    /// Construct a read-only cursor from children of an authenticated node.
    /// References convey no staging proof and are never publication roots.
    pub(crate) fn read_only(
        operation: &Operation,
        log: &'a Log,
        view: &'a View,
        node: &object_log::ReferenceNode,
        bytes: u64,
        width: usize,
    ) -> Result<Self, Error> {
        if width == 0 || width > super::FRAME_BYTES {
            return invalid("read-only pack width is invalid");
        }
        let length = usize::try_from(bytes).map_err(pack_error)?;
        let count = length.div_ceil(width);
        if count != node.children().len() || count > log.options().max_object_refs {
            return invalid("read-only pack geometry mismatch");
        }
        for (index, child) in node.children().iter().enumerate() {
            let expected = (length - index * width).min(width);
            if child.kind() != object_log::ObjectKind::Blob || child.len() != expected as u64 {
                return invalid("read-only chunk geometry mismatch");
            }
        }
        let memory = operation.reserve(count * size_of::<ObjectRef>() + 2 * size_of::<usize>())?;
        operation.work(count * size_of::<ObjectRef>())?;
        Ok(Self {
            log,
            view,
            operation: operation.clone(),
            context: Arc::new(()),
            chunks: Vec::new(),
            read_refs: Some(node.children().to_vec().into_boxed_slice()),
            inline: None,
            cache: std::sync::Mutex::new(None),
            bytes,
            width,
            maximum: count,
            memory,
        })
    }

    pub(crate) fn operation(&self) -> &Operation {
        &self.operation
    }
    pub(crate) fn log(&self) -> &'a Log {
        self.log
    }
    pub(crate) fn view(&self) -> &'a View {
        self.view
    }

    pub(crate) fn matches_context(&self, operation: &Operation, log: &Log, view: &View) -> bool {
        self.operation.same_as(operation)
            && std::ptr::eq(self.log, log)
            && std::ptr::eq(self.view, view)
    }

    /// Derive children from a verified same-domain staged root, under this exact
    /// attempt context. This neither refreshes a view nor leases its objects.
    pub(crate) async fn stored_pack(
        &self,
        root: &StagedObject,
        bytes: u64,
        width: usize,
    ) -> Result<Input<'a>, Error> {
        if width == 0 || width > super::FRAME_BYTES {
            return invalid("stored pack width is invalid");
        }
        let bytes_usize = usize::try_from(bytes).map_err(pack_error)?;
        let count = bytes_usize.div_ceil(width);
        if count > self.log.options().max_object_refs {
            return invalid("stored pack has too many chunks");
        }
        let memory = self.operation.reserve(
            count * (size_of::<StagedObject>() + size_of::<ObjectRef>()) + 2 * size_of::<usize>(),
        )?;
        let root_bytes = usize::try_from(root.reference().len()).map_err(pack_error)?;
        let _read_memory = self.operation.reserve(
            root_bytes + (root_bytes / 58) * (size_of::<ObjectRef>() + size_of::<StagedObject>()),
        )?;

        self.operation.work(root_bytes)?;
        let (_payload, chunks) = self.log.read_staged_node(self.view, root).await?;
        if chunks.len() != count {
            return invalid("stored pack chunk count mismatch");
        }
        for (index, child) in chunks.iter().enumerate() {
            let expected = if index + 1 == count {
                bytes_usize - index * width
            } else {
                width
            };
            if child.reference().kind() != object_log::ObjectKind::Blob
                || child.reference().len() != expected as u64
            {
                return invalid("stored pack chunk geometry mismatch");
            }
        }
        Ok(Input {
            log: self.log,
            view: self.view,
            operation: self.operation.clone(),
            context: Arc::new(()),
            chunks,
            read_refs: None,
            inline: None,
            cache: std::sync::Mutex::new(None),
            bytes,
            width,
            maximum: count,
            memory,
        })
    }

    pub(crate) async fn indexed_entry(
        &self,
        start: u64,
        end: u64,
        id: ObjectId,
        crc: u32,
    ) -> Result<IndexedEntry, Error> {
        if start < 12
            || end <= start
            || end > self.bytes.saturating_sub(id.format().digest_len() as u64)
        {
            return invalid("selected entry range is invalid");
        }
        let mut cursor = Cursor::new(self);
        cursor.position = start;
        let (header, _) = scan::read_header(&mut cursor, end, id.format()).await?;
        let result_size = usize::try_from(header.decompressed_size).map_err(pack_error)?;
        let mut entry = IndexedEntry {
            header,
            end,
            crc,
            id: Some(id),
            result_size,
        };
        if entry.header.header.is_delta() {
            let mut decoder = scratch::Inflated::new(self, &entry)?;
            decoder.integer().await?;
            entry.result_size = decoder.integer().await?;
            if entry.result_size > MAX_OBJECT_BYTES {
                return invalid("selected delta result exceeds limit");
            }
        }
        Ok(entry)
    }

    pub(crate) async fn decode_chain(
        &self,
        encoded: &Input<'_>,
        chain: &[IndexedEntry],
    ) -> Result<Decoded<'a>, Error> {
        if !self.matches_context(&encoded.operation, encoded.log, encoded.view) {
            return invalid("selected input belongs to another context");
        }
        let mut base = None;
        for entry in chain.iter().rev() {
            let id = entry
                .id
                .ok_or_else(|| pack_error("selected entry ID is missing"))?;
            base =
                Some(scratch::decode_from(self, encoded, entry, id.format(), base.as_ref()).await?);
        }
        let mut base = base.ok_or_else(|| pack_error("selected chain is empty"))?;
        // A repository thin base is emitted as a full object in normalized output.
        base.depth = 0;
        Ok(base)
    }
}
