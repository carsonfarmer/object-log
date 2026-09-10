//! Bounded byte streams over immutable blobs and an authenticated reference node.

use bytes::Bytes;

use crate::{Error, Log, ObjectKind, ObjectRef, StagedObject, View};

const TAG: &[u8; 8] = b"OLBS\0\0\0\x01";
const DESCRIPTOR_BYTES: usize = 24;
const MAX_CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// An unpublished byte stream. Dropping it leaves only garbage-collectable objects.
/// A failed or cancelled write makes the writer unusable; finish never publishes a head.
#[derive(Debug)]
pub struct ByteWriter {
    log: Log,
    view: View,
    chunk_bytes: usize,
    capacity: u64,
    len: u64,
    buffer: Vec<u8>,
    children: Vec<StagedObject>,
    failed: bool,
}

/// An authenticated byte stream with one cached storage chunk.
#[derive(Debug)]
pub struct ByteReader {
    log: Log,
    view: View,
    chunk_bytes: u64,
    len: u64,
    children: Vec<ObjectRef>,
    cached: Option<(usize, Bytes)>,
}

impl Log {
    /// Begins an unpublished byte stream. Chunk geometry is chosen by the WAL.
    ///
    /// # Errors
    /// Returns a limit error if the configured object size cannot hold a stream node.
    pub fn byte_writer(&self, view: &View) -> Result<ByteWriter, Error> {
        let chunk_bytes = self.options().max_object_bytes.min(MAX_CHUNK_BYTES);
        self.node_size(DESCRIPTOR_BYTES, [])?;
        // Each reference contains a 32-byte digest. Bound the search even when a
        // caller configures an enormous reference limit.
        let mut low = 0;
        let mut high = self
            .options()
            .max_object_refs
            .min(self.options().max_object_bytes / 32);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if self
                .node_size(
                    DESCRIPTOR_BYTES,
                    std::iter::repeat_n(chunk_bytes as u64, mid),
                )
                .is_ok()
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(ByteWriter {
            log: self.clone(),
            view: view.clone(),
            chunk_bytes,
            capacity: (low as u64)
                .checked_mul(chunk_bytes as u64)
                .ok_or(Error::LimitExceeded("byte stream length"))?,
            len: 0,
            buffer: Vec::new(),
            children: Vec::new(),
            failed: false,
        })
    }

    /// Opens a byte stream from its authenticated node, without reading its payload chunks.
    ///
    /// # Errors
    /// Returns an error for a foreign or expired view, malformed geometry, or storage failure.
    pub async fn open_bytes(&self, view: &View, root: &ObjectRef) -> Result<ByteReader, Error> {
        let node = self.read_node(view, root).await?;
        let descriptor = node.payload();
        if descriptor.len() != DESCRIPTOR_BYTES || &descriptor[..8] != TAG {
            return Err(invalid());
        }
        let number = |offset| {
            descriptor[offset..offset + 8]
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| invalid())
        };
        let len = number(8)?;
        let chunk_bytes = number(16)?;
        if chunk_bytes == 0
            || chunk_bytes > self.options().max_object_bytes.min(MAX_CHUNK_BYTES) as u64
            || len.div_ceil(chunk_bytes) != node.children().len() as u64
        {
            return Err(invalid());
        }
        for (index, child) in node.children().iter().enumerate() {
            let expected = (len - index as u64 * chunk_bytes).min(chunk_bytes);
            if child.kind() != ObjectKind::Blob || child.len() != expected {
                return Err(invalid());
            }
        }
        Ok(ByteReader {
            log: self.clone(),
            view: view.clone(),
            chunk_bytes,
            len,
            children: node.children,
            cached: None,
        })
    }
}

fn invalid() -> Error {
    Error::InvalidFormat("invalid byte stream".into())
}

impl ByteWriter {
    /// Appends bytes. Success accepts all input; on error or cancellation discard the writer.
    ///
    /// # Errors
    /// Returns an error for an unusable writer, configured limits, expiry, or storage failure.
    pub async fn write(&mut self, mut data: &[u8]) -> Result<(), Error> {
        if self.failed {
            return Err(invalid());
        }
        // Set before the first await: a dropped future cannot finish a truncated stream.
        self.failed = true;
        let len = self
            .len
            .checked_add(data.len() as u64)
            .filter(|&len| len <= self.capacity)
            .ok_or(Error::LimitExceeded("byte stream length"))?;
        while !data.is_empty() {
            let n = data.len().min(self.chunk_bytes - self.buffer.len());
            self.buffer.extend_from_slice(&data[..n]);
            data = &data[n..];
            if self.buffer.len() == self.chunk_bytes {
                let child = self
                    .log
                    .put_object(&self.view, Bytes::from(std::mem::take(&mut self.buffer)))
                    .await?;
                self.children.push(child);
            }
        }
        self.len = len;
        self.failed = false;
        Ok(())
    }

    /// Finishes the stream, returning one ordinary publication proof.
    /// The caller can publish this root or discard it as temporary storage.
    ///
    /// # Errors
    /// Returns an error for a failed write, stale proof, configured limit, or storage failure.
    pub async fn finish(mut self) -> Result<StagedObject, Error> {
        if self.failed {
            return Err(invalid());
        }
        if !self.buffer.is_empty() {
            self.children.push(
                self.log
                    .put_object(&self.view, Bytes::from(self.buffer))
                    .await?,
            );
        }
        let mut descriptor = Vec::with_capacity(DESCRIPTOR_BYTES);
        descriptor.extend_from_slice(TAG);
        descriptor.extend_from_slice(&self.len.to_be_bytes());
        descriptor.extend_from_slice(&(self.chunk_bytes as u64).to_be_bytes());
        self.log
            .put_node(&self.view, Bytes::from(descriptor), self.children)
            .await
    }
}

impl ByteReader {
    /// Returns the logical byte length, distinct from the root's encoded storage length.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Reports whether the stream contains no bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reads at most `max_len` bytes from an offset, stopping at the next chunk boundary.
    /// Zero-length requests and offsets at or beyond EOF return empty bytes.
    /// Each storage read authenticates a complete chunk; consecutive reads reuse one cache.
    ///
    /// # Errors
    /// Returns an error for an expired view, corrupt chunk, or storage failure.
    pub async fn read_at(&mut self, offset: u64, max_len: usize) -> Result<Bytes, Error> {
        if max_len == 0 || offset >= self.len {
            return Ok(Bytes::new());
        }
        let index = usize::try_from(offset / self.chunk_bytes).map_err(|_| invalid())?;
        if self
            .cached
            .as_ref()
            .is_none_or(|(cached, _)| *cached != index)
        {
            self.cached = None;
            let bytes = self
                .log
                .read_object(&self.view, &self.children[index])
                .await?;
            self.cached = Some((index, bytes));
        }
        let (_, bytes) = self.cached.as_ref().ok_or_else(invalid)?;
        let start = usize::try_from(offset % self.chunk_bytes).map_err(|_| invalid())?;
        Ok(bytes.slice(start..start + max_len.min(bytes.len() - start)))
    }
}

#[cfg(test)]
#[path = "byte_stream_tests.rs"]
mod tests;
