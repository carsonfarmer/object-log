//! Bound control retention and hand the uncollected pack tail to ingestion.

use super::FRAME_BYTES;
use crate::{
    Error,
    pack::{
        budget::{Operation, Reservation, hold},
        invalid,
    },
    wire::MAX_RECEIVE_BYTES,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use gix_packetline::{PacketLineRef, decode};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct Controls<S> {
    /// Includes the first flush. Semantic command validation remains in wire.
    pub(crate) bytes: Bytes,
    pub(crate) pack: PackFrames<S>,
}

pub(crate) struct PackFrames<S> {
    first: Option<Bytes>,
    first_memory: Option<Reservation>,
    stream: S,
}

impl<S: Stream<Item = Result<Bytes, Error>> + Unpin> Stream for PackFrames<S> {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(bytes) = self.first.take() {
            // The downstream bounded-frame consumer assumes allocation ownership
            // at yield and reserves the frame before processing it.
            self.first_memory = None;
            return Poll::Ready(Some(Ok(bytes)));
        }
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Each yielded frame must own backing allocation <= `FRAME_BYTES`. Producer-side
/// allocations remain producer-accounted before yield, as with `Input::receive`.
/// Stops at the first flush without polling the next frame or collecting PACK.
pub(crate) async fn split<S: Stream<Item = Result<Bytes, Error>> + Unpin>(
    operation: &Operation,
    mut stream: S,
) -> Result<Controls<S>, Error> {
    let memory = operation.reserve_state(MAX_RECEIVE_BYTES)?;
    let mut controls = Vec::with_capacity(MAX_RECEIVE_BYTES);
    let mut start = 0;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        if frame.is_empty() || frame.len() > FRAME_BYTES {
            return invalid("invalid receive frame");
        }
        let frame_memory = operation.reserve(FRAME_BYTES)?;
        let mut offset = 0;
        loop {
            operation.work(4)?;
            match decode::streaming(&controls[start..])
                .map_err(|_| Error::InvalidProtocol("invalid packet line"))?
            {
                decode::Stream::Incomplete { bytes_needed } => {
                    if offset == frame.len() {
                        break;
                    }
                    let count = bytes_needed.min(frame.len() - offset);
                    if count > MAX_RECEIVE_BYTES - controls.len() {
                        return Err(Error::InvalidProtocol("control bytes"));
                    }
                    operation.work(count)?;
                    controls.extend_from_slice(&frame[offset..offset + count]);
                    offset += count;
                }
                decode::Stream::Complete {
                    line: PacketLineRef::Data(_),
                    bytes_consumed,
                } => {
                    start += bytes_consumed;
                }
                decode::Stream::Complete {
                    line: PacketLineRef::Flush,
                    ..
                } => {
                    let first = (offset < frame.len()).then(|| frame.slice(offset..));
                    let first_memory = first.as_ref().map(|_| frame_memory);
                    return Ok(Controls {
                        bytes: hold(controls.into(), memory),
                        pack: PackFrames {
                            first,
                            first_memory,
                            stream,
                        },
                    });
                }
                decode::Stream::Complete { .. } => {
                    return Err(Error::InvalidProtocol("unexpected packet delimiter"));
                }
            }
        }
    }
    Err(Error::InvalidProtocol("truncated receive controls"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::budget::Pool;
    use futures::stream;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn splits_every_boundary_and_preserves_pack_bytes() -> Result<(), Error> {
        let data = b"0009hello0009world0000PACKtail";
        for width in 1..=data.len() {
            let operation = Pool::new(3 * FRAME_BYTES).admit()?;
            let frames = stream::iter(
                data.chunks(width)
                    .map(|bytes| Ok(Bytes::copy_from_slice(bytes))),
            );
            let mut split = split(&operation, frames).await?;
            assert_eq!(split.bytes.as_ref(), b"0009hello0009world0000");
            let mut tail = Vec::new();
            while let Some(bytes) = split.pack.next().await {
                tail.extend_from_slice(&bytes?);
            }
            assert_eq!(tail, b"PACKtail");
            drop(split);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn stops_polling_at_flush_and_releases_cancelled_tail() -> Result<(), Error> {
        let operation = Pool::new(3 * FRAME_BYTES).admit()?;
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = polls.clone();
        let frames = stream::iter([b"0000PACK".as_slice(), b"tail"]).map(move |bytes| {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::copy_from_slice(bytes))
        });
        let split = split(&operation, frames).await?;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(split.bytes.as_ref(), b"0000"); // Authentication probe is a caller decision.
        drop(split);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn accepts_exact_control_limit_and_propagates_tail_failure() -> Result<(), Error> {
        let mut controls = b"0005x".repeat((MAX_RECEIVE_BYTES - 4) / 5 - 1);
        controls.extend_from_slice(b"0007xyz0000");
        assert_eq!(controls.len(), MAX_RECEIVE_BYTES);
        let operation = Pool::new(3 * FRAME_BYTES).admit()?;
        let frames = stream::iter([
            Ok(Bytes::from(controls)),
            Err(Error::InvalidProtocol("producer failed")),
        ]);
        let mut split = split(&operation, frames).await?;
        assert_eq!(split.bytes.len(), MAX_RECEIVE_BYTES);
        assert!(matches!(
            split.pack.next().await,
            Some(Err(Error::InvalidProtocol("producer failed")))
        ));
        drop(split);
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_bad_boundaries_and_oversized_controls() -> Result<(), Error> {
        for bytes in [
            b"0001".as_slice(),
            b"0002",
            b"0003",
            b"0004",
            b"ffff",
            b"zzzz",
            b"0009x",
            b"",
        ] {
            let operation = Pool::new(3 * FRAME_BYTES).admit()?;
            assert!(
                split(
                    &operation,
                    stream::iter([Ok(Bytes::copy_from_slice(bytes))])
                )
                .await
                .is_err()
            );
            assert_eq!(operation.live_bytes(), 0);
        }
        let operation = Pool::new(3 * FRAME_BYTES).admit()?;
        let frames =
            stream::iter((0..=MAX_RECEIVE_BYTES / 5).map(|_| Ok(Bytes::from_static(b"0005x"))));
        assert!(split(&operation, frames).await.is_err());
        assert_eq!(operation.live_bytes(), 0);
        Ok(())
    }
}
