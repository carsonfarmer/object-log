//! Bounded host-side receive decoding. Engine admission precedes this producer.

use std::io;

use async_compression::futures::bufread::GzipDecoder;
use bytes::Bytes;
use futures::{
    Stream, StreamExt, TryStreamExt,
    future::Either,
    io::{AsyncRead, AsyncReadExt, BufReader, Cursor},
};
use object_log_git::Error;

const FRAME: usize = 64 * 1024;

fn invalid() -> Error {
    Error::InvalidProtocol("invalid or oversized request body")
}

fn reader<S>(input: S, gzip: bool, limit: usize) -> impl AsyncRead + Unpin
where
    S: Stream<Item = io::Result<Vec<u8>>> + Unpin,
{
    let mut encoded = 0_usize;
    // Spin SDK reads at most 16 KiB at a time. Bound adapters/test producers too;
    // neither an oversized frame nor its excess capacity may remain buffered.
    let input = input
        .map(move |chunk| {
            let chunk = chunk?;
            if chunk.is_empty() || chunk.capacity() > FRAME || chunk.len() > limit - encoded {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid encoded body frame",
                ));
            }
            encoded += chunk.len();
            Ok(chunk)
        })
        .into_async_read();
    let reader = BufReader::with_capacity(FRAME, input);
    if gzip {
        let mut decoder = GzipDecoder::new(reader);
        decoder.multiple_members(true);
        Either::Left(decoder)
    } else {
        Either::Right(reader)
    }
}

/// `None` is exactly Git's bare-flush authentication probe. Inspect at most five
/// decoded bytes, then hand a backpressured stream to the engine. Encoded and
/// decoded limits remain independent, including concatenated gzip members.
pub(super) async fn frames<S>(
    input: S,
    gzip: bool,
) -> Result<Option<impl Stream<Item = Result<Bytes, Error>> + Unpin>, Error>
where
    S: Stream<Item = io::Result<Vec<u8>>> + Unpin,
{
    frames_with_limit(input, gzip, super::RECEIVE_BODY_LIMIT).await
}

async fn frames_with_limit<S>(
    input: S,
    gzip: bool,
    limit: usize,
) -> Result<Option<impl Stream<Item = Result<Bytes, Error>> + Unpin>, Error>
where
    S: Stream<Item = io::Result<Vec<u8>>> + Unpin,
{
    let mut reader = reader(input, gzip, limit);
    let mut prefix = vec![0; 5];
    let mut length = 0;
    while length < prefix.len() {
        let count = reader
            .read(&mut prefix[length..])
            .await
            .map_err(|_| invalid())?;
        if count == 0 {
            break;
        }
        length += count;
    }
    prefix.truncate(length);
    if prefix == b"0000" {
        return Ok(None);
    }
    let reader = Cursor::new(prefix).chain(reader);
    Ok(Some(Box::pin(futures::stream::unfold(
        (reader, 0_usize, false),
        move |(mut reader, total, done)| async move {
            if done {
                return None;
            }
            let mut frame = vec![0; FRAME];
            match reader.read(&mut frame).await {
                Ok(0) => None,
                Ok(length) if length <= limit - total => {
                    frame.truncate(length);
                    Some((Ok(Bytes::from(frame)), (reader, total + length, false)))
                }
                Ok(_) | Err(_) => Some((Err(invalid()), (reader, total, true))),
            }
        },
    ))))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn chunks(input: &[u8]) -> impl Stream<Item = io::Result<Vec<u8>>> + Unpin {
        futures::stream::iter(
            input
                .chunks(16 * 1024)
                .map(|chunk| Ok(chunk.to_vec()))
                .collect::<Vec<_>>(),
        )
    }

    fn gzip(bytes: &[u8]) -> io::Result<Vec<u8>> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes)?;
        encoder.finish()
    }

    const TEST_LIMIT: usize = FRAME * 2;

    async fn decoded(input: &[u8], gzip: bool) -> Result<Option<Vec<u8>>, Error> {
        let Some(frames) = frames_with_limit(chunks(input), gzip, TEST_LIMIT).await? else {
            return Ok(None);
        };
        let chunks = frames.try_collect::<Vec<_>>().await?;
        assert!(chunks.iter().all(|chunk| chunk.len() <= FRAME));
        Ok(Some(chunks.concat()))
    }

    #[tokio::test]
    async fn probes_and_concatenated_members_preserve_exact_decoded_bytes() -> anyhow::Result<()> {
        for compressed in [false, true] {
            for content in [b"0000".as_slice(), b"0000x", b"0010hello", b""] {
                let encoded = if compressed {
                    gzip(content)?
                } else {
                    content.to_vec()
                };
                assert_eq!(
                    decoded(&encoded, compressed).await?,
                    if content == b"0000" {
                        None
                    } else {
                        Some(content.to_vec())
                    }
                );
            }
        }
        let mut compressed = gzip(b"first")?;
        compressed.extend(gzip(b"second")?);
        assert_eq!(
            decoded(&compressed, true).await?,
            Some(b"firstsecond".to_vec())
        );
        Ok(())
    }

    #[tokio::test]
    async fn checks_both_limits_truncation_and_late_crc() -> anyhow::Result<()> {
        assert!(decoded(&vec![b'x'; TEST_LIMIT + 1], false).await.is_err());
        assert!(
            decoded(&gzip(&vec![b'x'; TEST_LIMIT + 1])?, true)
                .await
                .is_err()
        );
        let good = gzip(&vec![b'x'; FRAME * 2])?;
        assert!(decoded(&good[..good.len() - 1], true).await.is_err());
        let mut corrupt = good;
        let last = corrupt.len() - 8;
        corrupt[last] ^= 1;
        assert!(decoded(&corrupt, true).await.is_err());
        let mut compressed = gzip(b"0000")?;
        compressed.pop();
        assert!(decoded(&compressed, true).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn decoder_does_not_collect_ahead_and_drop_releases_the_producer() -> anyhow::Result<()> {
        struct Held(Arc<AtomicUsize>);
        impl Drop for Held {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let hold = Held(drops.clone());
        let source = futures::stream::poll_fn(move |_| {
            let _ = &hold;
            count.fetch_add(1, Ordering::Relaxed);
            std::task::Poll::Ready(Some(Ok(vec![b'x'; 16 * 1024])))
        });
        let mut frames = frames(source, false)
            .await?
            .ok_or_else(|| io::Error::other("unexpected probe"))?;
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(
            frames.next().await.transpose()?.as_deref(),
            Some(b"xxxxx".as_slice())
        );
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        drop(frames);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        Ok(())
    }
}
