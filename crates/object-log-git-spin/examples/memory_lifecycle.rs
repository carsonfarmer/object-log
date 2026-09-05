//! Single-invocation WASI acceptance. `InMemory` is test storage, not persistent hosting.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[allow(dead_code)]
#[path = "../src/transport.rs"]
mod transport;

use bytes::Bytes;
use futures::StreamExt;
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, Log, LogId, Options, Resolution,
    TransactionId, ValidatedBackend,
};
use object_log_git::{Error, ObjectFormat, Repository};
use object_store::path::Path;
#[path = "../../object-log-git/tests/shared_performance/timed_store.rs"]
mod timed_store;
use spin_sdk::http::{Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};
use std::{sync::Arc, time::Instant};
use timed_store::{TimedStore, serial_depth};

const INPUT_LIMIT: usize = 10 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 20 * 1024 * 1024;

fn frame(output: &mut Vec<u8>, bytes: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        bytes.len() + 4 <= OUTPUT_LIMIT - output.len(),
        "fixture output limit"
    );
    output.extend_from_slice(&u32::try_from(bytes.len())?.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}
fn decode(input: &Bytes) -> anyhow::Result<(ObjectFormat, [Bytes; 6])> {
    anyhow::ensure!(input.get(..4) == Some(b"OLM1"), "fixture magic");
    let format = match input.get(4) {
        Some(1) => ObjectFormat::Sha1,
        Some(2) => ObjectFormat::Sha256,
        _ => anyhow::bail!("fixture hash"),
    };
    let mut cursor = 5;
    let mut frames = std::array::from_fn(|_| Bytes::new());
    for frame in &mut frames {
        let encoded = input
            .get(cursor..cursor + 4)
            .ok_or_else(|| anyhow::anyhow!("fixture length"))?;
        let length = usize::try_from(u32::from_be_bytes(encoded.try_into()?))?;
        cursor += 4;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("fixture overflow"))?;
        anyhow::ensure!(end <= input.len(), "fixture truncation");
        *frame = input.slice(cursor..end);
        cursor = end;
    }
    anyhow::ensure!(cursor == input.len(), "fixture trailing data");
    Ok((format, frames))
}
// Fixture input stays outside engine accounting. Copy one frame on demand so
// the serving receive API never gets a slice retaining the full test envelope.
fn receive_frames(input: &Bytes) -> impl futures::Stream<Item = Result<Bytes, Error>> + Unpin + '_ {
    futures::stream::iter(
        input
            .chunks(64 * 1024)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
    )
}

async fn publish(log: &Log, format: ObjectFormat, input: Bytes) -> anyhow::Result<Bytes> {
    let prepared = Repository::open(log, format)
        .await?
        .prepare_receive_stream(TransactionId::new(), receive_frames(&input))
        .await?;
    let (resolution, response) = prepared.publish_receive().await?;
    anyhow::ensure!(
        matches!(resolution, Resolution::Committed(_)),
        "fixture publication failed"
    );
    Ok(response)
}
fn io_measurement(store: &TimedStore) -> anyhow::Result<String> {
    let metrics = store.faults.metrics();
    let intervals = store.intervals();
    let calls = metrics.total_requests();
    let transfer = metrics.downloaded_bytes() + metrics.uploaded_bytes();
    anyhow::ensure!(intervals.len() as u64 == calls, "untimed operation");
    anyhow::ensure!(calls <= 512 && transfer <= 96 * 1024 * 1024, "I/O limits");
    Ok(format!(
        "{{\"calls\":{calls},\"transfer_bytes\":{transfer},\"serial_depth\":{},\"intervals_ns\":[{}]}}",
        serial_depth(&intervals),
        intervals
            .iter()
            .map(|(start, end)| format!("[{start},{end}]"))
            .collect::<Vec<_>>()
            .join(",")
    ))
}
#[allow(clippy::too_many_lines)] // Keep the acceptance lifecycle order explicit.
async fn lifecycle(input: Bytes, timings: bool) -> anyhow::Result<Vec<u8>> {
    let (
        format,
        [
            initial,
            first_fetch,
            incremental,
            final_fetch,
            have_fetch,
            rejected,
        ],
    ) = decode(&input)?;
    drop(input);
    // Test provider objects, envelope and aggregate responses live outside the Git
    // pool. Every command still uses the unchanged shared-engine admission limits.
    let bootstrap = Instant::now();
    let store = TimedStore::new();
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("wasip2-memory")).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let bootstrap_ns = bootstrap.elapsed().as_nanos();
    let mut measured = [0_u128; 5];
    let mut io: [String; 5] = std::array::from_fn(|_| "null".to_owned());
    let mut output = Vec::with_capacity(OUTPUT_LIMIT);
    store.reset();
    let started = Instant::now();
    let response = publish(&log, format, initial).await?;
    measured[0] = started.elapsed().as_nanos();
    if timings {
        io[0] = io_measurement(&store)?;
    }
    frame(&mut output, &response)?;
    drop(response);
    // Copy each result into the bounded test envelope, then drop its original
    // accounted Bytes before starting the next operation.
    store.reset();
    let started = Instant::now();
    let response = Repository::open(&log, format)
        .await?
        .upload_pack(first_fetch)
        .await?;
    measured[1] = started.elapsed().as_nanos();
    if timings {
        io[1] = io_measurement(&store)?;
    }
    frame(&mut output, &response)?;
    drop(response);
    if incremental.is_empty() {
        frame(&mut output, &[])?;
    } else {
        store.reset();
        let started = Instant::now();
        let response = publish(&log, format, incremental).await?;
        measured[2] = started.elapsed().as_nanos();
        if timings {
            io[2] = io_measurement(&store)?;
        }
        frame(&mut output, &response)?;
        drop(response);
    }
    if have_fetch.is_empty() {
        frame(&mut output, &[])?;
    } else {
        store.reset();
        let started = Instant::now();
        let response = Repository::open(&log, format)
            .await?
            .upload_pack(have_fetch)
            .await?;
        measured[3] = started.elapsed().as_nanos();
        if timings {
            io[3] = io_measurement(&store)?;
        }
        frame(&mut output, &response)?;
        drop(response);
    }
    let before = Repository::open(&log, format).await?.refs().clone();
    match Repository::open(&log, format)
        .await?
        .prepare_receive_stream(TransactionId::new(), receive_frames(&rejected))
        .await
    {
        Err(Error::ReceiveRejected { response, .. }) => frame(&mut output, &response)?,
        Err(error) => anyhow::bail!("unexpected rejection: {error:#}"),
        Ok(_) => anyhow::bail!("stale update was not rejected"),
    }
    anyhow::ensure!(
        Repository::open(&log, format).await?.refs() == &before,
        "rejection changed refs"
    );
    let view = log.load().await?;
    let orphan = log
        .put_object(
            &view,
            Bytes::from_static(b"unpublished memory fixture object"),
        )
        .await?;
    let CheckpointStatus::Published(view) =
        Repository::open(&log, format).await?.checkpoint().await?
    else {
        anyhow::bail!("checkpoint not published");
    };
    anyhow::ensure!(view.tail().is_empty(), "checkpoint retained tail");
    let CollectionStart::Installed(fenced, started) = log.start_collection(&view).await? else {
        anyhow::bail!("collection not installed");
    };
    let CollectionFinish::Complete(current, finished) = log.resume_collection(&fenced).await?
    else {
        anyhow::bail!("collection not complete");
    };
    anyhow::ensure!(
        started.candidate_count() > 0 && started.candidate_count() == finished.delete_attempts(),
        "collection did not delete candidates"
    );
    anyhow::ensure!(
        log.read_object(&current, orphan.reference()).await.is_err(),
        "orphan survived collection"
    );
    // Recovery must also survive a fresh core staging domain.
    drop(log);
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    store.reset();
    let started = Instant::now();
    let response = Repository::open(&log, format)
        .await?
        .upload_pack(final_fetch)
        .await?;
    measured[4] = started.elapsed().as_nanos();
    if timings {
        io[4] = io_measurement(&store)?;
    }
    frame(&mut output, &response)?;
    drop(response);
    frame(
        &mut output,
        format!(
            "checkpoint and collection passed: {} candidates\n",
            finished.delete_attempts()
        )
        .as_bytes(),
    )?;
    if timings {
        frame(&mut output, format!(
            "{{\"bootstrap_ns\":{bootstrap_ns},\"initial_push_ns\":{},\"initial_fetch_ns\":{},\"thin_push_ns\":{},\"incremental_fetch_ns\":{},\"recovered_fetch_ns\":{},\"io\":[{},{},{},{},{}]}}",
            measured[0], measured[1], measured[2], measured[3], measured[4], io[0], io[1], io[2], io[3], io[4]
        ).as_bytes())?;
    }
    Ok(output)
}

#[allow(unsafe_code, clippy::same_length_and_capacity)]
mod entry {
    use super::{
        Bytes, Fields, INPUT_LIMIT, IncomingRequest, OutgoingBody, OutgoingResponse,
        ResponseOutparam, StreamExt, lifecycle, transport,
    };
    #[cfg_attr(target_arch = "wasm32", spin_sdk::http_component)]
    async fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let timings = request.path_with_query().as_deref() == Some("/performance");
        let result = async {
            let mut input = Vec::with_capacity(INPUT_LIMIT);
            let mut stream = request.into_body_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                anyhow::ensure!(
                    chunk.len() <= INPUT_LIMIT - input.len(),
                    "fixture input limit"
                );
                input.extend_from_slice(&chunk);
            }
            lifecycle(Bytes::from(input.into_boxed_slice()), timings).await
        }
        .await;
        let (status, bytes) = match result {
            Ok(bytes) => (200, bytes),
            Err(error) => (
                500,
                format!("memory lifecycle failed: {error:#}\n").into_bytes(),
            ),
        };
        if let Err(error) = send(out, status, bytes).await {
            eprintln!("fixture response: {error:#}");
        }
    }
    async fn send(out: ResponseOutparam, status: u16, bytes: Vec<u8>) -> anyhow::Result<()> {
        let response = OutgoingResponse::new(Fields::from_list(&[(
            "content-length".into(),
            bytes.len().to_string().into_bytes(),
        )])?);
        response
            .set_status_code(status)
            .map_err(|()| anyhow::anyhow!("fixture status"))?;
        let body = response
            .body()
            .map_err(|()| anyhow::anyhow!("fixture body"))?;
        let output = body
            .write()
            .map_err(|()| anyhow::anyhow!("fixture output"))?;
        out.set(response);
        transport::write_chunk(&output, &bytes).await?;
        drop(output);
        OutgoingBody::finish(body, None)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, receive_frames};
    use bytes::Bytes;
    use futures::StreamExt;

    #[test]
    fn receive_frames_do_not_retain_the_fixture_envelope() -> anyhow::Result<()> {
        let input = Bytes::from(vec![7; 2 * 64 * 1024 + 1]);
        let backing = input.as_ptr() as usize..input.as_ptr() as usize + input.len();
        let mut frames = receive_frames(&input);
        let mut total = 0;
        while let Some(frame) = futures::executor::block_on(frames.next()) {
            let frame = frame?;
            assert!(frame.len() <= 64 * 1024);
            assert!(!backing.contains(&(frame.as_ptr() as usize)));
            assert!(frame.iter().all(|byte| *byte == 7));
            total += frame.len();
        }
        assert_eq!(total, input.len());
        Ok(())
    }

    #[test]
    fn envelope_rejects_truncation_trailing_data_and_large_lengths() -> anyhow::Result<()> {
        let mut input = b"OLM1\x01".to_vec();
        input.extend_from_slice(&[0; 24]);
        assert!(
            decode(&Bytes::copy_from_slice(&input))?
                .1
                .iter()
                .all(Bytes::is_empty)
        );
        assert!(decode(&Bytes::copy_from_slice(&input[..input.len() - 1])).is_err());
        input.push(0);
        assert!(decode(&Bytes::copy_from_slice(&input)).is_err());
        input.pop();
        input[5..9].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&Bytes::from(input)).is_err());
        Ok(())
    }
}
