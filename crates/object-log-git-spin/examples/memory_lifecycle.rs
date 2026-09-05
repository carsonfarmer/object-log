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
use object_store::{memory::InMemory, path::Path};
use spin_sdk::http::{Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam};
use std::sync::Arc;

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
async fn publish(log: &Log, format: ObjectFormat, input: Bytes) -> anyhow::Result<Bytes> {
    let prepared = Repository::open(log, format)
        .await?
        .prepare_receive(TransactionId::new(), input)
        .await?;
    let (resolution, response) = prepared.publish_receive().await?;
    anyhow::ensure!(
        matches!(resolution, Resolution::Committed(_)),
        "fixture publication failed"
    );
    Ok(response)
}
#[allow(clippy::too_many_lines)] // Keep the acceptance lifecycle order explicit.
async fn lifecycle(input: Bytes) -> anyhow::Result<Vec<u8>> {
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
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("wasip2-memory")).await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    let mut output = Vec::with_capacity(OUTPUT_LIMIT);
    frame(&mut output, &publish(&log, format, initial).await?)?;
    // Copy each result into the bounded test envelope, then drop its original
    // accounted Bytes before starting the next operation.
    frame(
        &mut output,
        &Repository::open(&log, format)
            .await?
            .upload_pack(first_fetch)
            .await?,
    )?;
    if incremental.is_empty() {
        frame(&mut output, &[])?;
    } else {
        frame(&mut output, &publish(&log, format, incremental).await?)?;
    }
    if have_fetch.is_empty() {
        frame(&mut output, &[])?;
    } else {
        frame(
            &mut output,
            &Repository::open(&log, format)
                .await?
                .upload_pack(have_fetch)
                .await?,
        )?;
    }
    let before = Repository::open(&log, format).await?.refs().clone();
    match Repository::open(&log, format)
        .await?
        .prepare_receive(TransactionId::new(), rejected)
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
    frame(
        &mut output,
        &Repository::open(&log, format)
            .await?
            .upload_pack(final_fetch)
            .await?,
    )?;
    frame(
        &mut output,
        format!(
            "checkpoint and collection passed: {} candidates\n",
            finished.delete_attempts()
        )
        .as_bytes(),
    )?;
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
            lifecycle(Bytes::from(input.into_boxed_slice())).await
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
    use super::decode;
    use bytes::Bytes;

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
