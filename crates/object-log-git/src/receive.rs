//! Thin-pack normalization against one authenticated repository view.
//!
//! gix reports one unresolved REF base, which can itself be a blocked in-pack
//! delta. Try available authenticated header candidates only after that error.
//! Once traversal succeeds, discard supplied bases also produced by the input
//! and normalize again. Original duplicates still fail after supplied bases
//! are removed. Every acquisition or removal consumes the same round budget;
//! every normalization attempt retains the existing work and memory limits.
//! Transient redundant candidates can therefore cause conservative rejection
//! near those limits; this helper never raises them to fit a final result.

use std::mem::size_of;

use crate::{
    Error, ObjectFormat, ObjectId,
    durable::{Object, Reader},
    pack::{
        self, ExternalBase, NormalizeError, Normalized,
        budget::{Operation, THIN_ROUNDS},
    },
};

/// The caller holds the input's memory reservation throughout this operation.
pub(crate) async fn normalize(
    operation: &Operation,
    format: ObjectFormat,
    input: &[u8],
    reader: &mut Reader<'_>,
) -> Result<Normalized, Error> {
    let _memory = operation.reserve(
        THIN_ROUNDS
            * (size_of::<ObjectId>()
                + size_of::<(ObjectId, Object)>()
                + size_of::<ExternalBase<'_>>()),
    )?;
    let mut bases: Vec<(ObjectId, Object)> = Vec::with_capacity(THIN_ROUNDS);
    let mut attempted = Vec::with_capacity(THIN_ROUNDS);
    loop {
        let external = bases
            .iter()
            .map(|(id, object)| ExternalBase {
                id: *id,
                kind: object.kind,
                data: &object.data,
            })
            .collect::<Vec<_>>();
        match pack::normalize_attempt(operation, format, input, &external) {
            Ok(normalized) => return Ok(normalized),
            Err(NormalizeError::Invalid(error)) => return Err(error),
            Err(NormalizeError::DuplicateObject(id)) => {
                let Some(index) = bases.iter().position(|(supplied, _)| *supplied == id) else {
                    return Err(Error::InvalidPack(
                        "pack contains duplicate object IDs".into(),
                    ));
                };
                operation.thin_round()?;
                bases.remove(index);
            }
            Err(NormalizeError::MissingBase {
                id,
                candidates,
                message,
                _memory: candidate_memory,
            }) => {
                // gix can report a blocked in-pack intermediate before the external root.
                // Catalog probes do no I/O; only acquire an unseen, available candidate.
                let id = std::iter::once(id)
                    .chain(candidates)
                    .find(|id| !attempted.contains(id) && reader.contains(*id))
                    .ok_or_else(|| Error::InvalidPack(message))?;
                drop(candidate_memory);
                operation.thin_round()?;
                let object = reader.find(id).await?.ok_or_else(|| {
                    Error::InvalidPack("thin-pack base disappeared from its view".into())
                })?;
                // Reader-owned Bytes carry their reservation across normalization attempts.
                attempted.push(id);
                bases.push((id, object));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use gix_pack::data::{
        Version,
        entry::Header,
        input::{EntriesToBytesIter, Entry},
    };
    use object_log::{
        Log, LogId, Options, ValidatedBackend, View,
        sim::{FaultStore, Operation as StoreOperation},
    };
    use object_store::{memory::InMemory, path::Path};

    use super::*;
    use crate::{
        durable,
        pack::{
            budget::{LIVE_BYTES, Pool, WORK_BYTES},
            object_hash,
        },
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn oid(format: ObjectFormat, data: &[u8]) -> TestResult<ObjectId> {
        Ok(ObjectId::from_bytes(
            format,
            gix_object::compute_hash(object_hash(format), gix_object::Kind::Blob, data)?.as_slice(),
        )?)
    }

    fn blob(format: ObjectFormat, data: &[u8]) -> TestResult<Entry> {
        Ok(Entry::from_data_obj(
            &gix_object::Data {
                kind: gix_object::Kind::Blob,
                object_hash: object_hash(format),
                data,
            },
            0,
            gix_zlib::Compression::default(),
        )?)
    }

    fn delta(format: ObjectFormat, base: &[u8], target: &[u8]) -> TestResult<Entry> {
        assert!(base.len() < 128 && target.len() < 128);
        let mut instructions = vec![
            u8::try_from(base.len())?,
            u8::try_from(target.len())?,
            u8::try_from(target.len())?,
        ];
        instructions.extend_from_slice(target);
        let mut entry = blob(format, &instructions)?;
        entry.header = Header::RefDelta {
            base_id: gix_hash::ObjectId::from_bytes_or_panic(oid(format, base)?.as_bytes()),
        };
        entry.header_size = u16::try_from(entry.header.size(entry.decompressed_size))?;
        entry.crc32 = Some(entry.compute_crc32());
        Ok(entry)
    }

    fn pack(format: ObjectFormat, mut entries: Vec<Entry>) -> TestResult<Vec<u8>> {
        let mut offset = 12;
        for entry in &mut entries {
            entry.pack_offset = offset;
            offset += entry.bytes_in_pack();
        }
        let mut output = Cursor::new(Vec::new());
        let writer = EntriesToBytesIter::new(
            entries.into_iter().map(Ok),
            &mut output,
            Version::V2,
            object_hash(format),
        );
        for entry in writer {
            entry?;
        }
        Ok(output.into_inner())
    }

    async fn log() -> TestResult<(FaultStore, Log, View)> {
        let store = FaultStore::from_arc(Arc::new(InMemory::new()));
        let backend =
            ValidatedBackend::new(Arc::new(store.clone()), Path::from("thin-resolution")).await?;
        let log = Log::open(
            &backend,
            &LogId::new("thin-resolution")?,
            Options::default(),
        )
        .await?;
        let view = log.load().await?;
        Ok((store, log, view))
    }

    #[tokio::test]
    async fn resolves_true_thin_base_and_releases_retained_memory() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let base = b"external base";
            let source = pack(format, vec![blob(format, base)?])?;
            let normalized = pack::normalize(&operation, format, &source, &[])?;
            let (descriptor, root) = durable::stage(&operation, &log, &view, normalized).await?;
            let catalog = durable::load(
                &operation,
                &log,
                &view,
                format,
                &[(descriptor, root.reference().clone())],
            )
            .await?;
            let before = operation.live_bytes();
            let mut reader = Reader::new(&log, &view, &catalog);
            let target = b"external target";
            let input = pack(format, vec![delta(format, base, target)?])?;
            store.reset();
            let normalized = normalize(&operation, format, &input, &mut reader).await?;
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
            let index = gix_pack::index::File::from_data(
                normalized.index.as_slice(),
                std::path::PathBuf::new(),
                object_hash(format),
            )?;
            assert_eq!(index.num_objects(), 2);
            assert!(
                index
                    .lookup(gix_hash::ObjectId::from_bytes_or_panic(
                        oid(format, target)?.as_bytes()
                    ))
                    .is_some()
            );
            // A second pass needs no external bases: stored output is self-contained.
            drop(index);
            let independent = pack::normalize(&operation, format, &normalized.bytes, &[])?;
            assert_eq!(independent.id, normalized.id);
            drop((independent, normalized, reader));
            assert_eq!(operation.live_bytes(), before);
            drop(catalog);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn internal_ref_chain_needs_no_store_lookup() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let catalog = durable::load(&operation, &log, &view, format, &[]).await?;
            let mut reader = Reader::new(&log, &view, &catalog);
            let input = pack(
                format,
                vec![
                    delta(format, b"middle", b"target")?,
                    delta(format, b"base", b"middle")?,
                    blob(format, b"base")?,
                ],
            )?;
            store.reset();
            let output = normalize(&operation, format, &input, &mut reader).await?;
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
            drop((output, reader));
            drop(catalog);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn missing_base_and_exhausted_budgets_do_not_read_storage() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for exhausted in [0, 1, 2] {
                let (store, log, view) = log().await?;
                let operation = Pool::new(LIVE_BYTES).admit()?;
                let log = log.with_request_guard(Arc::new(operation.clone()));
                let catalog = durable::load(&operation, &log, &view, format, &[]).await?;
                let mut reader = Reader::new(&log, &view, &catalog);
                let input = pack(format, vec![delta(format, b"missing", b"target")?])?;
                if exhausted == 1 {
                    for _ in 0..THIN_ROUNDS {
                        operation.thin_round()?;
                    }
                }
                if exhausted == 2 {
                    operation.work(WORK_BYTES)?;
                }
                store.reset();
                assert!(
                    normalize(&operation, format, &input, &mut reader)
                        .await
                        .is_err()
                );
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
                drop(reader);
                drop(catalog);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn mixed_chains_prune_redundant_durable_candidates() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for retain_middle in [false, true] {
                let mut values = (0..40)
                    .map(|i| format!("mixed-{i}").into_bytes())
                    .collect::<Vec<_>>();
                values.sort_by_key(|data| oid(format, data).ok());
                let full = &values[0];
                let blocked = &values[1];
                let resolved = &values[2];
                let external = &values[39];
                let (store, log, view) = log().await?;
                let operation = Pool::new(LIVE_BYTES).admit()?;
                let log = log.with_request_guard(Arc::new(operation.clone()));
                let mut remote = vec![
                    blob(format, full)?,
                    blob(format, resolved)?,
                    blob(format, external)?,
                ];
                if retain_middle {
                    remote.push(blob(format, blocked)?);
                }
                let source = pack(format, remote)?;
                let normalized = pack::normalize(&operation, format, &source, &[])?;
                let (descriptor, root) =
                    durable::stage(&operation, &log, &view, normalized).await?;
                let catalog = durable::load(
                    &operation,
                    &log,
                    &view,
                    format,
                    &[(descriptor, root.reference().clone())],
                )
                .await?;
                let input = pack(
                    format,
                    vec![
                        delta(format, blocked, b"external-final")?,
                        delta(format, external, blocked)?,
                        delta(format, resolved, b"internal-final")?,
                        delta(format, full, resolved)?,
                        blob(format, full)?,
                    ],
                )?;
                let mut reader = Reader::new(&log, &view, &catalog);
                store.reset();
                let normalized = normalize(&operation, format, &input, &mut reader).await?;
                let index = gix_pack::index::File::from_data(
                    normalized.index.as_slice(),
                    std::path::PathBuf::new(),
                    object_hash(format),
                )?;
                assert_eq!(
                    index.num_objects(),
                    6,
                    "only the true external base belongs in output"
                );
                drop(index);
                let independent = pack::normalize(&operation, format, &normalized.bytes, &[])?;
                assert_eq!(independent.id, normalized.id);
                let duplicated_input = pack(
                    format,
                    vec![
                        delta(format, blocked, b"external-final")?,
                        delta(format, external, blocked)?,
                        blob(format, full)?,
                        blob(format, full)?,
                    ],
                )?;
                assert!(
                    matches!(normalize(&operation, format, &duplicated_input, &mut reader).await,
                    Err(Error::InvalidPack(message)) if message == "pack contains duplicate object IDs")
                );
                // All verified candidates share one small cached durable chunk.
                assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
                drop((independent, normalized, reader));
                drop(catalog);
                assert_eq!(operation.live_bytes(), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn round_budget_is_checked_before_an_available_base_get() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let source = pack(format, vec![blob(format, b"base")?])?;
            let normalized = pack::normalize(&operation, format, &source, &[])?;
            let (descriptor, root) = durable::stage(&operation, &log, &view, normalized).await?;
            let catalog = durable::load(
                &operation,
                &log,
                &view,
                format,
                &[(descriptor, root.reference().clone())],
            )
            .await?;
            for _ in 0..THIN_ROUNDS {
                operation.thin_round()?;
            }
            let before = operation.live_bytes();
            let mut reader = Reader::new(&log, &view, &catalog);
            let input = pack(format, vec![delta(format, b"base", b"target")?])?;
            store.reset();
            let result = normalize(&operation, format, &input, &mut reader).await;
            assert!(
                matches!(result, Err(Error::InvalidPack(message)) if message == "thin-pack round limit exceeded")
            );
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
            drop(reader);
            assert_eq!(operation.live_bytes(), before);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_root_does_not_refetch_a_supplied_intermediate() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let source = pack(format, vec![blob(format, b"middle")?])?;
            let normalized = pack::normalize(&operation, format, &source, &[])?;
            let (descriptor, root) = durable::stage(&operation, &log, &view, normalized).await?;
            let catalog = durable::load(
                &operation,
                &log,
                &view,
                format,
                &[(descriptor, root.reference().clone())],
            )
            .await?;
            let mut reader = Reader::new(&log, &view, &catalog);
            let input = pack(
                format,
                vec![
                    delta(format, b"middle", b"target")?,
                    delta(format, b"missing", b"middle")?,
                ],
            )?;
            store.reset();
            assert!(
                normalize(&operation, format, &input, &mut reader)
                    .await
                    .is_err()
            );
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 1);
            // Exactly one acquisition round was consumed; the supplied ID is not retried.
            for _ in 1..THIN_ROUNDS {
                operation.thin_round()?;
            }
            assert!(operation.thin_round().is_err());
            drop(reader);
            drop(catalog);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_releases_helper_and_reader_reservations() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let source = pack(format, vec![blob(format, b"base")?])?;
            let normalized = pack::normalize(&operation, format, &source, &[])?;
            let (descriptor, root) = durable::stage(&operation, &log, &view, normalized).await?;
            let catalog = durable::load(
                &operation,
                &log,
                &view,
                format,
                &[(descriptor, root.reference().clone())],
            )
            .await?;
            let before = operation.live_bytes();
            let mut reader = Reader::new(&log, &view, &catalog);
            let input = pack(format, vec![delta(format, b"base", b"target")?])?;
            let mut pause = store.pause_next_get(object_log::sim::FailurePhase::Before);
            let mut pending = Box::pin(normalize(&operation, format, &input, &mut reader));
            let entered = tokio::select! {
                result = &mut pending => { result?; false },
                entered = pause.wait_until_entered() => entered,
            };
            assert!(entered);
            drop(pending);
            drop(reader);
            assert_eq!(operation.live_bytes(), before);
            drop(catalog);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn original_duplicates_and_corrupt_packs_are_not_repaired() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let (store, log, view) = log().await?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let log = log.with_request_guard(Arc::new(operation.clone()));
            let catalog = durable::load(&operation, &log, &view, format, &[]).await?;
            let mut reader = Reader::new(&log, &view, &catalog);
            let duplicate = pack(format, vec![blob(format, b"same")?, blob(format, b"same")?])?;
            store.reset();
            assert!(
                matches!(normalize(&operation, format, &duplicate, &mut reader).await, Err(Error::InvalidPack(message)) if message == "pack contains duplicate object IDs")
            );
            let mut corrupt = pack(format, vec![delta(format, b"missing", b"target")?])?;
            let last = corrupt.last_mut().ok_or("empty fixture")?;
            *last ^= 1;
            let work_before = operation.work_bytes();
            assert!(
                normalize(&operation, format, &corrupt, &mut reader)
                    .await
                    .is_err()
            );
            assert!(
                operation.work_bytes() > work_before,
                "work from failed attempts is cumulative"
            );
            assert_eq!(store.metrics().operation(StoreOperation::Get).requests, 0);
            drop(reader);
            drop(catalog);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn gix_missing_id_can_be_an_unresolved_internal_chain_base() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let mut values = (0..20)
                .map(|i| format!("object-{i}").into_bytes())
                .collect::<Vec<_>>();
            values.sort_by_key(|data| oid(format, data).ok());
            let middle = &values[0];
            let base = &values[19];
            let input = pack(
                format,
                vec![
                    delta(format, middle, b"target")?,
                    delta(format, base, middle)?,
                ],
            )?;
            let operation = Pool::new(LIVE_BYTES).admit()?;
            let Err(error) = pack::normalize_attempt(&operation, format, &input, &[]) else {
                return Err("thin input unexpectedly succeeded".into());
            };
            assert!(
                matches!(error, NormalizeError::MissingBase { id, .. } if id == oid(format, middle)?)
            );
            drop(error);
            assert_eq!(operation.live_bytes(), 0);
        }
        Ok(())
    }
}
