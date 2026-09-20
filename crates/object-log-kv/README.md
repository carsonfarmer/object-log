# object-log-kv

A small byte-key/value library on object-log. Each atomic batch publishes one
immutable compressed radix-tree root through the WAL's conditional head. Reads
load only the requested paths; writes copy those paths and reuse unchanged
subtrees. Reopening materializes root proofs from the checkpoint and bounded WAL
tail, without reading the database. There is no local database or second head.

```rust,no_run
# async fn example(log: object_log::Log) -> Result<(), Box<dyn std::error::Error>> {
use bytes::Bytes;
use object_log::{CommitStatus, TransactionId};
use object_log_kv::{KvCommand, KvStore, Limits, decode_results};

let store = KvStore::new(log, Limits::default());
let snapshot = store.snapshot().await?;
let candidate = snapshot.prepare(TransactionId::new(), &[
    KvCommand::Set { key: Bytes::from("name"), value: Bytes::from("Ada") },
]).await?;
// Persist both before publication if this operation must survive process loss.
let recovery_token = candidate.recovery_token()?;
let result_bytes = candidate.result().clone();
match store.log().commit(candidate).await? {
    CommitStatus::Committed(_) => { let results = decode_results(&result_bytes)?; }
    CommitStatus::Conflict(_) => { /* refresh snapshot, then prepare again */ }
    CommitStatus::Pending(pending) => { /* preserve evidence; resolve, don't replay */ }
}
let fresh = store.snapshot().await?;
assert_eq!(fresh.get(b"name").await?.as_deref(), Some(b"Ada".as_slice()));
# Ok(())
# }
```

Snapshots are exact and immutable. `get_many` and each scan page use that same
view; `scan` uses inclusive start, exclusive end, and an exclusive continuation
key. Continue on the same snapshot. `scan_prefix` includes the empty prefix and
arbitrary binary keys. Missing values are `None`; empty keys and values are valid.
Commands in a batch execute in order and become visible atomically. A mismatched
CAS returns false and changes nothing for that command. Other batch commands
still execute. Invalid integers, overflow, or limits abort the entire preparation.
Even a no-op batch records durable results when committed.

Prepare and publish are separate so applications can persist recovery evidence
before cancellation or process loss. Results are recorded in the WAL candidate;
persist its `result()` bytes alongside `recovery_token()` for typed process-loss
recovery, then use `Log::resume`. Only `Committed` permits returning those results.
Retry as new work only after `Conflict` or `NotCommitted`. `StillPending` requires
resolution; `Expired` means the result cannot safely be determined, not failure.
The WAL retention window bounds deduplication and recovery evidence.

Call `snapshot.checkpoint()` before the WAL tail fills. It publishes the current
root and copies no tree data. Handle its conflict/pending statuses through the
WAL. Retain `snapshot.view()` with `Log::retain` before long scans or concurrent
collection; confirm retention before use and release it afterward. Unretained
reads can return `ViewExpired` and never silently switch to a newer view. Use
`Log::start_collection` / `resume_collection` after checkpointing, following the
WAL's fencing and drained-reader recovery contract. Lost checkpoint responses
leave a recoverable WAL; reopen before starting new maintenance.

Limits bound key/value sizes, batch inputs, response bytes, and cumulative tree
bytes read/written and scan-prefix bytes allocated per call. Large values are
rejected rather than streamed.
The default value limit is 1 KiB, so one previous value fits the WAL's default
4 KiB result allowance. For larger values or batches, configure the WAL's
`max_inline_result_bytes` to fit the encoded results when creating the namespace,
then raise the KV limits. WAL options are durable and cannot be increased by
reopening. Set/delete return previous values; CAS returns only a boolean.
The tree budget includes repeated path reads and every command in a batch. It
excludes WAL metadata: WAL options separately bound tail, commit, checkpoint,
and head sizes. Use a shared WAL request guard across retries for cumulative
logical storage calls; provider-internal HTTP attempts remain provider-specific.
No automatic conflict retries, unbounded index cache, or background work exist.

This is a first storage implementation, not a production qualification claim.
Inline values make path copying expensive for value-bearing ancestors. Ordered
multi-key batches currently read and write each path separately. Work admission
is a byte bound, not an exact allocator/RSS accounting system; decoded nodes,
path stacks, result encoding, and caller-owned inputs add bounded memory overlap.
Remote S3 performance, streaming large values, global memory admission, and
long-running growth/provider qualification remain future work. The format is
pre-release and incompatible with the former materialized-map demonstration;
use a fresh WAL namespace. Every writer to a namespace must use this format.

Run `cargo test -p object-log-kv` for deterministic correctness and logical-I/O
bounds, and `cargo bench -p object-log-kv --bench kv` for the in-memory baseline.
The benchmark uses four-byte ordered keys, 64-byte values, 256/4,096-key trees,
20 Criterion samples, and two-second measurement windows. The reopen case reads
a checkpointed root and then one key. `prepare_set` measures staging an overwrite,
not WAL publication: detailed fault-store events identify its unpublished nodes
for untimed cleanup, keeping benchmark storage bounded. The other cases disable
event recording. These measurements are local in-memory results, not S3 latency.
