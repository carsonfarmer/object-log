# object-log-kv

A small byte-key/value library on object-log. Each atomic batch publishes one
immutable compressed radix-tree root through the WAL's conditional head. Reads
load only the requested paths; mutations evaluate and copy each path in one
traversal and reuse unchanged subtrees. Reopening materializes root proofs from the checkpoint and bounded WAL
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
# Ok(())
# }
```

Snapshots are exact and immutable. `get_many` and each scan page use that same
view; `scan` uses inclusive start, exclusive end, and an exclusive continuation
key. Continue on the same snapshot. `scan_prefix` includes the empty prefix and
arbitrary binary keys. Missing values are `None`; empty keys and values are valid.
Commands in a batch execute in order and become visible atomically. Set/delete
return `Changed(bool)`; setting an identical value or deleting an absent key
returns false. Read from the same snapshot before preparing when the application
needs the previous value. A mismatched CAS returns false and changes nothing for
that command. Other batch commands still execute. Invalid integers, overflow, or limits abort the entire preparation.
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
The default value limit is 64 KiB. Mutation results contain only booleans or
integers, so their size does not depend on stored value size. The WAL's result
allowance still bounds the encoded batch results.
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
Remote S3 performance, streaming large values, precise allocator admission, and
multi-hour growth/soak qualification remain future work. Local `MinIO` qualification
for finite growth, contention, recovery and collection is opt-in below. The format is
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


## Local `MinIO` qualification

Run memory and filesystem capability checks before the provider tests. The
filesystem backend is expected to reject conditional updates. From the repository
root (Docker, AWS CLI, curl, and `ps` installed):

```sh
cargo test --workspace --all-features
export CARGO_PROFILE_TEST_OPT_LEVEL=3 CARGO_PROFILE_TEST_DEBUG=0
cargo test -p object-log-kv --all-features
./scripts/test-minio.sh kv minio_correctness_matrix object-log-kv aws
./scripts/test-minio.sh qualification minio_growth_and_contention object-log-kv aws
```

Each command starts the existing pinned `MinIO` image on a random loopback port
with its own container and disposable bucket, and verifies container removal.
Each scenario uses a random KV prefix. The tests require explicit `aws` and
`--ignored` opt-ins, reject non-loopback endpoints, and never provision AWS.
Manual invocations must use a disposable local bucket; random test prefixes are
left for inspection until the instance is removed. The existing native `MinIO`
runner option is also supported.

The correctness matrix runs the same assertions as memory: ordered batches,
binary-key reference-model checks, conflicting conditions, hidden successful
publication, cancellation after head mutation, process-loss recovery and expired
evidence, definite noncommit, pending checkpoints, retained scans, and resumed
collection after a lost deletion response, stale writer fencing, and fresh
publication while collection is active. Faults are injected around real
provider operations; they do not simulate network partitions or process kills.

The growth test uses a fixed seed, ordered `records/` keys, 1 KiB values, and
256/1,024/4,096-key spaces in one namespace. It grows in 64-command batches,
checkpoints every eight growth batches, then mixes 32 rounds of point reads,
fresh-snapshot reads, eight-key multi-gets, 32-entry scans, and alternating
one/eight-command overwrites and deletes. Every fourth round targets a hot key;
others use deterministic random keys. Each size also runs four writers with
32 total two-key increments, two readers checking atomic visibility, retained
snapshot scans, cold reopen, full model comparison, and collection. Retries are
bounded and occur only on definite conflicts; any pending result stops that
workload. The separate fault matrix verifies pending-result resolution.

Acceptance gates require exact model equality, untorn batches, definite
conflicts, recoverable maintenance, and smaller storage after collection.
Checkpointed snapshot loading takes at most two logical GETs; point reads use at
most six GETs and 32 KiB, within a 128 KiB tree allowance even when live key/value data
exceeds 4 MiB. The existing sparse-call test additionally requires at most three
path GETs and PUTs for an overwrite on its binary-key fixture and no repeated
path reads. Latency is reported, not used as a machine-dependent pass/fail gate.

Output includes p50/p95 operation latency and sample counts, phase elapsed time,
logical calls and payload bytes, provider HTTP attempts (including retries and
list pagination), offered HTTP request-body bytes, conflict responses and transport/5xx errors,
stored object counts/bytes before and after GC, and process RSS at phase
boundaries. These HTTP counters wrap the established provider transport; they
are not a second storage client. HTTP body bytes include bulk-delete XML, exclude
headers/TLS, and are attempted bytes rather than proof of delivery. Logical
write amplification is uploaded object bytes divided by accepted command key
and value bytes (delete keys only); GC is reported separately through the direct provider so bulk deletion remains
intact; logical counters are unavailable for that phase. The fault wrapper used
by other phases serializes deletes to inject individual-object failures. Exact numerators
and denominators are printed, including live application bytes for storage
amplification. Operation samples include oracle comparisons; phase elapsed time also includes
untimed snapshot loads, checkpoints, and instrumentation.
RSS includes the oracle, runtime, transport, and allocator slack; it is neither
peak RSS nor an exact allocator/admission measure. No per-request event archive
is kept. Preserve raw command/Criterion output locally under ignored `target/`
when comparing runs. Fresh-snapshot reads include bounded WAL replay, but do not
flush provider or OS caches. These are local `MinIO` results, not remote-S3 or
production-readiness evidence.
