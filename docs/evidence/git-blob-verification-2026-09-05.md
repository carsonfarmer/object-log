# Bounded verification of selected full blobs

Issue: https://github.com/carsonfarmer/object-log/issues/26

Baseline: `5ae3e64fe9fa1ac6fc7a5d6936c7522dab657e72` on exclusive branch
`cf/capacity-range-fetch`. This follows the accepted authenticated range-copy
tranche. No main integration or push is performed by this worker.

Selected full blobs can now be authenticated without retaining decoded content.
`Repository::fetch_pack_or_ack` calls private `Reader::verify` for unverified
selected leaves and compares the verified kind with the graph's expected kind.
Delta and non-blob objects still use `find` and its existing bounded
materialization. Graph loading, receive preparation, catalog/state and Spin
host code are unchanged.

## Implementation and boundaries

The verifier parses the same bounded, canonical pack-entry header as fetch
reuse. The existing 8 MiB object cap applies before decoded-size arithmetic.
Full blobs flow through authenticated chunk slices, the original entry CRC
checker, an incremental zlib decoder and the selected Git hash implementation.
Hashing includes the Git loose-object header with the declared kind and size.
The final decoded size, zlib stream end, original entry CRC and requested OID
must all match. Trailing bytes, truncated/checksum-invalid streams and wrong
object IDs fail the operation before a response is returned.

The decoder uses a 32 KiB output window and the established 48 KiB codec
reservation: 80 KiB in total, independent of decoded blob size. Both are
reserved before allocating the decoder/window and are released on completion,
error or cancellation. This excludes the existing bounded authenticated chunk
cache, catalog/graph and host allocations; it is not an RSS measurement.

Compressed-range CRC work is charged before the first header read. Full-blob
decode/hash work is charged as twice the declared size plus one probe byte,
before constructing the decoder. The output slice is limited to remaining
declared bytes plus one, capped at 32 KiB. If that probe produces excess data,
verification rejects it before hashing. This also permits empty blobs and
exact-window endings to consume their final zlib bits/checksum. Each inflate
step must consume input or produce output; no-progress with input remaining
fails, and exhaustion of one input slice can await another. Complete input
still requires exact stream termination.

Fallback objects conservatively retain the prefix pass's work charge plus the
existing `find` charges. Chunk I/O/work remains charged by the shared reader;
no counters reset on retry. The prior bounded header helper is shared by fetch
reuse and verification. No public API, dependency, durable format or policy
constant changes. Raw pre-test `durable.rs` lines grow from 896 to 1,027 (+131).

## Verification

New tests cover:

- Both hashes and compressed/uncompressed 2 MiB full blobs with OIDs produced
  by installed Git. With cached compressed chunks, exactly 80 KiB of free
  pool memory permits verification, while full `find` fails under the same
  pressure. Verification retains no output allocation and performs zero GETs
  against that warm cache; after pressure is released `find` confirms size.
- Empty and one-byte blobs, and window-minus-one/exact/window-plus-one lengths;
  compressed input delivered one byte at a time, seven bytes at a time or whole.
- Wrong declared sizes in both directions, wrong OIDs, corrupt zlib checksum,
  missing trailer bytes, and trailing data in the same or a later input slice.
- Codec/window memory shortage and insufficient cumulative work before cached
  decoding, without storage GETs or leaked reservations.
- Cancellation while a later authenticated chunk GET is paused: decoder,
  outstanding chunk and reader reservations release, then admission reopens.

Existing negative fixtures now also run `verify`: CRC/zlib/trailing-byte/OID
corruption and invalid delta chains. Both-hash CRC mismatch controls and
REF/OFS delta verification exercise the new entry point. Repository tests retain
wrong tree/tag leaf-kind rejection and exact installed-Git fetch selection.

`make check` passes: **329 tests passed, 12 opt-in tests ignored**, formatting,
strict all-target/all-feature workspace Clippy, Git WASIp2 check and strict
Spin WASIp2 Clippy. Raw output is
[`git-blob-verification-check-2026-09-05.txt`](git-blob-verification-check-2026-09-05.txt).
The gate uses memory stores and installed-Git filesystem receivers; no MinIO
test was enabled for this tranche.

Independent Rust correctness/simplification review approved the final diff
with no blocking findings, checked the decoder/probe/progress arithmetic and
ownership, and independently passed six focused tests. Its initial output-end
probe finding was incorporated before the final gate.

Environment: macOS arm64, Apple M4 Pro, Rust 1.97.1, installed Apple Git 2.54.0.

## Remaining capacity dependencies

#26 remains open. The current 8 MiB decoded-object, 9 MiB incoming/raw-fetch,
16 MiB normalized-pack, 2 MiB index, 32,768-object and cumulative operation
limits are unchanged. This does not qualify 50 MiB blobs or 1 GiB pushes.
Ingest, normalization/indexing, delta resolution, graph-root materialization,
raw/framed response buffering, transport, scratch lifetime, quotas and actual
Spin/MinIO runtime qualification remain. The 128 MiB target is serving runtime;
build and executable-cache preparation are separate provisioning work.

The next design dependency is replayable bounded input/scratch processing,
coordinated with the catalog/compaction owner. Receive connectivity can adopt
the verifier in its separately owned work stream. See the
[capacity design](git-capacity-design-2026-09-05.md) for the dependency order,
GC/response-lifetime decisions and reusable WAL API friction.


## Native paired acceptance

The existing release `shared_git_performance_acceptance` passes all 14
both-hash cases: 10 measured alternating pairs plus one warmup each, no 1.25x
review flags. Existing exact-object, pack-size and operation-accounting checks
pass for 4 KiB/8 MiB transfers, history, incremental fetch and thin push.

```text
OBJECT_LOG_GIT_PERFORMANCE_OUTPUT=/tmp/object-log-blob-shared-performance.jsonl cargo test --locked --release -p object-log-git --test shared_performance -- --ignored --exact shared_git_performance_acceptance --nocapture
```

Raw [JSONL](git-blob-verification-performance-2026-09-05.jsonl) and
[command output](git-blob-verification-performance-2026-09-05.txt) are retained.
The recorded revision is the baseline HEAD; the run includes this uncommitted
source patch. Release compilation took 4.00 seconds and the test 121.18 seconds.
This compares native memory-store commands with installed-Git subprocesses,
with different whole-command scopes. It is not a before/after speedup result,
actual WASIp2/MinIO performance, runtime RSS qualification, or a resolution of
#23's historical WASIp2 SHA-1 finding. Compilation was outside any serving
memory constraint.
