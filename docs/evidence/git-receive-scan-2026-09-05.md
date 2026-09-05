# Fixed-window receive scanning and the replayable-input boundary

Issue: https://github.com/carsonfarmer/object-log/issues/26

Baseline: `935fe7ff210a3287f3a2d2cf407af8f82d8a4bd9`, exclusive branch
`cf/capacity-range-fetch`. Only `pack.rs` and this tranche's evidence change.
No core, catalog/state, namespace, receive-command or Spin host changes.

The production receive scanner no longer allocates a decoded buffer as large
as the largest pack entry. It reserves the existing 48 KiB codec allowance and
one reusable 32 KiB output window before construction. It discards decoded
output except for a 20-byte stack prefix sufficient for two maximal u64 delta
size integers. Delta result-size validation still uses the existing checked
target-width integer decoder.

This removes a decoded temporary. It does **not** remove whole incoming-pack
retention: the caller still holds `Bytes`, the scanner still copies compressed
entries, normalization still writes a complete output pack, and gix indexing
still has its full-pack/all-decoded-memory assumptions. All policy constants
are unchanged. There is no 50 MiB/1 GiB or 128 MiB runtime qualification claim.

## Scanner contract and checks

`scan_inflate` receives the compressed tail up to (excluding) the pack trailer,
a checked declared size, a delta flag, reusable codec state and output window.
It resets codec state per entry and returns the exact compressed length plus
the full-object or delta-result size. It must stop at the **first** zlib stream
end: following input may belong to the next pack entry. This differs from
selected-blob verification, which must reject bytes after its exact entry range.
The two helpers deliberately retain separate end-of-input contracts.

Every step requires input or output progress. Decoded output must match the
header's declared size exactly. The output slice is capped to remaining bytes
plus one probe, then to the fixed window; an overrun fails immediately. Work
preflight charges the declared size plus that single probe before inflation.
The existing exact thin-pack work-boundary test includes one probe per entry;
it still passes at the exact operation limit and fails one unit above it.

Pack trailer verification, canonical headers, OFS base offsets, entry CRCs,
object count, object/result-size limits, compressed copies, statistics, thin
dependency resolution, duplicate handling and object-ID/index validation retain
their existing paths. Every thin retry repeats charges on the same operation.

## Contract required to remove incoming retention

The current interfaces prevent a local scanner change from releasing the
incoming pack:

| Owner | Current requirement | Required follow-on contract |
| --- | --- | --- |
| HTTP/receive controls | Host collects a body; `prepare_receive(Bytes)` retains it; `ReceiveRequest` borrows its pack slice | Parse bounded controls independently and hand an incremental pack source to the engine |
| Thin normalization | `receive::normalize` repeatedly invokes `normalize_attempt` on the original slice | Replay exact compressed entry ranges and metadata across bounded dependency rounds, with cumulative accounting |
| Scan/normalization | `InputEntry` owns copied compressed vectors | Retain entry header/CRC/size/dependency/range metadata and copy from a replayable source only while writing output |
| Indexing | The current gix resolver closes over the complete normalized pack slice and reserves proportional to total resolved content | Verify objects through bounded delta/base scratch and write indexes from verified OID/CRC/offset records |
| Publication | Staging proofs are tied to exact view/collection semantics | Abort or rebuild expired staging through existing proofs; never publish a partially validated input |

The next source abstraction should be private and byte-oriented: a known or
checked cumulative length, bounded authenticated range reads, and a sink that
consumes bounded input chunks with backpressure. Its replayable handle carries
the exact observed view and immutable chunk references. Source metadata has a
separate count/byte reservation. Controls are owned bounded metadata; they no
longer borrow a full pack allocation. The normalized sink and standard-index
writer consume range metadata, not an adapter that silently collects the source
back into a single `Vec`.

Existing WAL `put_object`, `put_node`, `read_object`, `read_node` and staged-object
proofs are sufficient to prototype that source. No invented core stream API is
needed. Use bounded fanout as soon as chunk counts exceed direct-child limits;
keep every durable child reachable through node references. Never authenticate
a partial hashed blob without first verifying the whole bounded chunk. The
core-owned node-size preflight remains a separate coordinated API concern.

An object-backed source is temporary immutable staging, not publication
authority or a retention lease. A collection expiry must abort/restart bounded
work; interrupted-upload leftovers remain collectible. It cannot guarantee
replay after GC merely by retaining an old `View`. The host/client retry and
source lifetime need explicit handling before switching the public receive
path. A new durable retention protocol is not part of this tranche.

Adding a staging sink today while feeding all bytes back to the current
slice-based scanner/indexer would retain the input and add PUTs, rereads and
scratch lifetime problems. Independent architecture review therefore approved
the production-wired fixed-window scan prerequisite rather than presenting
such staging as removal of whole-pack retention. The receive-command API
change needs coordination with its owner before the next broader tranche.

## Evidence

Three new focused tests cover:

- Empty, one-byte, window-minus-one/exact/window-plus-one streams with output
  windows of 1, 17 and 32,768 bytes; exact consumed length with adjacent zlib
  streams; too-small/too-large declarations; truncated and corrupt checksums.
- Delta size integers spanning one-byte windows, output extending far beyond
  the retained prefix, and missing/truncated/overflowing delta integers.
- Both-hash installed-Git 2 MiB blob packs scanned in a 128 KiB operation pool,
  including input and retained compressed metadata. The decoded object cannot
  fit that pool, while scanning succeeds and releases reservations. A separate
  ordinary-pool normalization passes installed-Git strict validation. A pool
  one byte short of scan metadata plus codec/window allowance rejects cleanly.

The 2 MiB fixture is compressible and isolates decoder allocation. It is not
the incompressible large-push acceptance workload.

`make check` passes **332 tests, 12 opt-in tests ignored**, formatting, strict
all-target/all-feature native workspace Clippy, Git WASIp2 check and strict
Spin WASIp2 Clippy. Raw output:
[`git-receive-scan-check-2026-09-05.txt`](git-receive-scan-check-2026-09-05.txt).
Memory-store and installed-Git filesystem cases run before any provider work;
MinIO was not enabled for this local tranche.

Independent Rust correctness/simplification review approved with no actionable
findings and separately passed the three scanner tests and exact thin-pack
work-boundary test. Environment: macOS arm64, Apple M4 Pro, Rust 1.97.1,
installed Apple Git 2.54.0.

#26 remains open. The next coordinated implementation must change the
replayable input/entry representation, then bounded delta resolution and index
construction; the serving-runtime target remains separate from compilation
and executable-cache preparation.


The release `shared_git_performance_acceptance` also passes all 14 both-hash
cases, 10 measured alternating pairs plus one warmup each, with no 1.25x review
flags. Existing exact-object, pack-size and operation-accounting checks pass.
Raw [JSONL](git-receive-scan-performance-2026-09-05.jsonl) and
[command output](git-receive-scan-performance-2026-09-05.txt) are retained.

```text
OBJECT_LOG_GIT_PERFORMANCE_OUTPUT=/tmp/object-log-scan-performance.jsonl cargo test --locked --release -p object-log-git --test shared_performance -- --ignored --exact shared_git_performance_acceptance --nocapture
```

The recorded revision is the baseline HEAD; the run includes this uncommitted
scanner patch. The test took 110.12 seconds. This is native memory-store versus
installed-Git subprocess acceptance with different whole-command scopes, not
a before/after speedup claim, actual Spin/MinIO performance, or resolution of
#23's historical WASIp2 finding. Compilation was outside any serving-memory
constraint.
