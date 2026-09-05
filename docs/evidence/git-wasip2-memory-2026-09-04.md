# Actual WASIp2 memory-store lifecycle

Six actual Spin component invocations passed: SHA-1 and SHA-256, each with a
4 KiB fixture, an 8 MiB fixture, and a 384-commit history followed by a thin
incremental push. This qualifies the common engine against `InMemory` on the
WASIp2 runtime. It is separate from the S3 transport/MinIO qualification.

```sh
make git-spin-memory-acceptance
```

The target builds the test-only `memory_lifecycle` example, then runs
`crates/object-log-git-spin/tests/check_memory.py`. The driver generates all
fixtures in a temporary directory and validates returned packs using unchanged
Git. No large fixtures or generated component binaries are committed.

## Runtime and scope

The run used Rust 1.97.1, optimized `wasm32-wasip2`, Spin 4.0.2
(bfc7543 2026-06-23), and Git 2.54.0 (Apple Git-157), on macOS arm64.
The component was built against product revision `0ed9b52` plus this test
tranche. The exercised artifact was 2,118,663 bytes, SHA-256
`f0a9ee92d1fa56d7728180dab1484b0a3f52cb0802fb3561a46de14184552b64`.

Spin ran with pooling enabled, both instance-count settings fixed at one, and
`--max-instance-memory 134217728`. All six invocations completed without a trap
or allocation failure. This is a guest instance-memory cap, not a measurement
or claim that total Spin process RSS stays below 128 MiB. The fresh-process RSS
and Linux cgroup qualification remain separate gates.

Each invocation explicitly creates a fresh `InMemory`, `ValidatedBackend`, and
`Log`, then runs the entire lifecycle before replying. Nothing relies on provider
state surviving across HTTP requests. The example is test support, not a
production memory-backed hosting API. Its length-prefixed envelope merely
carries unchanged receive-pack and protocol-v2 upload request bytes between the
driver and one invocation. It has a 10 MiB input cap and a 20 MiB aggregate output
cap. The example has no allowed outbound hosts or preopened filesystem.

Memory-provider object bytes, the test envelope, and the aggregate test response
are outside the Git engine's live-byte pool. Each command still uses the original
public `Repository` admission and accounting. The test copies each completed
command reply into its bounded aggregate and drops the original accounted
`Bytes` before opening the next operation. The input envelope is also bounded;
these test-only allocations are deliberately part of the actual capped guest
workload. No engine limits, counters, retry limits, or product behavior changed.

## Checks performed

Every invocation:

1. Publishes a normal receive-pack transaction and verifies its success report.
2. Reopens `Repository` without any local cache and performs a full fetch.
3. For the history case, publishes a genuinely thin pack and performs a
   have-aware fetch. The driver first proves the thin input cannot index in an
   empty receiver because it needs an external delta base.
4. Rejects a stale ref update and checks that the ref snapshot is unchanged.
5. Stages an unpublished orphan through the core API, publishes the common Git
   checkpoint, and completes core garbage collection.
6. Checks that the orphan is absent, drops the original core `Log`, opens a
   fresh `Log` and `Repository`, and performs a full fetch from the
   post-collection state. This also replaces the core staging domain.

The driver compares every returned pack's exact OID set with Git's revision
walk. It indexes each pack into an empty receiver without graph checking first,
which rejects external delta bases. Full packs then undergo
`index-pack --strict --check-self-contained-and-connected`; incremental packs
undergo strict indexing in a receiver seeded only with the accepted-have
history. All resulting repositories pass `git fsck --strict` after updating the
fetched target ref. Raw and framed fetch-size caps remain assertions.

## Results

Raw invocation sizes, timings, object counts, and collection outcomes are in
[the JSONL evidence](git-wasip2-memory-2026-09-04.jsonl).

Both 8 MiB cases succeeded. The largest request was 8,392,133 bytes; each of its
full responses was 8,392,076 bytes. The 384-commit clones contained 1,152 objects;
after the incremental push, full recovery contained 1,155 objects and the
have-aware fetch contained exactly three new objects. Collections deleted two
candidates in the one-commit fixtures and three in the history fixtures,
including the explicitly staged orphan.

These are single invocation observations, not paired latency benchmarks. All
focused native example tests, native strict Clippy, locked WASIp2 strict Clippy,
formatting, and whitespace checks passed. Production source files are unchanged.

Independent review found no blocking fixture issues and recommended the fresh
core `Log` recovery step. That strengthening was implemented and all six actual
WASI cases were rerun successfully.
