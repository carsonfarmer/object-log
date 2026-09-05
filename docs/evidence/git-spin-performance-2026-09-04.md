# Paired Git performance inside actual WASIp2

All 14 hash/direction cases pass the functional and resource gates. SHA-1's
8 MiB push exceeds the latency review threshold after 30 paired samples:
1.655× p50 and 1.634× p95 against native Git. The native oracle remains; this
result requires owner performance review before its deletion.

Reproduce with `make git-spin-performance-acceptance`. The test-only component
uses the common engine and an InMemory provider under a 128 MiB instance cap.
It covers 4 KiB push/clone, 8 MiB push/clone, 384-commit clone, have-aware
incremental fetch, and a genuinely thin incremental push for both hashes.

One warmup and ten alternating paired samples ran per case; the SHA-1 8 MiB
push escalated to thirty pairs after the initial threshold breach. The final
run contains 160 measured pairs plus 14 warmup pairs, 174 component invocations
and 174 native Git observations. All raw samples, including warmups, remain in
[final JSONL](git-spin-performance-2026-09-04.jsonl). The preliminary run is
preserved separately and was not substituted for the final instrumented run.

| Hash | Case | Pairs | Spin p50 ms | Git p50 ms | p50 ratio | p95 ratio |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| sha1 | 4kib-push | 10 | 0.278 | 18.721 | 0.015 | 0.015 |
| sha1 | 4kib-fetch | 10 | 0.106 | 9.459 | 0.011 | 0.011 |
| sha1 | 8mib-push | 30 | 99.510 | 60.110 | 1.655 | 1.634 |
| sha1 | 8mib-fetch | 10 | 56.562 | 146.099 | 0.387 | 0.390 |
| sha1 | 384-fetch | 10 | 8.868 | 95.216 | 0.093 | 0.087 |
| sha1 | incremental-fetch | 10 | 3.214 | 19.169 | 0.168 | 0.149 |
| sha1 | thin-push | 10 | 8.586 | 19.553 | 0.439 | 0.424 |
| sha256 | 4kib-push | 10 | 0.274 | 19.601 | 0.014 | 0.015 |
| sha256 | 4kib-fetch | 10 | 0.100 | 10.301 | 0.010 | 0.011 |
| sha256 | 8mib-push | 10 | 93.486 | 81.515 | 1.147 | 1.145 |
| sha256 | 8mib-fetch | 10 | 57.782 | 170.338 | 0.339 | 0.342 |
| sha256 | 384-fetch | 10 | 9.394 | 116.005 | 0.081 | 0.082 |
| sha256 | incremental-fetch | 10 | 3.482 | 23.029 | 0.151 | 0.139 |
| sha256 | thin-push | 10 | 8.883 | 20.314 | 0.437 | 0.428 |

## Scope and verification

The guest timer includes repository open and the selected common command,
including InMemory log reads/writes, graph, pack, and ref work. The native Git
timer covers pack-objects, or strict index-pack followed by update-ref for
push, including subprocess startup. Fixture setup, seed import, and verification are excluded.
These are explicitly different storage/runtime scopes, not a claim about
complete HTTP service latency or remote object-store speed. Whole lifecycle
HTTP duration and runtime startup are recorded separately.

The run used Rust 1.97.1, Spin 4.0.2, Git 2.54.0, macOS arm64, and optimized
WASIp2. The exact base revision, source-diff hash, driver hash, fixture hashes,
and machine details are in the raw conditions. The normal lifecycle ran first
using the same component, so the compiler cache was warm; no compilation ran
concurrently with the final measured samples. This is not cold-JIT evidence.

Each invocation also completes receive, recovery, stale rejection, checkpoint,
collection, and another fresh-log recovery. Every candidate fetch is checked
for exact OIDs, standalone delta decoding, strict indexing, and fsck in an empty
or accepted-have-only receiver as appropriate. The driver independently proves
that thin input requires an external base. The 8 MiB and incremental fetches
pass the 1.10× same-run pack-size gate and all raw/framed byte caps.

A test-only store wrapper measures each selected command's calls, combined
payload bytes, and request intervals. Maximum observed calls were 23 and
combined transfer 16,787,110 bytes, within the unchanged 512/96 MiB limits.
Observed serial depth is the longest nonoverlapping interval chain, not a
causal dependency graph. Bootstrap and verification calls are outside the
selected-command measurement; the full Spin S3 adapter's separate handler
quota includes those bootstrap requests.

Independent review recomputed all 348 samples and 14 summaries, pairing order,
percentiles, ratios, fixture identity, interval depth, and resource bounds.
Strict native and WASIp2 Clippy pass. Measurement code adds no product lines.
Guest instance memory is not process RSS; see the separate Linux cgroup record.
