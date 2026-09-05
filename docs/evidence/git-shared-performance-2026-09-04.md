# Shared Git performance acceptance

The opt-in harness exercises five deterministic fixtures for SHA-1 and SHA-256:
4 KiB push and clone, 8 MiB push and clone, a full 384-commit clone, one fetch
after that history, and a thin incremental push. The thin fixture is checked
against an empty receiver to prove that it actually contains an external delta
base. History counts and wanted/accepted-have OIDs are recorded in JSONL.

Run:

```sh
make git-shared-performance-acceptance
```

Set `OBJECT_LOG_GIT_PERFORMANCE_OUTPUT` to select the JSONL path; the default is
`target/git-shared-performance.jsonl` under the workspace. Existing Criterion,
native performance, and provider tests remain unchanged.

## Measurement conditions

This run used Git 2.54.0 (Apple Git-157), Rust 1.97.1, optimized release tests,
macOS Darwin 27.0.0 on arm64 Mac16,8, 14 logical CPUs, and 48 GiB RAM.
No system or global Git configuration is loaded. Fixture and receiver Git
configuration sets `pack.threads=1`; author, committer, dates, and pseudorandom
contents are fixed. The raw file records the product revision; the harness
source in this tranche was uncommitted while these acceptance checks ran.
Both paired engines used that same revision, process environment, Git binary,
and fixture. This is a memory-store measurement with no simulated network
latency; it makes no remote object-store performance claim.

Each case runs one warm-up pair (sample 0), then ten measured pairs. Git runs
first for even sample numbers and shared runs first for odd sample numbers.
The samples include the warm-up, but percentile summaries exclude it. The p50
uses the nearest-rank median and p95 of ten samples is the maximum. When either
shared percentile exceeds 1.25 times the corresponding Git percentile, the
harness extends to thirty measured pairs and records an owner-review flag.
That flag concerns native-oracle removal, not an automatic latency rejection.

Fetch compares the shared `Repository::open` plus `upload_pack` command with
same-run `git pack-objects --stdout --revs` using exactly the same wants/haves.
Push compares shared open, `prepare_receive`, and `publish_receive` with Git
`index-pack --strict` (and `--fix-thin` when needed) plus `update-ref` into a
fresh local receiver. Fixture creation, receiver seeding, and correctness
verification are outside both timings. Input construction is included in the
shared timing. Shared timing includes log loading, validation, and memory
storage; Git timing includes subprocess startup and filesystem work. These
are equivalent requested results with different runtime/storage boundaries,
not an isolated codec-speed comparison. No have-ignoring native fetch latency
is presented as an equivalent incremental-fetch baseline.

`FaultStore` supplies logical calls and combined upload/download bytes. A
test-only wrapper records request start/end nanoseconds; GET end includes body
collection. Serial depth is the longest nonoverlapping interval chain,
computed by earliest finish, not a causal dependency graph or network latency.
All timed operations must match the `FaultStore` request count. The raw intervals
allow that observation to be audited. There is no product instrumentation.

For every measured and warm-up fetched pack, the harness compares the exact OID
set to Git's revision walk, indexes into an empty receiver to reject external
delta bases, then runs strict indexing and fsck. Full-clone strict indexing
also requires self-contained connectivity. Incremental strict indexing uses a
receiver seeded only with accepted-have history, followed by strict fsck of the
updated target. Successful push samples are fetched back and checked the same
way. The 8 MiB and incremental fetch packs must stay within 1.10 of Git's raw
pack size. Raw/framed limits and 512-call/96-MiB transfer limits are assertions.

## Results

The raw paired samples and percentile tables are in the adjacent JSONL file.

All fourteen direction/hash cases passed, each with one warm-up and ten paired
samples. No percentile exceeded the 1.25 escalation threshold. This observation
does not authorize native-oracle removal. The complete run took 136.11 seconds.

| Hash | Fixture / direction | Shared p50 / p95 ms | Git p50 / p95 ms |
| --- | --- | ---: | ---: |
| sha1 | 4kib / push | 0.196 / 0.214 | 19.902 / 66.329 |
| sha1 | 4kib / fetch | 0.060 / 0.165 | 10.047 / 24.696 |
| sha1 | 8mib / push | 55.743 / 56.655 | 63.544 / 66.298 |
| sha1 | 8mib / fetch | 31.795 / 31.936 | 143.625 / 144.197 |
| sha1 | history / fetch | 5.125 / 5.404 | 89.177 / 99.081 |
| sha1 | incremental / fetch | 1.859 / 2.002 | 18.707 / 22.114 |
| sha1 | thin / push | 5.116 / 5.243 | 17.377 / 25.925 |
| sha256 | 4kib / push | 0.150 / 0.156 | 16.126 / 18.342 |
| sha256 | 4kib / fetch | 0.045 / 0.047 | 8.160 / 8.900 |
| sha256 | 8mib / push | 22.869 / 32.456 | 78.094 / 90.198 |
| sha256 | 8mib / fetch | 15.810 / 17.995 | 154.446 / 262.172 |
| sha256 | history / fetch | 4.177 / 5.652 | 159.491 / 203.074 |
| sha256 | incremental / fetch | 1.836 / 2.010 | 17.378 / 21.237 |
| sha256 | thin / push | 3.801 / 3.913 | 17.530 / 21.273 |

Observed maxima across shared samples:

- `raw_bytes`: 8,391,414.
- `framed_bytes`: 8,392,076.
- `logical_calls`: 23.
- `transferred_bytes`: 16,787,110.
- `serial_depth`: 23.

The 8 MiB and 384-history incremental fetch pack ratios were exactly 1.00
for both hashes. Strict receiver validation and exact OID sets passed for every
sample. Focused tests, native all-target/all-feature strict Clippy, locked
WASIp2 check and strict Clippy, formatting, and whitespace checks passed.
