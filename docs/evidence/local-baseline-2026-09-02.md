# Local performance and compatibility baseline

## Revision and environment

- Revision: `ff13f52ec3cdedc48f8f0107cc49b2e35e9d14af`
- Date: 2026-09-02, America/Vancouver
- Host: Apple `Mac16,8`, 14 logical CPUs, 48 GiB memory
- Operating system: macOS 27.0, build 26A5421a
- Rust: 1.97.1, aarch64-apple-darwin
- Criterion: 0.7.0, optimized benchmark profile
- Backend: `object_store::memory::InMemory` wrapped by request accounting

The raw Criterion slope estimates for all 19 cases are retained in
[`criterion-slope-2026-09-02.tsv`](criterion-slope-2026-09-02.tsv). Times are
in nanoseconds. The table below presents those same 95% confidence intervals
with readable units and derived throughput.

This baseline measures process-local protocol cost. It does not measure disk,
MinIO, S3, network latency, or multi-process coordination.

## Append results

Each value is Criterion's reported 95% confidence interval.

| Workload | Time per durable commit | Reported logical throughput |
|---|---:|---:|
| Batch 1 × 32 B | 3.954–3.991 µs | 250.60–252.92 Kops/s |
| Batch 4 × 32 B | 4.176–4.224 µs | 947.07–957.84 Kops/s |
| Batch 16 × 32 B | 4.783–4.825 µs | 3.316–3.345 Mops/s |
| Batch 64 × 32 B | 7.395–7.512 µs | 8.520–8.654 Mops/s |
| Batch 256 × 32 B | 11.488–11.619 µs | 22.033–22.284 Mops/s |
| One 32 B operation | 3.996–4.028 µs | 248.26–250.26 K commits/s |
| One 256 B operation | 4.422–4.460 µs | 224.20–226.14 K commits/s |
| One 4 KiB operation | 7.863–7.929 µs | 126.12–127.18 K commits/s |

The batch cases encode one opaque byte array with `batch_size × 32` bytes. The
reported logical throughput shows protocol overhead amortization. It does not
yet parse or execute that number of application operations.

Each append iteration creates a fresh log and measures its first commit at tail
length zero. These values do not measure steady-state head growth, refresh, or
checkpoint cost.

## Staged-payload results

These cases include one content-addressed payload upload, payload verification,
one WAL entry upload, and one head CAS. The backend remains process-local
memory.

| Payload | Time per durable commit | Payload throughput |
|---:|---:|---:|
| 64 KiB | 57.774–58.184 µs | 1.049–1.056 GiB/s |
| 1 MiB | 841.70–846.62 µs | 1.154–1.160 GiB/s |

## Recovery results

Recovery loads the index and verifies all active WAL entries in parallel.

| Active tail | Recovery time | Verified entries per second |
|---:|---:|---:|
| 0 | 1.532–1.541 µs | not applicable |
| 16 | 37.326–37.989 µs | 421.18–428.65 K/s |
| 64 | 145.23–146.40 µs | 437.16–440.67 K/s |
| 256 | 577.78–583.10 µs | 439.03–443.07 K/s |
| 1,024 | 2.313–2.342 ms | 437.30–442.77 K/s |

Recovery verifies the index, WAL envelopes, reference metadata, and parent
chain. It does not fetch payloads or reference nodes. Adapters read and verify
those objects on demand.

## Contention results

Each writer prepares from the same cursor. Exactly one CAS wins. Every other
candidate returns a definite conflict.

| Concurrent candidates | Total classification time | Candidates per second |
|---:|---:|---:|
| 1 | 3.150–3.178 µs | 314.69–317.47 K/s |
| 2 | 7.824–7.915 µs | 252.67–255.61 K/s |
| 8 | 35.078–35.542 µs | 225.09–228.06 K/s |
| 32 | 151.85–153.70 µs | 208.20–210.73 K/s |

This workload measures one winner and many rejected candidates. It does not
retry the rejected logical operations.

Criterion detected faster append results than the prior hardened revision
`d675489`. Non-empty recovery did not change significantly. It detected no
change for one-writer contention and small improvements for 2, 8, and 32
writers. These are two local runs, not a stable regression threshold.

## Local MinIO evidence

- Harness and run revision: `ff13f52ec3cdedc48f8f0107cc49b2e35e9d14af`
- Image: `minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
- Endpoint: ephemeral loopback port
- Bucket: new empty `object-log-test` bucket in an ephemeral container
- Result: one integration test passed in 0.04 seconds
- Covered behavior: capability probe, conditional create and update,
  conditional read, lost successful update response, pending resolution,
  checkpoint publication, process reopen, base recovery, and container cleanup

The MinIO run is one compatibility flow. It is not the full conformance or
protocol suite. It is not a latency or throughput measurement.
