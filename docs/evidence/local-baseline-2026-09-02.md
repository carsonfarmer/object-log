# Local performance and compatibility baseline

## Revision and environment

- Revision: `d67548929a3b75e4d011efdfe682e36813a4100f`
- Date: 2026-09-02, America/Vancouver
- Host: Apple `Mac16,8`, 14 logical CPUs, 48 GiB memory
- Operating system: macOS 27.0, build 26A5421a
- Rust: 1.97.1, aarch64-apple-darwin
- Criterion: 0.7.0, optimized benchmark profile
- Backend: `object_store::memory::InMemory` wrapped by request accounting

This baseline measures process-local protocol cost. It does not measure disk,
MinIO, S3, network latency, or multi-process coordination.

## Append results

Each value is Criterion's reported 95% confidence interval.

| Workload | Time per durable commit | Reported logical throughput |
|---|---:|---:|
| Batch 1 × 32 B | 4.223–4.264 µs | 234.54–236.80 Kops/s |
| Batch 4 × 32 B | 4.447–4.553 µs | 878.61–899.47 Kops/s |
| Batch 16 × 32 B | 4.993–5.040 µs | 3.175–3.204 Mops/s |
| Batch 64 × 32 B | 7.727–7.908 µs | 8.093–8.283 Mops/s |
| Batch 256 × 32 B | 12.276–12.438 µs | 20.583–20.853 Mops/s |
| One 32 B operation | 4.156–4.209 µs | 237.59–240.60 K commits/s |
| One 256 B operation | 4.611–4.657 µs | 214.74–216.86 K commits/s |
| One 4 KiB operation | 8.266–8.367 µs | 119.52–120.97 K commits/s |

The batch cases encode one opaque byte array with `batch_size × 32` bytes. The
reported logical throughput shows protocol overhead amortization. It does not
yet parse or execute that number of application operations.

Each append iteration creates a fresh log and measures its first commit at tail
length zero. These values do not measure steady-state head growth, refresh, or
checkpoint cost.

## Recovery results

Recovery loads the index and verifies all active WAL entries in parallel.

| Active tail | Recovery time | Verified entries per second |
|---:|---:|---:|
| 0 | 1.591–1.625 µs | not applicable |
| 16 | 37.891–38.282 µs | 417.95–422.26 K/s |
| 64 | 145.70–147.71 µs | 433.29–439.26 K/s |
| 256 | 574.30–579.57 µs | 441.71–445.76 K/s |
| 1,024 | 2.350–2.374 ms | 431.32–435.76 K/s |

## Contention results

Each writer prepares from the same cursor. Exactly one CAS wins. Every other
candidate returns a definite conflict.

| Concurrent candidates | Total classification time | Candidates per second |
|---:|---:|---:|
| 1 | 3.150–3.170 µs | 315.50–317.42 K/s |
| 2 | 8.030–8.177 µs | 244.58–249.06 K/s |
| 8 | 35.985–36.362 µs | 220.01–222.31 K/s |
| 32 | 156.67–157.97 µs | 202.56–204.25 K/s |

This workload measures one winner and many rejected candidates. It does not
retry the rejected logical operations.

Compared with the first implementation baseline at revision `8b9307b`, append
point estimates are about 3–11% slower, non-empty recovery is about 10–16%
slower, and contention classification is about 13–24% slower. Empty recovery
adds about 0.47 µs. The final revision adds canonical durable formats, durable
limits, incarnation binding, checkpoint outcome evidence, bounded reads, and
recovery validation. These measurements retain the cost instead of hiding it.

## Local MinIO evidence

- Image: `minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
- Endpoint: ephemeral loopback port
- Bucket: new empty `object-log-test` bucket in an ephemeral container
- Result: one integration test passed in 0.07 seconds
- Covered behavior: capability probe, conditional create and update,
  conditional read, lost successful update response, pending resolution,
  checkpoint publication, process reopen, base recovery, and container cleanup

The MinIO run is one compatibility flow. It is not the full conformance or
protocol suite. It is not a latency or throughput measurement.
