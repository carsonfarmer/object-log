# Local performance and compatibility baseline

## Revision and environment

- Revision: `8b9307b9529ff84195fedfa2dc512a88f64fed29`
- Date: 2026-09-02, America/Vancouver
- Host: Apple `Mac16,8`, 14 logical CPUs, 48 GiB memory
- Operating system: macOS 27.0, build 26A5421a
- Rust: 1.97.1, aarch64-apple-darwin
- Criterion: 0.7.0, optimized benchmark profile
- Backend: `object_store::memory::InMemory` wrapped by request accounting

This baseline measures process-local protocol cost. It does not measure disk,
MinIO, S3, network latency, or multi-process coordination.

## Append results

Each value is Criterion's reported 95% confidence interval. The middle value is
the point estimate.

| Workload | Time per durable commit | Reported logical throughput |
|---|---:|---:|
| Batch 1 × 32 B | 3.939–4.003 µs | 249.84–253.90 Kops/s |
| Batch 4 × 32 B | 4.086–4.188 µs | 955.07–978.95 Kops/s |
| Batch 16 × 32 B | 4.662–4.694 µs | 3.409–3.432 Mops/s |
| Batch 64 × 32 B | 7.417–7.490 µs | 8.544–8.629 Mops/s |
| Batch 256 × 32 B | 11.950–11.999 µs | 21.334–21.423 Mops/s |
| One 32 B operation | 3.774–3.784 µs | 264.31–264.97 K commits/s |
| One 256 B operation | 4.162–4.202 µs | 237.97–240.26 K commits/s |
| One 4 KiB operation | 7.931–7.984 µs | 125.25–126.09 K commits/s |

The batch cases encode one opaque byte array with `batch_size × 32` bytes. The
reported logical throughput shows protocol overhead amortization. It does not
yet parse or execute that number of application operations.

## Recovery results

Recovery loads the index and verifies all active WAL entries in parallel.

| Active tail | Recovery time | Verified entries per second |
|---:|---:|---:|
| 0 | 1.131–1.139 µs | not applicable |
| 16 | 32.569–33.123 µs | 483.05–491.27 K/s |
| 64 | 132.44–132.84 µs | 481.77–483.22 K/s |
| 256 | 522.90–525.10 µs | 487.53–489.58 K/s |
| 1,024 | 2.140–2.156 ms | 474.98–478.53 K/s |

## Contention results

Each writer prepares from the same cursor. Exactly one CAS wins. Every other
candidate returns a definite conflict.

| Concurrent candidates | Total classification time | Candidates per second |
|---:|---:|---:|
| 1 | 2.797–2.815 µs | 355.29–357.53 K/s |
| 2 | 6.693–6.820 µs | 293.26–298.82 K/s |
| 8 | 29.155–29.348 µs | 272.59–274.40 K/s |
| 32 | 125.91–127.23 µs | 251.51–254.14 K/s |

This workload measures one winner and many rejected candidates. It does not
retry the rejected logical operations.

## Local MinIO evidence

- Image: `minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
- Endpoint: ephemeral loopback port
- Bucket: new empty `object-log-test` bucket in an ephemeral container
- Result: one integration test passed in 0.08 seconds
- Covered behavior: capability probe, conditional create and update,
  conditional read, lost successful update response, pending resolution,
  checkpoint publication, process reopen, base recovery, and container cleanup

The MinIO run is a compatibility test. It is not a latency or throughput
measurement.
