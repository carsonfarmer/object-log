# Large garbage-collection acceptance

## Outcome

Revision `576093b7b94177e7f12bab7bf5e16e13c6d9213f` passes the new
opt-in `make gc-acceptance` target.

| Backend | Unreachable objects | Timed collection phase | Deadline |
|---|---:|---:|---:|
| In-memory | 100,000 | 1.716509083 s | 30 s |
| Local MinIO | 10,001 | 1.608864583 s | 30 s |

The timed phase includes `start_collection` and `resume_collection`. It does
not include object creation.

Each case proved these terminal conditions:

- The start and finish reports contained the exact candidate count and byte
  count.
- The collection epoch advanced once.
- The committed root node and its child blob remained readable.
- Exactly four durable objects remained: the head, one commit, one node, and
  one blob.
- No collection-plan object remained.
- A second collection reported no candidates and did not change the generation
  or collection epoch.

## Environment

- Apple M4 Pro, arm64.
- macOS 27.0, build 26A5421a.
- Rust 1.97.1.
- MinIO image
  `minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
- The MinIO client and server shared one host and used a loopback endpoint.

The MinIO script created a new container and empty bucket. It removed the
container after the test.

## Limits

This acceptance test covers liveness and correctness. The elapsed values are
diagnostic results from one run, not Criterion benchmark results. The
30-second limit is a broad local failure boundary and makes no performance
promise.

The object-store `LocalFileSystem` backend does not supply the conditional
update operation that the log publication contract requires. Local MinIO gives
filesystem-backed object storage with the required S3-compatible operation.
This run does not provide cloud or remote-object-store evidence.
