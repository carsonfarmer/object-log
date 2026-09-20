# object-log plan

## Goal

Build a small Rust library that publishes a linearizable, byte-oriented log on
conditional object storage. The library owns ordering, recovery, immutable
object integrity, checkpoints, reader retention, and bounded collection.
Applications own their operation and snapshot formats.

The Git service is the primary proof: unchanged Git clients exercise the WAL
through an established Git implementation. The key-value consumer uses sparse
radix-tree paths and the same recovery and collection contract. Its next gate
is provider and sustained-load qualification, after the Git dependency review.

## Authority and storage

- Each logical log has one small mutable `index.cbor` object.
- A conditional update of that object is the only publication point.
- Commits, payloads, nodes, checkpoints, and collection plans are immutable.
- Immutable keys bind a random physical identity to a BLAKE3 content digest.
- Store failures that can hide a successful publication return an explicit
  pending result with durable recovery evidence.
- Checkpoints bound replay. A durable positive plan and collection epoch fence
  bounded garbage collection.
- Local state is a cache. Recovery requires only the object store.
- Durable limits are authenticated in the head and must match every opener.

The durable format is canonical CBOR under `schema/object-log-v1.cddl`. It is
pre-release: incompatible development revisions use a fresh namespace rather
than compatibility readers.

## Current state

The core implements and tests:

- backend capability validation and tenant isolation;
- commit, conflict, pending-result resolution, and process-loss recovery;
- authenticated blobs, reference trees, streaming writes, and sparse reads;
- checkpoints and bounded recent-outcome resolution;
- reader retention, collection fencing, restart-safe deletion, and view expiry;
- deterministic fault injection, model tests, filesystem tests, MinIO tests,
  large collection acceptance cases, and Criterion benchmarks; and
- native and WASIp2 compilation.

The Git proof supports SHA-1 and SHA-256 repositories, protocol-v2 clone and
fetch, classic push, shallow history, tags, access control, cold recovery, and
maintenance. It has passed local MinIO and live AWS S3 qualification. Its
implementation remains outside the core.

## Release gate

Before a tagged release:

1. Keep `make check`, `make minio-test`, and the Git provider suites green.
2. Keep crate packaging limited to library source, the schema, license, public
   README, integration tests, benchmarks, and Cargo metadata.
3. Maintain public documentation for the current API and operator obligations.
4. Review dependency forks and either land their fixes upstream or document the
   exact retained revisions.
5. Qualify the Git service behind the intended public TLS, routing,
   authentication, and host-level admission layer.
6. Assign a new durable format version for any incompatible change after the
   first tagged format release.

## Change discipline

New core API must remove consumer-side protocol work or serve a demonstrated
second use case. Domain policy stays in consumers. Tests carry durable evidence;
raw command transcripts and machine-specific reports do not belong in the
repository.
