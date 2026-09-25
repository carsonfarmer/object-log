# Contributing

`object-log` is a small generic WAL. Changes should preserve one conditional
head as the only mutable durable authority and keep application rules outside
the core.

## Setup

Install the Rust toolchain pinned by `rust-toolchain.toml`, the Go version in
`examples/git/go.mod`, `jq`, Python 3, `curl`, and the AWS CLI. The Git Makefile builds its pinned
`componentize-go` tool with Cargo.
The complete gate uses `jq` for a self-contained test of the AWS
temporary-credential helper; it does not contact AWS. Building the composed
component additionally requires `wac`; running it requires Spin. Local
S3-compatible tests use the pinned MinIO container image through Docker, or a
native binary named by `OBJECT_LOG_MINIO_BINARY` (which also requires `lsof`
and either `sha256sum` or `shasum`).
Community binary downloads are unavailable; a native binary can be built from
the archived MinIO source at a fixed revision:

```sh
go install github.com/minio/minio@7aac2a2c5b7c882e68c1ce017d8256be2feea27f
OBJECT_LOG_MINIO_BINARY="$(go env GOPATH)/bin/minio" make minio-test
```

The test script prints the binary version and SHA-256 digest, uses disposable
storage, and verifies that its listener stops. The default `make minio-test`
includes collection-limit and mature-tail cases. The 10,001-object collection
case remains in `make gc-acceptance`; finite provider measurements are in
`make minio-performance`.

Run the required local gate before submitting a change:

```sh
make check
```

Storage protocol changes should also run the local MinIO suite:

```sh
make minio-test
```

Git changes should follow the build and provider-test instructions in
[`examples/git/README.md`](examples/git/README.md). Network-backed tests are
opt-in and must use isolated disposable storage.

For the isolated native Spin key-value provider, run `make spin-kv-check` and
`make spin-kv-guest-test`. Its [guide](integrations/spin-key-value/README.md)
describes opt-in local MinIO tests and consumer registration.

## Performance measurements

Run optimized benchmarks and the finite operation measurements separately:

```sh
cargo bench --workspace --all-features
cargo test --release --features test-util --test performance memory_performance -- --ignored --nocapture
make minio-performance
```

Criterion covers payload sizes, replay depth, writer contention and collection
graph shapes. The finite tests report append cost as the tail grows, conflicts,
exact recovery, checkpoints and reader retention, including logical storage
calls and bytes. These counters exclude provider-internal HTTP retries.

Build before measuring process memory, avoid concurrent test workloads, and keep
raw output under ignored `target/`. Record the revision, machine, tool versions,
provider and cache conditions with each run. Fixtures use fresh namespaces;
neither suite flushes operating-system caches. The filesystem capability test
correctly rejects backends without conditional updates; local MinIO supplies
writable filesystem-backed measurements. Local results do not predict remote
storage latency.

## Change guidelines

- Keep the public API byte-oriented and small.
- Preserve explicit pending results for storage errors that can hide success.
- Treat local files and memory as disposable caches.
- Add a core capability only when a concrete consumer needs it.
- Keep durable decoding bounded and fail closed on unknown or non-canonical data.
- Include focused tests for protocol, recovery, or collection changes.
- Do not commit credentials, generated components, build output, or raw test logs.
- Update public documentation when user-visible behavior or operating limits change.

Pull requests should explain the behavior change, why it belongs in the selected
layer, and which checks passed. Performance claims need recorded conditions and
must distinguish memory, local MinIO, and remote object storage.
