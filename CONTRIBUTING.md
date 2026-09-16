# Contributing

`object-log` is a small generic WAL. Changes should preserve one conditional
head as the only mutable durable authority and keep application rules outside
the core.

## Setup

Install the Rust toolchain pinned by `rust-toolchain.toml`, the Go version in
`examples/git/go.mod`, and `jq`. The Git Makefile builds its pinned
`componentize-go` tool with Cargo.
The complete gate uses `jq` for a self-contained test of the AWS
temporary-credential helper; it does not contact AWS. Building the composed
component additionally requires `wac`; running it requires Spin. MinIO is needed
only for the opt-in storage tests.

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
