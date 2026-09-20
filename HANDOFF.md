# object-log handoff

## Goal

Build a small generic object-storage WAL and use a complete Git service to prove
its API. Domain rules stay outside the core. The conditional log head remains
the only mutable durable authority.

Read `AGENTS.md`, `PLAN.md`, and `GIT_PLAN.md` before changing behavior.

## Repository

- `src/`: Rust WAL, authenticated object graph, recovery, checkpoints,
  retention, collection, simulator, and request limits.
- `crates/object-log-kv/`: experimental sparse radix-tree consumer with atomic
  batches, exact snapshots, scans, and root checkpoints. Local correctness and
  logical-I/O tests pass; provider and sustained-load qualification remain.
- `examples/git/`: go-git smart-HTTP service and provider tests.
- `examples/wal-component/`: WASIp2 bridge from the Git service to the Rust WAL.
- `examples/git/qualification/aws/`: optional disposable S3 Terraform setup and
  temporary-credential helper.
- `docs/design.md` and `schema/object-log-v1.cddl`: durable protocol description
  and current pre-release schema.

## Current behavior

The WAL provides conditional publication, explicit uncertain results,
process-loss recovery, authenticated blobs and reference trees, streaming byte
objects, checkpoints, retained readers, and fenced bounded collection. It
compiles natively and for WASIp2. Memory, filesystem, fault-simulation, MinIO,
large collection, and benchmark coverage are retained.

The Git service supports unchanged SHA-1 and SHA-256 clients, protocol-v2 clone
and have-aware fetch, shallow history, classic push, branches, tags, access
control, cold recovery, automatic tail checkpoints, and explicit maintenance.
Refs and its sparse object catalog publish atomically through one WAL commit.
Local MinIO tests pass with unmodified go-git. Repeat remote qualification
before relying on its changed memory and upload behavior.

Incoming packs are staged as WAL streams. Unmodified go-git buffers delta bases
and results during import; object limits do not bound peak memory. Receive-pack
advertises `no-thin`, so ordinary clients include delta bases in their packs.
Fetch reuses compact client-provided deltas retained in the catalog, with
full-object fallback. It does not generate new deltas, so some transfers use
more bandwidth.
Partial-clone filters and packfile URIs are outside the current proof.

## Dependencies

The Git example uses unmodified upstream go-git and componentize-go, including
its bundled adapter. The SDK pins the unchanged contributor revision from
go-pkg PR #13 while it remains under review. Dependency provenance and references are in
`THIRD_PARTY.md`. Do not add another Git implementation or local storage
authority. Use ordinary Spin and unmodified
S3-compatible storage.

The synchronous WASIp2 service serializes allocating component calls through
lifting and upstream `Unpin`. Its HTTP body wrapper drains active reads and
rejects late reads after closure. Recheck that boundary before adding background
component calls or moving to asynchronous WASI. Buffer pins are released during
the handler; wasihttp closes its response after the handler returns. Separate
Spin requests still run concurrently.

The API and durable layout are pre-release. Use a fresh prefix after an
incompatible format change; do not add readers for discarded development
formats.

## Gates

Run:

```sh
make check
make minio-test
make git-build
make git-spin-config-test
```

Use `make gc-acceptance` for large collection changes and the provider commands
in `examples/git/README.md` for service changes. Network-backed tests are
opt-in, isolated, and disposable.

Before public deployment, qualify the exact TLS, routing, identity, monitoring,
and host-level admission layer. This is deployment work, not a new WAL protocol.

Keep final documentation user-facing and current. Plans capture internal intent;
executable tests and concise commits replace raw evidence archives.
