# object-log WASIp2 component

This reusable component exposes the generic Rust `object-log` API through WIT.
It contains no Git policy and introduces no second durable authority. Component
applications can compose it with `wac` and use the WAL without implementing an
S3 client, request signing, credential renewal, or a WASI HTTP connector.

The component uses `object_store`'s `aws-base` implementation with the WASI HTTP
interface. Static credentials and IMDSv2 instance-role credentials are
supported. Transport retries preserve uncertain conditional-write results, and
HTTP call and body-byte counters remain cumulative when a session is refreshed.

## Interface

`open` creates a configured log when absent. `open-existing` never creates one.
Each asserts that its host validated the backend before traffic; neither runs the
capability probe. `validate-backend` probes conditional writes and reads, listing,
and deletion for the same endpoint, bucket, prefix, credential mode, and
effective permissions. Hosts must run it before serving and after changing
that configuration or storage policy, and block every operation until it succeeds.
The AWS Git template enforces this with a startup probe and a local readiness
gate; a plain local `spin up` needs an explicit probe before use.
Both take:

- the S3 endpoint, bucket, region, credentials, prefix, and log identity;
- every durable `object_log::Options` limit; and
- positive per-session HTTP call and transferred-byte limits.

Durable limits are authenticated in the log head and must match on every open.
Transport limits are local admission policy. A refreshed session observes a new
view while sharing the original transport counters.

`session.recover` returns a cursor bound to one exact authenticated view. Each
`recovery.next` call returns at most one item: the optional checkpoint first,
then active-tail commits in order. Commit records preserve sequence,
transaction identity, operation bytes, recorded result bytes, and object proofs.
Consumers decide what application state to retain; the component never lifts a
whole configurable tail into memory.

`recovery.latest` reads only the last active commit, or the checkpoint when the
tail is empty, and reports the active-tail length. It neither advances the cursor
nor reconstructs application state. A consumer can treat that record as its
current root only when every application record contains a complete root.

View-bound reads, writes, preparation, and checkpointing live on the recovery
resource and always use its exact view. Callers reconstruct their application
state before preparing updates. Preparation consumes and authenticates the
remaining cursor. Checkpoint publication authenticates the existing checkpoint and
complete active tail, even when the caller has read only the latest record.
`recovery.prepare` accepts a 16-byte UUID,
operation bytes, recorded result bytes, and staged object roots. Call
`candidate.recovery-token` and persist the returned bytes before
`candidate.publish` when an operation must survive process loss. Publication
reports committed, conflict, or pending explicitly. Resolve a saved token with
`session.resume`; expired evidence never means that the operation was not
committed. After `candidate.publish` returns an error, do not retry that same
candidate: its immutable object may already exist. Reopen a session and use
`session.resume` with the token instead.

`recovery.checkpoint` publishes and resolves an uncertain result once, returning
complete, conflict, or pending. Pending includes expired resolution evidence;
it never proves that the checkpoint was not published. Reopen a session and
reconstruct the durable view before attempting another checkpoint. Native Rust
callers can use `PendingCheckpoint` and `Log::resolve_checkpoint` for repeated
resolution of the exact original candidate.

Object, recovery, reader, writer, candidate, and session
values are owned component resources. Consumers must drop handles they no longer
need. Byte streams retain the core's authenticated chunk geometry and bounded
offset reads.

## Build and compose

From the repository root:

```sh
cargo build --locked --release \
  --manifest-path examples/wal-component/Cargo.toml \
  --target wasm32-wasip2
```

The artifact is
`examples/wal-component/target/wasm32-wasip2/release/object_log_component.wasm`.
The Git example demonstrates composition and supplies local Spin and MinIO
tests:

```sh
make git-check
make git-build
make git-spin-config-test
make wasi-credential-test
```

The component remains pre-release with the core crate. Its WIT package is
`object-log:storage@0.1.0`; review and pin a repository revision before use.
