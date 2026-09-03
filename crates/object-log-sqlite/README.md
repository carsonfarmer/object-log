# object-log-sqlite

`object-log-sqlite` stores SQLite transactions in an `object-log`. The object
log is durable. A local SQLite database is a disposable cache that the adapter
can rebuild from a snapshot and later WAL ranges.

The crate is a local demonstration. It uses bundled SQLite, a 4096-byte page
size, and SQLite's built-in filesystem VFS. The regular tests use an in-memory
object store. The crate also has Criterion benchmarks and an opt-in loopback
MinIO test. A Spin factor remains follow-on work.

## Workflow

Open a `Database` with one `Log` and one cache path. Use `read` for a current
read. Use `stage_write` for a transaction. A changed transaction returns a
`StagedWrite`; an unchanged transaction returns its result without publishing.

Persist the staged result and recovery token together before calling `publish`.
If publication has an uncertain result, pass that token to `resume`. Do not run
the write callback again. Call `checkpoint` to publish a complete snapshot and
start a new WAL epoch.

Large snapshots and WAL ranges use ordered object chunks. Upload and recovery
keep at most 32 object operations in flight. The adapter checks configured
payload limits before it loads a snapshot or WAL range into memory.

## Safety boundary

The write callback receives a trusted `rusqlite::Transaction`. An authorizer
allows DDL and DML on the main database, including `ALTER TABLE`. It rejects
attachment, pragmas, outer transaction control, and writes outside the main
database. The adapter flushes SQLite's prepared-statement cache before each
user callback so a statement prepared under write policy cannot run under read
policy. This policy protects trusted Rust extensions. A Spin guest does not
receive the callback or SQLite handle.

Each live `Database` owns its connection and holds an operating-system lock for
its cache path. The crate rejects a second instance for that path in the same
process or another process. Independent hosts must use separate cache paths.

## Checks

From the repository root, run the local suite with:

```sh
cargo test --package object-log-sqlite --all-features
```

Run the Criterion matrix with:

```sh
cargo bench --package object-log-sqlite --bench sqlite --all-features
```

Run the loopback MinIO flow with:

```sh
make sqlite-minio-test
```

Run the exact 1,000-record cold-recovery case with:

```sh
make sqlite-recovery-acceptance
```

The MinIO script pins its container image, creates an empty bucket, and checks
container removal. See the
[local evidence](../../docs/evidence/sqlite-local-2026-09-03.md) for the
measured results and limits.

## Current limits

Object-log publication defines durability. SQLite uses `synchronous=NORMAL`, so
an operating-system or power failure can remove unpublished local changes.
Cold open rebuilds from the object log. The current crate has no Windows or
custom-VFS proof, live AWS qualification, Spin integration, sanitizer result,
Miri result, or remote performance data. It bounds each durable record but not
the aggregate retained WAL tail. Add an aggregate recovery limit or stream the
tail before multi-tenant use. SQLite callbacks, backup, and local file work run
on the caller's thread. A multi-tenant host must isolate that blocking work.
