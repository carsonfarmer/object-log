# object-log-sqlite

`object-log-sqlite` stores SQLite transactions in an `object-log`. The object
log is durable. A local SQLite database is a disposable cache that the adapter
can rebuild from a snapshot and later WAL ranges.

The crate is a local demonstration. It uses bundled SQLite, a 4096-byte page
size, and SQLite's built-in filesystem VFS. The regular tests use an in-memory
object store. MinIO, benchmarks, and a Spin factor remain follow-on work.

## Workflow

Open a `Database` with one `Log` and one cache path. Use `read` for a current
read. Use `stage_write` for a transaction. A changed transaction returns a
`StagedWrite`; an unchanged transaction returns its result without publishing.

Persist the staged result and recovery token together before calling `publish`.
If publication has an uncertain result, pass that token to `resume`. Do not run
the write callback again. Call `checkpoint` to publish a complete snapshot and
start a new WAL epoch.

## Safety boundary

The write callback receives a trusted `rusqlite::Transaction`. An authorizer
rejects attachment, pragmas, outer transaction control, and writes outside the
main database. This policy is a guard for trusted Rust code. It is not a guest
sandbox, and a Spin guest does not receive the callback or SQLite handle.

Each live `Database` owns its connection and holds an operating-system lock for
its cache path. The crate rejects a second instance for that path in the same
process or another process. Independent hosts must use separate cache paths.

## Current limits

Object-log publication defines durability. SQLite uses `synchronous=NORMAL`, so
an operating-system or power failure can remove unpublished local changes.
Cold open rebuilds from the object log. The current crate has no Windows or
custom-VFS proof, production object-store qualification, Spin integration, or
remote performance data.
