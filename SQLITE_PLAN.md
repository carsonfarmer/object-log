# SQLite demonstration plan

## Outcome

Build `object-log-sqlite` as a demonstration of the generic object-log WAL.
One log is the durable history for one SQLite database. The local database is
a disposable cache. This tranche does not include a Spin factor.

Move the key-value example to `object-log-kv`. Both adapter crates use only the
public `object-log` API. The only proposed core addition is a read-only getter
that prevents a duplicate limit configuration.

```rust
impl Log {
    pub const fn options(&self) -> Options;
}
```

## Selected contract

- Use stock bundled SQLite. SQLite 3.51.3 is the safety floor because it fixes
  the WAL-reset defect. `SQLITE_CHECKPOINT_NOOP` starts in 3.53.0, so pin a
  bundled build at 3.53.0 or later and verify its runtime version. Current
  `rusqlite` 0.40.2 bundles SQLite 3.53.2.
- Fix pages at 4096 bytes. Set `locking_mode=EXCLUSIVE` before the first WAL
  access. Set `journal_mode=WAL` and require the returned value to be `wal`.
  Set and verify `wal_autocheckpoint=0`, `synchronous=NORMAL`, and
  `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`.
- Own one long-lived live-cache connection behind `&mut Database`. A checkpoint
  can use one private temporary backup destination connection that cannot open
  the live path. Reject a same-path peer. Independent hosts use separate
  caches. Object-log CAS orders their writes.
- Refresh and rebuild before every write callback and linearizable read
  callback. This makes a zero-frame result current without publishing it.
- Use one object-log commit for each SQLite transaction that changes `main`.
  Never rerun a callback after a conflict.
- Make the first changed transaction a full database snapshot. Later changed
  transactions contain raw committed WAL ranges. A NOOP checkpoint returns
  `mxFrame`, the last valid committed frame. Ignore the physical suffix.
- Store small WAL ranges and snapshots inline. Split large data into ordered
  blobs. Derive every threshold from `Log::options()`.
- For a checkpoint, use SQLite backup, publish the object-log checkpoint, wait
  for a definite result, and then run local `wal_checkpoint(TRUNCATE)`. Any
  conflict, pending result, cancellation, busy result, or failure blocks the
  cache until recovery.
- Keep the SQLite record format at `v1` before release. Later factor work can
  refine this current format without a version increment or compatibility
  reader.

`synchronous=NORMAL` can lose local work after power or operating-system
failure. This is acceptable because only object-log publication confirms
durability. A cold open always rebuilds from object-log.

The private cache state has only `Clean`, `Dirty`, `PendingCommit`, and
`PendingCheckpoint`. Set `Dirty` after local SQLite commit and before the first
object-store await. Set `PendingCommit` before the head-CAS await. Only exact
resume is valid from that state.

## First gate: WAL access

Prototype only `SQLITE_FCNTL_JOURNAL_POINTER` for `main` plus the returned
`sqlite3_file.xRead` and `xFileSize` functions. Verify exact bytes through the
NOOP `mxFrame` boundary for commit, rollback, savepoint rollback, an old
physical suffix, reset, salt change, and zero frames on each supported system.
This selected recommendation requires explicit owner approval before the
prototype. Keep all FFI in one private
module with at most 50 audited unsafe lines, small unsafe blocks, and one safety
comment for each block. Stop if the method is not correct.

Direct live `-wal` capture is rejected. It bypasses the active VFS and cannot
establish a portable SQLite contract. Do not add a custom VFS in v1. Restore
may write a validated standard WAL before SQLite opens the cache under the
selected built-in filesystem VFS.

This gate passed on macOS and Linux with bundled SQLite 3.53.2. The prototype
read the exact committed prefix after commit, rollback, savepoint rollback,
WAL reset, salt change, stale physical suffix, and truncation. Proceed with
the journal-pointer design under its single-owner and built-in-filesystem-VFS
limits. Keep the proof cases as adapter tests. See
[`docs/evidence/sqlite-wal-prototype-2026-09-03.md`](docs/evidence/sqlite-wal-prototype-2026-09-03.md).

## Public API

```rust
pub struct Database { /* private */ }
pub struct StagedWrite { /* private */ }

pub enum StageStatus {
    ReadOnly(Bytes),
    Staged(StagedWrite),
}

pub enum SqliteCheckpointStatus {
    Published(View),
    Conflict(View),
    Pending,
    Expired(View),
}

impl Database {
    pub async fn open(log: Log, cache_path: impl AsRef<Path>)
        -> Result<Self, SqliteError>;
    pub async fn read<T>(
        &mut self,
        callback: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>,
    ) -> Result<T, SqliteError>;
    pub async fn stage_write(
        &mut self,
        transaction_id: TransactionId,
        callback: impl FnOnce(&rusqlite::Transaction<'_>)
            -> rusqlite::Result<Bytes>,
    ) -> Result<StageStatus, SqliteError>;
    pub async fn publish(&mut self, staged: StagedWrite)
        -> Result<CommitStatus, SqliteError>;
    pub async fn resume(&mut self, recovery_token: &[u8])
        -> Result<Resolution, SqliteError>;
    pub async fn checkpoint(&mut self)
        -> Result<SqliteCheckpointStatus, SqliteError>;
}

impl StagedWrite {
    pub fn result(&self) -> &Bytes;
    pub fn recovery_token(&self) -> &Bytes;
}
```

`stage_write` refreshes, rebuilds if needed, runs and commits the local
transaction, and reads the new logical WAL boundary. Equal old and new
boundaries return `StageStatus::ReadOnly(result)`. This result has no durable
publication claim. A changed first transaction stages a snapshot. A later one
stages its exact WAL range and computes the core recovery token without a head
update.

The caller must persist `StagedWrite::result()` and
`StagedWrite::recovery_token()` together before `publish`. A lost caller record
cannot be recovered by this library. A conflict marks the cache dirty and does
not replay the callback. A pending checkpoint keeps in-process evidence; a
repeated `checkpoint` resolves it first. After process loss, discard the cache
and open again.

Callbacks are trusted Rust extension points. The write callback receives a
borrowed `rusqlite::Transaction`. Keep one authorizer installed through prepare
and step. Deny `ATTACH`, `DETACH`, outer transaction control, all pragmas,
extension loading, direct schema-table writes, and mutations outside `main`,
including `TEMP`. The read callback also denies mutations. Allow savepoints.
Enable defensive mode and disable trusted schema. Because `Transaction` can
access its connection, callbacks remain trusted Rust extension points. A Spin
guest will not receive this callback or SQLite handle.

## Durable SQLite record v1

Operation bytes and checkpoint descriptors use one canonical CBOR map:

| Key | Field | Rule |
|---:|---|---|
| 0 | `version` | `1` |
| 1 | `kind` | `0` snapshot; `1` WAL range |
| 2 | `page_size` | `4096` |
| 3 | `payload_len` | Exact reconstructed length |
| 4 | `inline_payload` | Complete bytes for an inline payload; omitted for chunks |
| 5 | `chunk_count` | Exact positive object count; omitted for inline payloads |
| 6 | `wal_header` | Full 32-byte header for WAL; omitted for snapshots |
| 7 | `prior_mx_frame` | Prior WAL boundary; omitted for snapshots |
| 8 | `mx_frame` | New WAL boundary; omitted for snapshots |

Keys 4 and 5 are mutually exclusive. Snapshot records omit keys 6 through 8.
WAL records require all three keys. External chunks are the ordered `Blob`
references in the enclosing commit or checkpoint. Snapshot chunks contain
whole 4096-byte pages. WAL chunks contain whole 4120-byte frames. Reject
options that cannot hold one frame and data that exceeds byte or reference
limits. [`schema/object-log-sqlite-v1.cddl`](schema/object-log-sqlite-v1.cddl)
defines the four exact map forms.

A WAL payload contains frames `prior_mx_frame + 1` through `mx_frame`. Its
length is `(mx_frame - prior_mx_frame) * 4120`. Equal boundaries have no
payload and do not publish. Within one epoch, each nonempty record has the same
valid header, magic, format, page size, and salts. Its prior boundary equals
the earlier boundary. The first WAL record after a snapshot starts at zero.

Before publication, validate the WAL header, matching frame salts, nonzero
in-range page numbers, and the full rolling checksum chain through `mxFrame`.
Each nonempty captured transaction must have exactly one database-size commit
marker, on its final frame. Restore writes the snapshot and validated WAL, then
opens SQLite. Exact final `mxFrame` verification is the production recovery
test. `PRAGMA integrity_check` is acceptance and corruption-test evidence only.
Result bytes stay in the core commit result field.

## Required evidence

| Area | Required deterministic cases |
|---|---|
| Prototype | Journal pointer reads and file size; commit; rollback; savepoint rollback; old suffix; reset; salts; zero frames; each supported OS |
| Format | Stable golden bytes; reject unknown, noncanonical, mixed payload, page, count, length, order, header, salt, checksum, commit-marker, and chunk errors |
| Bootstrap | Empty open; read-only first call; first changed snapshot; conflict; pending; confirmed truncate; later WAL replay |
| Transactions | Callback error; savepoint rollback; schema and data writes; result bytes; over 64 KiB; multiple chunks; every limit |
| Publication | Commit; conflict; lost success; all resume results; cancellation at every object-store await; callback runs once |
| Reads and races | Unchanged refresh; changed rebuild; refresh failure before callback; two cache paths race one log; same-path rejection |
| Recovery and GC | Removed cache; snapshot plus 10 and 1,000 tail records; corrupt or missing data; exact recovered `mxFrame`; retained rebuild versus collection; acceptance integrity check |
| Checkpoint | Consistent backup; publish before truncate; conflict; pending; cancellation; busy or partial truncate; new epoch; 1 MiB and 100 MiB |
| Policy and backends | Dangerous SQL and TEMP writes rejected; ordinary main DDL, DML, triggers, and savepoints work; memory and filesystem suites; one opt-in MinIO flow |

A rebuild retains its exact object-log view until all snapshot, tail, and blob
reads finish. Missing or corrupt durable data fails closed. MinIO proves only
local compatibility and cleanup.

## Benchmarks

Use Criterion with 10 samples, a 1-second warm-up, and a 2-second measurement.
Keep setup outside timing. Compare direct SQLite with the adapter for a small
inline write, a 1 MiB external write, a multi-chunk transaction, an unchanged
refreshed read, conflict rebuild, cold recovery with 10 and 1,000 tail records,
and 1 MiB and 100 MiB checkpoints. Record latency, logical and transferred
bytes, object requests, and stored WAL or snapshot bytes. Report memory and
filesystem results separately. Do not claim remote latency from MinIO.

## Limits and stop gates

- Core product change: at most 10 lines.
- SQLite product code: at most 700 lines, including at most 50 approved unsafe
  lines. Tests and support: 1,100. Benchmarks: 200.
- Implementation documentation and workspace changes: 160 lines. This budget
  excludes this task record. The key-value move has near-zero net growth.

Stop before implementation if the journal-pointer prototype fails. Stop for
owner approval before the prototype, another core API, a custom VFS, another
authority, untrusted Rust callbacks, closure replay, or multiple local
connections. Run an independent correctness and line-deletion review if product
code exceeds 700 lines or any unsafe code remains.

## Implementation tasks

1. Obtain owner approval, then prove and review journal-pointer WAL capture.
2. Create the workspace and move key-value code to its public-only crate.
3. Add and test `Log::options()`.
4. Add the SQLite crate, bundled checks, connection, and cache states.
5. Add v1 codec validation and hybrid payload chunking.
6. Add the authorizer and trusted callbacks.
7. Add retained cold rebuild and standard WAL replay.
8. Add refresh and first-change snapshot staging.
9. Add WAL staging, publication, and exact resume.
10. Add conflict recovery without callback replay.
11. Add backup, checkpoint publication, and confirmed truncation.
12. Add deterministic, fault, race, and backend tests.
13. Add Criterion cases and the opt-in MinIO flow.
14. Run correctness, unsafe, line, and simplification reviews.

## Primary references

- SQLite [3.51.3](https://sqlite.org/releaselog/3_51_3.html), [WAL reset](https://sqlite.org/wal.html#walreset_bug), and [changes](https://sqlite.org/changes.html)
- SQLite [WAL format](https://sqlite.org/walformat.html), [file format](https://sqlite.org/fileformat.html#the_write_ahead_log), and [checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html)
- SQLite [pragmas](https://sqlite.org/pragma.html), [connection settings](https://sqlite.org/c3ref/c_dbconfig_defensive.html), and [backup](https://sqlite.org/backup.html)
- SQLite [file control](https://sqlite.org/c3ref/file_control.html), [file-control operations](https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html), and [VFS I/O](https://sqlite.org/c3ref/io_methods.html)
- SQLite [corruption guidance](https://sqlite.org/howtocorrupt.html), [`ATTACH`](https://sqlite.org/lang_attach.html), [Session limits](https://sqlite.org/sessionintro.html#limitations), and [authorizer](https://sqlite.org/c3ref/set_authorizer.html)
- `rusqlite` 0.40.2 [crate](https://docs.rs/crate/rusqlite/0.40.2), [`Transaction`](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Transaction.html), [authorizer](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Connection.html#method.authorizer), and [raw handle](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Connection.html#method.handle)
