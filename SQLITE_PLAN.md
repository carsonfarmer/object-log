# SQLite demonstration plan

## Outcome

`object-log-sqlite` now demonstrates the generic object-log WAL. One log is
the durable history for one SQLite database. The local database is a
disposable cache. This tranche does not include a Spin factor.

The key-value example lives in `object-log-kv`. Both adapter crates use only
the public `object-log` API. The core exposes the fixed options through this
read-only getter:

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

The private cache state has `Clean`, `Dirty`, and `PendingCheckpoint`. Set
`Dirty` before the write callback. Restore `Clean` only after an explicit
successful rollback, an unchanged transaction, a confirmed publication, or a
rebuild. A staged or uncertain commit leaves the cache dirty. Only exact
resume can classify its recovery token. `PendingCheckpoint` retains the
in-process checkpoint evidence needed by a repeated `checkpoint` call.

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
The implemented private module has 22 lines inside its unsafe blocks. The
approved limit is 50. Each block has a local safety statement, and the module
does not retain the borrowed SQLite file pointer.

## Public API

```rust
pub struct Database { /* private */ }
pub struct StagedWrite<'a> { /* private */ }

pub enum StageStatus<'a> {
    ReadOnly(Bytes),
    Staged(StagedWrite<'a>),
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
    ) -> Result<StageStatus<'_>, SqliteError>;
    pub async fn resume(&mut self, recovery_token: &[u8])
        -> Result<Resolution, SqliteError>;
    pub async fn checkpoint(&mut self)
        -> Result<SqliteCheckpointStatus, SqliteError>;
}

impl StagedWrite<'_> {
    pub fn result(&self) -> &Bytes;
    pub fn recovery_token(&self) -> &Bytes;
    pub async fn publish(self) -> Result<CommitStatus, SqliteError>;
}
```

`stage_write` refreshes and rebuilds if needed. It checks deterministic tail
and transaction-ID preconditions before it calls application code. It then
runs and commits the local transaction. A zero committed-frame count returns
`StageStatus::ReadOnly(result)`. This result has no durable publication claim.
A changed first transaction stages a snapshot without reading its WAL. A later
transaction stages its exact WAL range and computes the core recovery token
without a head update.

Snapshot and WAL sizes derive from `Log::options()`. The adapter rejects an
oversized WAL before its VFS read and rejects an oversized backup before it
loads the file into memory. Large payloads use zero-copy `Bytes` slices for
uploads. Upload and recovery preserve chunk order with at most 32 object
operations in flight.

The caller must persist `StagedWrite::result()` and
`StagedWrite::recovery_token()` together before `publish`. A lost caller record
cannot be recovered by this library. A conflict marks the cache dirty and does
not replay the callback. A pending checkpoint keeps in-process evidence; a
repeated `checkpoint` resolves it first. After process loss, discard the cache
and open again.

Callbacks are trusted Rust extension points. The write callback receives a
borrowed `rusqlite::Transaction`. Keep one authorizer installed through prepare
and step. Deny `ATTACH`, `DETACH`, outer transaction control, all pragmas,
extension loading, and mutations outside `main`, including `TEMP`. Denying
pragmas keeps `writable_schema` off, so SQLite rejects direct schema-table
writes. The read callback also denies mutations. Allow savepoints.
Enable defensive mode and disable trusted schema. Because `Transaction` can
access its connection, callbacks remain trusted Rust extension points. Flush
SQLite's prepared-statement cache before each callback so a cached statement
cannot cross a read or write policy transition. A Spin guest will not receive
this callback or SQLite handle.

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

## Evidence status

| Area | Implemented local evidence | Remaining qualification |
|---|---|---|
| WAL access | Exact committed prefixes, rollback, savepoint rollback, old suffix, reset, salts, and truncation on macOS and Linux | Windows and each additional VFS |
| Format and bounds | Canonical v1 golden bytes, corrupt records and WALs, chunk order, declared sizes, allocation bounds, and first-snapshot capacity | Release compatibility policy after the first release |
| Transactions | Changed and read-only work, callback rollback, savepoints, main DDL and DML, policy rejection, inline and chunked payloads | Spin guest boundary |
| Publication | Commit, conflict, lost success, exact resume results, cancellation, callback-once behavior, and checkpoint resolution | Live provider fault campaign |
| Recovery and GC | Deleted-cache recovery with exact 10- and 1,000-record tails, WAL verification, integrity checks, collection cleanup, and collection-race recovery | Larger retained recovery matrix |
| Checkpoints | Backup, publish-before-truncate, conflict, pending, cancellation before and after CAS, expiry, new epochs, and 1 MiB and 100 MiB benchmarks | Remote latency and request accounting |
| Backends | In-memory tests and one pinned loopback MinIO flow | Live AWS qualification |

A rebuild retries the current view if collection expires an older view during
snapshot, tail, or blob reads. It verifies that history did not change before
it accepts the rebuilt cache. Missing or corrupt current data fails closed.
MinIO proves only local compatibility and cleanup.

## Benchmarks

The Criterion suite uses 10 samples, a 1-second warm-up, and a 2-second
measurement. Its seven groups contain 11 benchmark IDs: direct and adapter
transactions at 64 bytes and 1 MiB, a 129-chunk 1 MiB WAL transaction,
unchanged adapter read, conflict publish and rebuild, cold recovery with 10
and 1,000 tail records, and checkpoints at 1 MiB and 100 MiB. Setup stays
outside the timed sections.

The retained local runs cover all 11 IDs. They use the in-memory object-store
backend on one macOS host. Criterion records latency and declared throughput.
A separate untimed audit records object requests, transferred bytes, and
durable growth without adding counters to the timed path. See the
[SQLite local evidence](docs/evidence/sqlite-local-2026-09-03.md) and
[raw intervals](docs/evidence/sqlite-criterion-2026-09-03.tsv). MinIO results
do not measure remote latency.

## Limits and stop gates

The original 700 product-line, 1,100 test-line, and 200 benchmark-line targets
triggered independent correctness and deletion reviews. The Rust-skills pass
removed 44 net product lines. The WAL boundary uses 22 of the 50 approved
unsafe lines. See the local evidence for the current repository counts and the
count command.

Stop for owner approval before another core API, a custom VFS, another durable
authority, untrusted Rust callbacks, callback replay, or multiple local
connections. Run the WAL-access proof on each newly supported system or VFS.

## Implementation tasks

1. [x] Obtain owner approval, then prove and review journal-pointer WAL capture.
2. [x] Create the workspace and move key-value code to its public-only crate.
3. [x] Add and test `Log::options()`.
4. [x] Add the SQLite crate, bundled checks, connection, and cache states.
5. [x] Add v1 codec validation and hybrid payload chunking.
6. [x] Add the authorizer and trusted callbacks.
7. [x] Add expiry-safe cold rebuild and standard WAL replay.
8. [x] Add refresh and first-change snapshot staging.
9. [x] Add WAL staging, publication, and exact resume.
10. [x] Add conflict recovery without callback replay.
11. [x] Add backup, checkpoint publication, and confirmed truncation.
12. [x] Add deterministic, fault, race, GC, and backend tests.
13. [x] Add Criterion cases and the opt-in MinIO flow.
14. [x] Run correctness, unsafe, line, and simplification reviews.

## Remaining qualification

1. Run the native memory-safety sanitizer for the WAL FFI boundary.
2. Prove Windows or another VFS before adding it to the support statement.
3. Run the reviewed live AWS campaign.
4. Bound aggregate recovery memory or stream the retained WAL tail.
5. Isolate synchronous SQLite and local file work in the host runtime.
6. Design the Spin factor around this public adapter API.

## Primary references

- SQLite [3.51.3](https://sqlite.org/releaselog/3_51_3.html), [WAL reset](https://sqlite.org/wal.html#walreset_bug), and [changes](https://sqlite.org/changes.html)
- SQLite [WAL format](https://sqlite.org/walformat.html), [file format](https://sqlite.org/fileformat.html#the_write_ahead_log), and [checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html)
- SQLite [pragmas](https://sqlite.org/pragma.html), [connection settings](https://sqlite.org/c3ref/c_dbconfig_defensive.html), and [backup](https://sqlite.org/backup.html)
- SQLite [file control](https://sqlite.org/c3ref/file_control.html), [file-control operations](https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html), and [VFS I/O](https://sqlite.org/c3ref/io_methods.html)
- SQLite [corruption guidance](https://sqlite.org/howtocorrupt.html), [`ATTACH`](https://sqlite.org/lang_attach.html), [Session limits](https://sqlite.org/sessionintro.html#limitations), and [authorizer](https://sqlite.org/c3ref/set_authorizer.html)
- `rusqlite` 0.40.2 [crate](https://docs.rs/crate/rusqlite/0.40.2), [`Transaction`](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Transaction.html), [authorizer](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Connection.html#method.authorizer), and [raw handle](https://docs.rs/rusqlite/0.40.2/rusqlite/struct.Connection.html#method.handle)
