# SQLite WAL access prototype

## Result

The journal-pointer approach is accepted for the SQLite adapter. The same
probe passed on native macOS and local Linux with bundled SQLite 3.53.2.

The adapter can use `SQLITE_CHECKPOINT_NOOP` to obtain the last committed WAL
frame. It can then obtain SQLite's active WAL file object with
`SQLITE_FCNTL_JOURNAL_POINTER` and read only the committed prefix through the
object's `xFileSize` and `xRead` methods.

## Contract evidence

SQLite documents that `SQLITE_FCNTL_JOURNAL_POINTER` returns the file object
for either the rollback journal or the WAL. In the 3.53.2 source, file control
selects the pager's journal object. The pager selects `sqlite3WalFile()` in WAL
mode, and that function returns the live `pWalFd` object. The `Wal` object owns
this file object and closes it when the WAL closes.

`SQLITE_CHECKPOINT_NOOP` checkpoints no frames. Its `pnLog` output is the
number of valid frames. A successful truncate checkpoint sets it to zero.
The VFS contract defines `xFileSize` and `xRead`. The implementation will treat
any non-`SQLITE_OK` exact-prefix read, including a short read, as failure.

Primary sources:

- [SQLite file control](https://sqlite.org/c3ref/file_control.html)
- [SQLite file-control operations](https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html)
- [SQLite checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html)
- [SQLite VFS I/O methods](https://sqlite.org/c3ref/io_methods.html)
- [SQLite 3.53.2 file-control routing](https://github.com/sqlite/sqlite/blob/version-3.53.2/src/btree.c#L4162-L4179)
- [SQLite 3.53.2 pager journal selection](https://github.com/sqlite/sqlite/blob/version-3.53.2/src/pager.c#L7113-L7122)
- [SQLite 3.53.2 WAL file ownership](https://github.com/sqlite/sqlite/blob/version-3.53.2/src/wal.c#L511-L515)
- [SQLite 3.53.2 WAL file close](https://github.com/sqlite/sqlite/blob/version-3.53.2/src/wal.c#L2548-L2562)
- [SQLite 3.53.2 WAL file access](https://github.com/sqlite/sqlite/blob/version-3.53.2/src/wal.c#L4633-L4637)

## Probe

The probe used `rusqlite` 0.40.2 with its bundled SQLite. It configured a
4,096-byte page size, exclusive locking, WAL mode, no automatic checkpoint,
normal synchronization, and no checkpoint on close. It then checked:

- committed DDL and data increase `mxFrame`;
- a rolled-back large transaction leaves the committed prefix unchanged;
- a savepoint rollback excludes the removed row from the committed state;
- a reset changes the WAL salts;
- a short new epoch can leave a long stale physical suffix;
- the VFS read equals the same filesystem prefix in both environments; and
- a truncate checkpoint produces zero frames and a zero-byte WAL.

The adverse suffix case was stable on both systems:

```text
sqlite=3.53.2 initial_frames=2 long_frames=203 reset_frames=1 reset_file=836392 reset_prefix=4152
```

The logical prefix contained one 4,096-byte page frame plus the 32-byte WAL
header. The physical file still contained the earlier 203-frame allocation.

## Environments

- Revision: `aed7add19076aac2d7fa4ba1459f4b601c6534d4`
- Native: Apple M4 Pro, arm64, macOS 27.0 build 26A5421a
- Linux: `rust:1.97.1-bookworm` arm64 container, image digest
  `sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`
- Rust: 1.97.1
- `rusqlite`: 0.40.2
- SQLite: 3.53.2

## Safety boundary

The probe used five small unsafe blocks and 21 lines inside those blocks. The
implementation must keep the pointer private and must not store it. It must
not call SQLite concurrently or call another SQLite interface while it uses
the file object. It must query `mxFrame` before it obtains the pointer. It must
check the physical size, split reads at the C `int` limit, and accept only
exact `SQLITE_OK` reads.

This proof covers SQLite's default Unix filesystem VFS on macOS and Linux. It
does not prove a custom VFS or Windows. The adapter must retain these cases on
each supported system. Miri cannot execute this SQLite C boundary, so native
tests and a memory-safety sanitizer must cover the retained module.

The probe source was temporary and is not product code. The adapter will keep
the minimum reviewed boundary and deterministic tests.
