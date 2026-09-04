# Local API simplification evidence

## Result

The review reduced allocations and removed one core API responsibility. It
compares base revision `ad7fb39` with `3064006`.

`Materializer` now restores a checkpoint and replays operations. It no longer
encodes checkpoints. Each domain owns its checkpoint encoding because only the
domain knows which object references the checkpoint retains. This change
reduced the changed `Materializer` regions from 658 to 645 product lines. The
strict tests for those regions grew from 423 to 432 lines.

The review also rejected a generic retry helper. The key-value adapter can
apply an operation again after a conflict because it evaluates the operation
against the winning state. Git and SQLite must not repeat the submitted
operation. They require domain-specific conflict handling.

## Key-value adapter

The CBOR types now borrow keys and values during encoding. The borrowed-codec
change reduced product code from 431 to 429 lines. Moving checkpoint encoding
from `Materializer` to the documented `KvMachine::checkpoint` method made the
final crate 434 product lines.

The allocation probe measured these changes:

| Operation | Base allocations | Current allocations | Base allocated bytes | Current allocated bytes |
|---|---:|---:|---:|---:|
| Encode a 4,096-key checkpoint | 8,196 | 2 | 4,730,903 | 2,441,208 |
| Encode a 4 KiB set operation | 7 | 3 | Not recorded | Not recorded |

The temporary `stats_alloc` probe was removed after the measurement. The
retained tests check the canonical encoded forms and decoding limits.

## Core checkpoint encoding

The core encoder borrows checkpoint bytes and object references. Encoding a
1 MiB checkpoint changed from 8 to 4 allocations and from 5,243,305 to
4,194,666 allocated bytes.

This encoder change added 19 core product lines. Removing checkpoint encoding
from `Materializer` removed 7, for a net increase of 12 core product lines.

## SQLite adapter

The SQLite record is now one derived four-variant CBOR enum. The adapter no
longer maintains a manual optional-field map. It also uses SQLite SQL for the
NOOP checkpoint query and keeps the journal-pointer VFS read.

| Measure | Base | Current |
|---|---:|---:|
| SQLite product lines | 1,512 | 1,476 |
| Snapshot descriptor bytes | 15 | 7 |
| WAL descriptor bytes | 54 | 43 |
| Lines inside unsafe blocks | 22 | 12 |

The focused small-transaction Criterion comparison found no significant
change. The [SQLite evidence](sqlite-local-2026-09-03.md) records the retained
benchmark conditions and limits.

## Product line result

The final changed-area product counts are:

| Area | Base | Current | Change |
|---|---:|---:|---:|
| Key-value | 431 | 434 | +3 |
| Core changes | n/a | n/a | +12 |
| Git | 1,855 | 1,844 | -11 |
| SQLite | 1,512 | 1,476 | -36 |
| Git HTTP | 1,027 | 1,027 | 0 |

These areas have 32 fewer product lines in total. The key-value crate's final
count includes the inherent checkpoint method that replaced the trait method.
The Git change removes production checkpoint encoding and keeps a nine-line
test helper for restore coverage.

## Focused gates

The following focused checks passed before this document was written:

- Key-value formatting, strict all-target Clippy, all 7 tests, and the core
  recovery-limit test.
- Core formatting, strict all-target and all-feature Clippy, 49 unit tests, 20
  checkpoint tests, 28 garbage-collection tests, 21 model tests, 19 protocol
  tests, and 4 store-conformance tests. The opt-in acceptance test was ignored.
- SQLite strict all-target and all-feature Clippy, all 43 regular tests, the
  exact 1,000-record cold recovery, the pinned MinIO recovery and collection
  flow, and 6 WAL and format tests in pinned Linux.
- Materializer integration formatting, strict all-target and all-feature
  Clippy for core, key-value, and Git, 173 regular tests, and doctests. Two
  opt-in tests were ignored.

The root workspace gate remains for final integration. Live AWS and remote
object-store performance qualification remain separate.

## Follow-up

Git cold recovery still returns `ViewExpired` if garbage collection advances
the collection epoch during a pack read.
[Issue #15](https://github.com/carsonfarmer/object-log/issues/15) tracks a
bounded retry of the complete disposable-cache rebuild. Current-epoch missing
or corrupt data must remain a hard failure.
