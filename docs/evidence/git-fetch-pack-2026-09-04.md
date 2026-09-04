# Git fetch-pack task 2

Date: 2026-09-04

Revision: `b322985c616d948e7365739e3286baeaeb460acc`

## Result

Task 2 is complete. The private durable reader now writes bounded,
self-contained SHA-1 and SHA-256 fetch packs. The task does not add a public
API, dependency, named crate feature, format-name change, index output, file
path, process call, or response-stream state.

The writer resolves each selected object to its canonical durable occurrence.
It sorts output by pack content ID, source offset, and object ID. Duplicate or
reordered input produces the same bytes. It writes a version-2 pack with the
checked object count and the repository hash-format trailer.

For a full object, the writer copies the validated stored zlib stream. For a
selected delta with a selected immediate base, it writes `REF_DELTA` and reuses
the delta stream. If the output omits that base, the writer materializes the
target, verifies its object ID, and writes a full object. The result does not
need an object outside the output pack.

The writer reserves one 9,437,184-byte output allocation before it reads an
entry. The returned `Bytes` retains that reservation. Cancellation releases
it. The writer rejects the next byte before it appends it. Call, transfer, and
work counters remain on the same operation.

## Verification

The focused tests cover:

- SHA-1 and SHA-256 output;
- full-object, `REF_DELTA`, and `OFS_DELTA` reuse;
- selected delta chains with depth of at least two;
- materialization when the selected set omits the immediate base;
- deterministic output for duplicate and reordered input;
- empty packs;
- the exact output limit and the next rejected byte;
- a missing delta base;
- failure before an object GET when preflight or work limits reject the
  request; and
- output-reservation lifetime and cancellation cleanup.

Git 2.54 accepted representative packs with `index-pack --stdin --strict
--check-self-contained-and-connected`. The checks used new empty SHA-1 and
SHA-256 repositories and verified every selected object ID.

`mise exec -- make check` passed at this revision. It ran formatting, strict
workspace Clippy, all workspace tests, and the locked no-default-feature
`wasm32-wasip2` Git check. A separate standalone all-feature check also passes.
The package manifest now declares the Tokio runtime support used by the native
oracle instead of receiving that feature through workspace unification.
This manifest correction is separate from the fetch-pack writer.

## Source size

The private foundation has 2,189 raw product lines before each module's
top-level `#[cfg(test)]` section:

| Module | Product lines |
| --- | ---: |
| Pack normalization | 618 |
| Durable staging, sparse reads, and fetch writing | 750 |
| Git wire protocol | 616 |
| Operation budgets | 205 |

Task 2 added 141 product lines and 287 test lines to the accepted task-1
foundation. Its limits were 160 product lines and 450 test lines.

## Remaining work

The writer remains private and disconnected from `Repository`. No real Git
protocol-v2 client trial has run against the replacement path. Task 3 adds one
common public `Repository` for native and WASIp2. It owns the exact `View`, refs,
authenticated pack roots and sizes, `Operation`, and retained-state
reservation. Each later object-reading command creates one private `Catalog`
and `Reader`, so `ls-refs` does not load pack indexes and no self-reference is
needed. This pre-release API correction uses
`Repository::open(&Log, ObjectFormat)` and `refs(&self)`, with no work directory
or path output. Receive-pack remains later.
