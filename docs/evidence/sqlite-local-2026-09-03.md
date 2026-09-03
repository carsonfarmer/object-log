# Local SQLite adapter evidence

## Result

`object-log-sqlite` implements the selected local demonstration. It stores a
canonical v1 snapshot record for the first changed transaction and canonical
v1 WAL records for later changed transactions. A local SQLite file is a
disposable cache. Cold open rebuilds it from the object log.

At `bc1e814`, the regular suite has 43 tests: 6 unit, 4 allocation-bound, 16
database, 11 fault, 1 garbage-collection, 1 collection-race, and 4 recovery
and policy tests. The separate acceptance target runs an exact 1,000-record
recovery case. The MinIO test also stays outside regular runs.

Relevant revisions:

- Final product code: `1cbde302f01cc249efaf7da31051c092a8a318c2`.
- Final regular and acceptance-test layout:
  `bc1e814f195acfbee654e3d1ae981c453e670379`.
- Retained base Criterion run: `626311062b46cb8acae8982773bc12bb54318a46`.
  The chunked WAL result is from `d36b987`, and the final conditional-read
  result is from `1cbde30`.
- Loopback MinIO implementation: `907ba8ae76911dd7c2ae7fb867433f39d51da3e9`
  and its target in `6f9cb9543b5779a87247be56b219be50d5d16930`.

## Durable and memory bounds

[`schema/object-log-sqlite-v1.cddl`](../../schema/object-log-sqlite-v1.cddl)
defines the four exact snapshot and WAL record forms. The decoder requires
canonical CBOR, version 1, 4,096-byte pages, exact payload lengths, and one
valid inline or external form. Recovery validates snapshot headers, WAL
headers, frame salts, page numbers, commit markers, frame order, and the full
rolling checksum chain.

The adapter derives snapshot and WAL capacities from `Log::options()`. It
rejects an oversized WAL range after the NOOP frame-count query and before the
VFS read or allocation. It checks a private backup file's size before it loads
the snapshot into memory. Recovery checks each record's declared object sizes
and aggregate before allocation or object reads. Fallible reserve handles each
large allocation.

External uploads use zero-copy `Bytes` slices. Upload and recovery reads keep
at most 32 object operations in flight and preserve chunk order.

## SQLite boundary

The crate uses bundled SQLite 3.53.2, a 4,096-byte page size, exclusive
locking, WAL mode, no automatic checkpoint, normal synchronization, no
checkpoint on close, defensive mode, and an untrusted schema. The authorizer
allows main-database DDL and DML, including `ALTER TABLE` through the database
name carried by that action. It rejects pragmas, attachment, transaction
control, and writes outside `main`. The adapter flushes SQLite's
prepared-statement cache before each user callback. A statement prepared under
write policy therefore cannot run later under read policy.

The private WAL module obtains the committed frame count with
`SQLITE_CHECKPOINT_NOOP`. It then reads the active WAL through
`SQLITE_FCNTL_JOURNAL_POINTER` and the borrowed VFS methods. Its unsafe blocks
contain 22 lines. The approved limit is 50. Each block has a local safety
statement, and no borrowed pointer leaves the `committed` function.

This command reproduces the unsafe line count:

```sh
awk '/unsafe \{/ { active=1 } active { count++ } active && /}/ { active=0 } END { print count }' \
  crates/object-log-sqlite/src/wal.rs
```

The earlier WAL probe passed on the default Unix filesystem VFS on macOS and
Linux. See [the WAL prototype record](sqlite-wal-prototype-2026-09-03.md).

## Garbage collection

The focused memory-backed test creates one database, eight checkpoint cycles,
three updates per cycle, and one final WAL update. Collection removed 228
objects and left the exact 17-object live set. Reported runs were between 5.8
and 6.7 ms. A rerun for this checkpoint reported 6.588375 ms. The timed section
has a 10-second failure bound.

The test removes the local cache, rebuilds from the collected history, checks
the logical row and `PRAGMA integrity_check`, and then proves that a second
collection is empty. These process-local timings do not measure filesystem or
network storage.

## Loopback MinIO

`make sqlite-minio-test` passed. The test binary reported 0.28 seconds. The
script used this pinned image:

```text
minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
```

The script created a new empty bucket on an ephemeral loopback port. The test
covered multi-object snapshot and WAL payloads, a hidden successful
publication, exact resume, checkpointing, collection, deleted-cache recovery,
and integrity checks. The cleanup check confirmed that the test container no
longer existed. The 0.28-second value excludes container startup and is not a
remote-performance result.

## Criterion results

The suite uses an in-memory object store on an Apple M4 Pro with 48 GiB of
memory. The host ran macOS 27.0 build 26A5421a and Rust 1.97.1 for
`aarch64-apple-darwin`. Each group uses 10 samples, a 1-second warm-up, and a
2-second measurement. The table gives the point estimate and 95% confidence
interval.

| Benchmark | Point estimate | 95% interval |
|---|---:|---:|
| Small direct transaction | 100.739 us | 59.288-158.069 us |
| Small adapter transaction | 67.109 us | 66.589-67.909 us |
| 1 MiB direct transaction | 3.761 ms | 3.710-3.808 ms |
| 1 MiB adapter transaction | 4.003 ms | 3.956-4.069 ms |
| 1 MiB adapter transaction, 129 chunks | 5.264 ms | 5.209-5.322 ms |
| Unchanged adapter read | 1.249 us | 1.244-1.255 us |
| Conflict publish and rebuild | 299.347 us | 292.622-309.847 us |
| Cold recovery, 10 tail records | 385.307 us | 382.043-389.305 us |
| Cold recovery, 1,000 tail records | 12.396 ms | 12.256-12.561 ms |
| 1 MiB checkpoint | 2.382 ms | 2.318-2.473 ms |
| 100 MiB checkpoint | 230.326 ms | 213.866-261.499 ms |

Conditional refresh made the unchanged read 61.7% faster and reduced its head
transfer to zero bytes. Criterion classified 2 of the 10 small direct
transaction samples as high severe outliers. The 100 MiB checkpoint had 3
outliers and measured 434.17 MiB/s, with a 382.41-467.58 MiB/s interval. The
[machine-readable intervals](sqlite-criterion-2026-09-03.tsv) retain the
reported nanosecond values.

These benchmarks measure one local process and an in-memory object store. They
do not report remote latency, memory use, or multi-process throughput. The
transaction timer excludes checkpoint and garbage-collection work. Checkpoint
setup warms the local database, and cold recovery uses a hot in-memory object
store. The direct path does not install the adapter's SQLite policy settings.

## Untimed object-store accounting

A separate audit at `c73b4fe` disabled detailed event recording and inspected
the counters after each operation. These counters did not run in the Criterion
timed path. Durable growth is namespace growth before garbage collection.

| Operation | Logical bytes | Requests | Uploaded | Downloaded | Durable growth |
|---|---:|---:|---:|---:|---:|
| 64-byte update | 64 | 1 GET, 2 PUT | 4,727 | 333 | 4,394 |
| 1 MiB update | 1,048,576 | 2 GET, 3 PUT | 1,059,507 | 1,059,171 | 1,059,176 |
| Recover 1,000 updates | 64,000 | 1,003 GET | 0 | 4,492,914 | 0 |
| 1 MiB checkpoint | 1,048,576 | 3 GET, 3 PUT | 1,057,315 | 1,057,198 | 1,057,075 |
| 100 MiB checkpoint | 104,857,600 | 4 GET, 4 PUT | 104,968,806 | 104,968,688 | 104,968,564 |

Publication reads every newly uploaded external object back through the core
dependency validator. A checkpoint after 1,000 small WAL records used 1,001
GET requests, 2 PUT requests, 4,412,518 downloaded bytes, and 88,424 uploaded
bytes. These costs are correct under the current untrusted-reference API, but
they are not suitable remote-storage targets. A later core design must remove
the read-back without accepting forged or missing object references.

## Simplification and line counts

The Rust-skills pass removed 44 net product lines. It removed copied source
views, repeated status state, a second WAL checksum scan, redundant record and
object checks, and per-callback policy clones. It retained the boxed prepared
commit because direct storage made the public result enum at least 664 bytes.

At `bc1e814`, the SQLite adapter contains 1,507 product lines, 2,585 test lines,
and 390 benchmark lines. The broader policy matrix superseded a 52-line policy
test, which the review removed. The review found no additional helper, layer,
or comment that it could remove without weakening a required boundary.

The line snapshot at `bc1e814f195acfbee654e3d1ae981c453e670379`
excludes `Cargo.lock` and `.gitignore`:

| Category | Lines |
|---|---:|
| Product | 6,371 |
| Test and support | 9,612 |
| Benchmark | 854 |
| Documentation | 2,671 |
| Schema | 184 |
| Operator and infrastructure | 229 |

The count treats `src/sim.rs` as test support. It moves each source file's
`#[cfg(test)]` suffix from product to test. This command reproduces the table:

```sh
revision=bc1e814f195acfbee654e3d1ae981c453e670379
git ls-tree -r --name-only "$revision" | while IFS= read -r file; do
  case "$file" in
    benches/*.rs|crates/*/benches/*.rs)
      git show "$revision:$file" | awk '{ print "benchmark" }' ;;
    src/sim.rs|tests/*.rs|crates/*/tests/*.rs)
      git show "$revision:$file" | awk '{ print "test" }' ;;
    src/*.rs|crates/*/src/*.rs)
      git show "$revision:$file" | awk \
        'BEGIN { kind="product" } /^#\[cfg\(test\)\]/ { kind="test" } { print kind }' ;;
    schema/*.cddl)
      git show "$revision:$file" | awk '{ print "schema" }' ;;
    *.md|docs/*)
      git show "$revision:$file" | awk '{ print "documentation" }' ;;
    Cargo.toml|crates/*/Cargo.toml|Makefile|scripts/*.sh|rust-toolchain.toml)
      git show "$revision:$file" | awk '{ print "operator" }' ;;
  esac
done | sort | uniq -c
```

The table stays fixed to that revision. Record a code revision before comparing
later counts.

## Limits

- No live AWS test has run.
- No Windows or custom-VFS proof has run.
- No native sanitizer or Miri result exists for this adapter.
- The MinIO test checks compatibility and cleanup on loopback. It does not
  qualify S3.
- The Criterion results do not predict remote object-store performance.
- Recovery bounds each record, but it does not bound the aggregate retained
  WAL tail. The 32-operation transfer limit is also a count limit, not a byte
  limit. Add an aggregate recovery limit or stream validated WAL ranges before
  using this adapter as a multi-tenant Spin factor.
- SQLite callbacks, backup, WAL capture, and local file operations are
  synchronous. A multi-tenant host must run each database owner where this
  work cannot block unrelated tenants.
