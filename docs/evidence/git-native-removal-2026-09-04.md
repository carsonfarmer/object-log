# Native Git engine and HTTP host removal

The owner requested removal of both the previous native Git engine and the
entire `object-log-git-http` crate. This supersedes earlier retention plans.
Installed Git remains the independent test and timing reference. The measured
WASIp2 SHA-1 8 MiB push finding is unchanged and tracked in issue #23.

## Removed and retained

Removed the high-level `gix` repository, local filesystem cache API,
`open_native`, old push/fetch/checkpoint paths, `native-oracle` feature,
native-only errors, Axum server, TaskTracker/shutdown machinery and HTTP crate.
`PreparedPush` now always carries common publication state. No second native
server was moved into test support. The common engine still builds natively
and for WASIp2; Spin is the HTTP host.

Generic object-log source is unchanged. Installed Git still validates packs,
refs, exact object sets, filesystem receivers and independent performance.

## Coverage preservation

- All three GC tests retain 8,240/8,240/16,384-byte object limits and physical
  deletion/timing assertions. Cold-open expiry races real checkpoint collection.
  Current-epoch corruption remains an error without a fresh-view retry.
- Cold recovery fetches through the common engine into a fresh Git receiver,
  using strict connected indexing, ref import, fsck and content checks.
- Portable receive tests exercise pending publication before and after a hidden
  head write, dropped-log/cold token recovery, corrupt resolution evidence after
  successful publication, and expired candidates with no false success or
  republication. Independent review reran all 12 common-receive cases.
- The actual Spin/MinIO suite now runs three tests for both hashes: full
  lifecycle/checkpoint/GC/cold clone; default 8 MiB chunked push and Git probe;
  and gzip multi-round fetch over 384-commit histories with ACK and tip checks.
  Spin chooses response transfer framing; the old Axum-specific chunked-response
  header assertion was removed while Git content type and result checks remain.
- Native TaskTracker shutdown draining and detached completion after TCP
  disconnect were behaviors of the deleted host. They are removed, not claimed
  for Spin. Spin awaits publication inline; standard Git lost-reply ambiguity
  remains. Portable tests retain concurrent CAS and exact-candidate recovery.
- Criterion and request audits use the common engine. Cold recovery timing is
  open/upload-pack/deframing, excluding filesystem receiver validation. It is
  not directly comparable to old local-repository recovery timing. The audit
  captures the first cold fetch; a separate fresh fetch validates its result.

## Final validation

Combined implementation source: `4a99384`, plus workspace membership, lockfile
pruning and Makefile retargeting in the acceptance commit containing this file.

- [Final workspace gate](git-native-removal-2026-09-04/workspace.txt): 322 passed,
  zero failed, 12 opt-in ignored; formatting, strict native Clippy, locked
  default-feature core WASIp2 check and strict Spin WASIp2 Clippy pass.
- [Final runtime/provider gate](git-native-removal-2026-09-04/spin-and-minio.txt):
  all six actual Spin memory lifecycle cases, Git MinIO recovery, all three
  both-hash actual Spin MinIO cases, and migrated request accounting pass.
- [Criterion](git-native-removal-2026-09-04/criterion.txt): all six migrated
  Git benchmark cases complete. Conditions: Rust 1.97.1, macOS arm64/M4 Pro,
  optimized bench profile, in-memory store, deterministic 4 KiB/8 MiB fixtures.
  Raw warmup/sample settings remain in the log. Other validation overlapped;
  these are regression observations, not isolated comparisons or remote speed.
- Independent Rust correctness, migration and simplification reviews found no
  remaining implementation blockers. Review verified target-specific native
  test dependencies do not leak into WASIp2.

The ordinary test count changes from 359 to 322. This is not an unchanged-suite
claim: obsolete native cases and host-specific semantics are removed, useful
cases migrated, and two portable recovery tests added. Twelve are still opt-in,
but membership changed: three actual Spin provider cases replace native-host
provider/loopback coverage. Core, SQLite and GC suites remain intact.

## Audit of the earlier twelve opt-in cases

All twelve ignored cases from the `c3273b0` ordinary workspace run have separate
passing evidence; “ignored” means not run by the ordinary command:

| Earlier opt-in cases | Separate evidence |
| --- | --- |
| Core MinIO, SQLite MinIO, Git MinIO, native HTTP MinIO (4) | `local-provider-regression-2026-09-04.txt` |
| Memory 100,000-object GC and MinIO 10,001-object GC (2) | `local-provider-regression-2026-09-04.txt` |
| SQLite 1,000 transactions, staged-object accounting, Git accounting (3) | `extended-acceptance-2026-09-04.txt` |
| Shared-native MinIO (1) | `pre-http-removal-runtime.txt` in this evidence directory |
| Actual Spin MinIO (1) | `git-spin-minio-2026-09-04/run.txt` |
| Shared paired performance (1) | `git-shared-performance-2026-09-04.md` and its raw JSONL |

Historical results apply to their stated source. The final runtime gate above
validates the migrated current provider tests; no live AWS run is claimed.

## Size and dependencies

A reproducible count splits source at top-level test modules. Raw production
preambles fall from 7,526 to 4,955 lines, including comments and blank lines.
The current preambles include 70 explicitly test-only helper lines; excluding
those gives Git 4,293 + Spin 592 = **4,885 product lines**. The previously reported
7,472 baseline used slightly different blank-line accounting, so use the raw
7,526 → 4,955 comparison for a consistent reduction (2,571 lines, 34.2%).

The lockfile removes 54 package identities, adds none and changes no surviving
versions. Historical evidence remains tied to its original revisions.
