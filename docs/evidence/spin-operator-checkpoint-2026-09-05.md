# Local operator retaining-pack checkpoints

The native operator now exposes `checkpoint --retain-packs` through the shared
`Repository::checkpoint_retaining_packs` helper. It passes the validated configured
Git format, opens an existing log only, and maps core Published/Conflict/Pending
directly to checkpointed (0), conflict (3), and pending (4). Empty tails are an
idempotent success. Pending contains no confirmed head metadata and does not
trigger a separate resolution pass outside the helper's budget. Unknown Git
validation/budget errors produce a static bounded failure, without error text.

This command converges against a fresh head after uncertainty. It does not
serialize exact checkpoint attempts or recover the historical outcome of a lost
attempt. Known pending commit tokens should be resolved before checkpointing;
pack retention does not prevent outcome-history expiry. Collection, pack pruning,
retention administration, and general cold-resume memory qualification remain
outside this tranche and #32 remains open.

## Functional evidence

The 12 operator tests pass. New checks require explicit `--retain-packs`, reject
destructive or arbitrary-budget flags before provider access, and verify bounded
pending JSON on deadline. Actual memory-store Git fixtures cover both hashes:
a store error after checkpoint head CAS; cancellation after successful head CAS;
fresh-head convergence with no duplicate publication; and a concurrent retention
update that produces conflict without publishing a snapshot. The command does
not treat any uncertain result as rejection or success. Core memory/filesystem
checks ran before MinIO: 173 ordinary tests passed, three provider tests ignored.
The Spin package passed seven existing tests plus the 12 operator tests; five
provider tests remained opt-in. Strict native and WASIp2 all-target/all-feature
Clippy, formatting, and release CLI/component builds passed.

The dedicated opt-in MinIO lifecycle uses a real release operator executable,
Spin 4.0.2, Rust 1.97.1, Git 2.54.0, macOS arm64 and a local Docker provider.
For SHA-1 and SHA-256 it verifies:

- Missing status/resume/checkpoint targets never initialize a head; an explicitly
  created empty log checkpoints without changing its head.
- Unchanged Git pushes create the source history; separate CLI processes resume
  a winning exact token twice and reject a losing token without rebasing.
- Two valid alternating tag records are produced by the public Git engine, then
  a trusted core-WAL producer fills a 1,024-entry tail with those opaque records.
  This is accumulated-state recovery evidence, not 1,024 HTTP pushes. Actual
  Spin ls-remote fails and its component diagnostic confirms the serving call
  ceiling, while CLI status still reports 1,024 entries.
- Wrong-format checkpointing leaves the head unchanged. A release CLI checkpoint
  clears the tail through sequence 1,023; repeating it preserves generation.
  A fresh Spin process serves a cold unchanged-client clone with exact tip,
  contents and strict fsck.
- Three more actual Spin tag pushes alternate with fresh release CLI checkpoints
  and cold Spin fetches into the filesystem clone; every cycle passes strict
  fsck and exact tag identity. All pack proofs remain retained.

The final provider run passed in 16.26 seconds. Runtime uses run.sh's existing
single-instance pooling and 128 MiB WASM memory cap. The test does not establish
a total Spin-host RSS cap, concurrency safety, arbitrary repository capacity,
or indefinite sustained operation.

## Resource evidence and remaining limits

Thirty fresh release CLI processes were measured with macOS `/usr/bin/time -l`,
excluding Cargo/compiler processes. Observed RSS ranged from 10,059,776 to
14,254,080 bytes (maximum about 13.59 MiB), including the two 1,024-tail checkpoint
runs. Raw measurements and outcomes are in the adjacent evidence directory.
These small pack/ref fixtures do not prove a general 128 MiB process bound and
were not run under a Linux cgroup. Core exact-commit resumption still lacks the
shared Git graph-memory budget and can inspect a much larger dependency graph.

Checkpointing uses the shared 8,192-call / 88 MiB live / 24 MiB retained-state /
96 MiB transfer / 256 MiB work profile and one cumulative expired-view retry.
Backend capability probes occur before helper admission, within the command's
async deadline. Input caps and the 60-second deadline are not decoded-memory
bounds. Shared accounting and fixture limits are documented in
[the helper evidence](git-metadata-maintenance-2026-09-05.md).

Independent source review checked retained-state precharge, publication overlap,
fault outcomes and CLI mapping. It identified a shared-helper empty BTreeMap
leaf accounting defect and physical collision-retry accounting caveat; these
were sent to the helper owner before integration. CLI review required explicit
pending-token ordering and a component diagnostic for the tail rejection; both
are included here. Root owns final helper correction and integration review.

Reproduction (after building the release component and CLI):

```sh
OBJECT_LOG_OPERATOR_BINARY="$PWD/target/release/object-log-git-maintain" \
  bash scripts/test-minio.sh operator_minio \
  operator_minio_status_and_exact_resume_preserve_both_hashes \
  object-log-git-spin operator
```
