# Spin operator status and exact commit resumption

Scope: the first status/resume portion of #32. The local one-shot command is
opt-in (`operator` feature), uses the established native S3/WAL stack, and has
no HTTP listener. Checkpointing, collection, retention administration and the
conservative metadata checkpoint budget remain separate assignments.

Base: operator proposal `03e1829`, plus core existing-only open dependency
`726a626` (the exact method and three tests from `88c9aaa`, relocated at a test
boundary with root authorization). Root must integrate the original core
dependency once and cherry-pick only the operator source commit afterward.

## Contract and review

`status` opens only an existing head and does not enter the Git engine or load
catalogs. `resume-commit` calls `Log::resume` once with the exact input bytes.
Committed, not-committed, pending and expired results stay distinct. The command
does not rebase, construct another transaction, mutate its token file or claim
that expired work failed. A deadline/output loss preserves uncertainty.

Configuration uses the same private TOML as Spin and validates both hash names
and string policy booleans before backend access. Operator privilege deliberately
bypasses serving `read_only`/`allow_non_fast_forward` policy; it is not Git HTTP
authentication. Head status cannot validate the actual Git object format.

Linux/macOS regular input files require private permissions; final symlinks,
directories and FIFOs are rejected. `O_NONBLOCK` prevents FIFO-open hangs.
Config/token reads are capped at 16 KiB/1 MiB including a growth check during
read. JSON is at most 2 KiB and contains only static strings/numeric head data.
Raw parser/provider errors, arguments, paths, credentials and tokens are not
printed. Native S3 retries are zero; connect/request timeouts are 5/30 seconds.
The asynchronous backend work has a 60-second deadline. Synchronous regular-file
input reads occur before that deadline.

Independent correctness/simplification review approved the scoped implementation.
Its collection-fence classification improvement was applied: collection fences,
expired views and unsupported backends have explicit bounded outcomes rather
than all being described as invalid evidence. No core/Git accounting changes
were made in the operator source tranche.

## Verification

- Formatting and strict native Spin all-target Clippy with `operator` pass.
- Strict WASIp2 all-feature/all-target Clippy passes. The command has an explicit
  unsupported-platform entrypoint outside native Unix; native dependencies do
  not enter the WASIp2 component. Default release WASIp2 component build passes.
- Core tests: 156 pass, one opt-in ignored, including memory/filesystem checks
  and the three supplied existing-only-open cases. These ran before MinIO.
- Spin package with operator: seven existing tests and nine new command tests
  pass; four provider tests remain opt-in in that ordinary run.
- New command tests exercise exact input boundaries/private files/FIFOs,
  strict configuration with zero provider connections, redacted JSON, a full
  1,024-entry head, exact-token duplicate and losing outcomes, foreign/corrupt
  tokens, pending/expired resolution, and a valid-digest envelope with a huge
  truncated nested CBOR length. The full-tail fixture uses generic WAL
  operations; it establishes head status availability, not Git checkpoint escape.
- A fault pauses the actual successful head CAS before its reply. Deadline
  expiry reports pending; a reopened log using the same token reports committed
  with exactly one tail entry. The expiry test uses a deliberately shortened
  outcome window to exercise classification, not to claim default-window scale.
- Opt-in local MinIO test passes SHA-1 and SHA-256 with installed Git and actual
  Spin: missing status/resume do not initialize a head; a writable push seeds
  the WAL; the shared engine prepares two candidates; separate release command
  processes publish/repeat one token and classify the other not-committed.
  Fresh Spin clone/fsck verifies the exact target OID and contents. Docker
  fixture cleanup succeeds.

Initial validation caught an invalid Clap global/required flag combination;
the command now requires `--config` before its subcommand. The first MinIO
fixture retained engine-owned token buffers and hit Busy when preparing another
candidate; it now copies receipt bytes so dropped preparation releases admission.
These were test/CLI construction fixes, not changes to WAL publication behavior.

## Measured runtime and limits

Host: macOS arm64, Rust 1.97.1, Spin 4.0.2, installed Git 2.54.0; the test
driver uses the pinned local MinIO image from `scripts/test-minio.sh`. Both
native executable and WASIp2 component are release builds. Twelve fresh native
command processes report `/usr/bin/time -l` maximum RSS from 9,945,088 to
10,321,920 bytes (maximum 9.84375 MiB) for these small fixtures. Raw per-process
timing and RSS are retained; they are not a latency benchmark or a concurrency
qualification. No process memory cap was imposed on this macOS run.

The 1 MiB token cap is an input bound, not a decoded/live-memory bound or a
universal token-schema maximum (provider version strings have no schema cap).
`Log::resume` may verify the full dependency graph and collection plan outside
Git operation budgets. Core traversal permits up to 32 concurrent node reads,
64 MiB objects and 100,000 graph objects under default durable options. The
small-fixture RSS does not qualify that full envelope under 128 MiB. A larger
graph needs separate shared accounting or an explicitly enforced and measured
deployment limit; merely adding OS RAM or input caps is not qualification.

The serving 128 MiB qualification is unchanged. Builds and executable-cache
preparation remain outside serving limits. This tranche does not silently treat
maintenance runtime as compilation or claim an unlimited runtime exemption.
#32 remains open for a fully qualified maintenance envelope and its remaining
commands; #35 retains public authentication/deployment design ownership.

Commands and full outputs: [raw validation record](spin-operator-command-2026-09-05.txt).
