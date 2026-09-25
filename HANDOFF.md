# object-log handoff

## Goal

Build a small generic object-storage WAL and use a complete Git service to prove
its API. Domain rules stay outside the core. The conditional log head remains
the only mutable durable authority.

Read `AGENTS.md`, `PLAN.md`, and `GIT_PLAN.md` before changing behavior.

## Repository

- `src/`: Rust WAL, authenticated object graph, recovery, checkpoints,
  retention, collection, simulator, and request limits.
- `crates/object-log-kv/`: sparse radix-tree consumer for bounded small records,
  with atomic batches, exact snapshots, scans, and root checkpoints. The guide
  defines its local qualification and caller-owned recovery/maintenance duties.
  Its qualified profile documents KV-owned per-call buffers separately from
  caller, WAL, provider, runtime, allocator, and concurrency costs.
- `integrations/spin-key-value/`: native provider for Spin 4.1's existing KV
  factor. Consumers link and register it in their own runtime; no CLI or Spin
  fork is shipped. Issue #52 covers registration, unchanged-guest execution,
  host limits, pending results, recovery, and local MinIO qualification.
- `examples/git/`: go-git smart-HTTP service and provider tests.
- `examples/wal-component/`: reusable WASIp2 object-log component with S3,
  static or IMDSv2 credentials, bounded transport, and exact recovery.
- `examples/git/qualification/aws/`: disposable S3 Terraform setup, temporary
  credentials, and optional EC2/Caddy/Cognito hosting with automatic maintenance.
- `docs/design.md` and `schema/object-log-v1.cddl`: durable protocol description
  and current pre-release schema.

## Current behavior

The WAL provides conditional publication, explicit uncertain results,
process-loss recovery, authenticated blobs and reference trees, streaming byte
objects, checkpoints, retained readers, and fenced bounded collection. It
compiles natively and for WASIp2. Memory, filesystem, fault-simulation, MinIO,
large collection, and benchmark coverage are retained.

The WAL owns authenticated subtree counts and checks publication admission,
including the commit/checkpoint envelope. Counts are conservative for shared
graphs; collection deduplicates physical keys. Git and the bridge no longer
carry chunk counts. Complete-state consumers can checkpoint their admitted
roots, but the union of historical tail roots can still exceed the bound.
The reference encoding changed; use a fresh prefix for this revision.

The core also exposes a bounded authenticated-history cursor. It returns at
most one checkpoint or commit per call, preserves complete commit metadata and
publication proofs, and remains bound to one exact view. The reusable WASIp2
component exposes that cursor directly. View-sensitive reads, staging,
publication, and checkpointing require a fully consumed recovery resource.
Uncertain commits have durable recovery tokens; uncertain checkpoints retain
their exact in-instance evidence through an owned resolver resource. The
component adds no durable authority and keeps Spin dependencies outside the
core crate.

The Git service supports unchanged SHA-1 and SHA-256 clients, protocol-v2 clone
and have-aware fetch, shallow history, classic push, branches, tags, access
control, cold recovery, automatic tail checkpoints, and explicit maintenance.
Refs and its sparse object catalog publish atomically through one WAL commit.
Local MinIO tests pass with unmodified go-git. Repository paths, formats, default
branches and per-action groups now come from configuration. Only an authorized
writer can materialize a configured repository; reads open existing state.
Cognito access-token validation and a separate administration-only machine
client are wired. Password mode is the explicit local default and requires a
password. Public EC2 HTTPS qualification has passed real Cognito browser login,
Git credential-helper refresh, expired-token rejection, independent repository
permissions, and machine-client administration. Stock Linux Git directly invoked
git-credential-oauth to refresh expired credentials before reader fetches and a
writer push. Request `openid git/access` with the helper so Cognito includes
repository group claims.

Incoming packs are staged as WAL streams. Unmodified go-git buffers delta bases
and results during import; object limits do not bound peak memory. Receive-pack
advertises `no-thin`, so ordinary clients include delta bases in their packs.
Fetch reuses compact client-provided deltas retained in the catalog, with
full-object fallback. It does not generate new deltas, so some transfers use
more bandwidth.
Partial-clone filters and packfile URIs are outside the current proof.

## Dependencies

The Git example uses unmodified upstream go-git and componentize-go, including
its bundled adapter. The SDK pins the unchanged contributor revision from
go-pkg PR #13 while it remains under review. Dependency provenance and references are in
`THIRD_PARTY.md`. Do not add another Git implementation or local storage
authority. Use ordinary Spin and unmodified
S3-compatible storage.

Spin 4.1's affected OpenTelemetry SDK remains pinned pending its coordinated
upstream upgrade. The optional Caddy host rejects oversized or overlong W3C
`baggage` before Spin; other HTTP embeddings must enforce the same boundary.

The synchronous WASIp2 service serializes allocating component calls through
lifting and upstream `Unpin`. Its HTTP body wrapper drains active reads and
rejects late reads after closure. Recheck that boundary before adding background
component calls or moving to asynchronous WASI. Buffer pins are released during
the handler; wasihttp closes its response after the handler returns. Separate
Spin requests still run concurrently.

The API and durable layout are pre-release. Use a fresh prefix after an
incompatible format change; do not add readers for discarded development
formats.

## Checks and opt-in suites

The portable project check is:

```sh
make check
```

Run the local, network-backed MinIO suite and composed Spin checks when
qualifying changes to those paths:

```sh
make minio-test
make git-build
make git-spin-config-test
```

Use `make gc-acceptance` for large collection changes and the provider commands
in `examples/git/README.md` for service changes. Network-backed tests are
opt-in, isolated, and disposable.

The service-readiness and remote HTTPS qualification in issue #45 are complete.
Issue #10 retains its completed local-Spin/live-S3 scope. Issue #47 is complete:
collection plans have an independent candidate cap. Issue #46 is closed after
deployed automatic-maintenance qualification and independent review.
KV's bounded small-record delivery in #39 is complete. The local workload reaches
65,536 one-KiB records in memory and 16,384 on MinIO, with model equality,
contention, retained scans, cold recovery and collection. Existing publication
and per-call limits remain unchanged. Issue #49 now documents a 4.07 MiB
KV-owned variable-buffer envelope for the qualified profile; whole-process RSS
also includes the database/provider, test oracle, WAL, runtime, allocator, and
concurrent calls. Issue #50 is complete: one ordered batch reuses shared paths
and stages each final node once, without raising limits.

Core performance measurements cover append through a 1,024-entry tail,
conflicts, exact recovery, checkpoints, retention and collection. Reproduction
commands are in CONTRIBUTING.md; #6 records measured conditions and results.
Criterion excludes fixture creation/destruction from operation timings.
Raw output belongs under ignored target/, not in committed reports.

Maintenance now resumes an active WAL deletion plan before loading the Git
catalog. Logical pruning/checkpointing is separate from physical /collect
followups. New plans have a per-operation candidate cap independent of the
live-graph limit. Each new plan authenticates live metadata and preserves opaque
blob keys without reading leaf payloads; normal reads and publication still
verify those bytes. Continuous reader retention can starve collection.
The optional host worker starts with logical maintenance, then uses physical
collection after `more` or `retained`, within finite per-repository budgets.
An enabled pause can retry logical
conflicts or pending results before that transition.
An optional admission pause defaults off; Caddy preserves admitted requests,
and systemd cleans the pause after worker failure. Progress during a pause
depends on admitted requests finishing within it. Never clear retentions
automatically; lost readers still require explicit drained recovery.
The bridge now explicitly selects static credentials or object_store IMDSv2
role acquisition/renewal. Native and composed stock-Spin fixtures pass, including
renewal failure. Live EC2 role replacement denied storage access, then restoring
the original role recovered exact data and accepted a new push without restarting
Spin. Natural credential expiry remains covered by the composed metadata fixture.
The component's S3 transport limits and all twelve durable WAL limits are
explicit inputs. Refreshes share cumulative transport counters. Composed
credential fixtures, local MinIO Git provider tests, native strict checks, and
locked WASIp2 checks cover the reusable interface.
Signing-key fetches have a five-second WASI deadline, including slow bodies;
stock-Spin TLS tests verify timeout and same-instance cleanup without an SDK fork.
Wrong-issuer rejection and controlled signing-key rotation have native coverage;
the run did not rotate Cognito's actual signing keys or use an alternate pool.

The full provider suite passes against the public HTTPS deployment; all five
live core S3 tests also pass. Exact complete refs were checked across all eight
repositories after process kill/automatic restart and again after an EC2 reboot.
The failure drills cold-cloned and ran fsck on the two main hash repositories.
Separate cold clones of idle and active repositories passed after scheduled
cleanup. A new nested repository was added through configuration with the same
component artifact; persisted HEAD survived a configured default-branch change.
Incompatible format configuration failed predictably before exact restoration.
HTTP redirects to HTTPS preserve the requested path and query, and normal
certificate validation passes.

The remote long-history workload passed 1,025 additional pushes per hash format,
with 421 and 403 concurrent fetch/fsck cycles in the two runs. Cold clones matched
the full histories and files. The recorded cgroup peak was 247,934,976 bytes
(236.4 MiB), with push medians of about 1.70–1.76 seconds for this workload.

Concurrent remote 513 MiB object lifecycles passed for both hash formats,
including related versions, clone, update and fetch with matching file bytes.
They completed in 646–650 seconds; service cgroup peak was 5,685,891,072 bytes
(5.30 GiB), with no unexpected restarts, on the 16 GiB EC2 host. This is a measured
workload, not an arbitrary-concurrency capacity guarantee. Bridge counters
include IMDS and retries and measure HTTP attempts/body bytes, not billed S3
requests or network traffic.

The final automatic cycle completed all eight repositories after the combined
workloads. All refs were unchanged; fresh mirror clones and full fsck passed
with 1,144 reachable commits in each main hash repository. Independent final
review passed. All created test infrastructure was removed and its absence
verified through AWS APIs. The domain registration was preserved and its
original delegation restored. The local client and temporary credentials are
removed after qualification.

Keep final documentation user-facing and current. Plans capture internal intent;
executable tests and concise commits replace raw evidence archives.
