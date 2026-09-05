# Partial clone and blob filters — 2026-09-05

This #24 tranche implements protocol-v2 `filter` for `blob:none` and
`blob:limit=<unsigned bytes>[kmg]` (also accepting uppercase unit letters).
Limits omit blobs whose size is **at least** the threshold, including threshold
zero. Repeated filters, malformed/overflowing numbers, and unsupported forms
fail explicitly. Supported forms are enumerated here; `tree:`, `object:type=`,
`sparse:oid=`, and `combine:` are not implemented. Packfile URIs remain the next
required #24 tranche and are not advertised.

The public byte-oriented Repository API and durable format are unchanged.
The server retains complete ref-reachable objects; filtering changes only a fetch
pack. Client Git writes its own `.promisor` markers and remote configuration.
There is no server promisor ledger, second authority, or cache dependency.

## Selection and integrity

Explicit tree/blob wants override possession inferred from ancestor commit haves.
Direct noncommit haves still prove their entire closures. Explicit annotated tags
also make their peeled target a provided object, matching Git; descendants of an
explicit tree remain subject to the filter. The installed-Git oracle checks this
with a matrix of blob/tree/nested-tag wants and commit/blob/tree/tag haves.

Only OIDs in the authenticated ref-rooted graph are accepted, including lazy
fetches. Merely appearing in a pack index is not authorization: tests put an
unreachable blob in a live durable pack and reject its want under both filters.
Client shallow boundaries continue to constrain have closure. `include-tag`
is applied after filtering, so implicitly added tags require an emitted peeled
target. Explicitly requested tag chains retain normal provided-object behavior.

`blob:none` does not read omitted blob bodies. `blob:limit` uses the shared private
`Reader::object_size` prerequisite (`1200c1d`). Its full-blob header size is
metadata authenticated by object-log chunk evidence, not proof of content kind
or Git OID. The caller never marks content verified based on size: every retained
blob still passes full `Reader::verify` before pack emission. Delta/structural
size lookup retains the prerequisite's bounded `find` fallback and associated
materialization limits. No capacity constants were increased.

Sources used for protocol semantics and independent oracle probes:

- [Git partial clone](https://git-scm.com/docs/partial-clone)
- [Git rev-list filters](https://git-scm.com/docs/git-rev-list)
- [Git protocol v2](https://git-scm.com/docs/protocol-v2)
- [Git list-object filters](https://raw.githubusercontent.com/git/git/v2.54.0/list-objects-filter.c)

## Acceptance and review

Environment: macOS, Git 2.54.0 (Apple Git-157), Rust 1.97.1, Spin 4.0.2,
local loopback MinIO using the project's pinned image. The final candidate includes
metadata, ReceivePolicy/Spin policy, and authentication prerequisites. Dedicated
shallow/partial fixtures set `auth_mode=disabled` explicitly; authentication's
separate tests remain the auth worker's proof.

Final all-feature workspace tests pass (345 pass reports, 16 opt-in ignored).
Formatting, strict all-feature native workspace Clippy, strict Spin WASIp2 Clippy,
default-feature core WASIp2 check and release WASIp2 build pass. The local
Spin partial fixture reported success with authentication explicitly disabled.
That run does not qualify cold-process recovery: later inspection found that
terminating the Spin parent could leave its HTTP child alive. The raw output is
retained, but its cold-host claim is withdrawn in that original environment.
Docker became unresponsive after disk recovery. A subsequent process-group-aware
run against an isolated native MinIO service passed, as recorded below.

The recorded client run covers both hashes, with the restart qualification below:

- `--no-checkout --filter` clones actually omit blobs, retain threshold-selected
  small blobs, and contain `.promisor` markers plus remote filter configuration.
- Missing objects pass promisor-aware strict fsck, then unchanged Git `show` and
  checkout lazily retrieve correct bytes.
- A lazy request reports remote unavailability without creating the missing object.
  A native library helper performs real checkpoint and collection against that
  namespace, followed by successful lazy retrieval. A fresh Spin serving process
  was not reliably established by the old fixture; that stronger claim is established only by the native rerun below.
- Shallow+filtered clone, deepen and unshallow retain correct commit counts and
  connectivity. Incremental fetch omits a newly added large blob until demand.
  `fetch --refetch` with a broader supported limit fills the remaining omissions.

The maintenance helper is opt-in test code with an isolated namespace and loopback
endpoint guard. It exercises the existing public checkpoint/collection APIs;
there is no new maintenance HTTP endpoint or production command.

A separate library test expires a lazy-fetch view through actual checkpoint/GC,
then checks cumulative calls/work, response memory ownership, and exhausted retry
rejection for both hashes. The existing shallow and general workspace tests are
preserved. Git launch/packet/pack-index helpers are shared between the shallow and
partial oracles instead of copied.

Independent correctness review found explicit tag-target and tree/subtree/have
precedence cases; actual Git probes drove fixes and the expanded matrix passes.
Final review found no remaining concrete defect. Independent simplification review
suggested skipping extra possession masks for commit-only wants and consolidating
oracle helpers; both changes were made. The metadata/content verification split
was retained deliberately.

Raw command output is in [git-partial-2026-09-05](git-partial-2026-09-05/).
During final validation the shared machine exhausted disk space. This task removed
only its disposable `target/debug/incremental` cache and used
`CARGO_INCREMENTAL=0` for subsequent gates. No source/evidence was deleted.

## Resource interpretation and remaining scope

The 2 MiB deterministic incompressible-blob fixture records engine calls/work,
store GETs/download bytes and retained response size for no filter, `blob:none`,
a 4 KiB limit and a 4 MiB limit. See `filter-counters.txt` for exact conditions and
raw output. Selected observations (all byte counts are exact fixture counters):

| Hash/filter | Engine calls | Store GETs | Download bytes | Retained response bytes |
| --- | ---: | ---: | ---: | ---: |
| SHA-1, none | 6 | 4 | 2,099,392 | 2,098,185 |
| SHA-1, blob:none or 4 KiB limit | 4 | 2 | 1,049,965 | 223 |
| SHA-256, none | 6 | 4 | 2,099,494 | 2,098,227 |
| SHA-256, blob:none or 4 KiB limit | 4 | 2 | 1,050,025 | 265 |

Blob omission drastically reduces response bytes, but authenticated
chunk granularity still requires roughly 1 MiB of reads in this fixture. Retained
response bytes are an allocation reservation after the repository drops, not peak
RSS. The retry fixture precharges 1 MiB work to make counter continuity observable.

Graph construction still loads all reachable metadata and retains at most 32,768
nodes. Catalog cost remains proportional to live packs. Existing 24 MiB state,
88 MiB live, transfer/work/call/retry and response limits remain. This tranche
makes no arbitrary-scale, remote object-store throughput or new hard 128 MiB host
qualification claim. Promised objects are recoverable while reachable under the
current ref authorization policy; no perpetual retention of objects abandoned by
all server refs is introduced.

Only the root integrates main. The final partial commit depends on previously
coordinated metadata/auth/receive-policy commits; cherry-pick the scoped partial
commit, not this worker's prerequisite copies. The only durable.rs edit in the
partial commit removes the prerequisite's temporary unused-method allowance.

Process cleanup follow-up: both fixtures now launch Spin in a private process
group, terminate that group, and require the old TCP listener to refuse connections
before proceeding. Startup failures also clean up the group. A local synthetic
parent/HTTP-child test passed for both helpers, including repeated shutdown. This
validates cleanup mechanics only; provider qualification comes from the native run below.

## Native MinIO requalification

Both-hash shallow and partial unchanged-client fixtures passed after the process
cleanup fix against native MinIO RELEASE.2025-09-07T16-13-09Z, Go 1.24.6,
darwin/arm64, SHA-256
`7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4`.
This is additional provider evidence, separate from the pinned Docker image.
The dedicated temporary service and storage were removed after the run. No Docker
restart or external bucket/service deletion occurred.

The new run proves the old Spin listener refuses connections before maintenance
and restart, then both SHA-1 and SHA-256 original partial clones lazily retrieve
correct objects after checkpoint/GC through a fresh serving process. It also
passes threshold omissions, promisor configuration, strict fsck, shallow/deepen/
unshallow, incremental lazy retrieval, broader-filter refetch, unavailable-remote
failure, and the complete shallow client regression. See `native-partial-clients.txt`,
`native-shallow-clients.txt`, and `native-*-spin-sha*.txt`. This does not qualify
the unavailable Docker environment or remote object-store performance.

Fixtures accept OBJECT_LOG_MINIO_ENDPOINT, OBJECT_LOG_MINIO_BUCKET,
OBJECT_LOG_MINIO_ACCESS_KEY and OBJECT_LOG_MINIO_SECRET_KEY for a prestarted
loopback service with an existing bucket. This mode never deletes the external
service or bucket. Without these variables the pinned Docker default is retained.
