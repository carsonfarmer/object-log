# Git on Spin

This HTTP adapter runs the shared `object-log-git::Repository` engine in a
WASIp2 component. The S3 backend uses `object_store`'s AWS implementation with
its HTTP and signing interfaces adapted to Spin SDK 5.2 and RustCrypto. Git
parsing, selection, pack generation, publication, and recovery stay in the
shared engine; the object-log head remains the only mutable durable authority.

Use Rust with the `wasm32-wasip2` target, Spin 4.0.2 (the qualified runtime),
and an existing S3-compatible bucket with conditional-write support. Build from
the workspace root, outside any serving memory limit:

```sh
cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release
```

Create a private configuration file outside the checkout (for example,
`/deployment/repository.toml`, readable only by the service operator):

```toml
endpoint = "http://127.0.0.1:9000"
bucket = "git-repositories"
access_key = "replace-with-storage-access-key"
secret_key = "replace-with-storage-secret-key"
region = "us-east-1"
prefix = "object-log-git"
log_id = "repository"
object_format = "sha1"
read_only = "false"
auth_mode = "basic"
auth_read_token = ""
auth_write_token = ""
```

Before serving, set at least one HTTP token to 64 hexadecimal characters from
32 random bytes; the two tokens must differ when both are present. Empty roles
are disabled, and the empty defaults above fail closed. See [private Git
authentication](AUTH.md) for credential helpers, HTTPS, and rotation.

All values are strings. `endpoint`, `bucket`, `access_key`, and `secret_key`
are required; the other values above are defaults. Use `sha256` for a SHA-256
repository and separate log IDs for different repositories and formats. The
bucket must already exist; the adapter initializes a missing log head when it
first opens storage. Storage credentials need the backend's read, write, list,
and delete operations, including conditional creation and update. Backend
validation writes disposable probe objects even when clients only fetch.

Start a local service using the file (this avoids putting credentials in the
process argument list):

```sh
spin up --from crates/object-log-git-spin/spin.toml --listen 127.0.0.1:3000 --variable @/deployment/repository.toml
```

The repository URL is `http://localhost:3000/repo`. Upload uses protocol v2;
receive uses classic receive-pack. Storage access is configured independently
of client HTTP access. No filesystem preopens are needed. Repository state
survives fresh Spin instances in S3, not in a process-local cache.

Check the storage-backed path with `git -c protocol.version=2 ls-remote
http://127.0.0.1:3000/repo`; an empty repository succeeds without refs. Spin's
health endpoint and upload discovery alone do not check storage readiness.
Push the first `main` branch from a matching-format local repository with
`git push http://127.0.0.1:3000/repo main`, then clone with
`git -c protocol.version=2 clone http://127.0.0.1:3000/repo`.

Client HTTP access uses Basic authentication with username `git`. A read token
allows clone/fetch; a write token also allows push. Configure a credential helper
before the commands above. Use HTTPS with a trusted loopback backend for private
deployments as described in [AUTH.md](AUTH.md). For local unauthenticated fixtures
only, explicitly set `auth_mode = "disabled"` and leave both tokens empty.
No public deployment is provided here.

For a repository that should accept only Git reads, set `read_only = "true"`
and restart every serving process. Authenticated receive discovery and receive POSTs
(including Git's authentication probe) return HTTP 403 before storage access or
body collection. Clone and fetch remain available. Only the exact strings
`true` and `false` are accepted; a misspelling fails requests with HTTP 500.
This is a repository-wide Git push policy, not client authentication or a
storage read-only mode. It does not cancel a push already running, change S3
permissions, or prevent separate processes with storage access from publishing.

The adapter validates request headers before backend access and acquires a
repository operation before reading a command body. Both transmitted and
gzip-expanded bodies are limited to 10 MiB. Receive decodes bounded frames into
replayable input; small decoded scratch objects use charged request memory and
larger objects use immutable storage. Upload-pack command bodies still use
bounded collection. Response bytes retain their engine owner
through the final stream write. Spin's per-instance engine admission does not
bound aggregate memory across concurrent host instances. Run ordinary Spin
with its default allocator, connection pooling, instance count, and memory
settings. The library budgets do not require a host memory cap or Spin patch.

The S3 connector streams request and response bodies. It applies a five-second
connect timeout and thirty-second first-byte and between-byte timeouts. It
rejects an overall timeout because WASI HTTP does not expose equivalent
semantics. Automatic object-store retries are disabled; uncertain publication
uses the core recovery contract. An uncertain receive publication returns HTTP
503 with an `application/octet-stream` body containing the opaque recovery
token. An operator can retain those exact binary bytes and use `Log::resume`;
the token is not written to logs. Confirmed acceptance and rejection retain
normal Git response framing. Each invocation validates the backend and
opens the log, so measurements must include that fixed provider work.

The one-shot operator command below supports head status, exact commit-token
resumption, explicit default-branch updates, metadata checkpoints that retain
every pack, catalog migration, pack compaction, and fresh or resumed collection.
Retention management still requires shared-library calls. Issue #32 remains
open for retention commands and sustained-service qualification.

## Local operator command

On Linux or macOS, build the native command separately from the WASIp2 service:

```sh
cargo build --locked -p object-log-git-spin --features operator --bin object-log-git-maintain --release
chmod 600 /deployment/repository.toml
target/release/object-log-git-maintain --config /deployment/repository.toml status
target/release/object-log-git-maintain --config /deployment/repository.toml resume-commit --token-file /private/push.token
target/release/object-log-git-maintain --config /deployment/repository.toml checkpoint --retain-packs
target/release/object-log-git-maintain --config /deployment/repository.toml collect
target/release/object-log-git-maintain --config /deployment/repository.toml collect --resume-only
target/release/object-log-git-maintain --config /deployment/repository.toml migrate-catalog --recovery-file /private/catalog.token
target/release/object-log-git-maintain --config /deployment/repository.toml compact-packs --recovery-file /private/compaction.token
target/release/object-log-git-maintain --config /deployment/repository.toml set-default-branch --expected refs/heads/main --target refs/heads/trunk --recovery-file /private/default-branch.token
```

The command opens an existing WAL; a missing or mistyped target never creates
its head. It has no HTTP listener. It accepts the same private TOML variables as
the service, including string booleans `read_only` and `allow_non_fast_forward`.
These serving policies do not restrict privileged operator mutations. The
command validates the configured format name, but head-only status and generic
WAL resumption do not inspect or certify the repository's actual Git format.
Keep operator execution and S3 credentials within the trusted OS boundary.

Storage-only operator configs may omit all HTTP authentication fields. If any
of `auth_mode`, `auth_read_token` or `auth_write_token` is supplied, the command
validates the complete policy with the same parser as Spin before provider
access. In that case an omitted mode defaults to `basic`; omitted tokens are
empty. These credentials do not authorize or restrict local maintenance.

Use regular files with no group/other permissions (`chmod 600`) for config and
token inputs; final-path symlinks, directories and FIFOs are rejected. Config
is limited to 16 KiB and tokens to 1 MiB. The token bound covers ordinary
default-options S3 tokens, but is a supported input cap, not a universal token
schema maximum: provider version strings have no schema bound. Preserve the
exact token file until resolution is confirmed. Resumption never changes its
token input.

`status` reads the head without materializing Git state or loading pack
catalogs. It reports generation, tail count, checkpoint-through sequence when
present, collection epoch and active-plan presence. Missing heads, corrupt
heads and unsupported durable options produce bounded failure outcomes.
Backend capability checks still write/delete disposable probe objects.

Stop ingress and drain all serving processes before `resume-commit`. This is
a mutation command: `Log::resume` can stage data and publish the original
conditional update. It never rebases or submits a new transaction. A read-only
Git service is not an operator authorization boundary. An expired token means
its historical outcome is unknown, not that the push failed. Ordinary Git
clients do not guarantee token capture after a lost response.

Stop ingress and drain serving processes before `checkpoint --retain-packs` too.
Resolve known pending commit tokens first: retaining packs does not preserve
historical outcome evidence, and a checkpoint can make an old push token expire.
It calls the shared Git metadata-maintenance helper with the configured object
format. It validates authenticated metadata and publishes through the existing
checkpoint head CAS, retaining every pack proof, including unreachable packs.
It does not inspect pack catalogs, prune packs or collect objects. A confirmed
`checkpointed` result means the observed head has been checkpointed; an empty
tail is already complete and does not cause another publication. `conflict`
reports the competing head. `pending` does not report a confirmed head.

After uncertainty, run status and repeat the checkpoint command against the
fresh head. This converges maintenance state but cannot establish the exact
historical outcome of the earlier attempt: this command does not persist an
exact checkpoint token or run unbounded resolution after the helper returns.

`collect` reloads the head and resumes its authenticated positive deletion plan
if one is active. Otherwise it makes one attempt to plan and install a fresh
collection, then runs only the installed plan. It does not checkpoint, release
retentions or delete arbitrary keys. `collect --resume-only` never starts a plan;
it reports `no_active_plan` when the loaded head has none. Neither command needs
a local plan or receipt, and neither retries a conflicting installation.

Every existing retention blocks fresh planning with `retained` (exit 3).
Retentions have no expiry. The command never creates, clears, bypasses or releases
them. Their owners must explicitly release the exact IDs through the library
when protection is no longer needed; an unknown owner remains a block.

Checkpoint first when the goal is to reclaim objects protected by older WAL
history. Resolve known pending Git tokens before advancing that history, since
checkpointing can expire their exact outcome evidence. A retained-packs
checkpoint alone does not discard unreachable packs; compact them first when
needed. Checkpointing improves reclamation but is not a prerequisite for core
GC safety: the planner verifies the complete current live graph, including the
tail. Conditional head writes, retentions and collection fences govern races.
Offline maintenance windows are the initial operator workflow, keeping the
reclamation objective predictable; process draining is not a durable lock.

`no_candidates` describes that invocation's scan, not a guarantee that the
latest namespace contains no garbage. `no_active_plan` does not classify an
earlier attempt or prove prior plan-file cleanup. `collected` confirms core
completion. An uncertain installation returns `pending` without starting any
deletion; partial deletion or a lost fence-clear reply can also return pending.
Reload and repeat the command to resume the head's plan, or make a new planning
attempt if none was installed. `conflict` stops without rebasing. Any failed
invocation may have partial effects; preserve head-based recovery and never
manually clear a fence or reconstruct a local plan.

The optional `collection` JSON object reports `candidate_count`,
`candidate_bytes` and `delete_attempts` for this invocation only. Attempts are
submitted candidates, not confirmed newly deleted objects or lifetime totals.
Missing objects count as successful deletion attempts; candidate bytes are not
confirmed reclaimed bytes. A deadline or error without core counters omits them,
which means unknown rather than zero. Once no plan remains,
restart Spin and verify clone/fetch before reopening normal ingress. Scheduling
uses offline maintenance windows with backoff on conflict and alerts on repeated
pending or blocked outcomes; this command does not establish sustained service
capacity or provide an online scheduler.

`set-default-branch` publishes an explicit symbolic HEAD update through the same
WAL CAS. Stop and drain serving processes first. Both names must be full branch
refs; the target may be unborn. Existing ref OIDs and packs are preserved. Legacy
repositories start with `refs/heads/main`. A stale expected default or any
competing head update rejects this candidate; the command never rebases it.
The persisted default survives checkpoints and cold serving restarts. This is
an explicit repository update, not a Spin bootstrap variable.

`migrate-catalog` explicitly converts a legacy pack catalog to the shared tree
representation using one conditional WAL publication. Stop and drain serving
processes first. It preserves refs, the default branch and Git objects; normal
push, fetch, checkpoint and collection use the tree after migration. `migrated`
confirms publication; `already_tree` means the observed repository was already
migrated and no new transaction was published. Neither outcome deletes objects.
Conflicts are returned without rebasing. Oversized or invalid histories are
rejected through the existing shared maintenance limits.

`compact-packs` requires a migrated catalog and a stopped, drained service. It
repackages reachable Git objects into bounded replacement packs and publishes
one new catalog root, preserving every ref OID and symbolic HEAD. `compacted`
confirms that publication; pending and conflict use the same receipt behavior
as migration. Repeated commands are new attempts, not exact-attempt recovery.
Resolve known pending receipts before starting another maintenance operation.

Compaction does not checkpoint or delete objects. After confirmed compaction,
run `checkpoint --retain-packs` to advance retained WAL history, then use
`collect` to plan and run collection. Old packs remain protected until history
advances. An oversized live set fails under the shared maintenance limits without partial root
publication; newly staged immutable objects may remain for later collection.

`set-default-branch`, `migrate-catalog` and `compact-packs` require a new
`--recovery-file` path on every invocation, including an already-migrated repository. The command
reserves and syncs an empty mode-0600 file before provider access, and never overwrites an existing
path. Reservation happens before configuration validation, so even an invalid
configuration can leave an empty file. Confirmed updates, already-tree results
and conflicts leave it empty. If publication returns pending, the command writes the exact core token
and fsyncs the file and directory before reporting `recovery_token: "saved"`.
Use `resume-commit --token-file` with that file to resolve the exact attempt.

A write/fsync failure or deadline reports pending without a saved-token claim.
The file may then be empty or partial. A crash or lost response before token
persistence can leave the exact attempt unknown: observing the desired default
later establishes visibility, not which attempt published it. Likewise, an
`already_tree` observation cannot classify an earlier migration whose receipt
was lost. Do not replay an unknown update automatically. Synchronous receipt writes and fsync are not
preempted by the asynchronous backend deadline. Stronger recovery before
publication remains tracked in #32.

Every normal invocation emits one JSON line of at most 2 KiB. Output contains
static outcome names and numeric head metadata; it deliberately omits target
strings, paths, credentials, token bytes and provider diagnostics. Use the
outcome together with the exit status:

| Exit | Meaning |
| --- | --- |
| 0 | `observed`, `committed`, `not_committed`, `checkpointed`, `updated`, `compacted`, `migrated`, `already_tree`, `collected`, `no_active_plan`, `no_candidates`, or requested help. `not_committed` means resolution completed, not a successful push. |
| 2 | Invalid arguments/configuration, unavailable/non-private/oversized input, unavailable recovery output, unsupported platform or incompatible durable options. |
| 3 | `conflict`, `retained`, `stale_default`, or shared-engine `busy`; inspect the fresh state before another update. |
| 4 | `pending`/`expired`, backend unavailable, or lost output. Preserve the token; do not automatically replay expired work. |
| 5 | Invalid/corrupt evidence, a missing head, resource limit, unsupported backend, collection fence, expired view or runtime setup failure. `invalid_git_state_or_limit` covers Git validation and budget failures. No raw error chain is printed. |

Native S3 retries are disabled, connect timeout is five seconds, request timeout
is thirty seconds, and the asynchronous backend operation has a sixty-second
deadline. Deadline expiry during a mutation is pending: cancellation
cannot prove publication failed. A new resume command may retry the same token.
Input file reads precede the asynchronous deadline.

Input caps do not establish decoded-memory limits. Core resumption may verify
complete immutable dependency graphs and active collection plans; it does not
use the serving Git operation budgets. Resource measurements should use normal
runtime settings; there is no imposed Spin host-memory acceptance cap.

Checkpointing uses the shared maintenance profile: 8,192 charged calls, 88 MiB
live pool, 24 MiB retained state, 96 MiB transfer and 256 MiB work, with one
cumulative expired-view retry. Those reservations bound shared-engine work,
not the whole process; backend setup/probes precede helper admission. Oversized
metadata can still reject. See the [shared helper evidence](../../docs/evidence/git-metadata-maintenance-2026-09-05.md)
for accounting and limits. GC, retention administration and HTTP authorization
remain separate work under #32/#35.

A local signed HTTP fixture tests the transport independently of a provider:

```sh
cargo build --locked -p object-log-git-spin --example transport_probe --target wasm32-wasip2 --release
python3 crates/object-log-git-spin/tests/check_transport.py
python3 crates/object-log-git-spin/tests/check_http.py
```

The fixture checks SigV4 signatures, conditional creation and update, conflict
mapping, full and ranged reads, listing, deletion, and bounded 503 propagation.
It is not MinIO qualification or evidence of unchanged-client parity; those
belong to the workspace's provider and Git acceptance gates.
The HTTP fixture also checks both hashes with the default write policy,
read-only rejection, and invalid policy configuration without a provider.
These checks use ordinary requests without a host-slot timing workaround.

The functional fixtures exercise real Spin with its defaults. The previous
constrained-runtime launcher and diagnostics are removed. Verification results
belong in commits and the issue tracker; Git history retains prior reports.

## Optional packfile URI downloads

Set `packfile_uri_base = "https://git.example.com/repo"` to advertise protocol-v2
packfile URIs. The default is empty (disabled). Use the canonical public URL;
its authority must match the request authority. The path is exactly `/repo`,
with no credentials, query, fragment or trailing slash. HTTP is accepted only
for loopback fixtures. Forwarded headers never choose the advertised origin.

Clients opt in with `fetch.uriprotocols=http,https`. Private clients using a
credential helper must also set `http.proactiveAuth=basic`: Git's separate URI
downloader does not retry a 401 challenge like smart HTTP does. Configure the
helper for the `/repo/packfiles/` paths as well as `/repo`; keep tokens in the
helper. For example, after configuring the helper:

```sh
git -c protocol.version=2 -c fetch.uriprotocols=https -c http.proactiveAuth=basic clone https://git.example.com/repo
```

Each response can move up to eight selected blobs of at least 64 KiB into
canonical single-blob packs; remaining objects stay in the normal pack. Filter,
shallow and explicit lazy-fetch selection still determine the exact object set.
This version computes the URI checksum during negotiation and regenerates the
pack on download, so it adds compression and storage reads. Each request keeps
the existing engine limits. This is useful client functionality, not a claimed
bandwidth or latency improvement.

Every download authenticates independently and verifies current reachability.
A URL grants no access or retention: deleting refs can revoke it. Checkpointing
and collection preserve the bytes while the blob remains reachable. Downloads
support byte ranges, ETags and Git's retained temporary-pack resume. Responses
are private and must not be cached. Credentials and pack URLs contain no signed
capability or second durable authority.

The opt-in local provider check uses ordinary Spin and unchanged Git, for both
hash formats, including authentication, ranges, shallow/filtered/lazy fetches,
cold checkpoint/collection, token rotation and ref deletion:

```sh
python3 crates/object-log-git-spin/tests/check_uri.py
```

Build the release WASIp2 component first. The check accepts the same loopback
`OBJECT_LOG_MINIO_*` settings as `check_partial.py`, or starts its own pinned
MinIO container when those settings are absent.
