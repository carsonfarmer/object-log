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
crates/object-log-git-spin/run.sh --listen 127.0.0.1:3000 --variable @/deployment/repository.toml
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
gzip-expanded bodies are limited to 10 MiB. During decompression the two host
buffers can coexist (20 MiB); these belong to the runtime allowance until the
engine charges the command input. Response bytes retain their engine owner
through the final stream write. Spin's per-instance engine admission does not
bound aggregate memory across concurrent host instances. The launch command
forces Spin's pooling allocator with `SPIN_WASMTIME_POOLING=1` and limits it
to one live component instance. Do not disable pooling. Unsupported hosts must
fail startup rather than silently use the on-demand allocator; run the
concurrent fixture on each qualification host to verify refusal of a second
live instance. These
limits follow [Spin 4.0.2's pooling configuration](https://github.com/spinframework/spin/blob/v4.0.2/crates/core/src/lib.rs#L92).

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
resumption, explicit default-branch updates and metadata checkpoints that retain
every pack. Collection still
requires shared-library calls, as demonstrated in [`tests/minio.rs`](tests/minio.rs)
while Spin is stopped. Issue #32 remains open for collection commands, retention
cleanup and broader memory and sustained-service qualification.

## Local operator command

On Linux or macOS, build the native command separately from the WASIp2 service:

```sh
cargo build --locked -p object-log-git-spin --features operator --bin object-log-git-maintain --release
chmod 600 /deployment/repository.toml
target/release/object-log-git-maintain --config /deployment/repository.toml status
target/release/object-log-git-maintain --config /deployment/repository.toml resume-commit --token-file /private/push.token
target/release/object-log-git-maintain --config /deployment/repository.toml checkpoint --retain-packs
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

`set-default-branch` publishes an explicit symbolic HEAD update through the same
WAL CAS. Stop and drain serving processes first. Both names must be full branch
refs; the target may be unborn. Existing ref OIDs and packs are preserved. Legacy
repositories start with `refs/heads/main`. A stale expected default or any
competing head update rejects this candidate; the command never rebases it.
The persisted default survives checkpoints and cold serving restarts. This is
an explicit repository update, not a Spin bootstrap variable.

Each call requires a new `--recovery-file` path. The command reserves and syncs
an empty mode-0600 file before provider access, and never overwrites an existing
path. Reservation happens before configuration validation, so even an invalid
configuration can leave an empty file. Confirmed updates and conflicts leave it
empty. If publication returns pending, the command writes the exact core token
and fsyncs the file and directory before reporting `recovery_token: "saved"`.
Use `resume-commit --token-file` with that file to resolve the exact attempt.

A write/fsync failure or deadline reports pending without a saved-token claim.
The file may then be empty or partial. A crash or lost response before token
persistence can leave the exact attempt unknown: observing the desired default
later establishes visibility, not which attempt published it. Do not replay an
unknown update automatically. Synchronous receipt writes and fsync are not
preempted by the asynchronous backend deadline. Stronger recovery before
publication remains tracked in #32.

Every normal invocation emits one JSON line of at most 2 KiB. Output contains
static outcome names and numeric head metadata; it deliberately omits target
strings, paths, credentials, token bytes and provider diagnostics. Use the
outcome together with the exit status:

| Exit | Meaning |
| --- | --- |
| 0 | `observed`, `committed`, `not_committed`, `checkpointed`, `updated`, or requested help. `not_committed` means resolution completed, not a successful push. |
| 2 | Invalid arguments/configuration, unavailable/non-private/oversized input, unavailable recovery output, unsupported platform or incompatible durable options. |
| 3 | `conflict`, `stale_default`, or shared-engine `busy`; inspect the fresh state before another update. |
| 4 | `pending`/`expired`, backend unavailable, or lost output. Preserve the token; do not automatically replay expired work. |
| 5 | Invalid/corrupt evidence, a missing head, resource limit, unsupported backend, collection fence, expired view or runtime setup failure. `invalid_git_state_or_limit` covers Git validation and budget failures. No raw error chain is printed. |

Native S3 retries are disabled, connect timeout is five seconds, request timeout
is thirty seconds, and the asynchronous backend operation has a sixty-second
deadline. Deadline expiry during a mutation is pending: cancellation
cannot prove publication failed. A new resume command may retry the same token.
Input file reads precede the asynchronous deadline.

Input caps do not establish decoded-memory limits. Core resumption may verify
complete immutable dependency graphs and active collection plans; it does not
use the serving Git operation budgets. This command has no general 128 MiB
maintenance qualification yet. Build resources remain separate from runtime;
the service's existing 128 MiB qualification is unchanged.

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
These policy checks use an explicit 50 ms inter-request gap: with one Spin
instance, response delivery can precede slot release, causing a subsequent
request to receive a host-generated 500. Run `check_http.py --back-to-back`
to probe this unresolved admission race tracked in #21; the spaced checks do
not qualify back-to-back or concurrent request admission.

See [initial adapter evidence](EVIDENCE.md) for exact local gates and their limits.

The supported runtime configuration disables outbound HTTP connection pooling.
A pooled provider run produced a transient protocol error; the unpooled
configuration passed the recorded Linux workload. The cause is not proven.
`run.sh` rejects competing `--runtime-config-file` and `RUNTIME_CONFIG_FILE`
settings instead of silently discarding them.

Linux serving under a hard 128 MiB process limit requires an executable cache
prepared with the same Spin version, platform, and component. Cold compilation
was OOM-killed under that limit; cache setup peaked at 228–231 MB across two runs. Prepare
the cache outside the serving cgroup, then retain it for fresh serving processes:

```sh
python3 crates/object-log-git-spin/prewarm_cache.py --directory /deployment/wasmtime-cache
crates/object-log-git-spin/run.sh --listen 127.0.0.1:3000 --variable @/deployment/repository.toml --cache /deployment/wasmtime-cache/wasmtime-cache.toml
```

This is a compiler cache, not repository state. See the
[Linux qualification evidence](../../../docs/evidence/git-spin-linux-2026-09-04.md)
for exact limits, raw counters, provider conditions, and the cold-start failure.
