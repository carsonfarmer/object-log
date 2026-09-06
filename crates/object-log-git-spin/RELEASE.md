# Operating a release

The generic WAL is the product; ordinary Spin hosts its Git adapter. Use the
single private `repository.toml` from [README.md](README.md) for both Spin and
the operator. Commands below run
from the checkout root, with the matching release component and operator built
as described in the README. Substitute your paths and namespace explicitly.

## Supported state and upgrades

This is a pre-1.0 service. Pin the source commit, Cargo.lock, built WASIp2
component, native operator and Spin version together. The qualified runtime is
Spin 4.0.2 with its ordinary defaults. Retain the previous artifact set privately
alongside its configuration; rebuilding later is not an exact binary rollback.

The current WAL format is version 1. Git records read versions 1 and 2;
legacy catalogs have an explicit `migrate-catalog` operation. A format number
alone does not certify compatibility: persisted WAL options must also match,
and Git metadata/catalog features must be understood by the reader. Keep the
configured `object_format` and `log_id` fixed. Changing `sha1` to `sha256` is not
a conversion. `status` checks the WAL head, not Git format or pack integrity.

There is no blanket compatibility promise across commits or a supported
in-place downgrade. Before a release, run `make check`, then rehearse the new
artifact set against an isolated restore of representative state: status,
authenticated ls-remote, full cold clone/fsck, push, and required maintenance.
An upgrade that changes durable encodings/options needs a documented migration
and reader/writer compatibility tests before adoption. Never edit CBOR, object
keys or the head to force compatibility. Do not mix old/new writers as an
upgrade strategy.

For cutover, close ingress, stop and drain **all** Spin hosts and operator jobs,
resolve known pending tokens, take the offline backup below, then start the new
artifact set. Verify authenticated reads before admitting writers. Stop the
old process groups, including Spin's HTTP children. Read-only mode alone does
not drain in-flight pushes or stop privileged writers.

## Offline backup and restore

Back up the complete configured root prefix, preserving every relative object
key and byte, including head, immutable objects, retentions and collection
state. Use a dedicated prefix per repository to keep the boundary simple.
A Git mirror is useful but does not preserve WAL history, symbolic unborn HEAD,
retentions or recovery evidence. An online recursive copy is not a snapshot.

Before copying, close ingress and drain every process with write/delete access,
including maintenance and external retention owners. Resolve known pending
tokens before history advancement. If collection is active, run
`collect --resume-only` until `status` reports no active plan; stop on unresolved
errors. Keep the namespace quiescent for the entire copy. A drain is an
operational exclusion, not a durable lock.

Capture the comparison baseline after resolving pending work and maintenance,
while writers remain excluded. Temporarily use a loopback Spin host with
`read_only = "true"` in the private TOML and a configured read credential helper.
Keep this baseline separate from the object backup directory:

```sh
umask 077
BASELINE=/private/backups/repository-2026-09-06-check
test ! -e "$BASELINE" && mkdir -p "$BASELINE"
git -c protocol.version=2 ls-remote --refs http://127.0.0.1:3000/repo > "$BASELINE/refs.unsorted"
LC_ALL=C sort "$BASELINE/refs.unsorted" > "$BASELINE/refs"
git -c protocol.version=2 clone --no-checkout http://127.0.0.1:3000/repo "$BASELINE/clone"
git -C "$BASELINE/clone" symbolic-ref HEAD > "$BASELINE/HEAD"
```

The disposable full clone records even an unborn symbolic HEAD, which
`ls-remote` may omit. Stop and drain this host too before copying; allow no
publication between baseline capture and backup. Run each step only after the
previous one succeeds.

Use the existing AWS CLI credential profile (never put keys in arguments),
pointed at the same backend as the private TOML. This is a copying recipe for a
qualified S3-compatible store, not AWS qualification. Select a fresh private
directory on protected storage; size it for the whole prefix. For example:

```sh
umask 077
export AWS_PROFILE=git-operator
ENDPOINT=http://127.0.0.1:9000
SOURCE=s3://git-repositories/object-log-git/
BACKUP=/private/backups/repository-2026-09-06
test ! -e "$BACKUP" && mkdir -p "$BACKUP"
target/release/object-log-git-maintain --config /deployment/repository.toml status
aws --endpoint-url "$ENDPOINT" s3 sync "$SOURCE" "$BACKUP" --only-show-errors
```

Run each step only if the preceding one succeeds. Check the selected prefix
against the private config before copying. Preserve the config and release
artifacts separately with the backup, including storage identity, log ID and
Git format. Keep token files private and tied to their original target. A copy
does not preserve provider ETags/version IDs; an old pending token is not
certified portable to a restored namespace. Unresolved operations need explicit
reconciliation, not blind replay. Protect backups like the repository and its
secrets. Backup frequency determines the maximum potential data loss; a
checkpoint alone is not a backup.

Restore into an **empty, isolated** prefix/bucket, with no serving process and
no lifecycle expiration rule. Do not restore over a live repository or merge
two snapshots. Preserve `log_id` and `object_format`; change only the storage
location and necessary credentials in a private copy of the same TOML:

```sh
RESTORED=s3://git-repositories/repository-restore-2026-09-06/
aws --endpoint-url "$ENDPOINT" s3 ls "$RESTORED" --recursive
# Proceed only after confirming this destination is empty and exclusively owned.
aws --endpoint-url "$ENDPOINT" s3 sync "$BACKUP" "$RESTORED" --only-show-errors
target/release/object-log-git-maintain --config /deployment/restore.toml status
spin up --from crates/object-log-git-spin/spin.toml --listen 127.0.0.1:3001 --variable @/deployment/restore.toml
```

Before starting Spin, require successful operator status: Spin can initialize a
missing head, which could hide a mistyped restore location. In another terminal,
with the credential helper configured for this test endpoint:

```sh
BASELINE=/private/backups/repository-2026-09-06-check
git -c protocol.version=2 ls-remote --symref http://127.0.0.1:3001/repo
git -c protocol.version=2 clone http://127.0.0.1:3001/repo /private/restore-check
git -C /private/restore-check fsck --strict --no-reflogs
git -c protocol.version=2 ls-remote --refs http://127.0.0.1:3001/repo > "$BASELINE/restored-refs.unsorted"
LC_ALL=C sort "$BASELINE/restored-refs.unsorted" > "$BASELINE/restored-refs"
cmp "$BASELINE/refs" "$BASELINE/restored-refs"
git -C /private/restore-check symbolic-ref HEAD > "$BASELINE/restored-HEAD"
cmp "$BASELINE/HEAD" "$BASELINE/restored-HEAD"
```

Compare all advertised refs and symbolic HEAD with the backup-time observation,
including an unborn default branch. Use an ordinary full clone, without shallow,
filter or URI options. Test a new push on the isolated drill copy. This validates
reachable Git state; it does not certify every historical or unreachable object.
For that push, set `read_only = "false"` in the isolated restore's private TOML,
restart its host, and use a write credential in the client helper.
Keep the old namespace offline when selecting the restored one for service.
Never expose two independently writable copies as the same repository.

The focused drill below uses random prefixes in an existing **loopback** MinIO,
copies offline to disk, restores to a new prefix, compares every copied byte,
removes the source, and checks cold refs, an annotated tag, unborn default HEAD,
fsck and a new push for both hashes. It deletes only its fixture prefixes:

```sh
cargo build --locked -p object-log-git-spin --target wasm32-wasip2 --release
cargo build --locked -p object-log-git-spin --features operator --bin object-log-git-maintain --release
# Set OBJECT_LOG_MINIO_ENDPOINT, _BUCKET, _ACCESS_KEY and _SECRET_KEY privately.
python3 crates/object-log-git-spin/tests/check_restore.py
```

The drill needs Python 3, AWS CLI, Git with SHA-256 support, Spin 4.0.2, Rust
with the WASIp2 target, and an existing local MinIO bucket. It does not start or
stop your provider. Repeat a restore rehearsal after storage, release or
backup-process changes; measure elapsed restore time against your recovery need.

## Readiness and operational signals

Use Spin's `/.well-known/spin/health` for process liveness only. For readiness,
run authenticated `git -c protocol.version=2 ls-remote --symref URL` with a read
credential and a bounded job timeout. It checks the storage-backed Git path;
an empty repository is valid. Discovery alone and operator status do not check
the same path. A periodic cold clone/fsck tests more than readiness does.
Readiness invokes provider validation, including disposable probe writes.

Use existing proxy response/latency counts and the operator's bounded JSON plus
exit status. Start with these actionable signals:

| Signal | Operator action |
| --- | --- |
| Repeated readiness failures or HTTP 5xx | Check target config, provider reachability and capacity. Keep ingress closed after a failed release; restart loops do not resolve uncertain writes. |
| HTTP 401/403 increase | Check token rotation and read-only policy; never log credentials or bodies to diagnose it. |
| Operator `pending`/`expired` (exit 4) | Preserve exact receipts, stop automatic replay and follow README recovery semantics. Expired means unknown historical outcome. |
| `conflict`/`retained` (exit 3), or persistent active collection plan | Find concurrent writers/retention owners; back off or resume the head's plan. Never clear a fence manually. |
| Growing tail count, storage usage, or repeated resource-limit failure | Schedule maintenance and assess the supported live-set bounds. Repeated collection drains garbage, not an oversized live graph. |

Operator counters are per invocation; absent counters mean unknown, not zero.
Candidate bytes are not confirmed reclaimed bytes. Choose alert frequency and
thresholds from your workload.

## Maintenance and rollback

Use offline windows and the [operator procedures](README.md#local-operator-command).
Resolve pending commits first. If needed, migrate the legacy catalog with a new
private recovery file, then compact with another new recovery file. After
confirmed compaction, checkpoint with `--retain-packs`, then repeat `collect`
until `no_candidates`. Resume installed plans from the head after interruption.
Do not proceed past pending/conflict/retained outcomes. Checkpoint retries can
converge state but cannot establish an earlier attempt's exact outcome.
Restart and verify cold reads before reopening ingress.

For rollback, stop and drain the new service first. Reuse the current namespace
with old artifacts only if that exact old reader/writer combination was tested
against all formats the new release could write. Otherwise restore the
pre-upgrade backup into an empty namespace and use its paired artifact set.
This loses post-backup publications; reconcile those explicitly with repository
owners before switching. Do not silently overwrite newer state. Keep failed
state isolated for diagnosis and recovery of acknowledged changes.

## Security and configuration defaults

Keep `auth_mode = "basic"`, distinct random read/write tokens, and
`allow_non_fast_forward = "false"` (default). Use `read_only = "true"` when
writers should remain disabled. Keep packfile URIs disabled unless required.
Bind ordinary Spin to loopback behind the existing trusted HTTPS proxy and
follow [AUTH.md](AUTH.md) for credential helpers, headers and rotation. Use
verified HTTPS for a non-loopback storage endpoint too. Do not allow clients
direct access to storage or the privileged operator.

Use private mode-0600 config/token files outside the checkout. Scope storage
credentials to the intended bucket/prefix and necessary read/write/list/delete
and conditional operations; read-only Git still needs probe writes. Disable
independent lifecycle deletion under the live prefix: only the WAL's collection
protocol knows retained roots. Bucket versioning is not a coherent backup and
may retain deleted versions beyond collection. Keep proxy/runtime logs free of
Authorization, bodies and credential URLs. Rotate HTTP and storage credentials
separately, restarting every serving host to finish revocation.
