# Git on object-log

This example is a working smart-HTTP Git service backed by `object-log`.
go-git handles Git protocols, object formats, and packs. A small Rust WASIp2
component connects the Go service to the same generic WAL used by other
consumers. The object-log head is the only mutable durable authority; the
service keeps no local repository.

Supported behavior includes:

- unchanged Git clients;
- SHA-1 and SHA-256 repositories;
- protocol-v2 discovery, clone, and have-aware fetch;
- classic receive-pack push;
- shallow clone, deepen, and unshallow;
- named repositories, branches, annotated tags, Cognito permissions, and read-only mode;
- atomic ref and catalog publication;
- restart recovery, checkpoints, reader retention, and bounded collection.

The example is tested against local MinIO. Run the provider suite against your
remote storage and host before deployment. It is pre-release: use a fresh
object-store prefix when changing incompatible revisions.

## Build

Install Go 1.27.1, the repository's pinned Rust toolchain, Spin 4, `wac`, MinIO,
and the MinIO `mc` client. Then run from the repository root:

```sh
rustup target add wasm32-wasip2
make git-check
make git-build
```

`git-build` creates the Go component, builds the Rust WAL component, and composes
both into `examples/git/git.wasm`. Spin loads that final file.
The first build also compiles the pinned component build tool; later builds use
the cached binary.

## Run locally

Start an isolated MinIO instance in one terminal. The command prints its unique
data directory; remove that directory after stopping MinIO:

```sh
minio_data="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-minio.XXXXXX")"
echo "MinIO data: $minio_data"
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  minio server "$minio_data" --address 127.0.0.1:19090
```

In another terminal, create the bucket and a protected Spin variables file.
Each run gets a new WAL prefix:

```sh
local_config="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-config.XXXXXX")"
mc --config-dir "$local_config/mc" alias set local-git \
  http://127.0.0.1:19090 objectlog local-test-secret
mc --config-dir "$local_config/mc" mb --ignore-existing local-git/wal-proof
test_id="local-$(date -u +%Y%m%dT%H%M%SZ)-$$"
(
  umask 077
  cat >"$local_config/variables.toml" <<EOF_VARS
wal_prefix = "$test_id"
wal_access_key = "objectlog"
wal_secret_key = "local-test-secret"
git_boot_id = "$test_id"
git_auth_mode = "password"
git_password = "local-git-password"
EOF_VARS
)
```

Start the service:

```sh
cd examples/git
spin up --listen 127.0.0.1:19100 \
  --variable "@$local_config/variables.toml" 2>&1 | tee /tmp/object-log-git-spin.log
```

The repositories are available at:

```text
http://127.0.0.1:19100/sha1.git
http://127.0.0.1:19100/sha256.git
```

Initialize a repository with its first push. Enter `git` and the local password
when Git prompts. Reads never create repositories:

```sh
git init --object-format=sha256 -b main demo
cd demo
echo 'Git on object-log' > README.md
git add README.md
git commit -m 'Initial commit'
git push http://127.0.0.1:19100/sha256.git main
cd ..
git clone http://127.0.0.1:19100/sha256.git cloned-demo
```

When finished, stop Spin and MinIO, then remove `"$local_config"` and the
printed MinIO data directory.

The local `password` mode uses HTTP Basic authentication; the username is
ignored. An empty password is a configuration error. Anonymous access requires
explicit `git_auth_mode = "anonymous"`, no password, and disables administration;
use it only for disposable loopback demos. Hosted services should use Cognito.
Branch updates must be fast-forward. Force pushes and force-with-lease do not
rewrite branches.

## Configuration

All values are Spin variables. The defaults target the local MinIO setup above.

| Variable | Default | Purpose |
| --- | --- | --- |
| `wal_endpoint` | `http://127.0.0.1:19090` | S3-compatible endpoint and outbound-host boundary |
| `wal_bucket` | `wal-proof` | Object-store bucket |
| `wal_region` | `us-east-1` | S3 signing region |
| `wal_prefix` | empty | Isolated storage prefix; required outside disposable local use |
| `wal_access_key` | empty | S3 access key |
| `wal_secret_key` | empty | S3 secret key |
| `wal_session_token` | empty | Optional temporary-credential token |
| `wal_credential_mode` | `static` | Explicit keys or `instance-role` for renewing EC2 IMDSv2 credentials |
| `wal_collection_candidates` | `1000` | Maximum entries in a new deletion plan, capped by the durable graph limit |
| `wal_max_collection_objects` | `100000` | Publication graph bound (shared paths count separately); maximum unique live objects or entries per collection plan |
| `wal_recover_retentions_after_drain` | `false` | Exclusive lost-retention recovery mode |
| `git_repositories` | two demo entries | JSON map of paths, WAL identities, formats, default branches and permissions |
| `git_auth_mode` | `password` | `password`, `cognito`, or explicit local `anonymous` mode |
| `git_password` | empty | Required password for local password mode |
| `git_cognito_issuer` | empty | HTTPS Cognito user-pool issuer |
| `git_cognito_client_id` | empty | Public OAuth client for users |
| `git_cognito_scope` | empty | Required access-token scope, e.g. `git/access` |
| `git_cognito_host` | Cognito in `us-east-1` | Allowed outbound signing-key host; match the issuer's region |
| `git_cognito_operator_client_id` | empty | Separate machine client allowed administration only; requires the access scope plus `git/maintenance` |
| `git_boot_id` | `local-boot` | Instance identity exposed for recovery tests |
| `git_read_only` | `false` | Reject push and maintenance when true |
| `git_max_push_bytes` | `2147483648` | Incoming push body limit |
| `git_max_negotiation_bytes` | `8388608` | Protocol command and negotiation limit |
| `git_max_object_bytes` | `1073741824` | Maximum decoded Git object |
| `git_max_metadata_bytes` | `16777216` | Maximum commit, tree, or tag |
| `git_max_pack_objects` | `1000000` | Maximum entries declared by one incoming pack |
| `git_max_catalog_bytes` | `67108864` | Catalog data decoded during one request |
| `git_request_timeout` | `5m` | Cooperative request deadline |

Invalid, zero, or inconsistent limits fail closed. These defaults are
configurable service policy, not requirements of the WAL or Spin. They bound
accepted requests and stored objects, not peak memory: go-git buffers incoming
delta bases and results before the object-size check. Deployment capacity must
account for that memory use and concurrent requests.

## Repositories and permissions

Repository names come from configuration, including nested names. For example:

```toml
git_repositories = '{"team/project.git":{"log_id":"team-project","format":"sha1","default_branch":"main","read_groups":["developers"],"write_groups":["developers"],"admin_groups":["operators"]}}'
```

Each name has its own WAL identity. Duplicate identities, ambiguous names and
invalid formats are rejected. Add another entry and reload Spin to provision a
repository without rebuilding. Keep each identity stable. The first authorized
push discovery persists its format and default branch; subsequent configuration
cannot reinterpret that stored format or change its default branch.

Cognito mode checks signed access tokens, issuer, client, expiry and scope before
opening storage. Git supplies the access token as its Basic password through an
OAuth credential helper; HTTP clients may also send a Bearer token. Read, write
and administrator groups are independent. Empty group lists grant no access.
The optional machine client is for automated maintenance across all configured
repositories and cannot clone or push. Its token must include both
`git_cognito_scope` and `git/maintenance`. It uses Cognito's client-credentials
grant, not a user's password or refresh token.

User authentication is separate from storage authentication. On EC2, set
`wal_credential_mode = "instance-role"` and leave all static credential fields
empty. The established object_store provider obtains and renews role credentials
through IMDSv2. Restrict the instance role to the application's bucket prefix.

## Test

Run the code-level gate from the repository root:

```sh
make git-check
```

Verify the composed component and Spin configuration:

```sh
make git-spin-config-test
./scripts/test-git-auth-transport.sh
python3 examples/wal-component/tests/test-credentials.py
```

Against a service with a fresh WAL prefix, run the unchanged-client provider
suite. The suite creates its own refs; do not reuse a repository containing
manual demo commits. Stop Spin, choose a new `wal_prefix` in the variables file,
and restart it before the first suite run or a rerun:

```sh
GIT_PROBE_URL=http://127.0.0.1:19100 \
GIT_PROBE_PASSWORD=local-git-password \
GIT_PROBE_LOG=/tmp/object-log-git-spin.log make git-provider-test
```

Set `GIT_PROBE_PASSWORD` and `GIT_PROBE_BRANCH` when those values are configured.
`GIT_PROBE_LOG` supplies per-request storage counters when Spin omits HTTP trailers.
The provider suite uses installed Git as an independent protocol and integrity
oracle. Larger opt-in cases are controlled by these environment variables:

| Variable | Workload |
| --- | --- |
| `GIT_REPEATED_PUSHES=1` | 1,025 pushes per hash with concurrent fetch and integrity checks |
| `GIT_LARGE_OBJECT_MIB=513` | Large push, clone, edit, and fetch lifecycle |
| `GIT_CONCURRENT_LARGE=1` | Run the large lifecycle for both hashes concurrently |
| `GIT_COLLECTION_CAPACITY=1` | Exercise the configured physical-object boundary |
| `GIT_COLD_CATALOG=1` | Verify a cold sparse-catalog update |
| `GIT_PROBE_LIMITS=1` | Verify a separately configured small-limit service |
| `GIT_PROBE_PERSISTED_HEAD=true` | Verify default-branch recovery after restart |

The optional [AWS qualification setup](qualification/aws) provisions an isolated
bucket and least-privilege temporary identity for the same tests. It is separate
from normal development and must never target production data.

## Maintenance

Automatic checkpointing keeps the WAL tail bounded; it does not remove
unreachable Git objects. Run authenticated maintenance periodically and after
ref deletion:

```sh
curl --fail --user git \
  -X POST http://127.0.0.1:19100/sha256.git/maintenance
```

The JSON response state is:

- `complete`: the physical scan found no remaining deletion candidates;
- `more`: call the same repository's `/collect` endpoint for another physical batch;
- `pending` or `conflict`: retry with a fresh request; or
- `retained`: a registered reader blocks collection; if its process stopped, use
  the explicit drain procedure below.

`/maintenance` prunes unreachable Git objects and checkpoints the resulting
catalog, then processes one WAL collection plan. `/collect` only reclaims
physical objects; it does not repeat the Git graph walk. An installed plan is
resumed before opening the catalog. New plans use `wal_collection_candidates`;
an installed larger plan always needs enough budget to finish in full.
Candidate counts are plan entries, not guaranteed unique physical deletions.
Each new plan still authenticates the complete live WAL graph. Continuous
readers can delay cleanup; scheduling alone does not remove that constraint.

Every fetch acquires WAL retention before opening catalog data and releases it
after the last response byte. If a stopped instance loses a retention ID, stop
new traffic and drain all readers. Start one authenticated instance with
`wal_recover_retentions_after_drain = "true"`, call
`/<repository>/recover-retentions-after-drain` for each configured repository,
then stop it and restart with the
setting disabled. Recovery mode rejects ordinary Git and maintenance traffic.
Never clear retentions while a reader may still be active.

## Current limits

- Push advertises Git's `no-thin` capability. Ordinary clients include any delta
  bases needed by the pack, which can increase upload size. No client
  configuration is required.
- Incoming delta reconstruction uses go-git's normal in-memory buffers. Large
  objects and long delta chains can use substantially more memory than the
  compressed upload size. Plain object data and WAL reads remain streamed.
- Fetch reuses compact deltas supplied by Git clients; it does not calculate
  new deltas. Each optional representation is limited to 64 KiB compressed,
  shares the catalog leaf's 512 KiB inline allowance, and uses an optional
  per-push allowance equal to `git_max_catalog_bytes`. Full objects remain available
  when a representation does not fit or its base is absent from the fetch.
  Such fetches can use more bandwidth. Have-aware negotiation still omits
  objects the client already owns. Retained deltas also add catalog-read bytes.
- Partial-clone filters and packfile URIs are not implemented.
- TLS termination, process supervision, and host-wide connection and memory
  policies belong to the deployment host. Cognito settings must match the
  deployed user pool and clients.
- go-git, Spin, MinIO, and componentize-go are unmodified. The Go SDK pins the
  proposed fix in go-pkg PR #13; see [its provenance](../../THIRD_PARTY.md).
- Durable development formats are not migrated. Start with a fresh prefix after
  an incompatible revision change.

The generic WAL remains independent from Git and Spin. Git-specific catalog,
reachability, validation, and protocol behavior stay in this example.
