# Git on object-log

This example is a complete smart-HTTP Git service backed by `object-log`.
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
- branches, annotated tags, optional authentication, and read-only mode;
- atomic ref and catalog publication;
- restart recovery, checkpoints, reader retention, and bounded collection.

The example has passed local MinIO and live AWS S3 qualification. It remains a
pre-release example: use a fresh object-store prefix when changing revisions.

## Build

Install Go 1.26.3, the repository's pinned Rust toolchain, Spin 4, `wac`, MinIO,
and the MinIO `mc` client. Then run from the repository root:

```sh
rustup target add wasm32-unknown-unknown wasm32-wasip2
make git-check
make git-build
```

`git-build` creates the Go component, builds the Rust WAL component, and composes
both into `examples/git/git.wasm`. Spin loads that final file.

## Run locally

Start an isolated MinIO instance:

```sh
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  minio server /tmp/object-log-git-minio --address 127.0.0.1:19090
mc alias set local-git http://127.0.0.1:19090 objectlog local-test-secret
mc mb --ignore-existing local-git/wal-proof
```

Create a protected Spin variables file:

```sh
cat >/tmp/object-log-git-vars.toml <<'EOF_VARS'
wal_prefix = "fresh-local-example"
wal_access_key = "objectlog"
wal_secret_key = "local-test-secret"
git_boot_id = "local-boot-1"
EOF_VARS
chmod 600 /tmp/object-log-git-vars.toml
```

Start the service:

```sh
cd examples/git
spin up --listen 127.0.0.1:19100 \
  --variable @/tmp/object-log-git-vars.toml
```

The repositories are available at:

```text
http://127.0.0.1:19100/sha1.git
http://127.0.0.1:19100/sha256.git
```

For example:

```sh
git clone http://127.0.0.1:19100/sha256.git
```

Authentication is disabled when `git_password` is empty and should only be used
that way on loopback. When configured, clients use HTTP Basic authentication;
the password is checked in constant time and the username is ignored. Branch
updates must be fast-forward. Force pushes and force-with-lease do not rewrite
branches.

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
| `wal_default_branch` | empty | Persisted default branch when supplied |
| `wal_max_collection_objects` | `100000` | Maximum physical objects in one repository graph or collection plan |
| `wal_recover_retentions_after_drain` | `false` | Exclusive lost-retention recovery mode |
| `git_password` | empty | Optional HTTP Basic password |
| `git_boot_id` | `local-boot` | Instance identity exposed for recovery tests |
| `git_read_only` | `false` | Reject push and maintenance when true |
| `git_max_push_bytes` | `2147483648` | Incoming push body limit |
| `git_max_negotiation_bytes` | `8388608` | Protocol command and negotiation limit |
| `git_max_object_bytes` | `1073741824` | Maximum decoded Git object |
| `git_max_metadata_bytes` | `16777216` | Maximum commit, tree, or tag |
| `git_max_pack_objects` | `1000000` | Maximum entries declared by one incoming pack |
| `git_max_catalog_bytes` | `67108864` | Catalog data decoded during one request |
| `git_request_timeout` | `5m` | Cooperative request deadline |

Invalid, zero, or inconsistent limits fail closed. These are protocol and
object-graph bounds, not a whole-process memory limit. The deployment host must
also bound concurrent requests and instance memory.

## Test

Run the code-level gate from the repository root:

```sh
make git-check
```

Verify the composed component and Spin configuration:

```sh
make git-spin-config-test
```

Against a running service, run the unchanged-client provider suite:

```sh
GIT_PROBE_URL=http://127.0.0.1:19100 make git-provider-test
```

Set `GIT_PROBE_PASSWORD` and `GIT_PROBE_BRANCH` when those values are configured.
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
curl --fail --user git:"$GIT_PASSWORD" \
  -X POST http://127.0.0.1:19100/sha1.git/maintenance
curl --fail --user git:"$GIT_PASSWORD" \
  -X POST http://127.0.0.1:19100/sha256.git/maintenance
```

The JSON response state is:

- `complete`: no further work remains;
- `more`: run maintenance again;
- `pending` or `conflict`: retry with a fresh request; or
- `retained`: an active reader currently blocks collection.

Each request prunes unreachable Git objects and processes one bounded WAL
collection batch. Candidate counts are plan entries, not guaranteed unique
physical deletions.

Every fetch acquires WAL retention before opening catalog data and releases it
after the last response byte. If a stopped instance loses a retention ID, stop
new traffic and drain all readers. Start one authenticated instance with
`wal_recover_retentions_after_drain = "true"`, call both
`/sha1.git/recover-retentions-after-drain` and
`/sha256.git/recover-retentions-after-drain`, then stop it and restart with the
setting disabled. Recovery mode rejects ordinary Git and maintenance traffic.
Never clear retentions while a reader may still be active.

## Current limits

- Fetch packs contain complete required objects rather than newly generated
  deltas. Have-aware negotiation still omits objects the client already owns,
  but similar large revisions can consume more bandwidth.
- Partial-clone filters and packfile URIs are not implemented.
- Host-wide TLS, routing, authentication integration, concurrency, and memory
  admission belong to the deployment host.
- Development currently pins reviewed go-git and Wasmtime fork revisions while
  their fixes are under upstream review. Spin, MinIO, and componentize-go are
  otherwise unpatched.
- Durable development formats are not migrated. Start with a fresh prefix after
  an incompatible revision change.

The generic WAL remains independent from Git and Spin. Git-specific catalog,
reachability, validation, and protocol behavior stay in this example.
