# Running Git on an object-storage WAL

*Draft for review. Describes the current service; the dependency design is
under review before publication.*

The Git service in `object-log` accepts an ordinary `git push`, stores the
repository in object storage, and serves it back through an ordinary
`git clone`. The server has no local repository to recover after a restart.

I built it to exercise the WAL's public API. Git supplies a familiar use case
with enough demands to expose awkward storage abstractions: large objects,
concurrent updates, graph traversal, interrupted writes, and readers that
must survive garbage collection.

[The first post](01-building-a-wal-on-object-storage.md) describes publication
through one conditional head. This post follows a push through the service
and shows how to run it locally.

## What runs where

The service uses Go and go-git for Git protocols, object formats, and packs.
A Rust component exposes the WAL through a small component interface. Spin
hosts the composed WebAssembly component and handles HTTP. The Rust WAL uses
`object_store` for S3 access through a WASI HTTP bridge.

```text
ordinary Git client
        |
     HTTP / Spin
        |
 Go service + go-git
        |
 Rust WAL component
        |
 object_store / WASI HTTP
        |
    MinIO or S3
```

Spin is the host for this example. The core WAL is a Rust library independent
of both Spin and Git, and can also run natively.

## From a push to durable refs

A push brings proposed ref changes and usually a pack of Git objects. The
service receives that pack into WAL-backed byte storage, imports its objects,
and validates the proposed repository update. Writing these objects is
preparation; it does not yet make the new branch tip visible.

The service maintains a sparse catalog from Git object IDs to their stored
representations. Small compressed objects can fit directly in catalog leaves.
Large objects use WAL byte streams. Updating a small part of the catalog
reuses unchanged subtrees.

Once validation succeeds, the service publishes the new catalog root and
refs together in one WAL commit. A competing writer can make that publication
conflict. The service cannot acknowledge the proposed update unless it knows
the update was accepted.

On a fetch, the service holds a WAL retention while it reads the catalog and
streams the response. Maintenance can remove unreachable Git objects from a
new catalog, but collection must preserve data protected by a live reader.

The service also keeps some compact delta representations supplied by Git
clients. go-git can reuse one when its base is included in the outgoing pack.
Otherwise it sends a full object. This saves transfers without running a new
search for similar objects on every fetch. It does not guarantee packs as
small as Git's repacking machinery would produce.

## Run it on a laptop

The local setup needs Git, Go 1.26.3, the repository's pinned Rust toolchain,
Spin 4, `wac`, MinIO, and MinIO's `mc` client. It uses two local processes:
MinIO for storage and Spin for HTTP. No cloud account is needed.

From the repository root, build the component:

```sh
rustup target add wasm32-unknown-unknown wasm32-wasip2
make git-check
make git-build
```

The result is `examples/git/git.wasm`. The first build also compiles the
component build tool and adapter; later builds reuse those artifacts. This
is currently a source-build workflow, not a prebuilt one-command install.

In one terminal, start an isolated local object store:

```sh
minio_data="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-minio.XXXXXX")"
echo "MinIO data: $minio_data"
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  minio server "$minio_data" --address 127.0.0.1:19090
```

In a second terminal, create a bucket and the service configuration. These
credentials are disposable local examples. The temporary `mc` configuration
keeps them separate from any existing MinIO setup.

```sh
local_config="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-config.XXXXXX")"
mc --config-dir "$local_config/mc" alias set local-git \
  http://127.0.0.1:19090 objectlog local-test-secret
mc --config-dir "$local_config/mc" mb local-git/wal-proof
test_id="local-$(date -u +%Y%m%dT%H%M%SZ)-$$"
(
  umask 077
  cat >"$local_config/variables.toml" <<EOF
wal_prefix = "$test_id"
wal_access_key = "objectlog"
wal_secret_key = "local-test-secret"
git_boot_id = "$test_id"
EOF
)
spin up --from examples/git/spin.toml --listen 127.0.0.1:19100 \
  --variable "@$local_config/variables.toml"
```

Run that block from the repository root. The two repository URLs are
`http://127.0.0.1:19100/sha1.git` and
`http://127.0.0.1:19100/sha256.git`. Authentication is disabled for this
loopback-only example.

In a third terminal, push a commit and clone it back. Use an empty working
directory for these commands and keep your ordinary Git author configuration:

```sh
git init --object-format=sha256 --initial-branch=main demo
cd demo
echo 'Stored through object-log.' >README.md
git add README.md
git commit -m 'Try object-log'
git remote add origin http://127.0.0.1:19100/sha256.git
git push -u origin main
cd ..
git -c protocol.version=2 clone \
  http://127.0.0.1:19100/sha256.git demo-clone
git -C demo-clone fsck --full
```

The SHA-1 version uses `--object-format=sha1` and `/sha1.git`. You can stop
Spin, restart it with the same configuration and MinIO data, and clone again.
Keeping the prefix is necessary for that restart check: a fresh prefix opens
a different namespace.

After trying it, stop Spin and MinIO with Ctrl-C. Remove `"$local_config"` in
the second terminal and `"$minio_data"` in the first if you want to discard
the test data. The variables refer only to the temporary directories created
above.

## What this proves so far

The local test suite runs installed Git clients against Spin and MinIO. It
covers both hash formats, clone and incremental fetch, push, shallow history,
tags, access controls, and maintenance. Additional cases exercise larger
objects, repeated pushes, restart recovery, and failures. Opt-in tests need
their own workload settings; an ordinary test run does not run all of them.

For the provider suite, start a fresh namespace rather than reuse the demo
repository. The [example guide](../../examples/git/README.md#test) gives the
commands and workload settings. The same service has also passed the
repository's disposable AWS S3 qualification. That is evidence for the
tested workloads, not a claim of arbitrary scale or a substitute for testing
the public deployment's TLS, routing, identity, and capacity controls.

There are costs I do not want to hide. The Go component build has several
moving parts. The current dependency pins include binding/runtime fixes and
a go-git extension for streamed delta import, alongside correctness fixes.
Those are described in [THIRD_PARTY.md](../../THIRD_PARTY.md). Minimizing that
maintenance burden is still unfinished work.

Branch updates must be fast-forward, including when a client requests a force
push. The service supports the Git operations listed above, but not partial-clone
filters or packfile URIs. It does not generate new outgoing deltas. Resource
limits are configurable; the deployment host must also control total memory
and concurrency. The durable format is pre-release.
