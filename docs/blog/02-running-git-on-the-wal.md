# Running Git on an object-storage WAL

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

Configuration maps repository paths to stable WAL identities, hash formats,
default branches, and permissions. Each repository has its own storage
namespace. An authorized writer's first push discovery creates its durable
state; reads only open existing repositories. Adding a repository changes
configuration, not the component binary.

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

The local setup needs Git, Go 1.27.1, the repository's pinned Rust toolchain,
Spin 4.1.0, `wac` 0.11.0, a MinIO binary, the AWS CLI, `jq`, Python 3, and
`curl`. The [contributor guide](../../CONTRIBUTING.md) covers setup, including
a MinIO build from pinned source. MinIO supplies storage; Spin serves HTTP.
No cloud account is needed.

From the repository root, build the component:

```sh
rustup target add wasm32-wasip2
make git-check
make git-build
```

The result is `examples/git/git.wasm`. The first build also compiles the
component build tool; later builds reuse it. This
is currently a source-build workflow. There is no prebuilt one-command install.

In one terminal, start an isolated local object store:

```sh
minio_data="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-minio.XXXXXX")"
echo "MinIO data: $minio_data"
minio_binary="${OBJECT_LOG_MINIO_BINARY:-$(go env GOPATH)/bin/minio}"
MINIO_ROOT_USER=objectlog MINIO_ROOT_PASSWORD=local-test-secret \
  "$minio_binary" server "$minio_data" --address 127.0.0.1:19090
```

In a second terminal, start from the repository root and create a bucket and
the service configuration. These credentials are disposable local examples.

```sh
local_config="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-config.XXXXXX")"
AWS_ACCESS_KEY_ID=objectlog AWS_SECRET_ACCESS_KEY=local-test-secret \
  AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url http://127.0.0.1:19090 \
  s3api create-bucket --bucket wal-proof
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
cd examples/git
spin up --listen 127.0.0.1:19100 \
  --variable "@$local_config/variables.toml"
```

With Spin running, validate the storage settings from another terminal before
sending Git traffic:

```sh
curl --fail-with-body -u git:local-git-password \
  -X POST http://127.0.0.1:19100/_validate_backend
```

The default configuration exposes
`http://127.0.0.1:19100/sha1.git` and
`http://127.0.0.1:19100/sha256.git`.

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

When Git prompts, use any username and `local-git-password`. The local password
mode uses HTTP Basic authentication; hosted deployments should use the Cognito
mode described in the example guide.

The SHA-1 version uses `--object-format=sha1` and `/sha1.git`. You can stop
Spin, restart it with the same configuration and MinIO data, and clone again.
Keeping the prefix is necessary for that restart check: a fresh prefix opens
a different namespace.

After trying it, stop Spin and MinIO with Ctrl-C. Remove `"$local_config"` in
the second terminal and `"$minio_data"` in the first if you want to discard
the test data. The variables refer only to the temporary directories created
above.

## What we tested

The local test suite runs installed Git clients against Spin and MinIO. It
covers both hash formats, clone and incremental fetch, push, shallow history,
tags, access controls, and maintenance. Additional cases exercise larger
objects, repeated pushes, restart recovery, and failures. Opt-in tests need
their own workload settings; an ordinary test run does not run all of them.

For the provider suite, start a fresh namespace rather than reuse the demo
repository. The [example guide](../../examples/git/README.md#test) gives the
commands and workload settings.

The [AWS deployment](../../examples/git/qualification/aws/HOSTING.md) was also
run with Spin behind Caddy on EC2, public HTTPS, Cognito authentication, and S3
access through an instance role. An OAuth credential helper supplied access
tokens to stock Git. Each repository had independent read, write, and
administrator groups; a separate machine identity could run maintenance but
could not clone or push.

That deployment survived concurrent writers, interrupted requests, a killed
service process, an EC2 reboot, and scheduled cleanup across eight repositories.
A storage drill removed the usable instance role and observed denied access.
After the role was restored, the service recovered exact data and accepted a
new push without restarting Spin. Fresh mirror clones and `git fsck --full`
verified all refs after the final maintenance cycle. Two concurrent workflows
with 513 MiB files completed for both object formats without an unexpected
restart. The service cgroup peaked at 5.30 GiB on that workload, which is a host
capacity measurement from that run. Deployments need limits based on their own
file sizes and concurrency.

Scheduled maintenance first removes unreachable entries from the Git catalog,
then asks the WAL to delete unreferenced storage in bounded batches. Running it
outside request traffic also covers idle repositories and uploads that never
published. Active reader retentions can delay collection; registrations lost by
a stopped process require an explicit drain-and-recover procedure.

There are costs I do not want to hide. The service uses unmodified go-git,
including its normal buffering of incoming delta bases and results. Large
updates can therefore need substantially more memory than their compressed
size. The service's object-size limit applies when decoded objects reach
storage, after delta reconstruction. Pushes advertise Git's `no-thin`
capability, so clients include delta bases in the upload. This can cost
bandwidth, but uses Git's existing negotiation without changing clients.

The Go component uses unmodified go-git, Spin, MinIO, and componentize-go. It
pins the unchanged contributor revision from an unmerged go-pkg pull request so
garbage collection does not run during a restricted component allocation step.
[THIRD_PARTY.md](../../THIRD_PARTY.md) records the exact revision and license.

Branch updates must be fast-forward, including when a client requests a force
push. Partial-clone filters and packfile URIs are outside the current service.
The service reuses suitable deltas received from clients but does not search for
new ones during fetch, so some outgoing packs are larger than packs produced by
Git's repacking machinery. Resource limits are configurable, and the deployment
host must account for total memory and concurrent requests. The durable format
is pre-release.
