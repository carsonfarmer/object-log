# Local Git HTTP server evidence

## Result

At `9057ae8`, `object-log-git-http` hosts one object-log-backed repository at
`/repo` with Axum. Product code does not run Git or link to C Git. The tests use
an unchanged Git 2.54.0 client.

The local suite passed eight unit tests and three loopback tests. It covers:

- empty discovery, push, clone, and fetch
- branch and annotated-tag creation and deletion
- non-fast-forward rejection
- a ref change between advertisement and fetch
- a multi-round fetch with gzip requests and chunked responses
- two receive-pack requests that enter the service before either can publish
- exact recovered content and Git object validation

The large fetch used one discovery request and six upload-pack POST requests.
Four POST requests used gzip. The clone received the expected remote tip and
could read its tree object.

The concurrent-push test held both receive-pack routes at entry. Both unchanged
clients sent a receive-pack POST. Exactly one push succeeded. A new clone read
the winner's file and passed `git fsck --strict`.

## MinIO restart test

Run:

```sh
make git-http-minio-test
```

The test uses the pinned image
`minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
on an ephemeral loopback port. It pushed through one server, stopped that
server, and opened the same log through a new S3 backend, server, and scratch
directory. A clone from the second server had the exact source tip and file
content and passed `git fsck --strict`. The test passed in 0.65 seconds. The
script removed the MinIO container.

`object_store::LocalFileSystem` did not pass the required conditional-update
probe. The host rejects that backend. MinIO is the persistent local test
backend. The in-memory backend remains useful for fast process tests.

## HTTP and failure behavior

The server implements the two discovery routes and two smart HTTP POST routes.
It checks their exact media types and cache headers. It accepts identity and
gzip request bodies over fixed-length or chunked HTTP transfer. It creates an
upload pack in an anonymous temporary file before it sends response headers.

The host returns `400` for invalid or truncated Git data, `413` for a decoded
protocol limit, `415` for an invalid content type or encoding, `503` when work
capacity is full, and `500` for an internal failure. A rejected ref update uses
Git's receive report with HTTP `200`.

A receive operation returns success only after durable publication. An
uncertain or expired result returns `503`. A fresh ref advertisement tells the
client whether its requested Git state is visible. The server does not retain
or disclose the recovery token, so it cannot classify the old HTTP attempt
exactly. It sends success only after confirmed durable publication. Unpublished
staged objects remain eligible for object-log collection.

Each active Git operation holds one semaphore permit. Admission returns `503`
when all permits are in use. The upload response owns its permit and temporary
file until the body reaches EOF or is dropped. A unit test checks permit
retention and release. Work tasks remain tracked after handler cancellation.
Graceful shutdown waits for those work tasks.

The host applies 60-second request-body and response-body idle timeouts. It does
not impose a whole-operation timeout that could abandon publication. A front
proxy must limit request header bytes before Hyper parses them.

## Bounds

- The default active-operation limit is four.
- Encoded request bodies are limited to 513 MiB.
- Input and output packs are limited to 512 MiB each.
- Upload control data is limited to 8 MiB.
- Receive control data is limited to 1 MiB.
- Upload accepts at most 1,024 wants and 65,536 haves.
- Receive accepts at most 1,024 ref commands.
- Repository traversal uses the existing 10-million-object graph limit.

## Environment and gates

- Date: 2026-09-03, America/Vancouver.
- Host: Apple `Mac16,8`, 14 logical CPUs, 48 GiB memory.
- Operating system: macOS 27.0, build 26A5421a.
- Rust: 1.97.1, `aarch64-apple-darwin`.
- Git test client: 2.54.0, Apple Git-157.

The focused gates passed:

- format check
- package Clippy for all targets with warnings denied
- eight library unit tests
- three unchanged-Git loopback tests in 8.68 seconds
- one opt-in MinIO cold-restart test in 0.65 seconds

## Line changes

The change from `072e9c1` through `9057ae8` has these physical line changes:

| Category | Added | Removed |
|---|---:|---:|
| Product libraries | 668 | 17 |
| Operator executable | 78 | 0 |
| Tests | 286 | 83 |
| Manifests, lockfile, Makefile, and test runner | 262 | 7 |
| Documentation | 77 | 46 |

These counts use `git diff --numstat`. Product libraries include the HTTP
library and the Git reachable-want race fix. The operator category is the
runnable server entry point.

## Limits

- The server has one fixed repository and a main-only `HEAD` policy.
- Authentication, TLS, tenant routing, and repository selection are outside
  this host.
- Fetch sends the full reachable object set and ignores haves.
- Protocol v2, SHA-256 smart HTTP, thin packs, and pack rewriting are not
  implemented.
- The tests do not drop a network response after publication. The concurrent
  test covers the required competing-publication case.
- The host has per-request limits but no aggregate recovered-repository disk
  quota.
- MinIO proves local S3 API behavior. Live AWS qualification is separate.
