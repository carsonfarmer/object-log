# Git proof contract

The Git service proves that an established Git implementation can use the
generic WAL without another durable authority. Git protocol and repository
rules remain outside the Rust core.

## Required behavior

- Unchanged clients using SHA-1 or SHA-256 repositories.
- Protocol-v2 discovery, clone, have-aware fetch, and shallow history.
- Classic receive-pack push, branches, annotated tags, stale-write rejection,
  fast-forward policy, connectivity validation, and malformed-input rejection.
- Atomic publication of refs and the sparse object catalog.
- Authentication, read-only mode, a persisted default branch, and recovery
  without a local repository cache.
- Maintenance that prunes unreachable Git objects and invokes bounded WAL
  collection while preserving active readers.

Partial-clone filters and packfile URIs are outside the current proof. Add them
through the Git library when real workloads justify them; do not build a second
Git protocol engine here.

## Implementation boundary

`examples/git` is a Go service built on go-git. `examples/wal-component` is the
small WASIp2 bridge to the unchanged Rust WAL. Spin supplies HTTP hosting.

The WAL owns authenticated chunking, bounded sparse reads, recovery, retention,
and collection. The Go consumer owns Git negotiation, pack handling, refs, its
sparse catalog, and repository policy. Compressed small objects live in catalog
leaves; larger objects and incoming packs use WAL streams. The catalog and refs
publish in one WAL commit.

Incoming packs use WAL streams; go-git buffers delta bases and results during
import. Receive-pack advertises `no-thin`, allowing ordinary clients to send
self-contained packs. The catalog retains compact client-provided deltas within
its existing inline allowance. Fetch uses go-git's stored-delta path without
generating new deltas; full objects remain
available when a representation or its base is unavailable. Negotiation still
omits objects the client already has.

## Recovery and maintenance

Push validation finishes before publication. A push is never replayed after an
uncertain response; ordinary clients refresh refs. Expired reads may reopen once
before response bytes are sent, with cumulative limits across the retry.

Fetch holds a WAL retention through the last response write. At the configured
tail threshold, push admission checkpoints the authenticated catalog. The
maintenance endpoint prunes unreachable Git objects, starts or resumes one WAL
collection batch, and reports whether another pass is needed. Clearing lost
reader retentions requires an explicitly drained service mode.

## Qualification

`make git-check` runs Go race tests, static checks, provider-test compilation,
and native/WASIp2 bridge checks. `make git-build` composes the deployable
component. Provider suites use ordinary Git as the external oracle and cover
both hash formats, clone/fetch/push, large files, repeated history, access
control, restarts, maintenance, and collection.

The upstream-go-git service passes local MinIO tests; remote tests remain.
Public-host work includes deployment-specific TLS, routing, identity integration,
host-wide request admission, monitoring, and qualification through that exact
edge.

## Dependency policy

The service uses unmodified upstream go-git and componentize-go, including its
bundled adapter. It pins the unchanged go-pkg PR #13 revision pending upstream
review. Exact revisions, licenses, and upstream references
live in `THIRD_PARTY.md`. New fork-only behavior requires owner review and
focused tests. The service uses
ordinary Spin and unmodified object storage.
