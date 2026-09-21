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
- Arbitrary configured repository paths with isolated WAL identities and an
  explicit SHA-1 or SHA-256 format. Adding a repository requires configuration,
  not source changes.
- Per-repository reader, writer, and administrator permissions; read-only mode;
  a persisted default branch; and recovery without a local repository cache.
- Automatic maintenance that prunes unreachable Git objects and invokes bounded
  WAL collection while preserving active readers. Manual maintenance remains an
  operator recovery tool, not the normal cleanup workflow.

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

The upstream-go-git service passes local MinIO tests, including three isolated
configured repository names and both hash formats. Cognito validation is wired;
real sign-in and helper refresh await remote qualification. Earlier live S3
testing ran Spin locally and does not qualify a remotely hosted service.
Service readiness requires completing the remaining gates below against the
exact deployed revision.

## Dependency policy

The service uses unmodified upstream go-git and componentize-go, including its
bundled adapter. It pins the unchanged go-pkg PR #13 revision pending upstream
review. Exact revisions, licenses, and upstream references
live in `THIRD_PARTY.md`. New fork-only behavior requires owner review and
focused tests. The service uses
ordinary Spin and unmodified object storage.


## Service readiness work

Use new issue #45 for the service queue and complete remote qualification,
#46 for maintenance design, and #6 for core performance. Completed issue #10
retains its original local-Spin/live-S3 scope. Root integrates reviewed tranches;
implementing workers use exclusive worktrees. KV provider qualification runs separately in
#39 and must not delay or broaden the Git work.

### 1. Repository identity and access

Replace the two hardcoded paths with a small declarative repository map: canonical
path, stable WAL identity, immutable object format, and read/write/admin groups.
Support nested names such as team/project.git without a repository database or
another mutable storage head. Validate duplicate identities, aliases, malformed
paths and incompatible format changes. Unknown repositories fail closed.

Separate opening an existing WAL from authorized creation. The core already has
open_existing; expose the needed distinction at the component boundary. Missing
repositories must not be initialized by reads. Config provision authorizes the
name and format; an authorized first-push discovery can materialize the WAL
before accepting a pack. Preserve format across creation races and later config
changes. Existing reader-retention writes and backend capability probes remain
part of the storage contract.

Acceptance: ordinary clients create/use several independently named repositories
of both formats; add one through configuration without rebuilding; prove refs,
objects, recovery and collection stay isolated. Read-only callers cannot create
repositories or push; unknown paths and configured-but-missing read discovery
leave that repository head absent. Format mismatch and conflicting creation
fail predictably.

### 2. User identity and storage credentials

Use Cognito and an established Git OAuth credential helper. Keep token validation
in the Git service with an established Go library, outside the generic WAL. Check
issuer, signature, expiry, access-token use, client identity and scope. Enforce
repository group policy and separate maintenance/recovery administration from
ordinary writes. Test the actual browser login, credential delivery and refresh
flow; signed test tokens alone do not establish Cognito interoperability.

S3 authenticates the service separately through IAM. The deployed host receives
a role restricted to its data prefix. The bridge now selects explicit static
credentials or the established object_store IMDSv2 credential provider through
the WASI HTTP connector.
Controlled metadata tests pass in the composed component, including renewal
and failure; still prove role credential renewal on EC2.
Do not assume native metadata access proves WASI compatibility or introduce
long-lived access keys.

Acceptance: real Git clone/fetch/push using the helper; expired and wrong-issuer
tokens rejected; read/write/admin permissions enforced per repository; signing
key rotation, token refresh, and S3 credential renewal do not lose committed data.
Native and WASIp2 checks remain required. Verify key-fetch deadlines through
the actual WASI transport; a native context-timeout test is not enough.

### 3. Automatic maintenance

Preserve the existing cheap write-triggered tail checkpoint. Logical Git
maintenance walks reachable objects and the catalog. Physical followups skip
that walk and resume installed deletion plans first. Do not put a full graph
scan on every push or start detached component calls after an HTTP handler returns.

The first selected changes in #46 are separate logical/physical passes and a
per-call deletion-plan cap. A periodic host worker with finite work budgets is
being implemented. An optional disposable ingress marker can pause new requests
while admitted requests finish; evaluate it without changing the WAL authority.
No scheduling approach alone guarantees progress with continuous readers.
Compare request-triggered work, publication events, periodic invocation and
checkpoint/epoch-driven approaches. Evaluate scheduling separately from the
reachability and deletion algorithms: repeating a full scan on every small
collection pass is not an adequate default.

Measure repeated Git traversal, full live-blob verification, scoped listing and
retry costs as live data and garbage grow. Examine resume-first collection,
separate logical pruning and physical reclamation, exact-root mark reuse, and
generic bounded-progress improvements. Keep successful-push pack staging,
abandoned uploads and idle repositories in scope. Continuous reader retention
can prevent collection from starting; do not promise progress merely because a
scheduler retries. Never clear active readers automatically.

Choose the smallest design with demonstrated correctness and useful cost/progress
guarantees, including fair service across repositories and interruption recovery.
A timer remains one candidate, not a settled architecture. Do not add a durable
job authority or speculative format rewrite; identify owner-review requirements
if a change to the collection contract is justified.

Acceptance: ordinary pushes/ref deletions and abandoned uploads are eventually
cleaned without manual curl commands; idle repositories are serviced; concurrent
readers and writers remain correct; interrupted cleanup resumes. Cleanup failure
must not turn a committed push into a reported rejected push. Lost-reader
recovery remains an explicit drain operation, never an automatic unsafe timeout.

### 4. Deploy and qualify the complete service

Extend existing Terraform rather than create a deployment framework. Start with
one disposable remote host running stock Spin, an established HTTPS proxy,
process supervision, Cognito, S3 and restricted workload identity. Define the
hostname/certificate and credential renewal paths before provisioning. Retain
only necessary health checks, logs, admission settings and automatic maintenance.

Run the existing provider scenarios from a different machine through the actual
HTTPS address. Remove loopback-only orchestration assumptions without weakening
checks of target identity or disposable namespaces. Exercise multiple named
repositories and both formats, login/refresh/permissions, shallow and have-aware
fetch, large transfers, concurrent push/fetch/collection, disconnected clients,
service/host restarts, S3 errors and temporary-credential renewal. After each
failure scenario, cold clone and verify acknowledged refs/objects independently.

Measure host memory, latency and S3 request/byte costs for finite representative
workloads. Set acceptance limits before running; a duration-only soak is not a
substitute for scenarios. No user data is used. Verify teardown of the host,
network resources, test identity, storage and other resources created by the run.
Issue #45 closes only after the exact deployed revision passes this gate.

### 5. Review and release claims

Every tranche gets focused tests and independent correctness/simplification
review before root integration. Keep existing native, WASIp2, memory, filesystem,
MinIO, recovery and collection gates. Complete #6 measurements using existing
benchmarks. Run the final integrated service through the remote gate after any
behavioral change made during qualification.

Update public setup instructions from a clean checkout and have a reviewer
follow them. Remove stale claims; report remaining limitations explicitly.
No fixed line quota, new Git engine, dependency fork, or second durable authority
is justified merely by this work. The completion claim is a qualified deployment
for its tested workload, not unlimited scale or high availability.
