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
generating new deltas; full objects remain available when a representation or its
base is unavailable. Negotiation still
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

The upstream-go-git service passes local MinIO tests and the full provider suite
against public EC2 HTTPS with S3. Real Cognito login, helper refresh, repository
permissions, role replacement, process/host recovery and configuration-only
repository addition have passed. All five live core S3 tests also pass.
The remote long-history gate has passed. Service readiness still requires the
large-object gate, combined cleanup verification, final review and teardown
against the exact deployed revision.

## Dependency policy

The service uses unmodified upstream go-git and componentize-go, including its
bundled adapter. It pins the unchanged go-pkg PR #13 revision pending upstream
review. Exact revisions, licenses, and upstream references live in `THIRD_PARTY.md`.
New fork-only behavior requires owner review and focused tests. The service uses
ordinary Spin and unmodified object storage.

## Service readiness

Issue #45 tracks the remaining remote workload gates, final review and teardown.
Automatic-maintenance issue #46 is closed after deployed qualification and
independent review. Issue #6 tracks core performance; completed issue #10 retains
its original local-Spin/live-S3 scope. Root integrates reviewed tranches;
implementing workers use exclusive worktrees. KV qualification remains separate
in #39.

### 1. Repository identity and access

The declarative repository map supplies canonical nested paths, stable WAL
identities, immutable object formats, default branches and independent
read/write/admin groups. Duplicate identities, aliases and malformed paths are
rejected. Unknown repositories fail closed. Reads and administration open
existing WALs; an authorized first-push discovery can materialize a configured
repository before accepting a pack.

Remote tests added a nested SHA-256 repository through configuration while
preserving the component artifact. Read discovery returned 404 with no durable
head before authorized writer discovery and push; a cold clone and fsck passed.
All eight original repositories kept exact refs. A changed configured default
branch preserved persisted HEAD. Incompatible format configuration returned a
predictable error, and restoring the exact configuration preserved the refs.
Creation races and conflicting formats also retain local test coverage.

### 2. User identity and storage credentials

Cognito token validation uses an established Go library in the Git service,
outside the WAL. It checks issuer, signature, expiry, access-token use, client
identity and scope before storage access. Repository groups independently grant
read, write and administration. A separate client-credentials identity requires
both `git/access` and `git/maintenance` and permits administration only.

Actual hosted browser login with git-credential-oauth, ordinary Basic token
delivery to Git clone/fetch/push, expired-token rejection and browser-free helper
refresh passed. Stock Linux Git also directly invoked the helper to refresh
expired credentials before reader fetches and a writer push over verified HTTPS.
Requesting `openid git/access` supplies Cognito's group claim.
Live disjoint reader/writer/admin and machine-client checks passed. Wrong-issuer
rejection and controlled key rotation have native tests; these are not live
alternate-pool or Cognito key-rotation results. Composed stock-Spin TLS tests
cover the signing-key deadline, slow bodies and cleanup. Local JWT validation
cannot immediately detect revocation of interactive or machine tokens.

S3 authenticates the service through a prefix-restricted instance role and the
established object_store IMDSv2 provider through WASI HTTP. Live role replacement
denied Git storage access; restoring the original role recovered exact data and
accepted a new push without restarting Spin. Natural expiry and renewal failure
remain covered by the composed metadata fixture. No long-lived S3 keys are
needed on the host.

Credential tests remain finite. The helper test used five-minute access tokens;
the provider suite uses a fixed token with a lifetime covering its planned run.
Keep the public Terraform default at 60 minutes and remove run-specific lifetime
overrides after qualification.

### 3. Automatic maintenance

Push admission retains its tail checkpoint. The periodic host worker starts with
logical Git pruning/checkpointing, then uses physical `/collect` followups after
`more` or `retained`. Physical followups resume installed deletion plans before
loading the catalog. New plans have a separate candidate cap, audit the live
reference graph and protect referenced blob keys without reading opaque payloads.
Each new plan still scans the live graph; collection is not constant-cost work.

Finite per-repository deadlines and a systemd run deadline bound the worker.
An optional admission pause defaults off and rejects new ordinary requests while
admitted requests finish. Exact administrative POST paths still require service
authentication. Worker exit and systemd cleanup remove the pause marker.
Deployed automatic cleanup, concurrency and interruption recovery passed
independent review in #46.

Continuous retention can prevent collection from starting. Even with the pause,
progress depends on admitted requests finishing within it; a timer alone cannot
guarantee progress. Lost-reader recovery requires an explicit drain and never
runs automatically. Cleanup failure must not turn a committed push into a
reported rejected push. The worker adds no durable job authority or detached
component calls.

### 4. Complete the remote workload gates

Terraform deploys one disposable EC2 host with stock Spin, Caddy HTTPS, systemd,
Cognito, S3 and restricted workload identity. A separate client has passed the
full provider suite through public HTTPS with normal certificate validation.
HTTP-to-HTTPS redirection preserves authority, path and query. Exact complete refs
were checked across all eight repositories after process kill/automatic restart
and again after an EC2 reboot. The failure drills cold-cloned and ran fsck on
the two main hash repositories. Separate cold clones of idle and active
repositories passed after scheduled cleanup.

The remote workload passed 1,025 additional pushes per hash format with concurrent
fetch/fsck cycles and matching full cold histories and files. Concurrent remote
513 MiB object lifecycles are running; this workload has passed locally. Record
deployed memory, latency and S3 request/byte costs without treating local
measurements as remote capacity evidence. Use response `X-Request-ID` values to
match complete usage records delivered from the service journal through SSM.

Issue #45 remains open through the large-object workload, combined cleanup
verification, final independent review and verified teardown of the host,
network resources, identities and test storage. Use disposable namespaces and
no user data.

### 5. Review and release claims

Every tranche gets focused tests and independent correctness/simplification
review before root integration. Keep existing native, WASIp2, memory, filesystem,
MinIO, recovery and collection gates. Complete #6 measurements using existing
benchmarks. Re-run affected remote gates after behavioral changes during
qualification.

Update public setup instructions from a clean checkout and have a reviewer
follow them. Remove stale claims; report remaining limitations explicitly.
No fixed line quota, new Git engine, dependency fork, or second durable authority
is justified merely by this work. The completion claim is a qualified deployment
for its tested workload, not unlimited scale or high availability.
