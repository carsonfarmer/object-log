# Git proof contract

The product is a small, generic object-storage WAL. The Git service proves that
an established Git library can use it without a second durable authority.
`examples/git` contains the Go consumer; `examples/wal-component` exposes the
unchanged Rust log through WASIp2. Spin provides ordinary HTTP hosting.

## Required behavior

- Unchanged Git clients: SHA-1 and SHA-256, protocol-v2 discovery, clone and
  have-aware fetch, classic receive-pack push, branches and annotated tags.
- Atomic ref/catalog publication, stale-write rejection, fast-forward policy,
  malformed-input rejection and object connectivity validation.
- Optional authentication, read-only serving, persisted default branch and
  cold recovery from object storage without a local repository cache.
- Sparse object lookup and streamed chunk reads; cleanup preserves live history.
- Shallow clone, deepen and unshallow, checked with ordinary Git clients.

The custom Rust Git engine and its native maintenance command are retired.
Installed Git remains the independent test oracle. Local provider and workspace
qualification passes. A failure in the endurance test was traced to installed
Git 2.54 background maintenance; deterministic native tests reproduce it, and
the client test keeps maintenance synchronous. Remote provider and deployment
qualification remain before production rollout.
Advanced partial filters and packfile URIs are not required for acceptance;
add them only when useful and supported without bespoke protocol machinery.

## Storage and recovery

Compressed loose objects of at most 512 bytes live in authenticated catalog
leaves; larger objects and temporary incoming packs use the WAL byte-stream API.
The WAL owns chunk geometry, authenticated length and bounded offset reads. A splitting radix catalog
provides sparse lookup. The catalog and refs publish through the same conditional
head update. Local handles and caches are disposable; no local repository is
needed. Git formats, negotiation and pack processing belong to go-git.

Push validation completes before one publication. An uncertain result stays
explicit; ordinary Git clients refresh refs after a lost response. Pushes are
never automatically replayed. Expired reads may reopen once before response
bytes are sent, with at most 1 MiB of request replay and cumulative storage
counters. A late failure stops the response rather than restarting it.
Fetch visibility checks current ref trees before older history and stops when
requested objects and relevant haves are proven. Blob payload readers stay
unopened until pack generation; unreachable objects remain unavailable.
The storage bridge can retry one identical conditional write after a connection
failure. A rejected retry preserves the original uncertain outcome for recovery.

At 64 tail entries, existing maintenance checkpoints the reachable catalog
before admitting another push. The HTTP maintenance endpoint also prunes unreachable objects and runs
one bounded fenced deletion batch. Repeat until complete to drain old data.
Collection, checkpoint safety and uncertain outcomes remain core responsibilities.

## Limits and tradeoffs

Incoming delta bases and results stream through go-git's decoder into WAL byte
streams. A small importer checks framing, lengths, checksums and dependencies.
Pack metadata remains proportional to object count; backward copies may reread bases.
Outgoing packs stream full objects without creating deltas, so transfer sizes
can exceed a delta-compressed server's. There is no fixed process-memory promise.
The generic WAL's configured object, reference and tail limits still apply.
The Git example also bounds request bytes, pack entry counts, structured-object
sizes and cumulative catalog decoding, with cooperative cancellation before
storage operations. These checks cannot stop
an already-running synchronous WASI import or bound total metadata memory.
Host-wide request admission remains a deferred hosting concern.
Normal Spin settings are used; no instance-count, pooling or host-memory wrapper.
The service temporarily pins our go-git fork with streaming-decoder fixes and
requires its v6 prerelease APIs for both hashes, v2
serving, shallow history and streamed writes. The component build uses a pinned
Wasmtime adapter fix from our fork for Go GC host calls. Spin and Go's
collector are unchanged.

Use a fresh storage prefix: the prior custom Git catalog is incompatible, and
there is no development-format migration tool. Original array leaves and
unvalidated experimental Go roots are rejected. Do not silently reinterpret them.

## Checks

`make check` runs core/consumer Rust checks and `make git-check` (pure Go tests,
provider-test compilation, bridge tests, strict native and WASIp2 Clippy).
`make git-build` builds and composes the actual component separately.
`make git-provider-test` runs unchanged-client tests against a supplied local
Spin/MinIO service. Its README lists opt-in large/repeated-push and restart cases.

Keep fault tests, recovery, collection, memory/filesystem checks and the core
benchmarks. New verification belongs in executable tests and concise commits,
not evidence archives. Local MinIO measurements do not establish cloud behavior.
