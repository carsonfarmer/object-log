# Git command request admission

Core composition `f21f0f4` and serving activation `406bbc9` build on the core
guard `f8fc256` (evidence `fcee20c`) and atomic Git adapter `8c11d7d`, with the
current partial-clone implementation `6759f0c` and its prerequisites. Independent
auth/admission review found no blockers and reran focused core, Git, retry and
publication cases; those overlapping counts are not summed below.

## Contract and scope

Log guard attachment is now additive, superseding the original candidate's
replacement semantics. Existing caller guards run first, then the command's
Operation. A refusal prevents later guards and the backend invocation. Earlier
admissions remain charged if a later guard refuses or execution is cancelled;
therefore admission counts can exceed submitted requests for earlier guards.
This is caller policy, not durable authority, and proof identity is preserved.

Git binds its Operation once at serving or maintenance command entry. Every
reopen and prepared publication retains that same bound Log. Private durable
helpers consume the command context without replacing or appending guards.
Direct helper tests bind an explicit operation context. The same mutex commits
call and transfer admission together; denied admission consumes neither counter.

The guard charges core-to-object_store client invocations and accepted body
bounds. It does not measure hidden HTTP retries, network headers, listing pages,
or provider batching. Work and memory reservations remain independently bounded.
Manual I/O precharges are removed from serving open/materialization, pack staging,
plan/catalog/chunk reads, receive publication and metadata checkpointing. CPU
work and allocation reservations remain. Checkpoint work uses a checked bound
for encoding and hashing once; physical identity retries reuse those bytes and
remain subject to actual request admission.

An over-budget history now reads admitted records and stops immediately before
the first forbidden request. Previously it could reject a synthetic full-tail
precharge after only the head read. Neither behavior publishes partial state.
RequestDenied is a distinct core refusal, not a backend error. Exact recovery
preserves pending evidence when admission prevents classification or retry.

## Evidence

Both hashes pass active-plan staging and normal/before-CAS/after-CAS receive
publication with operation-call deltas equal to FaultStore request counts.
Filtered fetches have the same equality after excluding already-counted open
requests. An expired-view reopen leaves caller and operation totals equal,
proving it did not append another copy of the operation guard. A caller refusal
causes zero store calls. At the serving call ceiling, 513 earlier caller
admissions correspond to 512 accepted operation/backend requests, explicitly
proving that later refusal does not refund caller admission.

For each hash a 1,024-record metadata maintenance command charges and performs
2,051 requests: one head GET, two complete tail passes, and checkpoint/head PUTs.
SHA-1 transfers 479,216 downloaded and 81,191 uploaded fixture bytes; SHA-256
transfers 505,869 and 81,232. These local memory-store counters are not remote
latency measurements. Raw maintenance and filtered-fetch output is adjacent.

Full workspace all-feature tests pass: 411 tests, with 19 opt-in tests ignored.
This includes the filesystem checks and three Git GC regressions. Strict
workspace all-target/all-feature Clippy, locked Git WASIp2 checking, strict Spin
WASIp2 all-target/all-feature Clippy, and formatting pass. Core alone passes 187
tests and Git library 140. No provider run or runtime-memory measurement is
claimed by this tranche; root coordinates provider acceptance separately.

## Follow-on integration

This base intentionally excludes the streaming capacity prototypes and the
symbolic-HEAD setter. When combined, remove that setter's commit/head I/O
precharges while preserving its work/memory checks. Capacity's future helpers
and the private catalog foundation/cache must consume their bound context and
remove their own manual I/O charges. Preserve Operation::same_as when combining
the separately owned prototype. These inactive paths are not claimed covered
by current-serving parity.
