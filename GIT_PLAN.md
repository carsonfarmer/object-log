# Git proof contract

The standalone object-storage WAL is the product. Git proves that its small,
byte-oriented API supports a demanding application. Keep Git rules in
`object-log-git`, HTTP hosting in the Spin adapter, and one conditional log head
as the only mutable durable authority.

## Service behavior

The service supports SHA-1 and SHA-256 with unchanged Git clients:

- Protocol-v2 discovery, `ls-refs`, clone and have-aware fetch.
- Classic receive-pack push, thin-pack normalization, atomic ref updates,
  stale-write rejection and fast-forward policy.
- Shallow clone, deepening, unshallow, time/ref exclusions, partial clones with
  `blob:none` and `blob:limit`, and lazy object retrieval.
- Authenticated packfile URIs with range resume and inline fallback.
- Persisted default branches, authentication, read-only operation, catalog
  migration, pack compaction, checkpoints, collection and cold recovery.

Wants must be reachable from published refs in the observed view. Haves are
validated against that same view. Ordinary fetch selects exactly
`reachable(wants) - reachable(valid haves)`. Explicit tree/blob wants from partial
clients remain selected unless noncommit haves prove ownership; commit haves
alone do not prove the client has those objects. Shallow boundaries and filters alter
that selection according to the advertised protocol. Negotiation acknowledges
common haves; a pack is sent after `done`. Applicable annotated-tag chains are
included when requested.

Stored packs are self-contained. Fetch reuses verified compressed entries and
safe deltas when possible; otherwise it verifies and reconstructs the object
through bounded windows. No scratch Git repository is required. Installed Git remains the
independent correctness and performance oracle.

## WAL and recovery

One log owns one repository. Immutable standard packs, indexes and catalog nodes
are stored in authenticated object-log objects. Pack and index data use variable
chunk geometry, and lookup reads only the selected indexes and pack ranges.

`Repository::open(&Log, ObjectFormat)` observes one exact view. Catalogs and
traversal state are command-local caches. A view is not a retention lease:
collection can expire it. Commands may reopen once, preserving all operation
counters, and validate before writing response bytes. Streamed responses never
retry after output starts; a late failure aborts without the final digest/flush.

Push verifies pack checksums, object IDs, deltas, connectivity, kinds and ref
policy before the ordered ref transaction is published through head CAS.
A pending result retains an exact-candidate recovery token. Standard Git has
normal lost-reply ambiguity; the operator can resolve stored recovery tokens.

Compaction publishes one replacement catalog and preserves refs and symbolic
HEAD. Checkpointing establishes the retained roots before collection. Collection
verifies the live graph, installs a positive deletion batch through head CAS,
and resumes that exact batch after interruption. Repeat collection until empty
to drain a large stale backlog. Stable retentions block fresh collection.

## Resource bounds

The engine shares an 88 MiB live allocation pool per native process or WASI
instance and a 24 MiB retained-state allowance. Spin controls host concurrency
with its default settings. These are library limits, not a host-memory cap.

Streaming receive supports 1 GiB decoded blobs and 1,040 MiB incoming packs.
Commit, tree and tag bodies remain bounded at 8 MiB. Each stored pack is bounded
at 2,080 MiB and 32,768 objects; aggregate fetch can combine several stored packs.
Buffered entry points retain their smaller limits. See the constants in
`crates/object-log-git/src/lib.rs`, `pack.rs` and `budget.rs` for exact bounds.

Ordinary traversal stores object membership and a frontier. Shallow, filtered
and URI selection retain adjacency within the same memory allowance. Neither
traversal inherits the individual pack's object-count limit. All paths charge
work, calls, bytes and allocation growth, including overlap while resizing.

Collection bounds both its live graph and each positive plan. A scan examines
at most the live count plus the plan limit plus one entry. Excess unknown
namespace entries can exhaust this bound without finding deletable objects.
This is a finite supported envelope, not an unlimited-scale claim.

## Verification

Run `make check` for formatting, strict native Clippy, workspace tests and
WASIp2 checks. Run memory and filesystem conformance, including rejection of
unsupported compare-and-swap, before local MinIO. Network-backed tests stay opt-in and use isolated local storage.

The provider suite covers both hashes with:

- Actual concurrent clients across independent ordinary Spin hosts, one winning
  conflicting push, immediate reads and cold recovery of the winner.
- 1 GiB push lifecycles and an aggregate clone over 2 GiB.
- Connected histories over 32,768 objects, including shallow/deepen/unshallow,
  filtered lazy retrieval and URI selection checked against installed Git.
- 1,100 file-changing pushes and 35 maintenance/cold-clone cycles.
- Real object-log history, interrupted upload, compaction, checkpoints and GC.

Use the existing `git-spin-*` Make targets and `make gc-acceptance`; commands and
prerequisites are in the Spin README and `docs/testing.md`. Ordinary Spin needs
no custom instance/pooling wrapper or patches. Live AWS qualification is separate.

Performance comparisons use matched fixtures and the installed Git oracle,
with warmup and ten pairs per case. A timing ratio above 1.25 triggers thirty
pairs and investigation. Exact object sets, pack size, calls and transfer limits
are hard checks. Guest/InMemory command timing is not HTTP or S3 latency.
Record results in tests, commits and the issue tracker.

## Maintenance

Use exclusive implementation worktrees and independent correctness reviews;
root alone integrates main. Keep source small through removal of duplication,
not by cutting behavior or weakening validation. Add a generic WAL capability
only when a consumer demonstrates a missing contract.

Git completion is tracked in #17. Scale #19 and ordinary Spin reliability #21
are complete; #25 records the final reduction pass. The next KV implementation is scoped
in #39; SQLite and verifiable KV remain separate follow-ons.
