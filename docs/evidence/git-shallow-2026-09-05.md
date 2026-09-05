# Protocol-v2 shallow tranche — 2026-09-05

Issue #24 now has an end-to-end shallow upload-pack implementation on branch
`cf/git-shallow-v2`. This tranche does not complete issue #24: partial clone,
filter/promisor/lazy retrieval, and packfile URI identity/access/lifetime/failure
behavior remain required subsequent work. Those capabilities are not advertised.

## Behavior

The byte-oriented public Repository API is unchanged. Discovery advertises
`fetch=shallow`. The private request parser accepts client shallow boundaries,
absolute depth, relative deepen, infinite-depth unshallow, committer-time cutoff,
and unique ref exclusions (including the advertised HEAD and remote HEAD aliases).
It rejects malformed IDs/numbers, duplicates of singleton options, conflicting
options, and collection/response limits before producing output.

Authenticated graph metadata now includes the committer timestamp. Commit-only
breadth-first traversal computes depth across all parents and wanted tips. Relative
depth adds the minimum reachable client-boundary depth, matching the installed
Git 2.54 implementation. Time/exclusion boundaries follow selected commit ancestry.
Wanted closure respects the new boundaries; have closure always respects the
client's original boundaries. Every newly unshallowed commit seeds its missing
ancestry even if a new merge boundary hides it from the wanted tip.

Negotiation remains ACK-only until `done`. Done responses omit acknowledgments,
write any shallow-info section and delimiter before packfile, then use the existing
bounded pack writer. Sparse authenticated reads, selected blob verification,
one exact view, and one cumulative expired-view retry remain intact.

Protocol references:

- [Git protocol v2](https://git-scm.com/docs/protocol-v2)
- [Git shallow implementation](https://raw.githubusercontent.com/git/git/v2.54.0/shallow.c)
- [Git upload-pack implementation](https://raw.githubusercontent.com/git/git/v2.54.0/upload-pack.c)

## Verification

Environment: macOS, Git 2.54.0 (Apple Git-157), Spin 4.0.2, Rust 1.97.1.
The opt-in client fixture starts an isolated local MinIO container using the same
pinned image as the existing project fixture, uses only loopback endpoints,
and destroys the container afterward. Builds and component compilation are
outside the serving budget.

- Formatting, strict all-feature native workspace Clippy, all-feature workspace
  tests, strict Spin WASIp2 Clippy, default-feature core WASIp2 check, and release
  Spin WASIp2 build pass.
- Installed-Git protocol oracle passes both hashes: depth, merges, multiple tips,
  annotated tags, relative deepen, unshallow, shallow ordinary fetch, since,
  exclusions/HEAD, unrelated boundaries, mixed-age tips, and merge-cut transitions.
  Boundaries match Git. Every omitted oracle object is proven already present in
  the client's old-boundary-aware have closure; Git may resend shared objects.
- Explicit negotiation assertion proves no shallow-info or pack is emitted before
  done. Existing pack framing/maximum-byte tests exercise the production encoder.
- Actual unchanged Git through Spin/MinIO passes both hashes: empty depth-1 clone,
  relative deepen, absolute depth, cold-host unshallow, since/exclude/HEAD clones,
  ordinary incremental shallow fetch, and merge deepen/unshallow. Strict fsck
  follows each relevant transition. A 50 ms inter-command gap isolates the known
  single-instance admission race, as the existing HTTP fixture does.
- Native Git MinIO push/checkpoint/collection/cold-recovery regression passes.
- New both-hash shallow retry test forces actual checkpoint/collection expiry,
  checks cumulative calls/work and retained response memory, and rejects a spent
  retry allowance. This is library/MemoryStore GC coverage; the new shallow Spin
  fixture's restart is a cold-host test, not a shallow Spin GC claim.

Raw evidence is in [git-shallow-2026-09-05](git-shallow-2026-09-05/).
The focused retry fixture precharges 1 MiB of work to prove it survives reopening:

| Hash | Cumulative engine calls | Cumulative work bytes | Retained response bytes | Fetch store GETs | Fetch downloaded bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| SHA-1 | 7 | 1,580,136 | 339 | 6 | 2,680 |
| SHA-256 | 7 | 1,581,082 | 416 | 6 | 2,845 |

These are tiny functional fixtures, not throughput or remote performance results.
Engine calls include the original open and expired attempt; store counters are
reset immediately before fetch. Retained bytes are the response reservation after
operation state is dropped, not peak process RSS.

## Independent review

A read-only correctness agent found and verified fixes for unrelated relative
boundaries, all-boundary unshallowing, mixed-age wanted tips, a merge cutoff that
hid a newly unshallowed parent's ancestry, and HEAD exclusion resolution. Final
review found no remaining concrete correctness/accounting defect.

A separate read-only simplification agent identified a discarded wanted-closure
traversal and a duplicate test-only pack encoder. Both were removed without
reducing supported behavior or weakening existing tests.

## Bounds and integration

No capacity constants were raised. Graph traversal still loads all ref-reachable
metadata, retains at most 32,768 nodes, and shares the existing 24 MiB state and
88 MiB live budgets. Catalog roots still scale with live packs; depth-1 clone does
not establish bounded startup independent of repository history. Added timestamp
and selection arrays are reserved; fixed graph edge capacity is still computed
inside the same graph budget. Network/call/work/retry limits remain unchanged.
The overall response byte limit is unchanged, so shallow metadata reduces the
available maximum pack response payload. A request exceeding it fails explicitly.
This run establishes no new hard 128 MiB host-process qualification or remote
object-store performance claim.

Only the coordinating root integrates main. Repository changes are limited to
fetch selection/serialization, exclusion lookup, parser reservation, and the
shallow-test include. Preserve the other worker's open/preflight/maintenance
changes when integrating. Update GIT_PLAN's historical exclusion list to mark
shallow implemented; retain partial/filter/packfile-URI work in #24.
