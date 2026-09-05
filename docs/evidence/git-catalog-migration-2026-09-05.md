# Explicit catalog migration candidate

This candidate builds on persisted metadata 6e9f436, catalog foundation c3c38a2,
cache 5b49f7b, and selected index enumeration 8a5ec68. It is not a serving cutover.
The migration entry point, tree-writing constructors, and catalog implementation
remain test-only until reader, receive, and maintenance consumers are complete.

The v2 codec recognizes TreeSnapshot/Migrate/Replace, with at most one node proof
and no legacy pack descriptors. Materialization validates mode transitions and
expected refs/metadata before mutation. Migration preserves refs and symbolic HEAD,
removes the legacy pack map, and releases its retained memory allowance. Empty
tree state cannot retain refs. Legacy snapshots and metadata-only v2 state remain
supported.

The candidate operator uses maintenance admission, reads one authenticated index
at a time, reserves exact ordered-entry vector capacity before allocation, and
builds a COW tree. It appends one transaction against the observed head. Conflict
returns without rebasing; pending returns the existing core recovery token.
Already-tree returns None without publication. One-shot budget exhaustion is an
error before publication, not a partially migrated state.

Local native evidence (exclusive target, CARGO_INCREMENTAL=0):

- Five focused migration tests pass: both hashes and both legacy metadata versions,
  cold tree checkpoints plus GC and selected-index lookup; empty/idempotent and
  conflict behavior; before/after lost-head-reply token recovery in a cold Log;
  invalid transition/root/count rejection; selected-index failure with zero PUTs.
- 157 Git library tests pass. This preceded the final exact Vec preallocation
  refinement; the focused suite ran again after that change.
- Strict Git all-target/all-feature Clippy passes.
- Locked WASIp2 Git check passes for the compiled codec/state; it does not exercise
  the test-only migration entry point.
- Formatting and diff checks pass.

The actual expired-view retry branch, cancellation during multi-pack migration,
provider storage, and complete service behavior still require integration tests.
The independent foundation already exercises COW conflicts, cancellation, and GC,
but that is not a substitute for the full operator path. No #19 completion or
provider/runtime compatibility claim is made by this candidate.
