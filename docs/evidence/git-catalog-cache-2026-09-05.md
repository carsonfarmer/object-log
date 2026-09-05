# Command-local catalog cache evidence

This candidate builds on the test-only catalog foundation c3c38a2. It does not
activate a new repository format or complete issue #19.

The cache borrows one Log, View, and Operation. It retains at most 256 decoded
nodes and 2 MiB of decoded vector storage. Its sorted entry vector is reserved
before allocation; authenticated reads reserve a conservative decoding bound
before I/O and shrink to retained vector capacities after decoding. Admission
pressure evicts existing nodes before retrying the reservation. It never resets
operation counters. Each traversal rechecks inherited bounds, including hits.

Native local results, exclusive worktree target and CARGO_INCREMENTAL=0:

- 12 focused catalog tests passed (focused.txt).
- 132 Git library tests passed (lib.txt).
- Strict all-target/all-feature Git Clippy passed (clippy.txt).
- Formatting and git diff checks passed.

For each hash format, two passes over a 128-object tree use exactly three GETs
and zero PUTs, with retained operation memory below 256 KiB. Dropping the cache
returns reservations to zero; a new cache authenticates paths again. A separate
regression leaves decoder admission one byte short, verifies eviction succeeds
before I/O, then proves the evicted path must be read again with cumulative
counters intact. The malformed ancestor-bound fixture rejects on both cold and
cached paths without additional GETs on the second rejection.

The first rerun encountered ENOSPC while creating a disposable incremental
compiler artifact. After reclaiming that artifact and other inactive build
outputs, the listed gates passed with incremental compilation disabled.

The full library gate preceded the final test-only addition that exercises the
cached bound rejection; the focused catalog suite and Clippy ran after it.
No native integration/provider or WASIp2 runtime evidence is claimed for the
cache: repository readers do not use it yet. Remaining work and the explicit
migration proposal are in docs/git-catalog-migration-plan.md.
