# Reader object size for partial-clone filtering

`Reader::object_size(id)` returns `Option<usize>` privately. Full blobs use the authenticated canonical entry header through the existing bounded 42-byte prefix reader. This is declared metadata for filtering; it does not verify decoded size or OID. Selected content must still pass `verify`. Delta and structural entries use the existing bounded `find` path, returning decoded result length. No caps or publication rules change.

Both SHA-1 and SHA-256 tests cover REF/OFS deltas, missing IDs, blob sizes 0/15/16/127/128/2 MiB, and a memory-pressure check that permits prefix reading but prevents inflation. A re-authenticated false declared size is returned by the metadata lookup while both content-verification paths reject it.

Validation: `make check` passed formatting, workspace all-target/all-feature Clippy, workspace tests (343 passed, 12 ignored), Git WASIp2 check and host WASIp2 Clippy. This working tree also contains five uncommitted receive-input prototype tests included in that total; they are excluded from this commit. Raw output: `git-object-size-check-2026-09-05.txt`. Independent read-only review by node_preflight_review approved with no blockers.

The temporary dead-code allowance can be removed when the coordinated partial-clone caller lands. No network tests or capacity claims.
