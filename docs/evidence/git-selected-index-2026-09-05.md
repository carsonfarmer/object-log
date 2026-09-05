# Selected authenticated index prerequisite

`durable::SelectedIndex::load(operation, log, view, descriptor, staged_root)` loads one bounded standard index and authenticated chunk geometry using the existing validator. It retains exact context lifetimes and a state reservation. It does not load unrelated packs or blob chunks and does not verify decoded objects. The supplied staged root contributes its immutable reference for this read-only lookup; this API does not derive or renew publication proofs.

`num_objects()` gives the u32 count. `object_id_at(position)` checks bounds and returns an ObjectId. `entries()` yields fallible `(ObjectId, position)` pairs. `verify_position(id, position)` rejects mismatched IDs, formats or positions. Enumeration and comparisons charge the original cumulative Operation. All metadata allocation and load/hash/sort work are reserved/charged before the root GET.

Both-hash tests assert one root GET and exact downloaded bytes, zero blob GETs, correct enumeration, wrong/overflowing positions, wrong OID/hash format, exhausted work, allocation failure before GET, wrong descriptor and reservation cleanup. Independent node_preflight_review approved with no blockers.

`CARGO_INCREMENTAL=0 make check` passed formatting, workspace all-target/all-feature lint/tests, Git WASIp2 check and host WASIp2 lint. Raw evidence is adjacent. The working tree includes seven additional uncommitted delta tests; these are excluded from this prerequisite commit. No state, format, migration or caller changes. Remove the temporary dead-code allowance when the coordinated catalog caller lands.
