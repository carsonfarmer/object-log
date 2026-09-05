# Git upload command acceptance

The common repository exposes static protocol-v2 discovery and a consuming
byte-oriented upload command. It handles ls-refs, fully peeled matching tags,
ACK-only negotiation, and done-fetch through exact want-minus-have selection.
One expired-view restart reopens with the same operation counters, including any
retry already spent during open. Encoded response bytes retain admission and
memory reservations through transport completion.

Five new both-hash tests cover command packets, full/empty fetch, ACK-only
responses, filtered nested tag peeling, malformed final tag target kinds,
request limits, response lifetime, collection expiry, and an exhausted retry.
Root's independent correctness review found the missing final peeled-kind check;
78ab3aa fixes it. Independent simplification review found no remaining upload
correctness defect. The native oracle remains.

Validation: macOS ARM64, Rust 1.97.1, Git 2.54. The full workspace gate passed
318 tests with 9 opt-in tests ignored, including formatting, native strict
Clippy, and locked WASIp2 check. Separate locked WASIp2 strict Clippy passed.
Raw workspace log: `/tmp/object-log-upload-full.log`.

This accepts the shared commands. Native unchanged-client and Spin/provider
qualification are separate later gates. The baseline object-log, SQLite,
native Git, and native Git HTTP local MinIO tests also passed at f77b197;
`/tmp/object-log-provider-baseline.log` records those runs.
