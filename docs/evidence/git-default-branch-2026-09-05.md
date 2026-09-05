# Shared persisted default branch candidate

This candidate implements issue #30's shared-library boundary. It is not yet
provider-qualified and has no operator command wiring. Independent review,
actual Spin/MinIO clone checkout through the operator, and combined integration
gates remain required before acceptance or closing the issue.

`Repository::default_branch()` returns the byte-oriented symbolic HEAD target.
`set_default_branch(self, transaction_id, expected, target)` publishes an explicit
expected-old/new metadata change through the existing WAL head CAS. Targets must
be valid full branch refs; they may be unborn. The method returns the core's
committed/conflict/pending outcome without implicitly changing or retrying a
candidate. It retains its operation through publication. Pending evidence can
produce the existing portable commit token. No startup configuration becomes a
second durable authority and there is no new HTTP administration route.

Version 1's canonical encoding remains unchanged. Metadata updates introduce
version 2 with required catalog operation (key 5) and metadata operation (key 6).
The initial catalog operations are legacy checkpoint and unchanged transaction;
reserved tree migration operations reject until their proof handling exists.
Version 2 LegacyPacks mode does not require catalog migration. Every later ref
transaction and checkpoint preserves version 2 and the symbolic target. Legacy
records after a metadata upgrade reject; a supported stale old writer loses the
head CAS. This does not claim detection of an arbitrary caller replacing opaque
Git checkpoints through the generic core API.

Focused cases cover SHA-1 and SHA-256, main/master/trunk, unborn advertisement,
initial push after configuration, HEAD OID/symref advertisement, cold fetch with
installed Git strict pack/fsck checks, ordinary checkpoint and GC, conservative
checkpoint after deleting the default branch, stale expected targets with zero
PUTs, concurrent default updates, concurrent ref pushes, stale legacy prepared
pushes, and before/after head-write uncertainty with exact token recovery.
Additional codec/state tests cover required canonical fields, reserved catalog
operations, non-UTF-8 valid branch names, invalid targets, transaction downgrade
rejection, and memory precharge before retained metadata mutation.

Deleting the target branch leaves symbolic HEAD pointing to that now-unborn
branch. Installed Git was separately checked for this behavior and for cloning
an empty `trunk` repository, for both hashes. This local oracle observation does
not substitute for the pending HTTP clone checkout acceptance.

Passed: 139 Git library tests, three GC tests, shallow/selection integration
checks and request-depth helper checks; strict all-target/all-feature Git Clippy;
locked Git library WASIp2 check; formatting. Three existing opt-in tests remain
ignored in this ordinary run. Raw outputs accompany this record. No new MinIO,
Spin runtime, deployment memory, or scale qualification is claimed.
