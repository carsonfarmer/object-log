# Relative deepen without a reachable boundary

Accepted-main baseline: `ac773db`. Hosted Linux run `33949010769` failed the final
shallow oracle case. Its checkout log reports Git 2.55.0 on Ubuntu 24.04;
the passing local environment used Apple Git 2.54.0. This was a real semantic
bug, not platform nondeterminism.

Git 2.55 corrected finite relative deepening with no reachable client shallow
boundary: it leaves the wanted history complete. Before that fix, Git could
interpret the requested increment as an absolute depth and truncate it. Our engine
copied the old behavior. The exact hosted mismatch was reproduced locally with
an unmodified official Git v2.55.0 build before changing the engine.
[Git 2.55 release notes](https://github.com/git/git/blob/v2.55.0/Documentation/RelNotes/2.55.0.adoc),
[shallow traversal](https://github.com/git/git/blob/v2.55.0/shallow.c).

The engine now preserves existing boundaries when finite relative deepening has
no reachable boundary. Infinite depth retains its separate unshallow behavior.
The expanded infinite-depth case also exposed transmission of a newly unshallowed
commit that the client already owns. Selection now omits that commit itself,
while preserving any needed parents and content, consistent with native
[unshallow handling](https://github.com/git/git/blob/v2.55.0/upload-pack.c).

No oracle assertion was relaxed and no Git version condition was introduced.
The finite regression compares the entire engine reply (new boundaries, removed
boundaries, and exact object IDs) with ordinary native fetch, the required no-op
semantics. It covers both hashes, absent/unrelated client boundaries, depths one
and three, and with/without a common have. The infinite case remains in the
same-input native oracle and retains its original boundary/pack assertions.

Independent correctness review confirmed finite behavior with six raw Git 2.55
requests, identified the infinite-depth exception, and independently probed the
newly unshallowed commit possession rule. Independent simplification review found
no changes needed and confirmed the stronger exact finite-response assertion.
Neither reviewer edited the implementation.

Focused both-hash shallow oracles pass with Git 2.55.0 and Apple Git 2.54.0.
Formatting, native workspace strict all-target/all-feature Clippy, full workspace
tests under Git 2.55, and strict Spin all-target/all-feature WASIp2 Clippy are
recorded in the sibling evidence directory. Provider integration is root's gate.

Git 2.55 was built locally from the official v2.55.0 source archive using
`make -j4 git NO_GETTEXT=YesPlease NO_TCLTK=YesPlease NO_CURL=YesPlease NO_OPENSSL=YesPlease`.
The oracle exercises local upload-pack, not HTTP. Builds use `CARGO_INCREMENTAL=0`.
Failed pre-fix and newly exposed infinite-case output are retained separately.
