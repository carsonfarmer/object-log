# Explicit default-branch operator updates

The operator adds `set-default-branch --expected REF --target REF
--recovery-file FILE` through the shared setter from `6e9f436`. It opens an
existing log only and preserves the core CAS and Pending contract. The target
may be unborn. Full branch names remain byte-oriented, including non-UTF8
names; ref OIDs and pack roots are unchanged. No Spin bootstrap variable or
alternate durable authority was added.

An observed Committed result maps to updated (0); a competing head maps to
conflict (3); an already stale expected default maps to stale_default (3).
There is no automatic rebase, resubmission or separate resolution pass.

## Recovery receipt contract

Argument handling reserves a new mode-0600 output file and syncs it and its
parent before provider access. Existing files, symlinks, FIFOs and invalid
parents reject without contacting storage or overwriting a prior token. The
parent uses directory-only, nonblocking opening, including the FIFO-parent
regression found during independent review. An invalid configuration can leave
an empty reserved file, which is documented in the README.

Only a returned Pending result writes a token. The command uses the exact core
serialization, bounds it to the existing 1 MiB supported token cap, writes it,
and fsyncs the file and directory before emitting recovery_token: saved.
Serialization, write or sync failure emits pending / recovery_token: unavailable.
Confirmed updates and conflicts leave the reserved file empty. Neither the
path nor token bytes appear in the bounded JSON report.

A crash or cancellation before persistence can leave the exact attempt unknown;
observing the desired default later proves visibility, not that attempt's
identity. The command does not claim a prepublication receipt or add an ad hoc
prepared-publication API. Synchronous file operations are not preempted by the
asynchronous backend deadline. Stronger recovery and general cold-resume memory
qualification remain open in #32.

## Focused and shared validation

The package passes 31 ordinary tests: 10 library and 21 binary tests. Five new
operator tests cover private output reservation before any TCP access, stale and
invalid names with no PUTs, byte-oriented branch names, exact Pending receipt
resumption twice for both hashes and both sides of head CAS, write failure after
a successful CAS, cancellation after CAS, and a competing retention publication.
No failed persistence or cancellation claims a saved token or confirmed view.
A shared test mutex keeps these Git operations within real single-operation
admission instead of relaxing the engine limit.

The four shared default-branch tests also pass, including checkpoint/deletion/
unborn behavior, concurrent ref publication and exact pending recovery. Strict
native and WASIp2 all-target/all-feature Clippy, formatting, and release CLI and
component builds pass. Core memory/filesystem validation on the prerequisite
accounting correction passed 175 tests before provider work. Independent CLI
review approved the receipt contract after the FIFO-parent fix; shared setter
integration review remains root's responsibility.

## Native provider evidence

Docker remained unavailable after host disk exhaustion. Root supplied and
verified MinIO RELEASE.2025-09-07T16-13-09Z, darwin-arm64, SHA256
7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4.
Each run starts a fresh native process, temporary data directory and loopback
port with fixture-only credentials, then stops that process. This is additional
native-provider evidence; Docker-image requalification remains distinct and
pending. No shared Docker restart was attempted here.

Using Spin 4.0.2, Git 2.54.0 and Rust 1.97.1 on macOS arm64, the final release CLI
lifecycle passes for SHA-1 and SHA-256 in 12.62 seconds. It covers missing-target
protection for the new setter, the earlier exact-resume and 1,024-tail checkpoint
cases, three more actual maintenance cycles, and real unchanged-client clones
with main, trunk and master as the default. A stale expected default leaves the
head unchanged. Selecting unborn master creates no branch; a fresh clone retains
that unborn symbolic HEAD. A subsequent push creates master, checkpointing
preserves it, and a cold clone checks out its exact tip and contents with strict
fsck. The raw log records bounded JSON outcomes for every operator invocation.

The corrected process-group shutdown helper verifies that the launcher, all
trigger processes and the listener are gone before maintenance or cold restart.
All four existing Spin provider cases also pass against native MinIO, including
GC recovery, both receive policies and multi-round gzip fetch (22.30 seconds).
That run preceded only the CLI FIFO-parent opening correction; the final operator
run used the corrected release binary. The server transport and shutdown helper
were unchanged between those runs. Raw timed Spin reports are retained.

Forty fresh release CLI processes were measured with /usr/bin/time -l. RSS ranged
from 10,125,312 to 14,303,232 bytes (maximum 13.640625 MiB). These small fixtures
include the 1,024-tail checkpoint and metadata setter; they do not establish a
general whole-process bound. Builds, native MinIO and the Spin host are
separate processes. Current fixtures use ordinary Spin defaults. No remote-store
performance or indefinite sustained-service claim is made. The prior disk-pressure failures remain preserved in
[auth-config evidence](spin-operator-auth-config-2026-09-05.md).

The adjacent native-provider.sh reproduces the fixture with the verified binary
and prebuilt release component/CLI from the repository root. Pass operator-only
to run just the expanded operator lifecycle; otherwise it also runs the four
existing Spin provider cases. The ordinary Docker script remains unchanged.
