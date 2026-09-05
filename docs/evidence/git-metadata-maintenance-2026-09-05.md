# Bounded Git metadata maintenance

This shared-library tranche addresses the prerequisites for #32 and #34.
It is based on `d5447ca`, plus the independently reviewed core child-proof and
materialization-bound changes (original commits `67a2d79` and `b89a5d3`).
Operator/Spin wiring, sustained provider cycles, and independent integration
review remain separate requirements. Neither issue is complete from this record.

`Repository::checkpoint_retaining_packs(&log, format)` materializes authenticated
Git records using the existing Machine and publishes the same snapshot format
through the existing checkpoint CAS. It retains every pack proof, including
unreachable packs, and performs no catalog lookup, object decoding, pruning,
automatic resolution, or collection. Published, Conflict, and Pending retain
core checkpoint semantics. Pending must not be treated as confirmed success.

## Memory and operation bounds

Serving retains its 512-call, 88 MiB live-pool, 24 MiB retained-state, 96 MiB
transfer, and 256 MiB work limits. Metadata maintenance changes only the call
ceiling to 8,192. Two 1,024-entry materialization/publication attempts consume
4,096 tail reads, with bounded head/classification/plan overhead; 8,192 leaves
room for that overhead. All other ceilings and one cumulative expired-view
retry are unchanged. Larger durable option sets can still exceed this profile.

The core owns the encoded materialization bound: the larger of the checkpoint
length and the sum of the largest 33 commit lengths (32 buffered reads plus a
consumed record). Git reserves 128 times that bound for transient decoding,
proof construction, capacity growth, and canonical re-encoding. It additionally
reserves 64 times max_head_bytes for an overlapping missing-record classification
or publication head. The possible classification read is charged before I/O.
At default options that head reservation is 16 MiB. Retained View state separately
reserves eight times max_head_bytes (2 MiB at defaults).

An optional borrowed StateBudget in the existing Machine separately charges
retained maps before allocation. It charges all new entries before applying any
deletion, then releases deleted entries. Existing-key replacements do not
accumulate charges. The bound uses four times entry/name storage and pack
proof/descriptor storage, plus a twelve-entry minimum leaf allowance per
nonempty BTreeMap. Its bookkeeping is proportional to record updates and a
mutex keeps materialization futures Send without holding a lock over I/O.

Both ordinary pruning checkpoints and conservative checkpoints use one snapshot
publication helper. Descriptor/proof vectors are reserved before allocation;
snapshot buffers reserve four times their checked encoded upper bound. The
publication-time core tail reread, possible classification head read, collection
plan, checkpoint envelope, candidate head, and possible conflict refresh are
charged before publication. Reservations coexist in the same live pool and
fail before exceeding it. No unbounded resolution is hidden after that pool is
released: the core's separate resolve_checkpoint path is outside this helper.

## Functional and resource evidence

For each hash, a trusted test producer fills a default-options WAL with 1,024
valid small Git transactions sharing one pack. This is recovery evidence, not
1,024 HTTP pushes. Ordinary serving open rejects after its head GET when the
512-call preflight limit is reached. Maintenance then publishes a checkpoint;
a cold shared-engine fetch imports into a fresh filesystem Git receiver with
strict index-pack/connectivity/fsck checks, and a subsequent receive succeeds.

The checkpoint phase charges 2,054 calls and performs 2,051 physical requests:
one head plus 1,024 commits read twice, then checkpoint/head PUTs. The three
extra charged calls cover possible classification/conflict reads. Measured
payloads were 479,216 downloaded / 81,191 uploaded bytes for SHA-1 and 505,869 /
81,232 for SHA-256. Upload budgeting conservatively reserves the configured
checkpoint/head maxima rather than only these small fixture payloads.

Fault tests cover before/after head-CAS uncertainty, a concurrent ref winner,
a late invalid 1,024th Git record with zero PUTs, preservation of unreachable
packs without catalog reads, and a real collection-expired read followed by the
single cumulative retry. State-budget tests reject before map mutation at one
byte below the needed bound and release deleted entries without charging
replacements repeatedly. A separate 384-transaction fixture has aggregate
128-times encoded history larger than 88 MiB but fits the bounded read window;
its valid refs and cold fetch remain usable.

The warmed native test binary was measured directly, without Cargo or a compiler,
using `/usr/bin/time -l`. The both-hash 1,024-tail fixture (including InMemory
fixture construction and filesystem Git verification) peaked at 20,234,240 bytes
RSS, with 11,174,440-byte peak memory footprint and zero swaps. This is a local
macOS observation, not a hard 128 MiB cgroup gate, S3 operator peak, or Spin/WASI
runtime qualification. Those deployment measurements remain required.

Focused maintenance tests, the Git suite and three GC tests, strict native
all-target/all-feature Clippy, locked WASIp2 check, formatting, and the unchanged
filesystem conditional-write rejection test pass. Raw logs are in the adjacent
evidence directory. Local MinIO and actual Spin/operator acceptance are next.
