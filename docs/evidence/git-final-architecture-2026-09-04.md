# Git architecture review

Date: 2026-09-04

Source-count snapshot: `0ed9b52`. Follow-up cleanup: `a51c806`.

## Assessment

The standalone object log remains the product. The complete Git implementation
is a demanding example of its API: it implements protocol-v2 discovery,
clone and have-aware fetch, classic receive-pack push, both object formats,
and recovery and collection through the same durable contract. Git object and
ref rules remain outside the generic log. The object-log head remains the
only mutable durable authority for repository state.

The implementation exceeds the plan's source-size review signals. That is a
reason to examine the architecture, not to remove required behavior. Most of
the additional code implements Git validation, bounded execution, recovery,
or native/WASI transport compatibility. This review found no justification
for moving Git policy into the log or introducing a second authority.

The native oracle is retained. This review does not authorize its removal,
replace provider qualification, or declare every acceptance gate complete.

## Source counts

Counts include comments and blank lines. The historical raw convention counts
Rust lines before each **top-level test module**, then counts that module
separately. The included `repository/receive_tests.rs` file is entirely test
code; it is not production code merely because it lives under `src`.

At `0ed9b52`:

| Crate | Raw production preambles | Test modules and included test file |
| --- | ---: | ---: |
| `object-log-git` | 5,655 | 7,049 |
| `object-log-git-http` | 1,284 | 322 |
| `object-log-git-spin` | 588 | 113 |
| Total | 7,527 | 7,484 |

There are another 37 lines of explicitly test-only helpers inside those
preambles: 15 in `git.rs`, 10 in `repository.rs`, and 12 in `budget.rs`.
Classifying those helpers by their actual compile conditions gives **7,490
production lines** and **7,521 test lines under `src`**.

Separate Rust test/support surfaces contain 2,609 integration-test lines,
194 Criterion benchmark lines, and 137 Spin probe/import-inspection example
lines. The two Python Spin qualification scripts contain another 250 lines.
These figures describe the snapshot, not later additions or a deployed binary.

The earlier combined `b322985` total of 4,970 needs correction. Applying the
same top-level-test-module convention gives **5,101**. In `git.rs`, an inline
`#[cfg(test)]` helper appears before further production functions; stopping
at that first attribute incorrectly omitted the remaining preamble. Future
counts should identify test items rather than truncate at the first test
attribute.

### Comparison with review signals

| Surface | Observed change or size | Plan's review signal |
| --- | ---: | ---: |
| Pack, durable reads, wire, and budgets | 2,449 raw lines, up 260 from 2,189 | Historical foundation thresholds require review when exceeded |
| Repository, graph, fetch, receive, and related public errors/state | Approximately 1,490 added lines | More than 1,275 added lines |
| Native HTTP and Spin additions | 849 added lines | More than 300 added lines |

Using the historical 2,165-line oracle-deletion allowance as an arithmetic
projection leaves 5,362 of the 7,527 raw preamble lines. That exceeds the
4,150-line review signal by 1,212. This is not an exact deletion estimate or
permission to delete the oracle; shared helpers and compatibility boundaries
would need a separate review before any removal.

Using the same raw convention, test modules plus integration/support Rust grew
by 4,900 lines from `2ee2174` to this snapshot. Tests exceed their initial
estimate too. Recovery, adversarial input, resource ownership, sparse reads,
client interoperability, and provider failures account for that coverage;
test growth should not be confused with production API growth.

## Where the complexity belongs

**Git domain behavior.** Pack normalization and thin-base resolution, object
reachability, branch ancestry, ref namespace conflicts, tag handling,
negotiation, compressed-entry reuse, and wire framing belong in the Git crate.
They use the generic log rather than redefine its publication protocol. The
common receive path stages immutable data, validates it through the same
sparse authenticated catalog used by readers, and prepares the head update
only after validation. This avoids a second in-memory object resolver.

**Lessons for the generic API.** The use case exposed three useful generic
requirements: authenticated variable chunk geometry must respect small object
limits; reference-node counts must be bounded before expensive allocation or
storage work; and adapters need the authenticated active collection-plan byte
length to admit reads hidden inside publication calls. The small
`View::collection_plan_bytes()` accessor provides that metadata without
exposing Git concepts or adding another authority. These are lessons about
bounded object-log use, not reasons to add a Git-specific storage API.

**Transport behavior.** Native HTTP handles request decoding, limits,
publication survival after disconnect, recovery responses, and response-owner
lifetimes. Spin additionally needs a WASI HTTP connector compatible with
`object_store`, explicit polling and cancellation, supported timeout mapping,
and a RustCrypto provider. S3 requests and signing remain in established
crates. This portability work explains much of the adapter overage; it should
not migrate into the generic log merely to make adapter files shorter.

## Simplification performed and deferred

Commit `a51c806` removes obsolete blanket dead-code allowances that referred
to future integration tranches. Helpers used only by tests or the retained
oracle now have explicit compile conditions. Two narrow exceptions document
parsed client permissions that self-contained `REF_DELTA` output does not
need. Native all-target strict Clippy, no-default-feature WASIp2 strict
Clippy, and formatting passed for that cleanup. No required behavior or
public API was removed.

Two later cleanups could improve navigation without changing the contract:
move the preserved native repository implementation into its own private
module, and consolidate repeated private branch/leaf checks where their
semantics match. Neither is required simply to meet a line count. A new
shared HTTP framework or broader public API would need to make both hosts
clearer, rather than just relocate lines.

## Independent quota review

The Spin quota change `a581564`, included in `0ed9b52`, creates one transport
budget per incoming handler. Connector, service, and log clones share its
`Arc`. Backend capability probes, log opening, engine retries, and streamed
storage responses therefore use the same counters. No reset or separate
bootstrap budget was found.

Checked atomic updates reject overflow and excess. Calls are charged before
entering WASI HTTP; upload payload is charged before writes; downloaded chunks
are charged before being yielded to `object_store`. Tests cover exact bounds,
overflow, concurrent clones, and rejection before native WASI imports.

The precise claim is **at most 512 outgoing HTTP attempts and 96 MiB of
payload accepted or sent by this connector**. It does not measure HTTP
headers or bound bytes already buffered by the remote server or host network
stack before a response chunk is rejected. Those limits complement the
common engine's logical-call, work, and live-memory budgets.

## Measurement and acceptance boundaries

The paired shared-engine performance harness compares the same requested
Git results, with explicit runtime differences: shared measurements include
log opening, validation, and memory storage; the Git baseline includes
subprocess startup and filesystem work. Warm-up exclusion, alternating pair
order, escalation, exact OID comparison, standalone delta-base verification,
and strict receiver checks were independently reviewed. Its interval-based
serial depth is an observed nonoverlapping request chain, not a causal graph
or a remote-latency measurement.

Memory-store timing does not establish remote object-store performance. Spin
qualification must include its complete per-request bootstrap costs. A
128 MiB WebAssembly instance limit is not a 128 MiB whole-process RSS or Linux
cgroup result. Test envelopes and in-memory provider storage are also distinct
from the common engine's reservation pool.

Retain the oracle and the existing filesystem, MinIO, Criterion, recovery,
and collection evidence. Report local, provider, guest-memory, and
whole-process measurements separately. Source-size review cannot substitute
for any required behavioral, resource, or provider gate.

## Final acceptance census

Independent recount at `27c54d0` reproduced the earlier method and historical
counts before calculating the final result. Raw production preambles contain
7,526 lines: Git 5,638, native HTTP 1,296, and Spin 592. Reclassifying 54 explicit
test-helper lines yields **7,472 product Rust lines**, 18 fewer than at
`0ed9b52`. Adjusted per-crate product counts are Git 5,584, HTTP 1,296, Spin 592.
Source tests contain 7,538 lines; separate Rust integration/support has 2,730,
Criterion 194, Spin examples 454, and Python qualification 910. Counts include
comments and blank lines. The fixed top-level test-module boundary and the
entire `receive_tests.rs` test file are treated consistently.

The small cleanup gates unused helpers to tests or the retained native oracle;
no required function was removed. Adapter additions implement Git's large-push
HTTP probe and preserve recovery tokens after invalid resolution evidence.
The final functional/resource and provider records pass under their documented
conditions. The 30-pair WASIp2 SHA-1 8 MiB push timing remains above the owner
review threshold, so the native oracle is retained. This is also the deletion
review decision: no deletion is accepted in this tranche.
