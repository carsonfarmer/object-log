# Proposal: a one-shot operator path for Spin Git maintenance

Status: proposed for root assignment; no product implementation or deployment.
Source reviewed: `d831db1`, based on `0368454`. Issues: #32 maintenance, #35
authorization, #21 runtime/admission, #19 compaction, #34 bounded materialization.

## Recommendation and next slice

Ship a local, one-shot `object-log-git-maintain` executable that opens the same
S3 WAL and invokes shared library code. An operator or an existing scheduler
runs the identical executable. Spin remains the Git HTTP host. Start with
existing-only status, exact commit-token resumption, and a conservative
checkpoint that retains every pack reference. This recovers tail-induced
serving failures without first requiring an expensive reachability walk.

The first acceptance is deliberately concrete: a default-options WAL with
1,024 small valid Git transactions, which the current serving library cannot
open within its 512-call budget, is checkpointed by a fresh operator process.
Restarted Spin must then clone, fsck and push with both hashes. Creating that
fixture through trusted test-library calls is recovery evidence; it is not
sustained Spin push evidence. #32 remains open until sustained pushes through
Spin, maintenance, interruption recovery and collection all pass.

A command wrapper alone is insufficient. The slice needs an existing-only log
open and a bounded, shared-library metadata checkpoint path. It must not call
ordinary `Repository::open` and assume extra host memory bypasses its limits.
The root should assign these prerequisites explicitly rather than allow a host
worker to duplicate the Git state format or modify core APIs incidentally.

## Execution options

| Option | Benefit | Cost and decision |
| --- | --- | --- |
| Separate authenticated Spin maintenance component/entrypoint | Uses the existing WASI S3 bridge; can run independently of a wedged serving component | Requires a separate manifest, private ingress, maintenance authorization under #35, command-body limits and lost-HTTP-response handling. The ordinary Git budget still applies unless changed in the library. A shared serving instance also inherits #21 slot refusal. Defer. |
| Local executable, invoked manually or by a scheduler | No incoming HTTP surface; OS process permission and scoped S3 credentials define the operator boundary. Can run while Spin is stopped. Uses established native `object_store` AWS support and the same Git/WAL logic | Adds a one-shot client binary, not a native Git server. Distribution and an explicit maintenance runtime envelope need qualification. Recommended. |
| Scheduler invokes a maintenance HTTP route | Operationally convenient on a platform with a reviewed private trigger | Scheduling does not confer authorization, fix resource limits or make retries safe. It depends on the first option and #35. Defer. |

There is no durable job queue, service lock in S3, per-tenant registry or new
authority record. Overlapping operators can lose head CAS and return conflict.
An optional local scheduler lock prevents wasted work; it provides no durable
correctness guarantee. Losing the scheduler, binary cache and local receipts
must leave recovery possible from the WAL and explicit caller-held tokens.

## Proposed operator interface

The following commands are a proposed contract, not currently installed tools.
Configuration reuses endpoint, bucket, region, prefix, log ID and object format
from the private serving TOML. A separately protected operator credential may
be substituted; clients never receive it. Print the selected non-secret target
and observed generation in structured results, not credentials or token bytes.
Bound token-file reads before allocation or decoding, and derive the supported
cap from the valid default-options token envelope; oversized and malformed
inputs must fail before publication. Do not use an unbounded file read.

```text
object-log-git-maintain --config /deployment/repository.toml status
object-log-git-maintain --config /deployment/repository.toml resume-commit --token-file /private/push.token
object-log-git-maintain --config /deployment/repository.toml checkpoint --retain-packs
```

Follow-up commands on the same executable are `collect`, `retentions`, and
`release-retention --id UUID`. Keep checkpoint and collection separate: a
checkpoint request never silently deletes objects. No arbitrary S3 key deletion,
raw head editing, force-unlock or token rebasing is exposed.

Return one bounded JSON result with `operation`, `outcome`, observed generation
when known, and safe counters. Exit 0 means the requested postcondition is
confirmed, 2 means input/configuration rejected, 3 means conflict/retry needed,
4 means pending/unknown, and 5 means corruption or a resource/operator block.
The `outcome` remains authoritative: `not_committed` can be a completed
resolution command (exit 0), but never means the Git push succeeded. Bound
error text and redact S3 secrets, authorization headers, ref contents and tokens.

`status` loads the existing head without opening the Git repository, loading
pack catalogs or reading blobs. It reports tail length, checkpoint-through
sequence, collection epoch/active-plan presence and, once supported, retention
IDs. A missing log is an error. Current `Log::open` creates a missing head;
introduce a small generic `Log::open_existing` operation rather than duplicate
the private key layout in the command. Preserve durable option and incarnation
checks. Backend validation may still perform its documented disposable probes;
"status" is not a promise of zero S3 writes.

## Recovery after serving limits

The normal checkpoint opens the repository, loads catalogs, walks reachable
objects and verifies unverified objects before publishing. It shares the
serving operation's 88 MiB live allocation pool, 24 MiB state, 512 calls,
96 MiB transfer and 256 MiB work limits. Running it in a different process or
container changes none of these limits.

For the first slice, add one narrow Git-library maintenance entrypoint for
`checkpoint --retain-packs`. Its contract is: materialize one authenticated
exact view using the existing `Machine`, encode the same snapshot format with
all refs and all pack descriptors/proofs, and publish it through the existing
core checkpoint CAS. It does not decode packs, discover reachability, prune
roots or invent a second Git interpreter. Factor snapshot construction with
the existing checkpoint where useful; preserve the existing pruning checkpoint
and its tests. Conservative extra references prevent reclamation of some dead
packs but cannot remove live Git objects.

This entrypoint needs a distinct bounded operation profile, not a global
increase to serving limits and not an unlimited mode. Derive and preflight the
metadata I/O allowance from the observed tail count and authenticated lengths,
including the core's publication-time tail reread, checkpoint/head encoding and
collection-plan validation. The 1,024-tail case must permit those required
calls. Keep actual cumulative counters across a bounded expired-view retry.
Use #34's ordered bounded materialization where available; its scope must also
account for `publish_checkpoint` currently calling `read_tail` again. A change
to the first read alone does not establish bounded checkpoint peak memory.

Do not change `Options` to reopen an existing WAL: options are checked against
the durable head and changing them is rejected. Do not accept arbitrary byte
limits from untrusted HTTP requests. Before implementation acceptance, record
the maintenance profile's numerical limits, accounted lifetimes, maximum
accepted metadata and measured process peak. Limits must reject before an
unsafe allocation. Include commit-token decoding and the core resume path's
full dependency verification in that envelope, not just checkpoint metadata.
The profile belongs to shared Git operation accounting;
transport and command code must not implement competing memory ledgers.

This repairs an accumulated tail, not every serving failure. A checkpoint that
retains all packs will not reduce the number of live pack catalogs, large-object
requirements or a state snapshot already beyond its own bound. Those failures
must be reported with their measured cause; #19 compaction and #26 capacity are
required for that recovery envelope. A damaged WAL is an integrity error, never
permission to skip records. Do not advertise arbitrary resource-exhaustion
recovery from the first successful tail fixture.

Use ordinary Spin defaults for serving and report maintenance resource use
separately. The library's operation budgets remain bounded; no host memory cap,
pooling override, or special Spin runtime is required for acceptance.

## Outcomes and crash recovery

| Operation/result | Required behavior |
| --- | --- |
| Commit resume: committed / not committed | Report the exact classification from `Log::resume`. This API can stage a missing commit and retry its original CAS, so it requires write/maintenance authority. |
| Commit resume: still pending | Preserve the exact input token; return pending. Retry only that token. Never infer success from current refs or create a new transaction automatically. |
| Commit resume: expired | Report unknown historical outcome and stop. Expired is not not-committed. Reconcile application intent separately; no automatic replay. |
| Checkpoint: published / conflict | Report confirmed publication or conflict. A later command may load a new view and compute a new checkpoint; it must not apply the old snapshot to a different prefix. |
| Checkpoint: pending | While alive, resolve the returned `PendingCheckpoint` with a bounded retry. If still uncertain, exit unknown without collecting. |
| Checkpoint process/receipt lost | Reload the head and recompute the current checkpoint objective. Report that the previous exact attempt is unknown, even if a fresh checkpoint or an already-checkpointed head now satisfies the objective. Do not claim exact-attempt recovery from unavailable evidence. |
| Collection start: active / pending / conflict | Load current head first. Resume only its authenticated active plan; if none exists, a fresh start is a new attempt. Never delete from an uninstalled local plan. |
| Collection finish: pending / conflict | Stop or retry from a fresh observed head. Repeated deletion of the installed positive set is safe. Completion means no active plan; counters cover this invocation, not invented lifetime totals. |

The current core has no serializable `PendingCheckpoint` and cannot expose
checkpoint evidence before CAS. The first slice therefore promises convergence
to a checkpointed current head, not exact checkpoint-attempt recovery after
process loss. This is explicit and safe because a checkpoint preserves state.
If exact checkpoint-attempt reporting is required, assign a generic prepared
checkpoint plus canonical recovery-token contract, with token persistence
before publication and cross-process verification tests. Do not serialize
private `Debug` fields or add an S3 job-status object to work around the gap.

Resolve known pending Git tokens before advancing checkpoints or collection.
Retention protects objects from GC; it does not extend the checkpoint outcome
resolution window. Losing a caller's only token loses its ability to identify
that exact attempt, not the repository's durable state. Ordinary Git clients
do not guarantee operators receive a token after every lost reply.

## Quiescence, retention and scheduling

For the first supported runbook, stop ingress and drain/stop every serving and
maintenance process for this repository before the mutation command. Read-only
mode is insufficient: reads may still run, and earlier pushes may remain active.
Check service state outside the WAL; a `--quiesced` assertion cannot prove it.
CAS, collection fences and corruption checks remain mandatory even when the
operator believes traffic is stopped. Do not add a second durable lock.

This offline slice creates no new retention IDs. Existing retentions still
block collection and must be visible. Add a narrow read-only retention-ID
accessor on `View`, not private-head parsing. Retentions have no expiry; never
release them automatically by age or clear all of them. Release only an
explicit selected ID after its owner is stopped/drained and its need for
protected objects is resolved. Retry a pending release with the same ID until
confirmed; later acquisition must use a new ID. If ownership is unknown, report
blocked and preserve the retention. Lost local receipts cannot justify release.

An existing scheduler runs the one-shot command and records exit/outcome. It
must back off on conflict, alert on persistent pending/blocked outcomes and
defer cleanup until a fresh-head reconciliation or newly confirmed checkpoint
satisfies the checkpoint objective and known pending Git commit tokens are
resolved. The interrupted checkpoint's exact historical outcome may remain
unknown; that fact alone must not block collection forever. Initial scheduling
uses planned maintenance windows; it cannot promise
uninterrupted writes. Measure a safe checkpoint interval on the supported
workload, below the earliest tail, call or memory exhaustion threshold with
headroom. A fixed "every N minutes" policy alone cannot bound bursty pushes.
Sustained acceptance requires repeated cycles of real Spin pushes, maintenance,
fresh-host reads and continued pushes, exceeding 1,024 total transactions.

## Authorization coordination and implementation acceptance

The local command requires trusted OS execution plus storage credentials with
the necessary conditional read/write/list/delete rights. Keep its config and
tokens private; no listener or public maintenance route is part of this slice.
#35 should ratify that local boundary alongside its Git read/write policy.
For any later HTTP command, maintenance is a separate privilege from clone or
push. Authenticate before backend probes/body allocation, bind authorization
to the configured repository, and test ordinary Git writers cannot invoke it.
Public deployment still requires review of the concrete #35 configuration.

Acceptance is sequential and prevents a demo wrapper from closing #32:

1. Existing-only status/commit resume: missing target does not create a head;
   invalid/foreign tokens do not mutate it; both-hash committed, losing,
   pending, expired and corrupt evidence give explicit outcomes after restart.
2. Conservative checkpoint escape: fill a default-options WAL to 1,024 valid
   small transactions reusing a bounded small pack set (for example repeated
   valid ref updates), show serving rejection, run the bounded maintenance
   process against the same WAL, then cold Spin clone/fsck and new push. Record
   head-only status availability, exact OIDs, calls/bytes and peak memory.
   A fixture with 1,024 distinct packs could still exhaust serving catalogs
   after the checkpoint and would not isolate tail recovery.
3. Faults before/after checkpoint CAS and process death: current state is
   preserved, unknown attempts stay labeled unknown, and a fresh command
   converges without cached repository state. Check concurrent CAS losers and
   late corrupt records; no partially materialized snapshot may publish.
4. Follow-up collection/retention: pending plan installation, partial deletion,
   fence clearing, process loss and safe same-ID release; unknown IDs block
   cleanup. Reopen without receipts and recover from head/plan objects.
5. Memory/fault-store tests first; retain filesystem capability rejection.
   Then opt-in local MinIO and unchanged Git through real Spin, both hashes,
   same-WAL recovery and sustained cycles beyond 1,024 pushes. Preserve raw
   runtime/call measurements. No live AWS or larger-capacity claim by analogy.

## Additional context for implementation handoff

Evidence anchors below refer to the reviewed revision; proposed APIs above do
not exist yet. No code was changed or new runtime qualification performed for
this design. An independent design agent verified the failure-to-open,
checkpoint-token, retention-enumeration and mutation-authority constraints.
Its full-proposal review requested the explicit small-pack-set fixture and
fresh-head cleanup reconciliation above; both were incorporated.

| Source | Verified constraint |
| --- | --- |
| `src/log.rs:24`, `src/log.rs:299`, `src/log.rs:1384` | Durable defaults include 1,024 tail entries; open creates absent head; options must match. |
| `crates/object-log-git/src/repository.rs:58`, `:322`; `src/budget.rs` within that crate | Repository admission and metadata preflight consume fixed serving budgets. |
| `crates/object-log-git/src/repository/receive_command.rs:353` | Existing checkpoint does reachability pruning and repeats tail accounting. |
| `src/materialize.rs:102`, `src/log.rs:1088` | Current materializer and checkpoint publication both load whole decoded tails. Coordinate #34. |
| `src/log.rs:1017`, `src/lib.rs:459` | Commit resume may publish; pending checkpoint lacks a public recovery token. |
| `src/log.rs:378`, `src/log.rs:448`, `src/lib.rs:353` | Retentions do not expire; release is retryable; public ID enumeration is missing. |
| `src/log.rs:503`, `src/log.rs:591` | Collection plan lives under head authority; positive deletes and fence clearing can resume. |

Root implementation decision: approve the local one-shot boundary and the
conservative checkpoint escape contract, assign the core/Git prerequisites to
their owners, and require measured maintenance limits before shipping. Exact
checkpoint tokens and online/authenticated scheduling are separate decisions.
