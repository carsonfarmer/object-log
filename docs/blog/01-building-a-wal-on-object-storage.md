# Building a WAL on object storage

*Draft for review.*

I want to build storage-backed applications without making every application
solve ordering, recovery, and garbage collection again.

The starting point for `object-log` was Cursor's
[Git at any scale](https://cursor.com/blog/git-at-any-scale). It got me thinking
about how much of the storage design could stand on its own. Git is a useful
way to test that idea: people already understand commits, branches, pushing,
and cloning, and the ordinary Git client gives us a demanding test of whether
the result works.

The library underneath that example is a Rust write-ahead log over conditional
object storage. An application supplies bytes describing its operations and
state. The log supplies their durable order, explicit recovery outcomes, and
the machinery to keep referenced data alive.

## Publish through one object

Each logical log has one mutable object, `index.cbor`. Everything else is
immutable: commits, payloads, checkpoints, and collection plans.

An application first writes the data needed for an update. It then prepares
an immutable commit against the head it read. Finally, it asks the object
store to replace that head only if its version still matches.

```text
write immutable data
        |
write an immutable commit referencing that data
        |
conditionally replace the observed head
        |
the update is published
```

The final conditional write decides whether the update becomes part of the
log. Uploading data alone does not publish application state.

Suppose two writers read the same head. Both can prepare and upload data.
Only one can replace that exact version of the head. The other must read the
winner and decide whether its operation still makes sense. The library does
not silently apply an operation to a state the application never checked.

There is one head per logical log. Separate repositories can use separate
logs. A single busy log still has a serialized publication point; this design
does not make one object store support unlimited writes to the same key, or
provide transactions across unrelated logs.

The backend must support the required conditional operations and consistent
reads. `object-log` probes those capabilities when creating a validated
backend handle. An S3-shaped API alone is not enough. The current implementation
uses the established Rust `object_store` crate for storage access.

## A timeout is an uncertain result

A conditional write can succeed and its response can disappear. The writer
then has a timeout, while another reader may already see the new state.

For a command such as “increment this counter,” submitting the command again
as a new operation can apply it twice. Reporting failure would also be wrong
if the first write was published.

The commit API therefore has three outcomes:

- `Committed`: publication is confirmed.
- `Conflict`: the candidate did not publish and a newer view is available.
- `Pending`: there is not enough information to make either claim safely.

Before publication, an application can obtain a recovery token for the exact
prepared candidate. If it needs recovery after losing the process, it must
persist that token before committing. Resuming it checks the original
candidate and its original publication position. It does not turn it into a
new command against current state.

Recovery evidence has a bounded lifetime. If enough history has been retired,
the answer can be `Expired`. That means the outcome is indeterminate, not
that the operation failed. Applications still need a policy for that case.

This is also why the log cannot transparently give an ordinary Git client a
new recovery protocol. After losing a push response, a user can inspect remote
refs to check the visible result. An application built directly against the WAL
can preserve and use the richer recovery token.

## Bytes, references, and sparse reads

The core knows nothing about branches, keys, or tables. An application can
put a small operation directly in a commit, or reference larger immutable
objects. Reference nodes contain application bytes and explicit child
references, so an application can build a tree without managing storage paths.

Large byte values use the log's streaming API. The log handles their chunks
and authenticated range reads. A caller can request a range without loading
the entire value. This keeps chunk bookkeeping out of each consumer.

Immutable object keys include both a content digest and a random physical
identity. Those serve different purposes. The digest lets the library verify
the bytes it reads. The physical identity prevents an old deletion from
accidentally targeting a later write of identical content. This is not a
global deduplication scheme.

The explicit references also define the graph the collector must preserve.
An application cannot hide a necessary object reference inside opaque bytes
and expect the collector to discover it.

Local state is a cache. A new process can reconstruct a view from the head,
its checkpoint, and the remaining log entries. The application chooses
whether that means rebuilding an in-memory state machine or opening a root
whose children it will read on demand.

## Checkpoints make deletion possible

An ever-growing log would make recovery progressively more expensive.
Applications publish checkpoints describing the state at a particular log
position, including the roots of every object that state still needs. The
head can then retire the covered prefix while retaining recent outcome
evidence for recovery.

Collection uses that same head as its authority. The library validates the
live graph, writes an explicit list of objects to delete, and conditionally
installs that plan in the head before deleting anything. Publication checks
the collection fence so a concurrent writer cannot bring a planned object
back into the live graph.

If deletion stops halfway through, another process resumes the same plan.
Already-missing objects are harmless. The plan is cleared only after its
deletions complete.

A long reader registers a retention before opening application data. Any
active retention prevents a new collection plan. This is deliberately coarse:
a reader can delay collection for the entire log. Retentions do not disappear
on a timer. Recovering a retention lost with a stopped process requires
stopping new traffic and draining readers first.

These choices avoid a separate durable coordinator, but they come with
operational obligations. External bucket lifecycle rules must not delete
protocol objects, and applications must checkpoint before reaching the
configured tail limit.

## Put the abstraction under load

The Git example publishes its refs and sparse object catalog together through
one WAL commit. It uses the generic byte streams and reference nodes, while
Git negotiation and repository rules stay outside the Rust library.

Large Git objects required streaming storage. Cold repository access required
sparse reads. Pushes with
uncertain responses required honest publication outcomes. Concurrent fetch
and collection required explicit reader retention.

The repository also has a small key-value state-machine example. I am
extending it into a usable store with sparse reads and writes. It should reuse
those properties while supplying its own index and query semantics. If it has
to rebuild recovery or chunk handling, there is more work to do in the WAL.

The API and durable format are still pre-release. The repository contains
memory, filesystem, fault-injection, MinIO, and Git-client tests, alongside
the [protocol description](../design.md). The core can be tried with an
in-memory backend using the [README example](../../README.md); running the
Git service is the subject of the next post.
