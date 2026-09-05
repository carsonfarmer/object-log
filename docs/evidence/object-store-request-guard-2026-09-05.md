# Operation-scoped core request admission

Core candidate `f8fc256`, based on `6d63884`, adds an optional request guard to
a clone of Log. The clone preserves log identity and staging proofs. All
nested store calls and subsequent clones share admission; replacing a guard
changes only the new clone. This remains caller policy, outside durable options.

The guard runs before each core GET or conditional PUT, including fresh-object
collision retries, plan reads, missing-object classification and head publication.
Reads report their accepted body bound; writes report exact encoded payload bytes.
Listing admission occurs on first poll and retains admission until stream drop.
Delete admission describes one logical batch and its number of objects. These
are object_store client invocations, not hidden HTTP retries, listing pages,
provider delete batching, headers, or exact downloaded wire bytes. Opening and
validation before attaching a guard are outside its accounting. Memory and CPU
work still require separate limits.

Refused requests produce RequestDenied without contacting the backend. Consuming
commit/checkpoint resolution retains exact pending evidence when classification
or retry is refused. A first publication whose CAS is refused returns a definite
request error. Collection reports exclude unsubmitted delete batches; denied
best-effort plan cleanup cannot undo an already completed collection.

Eleven new tests cover zero-I/O rejection on memory and filesystem stores,
proof-preserving clones, exact collision attempts, missing-record classification,
concurrent admission, cancellation without refunds, cold commit/checkpoint
recovery, active-plan reads, and lazy-list admission lifetime. The independent
metadata reviewer reran all eleven tests and approved the frozen implementation.

Local all-feature core tests: 186 passed, 3 provider tests explicitly ignored.
Strict all-target/all-feature core Clippy, locked default-feature core WASIp2
checking, and formatting passed. Raw logs are in the adjacent directory. No
provider, HTTP-attempt, Git integration, or runtime-memory qualification is
claimed. Git wiring and removal of overlapping manual precharges are a separate
tranche; checkpoint_write_bound remains available until guarded parity passes.
