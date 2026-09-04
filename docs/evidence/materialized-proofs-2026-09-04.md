# Materialized proof evidence

## Result

At revision `c173b0328c3c0ec36528b371bd37bc52920f1fe0`, `materialize`
creates process-local `StagedObject` proofs for roots named by authenticated
checkpoint and tail records. A state adapter can retain these proofs and use
them in a checkpoint against the returned view. Arbitrary references still use
`stage_objects` and full graph verification.

The proofs remain bound to one `Log` handle or its clones and one collection
epoch. A foreign handle or a changed collection epoch rejects them. Recovery
tokens do not contain them.

## Request evidence

The focused model test proves these request counts:

- Tail materialization reads no referenced node or blob.
- A proof-backed checkpoint reads one tail commit and no referenced node or
  blob.
- Materialization from that checkpoint reads no referenced node or blob.
- `stage_objects` for the same arbitrary root reads one node and one blob.
- A collection-epoch change makes the earlier proof invalid.

`make git-performance-acceptance` used 4,311-byte and 8.0 MiB Git packs. Each
checkpoint made three object-store requests: one commit GET and two PUTs. The
checkpoint downloaded 237 bytes for the small case and 240 bytes for the large
case. The prior request count increased with the pack chunk count.

## Verification

The independent Rust and safety review found no code defect. It checked proof
provenance, foreign handles, recovered evidence, collection fencing, stale
epochs, stale views, checkpoint races, and Git proof movement.

The full local gate passed after integration:

- formatting;
- strict workspace Clippy with all targets and features;
- 241 regular tests, with 9 opt-in tests ignored;
- documentation tests; and
- the no-default-feature `wasm32-wasip2` Git check.

The change adds 54 product lines and removes 29. It adds 170 test lines and
removes 12. It adds no public method, compatibility path, operator code, or
infrastructure code.

## Limit

The fast path relies on the object-store contract. Exact immutable bytes must
remain at their physical keys until object-log collection deletes them.
External lifecycle deletion or overwrite violates that contract.
