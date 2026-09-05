# Project diary

## Decisions log

| Timestamp | Decision | Choice | Rationale | Revisit if | Commit |
| --- | --- | --- | --- | --- | --- |
| 2026-09-04 16:36 PDT | Keep the full Git target | Complete fetch, push, WASI, Spin, and provider gates | Source-size estimates must improve design, not remove required behavior | An upstream library replaces the same behavior with less code and equal evidence | `1e0c0d3` |
| 2026-09-04 16:36 PDT | Preserve Task 3 as a checkpoint | Push the clean feature branch but do not integrate it | Focused and WASIp2 gates pass, but small-object Git GC cases fail | Variable durable chunk geometry passes the full gate | `f20a8d9` |
| 2026-09-04 16:36 PDT | Keep one durable authority | Use the object-log head as the only mutable publication point | This preserves atomic publication, uncertain-result recovery, and collection safety | Owner approves a different authority model | `b322985` |

## Learnings

### 2026-09-04 16:36 PDT - Durable chunk size is part of the backend contract

A fixed proof-specific chunk size can violate a valid object-log
`max_object_bytes` setting. Derive chunk geometry from authenticated child
lengths and test small limits before Task 3 integration.

---

### 2026-09-04 16:36 PDT - Source-size targets need a stable scope

Count core and each proof crate separately. Use size as an architecture signal.
Do not stop required functionality or move code between crates only to change a
count.

---

## Open questions

- [ ] Can Task 3 avoid retaining a full catalog for `ls-refs` without adding a
  second repository path?
- [ ] Which measured threshold should trigger Git pack compaction?
- [ ] Can `sley-protocol` remove enough wire code without weakening zero-copy
  parsing, operation bounds, or Rust 1.97 support?
