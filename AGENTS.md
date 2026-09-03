# object-log project rules

## Scope

- Keep this project independent from Spin and spincast.
- Build one generic object-storage log contract.
- Do not add a second authority protocol without owner review.
- Do not add garbage collection until checkpoint safety and uncertain-result
  resolution have tests.
- Do not use OpenSpec.

## Design

- Keep the public API small and byte-oriented.
- Keep domain rules outside the core log.
- Use immutable content-addressed objects for commits, blobs, and checkpoints.
- Use one small conditional-write head as the publication point.
- Return an explicit pending result when a store error can hide a successful
  publication.
- Treat local state as a cache. Recovery must work without it.
- Prefer established crates over custom storage clients.

## Work ownership

- Each implementing agent must use an exclusive Git worktree.
- Only the root agent integrates changes into `main`.
- Agents must not edit files assigned to another active work stream.
- Each work stream must end with focused tests and a short evidence report.

## Verification

- Run formatting, lint, and tests before each integration commit.
- Test memory and filesystem storage before MinIO.
- Keep network-backed tests opt-in and local.
- Record benchmark conditions and raw Criterion output.
- Never claim remote object-store performance from simulated latency alone.

## Commit attribution

End Codex commits with this trailer exactly once, separated from the body by
one blank line:

Co-authored-by: Codex <noreply@openai.com>
