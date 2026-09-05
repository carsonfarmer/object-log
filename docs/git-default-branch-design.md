# Persisted Git default branch

The Git repository stores one symbolic `HEAD` target in its WAL state. The
value is a byte-oriented, valid full ref under `refs/heads/`. It may name an
unborn branch. Deleting its referent preserves the symbolic target; it does not
select another branch or remove Git objects. This matches ordinary Git, verified
with installed Git for SHA-1 and SHA-256, including empty clone of `trunk`.

Spin reads this metadata from the shared repository. Configuration does not
rewrite it at startup. An explicit local operator command performs bootstrap
and later changes using expected-old-target comparison and the normal WAL head
CAS. Old logs retain the legacy effective target `refs/heads/main` until an
explicit metadata update. A master-only repository must be configured to master;
there is no lexical or first-push heuristic.

## API and operator

Proposed shared API:

```rust
fn default_branch(&self) -> &[u8];
async fn set_default_branch(
    self,
    transaction_id: TransactionId,
    expected: &[u8],
    target: &[u8],
) -> Result<object_log::CommitStatus, Error>;
```

The method validates names and expected state before publication, preserves the
operation budget through the exact commit, and returns the existing committed,
conflict, or pending outcome. It does not silently retry a different candidate.
A caller can retain the returned pending commit's portable recovery token. An
unborn target is valid; a ref under heads, if present, already obeys the common
commit-only branch invariant.

Proposed command:

```text
object-log-git-maintain --config repo.toml set-default-branch \
  --expected refs/heads/main --target refs/heads/trunk
```

This opens an existing WAL, applies explicit operator write authority, and uses
the established bounded result/error conventions. There is no new public HTTP
administration route. Configuration accepts UTF-8 CLI text; the library and WAL
retain byte semantics. Non-UTF-8 CLI support can use native OsString byte access
only if needed without broadening platform assumptions.

## Version transition coordinated with the catalog migration

Version 1 keeps its existing canonical five fields, numbered 0 through 4.
Version 2 has those fields plus required catalog operation (5) and metadata
operation (6). A version 1 reader rejects version 2. A version 2 reader accepts
legacy records only before a version 2 transition, and rejects missing,
unknown, misplaced, or noncanonical operations. Writers emit version 2 after
state upgrade, including ordinary ref updates and both checkpoint modes.

Metadata operations distinguish unchanged transaction state, full checkpoint
state, and expected-old/new transaction update. All default-branch bytes are
validated before application. A version 2 checkpoint always records the full
current symbolic target. Legacy snapshots restore the legacy main target.

Catalog and metadata versions are independent of catalog layout. A metadata
upgrade retains LegacyPacks mode and does not implicitly build an object index.
The catalog operation field reserves explicit legacy/tree snapshots, unchanged
transactions, migration and replacement. Tree operations remain rejected until
the catalog owner supplies their proof/state validation. The future catalog
migration must accept both version 1 and version 2 LegacyPacks state and preserve
metadata unchanged. Tree mode must reject legacy pack additions.

Supported old writers either fail to read version 2 or lose a stale head CAS.
The new materializer rejects transaction downgrades and invalid snapshot shapes.
A fresh reader cannot detect an arbitrary caller using the generic core API to
replace opaque Git state with a forged older snapshot; this is outside supported
Git writer behavior, not an additional durable authority mechanism.

## Required evidence

- Unchanged clients clone and check out main, master and trunk for both hashes.
- Unborn configured branches advertise `unborn HEAD` with the right symref;
  a subsequent push creates that branch without changing the default target.
- Changing the default changes later cold clones, without changing ref OIDs.
- Deleting the target leaves unborn HEAD; restoring it restores normal checkout.
- Checkpoint, cold recovery and GC preserve metadata and all reachable data.
- Concurrent default updates and ref pushes resolve by the existing head CAS;
  stale expected targets fail before writes. A losing update cannot clobber refs.
- Version 1 canonical records remain readable; version 2 missing metadata,
  downgrade transactions, malformed branch targets and unsupported catalog
  transitions reject before publication.
- Faults preserve exact pending and recovery semantics, counters remain
  cumulative, and memory is reserved before metadata allocation.

Protocol reference: [Git protocol v2](https://git-scm.com/docs/protocol-v2)
and [git symbolic-ref](https://git-scm.com/docs/git-symbolic-ref).
