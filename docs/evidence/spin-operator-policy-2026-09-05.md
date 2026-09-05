# Spin operator policy and startup review

Base: `0368454`; exclusive worktree branch `cf/spin-operator-usability`.
Host: macOS arm64, Rust 1.97.1, Spin 4.0.2, installed Git 2.54.0.
No public service was deployed. Compilation and tests ran without a 128 MiB
process limit; this work supplies no new memory or performance qualification.

## Change and independent review

The Spin adapter now accepts a string `read_only` variable, default `false`.
Strict parsing rejects invalid values. With `true`, receive discovery and POST
(including the `0000` probe) return 403 before headers, storage or body reads.
Upload operations remain available. This is a Git push gate, not authentication,
S3 permission reduction or cancellation of already running requests. Every
serving process must use the policy. Eight product Rust lines and two manifest
lines are added; generic WAL and shared Git engine source are unchanged.

The adapter README supplies a concrete private TOML configuration and loopback
startup command, storage-backed readiness and first push/clone steps. It now
states that clients are unauthenticated, backend probes require writable storage,
and missing heads initialize automatically. It identifies the missing operator
maintenance command instead of presenting library-only instructions as a usable
operator workflow. The broken Linux evidence link is corrected.

An independent read-only agent reviewed correctness and simplification of the
policy, tests and docs. Its follow-up review caught that a host-generated 500
could satisfy invalid-policy status assertions; the tests now require the
adapter's exact error body too. Scope stays at one
repository; no SaaS, tenant machinery, native HTTP host or public auth design.

## Focused evidence

- Release WASIp2 component build passed with the lockfile unchanged.
- Formatting and strict native all-target Spin Clippy passed; strict Spin
  WASIp2 library Clippy passed.
- Core `cargo test --locked --features test-util -p object-log`: 153 tests pass,
  one opt-in test ignored. This includes memory and filesystem checks and ran
  before MinIO. Spin native tests: seven pass, three opt-in tests ignored.
- Actual Spin HTTP fixture: both hashes pass with default write policy,
  read-only rejection and invalid configuration. Both receive endpoints,
  correct/missing content types, the authentication probe and malformed bodies
  are covered with an unavailable backend. The fixture uses the README's
  `--variable @file.toml` configuration path.
- Opt-in local MinIO lifecycle passes both hashes with installed Git: writable
  pushes, read-only restart, clone/fetch/fsck, push rejected with 403, absent
  rejected ref and unchanged durable head. Checkpoint/collection and another
  fresh Spin process's cold clone/fsck still pass. Docker fixture cleanup passed.

Commands and outputs are in [the raw gate record](spin-operator-policy-2026-09-05.txt).
The two other large MinIO tests were not rerun for this policy-only change; no
new 50 MiB file, 1 GiB push or Linux runtime qualification is claimed.

## Open admission observation

The initial unpaced HTTP fixture failed twice while native compilation was also
active. The [initial captured tool output](spin-operator-admission-observation-2026-09-05.txt)
preserves the first traceback and host log (the rotating runtime file was later
overwritten). The first captured host log was:

```text
2026-09-05T03:48:36.462910Z ERROR spin_trigger_http::server: Error processing request: maximum concurrent limit of 1 for component instances reached
```

The assertion expected 200 for the next upload discovery's content-encoding
check. This is a host refusal after the preceding response, distinct from the
engine's Busy/503 mapping. The fixture now prints its explicit 50 ms gap to
isolate HTTP policy checks. `check_http.py --back-to-back` removes the gap and
retains the failing case as a probe. One initial and ten subsequent unpaced
runs passed after compilation completed; the race is intermittent, not repaired
or established to be caused by compilation. Those ten raw runs are retained.
The spaced policy results do not qualify serial or concurrent admission.

## Prioritized operator gaps

1. [#32](https://github.com/carsonfarmer/object-log/issues/32): usable local
   token-resolution/checkpoint/collection commands and safe scheduling are
   missing. Sustained pushes eventually exhaust the default 1,024-commit tail
   or earlier budgets without maintenance; coordinate compaction with #19.
2. [#21](https://github.com/carsonfarmer/object-log/issues/21#issuecomment-5549155437):
   resolve host refusal for back-to-back requests, establish predictable
   admission, and qualify cache loss/version/platform mismatch and serving
   headroom. Cache provisioning stays outside the serving budget.
3. Before public hosting, approve a concrete client authentication/TLS design.
   Reads and default writes are currently unauthenticated. Eventual #24 packfile
   URIs must share repository policy and define expiry; no tenant platform is
   needed to make one repository usable.

The current bounded pack/object budgets and missing shallow/filter/URI behavior
remain tracked in #24/#26. This tranche does not expand those features.
