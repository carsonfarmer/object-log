# Operator configuration compatibility with Spin authentication

The operator accepts optional `auth_mode`, `auth_read_token` and
`auth_write_token` fields through the same `AuthConfig::parse` implementation
as the HTTP service. If all three are absent, storage-only configuration remains
valid. If any is explicit, the complete policy is validated before provider
access; omitted mode means basic and omitted tokens mean empty. Invalid policy
returns bounded static invalid_config JSON. HTTP credentials do not confer or
restrict local maintenance privileges.

The binary includes the existing auth module for configuration parsing, with
unused HTTP authorization code explicitly allowed in that binary only. No
second token parser or HTTP listener was introduced. The provider fixture opts
into disabled HTTP auth explicitly; it still uses private files and isolated
local MinIO prefixes. The README explains both configuration and authority.

## Validation

This combined qualification includes checkpoint command `450fd6f`, authentication
`947c72e`, and the shared maintenance accounting correction `421de4c`. The correction
releases an empty BTreeMap root before its reservation and precharges all 16
checkpoint identity-collision attempts using the core's checked, conservative
encoded-size bound. Independent review confirmed both fixes and approved the
optional configuration hook and its tests.

After those dependencies, 175 core memory/filesystem tests pass (three provider
tests remain opt-in). The Spin package passes 26 ordinary tests: 10 library and
16 binary tests, including the three shared auth tests in each target. New
operator checks cover storage-only, explicit disabled, read-only credential,
write-only credential and distinct dual-credential policies; malformed, empty,
unknown and equal-role configurations reject with no TCP connection and no
credential output. Strict native and WASIp2 all-target/all-feature Clippy,
formatting and release CLI/component builds pass.

Before the helper correction, the auth-compatible operator MinIO lifecycle passed
for both hashes in 16.84 seconds. Final corrected-provider qualification remains
pending Docker recovery and complete process-group cleanup. The lifecycle covers:
missing-target protection, exact token resume/idempotence/loser, full 1,024-tail
checkpoint recovery, wrong-format rejection, empty-tail idempotence, cold Spin
clone/fsck and three more actual Spin push/checkpoint/fetch cycles per hash.
Because the auth service sanitizes errors, the full-tail assertion now combines
an exact shared-engine call-limit rejection on authenticated WAL state, a real
Spin HTTP 400 and an unchanged head. It does not rely on private error text in
service logs or accept an arbitrary client failure.

Raw logs are in the adjacent directory. These remain small repository fixtures,
not 1,024 HTTP pushes, indefinite sustained operation, or a general 128 MiB
whole-process memory guarantee. The unresolved cold-resume graph-memory scope
and collection/retention commands remain in #32. Builds are separate from runtime.

Two runs during host disk exhaustion failed an early status assertion. The first
harness did not print its bounded report; the second captured backend_unavailable.
Both failure logs are retained. Root independently observed a full host disk and
explicit out-of-space failures in other concurrent gates. Provider tests paused
until cleanup restored space. The harness now records bounded JSON outcomes,
without provider error chains or credentials; no product retries or weakened
assertions were introduced.

After disk cleanup restored 43 GiB, Docker remained unresponsive even to read-only
queries. Root is coordinating shared-service recovery; this worktree cancelled
its blocked provider command before a fresh container was created. Separately,
fixture shutdown was corrected in `ade8be0`: earlier runs left HTTP trigger children
idle after killing their launcher. The new gate requires the complete process
group and listener to disappear before maintenance. See
[shutdown evidence](spin-fixture-shutdown-2026-09-05.md). This candidate's local
configuration behavior passes its focused tests, but corrected full provider
acceptance remains required before closing the operator qualification work.

## Additional native-provider qualification

The corrected process shutdown, authentication-compatible operator and shared
maintenance correction subsequently passed native MinIO lifecycles for both
hashes. See [the combined operator evidence](spin-operator-default-branch-2026-09-05.md).
Docker-image requalification remains pending and distinct.
