# Stop complete Spin fixture process groups

The Rust provider fixtures previously stopped only the Spin launcher. Spin 4.0.2
spawns a separate HTTP trigger process; those triggers survived launcher exit.
The operator worktree had 75 orphan triggers with its exact runtime-config path.
They were terminated, and root separately cleaned its own fixture processes.
This corrects a harness leak and the earlier claim that all serving processes
were stopped before maintenance. It does not establish the cause of separate
provider failures during host disk exhaustion.

Both Rust fixtures now start a dedicated process group. A shared test-only
shutdown helper signals that group, polls the launcher without blocking wait,
and requires both group disappearance and listener refusal. After two seconds
it escalates to group-wide SIGKILL, and it returns an error after four seconds.
The host retains its child handle on error so Drop can retry cleanup. Ordinary
shutdown errors propagate before any checkpoint or cold restart proceeds.

The direct operator fixture uses SIGTERM. The timed fixture uses SIGINT so
/usr/bin/time can reap its child and finish the RSS report; an empty report is a
failure. Process-group control is Unix-only, matching these local tool fixtures.
No production transport or log semantics changed.

Two focused ignored tests start actual Spin instances without contacting a
provider, stop them, and confirm idempotent cleanup. The operator test passes in
0.28 seconds; the timed fixture passes in 0.14 seconds and retains a nonempty raw
RSS report. Strict native and WASIp2 all-target/all-feature Clippy and formatting
pass. Independent review identified and then confirmed fixes for unbounded wait,
insufficient listener-only verification, and lost ownership on cleanup error.
Raw focused results and the timing report are in the adjacent directory.

Docker remained unresponsive after disk exhaustion and cleanup, so corrected
full MinIO lifecycles are still pending the owner's shared-service recovery.
These focused tests are process-lifecycle evidence, not provider or memory-cap
qualification. The fixture test names are:

- operator_spin_process_group_shutdown_closes_listener
- spin_host_process_group_shutdown_closes_listener

The latter needs the normal OBJECT_LOG_MINIO_* fixture variables; dummy values
with endpoint http://127.0.0.1:9 suffice because no HTTP request is made.

## Additional native-provider qualification

The corrected process shutdown, authentication-compatible operator and shared
maintenance correction subsequently passed native MinIO lifecycles for both
hashes. See [the combined operator evidence](spin-operator-default-branch-2026-09-05.md).
Docker-image requalification remains pending and distinct.
