# Shallow, partial, authentication and maintenance acceptance

Source candidate: `c0b9812`, plus shared provider runner and documentation changes
in this tranche. Accepted main before integration: `0a0d472`.

The combined workspace gate passes **395 tests, 21 opt-in ignored**, formatting,
strict native Clippy, locked Git WASIp2 checking and strict Spin WASIp2 Clippy.
The release component builds. All six actual-WASI memory lifecycle fixtures and
the separate Git request/byte audit pass. Original small-object GC tests pass.

The seven local provider targets pass: core recovery/GC, Git recovery/GC, four
Spin lifecycles, operator status/resume/1024-tail checkpoint and three cycles,
auth credential-helper lifecycle and rejection-before-body/backend, shallow
clients, and partial/promisor clients. Both hash formats pass. Partial retrieval
survives checkpoint/collection and cold restart; shallow checks include relative
and absolute deepening, unshallow, time/ref exclusions and merges.

Commands: `CARGO_INCREMENTAL=0 make check`; release Spin WASIp2 build;
`make git-spin-memory-acceptance git-performance-acceptance`; and, with
`OBJECT_LOG_MINIO_BINARY=/tmp/object-log-minio-native/minio`,
`make minio-test git-minio-test git-spin-minio-test git-spin-operator-minio-test
git-spin-auth-minio-test git-spin-shallow-test git-spin-partial-test`.
Python process-group regression checks also pass, including a child that stops
listening but ignores termination. The earlier mistaken invocation of nonexistent
`check_protocol_processes.py` exited 2; the correct `check_process_cleanup.py`
was then run successfully. No result is attributed to the nonexistent script.

Provider environment: native Darwin/arm64 MinIO
`RELEASE.2025-09-07T16-13-09Z`, commit
`07c3a429bfed433e49018cb0f78a52145d4bedeb`, SHA-256
`7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4`.
The runner starts an isolated loopback process and temporary bucket, verifies
listener ownership, and stops/removes only its own resources. Default Docker
mode remains pinned and unchanged. Native qualification exercises identical
assertions; it does **not** establish Docker or Linux 128 MiB runtime qualification.
Docker remains unresponsive and owner approval to restart is pending.

Independent review found no blockers in combined shallow/partial selection,
auth-before-storage/body, conservative maintenance and cold fixture isolation.
Separate runner review exercised startup failure, missing log, another process's
healthy listener and Cargo failure exit preservation. Fixture fixes supersede
prior parent-only shutdown evidence; they require complete process-group drain.

Open: packfile URIs (#24), capacity (#26), catalog migration/compaction (#19),
request admission accounting (#36), operator collection/general cold memory (#32),
and Spin runtime/admission/pooled HTTP (#21/#22). This tranche does not complete
the broader Git proof. No upstream communications occurred.
