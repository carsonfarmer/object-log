# WAL component experiment

A binding of the existing Rust WAL for the Go Git library experiment. It adds
sessions, object handles, and prepared candidates; it does not change the WAL
format or publication protocol. Handles retain the core's staging proofs.
Refresh shares request accounting. Save a candidate token before publication
when the caller needs exact recovery across a lost reply.

Build with `cargo build --locked --release --target wasm32-wasip2`.
Compose its output with the Go HTTP component using `wac plug --plug
wal_component_probe.wasm main.wasm -o git.wasm`, then run ordinary Spin with
local MinIO. The Go experiment contains the opt-in client test.

The S3 transport is reused from the existing Spin adapter, including safe read
retry. Its copied source counts toward this experiment; extract the shared
transport only if this architecture is accepted. The component caps each WAL
object at 2 MiB. The Go adapter chooses smaller chunks for sparse reads.

This is a feasibility test, not the replacement Git service. It does not yet
expose maintenance, enforce the previous engine's memory/work budgets, or prove
large-object behavior. Records identify checkpoint snapshots separately from
ordinary operations. No WAL port or upstream patch is required.
