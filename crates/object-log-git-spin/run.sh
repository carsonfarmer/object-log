#!/bin/sh
# Single live component; qualify allocator behavior with tests/check_transport.py.
set -eu
for argument in "$@"; do
    case "$argument" in
        --disable-pooling) echo 'The Git memory contract requires Spin pooling.' >&2; exit 2 ;;
    esac
done
adapter_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export SPIN_MAX_INSTANCE_COUNT=1
export SPIN_WASMTIME_INSTANCE_COUNT=1
exec spin up --from "$adapter_dir/spin.toml" --max-instance-memory 134217728 "$@"
