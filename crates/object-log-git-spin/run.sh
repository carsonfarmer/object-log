#!/bin/sh
# Single live component; qualify allocator behavior with tests/check_transport.py.
set -eu
for argument in "$@"; do
    case "$argument" in
        --runtime-config-file|--runtime-config-file=*) echo 'run.sh pins runtime-config.toml for the qualified S3 transport.' >&2; exit 2 ;;
        --disable-pooling) echo 'The Git memory contract requires Spin pooling.' >&2; exit 2 ;;
    esac
done
if [ -n "${RUNTIME_CONFIG_FILE:-}" ]; then
    echo 'Unset RUNTIME_CONFIG_FILE; run.sh uses its qualified runtime-config.toml.' >&2
    exit 2
fi
adapter_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export SPIN_WASMTIME_POOLING=1
export SPIN_MAX_INSTANCE_COUNT=1
export SPIN_WASMTIME_INSTANCE_COUNT=1
exec spin up --from "$adapter_dir/spin.toml" --max-instance-memory 134217728 --runtime-config-file "$adapter_dir/runtime-config.toml" "$@"
