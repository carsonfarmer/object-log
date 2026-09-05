#!/bin/bash
set -euo pipefail
native="${OBJECT_LOG_NATIVE_MINIO_BINARY:-/tmp/object-log-minio-native/minio}"
expected=7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4
actual="$(shasum -a 256 "$native" | cut -d ' ' -f 1)"
[[ "$actual" == "$expected" ]]
run_dir="$(mktemp -d /tmp/object-log-operator-native.XXXXXX)"
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
endpoint="http://127.0.0.1:$port"
export MINIO_ROOT_USER=objectlog
export MINIO_ROOT_PASSWORD=objectlog-local-test-secret
export MINIO_BROWSER=off
"$native" --version
printf 'Native provider SHA256: %s\nRun directory: %s\n' "$actual" "$run_dir"
"$native" server --address "127.0.0.1:$port" --console-address 127.0.0.1:0 "$run_dir/data" > "$run_dir/minio.log" 2>&1 &
provider_pid=$!
cleanup() {
  local result=$?
  trap - EXIT INT TERM
  kill -TERM "$provider_pid" 2>/dev/null || true
  wait "$provider_pid" 2>/dev/null || true
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for _ in $(seq 1 30); do
  if curl --connect-timeout 1 --max-time 1 --fail --silent "$endpoint/minio/health/ready" >/dev/null; then break; fi
  sleep 1
done
curl --connect-timeout 1 --max-time 1 --fail --silent "$endpoint/minio/health/ready" >/dev/null
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url "$endpoint" s3api create-bucket --bucket object-log-test
export OBJECT_LOG_MINIO_ENDPOINT="$endpoint"
export OBJECT_LOG_MINIO_ACCESS_KEY="$MINIO_ROOT_USER"
export OBJECT_LOG_MINIO_SECRET_KEY="$MINIO_ROOT_PASSWORD"
export OBJECT_LOG_MINIO_BUCKET=object-log-test
export OBJECT_LOG_OPERATOR_BINARY="$PWD/target/release/object-log-git-maintain"
export CARGO_INCREMENTAL=0
cargo +1.97.1 test --locked -p object-log-git-spin --features operator --test operator_minio operator_minio_status_and_exact_resume_preserve_both_hashes -- --ignored --nocapture
if [[ "${1:-all}" == all ]]; then
  cargo +1.97.1 test --locked -p object-log-git-spin --features operator --test minio spin_minio_ -- --ignored --nocapture
fi
