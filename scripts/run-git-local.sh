#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
for tool in make spin aws curl python3; do
  command -v "$tool" >/dev/null || { echo "Install $tool before running the Git demo." >&2; exit 2; }
done

# Build before starting disposable services, so a failed build leaves no data.
make -C "$root" git-build

demo_dir="$(mktemp -d "${TMPDIR:-/tmp}/object-log-git-local.XXXXXX")"
minio_pid=""
spin_pid=""
container=""
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$spin_pid" ]]; then
    kill "$spin_pid" 2>/dev/null || true
    wait "$spin_pid" 2>/dev/null || true
  fi
  if [[ -n "$minio_pid" ]]; then
    kill "$minio_pid" 2>/dev/null || true
    wait "$minio_pid" 2>/dev/null || true
  fi
  if [[ -n "$container" ]]; then
    docker stop "$container" >/dev/null 2>&1 || true
  fi
  rm -r -- "$demo_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

git_port="${OBJECT_LOG_GIT_LOCAL_PORT:-19100}"
python3 - "$git_port" <<'PY'
import socket
import sys

with socket.socket() as listener:
    try:
        port = int(sys.argv[1])
        if not 1 <= port <= 65535:
            raise ValueError("expected an integer from 1 to 65535")
        listener.bind(("127.0.0.1", port))
    except (OSError, ValueError) as error:
        raise SystemExit(f"Git port {sys.argv[1]} is unavailable: {error}")
PY

access_key="objectlog"
secret_key="local-test-secret"
bucket="wal-proof"
minio_binary="${OBJECT_LOG_MINIO_BINARY:-$(command -v minio || true)}"
if [[ -z "$minio_binary" && -x "$(go env GOPATH)/bin/minio" ]]; then
  minio_binary="$(go env GOPATH)/bin/minio"
fi
if [[ -n "$minio_binary" ]]; then
  [[ -x "$minio_binary" ]] || { echo "MinIO binary is not executable: $minio_binary" >&2; exit 2; }
  minio_port="$(python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
)"
  endpoint="http://127.0.0.1:$minio_port"
  MINIO_ROOT_USER="$access_key" MINIO_ROOT_PASSWORD="$secret_key" MINIO_BROWSER=off \
    "$minio_binary" server "$demo_dir/minio" --address "127.0.0.1:$minio_port" \
    >"$demo_dir/minio.log" 2>&1 &
  minio_pid=$!
else
  command -v docker >/dev/null || { echo "Install MinIO or Docker before running the Git demo." >&2; exit 2; }
  docker info >/dev/null
  container="object-log-git-local-$$"
  docker run --detach --rm --name "$container" --publish 127.0.0.1::9000 \
    --env "MINIO_ROOT_USER=$access_key" --env "MINIO_ROOT_PASSWORD=$secret_key" \
    'minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e' \
    server /data >/dev/null
  endpoint="http://$(docker port "$container" 9000/tcp)"
fi

ready=0
for _ in {1..60}; do
  if curl --connect-timeout 1 --max-time 2 --fail --silent "$endpoint/minio/health/ready" >/dev/null; then
    ready=1
    break
  fi
  if [[ -n "$minio_pid" ]] && ! kill -0 "$minio_pid" 2>/dev/null; then break; fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  if [[ -n "$minio_pid" ]]; then cat "$demo_dir/minio.log" >&2; else docker logs "$container" >&2; fi
  echo "MinIO did not start." >&2
  exit 1
fi

AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" \
  AWS_SESSION_TOKEN="" AWS_SECURITY_TOKEN="" \
  AWS_DEFAULT_REGION=us-east-1 aws --no-cli-pager --endpoint-url "$endpoint" \
  s3api create-bucket --bucket "$bucket" >/dev/null

cat >"$demo_dir/variables.toml" <<EOF
wal_endpoint = "$endpoint"
wal_bucket = "$bucket"
wal_prefix = "local-$$"
wal_access_key = "$access_key"
wal_secret_key = "$secret_key"
git_boot_id = "local-$$"
git_auth_mode = "password"
git_password = "local-git-password"
EOF
chmod 600 "$demo_dir/variables.toml"

(cd "$root/examples/git" && spin up --listen "127.0.0.1:$git_port" \
  --variable "@$demo_dir/variables.toml") >"$demo_dir/spin.log" 2>&1 &
spin_pid=$!

validated=0
for _ in {1..60}; do
  if curl --connect-timeout 1 --max-time 5 --fail --silent \
    --user git:local-git-password --request POST \
    "http://127.0.0.1:$git_port/_validate_backend" >/dev/null; then
    validated=1
    break
  fi
  if ! kill -0 "$spin_pid" 2>/dev/null; then break; fi
  sleep 1
done
if [[ "$validated" != 1 ]]; then
  cat "$demo_dir/spin.log" >&2
  echo "Spin did not validate its MinIO backend." >&2
  exit 1
fi

cat <<EOF
Git service ready at http://127.0.0.1:$git_port
  SHA-1:   http://127.0.0.1:$git_port/sha1.git
  SHA-256: http://127.0.0.1:$git_port/sha256.git
  Spin log: $demo_dir/spin.log

Create a repository with an initial push:
  git init --object-format=sha256 -b main demo
  cd demo
  echo hello > README.md
  git add README.md && git commit -m 'Initial commit'
  git push http://127.0.0.1:$git_port/sha256.git main

When prompted, use username git and password local-git-password.
Press Ctrl-C here to stop Spin and MinIO and remove their disposable data.
EOF
wait "$spin_pid"
