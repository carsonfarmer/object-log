#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
pid=
backend_pid=
cleanup() {
  [ -z "$pid" ] || { kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; }
  [ -z "$backend_pid" ] || { kill "$backend_pid" 2>/dev/null || :; wait "$backend_pid" 2>/dev/null || :; }
  rm -rf "$tmp"
}
fail() { cat "$tmp/spin.log" >&2; exit 1; }
trap cleanup EXIT INT TERM

python3 - "$tmp" <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import sys

directory = Path(sys.argv[1])
class Backend(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass
    def do_PUT(self):
        (directory / "hits").write_text("hit")
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()
server = HTTPServer(("127.0.0.1", 0), Backend)
(directory / "port").write_text(str(server.server_port))
server.serve_forever()
PY
backend_pid=$!
tries=0
until [ -s "$tmp/port" ]; do
  tries=$((tries + 1))
  [ "$tries" -lt 50 ] || fail
  sleep 0.1
done

cat >"$tmp/variables.toml" <<EOF
wal_endpoint = "http://127.0.0.1:$(cat "$tmp/port")"
wal_bucket = "config-test"
wal_region = "us-test-1"
wal_prefix = "spin/config-test"
wal_access_key = "config-test-key"
wal_secret_key = "config-test-secret"
git_auth_mode = "password"
git_password = "config-test-password"
git_boot_id = "config-test-boot"
EOF

cd "$root/examples/git"
spin up --listen 127.0.0.1:19101 --variable "@$tmp/variables.toml" >"$tmp/spin.log" 2>&1 &
pid=$!
url='http://127.0.0.1:19101/sha1.git/info/refs?service=git-upload-pack'
tries=0
until status=$(curl --max-time 2 -sS -D "$tmp/headers" -o /dev/null -w '%{http_code}' "$url" 2>/dev/null); do
  tries=$((tries + 1))
  [ "$tries" -lt 50 ] || fail
  sleep 0.1
done

[ "$status" = 401 ] || fail
grep -q '^x-git-boot-id: config-test-boot' "$tmp/headers" || fail
target_id=$(python3 - "$tmp/port" <<'PY'
import hashlib, pathlib, sys
endpoint = "http://127.0.0.1:" + pathlib.Path(sys.argv[1]).read_text()
print(hashlib.sha256("\0".join((endpoint, "config-test", "us-test-1", "spin/config-test", "")).encode()).hexdigest())
PY
)
grep -q "^x-git-target-id: $target_id" "$tmp/headers" || fail
status=$(curl --max-time 5 -sS -X POST -o /dev/null -w '%{http_code}' 'http://127.0.0.1:19101/_validate_backend')
[ "$status" = 401 ] || fail
[ ! -e "$tmp/hits" ] || fail
status=$(curl --max-time 5 -sS -u git:wrong -X POST -o /dev/null -w '%{http_code}' 'http://127.0.0.1:19101/_validate_backend')
[ "$status" = 401 ] || fail
[ ! -e "$tmp/hits" ] || fail
status=$(curl --max-time 10 -sS -u git:config-test-password -X POST -o "$tmp/body" -w '%{http_code}' 'http://127.0.0.1:19101/_validate_backend')
[ "$status" = 503 ] || fail
[ -e "$tmp/hits" ] || fail
[ "$(cat "$tmp/body")" = 'backend validation failed' ] || fail
status=$(curl --max-time 5 -sS -u git:config-test-password -o /dev/null -w '%{http_code}' "$url")
[ "$status" != 401 ] || fail
