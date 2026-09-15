#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
pid=
cleanup() {
  [ -z "$pid" ] || { kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; }
  rm -rf "$tmp"
}
fail() { cat "$tmp/spin.log" >&2; exit 1; }
trap cleanup EXIT INT TERM

cat >"$tmp/variables.toml" <<'EOF'
wal_endpoint = "http://127.0.0.1:1"
wal_bucket = "config-test"
wal_region = "us-test-1"
wal_prefix = "spin/config-test"
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
grep -q '^x-git-target-id: 0248b42a112bbb45f86b59b157ba54d51ad4415c26efdbcc57c5a2daaacfa6c4' "$tmp/headers" || fail
status=$(curl --max-time 5 -sS -u git:config-test-password -o /dev/null -w '%{http_code}' "$url")
[ "$status" != 401 ] || fail
