#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
rev=c0b3726aa4857961e20cf8616a0df5f0741af73d
checkout="$PWD/target/spin-guest-source"
if [[ ! -d "$checkout/.git" ]]; then
  git clone --branch v4.1.0 --depth 1 https://github.com/spinframework/spin "$checkout"
fi
[[ "$(git -C "$checkout" rev-parse HEAD)" == "$rev" ]]
git -C "$checkout" diff --exit-code -- tests/test-components wit
cargo build --manifest-path "$checkout/tests/test-components/components/Cargo.toml" \
  --locked --package key-value --release --target wasm32-wasip2
SPIN_KV_TEST_GUEST="$checkout/tests/test-components/components/target/wasm32-wasip2/release/key_value.wasm" \
  cargo test --locked --test guest -- --ignored --nocapture
