#!/bin/sh
# Temporary component-toolchain fix; Spin and the Go collector remain unchanged.
set -eu
cd "$(dirname "$0")"
cache="$(pwd)/.adapter"
revision=668016926adfd1b8a79dbce894f1e203d8892599
mkdir -p "$cache/source"
if [ ! -f "$cache/source.tar.gz" ]; then
    curl -fLsS "https://github.com/bytecodealliance/wasmtime/archive/$revision.tar.gz" -o "$cache/source.tar.gz"
fi
printf '%s  %s\n' e9047b787d84dce3e034319b0acb51e3330c7cdf00b147daf220fdda7ed9c03b "$cache/source.tar.gz" | shasum -a 256 -c -
tar -xzf "$cache/source.tar.gz" --strip-components=1 -C "$cache/source"
patch --batch --fuzz=0 -p1 -d "$cache/source" < adapter.patch
CARGO_TARGET_DIR="$cache/target" cargo build --locked --manifest-path "$cache/source/Cargo.toml" \
    -p wasi-preview1-component-adapter --target wasm32-unknown-unknown --release
