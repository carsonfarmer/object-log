#!/bin/sh
# Pinned adapter fix under upstream review; Spin and Go remain unchanged.
set -eu
cd "$(dirname "$0")"
cache="$(pwd)/.adapter"
revision=c8e24c308754f784fbb4a08205a2a9c08c461d00
source="$cache/source-$revision"
archive="$cache/$revision.tar.gz"
mkdir -p "$source"
if [ ! -f "$archive" ]; then
    curl -fLsS "https://github.com/carsonfarmer/wasmtime/archive/$revision.tar.gz" -o "$archive"
fi
printf '%s  %s\n' fc4bc30f49a81951d340c4411c1470fffc35a1244dad5d778aacfa0322d3a09a "$archive" | shasum -a 256 -c -
tar -xzf "$archive" --strip-components=1 -C "$source"
CARGO_TARGET_DIR="$cache/target" cargo build --locked --manifest-path "$source/Cargo.toml" \
    -p wasi-preview1-component-adapter --target wasm32-unknown-unknown --release
