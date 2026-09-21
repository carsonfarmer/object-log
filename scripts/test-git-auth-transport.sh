#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
export GIT_TRANSPORT_SOURCE=${GIT_TRANSPORT_SOURCE:-"$root/examples/git"}
export COMPONENTIZE_GO=${COMPONENTIZE_GO:-"$root/examples/git/.componentize/bin/componentize-go"}
go test -count=1 -v "$root/scripts/auth-transport/transport_test.go"
