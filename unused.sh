#!/usr/bin/env bash

set -euo pipefail
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR"

if ! command -v rust-analyzer &>/dev/null; then
  rustup component add rust-analyzer
fi

if ! command -v cargo-workspace-unused-pub &>/dev/null; then
  cargo install cargo-workspace-unused-pub
fi

RA_LOG=off rust-analyzer scip . --output index.scip --config-path <(echo '{"cargo":{"allTargets":false},"cfg":{"setTest":false}}') 2>&1 \
  | awk '
    /rust-analyzer: Loading/ { next }
    /Encountered duplicate scip symbols/ { skip=1; next }
    /Generating SCIP finished/ { skip=0 }
    !skip { print }
  '
cargo workspace-unused-pub
