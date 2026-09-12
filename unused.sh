#!/usr/bin/env bash

set -e
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR"

# 检查并自动安装 rust-analyzer
if ! command -v rust-analyzer &>/dev/null; then
  echo ">>> 安装 rust-analyzer..."
  rustup component add rust-analyzer
fi

# 检查并自动安装 cargo-workspace-unused-pub
if ! command -v cargo-workspace-unused-pub &>/dev/null; then
  echo ">>> 安装 cargo-workspace-unused-pub..."
  cargo install cargo-workspace-unused-pub
fi

# 如果未传参，则自动查找包含 Cargo.toml 的所有一级子目录
if [ $# -gt 0 ]; then
  TARGET_DIRS=("$@")
else
  TARGET_DIRS=()
  for toml in */Cargo.toml; do
    [ -f "$toml" ] || continue
    TARGET_DIRS+=("${toml%/Cargo.toml}")
  done
fi

for dir in "${TARGET_DIRS[@]}"; do
  [ -d "$dir" ] && [ -f "$dir/Cargo.toml" ] || continue

  scip_file="$dir/index.scip"
  rust-analyzer scip "$dir" --output "$scip_file" >/dev/null 2>&1 || true

  echo "# $dir"
  RUST_LOG=error cargo workspace-unused-pub "$dir" 2>&1 | awk -v dir="$dir" '
    /\.rs$/ && !/^[ \t]/ { file = dir "/" $1; next }
    /^[ \t]*[0-9]+/ {
      line_num = $1
      $1 = ""
      sub(/^[ \t]+/, "", $0)
      sub(/[ \t]*(\{\}|\{)?[ \t]*$/, "", $0)
      if (file != "") {
        print file ":" line_num ": " $0
        count++
      }
      next
    }
    END {
      if (count > 0) {
        print "(" count " unused)"
      } else {
        print "(none)"
      }
    }
  '
  echo ""
done


