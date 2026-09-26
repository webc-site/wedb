#!/usr/bin/env bash

set -euo pipefail
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

FAILED=""
for dir in ${TARGET_DIRS[@]+"${TARGET_DIRS[@]}"}; do
  [ -d "$dir" ] && [ -f "$dir/Cargo.toml" ] || continue

  scip_file="$dir/index.scip"
  # scip 生成失败不得静默吞没：点名跳过该目录并计红（检测器故障不冒充绿灯）
  if ! rust-analyzer scip "$dir" --output "$scip_file" >/dev/null 2>&1; then
    echo "❌ $dir: rust-analyzer scip 生成失败，跳过该目录（计红）" >&2
    FAILED="$FAILED $dir(scip)"
    continue
  fi

  echo "# $dir"
  # 上游检测器先行落地取退出码：非零即红点名，零且无输出才配得上 (none)；
  # 输出再经 pipefail 管道喂 awk，awk 自身失败同样点名计红。
  if ! scip_out=$(RUST_LOG=error cargo workspace-unused-pub "$dir" 2>&1); then
    echo "❌ $dir: cargo workspace-unused-pub 执行失败（非零退出），计红" >&2
    printf '%s\n' "$scip_out" | tail -5 | sed 's/^/     | /' >&2
    FAILED="$FAILED $dir(detector)"
    continue
  fi
  if ! printf '%s\n' "$scip_out" | awk -v dir="$dir" '
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
  ' ; then
    echo "❌ $dir: 输出解析（awk）失败，计红" >&2
    FAILED="$FAILED $dir(awk)"
    continue
  fi
  echo ""
done

if [ -n "$FAILED" ]; then
  echo "❌ 检测故障目录:$FAILED —— 本报告不完整，不得视为绿灯" >&2
  exit 1
fi


