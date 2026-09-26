#!/usr/bin/env bash

set -euo pipefail
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR"

command -v rg >/dev/null || { echo "缺少 ripgrep，allow 检测未执行" >&2; exit 1; }

FAILED=0

# 1. 检测 Rust 源码中的 lint 压制属性：#[allow]/#![allow] 与 #[expect]/#![expect]
#    （RFC 2383 的 expect 同为就地压制，不列豁免；注：匹配 "[allow" 与 "#!?[expect"；
#    cfg_attr 等复杂形态按需人工巡检）
#    rg 退出码契约：0 命中 / 1 无匹配 / 2 自身错误；错误不得冒充通过
rc=0
rg --line-number "(\[allow|#!?\[expect)" -t rust || rc=$?
if [ $rc -eq 0 ]; then
  echo "ERROR: 禁止在 Rust 源码中使用 #[allow] / #![allow] / #[expect] / #![expect] 压制警告" >&2
  FAILED=1
elif [ $rc -ne 1 ]; then
  echo "rg 扫描故障（exit ${rc}），检测未完成不得视为通过" >&2
  exit 1
fi

# 2. 检测 Cargo.toml lints 段中的 allow 压制
rc=0
rg --line-number -g 'Cargo.toml' '=\s*["'\'']allow["'\'']|level\s*=\s*["'\'']allow["'\'']' || rc=$?
if [ $rc -eq 0 ]; then
  echo "ERROR: 禁止在 Cargo.toml lints 段中使用 allow" >&2
  FAILED=1
elif [ $rc -ne 1 ]; then
  echo "rg 扫描故障（exit ${rc}），检测未完成不得视为通过" >&2
  exit 1
fi

if [ $FAILED -ne 0 ]; then
  exit 1
fi

echo "allow 检测通过"
