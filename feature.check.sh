#!/usr/bin/env bash
# 逐 crate 的 feature 声明门禁：对 workspace 每个 crate 单独 cargo check -p <crate>
#
# 为什么需要这一条（判据与案例见 task/ing/wbase-feature-decl-gate.md）：
# 两道既有门禁都是 workspace 级统一构建 —— ./test.sh 走 cargo nextest run --all-features、
# sh/clippy.sh 走 cargo clippy --tests --all-targets --all-features。cargo 在同一棵依赖图里
# 做 feature 统一，只要另有 crate 在清单里替它打开过某个 feature，漏写声明的一方就照样绿；
# 而 wbase 的 24 个模块全挂在 #[cfg(feature = "...")] 上、wbase 自身 default = []，
# 所以漏声明只有「单独构建该 crate」时才暴露（E0432 附 "the item is gated behind the
# <feature> feature"）。本脚本把这一类破口变成独立红点。
#
# 用法：
#   ./feature.check.sh                    # 全 workspace 逐 crate（含 tests/benches/examples）
#   ./feature.check.sh waof wcol wedb     # 只查指定 crate
#   LIB_ONLY=1 ./feature.check.sh         # 只查 --lib，快，但查不出 tests/ 侧的漏声明
#   CRATES_ARGS="--no-default-features" ./feature.check.sh   # 追加 cargo 端开关
#
# 退出码：0 = 全部单独可构建；1 = 有 crate 单独构建失败（末行点名）。
# 每个 crate 的完整输出留在 ${LOG_DIR}，失败者另打尾部 40 行。

set -uo pipefail

DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR/wedb" || exit 1

export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$DIR/wedb/target}
LOG_DIR=${LOG_DIR:-/tmp/feature.check}
mkdir -p "$LOG_DIR" || exit 1

META=$(cargo metadata --no-deps --format-version 1 2>/dev/null)
if [ -z "$META" ]; then
  echo "cargo metadata 失败，无法枚举 crate" >&2
  exit 1
fi
ALL=$(printf '%s' "$META" | jq -r '.packages[].name' | sort)

if [ $# -gt 0 ]; then
  CRATES="$*"
else
  CRATES="$ALL"
fi

TARGETS="--all-targets"
[ "${LIB_ONLY:-0}" = "1" ] && TARGETS="--lib"

TOTAL=0
FAILED=""
for c in $CRATES; do
  TOTAL=$((TOTAL + 1))
  log="$LOG_DIR/$c.log"
  # shellcheck disable=SC2086
  if cargo check -p "$c" $TARGETS ${CRATES_ARGS:-} >"$log" 2>&1; then
    printf 'ok   %s\n' "$c"
  else
    printf 'FAIL %s  (%s)\n' "$c" "$log"
    tail -40 "$log" | sed 's/^/     | /'
    FAILED="$FAILED $c"
  fi
done

echo
if [ -n "$FAILED" ]; then
  echo "单独构建失败:${FAILED}  (共 $TOTAL 个)"
  echo "多为清单漏声明 feature：该 crate 的 src/tests 引用了门控模块，却没在自己的 [dependencies] 里点名。"
  echo "补法：cargo add wbase --features <name>（或把 feature 名加进该 crate Cargo.toml 的 wbase features）。"
  exit 1
fi
echo "全部 $TOTAL 个 crate 单独构建通过"
