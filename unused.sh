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
  # 上游检测器语义：候选非零退出并打 "Found N possibly unused" 行；崩溃（panic）
  # 则无报告行。有报告行 → 继续分类甄别；无报告行 → 检测故障点名计红
  # （检测器故障不冒充绿灯，原语义保留）。
  set +e
  scip_out=$(RUST_LOG=error cargo workspace-unused-pub "$dir" 2>&1)
  detector_rc=$?
  set -e
  if [ $detector_rc -ne 0 ] && [[ "$scip_out" != *"possibly unused"* ]]; then
    echo "❌ $dir: cargo workspace-unused-pub 崩溃（无报告输出），计红" >&2
    printf '%s\n' "$scip_out" | tail -5 | sed 's/^/     | /' >&2
    FAILED="$FAILED $dir(detector)"
    continue
  fi
  # 报告行分三类：
  # 1. tests/ 下条目 = #[test]/#[tokio::test] 等 harness 入口，引用由测试框架
  #    在编译期注入、scip 不可见 → 永久误报，计数跳过的同时不淹没真阳性；
  # 2. known-external-trait 白名单 = 外部 crate trait 的必需 impl 方法（scip 只
  #    索引本 workspace、看不见 trait 定义），逐条附缘由登记，trait 面变更时须
  #    同步复核本表；
  # 3. 其余 = 真阳性，逐条列出并计红。
  set +e
  report=$(printf '%s\n' "$scip_out" | awk -v dir="$dir" '
    BEGIN {
      found = -1
      known["wedb/wtls/src/client.rs has_certs"] = 1
      known["wedb/wvector/src/provider/data_provider.rs status_by_external_id"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs is_not_start_point"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs num_starting_points"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs post_process_step"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs prune_accessor"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs insert_search_accessor"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs search_strategy"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs search_post_processor"] = 1
      known["wedb/wvector/src/provider/dynamic_quant.rs get_delete_element"] = 1
    }
    /\.rs$/ && !/^[ \t]/ { file = dir "/" $1; next }
    /Found [0-9]+ possibly unused/ {
      for (i = 1; i <= NF; i++) if ($i == "Found") { found = $(i + 1) + 0; break }
      next
    }
    /^[ \t]*[0-9]+/ {
      line_num = $1
      $1 = ""
      sub(/^[ \t]+/, "", $0)
      body = $0
      sub(/[ \t]*(\{\}|\{)?[ \t]*$/, "", body)
      if (file == "") next
      if (file ~ /(^|\/)tests\//) { harness++; next }
      name = ""
      if (match(body, /fn [A-Za-z0-9_]+/)) { name = substr(body, RSTART + 3, RLENGTH - 3) }
      if ((file " " name) in known) { extfp++; next }
      print file ":" line_num ": " body
      count++
      next
    }
    END {
      summary = sprintf("(%s unused; harness=%d known-external-trait=%d)", \
        (count > 0 ? count : "none"), harness + 0, extfp + 0)
      print summary
      # 对账：上游 "Found N" 总数必须等于分类计数之和——不等即报告不完整
      # （如 scip 过期时上游 warn 吞条目），方向取红不冒充绿灯
      if (found >= 0 && found != count + harness + extfp) {
        printf("❌ 报告对账失败：Found=%d ≠ 分类合计 %d（scip 或检测器异常），计红\n", \
          found, count + harness + extfp)
        exit 1
      }
      if (count > 0) exit 1
    }
  ')
  awk_rc=$?
  set -e
  printf '%s\n' "$report"
  case $awk_rc in
    0) ;;
    1) FAILED="$FAILED $dir(true-positive)" ;;
    *) echo "❌ $dir: 输出解析（awk）失败，计红" >&2; FAILED="$FAILED $dir(awk)" ;;
  esac
  echo ""
done

if [ -n "$FAILED" ]; then
  echo "❌ 检测故障目录:$FAILED —— 本报告不完整，不得视为绿灯" >&2
  exit 1
fi


