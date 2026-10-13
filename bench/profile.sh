#!/usr/bin/env bash
# wedb 性能分析与 CPU Profiling 驱动脚本
#
# 用法：
#   ./bench/profile.sh --engine hash --quick
#   ./bench/profile.sh --engine bftree --scale 0.2
#   ./bench/profile.sh --engine hash --open          # 采完直接在浏览器打开 Firefox Profiler
#   ./bench/profile.sh --sample                     # 使用 macOS 原生 sample 采样器
#   ./bench/profile.sh --hotspots 25                # 显示前 25 个热点函数

set -euo pipefail

DIR=$(realpath "$0") && ROOT=${DIR%/bench/profile.sh}
ENGINE="hash"
SCALE="0.02"
BENCH="compare_benchmark"
OUTDIR="${ROOT}/.bench_run/profiles"
HOTSPOTS="20"
DO_OPEN=false
USE_SAMPLE=false

while [ $# -gt 0 ]; do
  case "$1" in
    --engine) ENGINE=${2:?--engine 需要参数}; shift ;;
    --quick) SCALE="0.02" ;;
    --scale) SCALE=${2:?--scale 需要参数}; shift ;;
    --bench) BENCH=${2:?--bench 需要参数}; shift ;;
    --outdir) OUTDIR=${2:?--outdir 需要参数}; shift ;;
    --hotspots) HOTSPOTS=${2:?--hotspots 需要参数}; shift ;;
    --open) DO_OPEN=true ;;
    --sample) USE_SAMPLE=true ;;
    -h|--help)
      echo "用法: $0 [选项]"
      echo "选项:"
      echo "  --engine <hash|bftree>    指定被测引擎 (默认: hash)"
      echo "  --quick                   快速档位 (scale=0.02)"
      echo "  --scale <F>               指定缩放因子 (默认: 0.02)"
      echo "  --bench <name>            指定基准测试名称 (默认: compare_benchmark)"
      echo "  --outdir <dir>            Profile 输出目录 (默认: .bench_run/profiles)"
      echo "  --hotspots <N>            显示前 N 个热点函数 (默认: 20)"
      echo "  --open                    采样完成后打开浏览器分析火焰图与调用栈"
      echo "  --sample                  使用 macOS 原生 /usr/bin/sample 工具"
      exit 0
      ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
  shift
done

mkdir -p "$OUTDIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DATA_PATH="/tmp/wedb_profile_${ENGINE}_${TIMESTAMP}"
rm -rf "$DATA_PATH"
mkdir -p "$DATA_PATH"

export PATH=/opt/homebrew/bin:/opt/homebrew/opt/llvm/bin:$PATH

echo "── 1. 编译基准二进制 (带符号表) ──"
CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/tmp/wedb_bench_target}
BUILD_OUTPUT=$(
  cd "${ROOT}/bench"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo bench -q -p wedb-bench-compare \
    --no-default-features --features "$ENGINE" --bench "$BENCH" --no-run --message-format=json
)

BIN_PATH=$(echo "$BUILD_OUTPUT" | grep '"executable":' | grep -v 'null' | tail -n 1 | sed -E 's/.*"executable":"([^"]+)".*/\1/')

if [ -z "$BIN_PATH" ] || [ ! -f "$BIN_PATH" ]; then
  echo "❌ 未找到编译产物二进制文件" >&2
  exit 2
fi

echo "二进制路径: $BIN_PATH"

# macOS 提取 dSYM 符号表
if command -v dsymutil &>/dev/null && [ "$(uname -s)" = "Darwin" ]; then
  echo "生成 dSYM 调试符号表..."
  dsymutil "$BIN_PATH" 2>/dev/null || true
fi

echo "── 2. 执行采样分析 (engine=${ENGINE}, scale=${SCALE}) ──"

if [ "$USE_SAMPLE" = true ]; then
  SAMPLE_OUT="${OUTDIR}/sample_${ENGINE}_${SCALE}_${TIMESTAMP}.txt"
  echo "启动 /usr/bin/sample..."
  "$BIN_PATH" --only "$ENGINE" --scale "$SCALE" --data-path "$DATA_PATH" &
  PID=$!
  sample "$PID" 10 -file "$SAMPLE_OUT" >/dev/null 2>&1 || true
  wait "$PID" || true
  echo "Sample 输出文件: $SAMPLE_OUT"
  head -n 40 "$SAMPLE_OUT"
  exit 0
fi

if ! command -v samply &>/dev/null; then
  echo "⚠️ 未安装 samply，正在通过 cargo install samply 安装..."
  cargo install samply
fi

PROFILE_OUT="${OUTDIR}/profile_${ENGINE}_${SCALE}_${TIMESTAMP}.json.gz"

if [ "$DO_OPEN" = true ]; then
  echo "运行 samply 并启动 Web 交互分析..."
  samply record --unstable-presymbolicate -o "$PROFILE_OUT" -- \
    "$BIN_PATH" --only "$ENGINE" --scale "$SCALE" --data-path "$DATA_PATH"
else
  samply record --save-only --unstable-presymbolicate -o "$PROFILE_OUT" -- \
    "$BIN_PATH" --only "$ENGINE" --scale "$SCALE" --data-path "$DATA_PATH"
fi

rm -rf "$DATA_PATH"

echo "Profile 文件保存在: $PROFILE_OUT"

echo "── 3. 提取热点函数报告 ──"
python3 "${ROOT}/bench/profile_analyzer.py" "$PROFILE_OUT" "$HOTSPOTS"

echo "💡 提示: 可通过以下命令随时在浏览器查看完整交互火焰图:"
echo "   samply load $PROFILE_OUT"
echo "   或直接将文件上传至 https://profiler.firefox.com/"
