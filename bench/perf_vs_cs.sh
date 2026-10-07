#!/usr/bin/env bash
# Rust 与 C#（vendored Tsavorite）同机同档对表：跑测 -> 中位数 -> 门禁。
# 口径见 task/bench.md 第 2/4/5 节；门禁断言 = Rust 在任何指标上都不低于 C#。
#
# 用法：
#   bench/perf_vs_cs.sh --quick [--runs 3] [--engine hash] [--noise-ok]
#   bench/perf_vs_cs.sh --scale 0.2 ...        # 本机常规对比档
#   bench/perf_vs_cs.sh --scale 1.0 ...        # redb 标准档，单引擎可达 1 小时量级
#
# 档位必须显式给出：两侧任何一档不同都会让门禁直接 FAIL，与其默认值偷偷跑一小时
# 不如先让人确认。

set -euo pipefail

DIR=$(realpath "$0") && ROOT=${DIR%/bench/perf_vs_cs.sh}
OUTDIR=${ROOT}/.bench_run/perf_vs_cs
LOGDIR=${ROOT}/.bench_run
RUNS=3
ENGINE=hash
SCALE=""
GATE_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --quick) SCALE=0.02 ;;
    --scale) SCALE=${2:?--scale 需要取值}; shift ;;
    --runs) RUNS=${2:?--runs 需要取值}; shift ;;
    --engine) ENGINE=${2:?--engine 需要取值}; shift ;;
    --outdir) OUTDIR=${2:?--outdir 需要取值}; shift ;;
    --noise-ok) GATE_ARGS+=(--noise-ok) ;;
    *) echo "未知参数：$1" >&2; exit 2 ;;
  esac
  shift
done

if [ -z "$SCALE" ]; then
  echo "必须显式给档：--quick（0.02 冒烟）或 --scale F" >&2
  exit 2
fi

CS_PROJ=${ROOT}/bench/csharp/TsavoriteBench
if [ ! -f "${CS_PROJ}/Program.cs" ] && [ -z "$(find "$CS_PROJ" -name '*.csproj' -print -quit 2>/dev/null)" ]; then
  echo "C# 对基驱动尚未落地（bench/csharp/TsavoriteBench 不存在），先执行 task/bench.md 第 3 节 P2a。" >&2
  exit 3
fi

export DOTNET_ROOT=${DOTNET_ROOT:-/opt/homebrew/opt/dotnet/libexec}
export PATH=/opt/homebrew/bin:$PATH

mkdir -p "$LOGDIR" "$OUTDIR"
STAMP=$(date +%Y%m%d-%H%M%S)
RUN_LOG=${LOGDIR}/perf_vs_cs-${STAMP}.log
exec > >(tee -a "$RUN_LOG") 2>&1

COMMIT=$(git -C "$ROOT" rev-parse HEAD)
BRANCH=$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)

echo "对表开始：engine=${ENGINE} scale=${SCALE} runs=${RUNS} 档=${BRANCH}@${COMMIT:0:8}"
echo "数据与结果目录：${OUTDIR}    日志：${RUN_LOG}"
echo "提示：跑测期间不要在本机跑其它重负载，fsync 与读段计时会被污染。"

# 每次全新数据目录：禁复用旧段文件（会让写段虚高）
fresh() {
  local d=$1
  case "$d" in
    "$OUTDIR"/*) [ -n "${OUTDIR:?}" ] && rm -rf -- "$d" ;;
    *) echo "拒绝清理非本次输出目录：$d" >&2; exit 4 ;;
  esac
  mkdir -p "$d"
}

# 逐轮交错（台账 8i 方法论）：先 rust 后 cs 的块排程在负载波动期产生系统性
# 偏置（同晚实测 rust 块撞重载、cs 块撞轻窗，单项比值失真 2.4 倍）；交错后
# 两侧逐轮经历同一负载分布，「同机同负载」口径才成立。
RUST_JSON=()
CS_JSON=()
for i in $(seq 1 "$RUNS"); do
  fresh "${OUTDIR}/rust_${i}"
  echo "── Rust 第 ${i}/${RUNS} 次 ──"
  (
    cd "${ROOT}/bench"
    CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/tmp/wedb_bench_target} cargo bench -q -p wedb-bench-compare \
      --no-default-features --features "$ENGINE" --bench compare_benchmark -- \
      --only "$ENGINE" --scale "$SCALE" \
      --data-path "${OUTDIR}/rust_${i}" \
      --json "${OUTDIR}/rust_${i}.json" \
      --commit "$COMMIT" --branch "$BRANCH"
  )
  RUST_JSON+=("${OUTDIR}/rust_${i}.json")

  fresh "${OUTDIR}/cs_${i}"
  echo "── C# 第 ${i}/${RUNS} 次 ──"
  (
    cd "$CS_PROJ"
    dotnet build -c Release
    dotnet run -c Release --no-build -- \
      --engine "$ENGINE" \
      --scale "$SCALE" \
      --data-path "${OUTDIR}/cs_${i}" \
      --json "${OUTDIR}/cs_${i}.json" \
      --commit "$COMMIT" --branch "$BRANCH"
  )
  CS_JSON+=("${OUTDIR}/cs_${i}.json")
done

echo "── 门禁（Rust 不低于 C#，逐指标取 ${RUNS} 次中位数）──"
set +e
node "${ROOT}/bench/perf_compare.js" --rust "${RUST_JSON[@]}" --cs "${CS_JSON[@]}" ${GATE_ARGS[@]+"${GATE_ARGS[@]}"}
RC=$?
set -e

echo
echo "Rust 侧：${RUST_JSON[*]}"
echo "C#   侧：${CS_JSON[*]}"
echo "门禁退出码：${RC}（0=达标，1=有违规或噪声待复跑）"
exit "$RC"
