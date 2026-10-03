#!/usr/bin/env bash

set -e
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR" || exit 1

USER_RUST_LOG="$RUST_LOG"
. sh/env.sh
export RUST_LOG="${USER_RUST_LOG:-warn}"

JSON_PATH="$WEDB_TEST_JSON"
FILTERED_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --json)
      if [ $# -lt 2 ] || [ -z "$2" ]; then
        echo "usage: ./test.sh --json <path>（--json 须带非空路径）" >&2
        exit 2
      fi
      JSON_PATH="$2"
      shift 2
      ;;
    --json=*)
      JSON_PATH="${1#*=}"
      if [ -z "$JSON_PATH" ]; then
        echo "usage: ./test.sh --json=<path>（路径不得为空）" >&2
        exit 2
      fi
      shift
      ;;
    *)
      FILTERED_ARGS+=("$1")
      shift
      ;;
  esac
done

# 跑批降噪（对齐 garnet 跑批口径）：红点与慢测即时输出，进度条与绿流不入终端
# slow 状态级增量含 retry/fail（fail 会挡掉 SLOW 行与终局慢测摘要）；慢测阈值 5s 见 .config/nextest.toml
ARGS=(--all-features --failure-output immediate --status-level slow --final-status-level slow --show-progress=none)

if [ -n "$JSON_PATH" ]; then
  case "$JSON_PATH" in
    /*) ;;
    *) JSON_PATH="$PWD/$JSON_PATH" ;;
  esac
  mkdir -p "$(dirname "$JSON_PATH")"
  export NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1
  ARGS+=(--message-format libtest-json)
  echo "[test.sh] exporting libtest-json to $JSON_PATH" >&2
  set +e
  cargo nextest run "${ARGS[@]}" "${FILTERED_ARGS[@]}" > "$JSON_PATH"
  EXIT_CODE=$?
  set -e
  echo "[test.sh] test finished with exit code $EXIT_CODE, json saved to $JSON_PATH" >&2
  exit $EXIT_CODE
fi

exec cargo nextest run "${ARGS[@]}" "${FILTERED_ARGS[@]}"
