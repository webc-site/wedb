#!/usr/bin/env bash
# 主控专用：绕过 pre-commit 钩子的全量 git add，用临时索引只提交点名文件。
# 用法: bash js/safe_commit.sh "<msg>" <file> [<file> ...]
set -euo pipefail
REPO=/Users/z/git/db/wedb
MSG="$1"; shift
cd "$REPO"
for attempt in 1 2 3 4 5; do
  PARENT=$(git rev-parse HEAD)
  IDX=$(mktemp /tmp/safeidx.XXXXXX); rm -f "$IDX"
  GIT_INDEX_FILE="$IDX" git read-tree "$PARENT"
  GIT_INDEX_FILE="$IDX" git add -- "$@"
  TREE=$(GIT_INDEX_FILE="$IDX" git write-tree)
  NOW=$(git rev-parse HEAD)
  if [ "$NOW" != "$PARENT" ]; then
    rm -f "$IDX"; sleep 2; continue
  fi
  C=$(git commit-tree "$TREE" -p "$PARENT" -m "$MSG")
  if git update-ref refs/heads/dev "$C" "$PARENT" 2>/dev/null; then
    echo "COMMITTED $C (parent $PARENT)"
    git show --name-status --format="" "$C"
    rm -f "$IDX"; exit 0
  fi
  rm -f "$IDX"; sleep 2
done
echo "SAFE_COMMIT_FAILED: dev 反复被推进，放弃" >&2
exit 1
