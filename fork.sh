#!/usr/bin/env bash

set -e
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR"

NAME="$1"

if [ -z "$NAME" ]; then
  echo "用法: $0 <分支名>"
  echo "例如: $0 fix-something"
  exit 1
fi

TMP=/tmp/fork
mkdir -p $TMP

TARGET_DIR="$TMP/$NAME"

if [ -d "$TARGET_DIR" ]; then
  echo "目标目录已存在 $TARGET_DIR"
else
  if git rev-parse --verify "$NAME" >/dev/null 2>&1; then
    git worktree add "$TARGET_DIR" "$NAME"
  else
    git worktree add -b "$NAME" "$TARGET_DIR"
  fi
fi

if [ -e "$DIR/node_modules" ]; then
  ln -sf "$DIR/node_modules" "$TARGET_DIR/node_modules"
fi

if [ -e "$DIR/garnet" ]; then
  GARNET_REAL=$(realpath "$DIR/garnet")
  ln -sf "$GARNET_REAL" "$TARGET_DIR/garnet"
fi

echo "分支就绪 $TARGET_DIR"
