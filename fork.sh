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

# worktree 落仓库内持久路径:/tmp 清理风暴会连 worktree 带未提交工作整窝端掉
# (2026-09-30 两次事故),仓库内路径不吃清理;gitignore 防误提交
TMP="$DIR/.forks"
mkdir -p "$TMP"

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

link() {
  for i in "$@"; do
    if [ -e "$DIR/$i" ]; then
      ln -sfn "$(realpath "$DIR/$i")" "$TARGET_DIR/$i"
    fi
  done
}

link node_modules garnet .codegraph sh

# worktree 私有 target：注入未跟踪 cargo 配置，覆盖裸 cargo / test.sh / clippy.sh 全部调用形态，
# 根除多分支共写共享 target 的陈旧指纹假绿与 build-lock 互斥（复用旧树同样重写，自愈老沙箱）。
# 注意：workspace 内层 wedb/.cargo/config.toml 是 git 跟踪文件且写死共享 target——cargo 按
# 最近祖先解析配置，worktree 内层文件会覆盖根目录 overlay，故内层也须同拍重写（保留
# rustflags，仅换 target-dir；该文件在 worktree 属未跟踪脏文件，合并前 git checkout 还原）
mkdir -p "$TARGET_DIR/.cargo"
RS="$DIR/.rs-targets/$NAME"
cat > "$TARGET_DIR/.cargo/config.toml" <<EOF
[build]
target-dir = "$RS"
EOF
# 内层覆盖不走 sed（BSD/GNU 参数形态互不兼容，实测 fork-exit=1 整段写不进），
# 整文件重写：保留跟踪版的 rustflags（gxhash AES 链依赖），仅换 target-dir
INNER="$TARGET_DIR/wedb/.cargo/config.toml"
mkdir -p "$TARGET_DIR/wedb/.cargo"
printf '[build]\ntarget-dir = "%s"\nrustflags = ["-C", "target-feature=+aes"]\n' "$RS" > "$INNER"

echo "私有 target $RS"
echo "分支就绪 $TARGET_DIR"
