#!/usr/bin/env bash
# 本机 cargo 产物清理：只动「多少天没被碰过」的构建垃圾，默认干跑列清单。
#
# CI 侧的过期缓存由 .github/workflows/cache-gc.yml 按年龄回收；这个脚本管这台机器上
# 攒下来的三类：各 workspace 的 target/、/tmp 里一次性 bench 的 target、bench/.bench_run
# 的跑测日志。target/ 是纯产物，删了只赔重编时间、不赔数据，按年龄下手是安全的；
# ~/.cargo 注册表只报体积不删——断网时重下的代价比一次冷编大得多。
#
# 用法：
#   ./cargo_gc.sh              # 干跑：列出可回收项与总体积
#   ./cargo_gc.sh --apply      # 真删（本机有 cargo/rustc 在跑会自动降级为干跑）
#   ./cargo_gc.sh --days 30    # 改新鲜度闸口（默认 14 天）
#
# 退出码：0 = 正常报告完；1 = 参数不对。

set -e

DAYS=14
APPLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --apply) APPLY=1 ;;
    --days)
      shift
      if [ $# -eq 0 ]; then
        echo "❌ --days 后面要跟一个整数" >&2
        exit 1
      fi
      case "$1" in
        '' | *[!0-9]*)
          echo "❌ --days 只收整数天数，收到的是：$1" >&2
          exit 1
          ;;
        *) DAYS=$1 ;;
      esac
      ;;
    *)
      echo "用法: ./cargo_gc.sh [--apply] [--days N]" >&2
      exit 1
      ;;
  esac
  shift
done

root=$(realpath "$0") && root=${root%/*}
cd "$root"

echo "闸口：${DAYS} 天内没被写过就算垃圾（--days 可调），模式：$(
  [ "$APPLY" = "1" ] && echo "实删 --apply" || echo "干跑")"

# 正在编译/跑测时不删：target/ 随时会被写回，删一半比不删更糟（增量状态错位会编出假红）。
# 只认进程名和 argv 里的 target 路径，不用宽 -f 关键词匹配：
# 维护脚本与子会话的命令行里到处都是 "cargo build" 字样，那样会永远判定为忙
busy=$({
  pgrep -l -x 'cargo|cargo-nextest|rustc|sccache' 2>/dev/null
  pgrep -f 'target/(debug|release)' 2>/dev/null | head -5
} | sort -u | head -5)
if [ -n "$busy" ]; then
  echo "⚠ 检测到本机还在编译或跑测，本轮强制降为干跑（要实删请等它们收工）："
  echo "$busy" | sed 's/^/    /'
  APPLY=0
fi

total_kib=0
hits=0

# gc_one <路径> <说明>：目录里只要有一个文件在闸口内被写过就整体留着
# 变量一律带花括号：bash 3.2 在 zh_CN.UTF-8 下会把紧跟在 $name 后面的全角字节吃掉
gc_one() {
  path=$1
  why=$2
  [ -e "$path" ] || return 0
  # -mindepth 1：find 默认把起始目录自己算进来，而目录 mtime 只反映「今天增删过条目」，
  # 刚 mkdir 的空 target 会被它骗成新鲜；只看里面的文件
  if [ -n "$(find "$path" -mindepth 1 -mtime "-$DAYS" -print | head -1)" ]; then
    echo "  留着    ${path}（${why}，闸口内动过）"
    return 0
  fi
  kib=$(du -sk "$path" 2>/dev/null | cut -f1)
  [ -n "$kib" ] || kib=0
  echo "  可回收  ${path}（${why}，$((kib / 1024)) MiB）"
  total_kib=$((total_kib + kib))
  hits=$((hits + 1))
  if [ "$APPLY" = "1" ]; then
    rm -rf "$path" && echo "  已删    ${path}"
  fi
}

echo "── workspace target/"
# 只认 Cargo workspace 边上的 target/（父目录有 Cargo.toml）；.forks/ 是在途席位的
# worktree，它们的编译状态不归本机管，整棵跳过
while read -r dir; do
  [ -n "$dir" ] || continue
  parent=$(dirname "$dir")
  [ -f "${parent}/Cargo.toml" ] || continue
  gc_one "$dir" "workspace $(basename "$parent")"
done < <(find . -maxdepth 3 -type d -name target \
  -not -path "./.git/*" -not -path "./.forks/*" -not -path "*/node_modules/*" | sort)

echo "── /tmp 的一次性 bench target"
for dir in /tmp/wedb_tgt_*; do
  [ -d "$dir" ] || continue
  gc_one "$dir" "临时 target"
done

echo "── bench/.bench_run 跑测日志"
if [ -d "bench/.bench_run" ]; then
  while read -r f; do
    [ -n "$f" ] || continue
    kib=$(du -sk "$f" 2>/dev/null | cut -f1)
    [ -n "$kib" ] || kib=0
    echo "  可回收  ${f}（$((kib / 1024)) MiB，闸口外）"
    total_kib=$((total_kib + kib))
    hits=$((hits + 1))
    if [ "$APPLY" = "1" ]; then
      rm -f "$f" && echo "  已删    ${f}"
    fi
  done < <(find bench/.bench_run -type f ! -mtime "-$DAYS" | sort)
else
  echo "  没有 bench/.bench_run 目录"
fi

echo "── ~/.cargo 注册表（只报体积，不代删）"
if [ -d "$HOME/.cargo/registry" ]; then
  echo "  $(du -sk "$HOME/.cargo/registry" | cut -f1 | awk '{ printf "%.0f MiB", $1 / 1024 }')：
      要清就自己确认网络可重下，再动手 cargo clean / 删 registry/{cache,src}"
fi

echo ""
if [ "$hits" -eq 0 ]; then
  echo "没有闸口外的产物，本机已经很干净。"
elif [ "$APPLY" = "1" ]; then
  echo "已回收 ${hits} 项，合计 $((total_kib / 1024)) MiB。"
else
  echo "可回收 ${hits} 项，合计 $((total_kib / 1024)) MiB。确认后加 --apply 实删。"
fi
