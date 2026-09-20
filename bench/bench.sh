#!/usr/bin/env bash

set -e
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR"

# 统一加载环境变量
[ -f "sh/env.sh" ] && . sh/env.sh

# 调高文件描述符上限，防止大规模测试触发 EMFILE
ulimit -n 10240 2>/dev/null || ulimit -n 4096 2>/dev/null || true

# 独立 target 目录，防止与后台 cargo 测试/审查任务争抢文件锁
export CARGO_TARGET_DIR=/tmp/wedb_bench_target

set -x

# 编译并运行性能评测，输出原始 JSON 数据
cargo run --release --bin bench -- "$@"

# 执行 JS 脚本，生成高质量对比 SVG、上传 CDN 并渲染多语言 Markdown 报告
./js/benchGen.js "$@"
