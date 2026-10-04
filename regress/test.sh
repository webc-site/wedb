#!/usr/bin/env bash

# 口径局限声明:本门禁以 debug profile 执行(四档绝对阈值 10K/50K/20K/50K op/s 均在
# debug 构建上校准)。debug 无优化,吞吐特征与 release 全链路(opt-level 3 + fat LTO)
# 脱钩:release 专属劣化(如 LTO 内联退化)本门禁不可见,debug 独有劣化可能假红。
# 定位为 debug 烟测门禁;release 口径劣化靠 regress-run 基线对比(./regress.js)感知。

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
. sh/env.sh
set -x
exec cargo nextest run --all-features "$@"
