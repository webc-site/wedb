#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
. sh/env.sh
# 构建门禁双接线（工单 gate-featurecheck-wiring-lost）：nextest 前逐 crate feature 声明检查 + lint 压制扫描。
# 本脚本已 cd 自身目录（wedb/），仓库根门禁脚本即 ../ 相对位；feature.check.sh 自含 cd $DIR/wedb，勿重复传参。
# 失败码经 set -e 传播，禁改本两行（task/issue/process-commit-discipline.md 方案 3）。
../feature.check.sh
../allow.check.sh
exec cargo nextest run --failure-output immediate --all-features --status-level fail --final-status-level fail "$@"
