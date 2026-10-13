#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

command -v redis-server >/dev/null || { echo "未安装 redis-server" >&2; exit 1; }
# 实测 redis-server 运行时 setproctitle 自重写 argv 与 comm(如 "redis-server *:7791",裸名开头,无路径),
# 路径锚与 -x 精确匹配均失手;按重写后命令行起始裸名锚定,tail -f .../redis-server.log 等子串进程起始为 tail,不误杀
pkill -9 -f '^redis-server( |$)'
