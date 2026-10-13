#!/usr/bin/env bash

set -e
DIR=$(realpath "$0") && DIR=${DIR%/*}
cd "$DIR/wedb" || exit 1
exec ./test.sh "$@"
