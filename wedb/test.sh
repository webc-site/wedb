#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
. sh/env.sh
exec cargo nextest run --all-features --status-level fail --final-status-level fail "$@"
