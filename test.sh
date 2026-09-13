#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

for dir in wedb regress; do
  if [ -d "$dir" ] && [ -f "$dir/test.sh" ]; then
    ./$dir/test.sh
  fi
done
