#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

for dir in node cluster embed; do
  ./$dir/test.sh
done
