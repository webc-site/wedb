#!/usr/bin/env bash

DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -ex

if ! [ -d "garnet" ]; then
  git clone ssh://git@ssh.github.com:443/webc-fork/garnet.git
  git -C ../garnet remote add source ssh://git@ssh.github.com:443/microsoft/garnet.git
fi

if [ ! -d "sh" ]; then
  ln -s "$HOME/.local/share/cargo_sh" sh
fi

./node/init.sh
./cluster/init.sh
./embed/init.sh
