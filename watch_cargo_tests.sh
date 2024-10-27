#!/usr/bin/env sh
./install_bacon.sh || exit 1
export RUST_LOG=info
bacon nextest || exit 1
