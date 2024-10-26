#!/usr/bin/env sh
./install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache
cargo install --version 3.1.1 bacon || exit 1
export RUST_LOG=info
bacon nextest || exit 1
