#!/usr/bin/env bash
./scripts/install_sccache.sh || exit 1
export RUSTC_WRAPPER=sccache

repository=$(dirname "$0")
command=$1
export RUST_BACKTRACE=1

pushd $repository || exit 1
cargo run --bin astra --release -- $command || exit 1
popd

echo Success!
