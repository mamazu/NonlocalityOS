#!/usr/bin/env sh
repository=$(dirname "$0")
command=$1
RUST_BACKTRACE=full

cargo run --bin astra --release -- $repository $command || exit 1

echo Success!
