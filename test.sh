#!/usr/bin/env sh
set -e

cargo install --locked cargo-nextest
cargo nextest run
