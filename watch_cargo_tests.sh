#!/usr/bin/env sh
cargo install --locked --version 3.1.1 bacon || exit 1
bacon nextest || exit 1
