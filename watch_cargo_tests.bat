@echo off
cargo install --locked --version 3.1.1 bacon || exit /B 1
bacon nextest || exit /B 1
