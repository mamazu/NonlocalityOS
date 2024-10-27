@echo off
call install_sccache.bat || exit /B 1
set RUSTC_WRAPPER=sccache
cargo install --version 3.1.1 --locked bacon || exit /B 1
