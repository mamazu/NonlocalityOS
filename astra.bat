@echo off
cls || exit /B 1
call .\scripts\install_sccache.bat || exit /B 1

setlocal
set RUSTC_WRAPPER=sccache
set repository=%~dp0
set command=%1
set RUST_BACKTRACE=1

pushd %repository% || exit /B 1
cargo run --bin astra -- %command% || exit /B 1
popd

echo Success!
endlocal
