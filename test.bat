@echo off
cls || exit /B 1
setlocal

set repository=%~dp0

pushd astra || exit /B 1
cargo run --release %repository% || exit /B 1
popd

echo Success!
endlocal
