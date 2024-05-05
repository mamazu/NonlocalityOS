@echo off
cls || exit /B 1
setlocal
set command=%1
set RUST_BACKTRACE=1
set repository=%~dp0

cargo run --bin astra --release %repository% %command% || exit /B 1

echo Success!
endlocal
