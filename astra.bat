@echo off
set command=%1

cls || exit /B 1
setlocal

set repository=%~dp0

cargo run --bin astra --release %repository% %command% || exit /B 1

echo Success!
endlocal
