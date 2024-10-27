@echo off
call .\install_bacon.bat || exit /B 1
set RUST_LOG=info
bacon nextest || exit /B 1
