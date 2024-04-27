@echo off
call .\test.bat || exit /B 1
pushd management_service || exit /B 1
cargo run --release "%CD%\.." || exit /B 1
