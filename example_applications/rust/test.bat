@echo off
pushd hello_rust || exit /B 1
call .\test.bat || exit /B 1
popd

pushd idle_service || exit /B 1
call .\test.bat || exit /B 1
