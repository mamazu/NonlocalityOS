@echo off
pushd rust || exit /B 1
call .\test.bat || exit /B 1
popd
