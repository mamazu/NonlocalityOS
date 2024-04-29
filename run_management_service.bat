call .\build.bat || exit /B 1

cargo run --verbose --bin management_service --release "%CD%" || exit /B 1
