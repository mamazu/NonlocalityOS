@echo off
setlocal
cargo install grcov || exit /B 1
grcov . --binary-path ./management_service/target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o ./coverage/report.html || exit /B 1
endlocal
