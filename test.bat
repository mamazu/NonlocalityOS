@echo off
pushd engine || exit /B 1
cargo test || exit /B 1
popd

pushd example_applications || exit /B 1
.\run_all.bat || exit /B 1

echo Success!
