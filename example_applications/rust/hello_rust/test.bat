
cargo build --target wasm32-wasi || exit /B 1
cargo run || exit /B 1
