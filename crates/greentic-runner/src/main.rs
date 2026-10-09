//! The stock `greentic-runner` binary. Everything it does lives in the library
//! (`greentic_runner::cli_main`) so that a binary which registers agent-runtime
//! extensions first can run the identical CLI.

#[greentic_types::telemetry::main(service_name = "greentic-runner")]
async fn main() {
    greentic_runner::cli_main().await;
}
