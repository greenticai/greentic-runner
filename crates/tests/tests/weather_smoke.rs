use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::time::sleep;

/// Kills the spawned runner on every exit path (the child is the runner
/// binary itself, not `cargo run`, so `kill` reaches the right process).
struct RunnerProcess(Child);

impl Drop for RunnerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn runner_binary() -> Result<PathBuf> {
    if let Ok(path) = env::var("WEATHER_RUNNER_BIN") {
        return Ok(PathBuf::from(path));
    }
    let target = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..")
                .join("target")
        });
    let candidate = target.join("debug").join("greentic-runner");
    if candidate.is_file() {
        return Ok(candidate);
    }
    bail!(
        "runner binary not found at {}; run `cargo build -p greentic-runner` or set WEATHER_RUNNER_BIN",
        candidate.display()
    )
}

/// Live smoke test against a real weather pack.
///
/// Inputs: `WEATHER_BINDINGS_PATH` (a `*.gtbind` file whose `pack_locator`
/// points at the weather pack; the runner reads packs from bindings, not from
/// `PACK_*` index variables), optionally `WEATHER_RUNNER_BIN`,
/// `WEATHER_WEBHOOK_PATH` (an ingress path to POST to, e.g. one exposed by the
/// pack's provider) and `WEATHER_WEBHOOK_PAYLOAD`.
#[tokio::test]
#[ignore = "live test: needs WEATHER_BINDINGS_PATH (*.gtbind) pointing at a weather pack and a built greentic-runner binary"]
async fn weather_smoke() -> Result<()> {
    let bindings_path = match env::var("WEATHER_BINDINGS_PATH") {
        Ok(path) => path,
        Err(_) => bail!("set WEATHER_BINDINGS_PATH to a *.gtbind for the live smoke test"),
    };
    let payload: Value = env::var("WEATHER_WEBHOOK_PAYLOAD")
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_else(|| json!({ "ping": true }));

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let mut cmd = Command::new(runner_binary()?);
    cmd.args(["--bindings", &bindings_path, "--port", &port.to_string()])
        .stdout(Stdio::null())
        .env(
            "SECRETS_BACKEND",
            env::var("SECRETS_BACKEND").unwrap_or_else(|_| "env".into()),
        );
    if let Ok(token) = env::var("TELEGRAM_BOT_TOKEN") {
        cmd.env("TELEGRAM_BOT_TOKEN", token);
    }
    let _runner = RunnerProcess(cmd.spawn().context("spawn host binary")?);

    let client = reqwest::Client::new();
    let health_url = format!("http://127.0.0.1:{port}/healthz");
    let mut ready = false;
    for _ in 0..40 {
        if let Ok(response) = client.get(&health_url).send().await
            && response.status().is_success()
        {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
    if !ready {
        bail!("host never became ready");
    }

    if let Ok(path) = env::var("WEATHER_WEBHOOK_PATH") {
        let url = format!("http://127.0.0.1:{port}/{}", path.trim_start_matches('/'));
        let response = client
            .post(url)
            .json(&payload)
            .send()
            .await
            .context("dispatch webhook")?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("webhook failed with {status}: {body}");
        }
    }
    Ok(())
}
