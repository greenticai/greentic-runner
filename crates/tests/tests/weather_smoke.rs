use std::env;
use std::net::TcpListener;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use runner_core::packs::PackDigest;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::sleep;

#[tokio::test]
#[ignore = "requires a live weather pack + bindings + webhook-capable flow"]
async fn weather_smoke() -> Result<()> {
    let pack_path = match env::var("WEATHER_PACK_PATH") {
        Ok(path) => path,
        Err(_) => bail!("set WEATHER_PACK_PATH to a .gtpack for the live smoke test"),
    };
    let flow_id = env::var("WEATHER_FLOW_ID").unwrap_or_else(|_| "weather_flow".to_string());
    let bindings_path = env::var("WEATHER_BINDINGS_PATH")
        .unwrap_or_else(|_| "examples/bindings/default.bindings.yaml".to_string());
    let payload: Value = env::var("WEATHER_WEBHOOK_PAYLOAD")
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_else(|| json!({ "ping": true }));

    let temp = TempDir::new()?;
    let index_path = temp.path().join("index.json");
    let cache_dir = temp.path().join("cache");
    tokio::fs::create_dir_all(&cache_dir).await?;

    let digest = {
        let bytes = tokio::fs::read(&pack_path).await?;
        PackDigest::sha256_from_bytes(&bytes).raw_string()
    };
    let index = json!({
        "weather": {
            "main_pack": {
                "name": "weather.pack",
                "version": "0.0.0",
                "locator": pack_path,
                "digest": digest
            },
            "overlays": []
        }
    });
    tokio::fs::write(&index_path, serde_json::to_vec_pretty(&index)?).await?;

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let mut cmd = Command::new("cargo");
    cmd.args([
        "run",
        "-p",
        "greentic-runner",
        "--",
        "--bindings",
        &bindings_path,
        "--port",
        &port.to_string(),
    ])
    .env("PACK_SOURCE", "fs")
    .env("PACK_INDEX_URL", index_path.display().to_string())
    .env("PACK_CACHE_DIR", &cache_dir)
    .env(
        "SECRETS_BACKEND",
        env::var("SECRETS_BACKEND").unwrap_or_else(|_| "env".into()),
    );
    if let Ok(token) = env::var("TELEGRAM_BOT_TOKEN") {
        cmd.env("TELEGRAM_BOT_TOKEN", token);
    }
    let mut child = cmd.spawn().context("spawn host binary")?;

    let client = reqwest::Client::new();
    let health_url = format!("http://127.0.0.1:{port}/healthz");
    let mut ready = false;
    for _ in 0..40 {
        if client.get(&health_url).send().await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        bail!("host never became ready");
    }

    let webhook_url = format!("http://127.0.0.1:{port}/webhook/{flow_id}");
    let response = client
        .post(webhook_url)
        .json(&payload)
        .send()
        .await
        .context("dispatch webhook")?;
    let status = response.status();
    let body: Value = response.json().await.context("decode webhook response")?;
    if !status.is_success() {
        let _ = child.kill();
        let _ = child.wait();
        bail!("webhook failed with {status}: {body}");
    }

    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}
