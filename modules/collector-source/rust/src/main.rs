#![allow(unused)]

use std::sync::Arc;

use metrics::Scraper;
use targets::{
    HttpOptions,
    TlsOptions,
    UrlScrapeTarget,
};
use tokio::select;
use tokio::sync::Notify;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

mod core;
mod math;
mod metrics;
mod targets;
mod util;

const TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlA2bGluVnZDTWd2RlRxUXJIajcxbHV1Ry1fWmd2dks2OTZFUE1pMDVXR2sifQ.eyJhdWQiOlsiaHR0cHM6Ly9jb250YWluZXIuZ29vZ2xlYXBpcy5jb20vdjEvcHJvamVjdHMvZ3Vlc3Rib29rLTIyNzUwMi9sb2NhdGlvbnMvdXMtY2VudHJhbDEtYy9jbHVzdGVycy9kZXYtMSJdLCJleHAiOjE3Mzk2NDI4ODksImlhdCI6MTcwODEwNjg4OSwiaXNzIjoiaHR0cHM6Ly9jb250YWluZXIuZ29vZ2xlYXBpcy5jb20vdjEvcHJvamVjdHMvZ3Vlc3Rib29rLTIyNzUwMi9sb2NhdGlvbnMvdXMtY2VudHJhbDEtYy9jbHVzdGVycy9kZXYtMSIsImt1YmVybmV0ZXMuaW8iOnsibmFtZXNwYWNlIjoiYm9sdCIsInBvZCI6eyJuYW1lIjoia3ViZWNvc3QtY2xvdWQtcHJvbWV0aGV1cy1zZXJ2ZXItNThmNThjYmM4NC16YnRncCIsInVpZCI6IjVlZDM4ZWRjLTA5NWEtNDhmYy04NWMyLWVhZjk0MjczZDdmNyJ9LCJzZXJ2aWNlYWNjb3VudCI6eyJuYW1lIjoia3ViZWNvc3QtY2xvdWQtcHJvbWV0aGV1cy1zZXJ2ZXIiLCJ1aWQiOiJmZDYzNWYxYS0yODFmLTQ1MmItOTE2Yy1iMTJjZGZlZDVjZjEifSwid2FybmFmdGVyIjoxNzA4MTEwNDk2fSwibmJmIjoxNzA4MTA2ODg5LCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6Ym9sdDprdWJlY29zdC1jbG91ZC1wcm9tZXRoZXVzLXNlcnZlciJ9.NtMCKkSABXhdMlOUNzO7Rsu-znY9BFuXL2dRpnqgk9bQ6KR_ujedn9iv6nUV6GV015nrMoGCJYZFYrhZOjYCNHPnu40E68VR7FAsXRRcIvguKf-Cyjy5GWIQVV1mdv8-G-PbrEUwpyM8-KuaTqFlVAhFqF6OHdlvXxO1ngys3TerO69ukqNldHHcgHTnNNIb5xbuWbwTQaAZXkYwPkx6CTCwIId0cMrUxD_NezagEred2MoOPoQWg8M_2bLOl-hWvlzl0JT1FWW3oQnUKpBOcWGWy5PBufGAiDBVfZqxEDyAvx_fZctx5LdEj5MEwDRILyGFiorpJycmHb38l9xNjA";
const URL: &str = "https://35.238.20.104/api/v1/nodes/gke-dev-1-kc-cloud-pool-80a91fb4-s5hh/proxy/metrics/cadvisor";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(true)
                .without_time()
                .with_filter(LevelFilter::INFO), // EnvFilter::from_env("LOG_LEVEL")),
        )
        .init();

    // ScrapeTargets -> Collectors -> CollectorSet
    // -> On "Scrape Interval" -> Refresh CollectionSet -> Dispatches New, Updated,
    // Removed

    // how to create collectors
    let target = UrlScrapeTarget::new(
        URL,
        Some(HttpOptions {
            auth_token: Some(TOKEN.to_string()),
            tls: Some(TlsOptions {
                insecure_skip_verify: true,
            }),
        }),
    )?;

    let set = hashset!(
        String::from("container_cpu_usage_seconds_total"),
        String::from("container_cpu_system_seconds_total"),
        String::from("container_fs_io_current"),
    );

    let scraper = Scraper::new(target);
    let notify = Arc::new(Notify::new());
    let inner_notify = Arc::clone(&notify);

    let handle = tokio::spawn(async move {
        loop {
            if let Ok(metrics) = scraper.scrape(&set).await {
                info!("{:?}", metrics);
            }

            select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(10u64)) => {},
                _ = inner_notify.notified() => {
                    info!("Stopped!");
                    return;
                }
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(30u64)).await;
    notify.notify_one();

    handle.await?;

    info!("Exiting...");

    // Aggregator -> Listens for Events -> Updates stored metric descriptors
    //  -> Aggregate Router -> Routes Events to Metric/Aggregate Buckets

    Ok(())
}
