//! Manual P2P fetch tool: lookup + fetch an artifact over the configured
//! P2P transport (URMA), timing each stage and printing the full error
//! chain on failure.
//!
//! Usage:
//!   p2p-fetch <key> [dest]            lookup + fetch (stages timed)
//!   p2p-fetch --serve <file> <key>    publish <file> under <key> and serve
//!                                     until killed (prints endpoint)
//!
//! Examples:
//!   p2p-fetch snapshot/v1/artifacts/<snapshot-id>/vm_state.bin /tmp/vm_state.bin
//!   p2p-fetch overlaybd-layer/v1/uuid/<uuid> /tmp/layer.bin
//!   p2p-fetch --serve /tmp/payload.bin p2p-test/v1/payload

use std::time::Instant;

use agentenv::identity::NodeIdentity;
use agentenv::p2p::{transport_from_config, P2pArtifactProvider, P2pPublishRequest, P2pTransport};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Debug-level logs for the p2p path so every URMA stage is visible.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,agentenv::p2p=debug".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let t0 = Instant::now();
    let config_manager = agentenv::cfg::ConfigManager::init_global()?;
    let config = config_manager.config();
    let identity = NodeIdentity::from_config(&config.node_identity);
    let transport = transport_from_config(config, &identity).await?;
    println!("[stage 0] transport init: {:?}", t0.elapsed());
    let local_endpoint = transport
        .local_endpoint()
        .map(|e| e.address)
        .unwrap_or_default();
    println!("[stage 0] local endpoint: {local_endpoint}");

    if args.len() >= 4 && args[1] == "--serve" {
        let file = &args[2];
        let key = args[3].clone();
        let request = P2pPublishRequest::file(key.clone(), file.clone());
        transport.publish(&request).await?;
        println!("[serve] published {file} as {key}");
        println!("[serve] endpoint for peers: {local_endpoint}");
        println!("[serve] serving until killed...");
        tokio::signal::ctrl_c().await?;
        return Ok(());
    }

    if args.len() < 2 {
        anyhow::bail!("usage: p2p-fetch <key> [dest] | p2p-fetch --serve <file> <key>");
    }
    let key = args[1].clone();
    let dest = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "/tmp/p2p-fetch.out".to_string());

    // Stage 1: lookup
    let t1 = Instant::now();
    let descriptor = match transport.lookup(&key).await {
        Ok(Some(d)) => {
            println!("[stage 1] lookup: OK {:?}", t1.elapsed());
            d
        }
        Ok(None) => {
            println!("[stage 1] lookup: NOT FOUND {:?}", t1.elapsed());
            return Ok(());
        }
        Err(e) => {
            println!("[stage 1] lookup: FAILED {:?}\nerror: {:#}", t1.elapsed(), e);
            return Ok(());
        }
    };
    println!(
        "[stage 1] backend_locator: {:?}",
        descriptor.backend_locator
    );
    println!(
        "[stage 1] providers: {:?}",
        descriptor
            .providers
            .iter()
            .map(|p| match p {
                P2pArtifactProvider::Peer(peer) => {
                    format!("peer {} {}", peer.node_id, peer.endpoint.address)
                }
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
    );

    // Stage 2: fetch to file
    let t2 = Instant::now();
    match transport
        .fetch(&descriptor, std::path::Path::new(&dest))
        .await
    {
        Ok(size) => println!(
            "[stage 2] fetch: OK size={size} bytes ({:?}, {:.2} MiB/s) -> {dest}",
            t2.elapsed(),
            size as f64 / 1024.0 / 1024.0 / t2.elapsed().as_secs_f64().max(1e-9)
        ),
        Err(e) => println!(
            "[stage 2] fetch: FAILED after {:?}\nerror chain: {:#}",
            t2.elapsed(),
            e
        ),
    }

    // Stage 3 (optional): fetch_bytes for small artifacts
    if std::env::var("P2P_FETCH_BYTES").is_ok() {
        let t3 = Instant::now();
        match transport.fetch_bytes(&descriptor).await {
            Ok(bytes) => println!(
                "[stage 3] fetch_bytes: OK len={} ({:?})",
                bytes.len(),
                t3.elapsed()
            ),
            Err(e) => println!(
                "[stage 3] fetch_bytes: FAILED after {:?}\nerror chain: {:#}",
                t3.elapsed(),
                e
            ),
        }
    }

    // Stage 4 (optional): byte range fetch
    if std::env::var("P2P_FETCH_RANGE").is_ok() {
        let t4 = Instant::now();
        match transport.fetch_byte_range(&descriptor, 0, 4096).await {
            Ok(mut stream) => {
                use futures::StreamExt;
                let mut got = 0usize;
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(bytes) => got += bytes.len(),
                        Err(e) => {
                            println!("[stage 4] range stream error: {e:#}");
                            break;
                        }
                    }
                }
                println!(
                    "[stage 4] fetch_byte_range: OK got={got} bytes ({:?})",
                    t4.elapsed()
                );
            }
            Err(e) => println!(
                "[stage 4] fetch_byte_range: FAILED after {:?}\nerror chain: {:#}",
                t4.elapsed(),
                e
            ),
        }
    }

    transport.shutdown().await.ok();
    println!("done");
    Ok(())
}
