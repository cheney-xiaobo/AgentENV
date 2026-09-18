use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::cfg::P2pConfig;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    Disabled,
    Iroh,
    #[serde(alias = "urma")]
    Urma,
    /// URMA acceleration with iroh fallback (both legs are published).
    #[serde(alias = "composite")]
    Composite,
}

impl P2pTransportKind {
    pub(crate) fn backend_id(self) -> Option<&'static str> {
        match self {
            Self::Disabled => None,
            Self::Iroh => Some(super::iroh::IROH_BACKEND_ID),
            Self::Urma => Some(super::urma::URMA_BACKEND_ID),
            // Composite advertises iroh endpoints (universally reachable);
            // URMA reachability is discovered via the catalog/hints.
            Self::Composite => Some(super::iroh::IROH_BACKEND_ID),
        }
    }
}

/// Runtime-resolved URMA backend settings.
#[derive(Debug, Clone)]
pub(crate) struct UrmaSettings {
    /// UB device name to open; empty string picks the first available device.
    pub device: String,
    /// Number of send jetty slots requested when creating the URMA context.
    pub jfs_count: u32,
    /// Number of receive jetty slots requested when creating the URMA context.
    pub jfr_count: u32,
    /// Upper bound on simultaneously registered local segments (published artifacts).
    pub max_segments: usize,
    /// Bounce-window size used by the one-sided READ data path.
    pub read_chunk_bytes: u64,
    /// Timeout applied to dynamic jetty bind (connection establishment) attempts.
    pub connect_timeout: Duration,
    /// How long an idle peer connection is kept before it is torn down.
    pub idle_connection_ttl: Duration,
    /// Upper bound on imported remote segments (LRU cap).
    pub max_imported_segments: usize,
}

impl Default for UrmaSettings {
    fn default() -> Self {
        Self {
            device: String::new(),
            jfs_count: 8,
            jfr_count: 4,
            max_segments: 256,
            max_imported_segments: 1024,
            read_chunk_bytes: 4 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            idle_connection_ttl: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedP2pConfig {
    pub transport: P2pTransportKind,
    pub store_dir: PathBuf,
    pub listen_addr: Option<String>,
    pub lookup_timeout: Duration,
    pub fetch_timeout: Duration,
    pub peer_discovery_refresh_interval: Duration,
    pub urma: UrmaSettings,
}

impl ResolvedP2pConfig {
    pub(crate) fn from_config(p2p: &P2pConfig) -> Self {
        let transport = if p2p.enabled {
            p2p.transport
        } else {
            P2pTransportKind::Disabled
        };

        Self {
            transport,
            store_dir: p2p.store_dir.clone(),
            listen_addr: Some(str::trim(p2p.listen_addr.as_str()))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            lookup_timeout: Duration::from_millis(p2p.lookup_timeout_ms),
            fetch_timeout: Duration::from_millis(p2p.fetch_timeout_ms),
            peer_discovery_refresh_interval: Duration::from_secs(
                p2p.peer_discovery_refresh_interval_secs,
            )
            .max(Duration::from_secs(1)),
            urma: UrmaSettings {
                device: p2p.urma.device.trim().to_string(),
                jfs_count: p2p.urma.jfs_count,
                jfr_count: p2p.urma.jfr_count,
                max_segments: (p2p.urma.max_segments as usize).max(1),
                max_imported_segments: (p2p.urma.max_imported_segments as usize).max(1),
                read_chunk_bytes: p2p.urma.read_chunk_bytes.max(4096),
                connect_timeout: Duration::from_millis(p2p.urma.connect_timeout_ms),
                idle_connection_ttl: Duration::from_secs(p2p.urma.idle_connection_ttl_secs),
            },
        }
    }
}
