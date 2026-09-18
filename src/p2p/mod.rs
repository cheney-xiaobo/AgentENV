mod config;
mod composite;
mod discovery;
mod error;
mod iroh;
mod obs;
#[cfg(test)]
pub(crate) mod mock;
mod transport;
mod types;
mod urma;

use std::sync::Arc;

use tracing::info;

use crate::identity::NodeIdentity;

pub use config::P2pTransportKind;
pub use discovery::{
    NoopP2pPeerDiscovery, P2pPeerDiscovery, SchedulerPeerDiscovery, StaticP2pPeerDiscovery,
};
pub use error::{Error as P2pError, Result as P2pResult};
pub use transport::{DisabledP2pTransport, P2pByteStream, P2pTransport};
pub use types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pEndpoint, P2pPeer, P2pPublishMode, P2pPublishRequest, P2pPublishSource,
};

/// Construct an artifact transport from the app config, returning an error if the configured transport is invalid or fails to initialize.
pub async fn transport_from_config(
    config: &crate::cfg::AppConfig,
    node_identity: &NodeIdentity,
) -> anyhow::Result<Arc<dyn P2pTransport>> {
    let p2p = config::ResolvedP2pConfig::from_config(&config.p2p);
    match p2p.transport {
        P2pTransportKind::Disabled => Ok(Arc::new(DisabledP2pTransport)),
        P2pTransportKind::Iroh => {
            let peer_discovery = peer_discovery_from_config(config, &p2p, node_identity);
            Ok(Arc::new(
                iroh::IrohBlobsP2pTransport::new(p2p, node_identity.id.clone(), peer_discovery)
                    .await?,
            ))
        }
        P2pTransportKind::Urma => {
            #[cfg(feature = "p2p-urma")]
            {
                let peer_discovery = peer_discovery_from_config(config, &p2p, node_identity);
                Ok(Arc::new(urma::UrmaP2pTransport::new(
                    &p2p,
                    node_identity.id.clone(),
                    peer_discovery,
                )?))
            }
            #[cfg(not(feature = "p2p-urma"))]
            {
                Err(anyhow::anyhow!(
                    "p2p transport 'urma' requires building with the p2p-urma feature (UMDK liburma SDK)"
                ))
            }
        }
        P2pTransportKind::Composite => {
            #[cfg(not(feature = "p2p-urma"))]
            {
                Err(anyhow::anyhow!(
                    "p2p transport 'composite' requires building with the p2p-urma feature (UMDK liburma SDK)"
                ))
            }
            #[cfg(feature = "p2p-urma")]
            {
                // iroh fallback leg + URMA acceleration leg.
                let fallback_peer_discovery =
                    peer_discovery_from_config(config, &p2p, node_identity);
                let fallback = iroh::IrohBlobsP2pTransport::new(
                    p2p.clone(),
                    node_identity.id.clone(),
                    fallback_peer_discovery,
                )
                .await?;
                let fallback: Arc<dyn P2pTransport> = Arc::new(obs::InstrumentedP2pTransport::new(
                    iroh::IROH_BACKEND_ID,
                    Arc::new(fallback),
                ));
                let urma_peer_discovery =
                    peer_discovery_from_config(config, &p2p, node_identity);
                let primary = urma::UrmaP2pTransport::new(
                    &p2p,
                    node_identity.id.clone(),
                    urma_peer_discovery,
                )?;
                Ok(Arc::new(composite::CompositeP2pTransport::new(
                    Arc::new(primary),
                    fallback,
                )))
            }
        }
    }
}

fn peer_discovery_from_config(
    config: &crate::cfg::AppConfig,
    p2p: &config::ResolvedP2pConfig,
    node_identity: &NodeIdentity,
) -> Arc<dyn P2pPeerDiscovery> {
    // Statically configured URMA peers take precedence over the scheduler:
    // they enable two-node deployments without a scheduler deployment.
    let static_peers = config
        .p2p
        .urma
        .static_peers
        .iter()
        .filter_map(|raw| parse_static_urma_peer(raw))
        .collect::<Vec<_>>();
    if !static_peers.is_empty() {
        info!(
            count = static_peers.len(),
            "p2p using statically configured peers"
        );
        return Arc::new(StaticP2pPeerDiscovery::new(static_peers));
    }

    let Some(scheduler_endpoint) = config.cluster.scheduler_endpoint.clone() else {
        return Arc::new(NoopP2pPeerDiscovery);
    };

    SchedulerPeerDiscovery::start(
        scheduler_endpoint,
        node_identity.id.clone(),
        node_identity.cluster_id.to_string(),
        p2p.peer_discovery_refresh_interval,
        p2p.transport.backend_id().map(ToString::to_string),
    )
}

/// Parse "eid_hex:uasid:jetty_id" into a URMA-backend peer.
fn parse_static_urma_peer(raw: &str) -> Option<P2pPeer> {
    let mut parts = raw.split(':');
    let eid = parts.next()?.to_string();
    let uasid = parts.next()?.to_string();
    let jetty_id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if eid.is_empty() || uasid.is_empty() || jetty_id.is_empty() {
        return None;
    }
    Some(P2pPeer {
        node_id: format!("{eid}:{uasid}:{jetty_id}"),
        endpoint: P2pEndpoint {
            backend: urma::URMA_BACKEND_ID.to_string(),
            address: format!("{eid}:{uasid}:{jetty_id}"),
        },
    })
}
