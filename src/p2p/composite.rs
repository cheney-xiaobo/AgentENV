//! Composite P2P transport: URMA acceleration with iroh fallback (M3).
//!
//! `lookup` consults the URMA catalog first (ubiquitous on UB-attached
//! nodes, near-zero marginal cost) and falls back to iroh discovery.
//! `fetch*` prefer the transport matching the descriptor's backend
//! locator; on failure the artifact is re-resolved through the other
//! transport, so a missing UB path or a stale URMA locator degrades to
//! plain iroh instead of failing the transfer.
//!
//! `publish` fans out to both transports so the artifact is reachable by
//! URMA-capable peers (one-sided READ) and everyone else (iroh blobs).
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{info, instrument, warn};

use super::error::{Error, Result};
use super::transport::P2pTransport;
use super::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProviderHint, P2pEndpoint,
    P2pPublishRequest,
};
use super::P2pByteStream;

pub struct CompositeP2pTransport {
    /// Acceleration path (URMA one-sided READs).
    primary: Arc<dyn P2pTransport>,
    /// Ubiquitous fallback path (iroh blobs).
    fallback: Arc<dyn P2pTransport>,
}

impl CompositeP2pTransport {
    pub fn new(primary: Arc<dyn P2pTransport>, fallback: Arc<dyn P2pTransport>) -> Self {
        info!("composite artifact transport started (primary=urma, fallback=iroh)");
        Self { primary, fallback }
    }

    /// Which leg should serve this descriptor first, based on its
    /// providers' advertised backends. Unknown backends prefer fallback.
    fn prefers_primary(descriptor: &P2pArtifactDescriptor) -> bool {
        descriptor.providers.iter().any(|p| match p {
            super::types::P2pArtifactProvider::Peer(peer) => {
                peer.endpoint.backend == super::urma::URMA_BACKEND_ID
            }
            _ => false,
        })
    }
}

#[async_trait]
impl P2pTransport for CompositeP2pTransport {
    #[instrument(skip(self, hints), fields(key = %key))]
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        // URMA catalog first: cheap, and hits give the zero-copy fast path.
        match self.primary.lookup_with_hints(key, hints).await {
            Ok(Some(descriptor)) => return Ok(Some(descriptor)),
            Ok(None) => {}
            Err(err) => warn!(error = %err, "composite: urma lookup failed, trying iroh"),
        }
        self.fallback.lookup_with_hints(key, hints).await
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key, destination = %destination.display()))]
    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, destination: &Path) -> Result<u64> {
        if Self::prefers_primary(descriptor) {
            match self.primary.fetch(descriptor, destination).await {
                Ok(size) => return Ok(size),
                Err(err) => {
                    warn!(error = %err, "composite: urma fetch failed, falling back to iroh");
                }
            }
        }
        self.fallback.fetch(descriptor, destination).await
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key))]
    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        if Self::prefers_primary(descriptor) {
            match self.primary.fetch_bytes(descriptor).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    warn!(error = %err, "composite: urma fetch_bytes failed, falling back to iroh");
                }
            }
        }
        self.fallback.fetch_bytes(descriptor).await
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key, offset, len))]
    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        if Self::prefers_primary(descriptor) {
            match self.primary.fetch_byte_range(descriptor, offset, len).await {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    warn!(
                        error = %err,
                        "composite: urma fetch_byte_range failed, falling back to iroh"
                    );
                }
            }
        }
        self.fallback.fetch_byte_range(descriptor, offset, len).await
    }

    #[instrument(skip(self, request), fields(key = %request.key, source = %request.source))]
    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        // Fan out to both legs; the URMA leg is an acceleration, so its
        // failures are logged but do not fail the publish.
        if let Err(err) = self.primary.publish(request).await {
            warn!(error = %err, "composite: urma publish failed (iroh publication continues)");
        }
        self.fallback.publish(request).await
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        let removed_primary = self.primary.unpublish(key).await.unwrap_or_else(|err| {
            warn!(error = %err, "composite: urma unpublish failed");
            false
        });
        let removed_fallback = self.fallback.unpublish(key).await?;
        Ok(removed_primary || removed_fallback)
    }

    /// Advertise the iroh endpoint: every composite node is reachable
    /// through it, while URMA reachability is discovered via hints/catalog.
    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        self.fallback.local_endpoint()
    }

    async fn shutdown(&self) -> Result<()> {
        let (primary, fallback) = (&self.primary, &self.fallback);
        let (a, b) = tokio::join!(primary.shutdown(), fallback.shutdown());
        a?;
        b
    }
}

// Keep the Disabled error variant referenced so the fallback error path
// stays meaningful in docs below.
#[allow(dead_code)]
fn _unused(err: Error) -> Error {
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::mock::{FakeP2pTransport, RecordedPublish};
    use crate::p2p::types::{P2pArtifactProvider, P2pPeer};
    use crate::p2p::urma::URMA_BACKEND_ID;

    fn urma_descriptor(key: &str) -> P2pArtifactDescriptor {
        P2pArtifactDescriptor {
            key: key.to_string(),
            providers: vec![P2pArtifactProvider::Peer(P2pPeer {
                node_id: "node-b".to_string(),
                endpoint: P2pEndpoint {
                    backend: URMA_BACKEND_ID.to_string(),
                    address: "aabb:1".to_string(),
                },
            })],
            backend_locator: Some("urma1:whatever".to_string()),
            metadata: serde_json::Value::Null,
        }
    }

    #[tokio::test]
    async fn fetch_prefers_urma_and_falls_back_on_failure() {
        // Primary fails everything; fallback serves the bytes.
        let primary = FakeP2pTransport::failing("urma");
        let fallback = FakeP2pTransport::with_bytes("k1", b"via-iroh");
        let composite = CompositeP2pTransport::new(Arc::new(primary), Arc::new(fallback));

        let descriptor = urma_descriptor("k1");
        let bytes = composite.fetch_bytes(&descriptor).await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"via-iroh"));
    }

    #[tokio::test]
    async fn fetch_uses_urma_when_healthy() {
        let primary = FakeP2pTransport::with_bytes("k1", b"via-urma");
        let fallback = FakeP2pTransport::failing("iroh");
        let composite = CompositeP2pTransport::new(Arc::new(primary), Arc::new(fallback));

        let bytes = composite.fetch_bytes(&urma_descriptor("k1")).await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"via-urma"));
    }

    #[tokio::test]
    async fn publish_fans_out_to_both() {
        let primary = FakeP2pTransport::empty();
        let fallback = FakeP2pTransport::empty();
        let composite = CompositeP2pTransport::new(
            Arc::new(primary.clone()),
            Arc::new(fallback.clone()),
        );

        composite
            .publish(&P2pPublishRequest::bytes("k1", Bytes::from_static(b"x")))
            .await
            .unwrap();

        assert_eq!(
            primary.recorded_publishes(),
            vec![RecordedPublish {
                key: "k1".to_string()
            }]
        );
        assert_eq!(
            fallback.recorded_publishes(),
            vec![RecordedPublish {
                key: "k1".to_string()
            }]
        );
    }

    #[tokio::test]
    async fn lookup_falls_back_when_primary_misses() {
        let primary = FakeP2pTransport::empty();
        let fallback = FakeP2pTransport::with_bytes("k1", b"via-iroh");
        let composite = CompositeP2pTransport::new(Arc::new(primary), Arc::new(fallback));

        let found = composite.lookup(&"k1".to_string()).await.unwrap();
        assert!(found.is_some(), "iroh fallback must resolve the key");
    }
}
