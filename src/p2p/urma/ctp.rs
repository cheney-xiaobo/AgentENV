//! CTP (connection management): dynamic transport-channel establishment.
//!
//! Note: Despite the module name, all transports now use the RTP plane
//! (`tp_type = URMA_RTP`, priority 0) on UB hardware, which supports both
//! one-sided READ/WRITE and two-sided SEND/RECV. The module name is kept
//! for stability; the manager itself is transport-agnostic — it just
//! imports remote jetties and (for RC-style transports) binds them.
//!
//! URMA reliable transports are set up dynamically at runtime in two steps:
//!   1. `urma_import_jetty` — import the remote jetty id (obtained out of
//!      band, e.g. from the scheduler peer discovery / catalog).
//!   2. `urma_bind_jetty` — construct the transport channel between a local
//!      jetty and the imported remote jetty (RC-style transports only; UB
//!      RM is connectionless and skips this step).
//!
//! A local jetty can only be bound to a single remote jetty, so the manager
//! maintains one dedicated jetty per remote EID, caches established
//! connections and tears them down after an idle TTL.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::context::UrmaContext;
use super::ops::{JettyHandle, JettyId, TargetJettyHandle};
use crate::p2p::error::{Error as P2pError, Result as P2pResult};

/// An established (or being-established) connection to one remote peer,
/// keyed by the remote EID.
#[derive(Clone)]
pub struct PeerConnection {
    /// Remote EID hex string (cache key).
    pub remote_eid: String,
    /// Local jetty bound to the remote peer.
    pub jetty: JettyHandle,
    /// Imported remote jetty handle.
    pub tjetty: TargetJettyHandle,
    established_at: Instant,
    last_used: Arc<Mutex<Instant>>,
}

impl PeerConnection {
    pub fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used.lock().unwrap().elapsed()
    }
}

/// Manages per-peer jetties and dynamic bind/unbind lifecycle.
pub struct PeerConnectionManager {
    context: Arc<UrmaContext>,
    /// Remote EID hex -> connection.
    connections: Mutex<HashMap<String, PeerConnection>>,
    idle_ttl: Duration,
    connect_timeout: Duration,
}

impl PeerConnectionManager {
    pub fn new(context: Arc<UrmaContext>, connect_timeout: Duration, idle_ttl: Duration) -> Self {
        Self {
            context,
            connections: Mutex::new(HashMap::new()),
            idle_ttl,
            connect_timeout,
        }
    }

    /// Return a live connection to the remote jetty, establishing it
    /// (advise + bind) if necessary. Blocking RTP handshake; callers on the
    /// async runtime should use [`Self::ensure_connection`].
    pub fn connect_sync(&self, remote: &JettyId) -> P2pResult<PeerConnection> {
        let key = remote.eid.to_hex();
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(&key) {
                conn.touch();
                return Ok(conn.clone());
            }
        }

        // RM handshake outside the map lock: import the remote jetty and
        // (for RC-style transports) bind. A fresh local jetty is created
        // per peer because binds are 1:1.
        let ops = self.context.ops();
        let (jetty, _local_id) = self
            .context
            .create_jetty()
            .map_err(|e| P2pError::internal_message("urma connect: create_jetty", e))?;
        let tjetty = ops
            .import_jetty(self.context.handle(), remote)
            .map_err(|e| P2pError::internal_message("urma connect: import_jetty", e))?;

        let deadline = Instant::now() + self.connect_timeout;
        loop {
            match ops.bind_jetty(jetty, tjetty) {
                Ok(()) => break,
                Err(err) => {
                    // Treat transient failures as retryable until the
                    // deadline; permanent errors surface immediately after.
                    if Instant::now() >= deadline {
                        let _ = ops.unbind_jetty(jetty);
                        let _ = ops.unimport_jetty(tjetty);
                        let _ = ops.delete_jetty(jetty);
                        return Err(P2pError::internal_message("urma connect: bind_jetty", err));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }

        let conn = PeerConnection {
            remote_eid: key.clone(),
            jetty,
            tjetty,
            established_at: Instant::now(),
            last_used: Arc::new(Mutex::new(Instant::now())),
        };
        self.connections.lock().unwrap().insert(key, conn.clone());
        Ok(conn)
    }

    /// Async wrapper around [`Self::connect_sync`] using `spawn_blocking`
    /// because the bind handshake blocks.
    pub async fn ensure_connection(self: &Arc<Self>, remote: &JettyId) -> P2pResult<PeerConnection> {
        let key = remote.eid.to_hex();
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(&key) {
                conn.touch();
                return Ok(conn.clone());
            }
        }
        let this = Arc::clone(self);
        let remote = *remote;
        tokio::task::spawn_blocking(move || this.connect_sync(&remote))
            .await
            .map_err(|e| P2pError::internal_message("urma connect task join", e))?
    }

    /// Drop the cached connection for a peer (e.g. after a transport-level
    /// failure) so the next access re-establishes it.
    pub fn invalidate(&self, remote_eid: &str) {
        if let Some(conn) = self.connections.lock().unwrap().remove(remote_eid) {
            let ops = self.context.ops();
            let _ = ops.unbind_jetty(conn.jetty);
            let _ = ops.unimport_jetty(conn.tjetty);
            let _ = ops.delete_jetty(conn.jetty);
        }
    }

    /// Tear down connections idle beyond the TTL. Called periodically.
    pub fn reap_idle(&self) {
        let mut connections = self.connections.lock().unwrap();
        let expired: Vec<String> = connections
            .iter()
            .filter(|(_, conn)| conn.idle_for() > self.idle_ttl)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            if let Some(conn) = connections.remove(&key) {
                tracing::debug!("urma: reaping idle connection to {key}");
                let ops = self.context.ops();
                let _ = ops.unbind_jetty(conn.jetty);
                let _ = ops.unimport_jetty(conn.tjetty);
                let _ = ops.delete_jetty(conn.jetty);
            }
        }
    }

    /// Shut down every connection.
    pub fn shutdown(&self) {
        let mut connections = self.connections.lock().unwrap();
        for (_, conn) in connections.drain() {
            let ops = self.context.ops();
            let _ = ops.unbind_jetty(conn.jetty);
            let _ = ops.unimport_jetty(conn.tjetty);
            let _ = ops.delete_jetty(conn.jetty);
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ops::{Eid, MockUrmaOps};
    use super::*;

    fn remote_id() -> JettyId {
        JettyId {
            eid: Eid([0xaa; 16]),
            uasid: 0x1000,
            id: 7,
        }
    }

    #[tokio::test]
    async fn establishes_and_caches_connection() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let conn = manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
        // Second call hits the cache: no new advise/bind.
        let conn2 = manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
        assert_eq!(conn.jetty, conn2.jetty);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.jetties_imported.len(), 1);
        assert_eq!(status.bound.len(), 1);
    }

    #[tokio::test]
    async fn retries_transient_bind_failures_then_succeeds() {
        let ops = MockUrmaOps::new();
        ops.status.lock().unwrap().bind_failures = vec![22, 22];
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_millis(500),
            Duration::from_secs(3600),
        ));

        let conn = manager.ensure_connection(&remote_id()).await.unwrap();
        let status = ops.status.lock().unwrap();
        assert_eq!(status.bound.len(), 1);
        assert_ne!(conn.jetty.0, 0);
    }

    #[tokio::test]
    async fn bind_failure_after_timeout_invalidates_and_errors() {
        let ops = MockUrmaOps::new();
        // Repeated failure: bind never succeeds.
        ops.status.lock().unwrap().bind_failures = vec![28; 1000];
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_millis(200),
            Duration::from_secs(3600),
        ));

        assert!(manager.ensure_connection(&remote_id()).await.is_err());
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn invalidate_forces_reconnect() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let conn = manager.ensure_connection(&remote_id()).await.unwrap();
        manager.invalidate(&conn.remote_eid);
        assert_eq!(manager.connection_count(), 0);

        manager.ensure_connection(&remote_id()).await.unwrap();
        let status = ops.status.lock().unwrap();
        assert_eq!(status.bound.len(), 2);
    }

    #[tokio::test]
    async fn reap_idle_tears_down_idle_connections() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        // Very short idle TTL so the connection expires immediately.
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_millis(1),
        ));

        manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);

        // Sleep briefly so idle_for exceeds the 1ms TTL.
        std::thread::sleep(Duration::from_millis(10));
        manager.reap_idle();
        assert_eq!(manager.connection_count(), 0, "idle connection must be reaped");

        // Reconnect after reap: should establish fresh.
        manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
    }

    #[tokio::test]
    async fn multiple_peers_get_independent_jetties() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let peer_a = JettyId { eid: Eid([0xaa; 16]), uasid: 0x1000, id: 1 };
        let peer_b = JettyId { eid: Eid([0xbb; 16]), uasid: 0x1000, id: 2 };

        let conn_a = manager.ensure_connection(&peer_a).await.unwrap();
        let conn_b = manager.ensure_connection(&peer_b).await.unwrap();
        // TM_RC is 1:1: each peer must get its own local jetty.
        assert_ne!(conn_a.jetty, conn_b.jetty);
        assert_eq!(manager.connection_count(), 2);

        // Invalidation of one peer does not affect the other.
        manager.invalidate(&conn_a.remote_eid);
        assert_eq!(manager.connection_count(), 1);
        // conn_b is still usable (touch doesn't panic).
        conn_b.touch();
    }

    #[tokio::test]
    async fn shutdown_closes_all_connections() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let peer_a = JettyId { eid: Eid([0xaa; 16]), uasid: 0x1000, id: 1 };
        let peer_b = JettyId { eid: Eid([0xbb; 16]), uasid: 0x1000, id: 2 };
        manager.ensure_connection(&peer_a).await.unwrap();
        manager.ensure_connection(&peer_b).await.unwrap();
        assert_eq!(manager.connection_count(), 2);

        manager.shutdown();
        assert_eq!(manager.connection_count(), 0);

        let status = ops.status.lock().unwrap();
        // Each connection: unbind + unimport + delete_jetty.
        assert_eq!(status.unbound.len(), 2);
        assert_eq!(status.jetties_unimported.len(), 2);
        assert_eq!(status.deleted_jetties, 2);
    }
}
