//! Remote artifact catalog over URMA two-sided SEND/RECV (M2).
//!
//! `lookup` needs a way to discover artifacts published on peer nodes
//! without going through the scheduler. Each URMA transport runs a small
//! catalog server on its serve jetty: peers establish an RTP connection
//! (single transport plane for both SEND/RECV and one-sided READ/WRITE on
//! UB hardware) to the serve jetty, then exchange a single request/response
//! message pair.
//!
//! Wire protocol (JSON over a fixed 4KiB registered buffer):
//!
//! ```text
//! requester                              serve jetty owner
//!     |  post_recv(response buf)               |
//!     |  post_send(request: {"key": ...})  --> | (lands in pre-posted RECV)
//!     |                                        | resolve against local registry
//!     | <-- post_send(response: {descriptor})  |
//!     |  RECV completion (len = response size) |
//! ```
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

use super::context::UrmaContext;
use super::ctp::PeerConnectionManager;
use super::ops::{JettyId, MsgWr, RegisteredSeg, TargetJettyHandle};
use super::URMA_BACKEND_ID;
use crate::p2p::error::{Error, Result};
use crate::p2p::types::{P2pArtifactDescriptor, P2pArtifactKey, P2pPeer};

/// Fixed size of catalog control buffers (registered segments).
pub const CATALOG_BUF_SIZE: usize = 4096;

/// Catalog query request body.
#[derive(Serialize, Deserialize)]
pub struct CatalogRequest {
    pub key: String,
}

/// Catalog query response body.
#[derive(Serialize, Deserialize)]
pub struct CatalogResponse {
    pub descriptor: Option<P2pArtifactDescriptor>,
}

/// Parse a `"<eid-hex>:<uasid>:<jetty-id>"` endpoint address into an RM
/// target jetty id.
pub fn jetty_id_from_address(address: &str) -> Option<JettyId> {
    let (eid_hex, rest) = address.split_once(':')?;
    let (uasid_str, jetty_str) = rest.split_once(':')?;
    let eid = super::ops::Eid::from_hex(eid_hex)?;
    let uasid = uasid_str.parse::<u32>().ok()?;
    let jetty = jetty_str.parse::<u32>().ok()?;
    Some(JettyId { eid, uasid, id: jetty })
}

/// A registered 4KiB control buffer used as SEND source / RECV landing
/// buffer for catalog messages. The backing memory is page-aligned (URMA
/// requires it for `urma_register_seg`). It must stay alive (and untouched)
/// while a work request referencing it is in flight.
struct ControlBuffer {
    /// Page-aligned heap address (stored as usize to be Send+Sync).
    addr: usize,
    /// Registered segment metadata.
    registered: RegisteredSeg,
}

impl ControlBuffer {
    fn new(context: &UrmaContext) -> Result<Self> {
        use std::alloc::{alloc, dealloc, Layout};
        let layout = Layout::from_size_align(CATALOG_BUF_SIZE, 4096)
            .map_err(|e| Error::Internal(anyhow::anyhow!("urma: bad alloc layout: {e}")))?;
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(Error::Internal(anyhow::anyhow!("urma: failed to allocate page-aligned buffer")));
        }
        unsafe { std::ptr::write_bytes(ptr, 0, CATALOG_BUF_SIZE) };
        let addr = ptr as usize;
        let registered = context
            .ops()
            .register_local_seg(context.handle(), addr, CATALOG_BUF_SIZE as u64)
            .map_err(|e| {
                // Free on registration failure.
                unsafe { dealloc(ptr, layout); }
                Error::internal_message("urma register catalog buffer", e)
            })?;
        Ok(Self { addr, registered })
    }

    /// Return a mutable slice over the buffer for writing.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.addr as *mut u8, CATALOG_BUF_SIZE) }
    }

    /// Return a slice over the buffer for reading.
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.addr as *const u8, CATALOG_BUF_SIZE) }
    }

    /// Write a JSON payload into the buffer. Fails if it does not fit.
    fn write_json(&mut self, value: &impl Serialize) -> Result<usize> {
        let json = serde_json::to_vec(value)
            .map_err(|e| Error::internal_message("serialize catalog message", e))?;
        if json.len() > CATALOG_BUF_SIZE {
            return Err(Error::Internal(anyhow::anyhow!(
                "urma catalog message too large: {} bytes",
                json.len()
            )));
        }
        self.as_mut_slice()[..json.len()].copy_from_slice(&json);
        Ok(json.len())
    }

    fn read_json(&self, len: usize) -> Option<CatalogResponse> {
        if len > CATALOG_BUF_SIZE {
            return None;
        }
        serde_json::from_slice(&self.as_slice()[..len]).ok()
    }
}

impl Drop for ControlBuffer {
    fn drop(&mut self) {
        use std::alloc::{dealloc, Layout};
        let layout = Layout::from_size_align(CATALOG_BUF_SIZE, 4096).unwrap();
        unsafe { dealloc(self.addr as *mut u8, layout) };
    }
}

/// Client-side catalog abstraction (mockable in tests).
#[async_trait]
pub trait RemoteCatalog: Send + Sync {
    /// Query `peer` for the descriptor of `key`. `Ok(None)` = peer does not
    /// serve the key.
    async fn query(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>>;
}

/// Real catalog client: SEND/RECV over an RTP connection to the peer's serve
/// jetty. Control buffers are pooled (registered once, reused per query).
pub struct UrmaCatalog {
    context: Arc<UrmaContext>,
    connections: Arc<PeerConnectionManager>,
    pool: tokio::sync::Mutex<Vec<ControlBuffer>>,
    timeout: Duration,
}

impl UrmaCatalog {
    pub fn new(
        context: Arc<UrmaContext>,
        connections: Arc<PeerConnectionManager>,
        timeout: Duration,
    ) -> Self {
        Self {
            context,
            connections,
            pool: tokio::sync::Mutex::new(Vec::new()),
            timeout,
        }
    }

    async fn lease(&self) -> Result<ControlBuffer> {
        let existing = self.pool.lock().await.pop();
        match existing {
            Some(buf) => Ok(buf),
            None => ControlBuffer::new(&self.context),
        }
    }

    async fn release(&self, mut buffers: Vec<ControlBuffer>) {
        self.pool.lock().await.append(&mut buffers);
    }
}

#[async_trait]
impl RemoteCatalog for UrmaCatalog {
    #[instrument(skip(self, key), fields(peer = %peer.node_id, key = %key))]
    async fn query(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        if peer.endpoint.backend != URMA_BACKEND_ID {
            return Err(Error::InvalidDescriptor {
                reason: format!(
                    "peer endpoint backend {:?} is not {:?}",
                    peer.endpoint.backend, URMA_BACKEND_ID
                ),
            });
        }
        let remote = jetty_id_from_address(&peer.endpoint.address).ok_or_else(|| {
            Error::InvalidDescriptor {
                reason: format!(
                    "malformed urma endpoint address: {:?}",
                    peer.endpoint.address
                ),
            }
        })?;

        let conn = self.connections.ensure_connection(&remote).await?;
        let mut send_buf = self.lease().await?;
        let mut recv_buf = self.lease().await?;

        let result = async {
            let send_len = send_buf.write_json(&CatalogRequest { key: key.clone() })?;

            // Pre-post the response landing buffer before the request goes
            // out so the peer's SEND cannot race past it.
            let recv_wr_id = self.context.alloc_wr_id();
            let recv_rx = self.context.subscribe(recv_wr_id);
            self.context.ops().post_recv(
                conn.jetty,
                &MsgWr {
                    wr_id: recv_wr_id,
                    tjetty: conn.tjetty,
                    local_tseg: recv_buf.registered.tseg,
                    local_va: recv_buf.registered.local_va,
                    len: CATALOG_BUF_SIZE as u32,
                },
            ).map_err(|e| Error::internal_message("urma post_recv", e))?;

            let send_wr_id = self.context.alloc_wr_id();
            let send_rx = self.context.subscribe(send_wr_id);
            self.context.ops().post_send(
                conn.jetty,
                &MsgWr {
                    wr_id: send_wr_id,
                    tjetty: conn.tjetty,
                    local_tseg: send_buf.registered.tseg,
                    local_va: send_buf.registered.local_va,
                    len: send_len as u32,
                },
            ).map_err(|e| Error::internal_message("urma post_send", e))?;

            let send_cqe = tokio::time::timeout(self.timeout, send_rx)
                .await
                .map_err(|_| Error::Internal(anyhow::anyhow!(
                    "urma: catalog request send timed out after {:?}", self.timeout
                )))?
                .map_err(|_| Error::Internal(anyhow::anyhow!(
                    "urma: catalog send completion channel closed"
                )))?;
            if send_cqe.status != 0 {
                return Err(Error::Internal(anyhow::anyhow!(
                    "urma: catalog send completed with status {}",
                    send_cqe.status
                )));
            }

            let recv_cqe = tokio::time::timeout(self.timeout, recv_rx)
                .await
                .map_err(|_| Error::Internal(anyhow::anyhow!(
                    "urma: catalog response timed out after {:?}", self.timeout
                )))?
                .map_err(|_| Error::Internal(anyhow::anyhow!(
                    "urma: catalog recv completion channel closed"
                )))?;
            if recv_cqe.status != 0 {
                return Err(Error::Internal(anyhow::anyhow!(
                    "urma: catalog recv completed with status {}",
                    recv_cqe.status
                )));
            }

            let response = recv_buf
                .read_json(recv_cqe.len as usize)
                .ok_or_else(|| Error::Internal(anyhow::anyhow!(
                    "urma: malformed catalog response ({} bytes)",
                    recv_cqe.len
                )))?;
            Ok(response.descriptor)
        }
        .await;

        if result.is_err() {
            self.connections.invalidate(&conn.remote_eid);
        } else {
            conn.touch();
        }

        let buffers = vec![send_buf, recv_buf];
        self.release(buffers);
        result
    }
}

/// Server side of the catalog protocol: pre-posts a RECV on the serve
/// jetty, resolves requests against the local registry and SENDs back the
/// descriptor (or `null`).
pub struct CatalogServer;

impl CatalogServer {
    /// Spawn the catalog server task. `resolve` maps an artifact key to the
    /// local descriptor (if published). The task runs until aborted by the
    /// transport's shutdown.
    pub fn spawn(
        context: Arc<UrmaContext>,
        serve_jetty: super::ops::JettyHandle,
        resolve: Arc<dyn Fn(&str) -> Option<P2pArtifactDescriptor> + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut recv_buf = match ControlBuffer::new(&context) {
                Ok(buf) => buf,
                Err(err) => {
                    warn!(error = %err, "urma: catalog server failed to allocate buffer");
                    return;
                }
            };
            let mut send_buf = match ControlBuffer::new(&context) {
                Ok(buf) => buf,
                Err(err) => {
                    warn!(error = %err, "urma: catalog server failed to allocate buffer");
                    return;
                }
            };
            debug!("urma: catalog server listening on serve jetty");
            // Imported requester jettys, cached so repeat queries from the
            // same peer do not re-import. Entries live until shutdown (the
            // whole context is torn down then).
            let mut client_jetties: HashMap<JettyId, TargetJettyHandle> = HashMap::new();

            loop {
                // 1. Post the request landing buffer.
                let recv_wr_id = context.alloc_wr_id();
                let recv_rx = context.subscribe(recv_wr_id);
                if let Err(err) = context.ops().post_recv(
                    serve_jetty,
                    &MsgWr {
                        wr_id: recv_wr_id,
                        // RECV ignores the target; fill a placeholder.
                        tjetty: TargetJettyHandle(0),
                        local_tseg: recv_buf.registered.tseg,
                        local_va: recv_buf.registered.local_va,
                        len: CATALOG_BUF_SIZE as u32,
                    },
                ) {
                    warn!(error = %err, "urma: catalog server post_recv failed");
                    return;
                }

                // 2. Await the request.
                let cqe = match recv_rx.await {
                    Ok(cqe) => cqe,
                    Err(_) => {
                        debug!("urma: catalog server shutting down (channel closed)");
                        return;
                    }
                };
                if cqe.status != 0 {
                    warn!(status = cqe.status, "urma: catalog server recv failed");
                    return;
                }
                let request: CatalogRequest = match serde_json::from_slice(
                    &recv_buf.as_slice()[..cqe.len as usize],
                ) {
                    Ok(req) => req,
                    Err(err) => {
                        warn!(error = %err, "urma: catalog server got malformed request");
                        continue;
                    }
                };

                // The requester's jetty (from the RECV completion) is the
                // target the response SEND must address (RM is
                // connectionless, every SEND carries its target).
                let Some(client) = cqe.from else {
                    warn!("urma: catalog request without a sender jetty id");
                    continue;
                };
                let tjetty = match client_jetties.entry(client) {
                    std::collections::hash_map::Entry::Occupied(occupied) => *occupied.get(),
                    std::collections::hash_map::Entry::Vacant(vacant) => {
                        match context.ops().import_jetty(context.handle(), vacant.key()) {
                            Ok(handle) => *vacant.insert(handle),
                            Err(err) => {
                                warn!(error = %err, "urma: catalog server failed to import client jetty");
                                continue;
                            }
                        }
                    }
                };

                // 3. Resolve and send the response.
                let response = CatalogResponse {
                    descriptor: resolve(&request.key),
                };
                let send_len = match send_buf.write_json(&response) {
                    Ok(len) => len,
                    Err(err) => {
                        warn!(error = %err, "urma: catalog server response encode failed");
                        continue;
                    }
                };
                let send_wr_id = context.alloc_wr_id();
                let send_rx = context.subscribe(send_wr_id);
                if let Err(err) = context.ops().post_send(
                    serve_jetty,
                    &MsgWr {
                        wr_id: send_wr_id,
                        tjetty,
                        local_tseg: send_buf.registered.tseg,
                        local_va: send_buf.registered.local_va,
                        len: send_len as u32,
                    },
                ) {
                    warn!(error = %err, "urma: catalog server post_send failed");
                    return;
                }
                match send_rx.await {
                    Ok(cqe) if cqe.status == 0 => {}
                    Ok(cqe) => {
                        warn!(status = cqe.status, "urma: catalog server send failed");
                        return;
                    }
                    Err(_) => return,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jetty_id_address_roundtrip() {
        let id = jetty_id_from_address("11223344556677889900aabbccddeeff:4096:42");
        assert_eq!(id.map(|i| i.id), Some(42));
        assert_eq!(id.map(|i| i.uasid), Some(4096));
        assert!(jetty_id_from_address("no-colon").is_none());
        assert!(jetty_id_from_address("zz:1:2").is_none());
        assert!(jetty_id_from_address("1122:not-a-number:4").is_none());
        assert!(jetty_id_from_address("1122:4096").is_none());
    }

    #[test]
    fn catalog_message_codec_roundtrip() {
        let ops = super::super::ops::MockUrmaOps::new();
        let context = UrmaContext::mock(ops).unwrap();
        let mut buf = ControlBuffer::new(&context).unwrap();

        let descriptor = CatalogResponse {
            descriptor: Some(P2pArtifactDescriptor {
                key: "k1".to_string(),
                providers: vec![],
                backend_locator: Some("urma1:abc".to_string()),
                metadata: serde_json::Value::Null,
            }),
        };
        let len = buf.write_json(&descriptor).unwrap();
        assert!(len <= CATALOG_BUF_SIZE);
        let parsed = buf.read_json(len).unwrap();
        assert_eq!(parsed.descriptor.as_ref().unwrap().key, "k1");

        // Miss case: descriptor is null.
        let len = buf.write_json(&CatalogResponse { descriptor: None }).unwrap();
        let parsed = buf.read_json(len).unwrap();
        assert!(parsed.descriptor.is_none());
    }

    #[test]
    fn oversize_payload_rejected() {
        let ops = super::super::ops::MockUrmaOps::new();
        let context = UrmaContext::mock(ops).unwrap();
        let mut buf = ControlBuffer::new(&context).unwrap();
        let big = "x".repeat(CATALOG_BUF_SIZE * 2);
        assert!(buf.write_json(&CatalogRequest { key: big }).is_err());
    }
}
