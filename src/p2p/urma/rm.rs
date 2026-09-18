//! RM (remote memory) one-sided READ data path.
//!
//! Pulls a byte range from a remote segment into a locally registered
//! bounce buffer using one-sided READ WRs posted through the imported
//! remote jetty. The remote node is not involved in the data path (no
//! remote CPU), which is the point of RM.
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use super::context::UrmaContext;
use super::ctp::PeerConnectionManager;
use super::ops::{Completion, Eid, JettyId, ReadWr, RemoteSegInfo, TargetSegHandle};
use super::segment::{RemoteSegmentCache, UrmaLocator};
use crate::p2p::error::{Error as P2pError, Result as P2pResult};

pub struct RmReader {
    context: Arc<UrmaContext>,
    connections: Arc<PeerConnectionManager>,
    remote_cache: Arc<RemoteSegmentCache>,
    chunk_bytes: u64,
}

impl RmReader {
    pub fn new(
        context: Arc<UrmaContext>,
        connections: Arc<PeerConnectionManager>,
        remote_cache: Arc<RemoteSegmentCache>,
        chunk_bytes: u64,
    ) -> Self {
        Self {
            context,
            connections,
            remote_cache,
            chunk_bytes: chunk_bytes.max(4096),
        }
    }

    /// Pull `[offset, offset+len)` of the remote artifact described by
    /// `locator` into the local registered segment `local_tseg` whose base
    /// UBVA is `local_base_va` (the read lands at `local_base_va`).
    ///
    /// The local buffer must cover `len` bytes starting at its base.
    pub async fn read_range(
        &self,
        locator: &UrmaLocator,
        offset: u64,
        len: u64,
        local_tseg: TargetSegHandle,
        local_base_va: u64,
        timeout: Duration,
    ) -> P2pResult<()> {
        if offset.checked_add(len).map(|end| end > locator.len).unwrap_or(true) {
            return Err(P2pError::Internal(anyhow::anyhow!(
                "urma: read range [{offset}, {offset}+{len}) out of bounds for segment of {} bytes",
                locator.len
            )));
        }

        let remote_eid = Eid::from_hex(&locator.eid).ok_or_else(|| {
            P2pError::Internal(anyhow::anyhow!("urma: invalid EID in locator: {:?}", locator.eid))
        })?;
        let remote_jetty = JettyId {
            eid: remote_eid,
            uasid: locator.uasid,
            id: locator.jetty,
        };
        let remote_seg = RemoteSegInfo {
            eid: remote_eid,
            uasid: locator.uasid,
            va: locator.va,
            key: locator.key,
            len: locator.len,
        };

        // 1. Import the remote jetty (cached per peer).
        let conn = match self.connections.ensure_connection(&remote_jetty).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!(
                    eid = %locator.eid,
                    jetty = locator.jetty,
                    error = %err,
                    "urma: ensure_connection (import remote jetty) failed"
                );
                return Err(err);
            }
        };

        // 2. Import the remote segment (cached per EID/VA/key); READs
        //    address the remote memory through the imported mapping.
        let imported = match self.remote_cache.get_or_import(&self.context, &remote_seg) {
            Ok(imported) => imported,
            Err(err) => {
                warn!(
                    eid = %locator.eid,
                    va = locator.va,
                    key = locator.key,
                    len = locator.len,
                    error = %err,
                    "urma: import remote segment failed"
                );
                return Err(err);
            }
        };
        debug!(
            tjetty = conn.tjetty.0,
            remote_tseg = imported.tseg.0,
            mva = imported.mva,
            "urma: read_range connection and segment ready"
        );

        // 3. Chunked one-sided READs. M1 uses serial chunks; a pipelined
        //    window is a follow-up once the semantics are validated on
        //    real hardware.
        let mut remaining = len;
        let mut remote_off = offset;
        while remaining > 0 {
            let chunk = remaining.min(self.chunk_bytes) as u32;
            let wr_id = self.context.alloc_wr_id();
            let rx = self.context.subscribe(wr_id);
            let wr = ReadWr {
                wr_id,
                tjetty: conn.tjetty,
                remote_tseg: imported.tseg,
                remote_va: imported.mva + remote_off,
                local_tseg,
                local_va: local_base_va + (len - remaining),
                len: chunk,
            };
            if let Err(err) = self.context.ops().post_read(conn.jetty, &wr) {
                warn!(wr_id, error = %err, "urma: post_read failed");
                self.connections.invalidate(&conn.remote_eid);
                return Err(P2pError::internal_message("urma post_read", err));
            }

            let completion: Completion = match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(cqe)) => cqe,
                Ok(Err(_recv_err)) => {
                    warn!(wr_id, "urma: completion channel closed");
                    self.connections.invalidate(&conn.remote_eid);
                    return Err(P2pError::Internal(anyhow::anyhow!(
                        "urma: completion channel closed for wr {wr_id}"
                    )));
                }
                Err(_) => {
                    warn!(wr_id, ?timeout, "urma: read completion timed out");
                    self.connections.invalidate(&conn.remote_eid);
                    return Err(P2pError::Internal(anyhow::anyhow!(
                        "urma: read completion timeout after {timeout:?} for wr {wr_id}"
                    )));
                }
            };
            if completion.status != 0 {
                warn!(
                    wr_id,
                    status = completion.status,
                    "urma: read completed with error status"
                );
                self.connections.invalidate(&conn.remote_eid);
                return Err(P2pError::Internal(anyhow::anyhow!(
                    "urma: read wr {wr_id} completed with status {}",
                    completion.status
                )));
            }
            remote_off += chunk as u64;
            remaining -= chunk as u64;
        }
        conn.touch();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::ops::{MockUrmaOps, RegisteredSeg};
    use super::*;

    fn locator(eid_hex: &str, jetty: u32, len: u64) -> UrmaLocator {
        UrmaLocator {
            eid: eid_hex.to_string(),
            uasid: 0x1000,
            jetty,
            va: 0x2000,
            key: 9,
            len,
        }
    }

    async fn setup() -> (
        Arc<MockUrmaOps>,
        Arc<UrmaContext>,
        Arc<PeerConnectionManager>,
        Arc<RemoteSegmentCache>,
    ) {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let connections = Arc::new(PeerConnectionManager::new(
            context.clone(),
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));
        let cache = Arc::new(RemoteSegmentCache::new());
        (ops, context, connections, cache)
    }

    #[tokio::test]
    async fn reads_chunked_range() {
        let (ops, context, connections, cache) = setup().await;
        // chunk_bytes is clamped to >= 4096, so use 4096 explicitly.
        let chunk = 4096u64;
        let reader = RmReader::new(context, connections, cache, chunk);

        let remote = locator("22222222222222222222222222222222", 7, 8192);
        let local = RegisteredSeg {
            tseg: TargetSegHandle(1234),
            va: 0x9000,
            key: 1,
        };
        // 6000 bytes with 4096-byte chunks => 2 reads (4096 + 1904).
        reader
            .read_range(&remote, 500, 6000, local.tseg, local.va, Duration::from_secs(5))
            .await
            .unwrap();

        let status = ops.status.lock().unwrap();
        assert_eq!(status.reads_log.len(), 2);
        assert_eq!(status.reads_log[0].remote_va, remote.va + 500);
        assert_eq!(status.reads_log[0].len, 4096);
        assert_eq!(status.reads_log[0].local_va, local.va);
        assert_eq!(status.reads_log[1].remote_va, remote.va + 4596);
        assert_eq!(status.reads_log[1].len, 1904);
        assert_eq!(status.reads_log[1].local_va, local.va + 4096);
    }

    #[tokio::test]
    async fn rejects_out_of_bounds() {
        let (_ops, context, connections, cache) = setup().await;
        let reader = RmReader::new(context, connections, cache, 4096);

        let remote = locator("22222222222222222222222222222222", 7, 4096);
        let err = reader
            .read_range(&remote, 4000, 1000, TargetSegHandle(1), 0x9000, Duration::from_secs(1))
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn invalidates_connection_on_completion_error() {
        let (ops, context, connections, cache) = setup().await;
        ops.status.lock().unwrap().completion_failure = 5;
        let reader = RmReader::new(context, connections, cache, 4096);

        let remote = locator("22222222222222222222222222222222", 7, 4096);
        let result = reader
            .read_range(&remote, 0, 100, TargetSegHandle(1), 0x9000, Duration::from_secs(5))
            .await;
        assert!(result.is_err());
        assert_eq!(reader.connections.connection_count(), 0);
    }
}
