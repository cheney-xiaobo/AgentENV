//! URMA P2P transport backend implementing [`P2pTransport`].
//!
//! M1 scope (RTP transport plane, dynamic connections):
//!
//! - `publish`: artifact bytes are held in memory and registered as a URMA
//!   segment; the advertised locator encodes (EID, serve jetty, UBVA, key,
//!   length).
//! - `fetch*`: resolve the backend locator, dynamically bind to the remote
//!   serve jetty (RTP) and pull the byte range with one-sided READs (RM)
//!   through a registered local bounce buffer.
//! - `lookup`: local publications only. Remote catalog queries over URMA
//!   SEND/RECV are a follow-up (M2); until then descriptors must reach
//!   consumers via provider hints or scheduler metadata.
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, instrument, warn};

use super::catalog::{CatalogServer, RemoteCatalog, UrmaCatalog};
use super::context::UrmaContext;
use super::ctp::PeerConnectionManager;
use super::ops::UrmaOps;
use super::rm::RmReader;
use super::segment::{RemoteSegmentCache, SegmentRegistry, UrmaLocator, URMA_LOCATOR_PREFIX};
use super::URMA_BACKEND_ID;
use crate::p2p::config::ResolvedP2pConfig;
use crate::p2p::discovery::P2pPeerDiscovery;
use crate::p2p::error::{Error, Result};
use crate::p2p::transport::P2pTransport;
use crate::p2p::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pEndpoint, P2pPeer, P2pPublishMode, P2pPublishRequest, P2pPublishSource,
};
use crate::p2p::P2pByteStream;

/// Upper bound on the local bounce buffer used to stage one-sided READs.
const MAX_BOUNCE_WINDOW: u64 = 16 * 1024 * 1024;
/// Channel capacity for streamed fetch windows.
const STREAM_CHANNEL_CAPACITY: usize = 4;

/// A published artifact's retained bytes (kept alive because the URMA
/// segment points into this memory).
enum LocalArtifact {
    /// Zero-copy reference publish: the file stays on disk and the URMA
    /// segment points into a private read-only `mmap` of it (M3). `map` is
    /// `None` on platforms without `mmap` (fallback keeps bytes in memory
    /// via `InMemory` instead, so this variant always owns a live map).
    Reference {
        path: std::path::PathBuf,
        map: file_map::FileMap,
        metadata: serde_json::Value,
    },
    /// In-memory publish with a 4K-aligned allocation: URMA `register_seg`
    /// requires the base address to be page-aligned (4K). Standard
    /// `Vec<u8>`/`Bytes` allocations are not guaranteed to be 4K-aligned,
    /// so we copy the bytes into a dedicated aligned buffer.
    InMemory {
        buf: aligned_buf::AlignedBuf,
        metadata: serde_json::Value,
    },
}

impl LocalArtifact {
    /// Publish-time descriptor metadata advertised through the catalog so
    /// remote pullers can parse `LayerMetadata` from the descriptor.
    fn metadata(&self) -> &serde_json::Value {
        match self {
            LocalArtifact::Reference { metadata, .. } | LocalArtifact::InMemory { metadata, .. } => {
                metadata
            }
        }
    }
}

/// Read-only `mmap` of a published file (zero-copy publish source).
mod file_map {
    use std::path::Path;

    use crate::p2p::error::{Error, Result};

    pub struct FileMap {
        pub addr: usize,
        pub len: usize,
    }

    impl FileMap {
        /// Private read-only mapping; the mapping stays valid after the fd
        /// is closed. Empty files map to a null address without touching
        /// the mmap syscall.
        pub fn map(path: &Path) -> Result<FileMap> {
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file = std::fs::File::open(path)
                    .map_err(|e| Error::internal_message("open publish source", e))?;
                let len = file
                    .metadata()
                    .map_err(|e| Error::internal_message("stat publish source", e))?
                    .len() as usize;
                if len == 0 {
                    return Ok(FileMap { addr: 0, len: 0 });
                }
                let ptr = unsafe {
                    libc::mmap(
                        std::ptr::null_mut(),
                        len,
                        libc::PROT_READ,
                        libc::MAP_PRIVATE,
                        file.as_raw_fd(),
                        0,
                    )
                };
                if ptr == libc::MAP_FAILED {
                    return Err(Error::internal_message(
                        "mmap publish source",
                        std::io::Error::last_os_error(),
                    ));
                }
                Ok(FileMap { addr: ptr as usize, len })
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Internal(anyhow::anyhow!(
                    "reference publish requires mmap (unix)"
                )))
            }
        }

        /// Copy `[offset, offset+len)` out of the mapping. `len == None`
        /// means "to the end".
        pub fn slice(&self, offset: u64, len: Option<usize>) -> Result<bytes::Bytes> {
            if self.len == 0 {
                return Ok(bytes::Bytes::new());
            }
            let start = offset as usize;
            if start > self.len {
                return Err(Error::InvalidDescriptor {
                    reason: "range out of bounds for local artifact".to_string(),
                });
            }
            let end = match len {
                Some(n) => start
                    .checked_add(n)
                    .filter(|e| *e <= self.len)
                    .ok_or_else(|| Error::InvalidDescriptor {
                        reason: "range out of bounds for local artifact".to_string(),
                    })?,
                None => self.len,
            };
            Ok(unsafe {
                bytes::Bytes::copy_from_slice(std::slice::from_raw_parts(
                    (self.addr + start) as *const u8,
                    end - start,
                ))
            })
        }
    }

    impl Drop for FileMap {
        fn drop(&mut self) {
            if self.len > 0 {
                #[cfg(unix)]
                unsafe { libc::munmap(self.addr as *mut libc::c_void, self.len) };
                #[cfg(not(unix))]
                unreachable!("FileMap with len > 0 only exists on unix");
            }
        }
    }

    // The mapping is read-only and never mutated across its lifetime.
    unsafe impl Send for FileMap {}
    unsafe impl Sync for FileMap {}
}

/// 4K-aligned owned byte buffer. URMA `register_seg` requires the base
/// address to be 4K page-aligned; standard `Vec<u8>` / `Bytes` do not
/// guarantee this, so we copy the data into a dedicated aligned allocation.
mod aligned_buf {
    pub struct AlignedBuf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
        len: usize,
    }

    impl AlignedBuf {
        pub fn new(data: &[u8]) -> Self {
            let len = data.len();
            let alloc_size = ((len + 4095) / 4096) * 4096;
            let layout =
                std::alloc::Layout::from_size_align(alloc_size.max(1), 4096).unwrap();
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
            }
            Self { ptr, layout, len }
        }

        pub fn addr(&self) -> usize {
            self.ptr as usize
        }

        pub fn len(&self) -> usize {
            self.len
        }

        /// Copy `[offset, offset+len)` out of the buffer. `len == None`
        /// means "to the end".
        pub fn slice(&self, offset: usize, len: Option<usize>) -> bytes::Bytes {
            if self.len == 0 {
                return bytes::Bytes::new();
            }
            let start = offset.min(self.len);
            let end = match len {
                Some(n) => (start + n).min(self.len),
                None => self.len,
            };
            if start >= end {
                return bytes::Bytes::new();
            }
            unsafe {
                bytes::Bytes::copy_from_slice(std::slice::from_raw_parts(
                    self.ptr.add(start),
                    end - start,
                ))
            }
        }
    }

    impl Drop for AlignedBuf {
        fn drop(&mut self) {
            unsafe {
                std::alloc::dealloc(self.ptr, self.layout);
            }
        }
    }

    unsafe impl Send for AlignedBuf {}
    unsafe impl Sync for AlignedBuf {}
}

pub struct UrmaP2pTransport {
    node_id: String,
    peer_discovery: Arc<dyn P2pPeerDiscovery>,
    context: Arc<UrmaContext>,
    connections: Arc<PeerConnectionManager>,
    registry: Arc<SegmentRegistry>,
    remote_cache: Arc<RemoteSegmentCache>,
    reader: Arc<RmReader>,
    catalog: Arc<dyn RemoteCatalog>,
    /// Catalog server task; aborted on shutdown.
    catalog_server: Mutex<Option<tokio::task::JoinHandle<()>>>,
    local_endpoint: P2pEndpoint,
    local_artifacts: Arc<Mutex<HashMap<String, LocalArtifact>>>,
    fetch_timeout: Duration,
}

impl UrmaP2pTransport {
    #[cfg(feature = "p2p-urma")]
    pub fn new(
        config: &ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<Self> {
        Self::with_ops(
            Arc::new(super::ops::RealUrmaOps::new()),
            config,
            node_id,
            peer_discovery,
        )
    }

    fn with_ops(
        ops: Arc<dyn UrmaOps>,
        config: &ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<Self> {
        let settings = config.urma.clone();
        let context = Arc::new(UrmaContext::new(ops, &settings)?);

        // Serve jetty: remote consumers import this jetty (RM, connectionless)
        // before issuing one-sided READs against published segments. The same
        // jetty also serves catalog SEND/RECV requests (M2).
        let (serve_jetty, serve_jetty_id) = context.create_jetty()?;

        let registry = Arc::new(SegmentRegistry::new(
            context.clone(),
            serve_jetty_id,
            settings.max_segments,
        ));

        let connections = Arc::new(PeerConnectionManager::new(
            context.clone(),
            settings.connect_timeout,
            settings.idle_connection_ttl,
        ));
        let remote_cache = Arc::new(RemoteSegmentCache::with_limit(
            settings.max_imported_segments,
        ));
        let reader = Arc::new(RmReader::new(
            context.clone(),
            connections.clone(),
            remote_cache.clone(),
            settings.read_chunk_bytes,
        ));

        let local_endpoint = P2pEndpoint {
            backend: URMA_BACKEND_ID.to_string(),
            address: format!(
                "{}:{}:{}",
                serve_jetty_id.eid.to_hex(),
                serve_jetty_id.uasid,
                serve_jetty_id.id
            ),
        };

        // Catalog server: resolve remote queries against the local registry.
        // Published descriptor metadata is served from `local_artifacts` so
        // remote pullers can parse `LayerMetadata` from the descriptor.
        let local_artifacts: Arc<Mutex<HashMap<String, LocalArtifact>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let resolve_registry = registry.clone();
        let resolve_node_id = node_id.clone();
        let resolve_endpoint = local_endpoint.clone();
        let resolve_artifacts = local_artifacts.clone();
        let resolve = Arc::new(move |key: &str| {
            resolve_registry.locator(key).map(|locator| {
                let metadata = resolve_artifacts
                    .lock()
                    .unwrap()
                    .get(key)
                    .map(|artifact| artifact.metadata().clone())
                    .unwrap_or(serde_json::Value::Null);
                P2pArtifactDescriptor {
                    key: key.to_string(),
                    providers: vec![P2pArtifactProvider::Peer(P2pPeer {
                        node_id: resolve_node_id.clone(),
                        endpoint: resolve_endpoint.clone(),
                    })],
                    backend_locator: Some(locator.encode()),
                    metadata,
                }
            })
        });
        let catalog_server = CatalogServer::spawn(context.clone(), serve_jetty, resolve);

        // Catalog client: SEND/RECV over the shared RTP connections.
        let catalog = Arc::new(UrmaCatalog::new(
            context.clone(),
            connections.clone(),
            config.lookup_timeout,
        ));

        info!(
            node_id = %node_id,
            endpoint = %local_endpoint.address,
            "urma artifact transport started"
        );

        Ok(Self {
            node_id,
            peer_discovery,
            context,
            connections,
            registry,
            remote_cache,
            reader,
            catalog,
            catalog_server: Mutex::new(Some(catalog_server)),
            local_endpoint,
            local_artifacts,
            fetch_timeout: config.fetch_timeout,
        })
    }

    fn local_descriptor(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        let locator = self.registry.locator(key)?;
        let metadata = self
            .local_artifacts
            .lock()
            .unwrap()
            .get(key.as_str())
            .map(|artifact| artifact.metadata().clone())
            .unwrap_or(serde_json::Value::Null);
        Some(P2pArtifactDescriptor {
            key: key.clone(),
            providers: vec![
                P2pArtifactProvider::Local,
                P2pArtifactProvider::Peer(P2pPeer {
                    node_id: self.node_id.clone(),
                    endpoint: self.local_endpoint.clone(),
                }),
            ],
            backend_locator: Some(locator.encode()),
            metadata,
        })
    }

    fn locator_from_descriptor(&self, descriptor: &P2pArtifactDescriptor) -> Result<UrmaLocator> {
        let raw = descriptor.backend_locator.as_deref().ok_or_else(|| {
            Error::InvalidDescriptor {
                reason: "missing urma backend locator".to_string(),
            }
        })?;
        UrmaLocator::decode(raw).ok_or_else(|| Error::InvalidDescriptor {
            reason: format!("malformed urma backend locator: {raw}"),
        })
    }

    /// Serve a byte range from a locally published artifact (in-memory or
    /// mmap'd reference) without going through the fabric. `None` = not
    /// published locally.
    fn local_slice(&self, key: &str, offset: u64, len: Option<usize>) -> Result<Option<Bytes>> {
        let artifacts = self.local_artifacts.lock().unwrap();
        match artifacts.get(key) {
            Some(LocalArtifact::InMemory { buf, .. }) => {
                let start = offset as usize;
                if start > buf.len() {
                    return Err(Error::InvalidDescriptor {
                        reason: "range out of bounds for local artifact".to_string(),
                    });
                }
                let end = match len {
                    Some(n) => start
                        .checked_add(n)
                        .filter(|e| *e <= buf.len())
                        .ok_or_else(|| Error::InvalidDescriptor {
                            reason: "range out of bounds for local artifact".to_string(),
                        })?,
                    None => buf.len(),
                };
                Ok(Some(buf.slice(start, Some(end - start))))
            }
            Some(LocalArtifact::Reference { map, .. }) => {
                Ok(Some(map.slice(offset, len)?))
            }
            None => Ok(None),
        }
    }

    /// Stream `[offset, offset+len)` of the remote segment described by
    /// `locator` as a sequence of at-most-`MAX_BOUNCE_WINDOW` byte chunks.
    /// The bounce buffer is registered once and reused for all windows.
    fn remote_chunk_stream(
        &self,
        locator: UrmaLocator,
        offset: u64,
        len: u64,
    ) -> P2pByteStream {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(STREAM_CHANNEL_CAPACITY);
        let context = self.context.clone();
        let reader = self.reader.clone();
        let fetch_timeout = self.fetch_timeout;
        tokio::spawn(async move {
            if len == 0 {
                return;
            }
            let window = len.min(MAX_BOUNCE_WINDOW);
            // URMA requires page-aligned buffers for register_seg.
            let layout = std::alloc::Layout::from_size_align(window as usize, 4096).unwrap();
            let bounce_addr = unsafe { std::alloc::alloc(layout) as usize };
            if bounce_addr == 0 {
                let _ = tx
                    .send(Err(Error::Internal(anyhow::anyhow!("urma: bounce alloc failed"))))
                    .await;
                return;
            }
            unsafe { std::ptr::write_bytes(bounce_addr as *mut u8, 0, window as usize) };
            let registered = match context
                .ops()
                .register_seg(context.handle(), bounce_addr, window)
            {
                Ok(r) => r,
                Err(e) => {
                    unsafe { std::alloc::dealloc(bounce_addr as *mut u8, layout) };
                    let _ = tx
                        .send(Err(Error::internal_message("urma register bounce", e)))
                        .await;
                    return;
                }
            };
            let mut remaining = len;
            let mut pos = offset;
            while remaining > 0 {
                let chunk_len = remaining.min(window);
                if let Err(err) = reader
                    .read_range(&locator, pos, chunk_len, registered.tseg, registered.ubva, fetch_timeout)
                    .await
                {
                    let _ = tx.send(Err(err)).await;
                    break;
                }
                let end = chunk_len as usize;
                let bounce_slice = unsafe { std::slice::from_raw_parts(bounce_addr as *const u8, end) };
                if tx.send(Ok(Bytes::copy_from_slice(bounce_slice))).await.is_err() {
                    break; // consumer dropped the stream
                }
                pos += chunk_len;
                remaining -= chunk_len;
            }
            let _ = context.ops().unregister_seg(registered.tseg);
            unsafe { std::alloc::dealloc(bounce_addr as *mut u8, layout) };
        });
        Box::pin(ReceiverStream::new(rx))
    }
    async fn fetch_inner(
        &self,
        descriptor: &P2pArtifactDescriptor,
        destination: &Path,
    ) -> Result<u64> {
        if let Some(bytes) = self.local_slice(&descriptor.key, 0, None)? {
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::internal_message("create fetch destination dir", e))?;
            }
            tokio::fs::write(destination, &bytes)
                .await
                .map_err(|e| Error::internal_message("export local urma artifact", e))?;
            return Ok(bytes.len() as u64);
        }

        let locator = self.locator_from_descriptor(descriptor)?;
        let total_len = locator.len;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::internal_message("create fetch destination dir", e))?;
        }
        let mut file = tokio::fs::File::create(destination)
            .await
            .map_err(|e| Error::internal_message("create fetch destination file", e))?;
        use tokio::io::AsyncWriteExt;
        let mut total = 0u64;
        let mut stream = self.remote_chunk_stream(locator, 0, total_len);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)
                .await
                .map_err(|e| Error::internal_message("write fetched chunk", e))?;
            total += chunk.len() as u64;
        }
        file.flush()
            .await
            .map_err(|e| Error::internal_message("flush fetched artifact", e))?;
        debug!(size = total, "fetched artifact via urma one-sided reads");
        Ok(total)
    }

    async fn fetch_bytes_inner(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        if let Some(bytes) = self.local_slice(&descriptor.key, 0, None)? {
            return Ok(bytes);
        }

        let locator = self.locator_from_descriptor(descriptor)?;
        let total_len = locator.len;
        let mut collected = Vec::with_capacity(total_len.min(MAX_BOUNCE_WINDOW) as usize);
        let mut stream = self.remote_chunk_stream(locator, 0, total_len);
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk?);
        }
        Ok(Bytes::from(collected))
    }

    async fn fetch_byte_range_inner(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        if let Some(bytes) = self.local_slice(&descriptor.key, offset, Some(len))? {
            return Ok(Box::pin(futures::stream::once(async move {
                Ok(bytes)
            })));
        }

        let locator = self.locator_from_descriptor(descriptor)?;
        Ok(self.remote_chunk_stream(locator, offset, len as u64))
    }

    async fn lookup_with_hints_inner(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        if let Some(descriptor) = self.local_descriptor(key) {
            debug!("urma lookup found local descriptor");
            return Ok(Some(descriptor));
        }

        // M2: query peer catalogs over URMA SEND/RECV. Hints are
        // prioritized ahead of discovery results by `peers_with_hints`.
        let peers = match self.peer_discovery.peers_with_hints(hints).await {
            Ok(peers) => peers,
            Err(err) => {
                warn!(error = %err, "urma: peer discovery failed during lookup");
                Vec::new()
            }
        };
        for peer in peers.iter().filter(|p| p.endpoint.backend == URMA_BACKEND_ID) {
            match self.catalog.query(peer, key).await {
                Ok(Some(descriptor)) => {
                    debug!(peer = %peer.node_id, "urma lookup found remote descriptor");
                    return Ok(Some(descriptor));
                }
                Ok(None) => continue,
                Err(err) => {
                    warn!(peer = %peer.node_id, error = %err, "urma: catalog query failed");
                    continue;
                }
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl P2pTransport for UrmaP2pTransport {
    #[instrument(skip(self, hints), fields(key = %key))]
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        let timer = crate::p2p::obs::OpTimer::start(URMA_BACKEND_ID, crate::p2p::obs::OP_LOOKUP);
        let result = self.lookup_with_hints_inner(key, hints).await;
        if result.is_ok() {
            timer.succeed();
        }
        result
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key, destination = %destination.display()))]
    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, destination: &Path) -> Result<u64> {
        let timer = crate::p2p::obs::OpTimer::start(URMA_BACKEND_ID, crate::p2p::obs::OP_FETCH);
        let result = self.fetch_inner(descriptor, destination).await;
        if let Ok(size) = &result {
            timer.succeed();
            crate::p2p::obs::record_fetch_bytes(URMA_BACKEND_ID, *size);
        }
        result
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key))]
    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        let timer =
            crate::p2p::obs::OpTimer::start(URMA_BACKEND_ID, crate::p2p::obs::OP_FETCH_BYTES);
        let result = self.fetch_bytes_inner(descriptor).await;
        if let Ok(bytes) = &result {
            timer.succeed();
            crate::p2p::obs::record_fetch_bytes(URMA_BACKEND_ID, bytes.len() as u64);
        }
        result
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key, offset, len))]
    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        // M4: the sample covers lookup-to-first-chunk setup; streamed bytes
        // are attributed through the fetch byte counters when consumers
        // drain via fetch/fetch_bytes.
        let timer =
            crate::p2p::obs::OpTimer::start(URMA_BACKEND_ID, crate::p2p::obs::OP_FETCH_RANGE);
        let result = self.fetch_byte_range_inner(descriptor, offset, len).await;
        if result.is_ok() {
            timer.succeed();
        }
        result
    }

    #[instrument(skip(self, request), fields(key = %request.key, source = %request.source))]
    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        // For file-path sources, always try mmap first: mmap returns a
        // page-aligned address, which satisfies URMA `register_seg`'s 4K
        // alignment requirement. This works for both Reference and Copy
        // modes because the source files persist in the image cache.
        if let P2pPublishSource::Path(path) = &request.source {
            match file_map::FileMap::map(path) {
                Ok(map) => {
                    let locator = self
                        .registry
                        .publish(&request.key, map.addr, map.len as u64)?;
                    self.local_artifacts.lock().unwrap().insert(
                        request.key.clone(),
                        LocalArtifact::Reference {
                            path: path.clone(),
                            map,
                            metadata: request.metadata.clone(),
                        },
                    );
                    self.record_published(&request.key, &locator).await;
                    return Ok(());
                }
                Err(err) => {
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "urma transport: mmap publish failed, falling back to in-memory copy"
                    );
                }
            }
        }

        // In-memory fallback: read the bytes (if Path) and copy them into
        // a 4K-aligned buffer so `register_seg` accepts the address.
        let data = match &request.source {
            P2pPublishSource::Path(path) => {
                tokio::fs::read(path)
                    .await
                    .map_err(|e| Error::internal_message("read publish source", e))?
            }
            P2pPublishSource::Bytes(bytes) => bytes.as_ref().to_vec(),
        };
        let buf = aligned_buf::AlignedBuf::new(&data);
        let addr = buf.addr();
        let len = buf.len() as u64;
        let locator = self.registry.publish(&request.key, addr, len)?;
        self.local_artifacts.lock().unwrap().insert(
            request.key.clone(),
            LocalArtifact::InMemory {
                buf,
                metadata: request.metadata.clone(),
            },
        );
        self.record_published(&request.key, &locator).await;
        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        self.local_artifacts.lock().unwrap().remove(key);
        let removed = self.registry.unregister(key);
        if removed {
            let _ = self.peer_discovery.forget_key(key).await;
        }
        Ok(removed)
    }

    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        Some(self.local_endpoint.clone())
    }

    async fn shutdown(&self) -> Result<()> {
        if let Some(server) = self.catalog_server.lock().unwrap().take() {
            server.abort();
        }
        self.connections.shutdown();
        self.remote_cache.clear(&self.context);
        let keys: Vec<String> = self
            .local_artifacts
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for key in keys {
            let _ = self.registry.unregister(&key);
        }
        self.local_artifacts.lock().unwrap().clear();
        self.context.shutdown()?;
        Ok(())
    }
}

impl UrmaP2pTransport {
    async fn record_published(&self, key: &P2pArtifactKey, locator: &UrmaLocator) {
        if let Err(err) = self.peer_discovery.record_key(key).await {
            warn!(error = %err, "urma: failed to record artifact in discovery");
        }
        debug!(locator = %locator.encode(), "urma artifact published");
    }
}

#[cfg(test)]
mod tests {
    use super::super::ops::MockUrmaOps;
    use super::*;

    fn test_config() -> crate::p2p::config::ResolvedP2pConfig {
        crate::p2p::config::ResolvedP2pConfig::from_config(&crate::cfg::P2pConfig::default())
    }

    fn transport(ops: Arc<MockUrmaOps>) -> UrmaP2pTransport {
        let mut config = test_config();
        config.transport = crate::p2p::config::P2pTransportKind::Urma;
        UrmaP2pTransport::with_ops(
            ops,
            &config,
            "node-a".to_string(),
            Arc::new(crate::p2p::discovery::NoopP2pPeerDiscovery),
        )
        .unwrap()
    }

    fn remote_descriptor(len: u64) -> P2pArtifactDescriptor {
        let locator = UrmaLocator {
            eid: "22222222222222222222222222222222".to_string(),
            uasid: 0x1000,
            jetty: 7,
            va: 0x2000,
            key: 9,
            len,
        };
        P2pArtifactDescriptor {
            key: "k1".to_string(),
            providers: vec![P2pArtifactProvider::Peer(P2pPeer {
                node_id: "node-b".to_string(),
                endpoint: P2pEndpoint {
                    backend: URMA_BACKEND_ID.to_string(),
                    address: "peer".to_string(),
                },
            })],
            backend_locator: Some(locator.encode()),
            metadata: serde_json::Value::Null,
        }
    }

    #[tokio::test]
    async fn publish_then_local_lookup_and_fetch() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops.clone());

        let payload = Bytes::from_static(b"hello urma");
        let request = P2pPublishRequest::bytes("k1", payload.clone());
        transport.publish(&request).await.unwrap();

        let key: P2pArtifactKey = "k1".to_string();
        let descriptor = transport.lookup(&key).await.unwrap().unwrap();
        let raw = descriptor.backend_locator.clone().unwrap();
        assert!(raw.starts_with(URMA_LOCATOR_PREFIX));
        assert!(UrmaLocator::decode(&raw).is_some());

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let size = transport.fetch(&descriptor, &dest).await.unwrap();
        assert_eq!(size, payload.len() as u64);
        let fetched = tokio::fs::read(&dest).await.unwrap();
        assert_eq!(fetched, payload);

        assert!(transport.unpublish(&key).await.unwrap());
        assert!(transport.lookup(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn remote_fetch_pulls_via_one_sided_reads() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops.clone());

        let descriptor = remote_descriptor(10_000);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let size = transport.fetch(&descriptor, &dest).await.unwrap();
        assert_eq!(size, 10_000);

        let status = ops.status.lock().unwrap();
        // One jetty import, one segment import, one READ (10_000 bytes is a
        // single 4MiB chunk by default).
        assert_eq!(status.bound.len(), 1);
        assert_eq!(status.segs_imported.len(), 1);
        assert_eq!(status.reads_log.len(), 1);
        assert_eq!(status.reads_log[0].len, 10_000);
    }

    #[tokio::test]
    async fn remote_fetch_bytes_and_range() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops.clone());

        let descriptor = remote_descriptor(100);
        let bytes = transport.fetch_bytes(&descriptor).await.unwrap();
        assert_eq!(bytes.len(), 100);

        let mut stream = transport
            .fetch_byte_range(&descriptor, 10, 20)
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Some(chunk) = stream.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(got.len(), 20);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.reads_log.len(), 2);
        assert_eq!(status.reads_log[1].remote_va, 0x2000 + 10);
        assert_eq!(status.reads_log[1].len, 20);
    }

    #[tokio::test]
    async fn malformed_locator_rejected() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops);

        let mut descriptor = remote_descriptor(16);
        descriptor.backend_locator = Some("urma1:!!!not-base64!!!".to_string());
        assert!(transport.fetch_bytes(&descriptor).await.is_err());
    }

    fn transport_named(ops: Arc<MockUrmaOps>, node_id: &str) -> UrmaP2pTransport {
        let mut config = test_config();
        config.transport = crate::p2p::config::P2pTransportKind::Urma;
        UrmaP2pTransport::with_ops(
            ops,
            &config,
            node_id.to_string(),
            Arc::new(crate::p2p::discovery::NoopP2pPeerDiscovery),
        )
        .unwrap()
    }

    // Ignored: two transports sharing one MockUrmaOps causes completion
    // events to be consumed by the wrong context's driver thread.
    // TODO: redesign with per-ops mock or a mock-to-mock bridge.
    #[tokio::test]
    #[ignore]
    async fn remote_catalog_lookup_between_two_nodes() {
        let ops = MockUrmaOps::new();
        let a = transport_named(ops.clone(), "node-a");
        let b = transport_named(ops.clone(), "node-b");

        let payload = Bytes::from_static(b"shared artifact bytes");
        a.publish(&P2pPublishRequest::bytes("k1", payload)).await
            .unwrap();

        // b has no local copy; a hint points at a's advertised URMA endpoint.
        let a_endpoint = a.local_endpoint().unwrap();
        assert_eq!(a_endpoint.backend, URMA_BACKEND_ID);
        let hints = vec![P2pArtifactProviderHint {
            node_id: Some("node-a".to_string()),
            endpoint: Some(a_endpoint.clone()),
        }];

        let descriptor = b
            .lookup_with_hints(&"k1".to_string(), &hints)
            .await
            .unwrap()
            .expect("remote catalog should resolve k1");
        assert_eq!(descriptor.key, "k1");
        let locator =
            UrmaLocator::decode(descriptor.backend_locator.as_deref().unwrap()).unwrap();
        // The locator advertises a's serve jetty, not b's.
        assert_eq!(locator.eid, "11111111111111111111111111111111");

        // The exchange used URMA SEND/RECV on the shared mock fabric:
        // request + response, on both sides.
        {
            let status = ops.status.lock().unwrap();
            assert!(status.sends_log.len() >= 2);
        }

        // Miss: an unknown key must come back as None (descriptor: null).
        assert!(
            b.lookup_with_hints(&"missing".to_string(), &hints)
                .await
                .unwrap()
                .is_none()
        );
    }

    // Ignored: same shared-mock completion routing issue as above.
    #[tokio::test]
    #[ignore]
    async fn remote_catalog_query_failure_tolerated() {
        let ops = MockUrmaOps::new();
        let a = transport_named(ops.clone(), "node-a");
        let b = transport_named(ops.clone(), "node-b");
        a.publish(&P2pPublishRequest::bytes("k1", Bytes::from_static(b"x")))
            .await
            .unwrap();

        // Simulate a fabric-level SEND failure: lookup must surface an
        // error-free None rather than hanging or panicking.
        ops.status.lock().unwrap().send_failure = -1;
        let hints = vec![P2pArtifactProviderHint {
            node_id: Some("node-a".to_string()),
            endpoint: Some(a.local_endpoint().unwrap()),
        }];
        let result = b.lookup_with_hints(&"k1".to_string(), &hints).await;
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn reference_publish_serves_bytes_locally() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops.clone());

        let payload: Vec<u8> = b"zero-copy reference payload ".repeat(64);
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("artifact.bin");
        tokio::fs::write(&src, &payload).await.unwrap();

        let mut request = P2pPublishRequest::file("k1", &src);
        request.publish_mode = P2pPublishMode::Reference;
        transport.publish(&request).await.unwrap();

        // The local fast path must serve the exact bytes (mmap mapping on
        // unix; in-memory fallback where mmap is unavailable).
        let descriptor = transport.lookup(&"k1".to_string()).await.unwrap().unwrap();
        let fetched = transport.fetch_bytes(&descriptor).await.unwrap();
        assert_eq!(fetched.to_vec(), payload);

        // Range reads take the same local path.
        let mut stream = transport
            .fetch_byte_range(&descriptor, 10, 20)
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Some(chunk) = stream.next().await {
            got.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(got, &payload[10..30]);
    }

    #[tokio::test]
    async fn unpublish_nonexistent_key_returns_false() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops);

        let result = transport.unpublish(&"ghost".to_string()).await.unwrap();
        assert!(!result, "unpublishing a never-published key must return false");
    }

    #[tokio::test]
    async fn publish_unregister_republish_roundtrip() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops.clone());

        let payload = Bytes::from_static(b"first version");
        transport.publish(&P2pPublishRequest::bytes("k1", payload.clone())).await.unwrap();
        assert!(transport.lookup(&"k1".to_string()).await.unwrap().is_some());

        // Unpublish removes the artifact.
        assert!(transport.unpublish(&"k1".to_string()).await.unwrap());
        assert!(transport.lookup(&"k1".to_string()).await.unwrap().is_none());

        // Republish with different content succeeds.
        let payload2 = Bytes::from_static(b"second version");
        transport.publish(&P2pPublishRequest::bytes("k1", payload2.clone())).await.unwrap();
        let descriptor = transport.lookup(&"k1".to_string()).await.unwrap().unwrap();
        let fetched = transport.fetch_bytes(&descriptor).await.unwrap();
        assert_eq!(fetched, payload2);
    }

    #[tokio::test]
    async fn fetch_byte_range_full_equivalent_to_fetch_bytes() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops);

        let descriptor = remote_descriptor(256);
        let full = transport.fetch_bytes(&descriptor).await.unwrap();

        let mut stream = transport
            .fetch_byte_range(&descriptor, 0, 256)
            .await
            .unwrap();
        let mut ranged = Vec::new();
        while let Some(chunk) = stream.next().await {
            ranged.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(full.to_vec(), ranged);
    }

    #[tokio::test]
    async fn multiple_artifacts_published_simultaneously() {
        let ops = MockUrmaOps::new();
        let transport = transport(ops);

        for i in 0..5u8 {
            let data = vec![i; 64];
            transport
                .publish(&P2pPublishRequest::bytes(
                    format!("k{i}"),
                    Bytes::from(data),
                ))
                .await
                .unwrap();
        }

        // All 5 artifacts are visible via lookup.
        for i in 0..5u8 {
            let key = format!("k{i}");
            let descriptor = transport.lookup(&key).await.unwrap().expect(&key);
            let bytes = transport.fetch_bytes(&descriptor).await.unwrap();
            assert_eq!(bytes.len(), 64);
            assert!(bytes.iter().all(|&b| b == i));
        }

        // Unpublish even-numbered keys only.
        for i in (0..5u8).step_by(2) {
            assert!(transport.unpublish(&format!("k{i}")).await.unwrap());
        }
        for i in 0..5u8 {
            let key = format!("k{i}");
            let exists = transport.lookup(&key).await.unwrap().is_some();
            assert_eq!(exists, i % 2 != 0, "only odd keys should remain");
        }
    }
}
