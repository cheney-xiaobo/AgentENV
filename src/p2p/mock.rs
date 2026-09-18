//! Test-only mock transports for unit tests.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::error::{Error, Result};
use super::transport::{P2pByteStream, P2pTransport};
use super::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProviderHint, P2pPublishRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// MockTransport — full-featured mock used across image-cache, overlaybd,
// snapshot-manager and composite tests.
// ---------------------------------------------------------------------------

/// Test mock that records calls and serves pre-loaded descriptors/blobs.
pub struct MockTransport {
    /// Pre-loaded artifact descriptors.
    pub descriptors: RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>,
    /// Pre-loaded artifact bytes.
    pub blobs: RwLock<HashMap<P2pArtifactKey, Bytes>>,
    /// When true, `publish` returns an error.
    pub fail_publish: AtomicBool,
    /// When true, `lookup_with_hints` returns an error.
    pub fail_lookup: AtomicBool,
    /// When true, `fetch_byte_range` returns an error after the first chunk.
    pub fail_fetch_range_stream_after_first_chunk: AtomicBool,
    /// Number of `lookup` / `lookup_with_hints` calls.
    pub lookup_count: AtomicUsize,
    /// Number of `fetch` / `fetch_bytes` calls.
    pub fetch_count: AtomicUsize,
    /// Number of `fetch_byte_range` calls.
    pub fetch_range_count: AtomicUsize,
    /// Number of `publish` calls.
    pub publish_count: AtomicUsize,
    /// Keys passed to `unpublish`, in order.
    pub unpublished_keys: RwLock<Vec<P2pArtifactKey>>,
    /// Artificial delay before `lookup_with_hints` returns.
    pub lookup_delay: Option<Duration>,
    /// Artificial delay before `fetch_byte_range` returns.
    pub fetch_range_delay: Option<Duration>,
}

impl Default for MockTransport {
    fn default() -> Self {
        Self {
            descriptors: RwLock::new(HashMap::new()),
            blobs: RwLock::new(HashMap::new()),
            fail_publish: AtomicBool::new(false),
            fail_lookup: AtomicBool::new(false),
            fail_fetch_range_stream_after_first_chunk: AtomicBool::new(false),
            lookup_count: AtomicUsize::new(0),
            fetch_count: AtomicUsize::new(0),
            fetch_range_count: AtomicUsize::new(0),
            publish_count: AtomicUsize::new(0),
            unpublished_keys: RwLock::new(Vec::new()),
            lookup_delay: None,
            fetch_range_delay: None,
        }
    }
}

#[async_trait]
impl P2pTransport for MockTransport {
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        _hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        self.lookup_count.fetch_add(1, Ordering::Relaxed);
        if self.fail_lookup.load(Ordering::Relaxed) {
            return Err(Error::internal_message("mock", "lookup disabled"));
        }
        if let Some(d) = self.lookup_delay {
            tokio::time::sleep(d).await;
        }
        let descriptors = self.descriptors.read().await;
        match descriptors.get(key) {
            Some(desc) => Ok(Some(desc.clone())),
            None => Ok(None),
        }
    }

    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, destination: &Path) -> Result<u64> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let blobs = self.blobs.read().await;
        let data = blobs
            .get(&descriptor.key)
            .ok_or_else(|| Error::internal_message("mock", "key not found"))?;
        tokio::fs::write(destination, data.as_ref())
            .await
            .map_err(|e| Error::internal_message("mock write", e))?;
        Ok(data.len() as u64)
    }

    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let blobs = self.blobs.read().await;
        blobs
            .get(&descriptor.key)
            .cloned()
            .ok_or_else(|| Error::internal_message("mock", "key not found"))
    }

    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        self.fetch_range_count.fetch_add(1, Ordering::Relaxed);
        if let Some(d) = self.fetch_range_delay {
            tokio::time::sleep(d).await;
        }
        let blobs = self.blobs.read().await;
        let data = blobs
            .get(&descriptor.key)
            .ok_or_else(|| Error::internal_message("mock", "key not found"))?
            .clone();
        drop(blobs);
        let start = offset as usize;
        let end = start + len;
        let slice = data.slice(start..end.min(data.len()));
        Ok(Box::pin(futures::stream::once(async move { Ok(slice) })))
    }

    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        self.publish_count.fetch_add(1, Ordering::Relaxed);
        if self.fail_publish.load(Ordering::Relaxed) {
            return Err(Error::internal_message("mock", "publish disabled"));
        }
        let _ = request;
        Ok(())
    }

    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        self.unpublished_keys.write().await.push(key.clone());
        Ok(false)
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeP2pTransport — lightweight fake for composite-backend tests only.
// ---------------------------------------------------------------------------

/// Record of a publish call for assertions in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedPublish {
    pub key: String,
}

/// A trivial fake transport for composite-backend tests.
///
/// - `with_bytes`: stores one artifact and serves it from `fetch_bytes`.
/// - `failing`: every operation returns an error.
/// - `empty`: succeeds on publish (recording it) but returns `None` on lookup.
#[derive(Clone)]
pub struct FakeP2pTransport {
    inner: Arc<FakeInner>,
}

struct FakeInner {
    artifacts: std::sync::Mutex<HashMap<String, Bytes>>,
    always_fail: bool,
    publishes: std::sync::Mutex<Vec<RecordedPublish>>,
}

impl FakeP2pTransport {
    pub fn with_bytes(key: &str, data: &[u8]) -> Self {
        let mut artifacts = HashMap::new();
        artifacts.insert(key.to_string(), Bytes::copy_from_slice(data));
        Self {
            inner: Arc::new(FakeInner {
                artifacts: std::sync::Mutex::new(artifacts),
                always_fail: false,
                publishes: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn failing(_name: &str) -> Self {
        Self {
            inner: Arc::new(FakeInner {
                artifacts: std::sync::Mutex::new(HashMap::new()),
                always_fail: true,
                publishes: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(FakeInner {
                artifacts: std::sync::Mutex::new(HashMap::new()),
                always_fail: false,
                publishes: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn recorded_publishes(&self) -> Vec<RecordedPublish> {
        self.inner.publishes.lock().unwrap().clone()
    }

    fn fail(&self) -> Result<()> {
        if self.inner.always_fail {
            Err(Error::internal_message("fake", "always-fail"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl P2pTransport for FakeP2pTransport {
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        _hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        self.fail()?;
        let artifacts = self.inner.artifacts.lock().unwrap();
        if artifacts.contains_key(key) {
            Ok(Some(P2pArtifactDescriptor {
                key: key.clone(),
                providers: Vec::new(),
                backend_locator: None,
                metadata: serde_json::Value::Null,
            }))
        } else {
            Ok(None)
        }
    }

    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, _destination: &Path) -> Result<u64> {
        self.fail()?;
        let artifacts = self.inner.artifacts.lock().unwrap();
        match artifacts.get(&descriptor.key) {
            Some(b) => Ok(b.len() as u64),
            None => Err(Error::internal_message("fake", "key not found")),
        }
    }

    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        self.fail()?;
        let artifacts = self.inner.artifacts.lock().unwrap();
        match artifacts.get(&descriptor.key) {
            Some(b) => Ok(b.clone()),
            None => Err(Error::internal_message("fake", "key not found")),
        }
    }

    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        self.fail()?;
        let artifacts = self.inner.artifacts.lock().unwrap();
        let data = artifacts
            .get(&descriptor.key)
            .ok_or_else(|| Error::internal_message("fake", "key not found"))?
            .clone();
        drop(artifacts);
        let start = offset as usize;
        let end = start + len;
        let slice = data.slice(start..end.min(data.len()));
        Ok(Box::pin(futures::stream::once(async move { Ok(slice) })))
    }

    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        self.fail()?;
        self.inner
            .publishes
            .lock()
            .unwrap()
            .push(RecordedPublish {
                key: request.key.clone(),
            });
        Ok(())
    }

    async fn unpublish(&self, _key: &P2pArtifactKey) -> Result<bool> {
        self.fail()?;
        Ok(false)
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
