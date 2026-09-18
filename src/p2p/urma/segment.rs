//! Segment management: publish-side registry, wire locator codec and
//! import-side remote segment cache.
//!
//! A published artifact is registered as a URMA segment covering its bytes
//! (file-backed via mmap or in-memory). The locator advertised in the
//! catalog encodes everything a peer needs to import the segment and issue
//! one-sided READs: home EID, segment UBVA, access key, length.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use super::context::UrmaContext;
use super::ops::{Eid, ImportedSeg, JettyId, RemoteSegInfo, TargetSegHandle};
use crate::p2p::error::{Error as P2pError, Result as P2pResult};

/// Prefix identifying URMA locators in `backend_locator` strings.
pub const URMA_LOCATOR_PREFIX: &str = "urma1:";

/// Wire format of the URMA backend locator stored in
/// `P2pArtifactDescriptor::backend_locator`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UrmaLocator {
    /// Segment home node EID (hex).
    pub eid: String,
    /// Home node uasid (part of the fabric jetty/segment identity).
    pub uasid: u32,
    /// Jetty id on the home node serving one-sided READs.
    pub jetty: u32,
    /// Segment base UBVA on the home node.
    pub va: u64,
    /// Access token for one-sided READ.
    pub key: u32,
    /// Segment length in bytes.
    pub len: u64,
}

impl UrmaLocator {
    pub fn encode(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        format!("{URMA_LOCATOR_PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
    }

    pub fn decode(value: &str) -> Option<Self> {
        let payload = value.strip_prefix(URMA_LOCATOR_PREFIX)?;
        let json = URL_SAFE_NO_PAD.decode(payload).ok()?;
        serde_json::from_slice(&json).ok()
    }
}

/// A locally published artifact backed by a registered URMA segment.
pub struct PublishedSegment {
    pub key: String,
    pub locator: UrmaLocator,
    /// Handle of the registered local segment (for unregister on evict).
    pub tseg: TargetSegHandle,
    /// Number of bytes covered (also used for fetch-side validation).
    pub len: u64,
}

/// LRU bookkeeping for a cached entry (publish-side segments and
/// import-side remote segments share the mechanism).
struct LruSlot<T> {
    value: T,
    last_used: u64,
}

/// Monotonic clock feeding `LruSlot::last_used` stamps.
#[derive(Default)]
struct LruClock(AtomicU64);

impl LruClock {
    fn tick(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// Evict the least-recently-used entry from `map` and return it.
fn evict_lru<K: std::hash::Hash + Eq + Clone, T>(
    map: &mut HashMap<K, LruSlot<T>>,
) -> Option<T> {
    let victim = map
        .iter()
        .min_by_key(|(_, slot)| slot.last_used)
        .map(|(k, _)| k.clone())?;
    map.remove(&victim).map(|slot| slot.value)
}

/// Publish-side registry of local segments with LRU eviction at capacity.
pub struct SegmentRegistry {
    context: Arc<UrmaContext>,
    /// Jetty (id) on this node that consumers target for one-sided READs.
    /// RM is connectionless, so a single serve jetty serves every consumer.
    serve_jetty_id: JettyId,
    max_segments: usize,
    segments: Mutex<HashMap<String, LruSlot<PublishedSegment>>>,
    clock: LruClock,
}

impl SegmentRegistry {
    pub fn new(context: Arc<UrmaContext>, serve_jetty_id: JettyId, max_segments: usize) -> Self {
        Self {
            context,
            serve_jetty_id,
            max_segments: max_segments.max(1),
            segments: Mutex::new(HashMap::new()),
            clock: LruClock::default(),
        }
    }

    /// Register `[addr, addr+len)` as a published segment under `key` and
    /// return the wire locator to advertise.
    pub fn publish(
        &self,
        key: &str,
        addr: usize,
        len: u64,
    ) -> P2pResult<UrmaLocator> {
        if len == 0 {
            return Err(P2pError::Internal(anyhow::anyhow!(
                "urma: refusing to publish empty segment for {key}"
            )));
        }
        {
            let mut segments = self.segments.lock().unwrap();
            if let Some(existing) = segments.get_mut(key) {
                existing.last_used = self.clock.tick();
                return Ok(existing.value.locator.clone());
            }
        }

        let registered = self
            .context
            .ops()
            .register_seg(self.context.handle(), addr, len)
            .map_err(|e| P2pError::internal_message("urma register_seg", e))?;
        let serve = self.serve_jetty_id;

        let locator = UrmaLocator {
            eid: serve.eid.to_hex(),
            uasid: serve.uasid,
            jetty: serve.id,
            va: registered.ubva,
            key: registered.key,
            len,
        };
        let entry = PublishedSegment {
            key: key.to_string(),
            locator: locator.clone(),
            tseg: registered.tseg,
            len,
        };

        let mut segments = self.segments.lock().unwrap();
        // LRU eviction: drop the least recently used published segment when
        // at capacity. (The transport's `local_artifacts` entry keeps the
        // backing memory alive; only the fabric-visible segment is dropped.)
        while segments.len() >= self.max_segments {
            match segments
                .iter()
                .min_by_key(|(_, slot)| slot.last_used)
                .map(|(k, _)| k.clone())
            {
                Some(victim_key) => {
                    if let Some(victim) = segments.remove(&victim_key) {
                        let _ = self.context.ops().unregister_seg(victim.value.tseg);
                    }
                }
                None => break,
            }
        }
        segments.insert(
            key.to_string(),
            LruSlot {
                value: entry,
                last_used: self.clock.tick(),
            },
        );
        Ok(locator)
    }

    /// Look up the locator advertised for a locally published key (and
    /// refresh the entry's LRU position).
    pub fn locator(&self, key: &str) -> Option<UrmaLocator> {
        let mut segments = self.segments.lock().unwrap();
        let slot = segments.get_mut(key)?;
        slot.last_used = self.clock.tick();
        Some(slot.value.locator.clone())
    }

    /// Take ownership of a published segment (used by LRU-aware callers
    /// that must also drop the backing memory).
    fn take(&self, key: &str) -> Option<PublishedSegment> {
        self.segments
            .lock()
            .unwrap()
            .remove(key)
            .map(|slot| slot.value)
    }

    pub fn unregister(&self, key: &str) -> bool {
        match self.take(key) {
            Some(entry) => {
                let _ = self.context.ops().unregister_seg(entry.tseg);
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.segments.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.lock().unwrap().is_empty()
    }
}

/// Import-side cache of remote segments with an LRU cap. `urma_import_seg`
/// results (which carry the local mapping address used to address READs)
/// are cached per (EID, uasid, VA, key) so repeated range fetches from
/// the same artifact do not re-import; when the cache exceeds
/// `max_imported` the least recently used import is unimported.
pub struct RemoteSegmentCache {
    imported: Mutex<HashMap<RemoteSegInfo, LruSlot<ImportedSeg>>>,
    max_imported: usize,
    clock: LruClock,
}

/// Default cap on simultaneously imported remote segments.
pub const DEFAULT_MAX_IMPORTED_SEGMENTS: usize = 1024;

impl RemoteSegmentCache {
    /// Unlimited cache (kept for existing callers/tests).
    pub fn new() -> Self {
        Self::with_limit(usize::MAX)
    }

    /// LRU-bounded cache; `max_imported` is clamped to at least 1.
    pub fn with_limit(max_imported: usize) -> Self {
        Self {
            imported: Mutex::new(HashMap::new()),
            max_imported: max_imported.max(1),
            clock: LruClock::default(),
        }
    }

    /// Return the imported handle for the remote segment, importing it on
    /// first use. Hits refresh the entry's LRU position; inserts past the
    /// cap evict the least recently used import (unimporting its segment).
    pub fn get_or_import(
        &self,
        context: &UrmaContext,
        remote: &RemoteSegInfo,
    ) -> P2pResult<ImportedSeg> {
        {
            let mut imported = self.imported.lock().unwrap();
            if let Some(slot) = imported.get_mut(remote) {
                slot.last_used = self.clock.tick();
                return Ok(slot.value);
            }
        }
        let seg = context
            .ops()
            .import_seg(context.handle(), remote)
            .map_err(|e| P2pError::internal_message("urma import_seg", e))?;
        let mut imported = self.imported.lock().unwrap();
        while imported.len() >= self.max_imported {
            match evict_lru(&mut imported) {
                Some(victim) => {
                    let _ = context.ops().unimport_seg(victim.tseg);
                }
                None => break,
            }
        }
        imported.insert(
            *remote,
            LruSlot {
                value: seg,
                last_used: self.clock.tick(),
            },
        );
        Ok(seg)
    }

    /// Release all imported segments.
    pub fn clear(&self, context: &UrmaContext) {
        let drained: Vec<ImportedSeg> = {
            let mut imported = self.imported.lock().unwrap();
            imported.drain().map(|(_, slot)| slot.value).collect()
        };
        for seg in drained {
            let _ = context.ops().unimport_seg(seg.tseg);
        }
    }

    pub fn len(&self) -> usize {
        self.imported.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.imported.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ops::{Eid, JettyId, MockUrmaOps};
    use super::*;

    fn remote_seg() -> RemoteSegInfo {
        RemoteSegInfo {
            eid: Eid([0x22; 16]),
            uasid: 0x1000,
            va: 0x1000,
            key: 42,
            len: 4096,
        }
    }

    fn serve_id() -> JettyId {
        JettyId {
            eid: Eid([0x33; 16]),
            uasid: 0x2000,
            id: 9,
        }
    }

    #[test]
    fn locator_roundtrip() {
        let locator = UrmaLocator {
            eid: "1111".to_string(),
            uasid: 0x1000,
            jetty: 5,
            va: 0xdead_beef,
            key: 7,
            len: 1 << 20,
        };
        let encoded = locator.encode();
        assert!(encoded.starts_with("urma1:"));
        assert_eq!(UrmaLocator::decode(&encoded), Some(locator));
        assert!(UrmaLocator::decode("iroh:garbage").is_none());
    }

    #[tokio::test]
    async fn publish_registers_and_caches() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let registry = SegmentRegistry::new(context, serve_id(), 8);

        let buf = vec![0u8; 4096];
        let l1 = registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();
        // Second publish of same key reuses the cached entry.
        let l2 = registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();
        assert_eq!(l1, l2);
        assert_eq!(registry.len(), 1);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.registered.len(), 1);
    }

    #[tokio::test]
    async fn unpublish_unregisters() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let registry = SegmentRegistry::new(context, serve_id(), 8);

        let buf = vec![0u8; 4096];
        registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();
        assert!(registry.unregister("k1"));
        assert!(!registry.unregister("k1"));
        assert_eq!(registry.len(), 0);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.unregistered, 1);
    }

    #[tokio::test]
    async fn remote_cache_imports_once() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let cache = RemoteSegmentCache::new();

        let h1 = cache.get_or_import(&context, &remote_seg()).unwrap();
        let h2 = cache.get_or_import(&context, &remote_seg()).unwrap();
        assert_eq!(h1, h2);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.segs_imported.len(), 1);
    }

    #[tokio::test]
    async fn publish_side_lru_evicts_least_recently_used() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let registry = SegmentRegistry::new(context, serve_id(), 2);

        let buf = vec![0u8; 4096];
        registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();
        registry.publish("k2", buf.as_ptr() as usize, 4096).unwrap();
        // Touch k1 so k2 becomes the LRU victim.
        assert!(registry.locator("k1").is_some());
        registry.publish("k3", buf.as_ptr() as usize, 4096).unwrap();

        assert!(registry.locator("k1").is_some());
        assert!(registry.locator("k2").is_none(), "k2 must be LRU-evicted");
        assert!(registry.locator("k3").is_some());
        assert_eq!(registry.len(), 2);
    }

    #[tokio::test]
    async fn import_side_lru_unimports_at_cap() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let cache = RemoteSegmentCache::with_limit(2);

        let mk = |key: u32| RemoteSegInfo {
            eid: Eid([0x22; 16]),
            uasid: 0x1000,
            va: 0x1000 + (key as u64) * 0x1000,
            key,
            len: 4096,
        };
        let s1 = mk(1);
        let s2 = mk(2);
        let s3 = mk(3);

        cache.get_or_import(&context, &s1).unwrap();
        cache.get_or_import(&context, &s2).unwrap();
        // Touch s1 so s2 is the LRU victim once s3 arrives.
        cache.get_or_import(&context, &s1).unwrap();
        cache.get_or_import(&context, &s3).unwrap();

        assert_eq!(cache.len(), 2);
        let status = ops.status.lock().unwrap();
        assert_eq!(status.segs_imported.len(), 3);
        assert_eq!(status.unimported_segs, 1, "one import must be LRU-unimported");
    }

    #[tokio::test]
    async fn publish_unregister_republish_same_key() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let registry = SegmentRegistry::new(context, serve_id(), 8);

        let buf = vec![1u8; 4096];
        let l1 = registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();

        // Unregister then republish: gets a fresh segment (new VA).
        assert!(registry.unregister("k1"));
        assert!(registry.locator("k1").is_none());

        let buf2 = vec![2u8; 4096];
        let l2 = registry.publish("k1", buf2.as_ptr() as usize, 4096).unwrap();
        // The two locators should differ because the second publish allocates
        // a new VA from the mock.
        assert_ne!(l1.va, l2.va, "republish must allocate a new segment");
        assert_eq!(registry.len(), 1);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.registered.len(), 2);
        assert_eq!(status.unregistered, 1);
    }

    #[tokio::test]
    async fn publish_at_capacity_evicts_lru() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        // Cap of 1 segment: second publish must evict the first.
        let registry = SegmentRegistry::new(context, serve_id(), 1);

        let buf = vec![0u8; 4096];
        registry.publish("k1", buf.as_ptr() as usize, 4096).unwrap();
        assert_eq!(registry.len(), 1);

        registry.publish("k2", buf.as_ptr() as usize, 4096).unwrap();
        assert_eq!(registry.len(), 1, "k1 must be evicted");
        assert!(registry.locator("k1").is_none(), "k1 evicted");
        assert!(registry.locator("k2").is_some(), "k2 remains");
    }

    #[tokio::test]
    async fn remote_cache_clear_unimports_all() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let cache = RemoteSegmentCache::with_limit(10);

        let mk = |key: u32| RemoteSegInfo {
            eid: Eid([0x22; 16]),
            uasid: 0x1000,
            va: 0x1000 + (key as u64) * 0x1000,
            key,
            len: 4096,
        };
        cache.get_or_import(&context, &mk(1)).unwrap();
        cache.get_or_import(&context, &mk(2)).unwrap();
        cache.get_or_import(&context, &mk(3)).unwrap();
        assert_eq!(cache.len(), 3);

        cache.clear(&context);
        assert_eq!(cache.len(), 0);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.segs_imported.len(), 3);
        assert_eq!(status.unimported_segs, 3, "clear must unimport all");
    }

    #[tokio::test]
    async fn locator_decode_rejects_truncated_and_corrupt() {
        // Prefix only — missing payload.
        assert!(UrmaLocator::decode("urma1:").is_none());
        // Wrong prefix.
        assert!(UrmaLocator::decode("iroh:abc").is_none());
        // Valid prefix but corrupt base64.
        assert!(UrmaLocator::decode("urma1:!!!").is_none());
    }
}
