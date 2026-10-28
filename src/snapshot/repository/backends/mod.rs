pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;
pub(crate) mod sharedfs;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{
    AppConfig, ConfigManager, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind,
};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
pub use oss::OssBackend;
pub use posixfs::{PosixFsBackend, PosixFsBackendConfig};
use sharedfs::{
    LayerStorage, SharedFsRuntimeResolver, SharedFsSnapshotRepository, SharedFsStorage,
};

/// Backend components (repository + its layer-store adapter + runtime resolver).
type BackendParts = (
    Arc<dyn SnapshotRepository>,
    Arc<dyn LayerStorage>,
    Arc<dyn SnapshotRuntimeResolver>,
);

/// Builds the optional node-shared-filesystem-backed snapshot store.
pub(crate) fn build_shared_fs_backend(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<
    Option<(
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    )>,
> {
    let config = ConfigManager::global_config();
    if !config.snapshot.shared_fs.enabled {
        return Ok(None);
    }
    let (repository, layers, runtime_resolver) = build_backend_parts(config, p2p_transport)?;
    let storage = make_shared_fs_storage(config)?;
    Ok(Some((
        Arc::new(SharedFsSnapshotRepository::new(
            repository,
            Arc::clone(&storage),
            layers.clone(),
        )),
        Arc::new(SharedFsRuntimeResolver::new(
            runtime_resolver,
            storage,
            layers,
        )),
    )))
}

/// Builds the configured snapshot repository backend and its matching runtime resolver from the global configuration.
pub fn build_snapshot_backend(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    let (repository, _, runtime_resolver) =
        build_backend_parts(ConfigManager::global_config(), p2p_transport)?;
    Ok((repository, runtime_resolver))
}

fn build_backend_parts(
    config: &AppConfig,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<BackendParts> {
    let shared_cache_root = shared_runtime_cache_root();
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            let cache = LocalArtifactCache::new(shared_cache_root.clone(), None)?;
            Ok(PosixFsBackend::from_parts(
                PosixFsBackendConfig {
                    root,
                    cache_root: Some(shared_cache_root.clone()),
                    runtime_cache_root: Some(shared_cache_root.join("runtime")),
                },
                overlaybd_layers,
                cache,
            )
            .into_parts_with_layers())
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
            let cache =
                LocalArtifactCache::new(shared_cache_root.clone(), oss_config.cache_max_size_gb)?;
            Ok(OssBackend::from_parts(
                oss_config,
                snapshot_image_storage,
                cache,
                shared_cache_root.join("runtime"),
                overlaybd_layers,
                p2p_transport,
            )?
            .into_parts_with_layers())
        }
    }
}

fn make_shared_fs_storage(config: &AppConfig) -> anyhow::Result<Arc<SharedFsStorage>> {
    let root = config.snapshot.shared_fs.root.clone();
    if root.as_os_str().is_empty() {
        anyhow::bail!("snapshot.shared_fs.root is not configured");
    }
    Ok(Arc::new(SharedFsStorage::new(root)))
}

pub(crate) fn shared_runtime_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_cache_path
        .clone()
}
