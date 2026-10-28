//! Backend-agnostic node-shared filesystem storage for snapshots.
//!
//! A node-shared filesystem can host snapshot-generated content for **any**
//! snapshot backend without touching that backend's source:
//!
//! - the [`SharedFsSnapshotRepository`] decorator wraps any backend repository,
//!   routing records, fixed artifacts and delta layers onto the shared
//!   filesystem while delegating base-layer storage to the backend's
//!   [`LayerStorage`] implementation;
//! - the [`SharedFsRuntimeResolver`] decorator wraps any backend resolver,
//!   reading fixed artifacts from the shared filesystem and materializing
//!   image configs with delta layers as direct file paths.
//!
//! Backend code never references the shared filesystem; adding a new backend
//! only requires implementing [`LayerStorage`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use overlaybd::config::{
    load_image_config as load_overlaybd_image_config, ImageConfig, LayerConfig,
};

use crate::image::local_layer::rootfs_layer_is_runtime_generated_delta;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::artifact_cache::CacheArtifactLease;
use crate::snapshot::repository::backends::posixfs::PosixFsCatalogStore;
use crate::snapshot::repository::errors::{RepositoryError, RepositoryResult};
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::types::{
    CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    ResolvedAttachedDrive, RunnableSnapshot, SharedFSPathLayer, SnapshotId,
    SnapshotPublishMetadata, SnapshotRecord, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Relative directory under `{root}/snapshots/` holding snapshot fixed
/// artifacts and delta layers.
pub(crate) const SHARED_FS_ARTIFACTS_DIR: &str = "snapshots";
pub(crate) const SHARED_FS_LAYERS_DIR: &str = "layers";

/// Node-shared filesystem storage for snapshot-generated content.
///
/// Layout (`{root}` is the configured `[snapshot.shared_fs].root`):
///
/// ```text
/// {root}/
/// ├── catalog/records/{id}.json
/// ├── catalog/aliases/{alias}
/// └── snapshots/{id}/
///     ├── vm_state.bin
///     ├── firecracker-manifest.json
///     ├── memory-startup.pack   (optional)
///     └── layers/               (delta layers)
/// ```
pub(crate) struct SharedFsStorage {
    root: PathBuf,
    catalog: Arc<PosixFsCatalogStore>,
}

impl SharedFsStorage {
    pub(crate) fn new(root: PathBuf) -> Self {
        let catalog = Arc::new(PosixFsCatalogStore::new(root.clone()));
        Self { root, catalog }
    }

    #[allow(dead_code)]
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn catalog(&self) -> &PosixFsCatalogStore {
        &self.catalog
    }

    pub(crate) fn snapshot_dir(&self, id: &SnapshotId) -> PathBuf {
        self.root.join(SHARED_FS_ARTIFACTS_DIR).join(id.to_string())
    }

    pub(crate) fn layers_dir(&self, id: &SnapshotId) -> PathBuf {
        self.snapshot_dir(id).join(SHARED_FS_LAYERS_DIR)
    }

    pub(crate) fn store_fixed_artifact(
        &self,
        id: &SnapshotId,
        relative_name: &str,
        bytes: &[u8],
    ) -> RepositoryResult<PathBuf> {
        let dest = self.snapshot_dir(id).join(relative_name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create shared-fs snapshot dir '{}'", parent.display()),
                    error,
                )
            })?;
        }
        std::fs::write(&dest, bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write shared-fs artifact '{}'", dest.display()),
                error,
            )
        })?;
        Ok(dest)
    }

    pub(crate) fn read_fixed_artifact(
        &self,
        id: &SnapshotId,
        relative_name: &str,
    ) -> RepositoryResult<Vec<u8>> {
        let path = self.snapshot_dir(id).join(relative_name);
        std::fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                RepositoryError::ArtifactNotFound {
                    artifact: format!("shared-fs artifact '{}'", path.display()),
                }
            } else {
                RepositoryError::backend(
                    format!("read shared-fs artifact '{}'", path.display()),
                    error,
                )
            }
        })
    }

    pub(crate) fn cleanup_snapshot_dir(&self, id: &SnapshotId) {
        let _ = std::fs::remove_dir_all(self.snapshot_dir(id));
    }

    /// Copies a delta layer file into the shared filesystem and returns a
    /// [`SharedFSPathLayer`] reference.
    pub(crate) fn store_delta_layer(
        &self,
        id: &SnapshotId,
        source: &Path,
        digest: &str,
        size: u64,
        uuid: Option<String>,
    ) -> RepositoryResult<SharedFSPathLayer> {
        let dir = self.layers_dir(id);
        std::fs::create_dir_all(&dir).map_err(|error| {
            RepositoryError::backend(
                format!("create shared-fs layers dir '{}'", dir.display()),
                error,
            )
        })?;
        let file_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| RepositoryError::InvalidRequest {
                reason: format!("delta layer has no file name: '{}'", source.display()),
            })?;
        let destination = dir.join(file_name);
        if !destination.exists() {
            std::fs::copy(source, &destination).map_err(|error| {
                RepositoryError::backend(
                    format!(
                        "copy delta layer '{}' -> '{}'",
                        source.display(),
                        destination.display()
                    ),
                    error,
                )
            })?;
        }
        Ok(SharedFSPathLayer {
            path: destination.display().to_string(),
            digest: digest.to_string(),
            size,
            uuid,
        })
    }

    pub(crate) fn layer_config(layer: &SharedFSPathLayer) -> LayerConfig {
        LayerConfig {
            file: layer.path.clone(),
            digest: layer.digest.clone(),
            size: layer.size,
            uuid: layer.uuid.clone().unwrap_or_default(),
            ..Default::default()
        }
    }
}

/// Backend-specific storage for **base** layers.
///
/// Each snapshot backend implements this to store a non-delta layer the way it
/// natively does (e.g. upload to OSS managed-layers, hard-link into a local
/// managed-layer store). The shared-fs decorator only ever calls these two
/// methods; delta layers are always handled by the shared filesystem, and
/// registry-referenced (`External`) layers are handled by the shared module.
#[async_trait]
pub(crate) trait LayerStorage: Send + Sync {
    /// Stores a local, descriptor-bearing disk layer as the backend's managed layer.
    async fn import_disk_base_layer(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
    ) -> RepositoryResult<ManagedLayer>;

    /// Stores a local, descriptor-bearing memory layer as the backend's managed layer.
    async fn import_memory_base_layer(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
    ) -> RepositoryResult<ManagedLayer>;

    /// Materializes the runtime `LayerConfig` for a base Managed layer the way
    /// the backend addresses it (e.g. its managed-layer path or repoBlobUrl).
    fn managed_base_layer_config(&self, layer: &ManagedLayer) -> LayerConfig;
}

/// Derives the layer list from one runtime disk image config, routing delta
/// layers onto the shared filesystem, delegating `.commit`-style base layers to
/// `layers.import_disk_base_layer`, and keeping registry-referenced layers as
/// `External` (backend-agnostic).
async fn derive_disk_layers(
    storage: &SharedFsStorage,
    id: &SnapshotId,
    image_config_path: &Path,
    layers: &dyn LayerStorage,
) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
    let image_config = load_overlaybd_image_config(image_config_path).map_err(|error| {
        RepositoryError::backend(
            format!(
                "load overlaybd image config '{}'",
                image_config_path.display()
            ),
            error,
        )
    })?;
    let mut out = Vec::with_capacity(image_config.lowers.len());
    for (index, layer) in image_config.lowers.into_iter().enumerate() {
        if !layer.file.is_empty() {
            let layer_path = Path::new(&layer.file);
            if rootfs_layer_is_runtime_generated_delta(layer_path) {
                let descriptor =
                    crate::digest::FileDigest::describe_blocking(layer_path).map_err(|error| {
                        RepositoryError::backend(
                            format!("describe delta layer '{}'", layer_path.display()),
                            error,
                        )
                    })?;
                let shared = storage.store_delta_layer(
                    id,
                    layer_path,
                    if layer.digest.is_empty() {
                        &descriptor.sha256
                    } else {
                        &layer.digest
                    },
                    if layer.size > 0 {
                        layer.size
                    } else {
                        descriptor.size
                    },
                    overlaybd_layer_uuid(layer_path),
                )?;
                out.push(OverlaybdLayerRef::SharedFS(shared));
                continue;
            }
            // Local base layer with a descriptor -> backend storage.
            if !layer.digest.is_empty() && layer.size > 0 {
                let managed = layers
                    .import_disk_base_layer(layer_path, &layer.digest, layer.size)
                    .await?;
                out.push(OverlaybdLayerRef::Managed(managed));
                continue;
            }
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "shared-fs layer store: base layer {index} '{}' has no descriptor",
                    layer_path.display()
                ),
            });
        }
        // Registry-referenced layer.
        let repo_blob_url = layer
            .effective_repo_blob_url(&image_config.repo_blob_url)
            .to_string();
        if repo_blob_url.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }
        let digest = if !layer.digest.is_empty() {
            layer.digest
        } else if !layer.target_digest.is_empty() {
            layer.target_digest
        } else {
            format!("external:{index}")
        };
        out.push(OverlaybdLayerRef::External(ExternalLayer {
            digest,
            repo_blob_url: repo_blob_url.clone(),
            size: layer.size,
        }));
        continue;
    }
    Ok(out)
}

fn overlaybd_layer_uuid(source: &Path) -> Option<String> {
    overlaybd::layer_metadata::read_overlaybd_layer_uuid(source)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
}

/// Repository decorator routing snapshot-generated content onto a node-shared
/// filesystem while delegating base-layer storage and all non-snapshot APIs to
/// the wrapped backend.
pub(crate) struct SharedFsSnapshotRepository {
    inner: Arc<dyn SnapshotRepository>,
    storage: Arc<SharedFsStorage>,
    layers: Arc<dyn LayerStorage>,
}

impl SharedFsSnapshotRepository {
    pub(crate) fn new(
        inner: Arc<dyn SnapshotRepository>,
        storage: Arc<SharedFsStorage>,
        layers: Arc<dyn LayerStorage>,
    ) -> Self {
        Self {
            inner,
            storage,
            layers,
        }
    }

    fn committed(
        &self,
        metadata: &SnapshotPublishMetadata,
        rootfs_layers: Vec<OverlaybdLayerRef>,
        memory_layers: Vec<OverlaybdLayerRef>,
        attached_drives: Vec<CommittedAttachedDrive>,
    ) -> CommittedSnapshot {
        CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            custom_extension_params: metadata.custom_extension_params.clone(),
            rootfs_layers,
            attached_drives,
            memory_layers,
            disk_publications: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_async(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = metadata.id.clone();
        let session = self.storage.catalog().begin_publish(&id)?;

        let publish_result = async {
            // Duplicate drive-id guard.
            let mut seen = std::collections::HashSet::new();
            for drive in &manifest.attached_drives {
                if !seen.insert(drive.drive_id.clone()) {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "duplicate attached drive id in publish request: {}",
                            drive.drive_id
                        ),
                    });
                }
                if drive.virtual_size == 0 {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "attached drive '{}' virtual_size must be non-zero",
                            drive.drive_id
                        ),
                    });
                }
            }

            // Fixed artifacts.
            let vm_state = std::fs::read(&manifest.vm_state.path).map_err(|error| {
                RepositoryError::backend(
                    format!("read vm_state '{}'", manifest.vm_state.path.display()),
                    error,
                )
            })?;
            self.storage
                .store_fixed_artifact(&id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state, &vm_state)?;
            let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
                RepositoryError::backend("serialize firecracker manifest", error)
            })?;
            self.storage.store_fixed_artifact(
                &id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                &manifest_bytes,
            )?;

            // Layers.
            let rootfs_layers = derive_disk_layers(
                &self.storage,
                &id,
                &manifest.rootfs.image_config_path,
                self.layers.as_ref(),
            )
            .await?;
            let memory_layers = derive_memory_layers(
                &self.storage,
                &id,
                &manifest.memory.image_config_path,
                self.layers.as_ref(),
            )
            .await?;
            let attached_drives = {
                let mut drives = Vec::with_capacity(manifest.attached_drives.len());
                for drive in &manifest.attached_drives {
                    let layers = derive_disk_layers(
                        &self.storage,
                        &id,
                        &drive.image_config_path,
                        self.layers.as_ref(),
                    )
                    .await?;
                    drives.push(CommittedAttachedDrive::Overlaybd {
                        drive_id: drive.drive_id.clone(),
                        layers,
                        read_only: drive.read_only,
                        virtual_size: drive.virtual_size,
                        mount_path: crate::sandbox::normalize_mount_path_for_drive(
                            &drive.drive_id,
                            drive.mount_path.clone(),
                        )
                        .unwrap_or_else(|_| {
                            crate::sandbox::ExtraDrive::default_mount_path(&drive.drive_id)
                        }),
                        sub_path: drive.sub_path.clone(),
                    });
                }
                drives
            };

            let committed =
                self.committed(&metadata, rootfs_layers, memory_layers, attached_drives);
            self.storage
                .catalog()
                .commit_publish(&session, metadata, committed)
        }
        .await;

        match publish_result {
            Ok(record) => Ok(record),
            Err(error) => {
                self.storage.cleanup_snapshot_dir(&id);
                Err(error)
            }
        }
    }
}

/// Derives memory layers, routing each layer to its declared storage:
/// remote/base layers become `Managed` (stay in the backend, e.g. OSS),
/// capture-time deltas become `SharedFS` (copied onto the shared filesystem
/// and referenced by an absolute path, mirroring `derive_disk_layers`).
async fn derive_memory_layers(
    storage: &SharedFsStorage,
    id: &SnapshotId,
    mem_image_config_path: &Path,
    layers: &dyn LayerStorage,
) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
    let image_config = load_overlaybd_image_config(mem_image_config_path).map_err(|error| {
        RepositoryError::backend(
            format!(
                "load mem image config '{}'",
                mem_image_config_path.display()
            ),
            error,
        )
    })?;
    let mut out = Vec::with_capacity(image_config.lowers.len());
    for (index, layer) in image_config.lowers.into_iter().enumerate() {
        // Remote managed layer (already in OSS, no local file): keep as-is.
        // The layer has a digest and size but an empty file path because it
        // was inherited from a prior snapshot's memory image config and was
        // not materialized locally during this capture.
        if layer.file.is_empty() {
            if !layer.digest.is_empty() && layer.size > 0 {
                out.push(OverlaybdLayerRef::Managed(ManagedLayer {
                    digest: layer.digest,
                    size: layer.size,
                    uuid: None,
                }));
                continue;
            }
            return Err(RepositoryError::Unsupported {
                feature: format!("memory layer {index} without local file path or descriptor"),
            });
        }

        // Base (non-delta) memory layers go to the backend; capture-time deltas
        // (default for newly captured snapshots) stay on the shared filesystem.
        let layer_path = Path::new(&layer.file);
        if !rootfs_layer_is_runtime_generated_delta(layer_path)
            && !layer.digest.is_empty()
            && layer.size > 0
        {
            let managed = layers
                .import_memory_base_layer(layer_path, &layer.digest, layer.size)
                .await?;
            out.push(OverlaybdLayerRef::Managed(managed));
            continue;
        }
        let digest = if !layer.digest.is_empty() {
            layer.digest.clone()
        } else {
            crate::digest::FileDigest::describe_blocking(layer_path)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("describe memory layer '{}'", layer_path.display()),
                        error,
                    )
                })?
                .sha256
        };
        let size = if layer.size > 0 {
            layer.size
        } else {
            std::fs::metadata(layer_path)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("stat memory layer '{}'", layer_path.display()),
                        error,
                    )
                })?
                .len()
        };
        let shared = storage.store_delta_layer(
            id,
            layer_path,
            &digest,
            size,
            overlaybd_layer_uuid(layer_path),
        )?;
        out.push(OverlaybdLayerRef::SharedFS(shared));
    }
    Ok(out)
}

#[async_trait]
impl SnapshotRepository for SharedFsSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.storage.catalog().create(record)
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        self.publish_async(metadata, manifest).await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.storage.catalog().get(id_or_alias)
    }

    async fn list(
        &self,
        filter: crate::snapshot::repository::SnapshotListFilter,
    ) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.storage.catalog().list(filter)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let record = self.storage.catalog().get(id_or_alias)?;
        if let Some(record) = record {
            self.storage.cleanup_snapshot_dir(&record.id);
            self.storage.catalog().delete_record(&record.id)?;
        }
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        self.storage.catalog().resolve_alias(alias)
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        self.storage.catalog().try_start(id)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        self.storage.catalog().mark_error(id, reason)
    }
}

/// Runtime resolver decorator that reads fixed artifacts from the shared
/// filesystem and materializes runnable image configs from committed layers
/// (delta layers as direct file paths, base layers via the backend's managed
/// layer addressing).
pub(crate) struct SharedFsRuntimeResolver {
    inner: Arc<dyn SnapshotRuntimeResolver>,
    storage: Arc<SharedFsStorage>,
    layers: Arc<dyn LayerStorage>,
}

impl SharedFsRuntimeResolver {
    pub(crate) fn new(
        inner: Arc<dyn SnapshotRuntimeResolver>,
        storage: Arc<SharedFsStorage>,
        layers: Arc<dyn LayerStorage>,
    ) -> Self {
        Self {
            inner,
            storage,
            layers,
        }
    }

    fn layer_config(&self, layer: &OverlaybdLayerRef) -> LayerConfig {
        match layer {
            OverlaybdLayerRef::SharedFS(shared) => SharedFsStorage::layer_config(shared),
            OverlaybdLayerRef::Managed(managed) => self.layers.managed_base_layer_config(managed),
            OverlaybdLayerRef::External(external) => LayerConfig {
                repo_blob_url: external.repo_blob_url.clone(),
                digest: external.digest.clone(),
                size: external.size,
                ..Default::default()
            },
        }
    }
}

#[async_trait]
impl SnapshotRuntimeResolver for SharedFsRuntimeResolver {
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot> {
        let id = snapshot.id.clone();
        let committed =
            snapshot
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' is not ready", snapshot.id),
                })?;

        // Fixed artifacts from the shared filesystem.
        let vm_state_path = self
            .storage
            .snapshot_dir(&id)
            .join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        if !vm_state_path.exists() {
            return Err(RepositoryError::ArtifactNotFound {
                artifact: format!("shared-fs vm state at '{}'", vm_state_path.display()),
            });
        }
        let manifest_bytes = self
            .storage
            .read_fixed_artifact(&id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest)?;
        let committed_manifest: FirecrackerSnapshotManifest =
            serde_json::from_slice(&manifest_bytes).map_err(|error| {
                RepositoryError::backend(
                    format!("parse firecracker manifest for snapshot '{}'", id),
                    error,
                )
            })?;

        // Memory layers are already typed in the committed record: `SharedFS`
        // variants reference the shared filesystem by absolute path, `Managed`
        // variants are resolved by the inner backend (e.g. OSS repoBlobUrl).
        // No filesystem probe is needed: the variant declared at publish time
        // is the source of truth for where the layer bytes live.
        let memory_layers: Vec<OverlaybdLayerRef> = committed.memory_layers.clone();

        let runtime_root = crate::cfg::ConfigManager::global_config()
            .snapshot
            .local_cache_path
            .join("runtime")
            .join(id.to_string());
        let mem_image_config_path =
            write_image_config(&self, &runtime_root, "memory/image.json", &memory_layers)?;
        let rootfs_image_config_path = write_image_config(
            &self,
            &runtime_root,
            "rootfs/image.json",
            &committed.rootfs_layers,
        )?;

        let attached_drives = {
            let mut drives = Vec::with_capacity(committed.attached_drives.len());
            for drive in &committed.attached_drives {
                match drive {
                    CommittedAttachedDrive::Overlaybd {
                        drive_id,
                        layers,
                        read_only,
                        virtual_size,
                        mount_path,
                        sub_path,
                    } => {
                        let path = write_image_config(
                            &self,
                            &runtime_root,
                            &format!("drives/{drive_id}/image.json"),
                            layers,
                        )?;
                        drives.push(ResolvedAttachedDrive::Overlaybd {
                            drive_id: drive_id.clone(),
                            image_config_path: path,
                            read_only: *read_only,
                            virtual_size: *virtual_size,
                            mount_path: crate::sandbox::normalize_mount_path_for_drive(
                                drive_id,
                                mount_path.clone(),
                            )
                            .unwrap_or_else(|_| {
                                crate::sandbox::ExtraDrive::default_mount_path(drive_id)
                            }),
                            sub_path: sub_path.clone(),
                        });
                    }
                }
            }
            drives
        };

        let cache_lease: Arc<dyn crate::snapshot::types::RuntimeArtifactLease> =
            Arc::new(CacheArtifactLease {
                _handles: Vec::new(),
            });

        let runtime_manifest = crate::snapshot::runtime_support::hydrate_runtime_manifest(
            committed_manifest,
            vm_state_path,
            mem_image_config_path,
            rootfs_image_config_path,
            &attached_drives,
        )?;
        let _ = &self.inner; // inner kept for parity; resolution is self-contained
        Ok(RunnableSnapshot::new(
            (*snapshot).clone(),
            runtime_manifest,
            cache_lease,
        ))
    }
}

/// Writes a runtime overlaybd image config for the given layer refs and returns
/// its absolute path.
fn write_image_config(
    resolver: &SharedFsRuntimeResolver,
    runtime_root: &Path,
    relative_path: &str,
    layers: &[OverlaybdLayerRef],
) -> RepositoryResult<PathBuf> {
    let destination = runtime_root.join(relative_path);
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create runtime image config dir '{}'", parent.display()),
                error,
            )
        })?;
    }
    let image_config = ImageConfig {
        repo_blob_url: String::new(),
        lowers: layers
            .iter()
            .map(|layer| resolver.layer_config(layer))
            .collect(),
        ..Default::default()
    };
    let bytes = serde_json::to_vec_pretty(&image_config)
        .map_err(|error| RepositoryError::backend("serialize runtime image config", error))?;
    let tmp = destination.with_extension("tmp");
    std::fs::write(&tmp, &bytes).map_err(|error| {
        RepositoryError::backend(
            format!("write runtime image config '{}'", tmp.display()),
            error,
        )
    })?;
    std::fs::rename(&tmp, &destination).map_err(|error| {
        RepositoryError::backend(
            format!("install runtime image config '{}'", destination.display()),
            error,
        )
    })?;
    Ok(destination)
}
