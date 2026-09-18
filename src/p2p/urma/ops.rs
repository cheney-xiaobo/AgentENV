//! Safe(ish) wrapper layer over the raw liburma FFI.
//!
//! The [`UrmaOps`] trait isolates all URMA C API interaction behind Rust
//! native types so the rest of the backend (connection management, segment
//! registry, one-sided READ path) is unit-testable with [`MockUrmaOps`]
//! without a UB device or even the liburma SDK being installed.
//!
//! Model notes (matching the real liburma API as of UMDK 25.12):
//!
//! - UB devices run in `URMA_TM_RM` (reliable message, connectionless):
//!   every WR carries the imported remote jetty (`tjetty`); there is no
//!   RC-style bind handshake. [`UrmaOps::bind_jetty`] is kept as a hook so
//!   connection management code stays transport-agnostic.
//! - A jetty on UB must be created with the `share_jfr` flag and an
//!   explicitly created JFR attached; the real ops create one JFR per
//!   jetty and remember it for teardown.
//! - Identifiers are `(eid, uasid, id)` triples; the `uasid` is part of
//!   the fabric-visible jetty id and must be exchanged alongside the EID.
//! - Imported remote segments are mapped (`URMA_SEG_MAPPED`) and reads
//!   address the imported segment through its local mapping address
//!   (`mva`), not the remote UBVA.
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Length in bytes of an EID (Unified Bus endpoint identifier). Mirrors
/// `URMA_EID_SIZE` in ffi.rs; duplicated so mock-based tests compile without
/// liburma.
pub const URMA_EID_LEN: usize = 16;

/// Unified Bus endpoint identifier of a node.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Eid(pub [u8; URMA_EID_LEN]);

impl Eid {
    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn from_hex(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.len() != URMA_EID_LEN * 2 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let mut out = [0u8; URMA_EID_LEN];
        for (i, chunk) in value.as_bytes().chunks(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16)?;
            let lo = (chunk[1] as char).to_digit(16)?;
            out[i] = ((hi << 4) | lo) as u8;
        }
        Some(Self(out))
    }
}

/// Jetty identifier on a (possibly remote) node: (EID, uasid, jetty id).
/// The triple matches `urma_jetty_id_t` on the wire.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct JettyId {
    pub eid: Eid,
    /// URMA address-space id of the owning process on the remote node.
    pub uasid: u32,
    pub id: u32,
}

/// Description of a segment published by a remote node, exchanged via the
/// catalog (out-of-band): everything needed to `urma_import_seg` and issue
/// one-sided READs against it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RemoteSegInfo {
    /// Segment home node EID.
    pub eid: Eid,
    /// Home node uasid (part of the segment UBVA).
    pub uasid: u32,
    /// Segment base UBVA (virtual address on the UB).
    pub va: u64,
    /// Access token id handed out by the home node.
    pub key: u32,
    /// Segment length in bytes (for bounds checks).
    pub len: u64,
}

// ---------------------------------------------------------------------------
// Opaque handle wrappers
//
// Handles are plain `usize` tokens so they are `Send + Sync + Copy` and can
// be fabricated by the mock. `RealUrmaOps` stores raw pointers in them.
// ---------------------------------------------------------------------------

macro_rules! opaque_handle {
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        pub struct $name(pub usize);

        impl $name {
            pub fn from_raw(ptr: *mut c_void) -> Self {
                Self(ptr as usize)
            }

            #[cfg(feature = "p2p-urma")]
            pub fn as_ptr<T>(self) -> *mut T {
                self.0 as *mut T
            }
        }
    };
}

opaque_handle!(CtxHandle);
opaque_handle!(JfcHandle);
opaque_handle!(JettyHandle);
opaque_handle!(TargetJettyHandle);
opaque_handle!(TokenIdHandle);
opaque_handle!(TargetSegHandle);

/// A locally registered segment plus the info peers need to read it.
#[derive(Clone, Copy, Debug)]
pub struct RegisteredSeg {
    pub tseg: TargetSegHandle,
    /// Local virtual address (the `seg_cfg.va` passed to `urma_register_seg`).
    /// Used for `post_recv` / `post_send` SGE addr.
    pub local_va: u64,
    /// UBVA assigned to the segment (advertised to peers for import).
    pub ubva: u64,
    /// Access token id (advertised to peers).
    pub key: u32,
}

/// An imported remote segment plus its local mapping address. One-sided
/// READs address the remote memory through `mva`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImportedSeg {
    pub tseg: TargetSegHandle,
    /// Local mapping address of the remote segment (read addressing base).
    pub mva: u64,
}

/// One-sided READ work request.
#[derive(Clone, Copy, Debug)]
pub struct ReadWr {
    pub wr_id: u64,
    /// Imported remote jetty that processes the READ (RM is connectionless,
    /// so every WR carries its target).
    pub tjetty: TargetJettyHandle,
    pub remote_tseg: TargetSegHandle,
    /// Remote address (within the imported segment's mapping).
    pub remote_va: u64,
    pub local_tseg: TargetSegHandle,
    pub local_va: u64,
    pub len: u32,
}

/// Two-sided SEND/RECV work request.
#[derive(Clone, Copy, Debug)]
pub struct MsgWr {
    pub wr_id: u64,
    /// Imported remote jetty the SEND targets (unused for RECV).
    pub tjetty: TargetJettyHandle,
    pub local_tseg: TargetSegHandle,
    pub local_va: u64,
    pub len: u32,
}

/// Completion for a submitted work request.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    pub wr_id: u64,
    pub status: i32,
    pub len: u32,
    /// For RECV completions: the jetty id the received message came from
    /// (`cr.remote_id`). Used by catalog responders to address the reply.
    pub from: Option<JettyId>,
}

/// Configuration for creating a URMA context. The counts are the JFS/JFR
/// depths of the jettys created on the context.
#[derive(Clone, Copy, Debug)]
pub struct ContextConfig {
    pub jfs_count: u32,
    pub jfr_count: u32,
}

/// Result type for ops failures carrying the URMA status code.
#[derive(Debug, thiserror::Error)]
#[error("urma operation {operation} failed with status {status}")]
pub struct UrmaOpError {
    pub operation: &'static str,
    pub status: i32,
}

pub type OpsResult<T> = std::result::Result<T, UrmaOpError>;

/// Abstraction over the URMA C API used by the backend.
///
/// All methods are synchronous; callers on the async runtime wrap them with
/// `spawn_blocking` where they may block.
pub trait UrmaOps: Send + Sync {
    /// Enumerate available UB device names.
    fn list_devices(&self) -> OpsResult<Vec<String>>;

    /// Create a context (with one JFC) on the given device.
    fn create_context(&self, device: &str, cfg: &ContextConfig) -> OpsResult<(CtxHandle, JfcHandle)>;

    fn delete_context(&self, ctx: CtxHandle) -> OpsResult<()>;

    fn delete_jfc(&self, jfc: JfcHandle) -> OpsResult<()>;

    /// Create an RM-mode duplex jetty whose completions land on `jfc`.
    fn create_jetty(&self, ctx: CtxHandle, jfc: JfcHandle) -> OpsResult<JettyHandle>;

    fn delete_jetty(&self, jetty: JettyHandle) -> OpsResult<()>;

    /// Query the local jetty id (EID + uasid + id) to advertise to peers.
    fn jetty_id(&self, jetty: JettyHandle) -> OpsResult<JettyId>;

    /// Import a remote jetty so WRs can target it (RM connection setup).
    fn import_jetty(&self, ctx: CtxHandle, remote: &JettyId) -> OpsResult<TargetJettyHandle>;

    fn unimport_jetty(&self, tjetty: TargetJettyHandle) -> OpsResult<()>;

    /// Establish the transport channel between a local jetty and an
    /// imported remote jetty. UB RM is connectionless, so the real
    /// implementation is a no-op; the hook exists for RC-style transports.
    fn bind_jetty(&self, jetty: JettyHandle, tjetty: TargetJettyHandle) -> OpsResult<()>;

    fn unbind_jetty(&self, jetty: JettyHandle) -> OpsResult<()>;

    /// Register local memory `[addr, addr+len)` as a segment and return the
    /// info remote peers need for one-sided access.
    fn register_seg(&self, ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg>;

    /// Register a segment for local two-sided ops (SEND/RECV buffers).
    /// URMA access bits are mutually exclusive: LOCAL_ONLY grants local
    /// jetty ops while READ|WRITE|ATOMIC grants remote one-sided access;
    /// combining them fails registration (verified against hardware).
    fn register_local_seg(&self, ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg>;

    fn unregister_seg(&self, tseg: TargetSegHandle) -> OpsResult<()>;

    /// Import a remote segment for local one-sided access (RM). The
    /// returned [`ImportedSeg::mva`] is the base address for READs.
    fn import_seg(&self, ctx: CtxHandle, remote: &RemoteSegInfo) -> OpsResult<ImportedSeg>;

    fn unimport_seg(&self, tseg: TargetSegHandle) -> OpsResult<()>;

    /// Submit a one-sided READ: pull `len` bytes from
    /// `(remote_tseg, remote_va)` into `(local_tseg, local_va)`.
    fn post_read(&self, jetty: JettyHandle, wr: &ReadWr) -> OpsResult<()>;

    /// Submit a two-sided SEND: transmit `len` bytes from
    /// `(local_tseg, local_va)` to the WR's target jetty, landing in one of
    /// its pre-posted RECV buffers.
    fn post_send(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()>;

    /// Pre-post a receive buffer on `jetty`; an incoming SEND from a peer
    /// lands at `(local_tseg, local_va)` and completes with the received
    /// length.
    fn post_recv(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()>;

    /// Poll the context JFC for completions, appending them to `out`.
    /// Returns the number of new completions.
    fn poll_completions(&self, jfc: JfcHandle, out: &mut Vec<Completion>) -> OpsResult<usize>;
}

// ---------------------------------------------------------------------------
// Real implementation (feature-gated: requires liburma at link time)
// ---------------------------------------------------------------------------

#[cfg(feature = "p2p-urma")]
use super::ffi;

/// `urma_init` is process-global and may only run once.
#[cfg(feature = "p2p-urma")]
static URMA_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Raw JFR pointer made `Send` (the FFI object is used from multiple
/// threads behind the ops mutex; liburma handles are thread-safe).
#[cfg(feature = "p2p-urma")]
#[derive(Clone, Copy)]
struct JfrPtr(*mut ffi::urma_jfr_t);

#[cfg(feature = "p2p-urma")]
unsafe impl Send for JfrPtr {}

#[cfg(feature = "p2p-urma")]
pub struct RealUrmaOps {
    /// JFR created per jetty (UB share_jfr requirement), for teardown.
    jetty_jfrs: Mutex<HashMap<JettyHandle, JfrPtr>>,
}

#[cfg(feature = "p2p-urma")]
impl Default for RealUrmaOps {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "p2p-urma")]
impl RealUrmaOps {
    pub fn new() -> Self {
        if !URMA_INITIALIZED.swap(true, Ordering::SeqCst) {
            let status = unsafe { ffi::urma_init(std::ptr::null_mut()) };
            if status != ffi::URMA_SUCCESS {
                tracing::warn!(status, "urma_init failed (continuing; it may already be initialized)");
            }
        }
        Self {
            jetty_jfrs: Mutex::new(HashMap::new()),
        }
    }

    fn check(operation: &'static str, status: ffi::urma_status_t) -> OpsResult<()> {
        if status == ffi::URMA_SUCCESS {
            Ok(())
        } else {
            Err(UrmaOpError { operation, status })
        }
    }

    fn check_non_null<T>(operation: &'static str, ptr: *mut T) -> OpsResult<*mut T> {
        if ptr.is_null() {
            Err(UrmaOpError { operation, status: -1 })
        } else {
            Ok(ptr)
        }
    }

    fn device_name(dev: *mut ffi::urma_device_t) -> String {
        unsafe {
            let name_ptr = (*dev).name.as_ptr();
            let bytes = std::ffi::CStr::from_ptr(name_ptr).to_bytes();
            String::from_utf8_lossy(bytes).into_owned()
        }
    }

    /// Register `[addr, addr+len)` with either remote (RW|ATOMIC + token) or
    /// local (LOCAL_ONLY) access. The two flag sets are mutually exclusive in
    /// URMA: two-sided SEND/RECV buffers require LOCAL_ONLY, while one-sided
    /// remote access requires READ|WRITE|ATOMIC plus a published token.
    fn register_seg_with_access(
        &self,
        ctx: CtxHandle,
        addr: usize,
        len: u64,
        local: bool,
    ) -> OpsResult<RegisteredSeg> {
        unsafe {
            let token_id = if local {
                // Control buffers never publish a token: no token_id.
                std::ptr::null_mut()
            } else {
                Self::check_non_null(
                    "urma_alloc_token_id",
                    ffi::urma_alloc_token_id(ctx.as_ptr()),
                )?
            };
            let flag_val = if local {
                ffi::REG_SEG_FLAG_ACCESS_LOCAL_ONLY
            } else {
                ffi::REG_SEG_FLAG_ACCESS_RW_ATOMIC | ffi::REG_SEG_FLAG_TOKEN_ID_VALID
            };
            let mut cfg = ffi::urma_seg_cfg_t {
                va: addr as u64,
                len,
                token_id,
                token_value: ffi::urma_token_t { token: 0 },
                flag: flag_val,
                user_ctx: 0,
                iova: 0,
            };
            let tseg = Self::check_non_null(
                "urma_register_seg",
                ffi::urma_register_seg(ctx.as_ptr(), &mut cfg),
            )?;
            if local {
                // Local control buffers are never imported remotely: no UBVA.
                return Ok(RegisteredSeg {
                    tseg: TargetSegHandle::from_raw(tseg as *mut c_void),
                    local_va: addr as u64,
                    ubva: 0,
                    key: 0,
                });
            }
            // Serialize the segment to learn its UBVA + token id.
            let mut seg_ctx: *mut ffi::urma_seg_t = std::ptr::null_mut();
            let mut size: u32 = 0;
            if Self::check(
                "urma_get_seg_ctx",
                ffi::urma_get_seg_ctx(tseg, &mut seg_ctx, &mut size),
            )
            .is_err()
                || seg_ctx.is_null()
            {
                let _ = ffi::urma_unregister_seg(tseg);
                let _ = ffi::urma_free_token_id(token_id);
                return Err(UrmaOpError { operation: "urma_get_seg_ctx", status: -6 });
            }
            let va = (*seg_ctx).ubva.va;
            let token = (*seg_ctx).token_id;
            ffi::urma_put_seg_ctx(seg_ctx);
            Ok(RegisteredSeg {
                tseg: TargetSegHandle::from_raw(tseg as *mut c_void),
                local_va: addr as u64,
                ubva: va,
                key: token,
            })
        }
    }
}

#[cfg(feature = "p2p-urma")]
impl UrmaOps for RealUrmaOps {
    fn list_devices(&self) -> OpsResult<Vec<String>> {
        unsafe {
            let mut num: i32 = 0;
            let devices = ffi::urma_get_device_list(&mut num);
            if devices.is_null() || num <= 0 {
                if !devices.is_null() {
                    ffi::urma_free_device_list(devices);
                }
                return Ok(Vec::new());
            }
            let mut names = Vec::with_capacity(num as usize);
            for i in 0..num as usize {
                let dev = *devices.add(i);
                if !dev.is_null() {
                    names.push(Self::device_name(dev));
                }
            }
            ffi::urma_free_device_list(devices);
            Ok(names)
        }
    }

    fn create_context(&self, device: &str, cfg: &ContextConfig) -> OpsResult<(CtxHandle, JfcHandle)> {
        unsafe {
            let cname = std::ffi::CString::new(device)
                .map_err(|_| UrmaOpError { operation: "urma device name", status: -4 })?;
            let dev = Self::check_non_null(
                "urma_get_device_by_name",
                ffi::urma_get_device_by_name(cname.as_ptr() as *mut _),
            )?;

            // Pick the first EID of the device.
            let mut cnt: u32 = 0;
            let eids = ffi::urma_get_eid_list(dev, &mut cnt);
            if eids.is_null() || cnt == 0 {
                if !eids.is_null() {
                    ffi::urma_free_eid_list(eids);
                }
                return Err(UrmaOpError { operation: "urma_get_eid_list", status: -5 });
            }
            let eid_index = (*eids).eid_index;
            ffi::urma_free_eid_list(eids);

            let ctx = Self::check_non_null(
                "urma_create_context",
                ffi::urma_create_context(dev, eid_index),
            )?;

            // JFC deep enough for the pipeline depths of this context.
            let jfc_depth = (cfg.jfs_count + cfg.jfr_count).max(16) * 8;
            let mut jfc_cfg = ffi::urma_jfc_cfg_t {
                depth: jfc_depth,
                flag: 0,
                ceqn: 0,
                jfce: std::ptr::null_mut(),
                user_ctx: 0,
            };
            let jfc = Self::check_non_null("urma_create_jfc", ffi::urma_create_jfc(ctx, &mut jfc_cfg))?;
            Ok((CtxHandle::from_raw(ctx as *mut c_void), JfcHandle::from_raw(jfc as *mut c_void)))
        }
    }

    fn delete_context(&self, ctx: CtxHandle) -> OpsResult<()> {
        unsafe { Self::check("urma_delete_context", ffi::urma_delete_context(ctx.as_ptr())) }
    }

    fn delete_jfc(&self, jfc: JfcHandle) -> OpsResult<()> {
        unsafe { Self::check("urma_delete_jfc", ffi::urma_delete_jfc(jfc.as_ptr())) }
    }

    fn create_jetty(&self, ctx: CtxHandle, jfc: JfcHandle) -> OpsResult<JettyHandle> {
        unsafe {
            let ctx_ptr = ctx.as_ptr::<ffi::urma_context_t>();
            let jfc_ptr = jfc.as_ptr::<ffi::urma_jfc_t>();

            // UB only supports share_jfr: create a standalone JFR and attach
            // it through the jetty config's shared arm.
            let mut jfr_cfg = ffi::urma_jfr_cfg_t {
                id: 0,
                depth: 64,
                flag: 0,
                trans_mode: ffi::URMA_TM_RM,
                max_sge: 1,
                min_rnr_timer: 12,
                jfc: jfc_ptr,
                token_value: ffi::urma_token_t { token: 0 },
                user_ctx: 0,
            };
            let jfr = Self::check_non_null("urma_create_jfr", ffi::urma_create_jfr(ctx_ptr, &mut jfr_cfg))?;

            let mut jfs_cfg = ffi::urma_jfs_cfg_t {
                depth: 64,
                flag: 0,
                trans_mode: ffi::URMA_TM_RM,
                priority: 0, // RTP plane (priority 0-5) supports one-sided READ/WRITE and SEND/RECV on UB
                max_sge: 1,
                max_rsge: 1,
                max_inline_data: 0,
                rnr_retry: 7,
                err_timeout: 17,
                jfc: jfc_ptr,
                user_ctx: 0,
            };
            let mut jetty_cfg = ffi::urma_jetty_cfg_t {
                id: 0,
                flag: ffi::JETTY_FLAG_SHARE_JFR,
                jfs_cfg,
                recv: ffi::urma_jetty_recv_u {
                    shared: ffi::urma_jetty_shared_t {
                        jfr,
                        jfc: jfc_ptr,
                    },
                },
                jetty_grp: std::ptr::null_mut(),
                user_ctx: 0,
            };
            let jetty = Self::check_non_null(
                "urma_create_jetty",
                ffi::urma_create_jetty(ctx_ptr, &mut jetty_cfg),
            )?;
            let handle = JettyHandle::from_raw(jetty as *mut c_void);
            self.jetty_jfrs.lock().unwrap().insert(handle, JfrPtr(jfr));
            Ok(handle)
        }
    }

    fn delete_jetty(&self, jetty: JettyHandle) -> OpsResult<()> {
        unsafe {
            let jfr = self.jetty_jfrs.lock().unwrap().remove(&jetty);
            let result = Self::check("urma_delete_jetty", ffi::urma_delete_jetty(jetty.as_ptr()));
            if let Some(jfr) = jfr {
                let _ = Self::check("urma_delete_jfr", ffi::urma_delete_jfr(jfr.0));
            }
            result
        }
    }

    fn jetty_id(&self, jetty: JettyHandle) -> OpsResult<JettyId> {
        unsafe {
            // urma_jetty_t exposes its public jetty_id right after the
            // context pointer.
            let raw: *mut ffi::urma_jetty_t = jetty.as_ptr();
            let id = (*raw).jetty_id;
            Ok(JettyId {
                eid: Eid(id.eid.raw),
                uasid: id.uasid,
                id: id.id,
            })
        }
    }

    fn import_jetty(&self, ctx: CtxHandle, remote: &JettyId) -> OpsResult<TargetJettyHandle> {
        unsafe {
            let mut rjetty = ffi::urma_rjetty_t {
                jetty_id: ffi::urma_jetty_id_t {
                    eid: ffi::urma_eid_t { raw: remote.eid.0 },
                    uasid: remote.uasid,
                    id: remote.id,
                },
                trans_mode: ffi::URMA_TM_RM,
                policy: 0,
                type_: ffi::URMA_JETTY,
                // RTP plane: required for one-sided READ/WRITE on UB hardware.
                // CTP (priority 6) only supports SEND/RECV and returns
                // URMA_CR_ACK_TIMEOUT_ERR (status 9) for one-sided operations.
                flag: 0,
                tp_type: ffi::URMA_RTP,
            };
            let mut token = ffi::urma_token_t { token: 0 };
            let tjetty = Self::check_non_null(
                "urma_import_jetty",
                ffi::urma_import_jetty(ctx.as_ptr(), &mut rjetty, &mut token),
            )?;
            Ok(TargetJettyHandle::from_raw(tjetty as *mut c_void))
        }
    }

    fn unimport_jetty(&self, tjetty: TargetJettyHandle) -> OpsResult<()> {
        unsafe { Self::check("urma_unimport_jetty", ffi::urma_unimport_jetty(tjetty.as_ptr())) }
    }

    fn bind_jetty(&self, _jetty: JettyHandle, _tjetty: TargetJettyHandle) -> OpsResult<()> {
        // UB RM is connectionless: posting WRs against the imported remote
        // jetty works without a channel setup. urma_bind_jetty is
        // RC-only and returns an error on UB.
        Ok(())
    }

    fn unbind_jetty(&self, _jetty: JettyHandle) -> OpsResult<()> {
        Ok(())
    }

    fn register_seg(&self, ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg> {
        self.register_seg_with_access(ctx, addr, len, false)
    }

    fn register_local_seg(&self, ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg> {
        self.register_seg_with_access(ctx, addr, len, true)
    }

    fn unregister_seg(&self, tseg: TargetSegHandle) -> OpsResult<()> {
        unsafe { Self::check("urma_unregister_seg", ffi::urma_unregister_seg(tseg.as_ptr())) }
    }

    fn import_seg(&self, ctx: CtxHandle, remote: &RemoteSegInfo) -> OpsResult<ImportedSeg> {
        unsafe {
            let mut seg = ffi::urma_seg_t {
                ubva: ffi::urma_ubva_t {
                    eid: remote.eid.0,
                    uasid: remote.uasid,
                    va: remote.va,
                },
                len: remote.len,
                attr: ffi::REG_SEG_FLAG_ACCESS_READ,
                token_id: remote.key,
            };
            let mut token = ffi::urma_token_t { token: 0 };
            // NOMAP import: READs address the remote memory through the
            // remote UBVA directly (`ImportedSeg::mva` = remote va). A mapped
            // import would require a local ummu mapping whose base (`mva`)
            // is provider-assigned and may be 0, which breaks READ addressing.
            let flag = ffi::IMPORT_SEG_FLAG_ACCESS_READ;
            let tseg = Self::check_non_null(
                "urma_import_seg",
                ffi::urma_import_seg(ctx.as_ptr(), &mut seg, &mut token, 0, flag),
            )?;
            Ok(ImportedSeg {
                tseg: TargetSegHandle::from_raw(tseg as *mut c_void),
                mva: remote.va,
            })
        }
    }

    fn unimport_seg(&self, tseg: TargetSegHandle) -> OpsResult<()> {
        unsafe { Self::check("urma_unimport_seg", ffi::urma_unimport_seg(tseg.as_ptr())) }
    }

    fn post_read(&self, jetty: JettyHandle, wr: &ReadWr) -> OpsResult<()> {
        unsafe {
            let mut src_sge = ffi::urma_sge_t {
                addr: wr.remote_va,
                len: wr.len,
                tseg: wr.remote_tseg.as_ptr(),
                user_tseg: std::ptr::null_mut(),
            };
            let mut dst_sge = ffi::urma_sge_t {
                addr: wr.local_va,
                len: wr.len,
                tseg: wr.local_tseg.as_ptr(),
                user_tseg: std::ptr::null_mut(),
            };
            let mut raw_wr = ffi::urma_jfs_wr_t {
                opcode: ffi::URMA_OPC_READ,
                flag: ffi::JFS_WR_FLAG_COMPLETE_ENABLE,
                tjetty: wr.tjetty.as_ptr(),
                user_ctx: wr.wr_id,
                op: ffi::urma_jfs_op_u {
                    rw: ffi::urma_rw_wr_t {
                        src: ffi::urma_sg_t { sge: &mut src_sge, num_sge: 1 },
                        dst: ffi::urma_sg_t { sge: &mut dst_sge, num_sge: 1 },
                        target_hint: 0,
                        notify_data: 0,
                    },
                },
                next: std::ptr::null_mut(),
            };
            // liburma rejects a NULL bad_wr out-pointer with URMA_EINVAL.
            let mut bad_wr: *mut ffi::urma_jfs_wr_t = std::ptr::null_mut();
            Self::check(
                "urma_post_jetty_send_wr",
                ffi::urma_post_jetty_send_wr(jetty.as_ptr(), &mut raw_wr, &mut bad_wr),
            )
        }
    }

    fn post_send(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()> {
        // CTP/RTP SEND on UB hardware is limited to 4KiB per message
        // (matches the catalog protocol's CATALOG_BUF_SIZE). Reject
        // oversized sends in software to surface the error at the
        // boundary instead of as a hardware completion failure.
        const MAX_SEND_LEN: u32 = 4096;
        if wr.len > MAX_SEND_LEN {
            return Err(UrmaOpError {
                operation: "urma_post_jetty_send_wr (len > 4K CTP limit)",
                status: -1,
            });
        }
        unsafe {
            let mut src_sge = ffi::urma_sge_t {
                addr: wr.local_va,
                len: wr.len,
                tseg: wr.local_tseg.as_ptr(),
                user_tseg: std::ptr::null_mut(),
            };
            let mut raw_wr = ffi::urma_jfs_wr_t {
                opcode: ffi::URMA_OPC_SEND,
                flag: ffi::JFS_WR_FLAG_COMPLETE_ENABLE,
                tjetty: wr.tjetty.as_ptr(),
                user_ctx: wr.wr_id,
                op: ffi::urma_jfs_op_u {
                    send: ffi::urma_send_wr_t {
                        src: ffi::urma_sg_t { sge: &mut src_sge, num_sge: 1 },
                        target_hint: 0,
                        imm_data: 0,
                        tseg: std::ptr::null_mut(),
                    },
                },
                next: std::ptr::null_mut(),
            };
            let mut bad_wr: *mut ffi::urma_jfs_wr_t = std::ptr::null_mut();
            Self::check(
                "urma_post_jetty_send_wr",
                ffi::urma_post_jetty_send_wr(jetty.as_ptr(), &mut raw_wr, &mut bad_wr),
            )
        }
    }

    fn post_recv(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()> {
        unsafe {
            // Zero-init the WR like C does, then fill fields.
            let mut raw_wr: ffi::urma_jfr_wr_t = std::mem::zeroed();
            let mut src_sge: ffi::urma_sge_t = std::mem::zeroed();
            src_sge.addr = wr.local_va;
            src_sge.len = wr.len as u32;
            src_sge.tseg = wr.local_tseg.as_ptr();
            src_sge.user_tseg = std::ptr::null_mut();
            raw_wr.src.sge = &mut src_sge;
            raw_wr.src.num_sge = 1;
            raw_wr.user_ctx = wr.wr_id;
            raw_wr.next = std::ptr::null_mut();
            // liburma requires a non-NULL bad_wr out-pointer (it returns
            // URMA_EINVAL when bad_wr == NULL). Mirror the C tools: pass a
            // local variable.
            let mut bad_wr: *mut ffi::urma_jfr_wr_t = std::ptr::null_mut();
            let st = ffi::urma_post_jetty_recv_wr(jetty.as_ptr(), &mut raw_wr, &mut bad_wr);
            Self::check("urma_post_jetty_recv_wr", st)
        }
    }

    fn poll_completions(&self, jfc: JfcHandle, out: &mut Vec<Completion>) -> OpsResult<usize> {
        unsafe {
            let mut crs = [std::mem::zeroed::<ffi::urma_cr_t>(); 16];
            let n = ffi::urma_poll_jfc(jfc.as_ptr(), crs.len() as i32, crs.as_mut_ptr());
            if n < 0 {
                return Err(UrmaOpError { operation: "urma_poll_jfc", status: n });
            }
            for cr in &crs[..n as usize] {
                // cr.flag bit 0 marks a receive CR; only then is remote_id
                // meaningful.
                let from = if cr.flag & 0x1 == 0x1 {
                    Some(JettyId {
                        eid: Eid(cr.remote_id.eid.raw),
                        uasid: cr.remote_id.uasid,
                        id: cr.remote_id.id,
                    })
                } else {
                    None
                };
                out.push(Completion {
                    wr_id: cr.user_ctx,
                    status: cr.status,
                    len: cr.completion_len,
                    from,
                });
            }
            Ok(n as usize)
        }
    }
}

// ---------------------------------------------------------------------------
// Mock implementation (unit tests; no liburma required)
// ---------------------------------------------------------------------------

/// Scriptable mock used by unit tests. Behavior knobs are public so each
/// test can shape failure/latency scenarios.
#[cfg(test)]
pub struct MockUrmaOps {
    next_handle: AtomicU64,
    pub status: Mutex<MockStatus>,
}

#[cfg(test)]
#[derive(Default)]
pub struct MockStatus {
    pub devices: Vec<String>,
    /// Simulated failures for `bind_jetty`; each attempt consumes one entry.
    /// `Err(status)` fails, `Ok(())` succeeds. Missing entries succeed.
    pub bind_failures: Vec<i32>,
    /// Simulated failure status for `post_read`; 0 = success.
    pub read_failure: i32,
    /// Simulated failure status for `post_send`; 0 = success.
    pub send_failure: i32,
    /// Simulated failure status delivered via completions (async failure).
    pub completion_failure: i32,
    pub created_contexts: usize,
    pub deleted_contexts: usize,
    pub created_jetties: usize,
    pub deleted_jetties: usize,
    /// Remote jetty ids passed to `import_jetty`.
    pub jetties_imported: Vec<JettyId>,
    pub jetties_unimported: Vec<TargetJettyHandle>,
    pub bound: Vec<(JettyHandle, TargetJettyHandle)>,
    pub unbound: Vec<JettyHandle>,
    pub registered: Vec<(usize, u64)>,
    pub unregistered: usize,
    /// Remote segments passed to `import_seg`.
    pub segs_imported: Vec<RemoteSegInfo>,
    pub unimported_segs: usize,
    /// Pending READs (drained by `poll_completions`).
    pub reads: Vec<ReadWr>,
    /// Cumulative log of every posted READ (never drained), for assertions.
    pub reads_log: Vec<ReadWr>,
    /// Cumulative log of every posted SEND (never drained), for assertions.
    pub sends_log: Vec<MsgWr>,
    /// Pending RECV buffers `(jetty, wr)`, paired off (and completed) when a
    /// SEND is routed to the same jetty.
    pub recvs: Vec<(JettyHandle, MsgWr)>,
    /// SENDs that found no pending RECV on the destination jetty; completed
    /// retroactively when the RECV is posted (order-independent pairing).
    /// Entries carry the sender's mock jetty id for `Completion::from`.
    pub unmatched_sends: Vec<(JettyHandle, MsgWr, JettyId)>,
    /// Completions ready for the next `poll_completions` (SEND/RECV pairs).
    pub completed: Vec<Completion>,
    /// Registered host windows: handle -> (addr, len, advertised va).
    pub seg_windows: HashMap<TargetSegHandle, (usize, u64, u64)>,
    /// Imported target jetty -> the mock jetty handle it addresses.
    pub import_targets: HashMap<TargetJettyHandle, JettyHandle>,
    /// Mock routing edges between bound jettys (local <-> remote).
    pub jetty_binds: HashMap<JettyHandle, JettyHandle>,
}

#[cfg(test)]
impl MockUrmaOps {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(1),
            status: Mutex::new(MockStatus {
                devices: vec!["ub_0".to_string()],
                ..Default::default()
            }),
        })
    }

    fn alloc_handle(&self) -> usize {
        self.next_handle.fetch_add(1, Ordering::SeqCst) as usize
    }
}

/// Deliver a mock SEND to a RECV landing buffer: copy the payload between
/// the two registered host windows and enqueue the RECV completion.
#[cfg(test)]
fn deliver_mock_message(
    status: &mut MockStatus,
    sender: JettyId,
    send_wr: &MsgWr,
    recv_wr: &MsgWr,
) {
    let failure = status.completion_failure;
    if let (Some(&(s_addr, s_len, s_va)), Some(&(r_addr, r_len, r_va))) = (
        status.seg_windows.get(&send_wr.local_tseg),
        status.seg_windows.get(&recv_wr.local_tseg),
    ) {
        let src_off = (send_wr.local_va - s_va) as usize;
        let dst_off = (recv_wr.local_va - r_va) as usize;
        if src_off + send_wr.len as usize <= s_len as usize
            && dst_off + send_wr.len as usize <= r_len as usize
        {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (s_addr + src_off) as *const u8,
                    (r_addr + dst_off) as *mut u8,
                    send_wr.len as usize,
                );
            }
        }
    }
    status.completed.push(Completion {
        wr_id: recv_wr.wr_id,
        status: failure,
        len: send_wr.len,
        from: Some(sender),
    });
}

#[cfg(test)]
impl UrmaOps for MockUrmaOps {
    fn list_devices(&self) -> OpsResult<Vec<String>> {
        Ok(self.status.lock().unwrap().devices.clone())
    }

    fn create_context(&self, _device: &str, _cfg: &ContextConfig) -> OpsResult<(CtxHandle, JfcHandle)> {
        self.status.lock().unwrap().created_contexts += 1;
        Ok((CtxHandle(self.alloc_handle()), JfcHandle(self.alloc_handle())))
    }

    fn delete_context(&self, _ctx: CtxHandle) -> OpsResult<()> {
        self.status.lock().unwrap().deleted_contexts += 1;
        Ok(())
    }

    fn delete_jfc(&self, _jfc: JfcHandle) -> OpsResult<()> {
        Ok(())
    }

    fn create_jetty(&self, _ctx: CtxHandle, _jfc: JfcHandle) -> OpsResult<JettyHandle> {
        let h = JettyHandle(self.alloc_handle());
        self.status.lock().unwrap().created_jetties += 1;
        Ok(h)
    }

    fn delete_jetty(&self, _jetty: JettyHandle) -> OpsResult<()> {
        self.status.lock().unwrap().deleted_jetties += 1;
        Ok(())
    }

    fn jetty_id(&self, jetty: JettyHandle) -> OpsResult<JettyId> {
        Ok(JettyId {
            eid: Eid([0x11; URMA_EID_LEN]),
            uasid: 0x1000,
            id: jetty.0 as u32,
        })
    }

    fn import_jetty(&self, _ctx: CtxHandle, remote: &JettyId) -> OpsResult<TargetJettyHandle> {
        let tjetty = TargetJettyHandle(self.alloc_handle());
        let mut status = self.status.lock().unwrap();
        status.jetties_imported.push(*remote);
        // Mock jetty ids equal the raw handle value (see `jetty_id`), so the
        // remote jetty handle can be recovered from its advertised id.
        status
            .import_targets
            .insert(tjetty, JettyHandle(remote.id as usize));
        Ok(tjetty)
    }

    fn unimport_jetty(&self, tjetty: TargetJettyHandle) -> OpsResult<()> {
        self.status.lock().unwrap().jetties_unimported.push(tjetty);
        Ok(())
    }

    fn bind_jetty(&self, jetty: JettyHandle, tjetty: TargetJettyHandle) -> OpsResult<()> {
        let mut status = self.status.lock().unwrap();
        if let Some(err) = status.bind_failures.first().copied() {
            status.bind_failures.remove(0);
            if err != 0 {
                return Err(UrmaOpError { operation: "urma_bind_jetty", status: err });
            }
        }
        status.bound.push((jetty, tjetty));
        // Record the mock routing edge: local jetty <-> remote jetty behind
        // the imported target, so SENDs can be delivered to peer RECVs.
        if let Some(remote) = status.import_targets.get(&tjetty).copied() {
            status.jetty_binds.insert(jetty, remote);
            status.jetty_binds.insert(remote, jetty);
        }
        Ok(())
    }

    fn unbind_jetty(&self, jetty: JettyHandle) -> OpsResult<()> {
        self.status.lock().unwrap().unbound.push(jetty);
        Ok(())
    }

    fn register_seg(&self, _ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg> {
        let handle = TargetSegHandle(self.alloc_handle());
        let mut status = self.status.lock().unwrap();
        let va = 0x1000_0000u64 + (handle.0 as u64) * 0x10_0000;
        let key = 0xdead_beefu32 + handle.0 as u32;
        status.registered.push((addr, len));
        status.seg_windows.insert(handle, (addr, len, va));
        Ok(RegisteredSeg { tseg: handle, local_va: addr as u64, ubva: va, key })
    }

    fn register_local_seg(&self, ctx: CtxHandle, addr: usize, len: u64) -> OpsResult<RegisteredSeg> {
        // Same as register_seg for the mock: LOCAL_ONLY vs remote access is
        // a hardware distinction only.
        self.register_seg(ctx, addr, len)
    }

    fn unregister_seg(&self, _tseg: TargetSegHandle) -> OpsResult<()> {
        self.status.lock().unwrap().unregistered += 1;
        Ok(())
    }

    fn import_seg(&self, _ctx: CtxHandle, remote: &RemoteSegInfo) -> OpsResult<ImportedSeg> {
        self.status.lock().unwrap().segs_imported.push(*remote);
        // The mock maps the remote segment identity-style: reads address
        // the same advertised va.
        Ok(ImportedSeg {
            tseg: TargetSegHandle(self.alloc_handle()),
            mva: remote.va,
        })
    }

    fn unimport_seg(&self, _tseg: TargetSegHandle) -> OpsResult<()> {
        self.status.lock().unwrap().unimported_segs += 1;
        Ok(())
    }

    fn post_read(&self, _jetty: JettyHandle, wr: &ReadWr) -> OpsResult<()> {
        let mut status = self.status.lock().unwrap();
        if status.read_failure != 0 {
            return Err(UrmaOpError { operation: "urma_post_jetty_send_wr", status: status.read_failure });
        }
        status.reads.push(*wr);
        status.reads_log.push(*wr);
        Ok(())
    }

    fn post_send(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()> {
        let mut status = self.status.lock().unwrap();
        if status.send_failure != 0 {
            return Err(UrmaOpError { operation: "urma_post_jetty_send_wr", status: status.send_failure });
        }
        status.sends_log.push(*wr);
        let cf = status.completion_failure;
        status.completed.push(Completion {
            wr_id: wr.wr_id,
            status: cf,
            len: wr.len,
            from: None,
        });
        // The mock jetty id of the posting jetty (mirrors `jetty_id`).
        let sender = JettyId {
            eid: Eid([0x11; URMA_EID_LEN]),
            uasid: 0x1000,
            id: jetty.0 as u32,
        };
        // Deliver to the oldest pending RECV on the destination jetty:
        // the bound peer if known, else the sending jetty (loopback).
        let dst = status.jetty_binds.get(&jetty).copied().unwrap_or(jetty);
        if let Some(pos) = status.recvs.iter().position(|(j, _)| *j == dst) {
            let (_, recv_wr) = status.recvs.remove(pos);
            deliver_mock_message(&mut status, sender, wr, &recv_wr);
        } else {
            // No RECV posted yet; deliver when it arrives.
            status.unmatched_sends.push((dst, *wr, sender));
        }
        Ok(())
    }

    fn post_recv(&self, jetty: JettyHandle, wr: &MsgWr) -> OpsResult<()> {
        let mut status = self.status.lock().unwrap();
        // Complete a previously unmatched SEND waiting on this jetty.
        if let Some(pos) = status
            .unmatched_sends
            .iter()
            .position(|(j, _, _)| *j == jetty)
        {
            let (_, send_wr, sender) = status.unmatched_sends.remove(pos);
            deliver_mock_message(&mut status, sender, &send_wr, wr);
            return Ok(());
        }
        status.recvs.push((jetty, *wr));
        Ok(())
    }

    fn poll_completions(&self, _jfc: JfcHandle, out: &mut Vec<Completion>) -> OpsResult<usize> {
        let mut status = self.status.lock().unwrap();
        let reads = std::mem::take(&mut status.reads);
        let mut n = 0;
        for wr in reads {
            out.push(Completion {
                wr_id: wr.wr_id,
                status: status.completion_failure,
                len: wr.len,
                from: None,
            });
            n += 1;
        }
        let completed = std::mem::take(&mut status.completed);
        for cqe in completed {
            out.push(cqe);
            n += 1;
        }
        Ok(n)
    }
}
