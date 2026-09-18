//! Raw FFI bindings to liburma (UMDK URMA user API), version 0.9.
//!
//! Struct layouts and function signatures mirror `/usr/include/ub/umdk/urma/urma_api.h`
//! and `urma_types.h` from the umdk-urma-devel package (25.12.0). All
//! `repr(C)` items were laid out field-by-field against those headers.
//! Only [`super::ops`] (`RealUrmaOps`) touches these declarations, so a
//! mismatch is contained to a single file.
//!
//! Key facts encoded here:
//! - UB devices (`udma*` / `bonding_dev_*`) operate in `URMA_TM_RM`
//!   (reliable message, connectionless): posting a WR only needs the
//!   imported remote jetty, `urma_bind_jetty` is RC-only.
//! - UB transport only supports jettys created with the `share_jfr` flag
//!   and an explicitly created JFR attached via `jetty_cfg.shared.jfr`.
//! - One-sided READ addressing uses the locally imported segment's `mva`
//!   (mapping address) when imported with `URMA_SEG_MAPPED`.
#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use std::ffi::c_void;

// ---------------------------------------------------------------------------
// Status / enums
// ---------------------------------------------------------------------------

/// `urma_status_t` (int). Success is `URMA_SUCCESS == 0`.
pub type urma_status_t = i32;
pub const URMA_SUCCESS: urma_status_t = 0;

/// `urma_transport_mode_t`.
pub type urma_transport_mode_t = u32;
pub const URMA_TM_RM: urma_transport_mode_t = 0x1;
pub const URMA_TM_RC: urma_transport_mode_t = 0x2;

/// `urma_tp_type_t`.
pub type urma_tp_type_t = u32;
pub const URMA_RTP: urma_tp_type_t = 0;
pub const URMA_CTP: urma_tp_type_t = 1;

/// `urma_target_type_t`.
pub type urma_target_type_t = u32;
pub const URMA_JETTY: urma_target_type_t = 1;

/// `urma_opcode_t` values used by the backend.
pub type urma_opcode_t = u32;
pub const URMA_OPC_READ: urma_opcode_t = 0x10;
pub const URMA_OPC_SEND: urma_opcode_t = 0x40;

/// `urma_cr_status_t` values (completion record status).
pub type urma_cr_status_t = i32;
pub const URMA_CR_SUCCESS: urma_cr_status_t = 0;

// ---------------------------------------------------------------------------
// Access flag bit fields
// ---------------------------------------------------------------------------

/// Raw access capabilities (`URMA_ACCESS_*`).
pub const URMA_ACCESS_LOCAL_ONLY: u32 = 0x1;
pub const URMA_ACCESS_READ: u32 = 0x1 << 1;
pub const URMA_ACCESS_WRITE: u32 = 0x1 << 2;
pub const URMA_ACCESS_ATOMIC: u32 = 0x1 << 3;

/// `urma_reg_seg_flag_t` / `urma_seg_attr_t` bit layout:
/// token_policy:3 | cacheable:1 | dsva:1 | **access:6** | non_pin:1 | user_iova:1 | **token_id_valid:1** | reserved:18
/// so the access field starts at bit 5, token_id_valid at bit 13.
pub const REG_SEG_FLAG_ACCESS_READ: u32 = URMA_ACCESS_READ << 5;
/// READ|WRITE|ATOMIC — required for RECV buffers (data is written into them).
pub const REG_SEG_FLAG_ACCESS_RW_ATOMIC: u32 = (URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC) << 5;
/// LOCAL_ONLY access — required for two-sided SEND/RECV buffers. Mutually
/// exclusive with the remote-access bits (combined registration fails).
pub const REG_SEG_FLAG_ACCESS_LOCAL_ONLY: u32 = URMA_ACCESS_LOCAL_ONLY << 5;
pub const REG_SEG_FLAG_TOKEN_ID_VALID: u32 = 1 << 13;

/// `urma_import_seg_flag_t` bit layout:
/// cacheable:1 | **access:6** | **mapping:1** | reserved:24.
pub const IMPORT_SEG_FLAG_ACCESS_READ: u32 = URMA_ACCESS_READ << 1;
pub const IMPORT_SEG_FLAG_MAPPED: u32 = 1 << 7;

/// `urma_jfs_wr_flag_t` bit layout:
/// place_order:2 | comp_order:1 | fence:1 | solicited:1 | **complete_enable:1** | ...
/// so complete_enable is bit 5. Required for the WR to produce a JFC entry.
pub const JFS_WR_FLAG_COMPLETE_ENABLE: u32 = 1 << 5;

/// `urma_jetty_flag_t`: share_jfr is bit 0 (mandatory for UB).
pub const JETTY_FLAG_SHARE_JFR: u32 = 1;

// ---------------------------------------------------------------------------
// Core value types
// ---------------------------------------------------------------------------

pub const URMA_EID_SIZE: usize = 16;

/// `urma_eid_t`: union with `u64` members, so natural alignment is 8.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct urma_eid_t {
    pub raw: [u8; URMA_EID_SIZE],
}

/// `urma_jetty_id_t`: identifies a jetty on the fabric.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct urma_jetty_id_t {
    pub eid: urma_eid_t,
    pub uasid: u32,
    pub id: u32,
}

/// `urma_token_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct urma_token_t {
    pub token: u32,
}

/// `urma_init_attr_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct urma_init_attr_t {
    pub token: u64,
    pub uasid: u32,
}

/// `urma_device_t`: only the leading public `name` field is read.
#[repr(C)]
pub struct urma_device_t {
    pub name: [std::os::raw::c_char; 64],
    pub path: [std::os::raw::c_char; 4096],
    pub type_: urma_transport_type_t,
    pub ops: *mut c_void,
    pub sysfs_dev: *mut c_void,
}

/// `urma_transport_type_t`.
pub type urma_transport_type_t = i32;

/// `urma_eid_info_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_eid_info_t {
    pub eid: urma_eid_t,
    pub eid_index: u32,
}

// ---------------------------------------------------------------------------
// JFC / JFS / JFR / jetty configuration
// ---------------------------------------------------------------------------

/// `urma_jfc_cfg_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_jfc_cfg_t {
    pub depth: u32,
    pub flag: u32,
    pub ceqn: u32,
    pub jfce: *mut c_void,
    pub user_ctx: u64,
}

/// `urma_jfs_cfg_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_jfs_cfg_t {
    pub depth: u32,
    pub flag: u32,
    pub trans_mode: urma_transport_mode_t,
    pub priority: u8,
    pub max_sge: u8,
    pub max_rsge: u8,
    pub max_inline_data: u32,
    pub rnr_retry: u8,
    pub err_timeout: u8,
    pub jfc: *mut urma_jfc_t,
    pub user_ctx: u64,
}

/// `urma_jfr_cfg_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_jfr_cfg_t {
    pub id: u32,
    pub depth: u32,
    pub flag: u32,
    pub trans_mode: urma_transport_mode_t,
    pub max_sge: u8,
    pub min_rnr_timer: u8,
    pub jfc: *mut urma_jfc_t,
    pub token_value: urma_token_t,
    pub user_ctx: u64,
}

/// `urma_jetty_cfg_t` receive-side union (both arms are pointers).
#[repr(C)]
#[derive(Clone, Copy)]
pub union urma_jetty_recv_u {
    /// `shared` arm (required on UB).
    pub shared: urma_jetty_shared_t,
    /// `jfr_cfg` arm (deprecated upstream).
    pub jfr_cfg: *mut urma_jfr_cfg_t,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_jetty_shared_t {
    pub jfr: *mut urma_jfr_t,
    pub jfc: *mut urma_jfc_t,
}

/// `urma_jetty_cfg_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_jetty_cfg_t {
    pub id: u32,
    pub flag: u32,
    pub jfs_cfg: urma_jfs_cfg_t,
    pub recv: urma_jetty_recv_u,
    pub jetty_grp: *mut c_void,
    pub user_ctx: u64,
}

/// `urma_rjetty_t`: serializable remote jetty descriptor.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_rjetty_t {
    pub jetty_id: urma_jetty_id_t,
    pub trans_mode: urma_transport_mode_t,
    pub policy: u32,
    pub type_: urma_target_type_t,
    pub flag: u32,
    pub tp_type: urma_tp_type_t,
}

// ---------------------------------------------------------------------------
// Segment types
// ---------------------------------------------------------------------------

/// `urma_seg_cfg_t` (registration).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_seg_cfg_t {
    pub va: u64,
    pub len: u64,
    pub token_id: *mut urma_token_id_t,
    pub token_value: urma_token_t,
    pub flag: u32,
    pub user_ctx: u64,
    pub iova: u64,
}

/// `urma_ubva_t` (`__attribute__((packed))`). The EID is raw bytes here
/// because the packed layout forces 1-byte alignment.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct urma_ubva_t {
    pub eid: [u8; URMA_EID_SIZE],
    pub uasid: u32,
    pub va: u64,
}

/// `urma_seg_t`: serializable segment descriptor (from `urma_get_seg_ctx`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_seg_t {
    pub ubva: urma_ubva_t,
    pub len: u64,
    pub attr: u32,
    pub token_id: u32,
}

// ---------------------------------------------------------------------------
// Work requests / completions
// ---------------------------------------------------------------------------

/// `urma_sge_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_sge_t {
    pub addr: u64,
    pub len: u32,
    pub tseg: *mut urma_target_seg_t,
    pub user_tseg: *mut c_void,
}

/// `urma_sg_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_sg_t {
    pub sge: *mut urma_sge_t,
    pub num_sge: u32,
}

/// `urma_rw_wr_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_rw_wr_t {
    pub src: urma_sg_t,
    pub dst: urma_sg_t,
    pub target_hint: u8,
    pub notify_data: u64,
}

/// `urma_send_wr_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_send_wr_t {
    pub src: urma_sg_t,
    pub target_hint: u8,
    pub imm_data: u64,
    pub tseg: *mut urma_target_seg_t,
}

/// `urma_jfs_wr_t` operation union (cas/faa arms are smaller than rw).
#[repr(C)]
#[derive(Clone, Copy)]
pub union urma_jfs_op_u {
    pub rw: urma_rw_wr_t,
    pub send: urma_send_wr_t,
}

/// `urma_jfs_wr_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_jfs_wr_t {
    pub opcode: urma_opcode_t,
    pub flag: u32,
    pub tjetty: *mut urma_target_jetty_t,
    pub user_ctx: u64,
    pub op: urma_jfs_op_u,
    pub next: *mut urma_jfs_wr_t,
}

/// `urma_jfr_wr_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_jfr_wr_t {
    pub src: urma_sg_t,
    pub user_ctx: u64,
    pub next: *mut urma_jfr_wr_t,
}

/// `urma_cr_t` (completion record).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct urma_cr_t {
    pub status: urma_cr_status_t,
    pub user_ctx: u64,
    pub opcode: u32,
    pub flag: u8,
    pub completion_len: u32,
    pub local_id: u32,
    pub remote_id: urma_jetty_id_t,
    pub imm_data: u64,
    pub tpn: u32,
    pub user_data: usize,
}

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct urma_context_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct urma_jfc_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct urma_jfr_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct urma_jetty_t {
    /// Prefix of the public layout; only `jetty_id` (offset 8) is read.
    pub _urma_ctx: *mut c_void,
    pub jetty_id: urma_jetty_id_t,
}

#[repr(C)]
pub struct urma_target_jetty_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct urma_token_id_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct urma_target_seg_t {
    /// Prefix of the public layout; `mva` (offset 56) is read after a
    /// mapped import.
    _seg: urma_seg_t,
    _user_ctx: u64,
    pub mva: u64,
}

// ---------------------------------------------------------------------------
// liburma entry points
// ---------------------------------------------------------------------------

#[link(name = "urma")]
extern "C" {
    /// Initialize the URMA environment. `conf == null` picks a random uasid.
    pub fn urma_init(conf: *mut urma_init_attr_t) -> urma_status_t;
    pub fn urma_uninit() -> urma_status_t;

    // Device management
    pub fn urma_get_device_list(num_devices: *mut i32) -> *mut *mut urma_device_t;
    pub fn urma_free_device_list(device_list: *mut *mut urma_device_t);
    pub fn urma_get_device_by_name(dev_name: *mut std::os::raw::c_char) -> *mut urma_device_t;
    pub fn urma_get_eid_list(dev: *mut urma_device_t, cnt: *mut u32) -> *mut urma_eid_info_t;
    pub fn urma_free_eid_list(eid_list: *mut urma_eid_info_t);

    // Context
    pub fn urma_create_context(
        dev: *mut urma_device_t,
        eid_index: u32,
    ) -> *mut urma_context_t;
    pub fn urma_delete_context(ctx: *mut urma_context_t) -> urma_status_t;

    // JFC (completion queue)
    pub fn urma_create_jfc(ctx: *mut urma_context_t, jfc_cfg: *mut urma_jfc_cfg_t) -> *mut urma_jfc_t;
    pub fn urma_delete_jfc(jfc: *mut urma_jfc_t) -> urma_status_t;

    // Standalone JFR (receive jetty) for share_jfr jetty creation
    pub fn urma_create_jfr(ctx: *mut urma_context_t, jfr_cfg: *mut urma_jfr_cfg_t) -> *mut urma_jfr_t;
    pub fn urma_delete_jfr(jfr: *mut urma_jfr_t) -> urma_status_t;

    // Jetty (jfs + jfr pair)
    pub fn urma_create_jetty(
        ctx: *mut urma_context_t,
        jetty_cfg: *mut urma_jetty_cfg_t,
    ) -> *mut urma_jetty_t;
    pub fn urma_delete_jetty(jetty: *mut urma_jetty_t) -> urma_status_t;

    /// Serialize the local jetty's remote-viewable id (allocation freed by
    /// `urma_put_rjetty`).
    pub fn urma_get_rjetty(
        jetty: *mut urma_jetty_t,
        rjetty: *mut *mut urma_rjetty_t,
        length: *mut u32,
    ) -> urma_status_t;
    pub fn urma_put_rjetty(rjetty: *mut urma_rjetty_t);

    // Remote jetty import (RTP / RM).
    pub fn urma_import_jetty(
        ctx: *mut urma_context_t,
        rjetty: *mut urma_rjetty_t,
        token_value: *mut urma_token_t,
    ) -> *mut urma_target_jetty_t;
    pub fn urma_unimport_jetty(tjetty: *mut urma_target_jetty_t) -> urma_status_t;

    /// RC-mode channel construction (not used on UB RM hardware).
    pub fn urma_bind_jetty(
        jetty: *mut urma_jetty_t,
        tjetty: *mut urma_target_jetty_t,
    ) -> urma_status_t;
    pub fn urma_unbind_jetty(jetty: *mut urma_jetty_t) -> urma_status_t;

    // Token ids (protection table keys)
    pub fn urma_alloc_token_id(ctx: *mut urma_context_t) -> *mut urma_token_id_t;
    pub fn urma_free_token_id(token_id: *mut urma_token_id_t) -> urma_status_t;

    // Segments
    pub fn urma_register_seg(
        ctx: *mut urma_context_t,
        seg_cfg: *mut urma_seg_cfg_t,
    ) -> *mut urma_target_seg_t;
    pub fn urma_unregister_seg(target_seg: *mut urma_target_seg_t) -> urma_status_t;
    pub fn urma_import_seg(
        ctx: *mut urma_context_t,
        seg: *mut urma_seg_t,
        token_value: *mut urma_token_t,
        addr: u64,
        flag: u32,
    ) -> *mut urma_target_seg_t;
    pub fn urma_unimport_seg(tseg: *mut urma_target_seg_t) -> urma_status_t;
    /// Serialize a registered segment (allocation freed by `urma_put_seg_ctx`).
    pub fn urma_get_seg_ctx(
        tseg: *mut urma_target_seg_t,
        seg: *mut *mut urma_seg_t,
        size: *mut u32,
    ) -> urma_status_t;
    pub fn urma_put_seg_ctx(seg: *mut urma_seg_t);

    // Work request submission
    pub fn urma_post_jetty_send_wr(
        jetty: *mut urma_jetty_t,
        wr: *mut urma_jfs_wr_t,
        bad_wr: *mut *mut urma_jfs_wr_t,
    ) -> urma_status_t;
    pub fn urma_post_jetty_recv_wr(
        jetty: *mut urma_jetty_t,
        wr: *mut urma_jfr_wr_t,
        bad_wr: *mut *mut urma_jfr_wr_t,
    ) -> urma_status_t;

    // Completion polling
    /// Returns the number of CRs written (0 = none), or a negative error.
    /// At most 16 CRs per call on RDMA devices.
    pub fn urma_poll_jfc(jfc: *mut urma_jfc_t, cr_cnt: i32, cr: *mut urma_cr_t) -> i32;
}
