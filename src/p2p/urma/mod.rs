//! URMA (Unified Remote Memory Access) P2P transport backend.
//!
//! Uses URMA RM (reliable message) transport over the UB (Unified Bus)
//! fabric: remote jettys are imported connectionlessly, one-sided READs
//! pull published artifacts from peer nodes, and a small SEND/RECV catalog
//! protocol resolves artifact keys. See the module docs of the submodules
//! for details.
//!
//! Without the `p2p-urma` feature only the backend id constant is compiled,
//! so configuration parsing works everywhere while the data path requires a
//! UMDK build.

pub const URMA_BACKEND_ID: &str = "urma";

#[cfg(any(test, feature = "p2p-urma"))]
mod catalog;
#[cfg(any(test, feature = "p2p-urma"))]
mod context;
#[cfg(any(test, feature = "p2p-urma"))]
mod ctp;
#[cfg(feature = "p2p-urma")]
mod ffi;
#[cfg(any(test, feature = "p2p-urma"))]
mod ops;
#[cfg(any(test, feature = "p2p-urma"))]
mod rm;
#[cfg(any(test, feature = "p2p-urma"))]
mod segment;
#[cfg(any(test, feature = "p2p-urma"))]
mod transport;

#[cfg(any(test, feature = "p2p-urma"))]
pub use transport::UrmaP2pTransport;
