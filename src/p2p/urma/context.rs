//! URMA context lifecycle and the completion-event reactor thread.
//!
//! liburma is a synchronous polling C API. The [`CompletionDriver`] runs a
//! dedicated OS thread that polls the context JFC and resolves per-request
//! `oneshot` channels keyed by the work request id, bridging completions
//! into the async runtime.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use super::ops::{
    Completion, ContextConfig, CtxHandle, JfcHandle, JettyHandle, JettyId, UrmaOps,
};
use crate::p2p::config::UrmaSettings;
use crate::p2p::error::{Error as P2pError, Result as P2pResult};

/// Poll interval for the reactor thread when the queue is idle.
const POLL_IDLE_INTERVAL: Duration = Duration::from_micros(200);
/// Batch of CQEs fetched per poll call.
const POLL_BATCH: usize = 64;

/// Owns the URMA context, its JFC and the completion reactor.
pub struct UrmaContext {
    ops: Arc<dyn UrmaOps>,
    ctx: CtxHandle,
    jfc: JfcHandle,
    driver: Arc<CompletionDriver>,
    /// Jetties created on this context (data-plane + control-plane).
    jetties: Mutex<Vec<JettyHandle>>,
    driver_thread: Mutex<Option<JoinHandle<()>>>,
    /// Local EID (queried from a bootstrap jetty at context creation).
    local_eid: super::ops::Eid,
    closed: AtomicBool,
}

impl UrmaContext {
    /// Open the configured device, create the context + JFC and start the
    /// completion reactor.
    pub fn new(ops: Arc<dyn UrmaOps>, settings: &UrmaSettings) -> P2pResult<Self> {
        let devices = ops
            .list_devices()
            .map_err(|e| P2pError::internal_message("urma list_devices", e))?;
        if devices.is_empty() {
            return Err(P2pError::Internal(anyhow::anyhow!(
                "urma: no UB devices available (required kernel modules / drivers missing?)"
            )));
        }
        let device = if settings.device.is_empty() {
            devices[0].clone()
        } else if devices.iter().any(|d| d == &settings.device) {
            settings.device.clone()
        } else {
            return Err(P2pError::Internal(anyhow::anyhow!(
                "urma: configured device {:?} not found, available: {devices:?}",
                settings.device
            )));
        };

        let (ctx, jfc) = ops
            .create_context(
                &device,
                &ContextConfig {
                    jfs_count: settings.jfs_count,
                    jfr_count: settings.jfr_count,
                },
            )
            .map_err(|e| P2pError::internal_message("urma create_context", e))?;

        let driver = Arc::new(CompletionDriver {
            ops: ops.clone(),
            jfc,
            pending: Mutex::new(HashMap::new()),
            next_wr_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        });
        let thread = driver.clone().spawn();

        // Bootstrap jetty to learn the local EID; kept in the jetty list so
        // it is cleaned up on shutdown.
        let bootstrap = Self {
            ops,
            ctx,
            jfc,
            driver,
            jetties: Mutex::new(Vec::new()),
            driver_thread: Mutex::new(Some(thread)),
            local_eid: super::ops::Eid([0; super::ops::URMA_EID_LEN]),
            closed: AtomicBool::new(false),
        };
        let (_, bootstrap_id) = bootstrap
            .create_jetty()
            .map_err(|e| P2pError::internal_message("urma bootstrap jetty", e))?;
        let mut context = bootstrap;
        context.local_eid = bootstrap_id.eid;
        Ok(context)
    }

    pub fn handle(&self) -> CtxHandle {
        self.ctx
    }

    pub fn ops(&self) -> Arc<dyn UrmaOps> {
        self.ops.clone()
    }

    /// Local (this node's) EID.
    pub fn local_eid(&self) -> super::ops::Eid {
        self.local_eid
    }

    /// Create an RM-mode duplex jetty on this context and return its advertised id.
    pub fn create_jetty(&self) -> P2pResult<(JettyHandle, JettyId)> {
        let jetty = self
            .ops
            .create_jetty(self.ctx, self.jfc)
            .map_err(|e| P2pError::internal_message("urma create_jetty", e))?;
        let id = self
            .ops
            .jetty_id(jetty)
            .map_err(|e| P2pError::internal_message("urma get_jetty_id", e))?;
        self.jetties.lock().unwrap().push(jetty);
        Ok((jetty, id))
    }

    /// Allocate a fresh work request id for correlating completions.
    pub fn alloc_wr_id(&self) -> u64 {
        self.driver.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register interest in a work request completion. Dropping the receiver
    /// cancels interest.
    pub fn subscribe(&self, wr_id: u64) -> oneshot::Receiver<Completion> {
        let (tx, rx) = oneshot::channel();
        self.driver.pending.lock().unwrap().insert(wr_id, tx);
        rx
    }

    /// Stop the reactor and release all context resources. Idempotent.
    pub fn shutdown(&self) -> P2pResult<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.driver.stopped.store(true, Ordering::SeqCst);
        self.driver.pending.lock().unwrap().clear();
        if let Some(thread) = self.driver_thread.lock().unwrap().take() {
            let _ = thread.join();
        }
        let jetties: Vec<JettyHandle> = self.jetties.lock().unwrap().drain(..).collect();
        for jetty in jetties {
            let _ = self.ops.unbind_jetty(jetty);
            let _ = self.ops.delete_jetty(jetty);
        }
        let _ = self.ops.delete_jfc(self.jfc);
        self.ops
            .delete_context(self.ctx)
            .map_err(|e| P2pError::internal_message("urma delete_context", e))
    }
}

impl Drop for UrmaContext {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Polls the JFC on a dedicated thread and resolves subscribers.
pub struct CompletionDriver {
    ops: Arc<dyn UrmaOps>,
    jfc: JfcHandle,
    pending: Mutex<HashMap<u64, oneshot::Sender<Completion>>>,
    next_wr_id: AtomicU64,
    stopped: AtomicBool,
}

impl CompletionDriver {
    fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::spawn(move || self.run())
    }

    fn run(&self) {
        let mut cqes = Vec::with_capacity(POLL_BATCH);
        while !self.stopped.load(Ordering::SeqCst) {
            cqes.clear();
            match self.ops.poll_completions(self.jfc, &mut cqes) {
                Ok(0) => std::thread::sleep(POLL_IDLE_INTERVAL),
                Ok(_) => {
                    let mut pending = self.pending.lock().unwrap();
                    for cqe in cqes.drain(..) {
                        if let Some(tx) = pending.remove(&cqe.wr_id) {
                            let _ = tx.send(cqe);
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!("urma completion poll error: {err}");
                    std::thread::sleep(POLL_IDLE_INTERVAL);
                }
            }
        }
        // Wake waiters still parked on completions that will never arrive.
        let mut pending = self.pending.lock().unwrap();
        for (_, tx) in pending.drain() {
            let _ = tx.send(Completion {
                wr_id: 0,
                status: -1,
                len: 0,
                from: None,
            });
        }
    }
}

#[cfg(test)]
impl UrmaContext {
    /// Test helper: build a context backed by mock ops with default settings.
    pub fn mock(ops: Arc<dyn UrmaOps>) -> P2pResult<Self> {
        Self::new(ops, &UrmaSettings::default())
    }
}
