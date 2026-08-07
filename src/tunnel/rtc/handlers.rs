//! Handler registration methods for [`super::manager::RTCManagerHandle`]:
//! chat/tunnel/stdio message subscriptions, tunnel/stdio open/close
//! notifications, peer connect/join/leave hooks, and forward request/
//! response subscriptions.
//!
//! Ported verbatim from `p2p/src/rtc/manager/handlers.rs`. The only change
//! is the import path for `RTCManagerHandle`/`ForwardRequestEvent`/
//! `ForwardResponseEvent`: they now live in the sibling `manager` module
//! rather than this module's direct parent, because this port's file
//! ownership (`TUNNEL_INTEGRATION_CONTRACT.md`) places `event.rs`,
//! `handlers.rs`, `payload.rs`, and `state.rs` directly under `rtc/` instead
//! of nested under `rtc/manager/` the way upstream had them.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::manager::RTCManagerHandle;
use super::state::DataHandlerEntry;

#[allow(dead_code)]
impl RTCManagerHandle {
    pub async fn on_chat_message<F: Fn(String, String) + Send + Sync + 'static>(&self, f: F) {
        self.inner.chat_handlers.write().await.push(Arc::new(f));
    }

    pub async fn on_forward_request<
        F: Fn(String, super::manager::ForwardRequestEvent) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) {
        self.inner
            .forward_request_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_forward_response<
        F: Fn(String, super::manager::ForwardResponseEvent) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) {
        self.inner
            .forward_response_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_message<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(
        &self,
        f: F,
    ) -> u64 {
        let id = self
            .inner
            .next_tunnel_handler_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .tunnel_msg_handlers
            .write()
            .await
            .push(DataHandlerEntry {
                id,
                target: None,
                handler: Arc::new(f),
            });
        id
    }

    pub async fn on_tunnel_message_for<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(
        &self,
        target: String,
        f: F,
    ) -> u64 {
        {
            let mut default = self.inner.default_tunnel_target.write().await;
            if default.is_none() {
                *default = Some(target.clone());
            }
        }

        let id = self
            .inner
            .next_tunnel_handler_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .tunnel_msg_handlers
            .write()
            .await
            .push(DataHandlerEntry {
                id,
                target: Some(target),
                handler: Arc::new(f),
            });
        id
    }

    pub async fn remove_tunnel_message_handler(&self, id: u64) -> bool {
        let mut handlers = self.inner.tunnel_msg_handlers.write().await;
        let removed_target = handlers
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.target.clone());
        let old_len = handlers.len();
        handlers.retain(|entry| entry.id != id);
        let removed = handlers.len() != old_len;
        drop(handlers);

        if removed_target.is_some() {
            self.refresh_default_tunnel_target().await;
        }

        removed
    }

    async fn refresh_default_tunnel_target(&self) {
        let handlers = self.inner.tunnel_msg_handlers.read().await;
        let next_default = handlers.iter().find_map(|entry| entry.target.clone());
        drop(handlers);

        *self.inner.default_tunnel_target.write().await = next_default;
    }

    pub async fn on_stdio_message<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_msg_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_open<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .tunnel_open_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_stdio_open<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_open_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_close<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .tunnel_close_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_stdio_close<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_close_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_peer_connected<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .peer_conn_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    /// Registers `f` to run when a peer's `EVENT_JOIN` is processed, with
    /// its post-increment session epoch (see [`RTCManagerHandle::peer_epoch`]).
    /// `f` runs synchronously on the manager's serialized event worker; if it
    /// needs to do async work, spawn a task from within it (as
    /// `on_tunnel_close` callers already do), rather than blocking here.
    pub async fn on_peer_join<F: Fn(String, u64) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .peer_join_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    /// Registers `f` to run when a peer's `EVENT_LEAVE` is processed, with
    /// the session epoch the peer held at the time it left (see
    /// [`RTCManagerHandle::peer_epoch`]). Same execution contract as
    /// [`RTCManagerHandle::on_peer_join`].
    pub async fn on_peer_leave<F: Fn(String, u64) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .peer_leave_handlers
            .write()
            .await
            .push(Arc::new(f));
    }
}
