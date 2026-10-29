//! Room-scoped durable blocks exposed through mistlib's storage service.
use super::Service;
use mistlib::storage::{BlockProvider, register_block_provider};
use mistlib_core::types::NodeId;
use std::sync::{Arc, Weak};

// The service owns the registration. A weak reference avoids a registry/service cycle.
pub(super) struct EscrowBlockProvider(pub Weak<Service>);

#[async_trait::async_trait]
impl BlockProvider for EscrowBlockProvider {
    async fn get_block(&self, room_id: &str, from: &NodeId, cid: &str) -> Option<Vec<u8>> {
        let service = self.0.upgrade()?;
        if !service.cfg.rooms.iter().any(|room| room == room_id) {
            return None;
        }
        let local = service.repo.block(room_id, cid).or_else(|| {
            service
                .cluster(room_id)
                .and_then(|cluster| cluster.block(cid))
        });
        if local.is_some() {
            return local;
        }
        service.mesh(room_id)?.proxy(room_id, &from.0, cid).await
    }
}

impl Service {
    pub(super) fn register_block_provider(self: &Arc<Self>) {
        let mut registration = self.block_provider.lock().unwrap();
        if registration.is_none() {
            *registration = Some(register_block_provider(Arc::new(EscrowBlockProvider(
                Arc::downgrade(self),
            ))));
        }
    }
}
