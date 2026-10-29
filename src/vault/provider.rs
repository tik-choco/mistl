//! Verified, active room members alone can read the ciphertext store.
use super::service::Service;
use mistlib::storage::{BlockProvider, register_block_provider};
use mistlib_core::types::NodeId;
use std::sync::{Arc, Weak};

struct VaultProvider(Weak<Service>);

fn authorized(group: &super::group::Group, room: &str, did: Option<&str>, local: &str) -> bool {
    group.room() == room && did.is_some_and(|did| group.is_member(did)) && group.is_member(local)
}

#[async_trait::async_trait]
impl BlockProvider for VaultProvider {
    async fn get_block(&self, room: &str, from: &NodeId, cid: &str) -> Option<Vec<u8>> {
        let service = self.0.upgrade()?;
        if !service.app.network.permitted() || cid.len() > 128 {
            return None;
        }
        let _permit = service.serving.clone().try_acquire_owned().ok()?;
        let did = crate::net::peer_auth::verified_did(room, &from.0)?;
        let (room, cid) = (room.to_string(), cid.to_string());
        service
            .work(move |core| {
                let allowed = core
                    .group
                    .as_ref()
                    .is_some_and(|g| authorized(g, &room, Some(&did), core.me.did()));
                Ok(if allowed { core.store.get(&cid) } else { None })
            })
            .await
            .ok()
            .flatten()
    }
}

impl Service {
    pub(super) fn register_provider(self: &Arc<Self>) {
        let mut provider = self.provider.lock().expect("vault provider lock");
        if provider.is_none() {
            *provider = Some(register_block_provider(Arc::new(VaultProvider(
                Arc::downgrade(self),
            ))));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::{group::Group, keys::OwnerKey};

    #[test]
    fn provider_requires_verified_active_room_members() {
        let creator = OwnerKey::generate().identity();
        let peer = OwnerKey::generate().identity();
        let mut group = Group::create(&creator, 1).unwrap();
        let room = group.room();
        assert!(!authorized(&group, &room, None, creator.did()));
        assert!(!authorized(&group, &room, Some(peer.did()), creator.did()));
        group.admit(&creator, peer.did(), 2).unwrap();
        assert!(authorized(&group, &room, Some(peer.did()), creator.did()));
        assert!(!authorized(
            &group,
            "another-room",
            Some(peer.did()),
            creator.did()
        ));
        group.revoke(&creator, peer.did(), 3).unwrap();
        assert!(!authorized(&group, &room, Some(peer.did()), creator.did()));
        assert!(!authorized(&group, &room, Some(creator.did()), peer.did()));
    }
}
