//! All externally active startup services share the same connection gate.
use super::AppState;
use std::sync::Arc;

pub(super) fn spawn(state: Arc<AppState>) {
    crate::ai::migrate_provide_state();
    if !state.network.permitted() {
        return;
    }
    crate::ai::spawn_http_refresh(state.clone(), None);
    // Background self-update: periodically check GitHub Releases and, if
    // enabled, stage a newer binary (applied on the next daemon start).
    crate::update::spawn_auto_update(state.clone());

    // tc-chat room relay/bot: starts only if `[chat_relay] enabled` and
    // `rooms` are configured (a no-op otherwise). Spawned eagerly here --
    // rather than lazily on first `chat.*` IPC call, like ai/stream's own
    // services -- because the whole point is receiving tc-chat traffic
    // while nobody is asking.
    crate::chat_relay::spawn_background(state.clone());

    // mistl-to-mistl direct messages: registers the inbound handler so
    // messages from verified peers are received while nobody is asking.
    crate::dm::spawn_background(state.clone());

    // tc-storage folder-share sync (requester side) and owner
    // responder/announcer: both no-ops unless syncs/shares are persisted.
    crate::storage::folder_sync::spawn_background(state.clone());
    crate::storage::folder_owner::spawn_background(state.clone());

    // Cron-like job scheduler: fires due jobs on a 1-second tick. A no-op
    // (logged) when `[scheduler] enabled = false`.
    crate::scheduler::spawn_background(state.clone());

    // Bot pipeline engine: source -> transform(s) -> sink(s) automation
    // runs, fired on a 1-second tick. A no-op (logged) when
    // `[bot] enabled = false`.
    crate::bot::spawn_background(state.clone());

    // Join referenced rooms and provide according to enabled Room flags.
    crate::ai::spawn_room_connections(state.clone());

    // Restore the local OpenAI-compatible API listener when it was left
    // running. Its bind address still comes from the current config.
    crate::ai::spawn_serve_autoresume(state.clone());

    // WebRTC P2P tunnel (ported from the standalone `p2p` tool): joins
    // `[tunnel] room_id` and restores persisted forwards only if `[tunnel]
    // enabled = true`. A no-op (logged) otherwise -- `mistl tunnel start`
    // (CLI, dashboard, or TUI) still starts it manually regardless, exactly
    // like `crate::scheduler`/`crate::bot`'s own `enabled` flags gate only
    // their background loops, not their manual commands.
    crate::tunnel::spawn_background(state.clone());
}
