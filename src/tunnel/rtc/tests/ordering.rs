//! Ported verbatim from `p2p/src/rtc/manager/tests/ordering.rs`.
//!
//! `mistlib::EVENT_RAW`/`EVENT_JOIN`/`EVENT_LEAVE` are used directly here
//! (rather than `crate::net`'s re-exported constants of the same value) to
//! stay as close as possible to upstream: `dispatch_event`/`run_payload_worker`
//! are pure functions over a `u32` event-type code and have no notion of
//! "room" (see `event.rs`'s module doc comment), so driving them directly
//! bypasses the room-filtering closure `manager::RTCManagerHandle::new`
//! registers with `crate::net::register_room_handler` -- exactly as
//! upstream's tests bypassed `mistlib::register_raw_handler` to drive
//! `dispatch_event` directly.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::event::{dispatch_event, run_payload_worker};
use super::super::payload::P2pPayload;
use super::super::state::PeerRole;
use super::{encode, test_manager};

/// Regression test for the ordering bug fixed by routing `EVENT_RAW`/
/// `EVENT_OVERLAY` through a single FIFO worker instead of a per-event
/// `tokio::spawn`.
///
/// mistlib delivers events to the raw handler in strict order from a single
/// dispatch thread (see `mistlib-native/src/engine.rs::spawn_event_dispatch_thread`),
/// but `tokio::spawn`ing a task per event does not preserve execution order
/// across tasks on a multi-thread runtime: a later-spawned task can finish
/// before an earlier one if the earlier one's handler takes longer. That
/// reordering is exactly what corrupted `TunnelMessage` sequencing in
/// production (a later `seq` got processed -- and forwarded to the TCP
/// tunnel -- before an earlier one, so the receiver's gap detection closed
/// the connection).
///
/// This drives events through the same `dispatch_event` + `run_payload_worker`
/// pair that `manager::RTCManagerHandle::new` wires together in production
/// (behind its room-filtering closure), so it exercises the real dispatch
/// path rather than only `handle_payload` directly. The first event's
/// handler is made deliberately slow (a synchronous sleep, blocking
/// whichever task/thread executes it) so that, under the old "spawn per
/// event" design, later events would very likely finish first; run on a
/// multi-thread runtime so there are free worker threads for a wrongly-
/// spawned task to race ahead on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_raw_events_is_processed_in_order() {
    let manager = test_manager("self", PeerRole::Client);
    let order = Arc::new(Mutex::new(Vec::new()));

    {
        let order = order.clone();
        manager
            .on_tunnel_message(move |_peer, data| {
                let seq = String::from_utf8(data).unwrap();
                if seq == "0" {
                    // Deliberately slow: if events are still independently
                    // spawned, later ones have every opportunity to
                    // complete first.
                    std::thread::sleep(Duration::from_millis(75));
                }
                order.lock().unwrap().push(seq);
            })
            .await;
    }

    let weak = Arc::downgrade(&manager.inner);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run_payload_worker(weak.clone(), rx));

    const N: u32 = 10;
    for seq in 0..N {
        let payload = encode(P2pPayload::Tunnel {
            data: seq.to_string().into_bytes(),
        });
        dispatch_event(
            &weak,
            &tx,
            mistlib::EVENT_RAW,
            "peer-1".to_string(),
            payload,
        );
    }

    // Give the FIFO worker time to drain the queue (well over the 75ms
    // artificial delay plus (N - 1) fast iterations).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let expected: Vec<String> = (0..N).map(|n| n.to_string()).collect();
    assert_eq!(*order.lock().unwrap(), expected);
}

/// Regression test for (A) in the manager-layer audit: `EVENT_JOIN`/
/// `EVENT_LEAVE` used to be dispatched via independent `tokio::spawn` calls,
/// so a rapid leave->rejoin for the same peer_id could have its join task
/// finish before its leave task, leaving a live peer absent from `peers`.
/// Now that join/leave share the same FIFO worker as payloads, arrival order
/// (leave, then join) is guaranteed to be preserved, so the peer must always
/// end up present. Runs many peer_id's worth of leave->join pairs on a
/// multi-thread runtime to give a reordering bug every opportunity to show
/// up if the fix regressed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_leave_then_join_always_ends_with_peer_present() {
    let manager = test_manager("self", PeerRole::Client);
    let weak = Arc::downgrade(&manager.inner);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run_payload_worker(weak.clone(), rx));

    const ITERATIONS: u32 = 200;
    for i in 0..ITERATIONS {
        let peer_id = format!("peer-{i}");
        dispatch_event(
            &weak,
            &tx,
            mistlib::EVENT_LEAVE,
            peer_id.clone(),
            Vec::new(),
        );
        dispatch_event(&weak, &tx, mistlib::EVENT_JOIN, peer_id, Vec::new());
    }

    // Wait for every enqueued leave/join pair to actually be processed by
    // registering a sentinel tunnel handler and dispatching one last event
    // behind all of them, instead of guessing a sleep duration.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));
    manager
        .on_tunnel_message(move |_peer, _data| {
            if let Some(done_tx) = done_tx.lock().unwrap().take() {
                let _ = done_tx.send(());
            }
        })
        .await;
    let sentinel = encode(P2pPayload::Tunnel { data: vec![0] });
    dispatch_event(
        &weak,
        &tx,
        mistlib::EVENT_RAW,
        "sentinel".to_string(),
        sentinel,
    );
    done_rx.await.unwrap();

    let connected = manager.connected_peers().await;
    for i in 0..ITERATIONS {
        let peer_id = format!("peer-{i}");
        assert!(
            connected.contains(&peer_id),
            "{peer_id} missing after leave->join: reordering regressed"
        );
    }
}
