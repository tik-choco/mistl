//! Room-scoped voice requests. Replies are bound to the transport sender;
//! bounded channels, request caps and inactivity timeouts limit peer-driven memory.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use tokio::sync::mpsc;

use super::protocol::{self, ProtocolMessage};
use super::{SendFn, stt, tts};

const MAX_REQUESTS: usize = 8;
const MAX_AUDIO_BYTES: usize = 32 * 1024 * 1024;
const CHUNK_BYTES: usize = 9 * 1024;

pub(super) struct VoiceConsumer {
    send: SendFn,
    pending: Mutex<HashMap<String, (String, mpsc::Sender<ProtocolMessage>)>>,
}

struct PendingGuard<'a> {
    consumer: &'a VoiceConsumer,
    id: String,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.consumer
            .pending
            .lock()
            .expect("voice pending lock")
            .remove(&self.id);
    }
}

impl VoiceConsumer {
    pub fn new(send: SendFn) -> Arc<Self> {
        Arc::new(Self {
            send,
            pending: Mutex::new(HashMap::new()),
        })
    }

    pub fn handle_message(&self, from: &str, msg: &ProtocolMessage) {
        let (id, within_limit) = match msg {
            ProtocolMessage::TtsResponse { id, data, mime, .. } => {
                (id, data.len() <= 16 * 1024 && mime.len() <= 256)
            }
            ProtocolMessage::SttResponse { id, text } => (id, text.len() <= 1024 * 1024),
            ProtocolMessage::VoiceError { id, message, .. } => (id, message.len() <= 4096),
            _ => return,
        };
        let mut pending = self.pending.lock().expect("voice pending lock");
        if let Some((peer, tx)) = pending.get(id) {
            if peer != from {
                return;
            }
            if !within_limit || tx.try_send(msg.clone()).is_err() {
                pending.remove(id);
            }
        }
    }

    pub fn on_peer_left(&self, from: &str) {
        self.pending
            .lock()
            .expect("voice pending lock")
            .retain(|_, (peer, _)| peer != from);
    }

    pub fn reject_all(&self) {
        self.pending.lock().expect("voice pending lock").clear();
    }

    fn begin(&self, peer: &str) -> Result<(PendingGuard<'_>, mpsc::Receiver<ProtocolMessage>)> {
        let mut pending = self.pending.lock().expect("voice pending lock");
        if pending.len() >= MAX_REQUESTS {
            bail!("ai: too many pending voice requests");
        }
        let id = protocol::random_id();
        let (tx, rx) = mpsc::channel(8);
        pending.insert(id.clone(), (peer.to_string(), tx));
        Ok((PendingGuard { consumer: self, id }, rx))
    }

    async fn receive(
        rx: &mut mpsc::Receiver<ProtocolMessage>,
        timeout: Duration,
    ) -> Result<ProtocolMessage> {
        let msg = tokio::time::timeout(timeout, rx.recv())
            .await
            .context("ai: voice request timed out")?
            .context("ai: voice request closed (peer left, queue overflow or invalid response)")?;
        if let ProtocolMessage::VoiceError { message, code, .. } = msg {
            bail!(
                "ai: {message} ({})",
                code.as_deref().unwrap_or("voice_error")
            );
        }
        Ok(msg)
    }

    pub async fn synthesize(
        &self,
        peer: &str,
        req: tts::TtsParams,
        lang: Option<String>,
        timeout: Duration,
    ) -> Result<tts::TtsAudio> {
        if req.input.chars().count() > tts::MAX_INPUT_CHARS {
            bail!("ai: TTS input too large");
        }
        let (guard, mut rx) = self.begin(peer)?;
        (self.send)(
            peer,
            ProtocolMessage::TtsRequest {
                id: guard.id.clone(),
                text: req.input,
                model: (!req.model.is_empty() && req.model != "network-auto").then_some(req.model),
                voice: (!req.voice.is_empty()).then_some(req.voice),
                lang,
            },
        );
        let mut bytes = Vec::new();
        let mut expected_seq = 0;
        loop {
            let ProtocolMessage::TtsResponse {
                seq,
                data,
                last,
                mime,
                ..
            } = Self::receive(&mut rx, timeout).await?
            else {
                bail!("ai: unexpected TTS response");
            };
            if seq < expected_seq {
                continue;
            }
            if seq != expected_seq {
                bail!("ai: TTS response sequence gap");
            }
            expected_seq += 1;
            let chunk = base64::engine::general_purpose::STANDARD
                .decode(data)
                .context("ai: invalid TTS base64")?;
            if bytes.len().saturating_add(chunk.len()) > MAX_AUDIO_BYTES {
                bail!("ai: TTS response too large");
            }
            bytes.extend_from_slice(&chunk);
            if last {
                return Ok(tts::TtsAudio { bytes, mime });
            }
        }
    }

    pub async fn transcribe(
        &self,
        peer: &str,
        req: stt::SttParams,
        timeout: Duration,
    ) -> Result<String> {
        if req.audio.len() > 25 * 1024 * 1024 {
            bail!("ai: STT audio too large");
        }
        let (guard, mut rx) = self.begin(peer)?;
        let count = req.audio.len().div_ceil(CHUNK_BYTES).max(1);
        let model = (!req.model.is_empty() && req.model != "network-auto").then_some(req.model);
        for seq in 0..count {
            let start = seq * CHUNK_BYTES;
            let end = (start + CHUNK_BYTES).min(req.audio.len());
            (self.send)(
                peer,
                ProtocolMessage::SttRequest {
                    id: guard.id.clone(),
                    seq: seq as u64,
                    data: base64::engine::general_purpose::STANDARD.encode(&req.audio[start..end]),
                    last: seq + 1 == count,
                    mime: req.mime.clone(),
                    model: model.clone(),
                    file_name: req.file_name.clone(),
                },
            );
            // Let the bounded wire send queue drain for long uploads.
            tokio::task::yield_now().await;
        }
        match Self::receive(&mut rx, timeout).await? {
            ProtocolMessage::SttResponse { text, .. } => Ok(text),
            _ => bail!("ai: unexpected STT response"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tts_network_auto_omits_model_and_reassembles_audio() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let consumer = VoiceConsumer::new(Arc::new(move |peer, msg| {
            tx.send((peer.to_string(), msg)).unwrap();
        }));
        let task = {
            let consumer = consumer.clone();
            tokio::spawn(async move {
                consumer
                    .synthesize(
                        "owner",
                        tts::TtsParams {
                            model: "network-auto".into(),
                            voice: String::new(),
                            input: "hello".into(),
                            format: None,
                            speed: None,
                        },
                        Some("en".into()),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };
        let (peer, msg) = rx.recv().await.unwrap();
        assert_eq!(peer, "owner");
        let ProtocolMessage::TtsRequest {
            id,
            model,
            voice,
            lang,
            ..
        } = msg
        else {
            panic!("TTS request");
        };
        assert!(model.is_none());
        assert!(voice.is_none());
        assert_eq!(lang.as_deref(), Some("en"));
        for (seq, bytes, last) in [(0, "hello", false), (1, " world", true)] {
            consumer.handle_message(
                "owner",
                &ProtocolMessage::TtsResponse {
                    id: id.clone(),
                    seq,
                    data: base64::engine::general_purpose::STANDARD.encode(bytes),
                    last,
                    mime: "audio/wav".into(),
                },
            );
        }
        let audio = task.await.unwrap().unwrap();
        assert_eq!(audio.bytes, b"hello world");
        assert_eq!(audio.mime, "audio/wav");
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stt_sends_ordered_chunks_and_uses_sender_bound_response() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let consumer = VoiceConsumer::new(Arc::new(move |_, msg| {
            tx.send(msg).unwrap();
        }));
        let task = {
            let consumer = consumer.clone();
            tokio::spawn(async move {
                consumer
                    .transcribe(
                        "owner",
                        stt::SttParams {
                            model: "speech-model".into(),
                            audio: vec![7; CHUNK_BYTES + 1],
                            mime: "audio/wav".into(),
                            file_name: Some("clip.wav".into()),
                        },
                        Duration::from_secs(1),
                    )
                    .await
            })
        };
        let mut request_id = String::new();
        for expected in 0..2 {
            let ProtocolMessage::SttRequest {
                id,
                seq,
                data,
                last,
                model,
                ..
            } = rx.recv().await.unwrap()
            else {
                panic!("STT request");
            };
            assert_eq!(seq, expected);
            assert_eq!(last, expected == 1);
            assert_eq!(model.as_deref(), Some("speech-model"));
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .unwrap()
                    .len(),
                if expected == 0 { CHUNK_BYTES } else { 1 }
            );
            if expected == 0 {
                request_id = id;
            } else {
                assert_eq!(id, request_id);
            }
        }
        consumer.handle_message(
            "owner",
            &ProtocolMessage::SttResponse {
                id: request_id,
                text: "transcript".into(),
            },
        );
        assert_eq!(task.await.unwrap().unwrap(), "transcript");
    }

    #[tokio::test]
    async fn timeout_releases_pending_request() {
        let consumer = VoiceConsumer::new(Arc::new(|_, _| {}));
        let result = consumer
            .transcribe(
                "owner",
                stt::SttParams {
                    model: "network-auto".into(),
                    audio: vec![],
                    mime: "audio/wav".into(),
                    file_name: None,
                },
                Duration::from_millis(1),
            )
            .await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replies_are_bound_to_the_requested_peer_and_disconnect_closes_pending() {
        let consumer = VoiceConsumer::new(Arc::new(|_, _| {}));
        let (guard, mut rx) = consumer.begin("owner").unwrap();
        consumer.handle_message(
            "stranger",
            &ProtocolMessage::SttResponse {
                id: guard.id.clone(),
                text: "forged".into(),
            },
        );
        assert!(rx.try_recv().is_err());
        consumer.handle_message(
            "owner",
            &ProtocolMessage::SttResponse {
                id: guard.id.clone(),
                text: "valid".into(),
            },
        );
        assert!(
            matches!(rx.recv().await, Some(ProtocolMessage::SttResponse { text, .. }) if text == "valid")
        );
        consumer.on_peer_left("owner");
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn cancelled_requests_release_slots_and_pending_count_is_bounded() {
        let consumer = VoiceConsumer::new(Arc::new(|_, _| {}));
        let mut pending = Vec::new();
        for _ in 0..MAX_REQUESTS {
            pending.push(consumer.begin("owner").unwrap());
        }
        assert!(consumer.begin("owner").is_err());
        pending.pop();
        assert!(consumer.begin("owner").is_ok());
    }

    #[tokio::test]
    async fn oversized_reply_and_full_queue_close_the_request() {
        let consumer = VoiceConsumer::new(Arc::new(|_, _| {}));
        let (guard, mut rx) = consumer.begin("owner").unwrap();
        consumer.handle_message(
            "owner",
            &ProtocolMessage::TtsResponse {
                id: guard.id.clone(),
                seq: 0,
                data: "A".repeat(16 * 1024 + 1),
                last: true,
                mime: "audio/wav".into(),
            },
        );
        assert!(rx.recv().await.is_none());
        let (guard, mut rx) = consumer.begin("owner").unwrap();
        for _ in 0..9 {
            consumer.handle_message(
                "owner",
                &ProtocolMessage::SttResponse {
                    id: guard.id.clone(),
                    text: "ok".into(),
                },
            );
        }
        for _ in 0..8 {
            assert!(rx.recv().await.is_some());
        }
        assert!(rx.recv().await.is_none());
    }
}
