//! Minimal RTSP 1.0 server (OPTIONS/DESCRIBE/SETUP/PLAY/TEARDOWN) for a
//! single H264 video stream, playable by VRChat video players (AVPro).
//! Session handling and the dummy SPS/PPS/NALU keepalive loop mirror
//! mistlink's `internal/rtsp/server.go` (built on `gortsplib`); here the
//! RTSP message framing comes from the `rtsp-types` crate and everything
//! else (session table, transport negotiation, RTP fan-out, keepalive
//! timer) is hand-rolled.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rtsp_types::headers::{self, RtpLowerTransport, RtpProfile, RtpTransport, RtpTransportParameters, Transport, Transports};
use rtsp_types::{Method, Request, Response, StatusCode, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::rtp_out::{self, DUMMY_NALU, RtpHeaderFields};

/// How long after the last real access unit the dummy keepalive resumes.
const REAL_DATA_IDLE: Duration = Duration::from_millis(1000);

/// Per-client RTSP session: how to reach it (UDP endpoint or TCP
/// interleaved channel) and whether `PLAY` has been issued.
struct Session {
    transport: SessionTransport,
    playing: bool,
}

enum SessionTransport {
    Udp(SocketAddr),
    Tcp {
        channel: u8,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    },
}

/// Shared mutable state, behind one lock; traffic here is at most
/// video-frame-rate, so a single `tokio::sync::Mutex` is plenty.
struct Inner {
    sessions: HashMap<String, Session>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    /// When the last real access unit was fanned out. The dummy keepalive
    /// stands down only while real data flowed recently: damage-driven
    /// capture backends (Windows.Graphics.Capture) emit nothing at all on a
    /// static screen, and AVPro drops the stream if RTP goes silent.
    last_real_au: Option<Instant>,
    real_seq: u16,
    real_ts: u32,
    dummy_seq: u16,
    dummy_ts: u32,
    payloader: rtp_out::H264Rtp,
}

/// The RTSP server: one TCP listener plus one shared UDP socket used to
/// send RTP to all UDP-transport clients.
pub struct RtspServer {
    rtp_socket: UdpSocket,
    frame_rate: u32,
    inner: Mutex<Inner>,
    background_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl RtspServer {
    fn new(rtp_socket: UdpSocket, frame_rate: u32) -> Arc<Self> {
        Arc::new(Self {
            rtp_socket,
            frame_rate,
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                sps: None,
                pps: None,
                last_real_au: None,
                real_seq: rand::random(),
                real_ts: rand::random(),
                dummy_seq: 0,
                dummy_ts: 0,
                payloader: rtp_out::H264Rtp::new(),
            }),
            background_tasks: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Bind the RTSP TCP listener and a shared RTP UDP socket on
    /// `bind_addr`'s IP, then spawn the accept loop and the dummy keepalive
    /// loop as background tasks.
    pub async fn start(bind_addr: SocketAddr, frame_rate: u32) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("binding RTSP listener on {bind_addr}"))?;
        let rtp_socket = UdpSocket::bind((bind_addr.ip(), 0))
            .await
            .context("binding RTP UDP socket")?;

        let server = Self::new(rtp_socket, frame_rate);

        let accept_server = server.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, peer_addr)) => {
                        let server = accept_server.clone();
                        tokio::spawn(async move {
                            handle_connection(server, socket, peer_addr).await;
                        });
                    }
                    Err(error) => warn!(%error, "rtsp accept failed"),
                }
            }
        });

        let dummy_server = server.clone();
        let dummy_task = tokio::spawn(async move {
            dummy_keepalive_loop(dummy_server).await;
        });

        server
            .background_tasks
            .lock()
            .expect("background_tasks mutex poisoned")
            .extend([accept_task, dummy_task]);

        Ok(server)
    }

    /// Stop accepting connections and stop the keepalive loop. Existing
    /// client connections are dropped when their tasks next try to use
    /// state that's gone (handled by simply aborting all our own tasks).
    pub async fn stop(&self) {
        let tasks = std::mem::take(
            &mut *self
                .background_tasks
                .lock()
                .expect("background_tasks mutex poisoned"),
        );
        for task in tasks {
            task.abort();
        }
    }

    pub async fn client_count(&self) -> usize {
        self.inner
            .lock()
            .await
            .sessions
            .values()
            .filter(|session| session.playing)
            .count()
    }

    /// Cache the latest SPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_sps(&self, sps: Vec<u8>) {
        self.inner.lock().await.sps = Some(sps);
    }

    /// Cache the latest PPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_pps(&self, pps: Vec<u8>) {
        self.inner.lock().await.pps = Some(pps);
    }

    /// Packetize one Annex-B access unit and fan it out as RTP to every
    /// playing session; also marks real data as flowing so the dummy
    /// keepalive loop stands down while frames keep arriving.
    pub async fn send_video_access_unit(&self, annex_b: &[u8]) {
        let mut inner = self.inner.lock().await;
        inner.last_real_au = Some(Instant::now());

        let payloads = inner.payloader.payload(annex_b);
        if payloads.is_empty() {
            return;
        }

        let ts = inner.real_ts;
        let step = rtp_out::CLOCK_RATE / self.frame_rate.max(1);
        inner.real_ts = inner.real_ts.wrapping_add(step);

        let last = payloads.len() - 1;
        for (i, payload) in payloads.iter().enumerate() {
            let seq = inner.real_seq;
            inner.real_seq = inner.real_seq.wrapping_add(1);
            let bytes = rtp_out::serialize_rtp_packet(
                RtpHeaderFields {
                    sequence_number: seq,
                    timestamp: ts,
                    marker: i == last,
                },
                payload,
            );
            Self::dispatch(&inner.sessions, &self.rtp_socket, &bytes).await;
        }
    }

    async fn build_sdp(&self) -> String {
        let (sps, pps) = {
            let inner = self.inner.lock().await;
            match (&inner.sps, &inner.pps) {
                (Some(sps), Some(pps)) => (sps.clone(), pps.clone()),
                _ => (rtp_out::DUMMY_SPS.to_vec(), rtp_out::DUMMY_PPS.to_vec()),
            }
        };

        use base64::Engine as _;
        let sps_b64 = base64::engine::general_purpose::STANDARD.encode(&sps);
        let pps_b64 = base64::engine::general_purpose::STANDARD.encode(&pps);

        format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=mistl\r\n\
             c=IN IP4 0.0.0.0\r\n\
             t=0 0\r\n\
             a=tool:mistl\r\n\
             a=control:*\r\n\
             a=recvonly\r\n\
             m=video 0 RTP/AVP {pt}\r\n\
             a=rtpmap:{pt} H264/{clock_rate}\r\n\
             a=fmtp:{pt} packetization-mode=1;sprop-parameter-sets={sps_b64},{pps_b64}\r\n",
            pt = rtp_out::PAYLOAD_TYPE_H264,
            clock_rate = rtp_out::CLOCK_RATE,
        )
    }

    /// Send `bytes` (a full RTP packet) to every playing session, over UDP
    /// or wrapped as an RTSP interleaved `Data` frame over TCP.
    async fn dispatch(sessions: &HashMap<String, Session>, rtp_socket: &UdpSocket, bytes: &[u8]) {
        for session in sessions.values() {
            if !session.playing {
                continue;
            }
            match &session.transport {
                SessionTransport::Udp(addr) => {
                    let _ = rtp_socket.send_to(bytes, *addr).await;
                }
                SessionTransport::Tcp { channel, tx } => {
                    let mut framed = Vec::with_capacity(4 + bytes.len());
                    framed.push(b'$');
                    framed.push(*channel);
                    framed.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                    framed.extend_from_slice(bytes);
                    let _ = tx.send(framed);
                }
            }
        }
    }
}

/// Mirrors mistlink's `dummyPacketLoop`: while no real video is flowing,
/// periodically emit a filler NALU RTP packet so AVPro's session doesn't
/// time out. Cadence backs off once a client is actually connected (500ms)
/// versus idle (100ms), matching the Go reference exactly.
async fn dummy_keepalive_loop(server: Arc<RtspServer>) {
    let mut current_interval = Duration::from_millis(100);
    let mut ticker = tokio::time::interval(current_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let mut inner = server.inner.lock().await;

        let client_count = inner.sessions.values().filter(|s| s.playing).count();
        let target_interval = if client_count > 0 {
            Duration::from_millis(500)
        } else {
            Duration::from_millis(100)
        };
        if target_interval != current_interval {
            current_interval = target_interval;
            ticker = tokio::time::interval(current_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        }

        if inner
            .last_real_au
            .is_some_and(|at| at.elapsed() < REAL_DATA_IDLE)
        {
            continue;
        }

        let seq = inner.dummy_seq;
        inner.dummy_seq = inner.dummy_seq.wrapping_add(1);
        let ts = inner.dummy_ts;
        inner.dummy_ts = inner.dummy_ts.wrapping_add(rtp_out::DUMMY_TIMESTAMP_INCREMENT);

        let bytes = rtp_out::serialize_rtp_packet(
            RtpHeaderFields {
                sequence_number: seq,
                timestamp: ts,
                marker: true,
            },
            DUMMY_NALU,
        );
        RtspServer::dispatch(&inner.sessions, &server.rtp_socket, &bytes).await;
    }
}

/// Reads RTSP requests off one TCP connection, dispatches them, and writes
/// back responses (and, once a TCP-interleaved SETUP has happened, RTP
/// `Data` frames pushed from elsewhere via `tcp_tx`).
async fn handle_connection(server: Arc<RtspServer>, stream: TcpStream, peer_addr: SocketAddr) {
    let (mut read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let writer_task = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(bytes) = rx.recv().await {
            if write_half.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 8192];
    let mut session_ids: Vec<String> = Vec::new();

    'conn: loop {
        loop {
            match rtsp_types::Message::<Vec<u8>>::parse(&buf) {
                Ok((message, consumed)) => {
                    buf.drain(0..consumed);
                    if let rtsp_types::Message::Request(request) = message {
                        let (response, new_session) =
                            handle_request(&server, &request, peer_addr, tx.clone()).await;
                        if let Some(id) = new_session {
                            session_ids.push(id);
                        }
                        let mut out = Vec::new();
                        if response.write(&mut out).is_err() || tx.send(out).is_err() {
                            break 'conn;
                        }
                    }
                    // Data/Response messages from the client (e.g. RTCP
                    // sent over an interleaved channel) are ignored.
                }
                Err(rtsp_types::ParseError::Incomplete(_)) => break,
                Err(rtsp_types::ParseError::Error) => {
                    debug!(%peer_addr, "bad RTSP message, closing connection");
                    break 'conn;
                }
            }
        }

        match read_half.read(&mut read_buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
        }
    }

    {
        let mut inner = server.inner.lock().await;
        for id in &session_ids {
            inner.sessions.remove(id);
        }
    }
    writer_task.abort();
}

fn base_response(version: Version, status: StatusCode, cseq: Option<headers::CSeq>) -> rtsp_types::ResponseBuilder {
    let mut builder = Response::builder(version, status);
    if let Some(cseq) = cseq {
        builder = builder.typed_header(&cseq);
    }
    builder
}

/// Handles one parsed RTSP request, returning the response to send and,
/// for a successful SETUP, the newly-allocated session id (so the
/// connection task can clean it up on disconnect).
async fn handle_request(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    peer_addr: SocketAddr,
    tcp_tx: mpsc::UnboundedSender<Vec<u8>>,
) -> (Response<Vec<u8>>, Option<String>) {
    let version = request.version();
    let cseq = request.typed_header::<headers::CSeq>().ok().flatten();

    match request.method() {
        Method::Options => {
            let response = base_response(version, StatusCode::Ok, cseq)
                .typed_header(
                    &headers::Public::builder()
                        .method(Method::Options)
                        .method(Method::Describe)
                        .method(Method::Setup)
                        .method(Method::Play)
                        .method(Method::Teardown)
                        .build(),
                )
                .build(Vec::new());
            (response, None)
        }
        Method::Describe => {
            let sdp = server.build_sdp().await;
            let mut builder =
                base_response(version, StatusCode::Ok, cseq).header(headers::CONTENT_TYPE, "application/sdp");
            if let Some(uri) = request.request_uri() {
                builder = builder.header(headers::CONTENT_BASE, uri.as_str().to_string());
            }
            (builder.build(sdp.into_bytes()), None)
        }
        Method::Setup => handle_setup(server, request, peer_addr, tcp_tx, version, cseq).await,
        Method::Play => handle_play(server, request, version, cseq).await,
        Method::Teardown => handle_teardown(server, request, version, cseq).await,
        Method::GetParameter => (base_response(version, StatusCode::Ok, cseq).build(Vec::new()), None),
        _ => (
            base_response(version, StatusCode::MethodNotAllowed, cseq).build(Vec::new()),
            None,
        ),
    }
}

async fn handle_setup(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    peer_addr: SocketAddr,
    tcp_tx: mpsc::UnboundedSender<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    let Ok(Some(transports)) = request.typed_header::<Transports>() else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let rtp_transport = transports.iter().find_map(|t| match t {
        Transport::Rtp(rtp) => Some(rtp.clone()),
        Transport::Other(_) => None,
    });

    let Some(rtp_transport) = rtp_transport else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let (session_transport, response_transport) = if let Some(interleaved) = rtp_transport.params.interleaved {
        let session_transport = SessionTransport::Tcp {
            channel: interleaved.0,
            tx: tcp_tx,
        };
        let response_transport = Transports::from(vec![Transport::Rtp(RtpTransport {
            profile: RtpProfile::Avp,
            lower_transport: Some(RtpLowerTransport::Tcp),
            params: RtpTransportParameters {
                unicast: true,
                interleaved: Some(interleaved),
                ..Default::default()
            },
        })]);
        (session_transport, response_transport)
    } else if let Some(client_port) = rtp_transport.params.client_port {
        let client_addr = SocketAddr::new(peer_addr.ip(), client_port.0);
        let server_rtp_port = server.rtp_socket.local_addr().map(|a| a.port()).unwrap_or(0);
        let session_transport = SessionTransport::Udp(client_addr);
        let response_transport = Transports::from(vec![Transport::Rtp(RtpTransport {
            profile: RtpProfile::Avp,
            lower_transport: Some(RtpLowerTransport::Udp),
            params: RtpTransportParameters {
                unicast: true,
                client_port: Some(client_port),
                server_port: Some((server_rtp_port, Some(server_rtp_port + 1))),
                ..Default::default()
            },
        })]);
        (session_transport, response_transport)
    } else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let session_id = new_session_id();
    {
        let mut inner = server.inner.lock().await;
        inner.sessions.insert(
            session_id.clone(),
            Session {
                transport: session_transport,
                playing: false,
            },
        );
    }

    let response = base_response(version, StatusCode::Ok, cseq)
        .typed_header(&response_transport)
        .typed_header(&headers::Session::with_timeout(session_id.clone(), 60))
        .build(Vec::new());
    (response, Some(session_id))
}

async fn handle_play(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    let Ok(Some(session)) = request.typed_header::<headers::Session>() else {
        return (
            base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
            None,
        );
    };

    let mut inner = server.inner.lock().await;
    let Some(entry) = inner.sessions.get_mut(session.0.as_str()) else {
        return (
            base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
            None,
        );
    };
    entry.playing = true;
    drop(inner);

    let response = base_response(version, StatusCode::Ok, cseq)
        .typed_header(&headers::Session::from(session.0))
        .build(Vec::new());
    (response, None)
}

async fn handle_teardown(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    if let Ok(Some(session)) = request.typed_header::<headers::Session>() {
        let mut inner = server.inner.lock().await;
        inner.sessions.remove(session.0.as_str());
    }
    (base_response(version, StatusCode::Ok, cseq).build(Vec::new()), None)
}

fn new_session_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtsp_types::Url;

    async fn test_server() -> Arc<RtspServer> {
        let rtp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        RtspServer::new(rtp_socket, 30)
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:5555".parse().unwrap()
    }

    #[tokio::test]
    async fn options_lists_supported_methods() {
        let server = test_server().await;
        let request = Request::builder(Method::Options, Version::V1_0)
            .header(headers::CSEQ, "1")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let (response, session) = handle_request(&server, &request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session.is_none());
        let public = response.typed_header::<headers::Public>().unwrap().unwrap();
        assert!(public.contains(&Method::Describe));
        assert!(public.contains(&Method::Setup));
        assert!(public.contains(&Method::Play));
        assert!(public.contains(&Method::Teardown));
    }

    #[tokio::test]
    async fn describe_returns_sdp_with_h264_media_and_dummy_params() {
        let server = test_server().await;
        let request = Request::builder(Method::Describe, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream").unwrap())
            .header(headers::CSEQ, "2")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let (response, session) = handle_request(&server, &request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session.is_none());
        assert_eq!(
            response.header(&headers::CONTENT_TYPE).map(|v| v.as_str()),
            Some("application/sdp")
        );
        let body = String::from_utf8(response.body().clone()).unwrap();
        assert!(body.contains("m=video 0 RTP/AVP 96"));
        assert!(body.contains(&format!("rtpmap:{} H264/90000", rtp_out::PAYLOAD_TYPE_H264)));
        assert!(body.contains("sprop-parameter-sets="));
    }

    #[tokio::test]
    async fn setup_udp_then_play_marks_session_playing() {
        let server = test_server().await;

        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &setup_request, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("SETUP should allocate a session id");

        {
            let inner = server.inner.lock().await;
            assert!(!inner.sessions.get(&session_id).unwrap().playing);
        }

        let play_request = Request::builder(Method::Play, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response, _) = handle_request(&server, &play_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let inner = server.inner.lock().await;
        assert!(inner.sessions.get(&session_id).unwrap().playing);
    }

    #[tokio::test]
    async fn setup_tcp_interleaved_is_accepted() {
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP/TCP;unicast;interleaved=0-1")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &setup_request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session_id.is_some());
        let transport_header = response.header(&headers::TRANSPORT).unwrap().as_str();
        assert!(transport_header.contains("interleaved=0-1"));
    }

    #[tokio::test]
    async fn teardown_removes_session() {
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=6000-6001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (_, session_id) = handle_request(&server, &setup_request, peer(), tx.clone()).await;
        let session_id = session_id.unwrap();

        let teardown_request = Request::builder(Method::Teardown, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response, _) = handle_request(&server, &teardown_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let inner = server.inner.lock().await;
        assert!(!inner.sessions.contains_key(&session_id));
    }

    #[test]
    fn dummy_nal_constants_match_mistlink_reference() {
        assert_eq!(rtp_out::DUMMY_SPS, &[0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2]);
        assert_eq!(rtp_out::DUMMY_PPS, &[0x68, 0xce, 0x3c, 0x80]);
        assert_eq!(rtp_out::DUMMY_NALU, &[0x0c, 0xff, 0xff, 0xff]);
    }
}
