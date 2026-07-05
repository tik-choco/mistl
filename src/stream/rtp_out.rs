//! RTP packetization for the H264 elementary stream, plus the constants
//! mirroring mistlink's Go RTSP server (`internal/rtsp/server.go` and
//! `internal/rtp_utils/nal.go`): payload type, SSRC, and the dummy
//! SPS/PPS/NALU keepalive bytes.
//!
//! The `rtp` crate's `Packet`/`Header` wire (de)serialization requires the
//! `webrtc-util` crate's `Marshal`/`Unmarshal` traits, which is a transitive
//! dependency here (pulled in by `rtp`) rather than a direct one -- Rust
//! doesn't let us `use` a crate that isn't a direct dependency, and the
//! hard rules for this module say not to edit `Cargo.toml`. So we still use
//! `rtp`'s H264 payloader (STAP-A / single NALU / FU-A fragmentation, which
//! *is* usable without `webrtc-util`) but hand-roll the 12-byte RTP header
//! serialization ourselves; it's a fixed, well-known format with no
//! extensions/CSRC needed here.

use bytes::Bytes;
use rtp::codecs::h264::H264Payloader;
use rtp::packetizer::Payloader;

/// RTP payload type for H264, matching mistlink's `rtp_utils.PayloadTypeH264`.
pub const PAYLOAD_TYPE_H264: u8 = 96;

/// Fixed SSRC for the video stream, matching mistlink's `rtp_utils.VideoSSRC`.
pub const VIDEO_SSRC: u32 = 0x1234_5678;

/// RTP clock rate for H264 (RFC 6184).
pub const CLOCK_RATE: u32 = 90_000;

/// Timestamp step applied to each dummy keepalive packet, matching
/// mistlink's `rtp_utils.DummyTimestampIncrement`.
pub const DUMMY_TIMESTAMP_INCREMENT: u32 = 9_000;

/// Target RTP packet size (including the 12-byte header).
pub const RTP_MTU: usize = 1400;

/// Dummy SPS/PPS/NALU used to keep AVPro connected while no real video is
/// flowing yet, copied byte-for-byte from mistlink's
/// `internal/rtsp/server.go` (`DummySPS`/`DummyPPS`/`DummyNALU`).
pub static DUMMY_SPS: &[u8] = &[0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2];
pub static DUMMY_PPS: &[u8] = &[0x68, 0xce, 0x3c, 0x80];
pub static DUMMY_NALU: &[u8] = &[0x0c, 0xff, 0xff, 0xff];

/// The header fields needed to serialize one RTP packet; version/padding/
/// extension/CSRC are fixed (V=2, no padding, no extension, no CSRC) and the
/// payload type/SSRC are the constants above.
#[derive(Debug, Clone, Copy)]
pub struct RtpHeaderFields {
    pub sequence_number: u16,
    pub timestamp: u32,
    pub marker: bool,
}

/// Serialize one RTP packet (12-byte fixed header + payload) to wire bytes.
pub fn serialize_rtp_packet(fields: RtpHeaderFields, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + payload.len());
    out.push(0x80); // V=2, P=0, X=0, CC=0
    out.push(PAYLOAD_TYPE_H264 | if fields.marker { 0x80 } else { 0 });
    out.extend_from_slice(&fields.sequence_number.to_be_bytes());
    out.extend_from_slice(&fields.timestamp.to_be_bytes());
    out.extend_from_slice(&VIDEO_SSRC.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Packetizes one Annex-B access unit (e.g. SPS+PPS+slice, start-code
/// prefixed) into RTP payloads (STAP-A / single NALU / FU-A), using the same
/// fragmentation scheme as pion's H264 payloader (mistlink's Go sender is
/// built on pion/rtp).
pub struct H264Rtp {
    payloader: H264Payloader,
}

impl H264Rtp {
    pub fn new() -> Self {
        Self {
            payloader: H264Payloader::default(),
        }
    }

    /// Fragments `annex_b` into RTP payload chunks. The caller is
    /// responsible for wrapping each chunk in an RTP header and setting the
    /// marker bit only on the last chunk.
    pub fn payload(&mut self, annex_b: &[u8]) -> Vec<Bytes> {
        // The payloader's `mtu` bounds the RTP *payload*, so the 12-byte RTP
        // header is excluded up front to keep whole packets within RTP_MTU.
        let mtu = RTP_MTU.saturating_sub(12);
        self.payloader
            .payload(mtu, &Bytes::copy_from_slice(annex_b))
            .unwrap_or_default()
    }
}

impl Default for H264Rtp {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_expected_header_layout() {
        let bytes = serialize_rtp_packet(
            RtpHeaderFields {
                sequence_number: 0x0102,
                timestamp: 0x0304_0506,
                marker: true,
            },
            &[0xAA, 0xBB],
        );

        assert_eq!(bytes[0], 0x80);
        assert_eq!(bytes[1], PAYLOAD_TYPE_H264 | 0x80);
        assert_eq!(&bytes[2..4], &[0x01, 0x02]);
        assert_eq!(&bytes[4..8], &[0x03, 0x04, 0x05, 0x06]);
        assert_eq!(&bytes[8..12], &VIDEO_SSRC.to_be_bytes());
        assert_eq!(&bytes[12..], &[0xAA, 0xBB]);
    }

    #[test]
    fn serializes_without_marker_bit() {
        let bytes = serialize_rtp_packet(
            RtpHeaderFields {
                sequence_number: 1,
                timestamp: 1,
                marker: false,
            },
            &[],
        );
        assert_eq!(bytes[1], PAYLOAD_TYPE_H264);
    }

    #[test]
    fn small_nal_becomes_single_rtp_payload() {
        let mut rtp = H264Rtp::new();
        let mut annex_b = vec![0, 0, 0, 1];
        annex_b.extend_from_slice(&[0x65, 0x01, 0x02, 0x03]); // small IDR slice
        let payloads = rtp.payload(&annex_b);
        assert_eq!(payloads.len(), 1);
        assert_eq!(&payloads[0][..], &[0x65, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn sps_pps_then_slice_packs_stap_a() {
        let mut rtp = H264Rtp::new();
        let mut annex_b = Vec::new();
        annex_b.extend_from_slice(&[0, 0, 0, 1]);
        annex_b.extend_from_slice(DUMMY_SPS);
        annex_b.extend_from_slice(&[0, 0, 0, 1]);
        annex_b.extend_from_slice(DUMMY_PPS);
        annex_b.extend_from_slice(&[0, 0, 0, 1]);
        annex_b.extend_from_slice(&[0x65, 0x01, 0x02]); // slice

        let payloads = rtp.payload(&annex_b);
        // STAP-A (sps+pps) then the slice as a single NALU.
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0][0], 0x78); // STAP-A NAL header used by the payloader
        assert_eq!(&payloads[1][..], &[0x65, 0x01, 0x02]);
    }
}
