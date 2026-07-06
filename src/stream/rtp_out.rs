//! RTP/RTCP packetization for the H264 video track and the optional AAC/Opus
//! audio track, plus the constants mirroring mistlink's Go RTSP server
//! (`internal/rtsp/server.go`, `internal/rtp_utils/nal.go`, and
//! `aac_processor.go`): payload types, SSRCs, the dummy SPS/PPS/NALU
//! keepalive bytes, the RFC 3640 AAC AU-header layout, and RTCP Sender
//! Report serialization.
//!
//! The `rtp` crate's `Packet`/`Header` wire (de)serialization requires the
//! `webrtc-util` crate's `Marshal`/`Unmarshal` traits, which is a transitive
//! dependency here (pulled in by `rtp`) rather than a direct one -- Rust
//! doesn't let us `use` a crate that isn't a direct dependency, and the
//! hard rules for this module say not to edit `Cargo.toml`. So we still use
//! `rtp`'s H264 payloader (STAP-A / single NALU / FU-A fragmentation, which
//! *is* usable without `webrtc-util`) but hand-roll the 12-byte RTP header
//! and the 28-byte RTCP Sender Report ourselves; both are fixed, well-known
//! formats (no extensions/CSRC, no SR report blocks) with nothing to gain
//! from a library here.

use bytes::Bytes;
use rtp::codecs::h264::H264Payloader;
use rtp::packetizer::Payloader;

/// RTP payload type for H264, matching mistlink's `rtp_utils.PayloadTypeH264`.
pub const PAYLOAD_TYPE_H264: u8 = 96;

/// RTP payload types for audio, matching mistlink's `rtp_utils`.
pub const PAYLOAD_TYPE_OPUS: u8 = 111;
pub const PAYLOAD_TYPE_AAC: u8 = 112;

/// Fixed SSRC for the video stream, matching mistlink's `rtp_utils.VideoSSRC`.
pub const VIDEO_SSRC: u32 = 0x1234_5678;

/// Fixed SSRC for the audio stream, matching mistlink's `rtp_utils.AudioSSRC`.
pub const AUDIO_SSRC: u32 = 0x8765_4321;

/// RTP clock rate for both audio codecs (Opus RFC 7587 fixes 48 kHz; the AAC
/// track reuses the same rate so relayed Opus timestamps carry over 1:1).
pub const AUDIO_CLOCK_RATE: u32 = 48_000;

/// Audio codec served on the RTSP audio track.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodec {
    /// AAC-LC 48 kHz stereo, transcoded from Opus (RFC 3640 mpeg4-generic;
    /// what AVPro reliably plays). mistlink's default.
    Aac,
    /// Opus passthrough (RFC 7587), no transcoding.
    Opus,
}

impl AudioCodec {
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw {
            "aac" => Ok(Self::Aac),
            "opus" => Ok(Self::Opus),
            other => anyhow::bail!(
                "invalid stream.audio_codec {other:?}; valid values: \"aac\", \"opus\""
            ),
        }
    }

    // Inverse of `parse`; retained for symmetry, not yet called.
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aac => "aac",
            Self::Opus => "opus",
        }
    }
}

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
/// extension/CSRC are fixed (V=2, no padding, no extension, no CSRC). Payload
/// type and SSRC are passed separately to [`serialize_rtp_packet`] since both
/// the video track (PT 96 / [`VIDEO_SSRC`]) and the audio track (PT 111 or
/// 112 / [`AUDIO_SSRC`]) share this same wire format.
#[derive(Debug, Clone, Copy)]
pub struct RtpHeaderFields {
    pub sequence_number: u16,
    pub timestamp: u32,
    pub marker: bool,
}

/// Serialize one RTP packet (12-byte fixed header + payload) to wire bytes.
pub fn serialize_rtp_packet(payload_type: u8, ssrc: u32, fields: RtpHeaderFields, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + payload.len());
    out.push(0x80); // V=2, P=0, X=0, CC=0
    out.push(payload_type | if fields.marker { 0x80 } else { 0 });
    out.extend_from_slice(&fields.sequence_number.to_be_bytes());
    out.extend_from_slice(&fields.timestamp.to_be_bytes());
    out.extend_from_slice(&ssrc.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Builds the 4-byte RFC 3640 AU-header block that must precede one
/// AAC-hbr RTP payload: a fixed 16-bit AU-headers-length (one 16-bit AU
/// header follows), then the AU header itself (13-bit payload size + 3-bit
/// index, index always 0 here since we send one AU per RTP packet).
/// Byte-for-byte what mistlink's `aac_processor.go` prepends.
pub fn aac_au_header(payload_len: usize) -> [u8; 4] {
    let len = payload_len as u16;
    [0x00, 0x10, (len >> 5) as u8, ((len & 0x1F) << 3) as u8]
}

/// Seconds between the NTP epoch (1900-01-01) and the Unix epoch, needed to
/// convert `SystemTime` into an RTCP NTP timestamp.
const NTP_UNIX_EPOCH_DELTA: u64 = 2_208_988_800;

/// Current wall-clock time as an NTP timestamp, split into the 32-bit
/// seconds and 32-bit fractional-second words used by RTCP Sender Reports.
pub fn ntp_now() -> (u32, u32) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = (now.as_secs() + NTP_UNIX_EPOCH_DELTA) as u32;
    let fraction = (((now.subsec_nanos() as u64) << 32) / 1_000_000_000) as u32;
    (seconds, fraction)
}

/// Serializes a 28-byte RTCP Sender Report (RC=0, no report blocks) per
/// RFC 3550 6.4.1. Hand-rolled for the same reason [`serialize_rtp_packet`]
/// is: the `rtp` crate's RTCP types need `webrtc-util`'s (de)serialization
/// traits, which aren't a direct dependency here. The wire format is fixed
/// and small, so there's little to gain from a library for it.
pub fn serialize_sender_report(
    ssrc: u32,
    ntp_seconds: u32,
    ntp_fraction: u32,
    rtp_timestamp: u32,
    packet_count: u32,
    octet_count: u32,
) -> [u8; 28] {
    let mut out = [0u8; 28];
    out[0] = 0x80; // V=2, P=0, RC=0
    out[1] = 200; // PT=200 (SR)
    out[2..4].copy_from_slice(&6u16.to_be_bytes()); // length in 32-bit words, minus one
    out[4..8].copy_from_slice(&ssrc.to_be_bytes());
    out[8..12].copy_from_slice(&ntp_seconds.to_be_bytes());
    out[12..16].copy_from_slice(&ntp_fraction.to_be_bytes());
    out[16..20].copy_from_slice(&rtp_timestamp.to_be_bytes());
    out[20..24].copy_from_slice(&packet_count.to_be_bytes());
    out[24..28].copy_from_slice(&octet_count.to_be_bytes());
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
            PAYLOAD_TYPE_H264,
            VIDEO_SSRC,
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
            PAYLOAD_TYPE_H264,
            VIDEO_SSRC,
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
    fn serializes_audio_packet_with_given_payload_type_and_ssrc() {
        let bytes = serialize_rtp_packet(
            PAYLOAD_TYPE_OPUS,
            AUDIO_SSRC,
            RtpHeaderFields {
                sequence_number: 7,
                timestamp: 960,
                marker: false,
            },
            &[0x11, 0x22],
        );
        assert_eq!(bytes[1], PAYLOAD_TYPE_OPUS); // no marker bit set
        assert_eq!(&bytes[8..12], &AUDIO_SSRC.to_be_bytes());
        assert_eq!(&bytes[12..], &[0x11, 0x22]);
    }

    #[test]
    fn aac_au_header_matches_rfc3640_hbr_layout() {
        // len=100: headers-length fixed at 16 bits, then 13-bit size (100)
        // followed by a 3-bit index (0). 100 = 0b0_0110_0100.
        assert_eq!(aac_au_header(100), [0x00, 0x10, 0x03, 0x20]);
        // len=0 is representable too (silence/zero-length frame).
        assert_eq!(aac_au_header(0), [0x00, 0x10, 0x00, 0x00]);
    }

    #[test]
    fn sender_report_has_expected_28_byte_layout() {
        let bytes = serialize_sender_report(VIDEO_SSRC, 0x1122_3344, 0x5566_7788, 0x9900_1122, 42, 12_345);
        assert_eq!(bytes.len(), 28);
        assert_eq!(bytes[0], 0x80); // V=2 P=0 RC=0
        assert_eq!(bytes[1], 200); // PT=200 SR
        assert_eq!(&bytes[2..4], &6u16.to_be_bytes()); // length=6
        assert_eq!(&bytes[4..8], &VIDEO_SSRC.to_be_bytes());
        assert_eq!(&bytes[8..12], &0x1122_3344u32.to_be_bytes());
        assert_eq!(&bytes[12..16], &0x5566_7788u32.to_be_bytes());
        assert_eq!(&bytes[16..20], &0x9900_1122u32.to_be_bytes());
        assert_eq!(&bytes[20..24], &42u32.to_be_bytes());
        assert_eq!(&bytes[24..28], &12_345u32.to_be_bytes());
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
