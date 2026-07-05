//! UDP MPEG-TS ingest: receive ffmpeg's MPEG-TS-over-UDP output, demux the
//! H264 elementary stream (using the `mpeg2ts` crate for TS/PSI framing),
//! split it into Annex-B NAL units, group NALs into access units (one
//! video frame's worth of NALs), and forward each access unit to the RTSP
//! server for RTP packetization and fan-out. Mirrors the role of
//! mistlink's `internal/sender` UDP-to-RTP bridge, adapted to a real MPEG-TS
//! ingest (this Rust port receives MPEG-TS from `ffmpeg`, rather than
//! already-packetized RTP from `pion/mediadevices`).

use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use mpeg2ts::es::StreamType;
use mpeg2ts::ts::{Pid, ReadTsPacket, TsPacket, TsPacketReader, TsPayload};
use tokio::net::UdpSocket;
use tracing::debug;

use super::rtsp::RtspServer;

const NAL_TYPE_SLICE_NON_IDR: u8 = 1;
const NAL_TYPE_SEI: u8 = 6;
const NAL_TYPE_SPS: u8 = 7;
const NAL_TYPE_PPS: u8 = 8;
const NAL_TYPE_AUD: u8 = 9;
const NAL_TYPE_SLICE_IDR: u8 = 5;

/// A `Read` adapter over a growing byte queue fed from received UDP
/// datagrams. Only call [`ReadTsPacket::read_ts_packet`] while at least
/// [`TsPacket::SIZE`] bytes are queued, so a single `TsPacketReader` read
/// never straddles a "no data yet" gap (see [`run`]).
#[derive(Clone, Default)]
struct SharedQueue(Arc<Mutex<VecDeque<u8>>>);

impl SharedQueue {
    fn len(&self) -> usize {
        self.0.lock().expect("stream ingest queue poisoned").len()
    }

    fn extend(&self, data: &[u8]) {
        self.0
            .lock()
            .expect("stream ingest queue poisoned")
            .extend(data.iter().copied());
    }

    /// Recover from a parse error by dropping bytes up to the next plausible
    /// TS packet boundary: a `0x47` sync byte that's *also* followed by
    /// another `0x47` one packet (188 bytes) later, when there's enough
    /// buffered data to check that far ahead (accepted optimistically
    /// otherwise -- a false positive just triggers another resync).
    ///
    /// This is a fast byte-level *scan* (no packet parsing, and nothing is
    /// popped until the boundary to keep is known), so a run of non-TS-like
    /// bytes gets skipped in one lock acquisition instead of one
    /// `TsPacketReader::read_ts_packet` attempt per byte -- that matters
    /// because a byte-at-a-time-with-a-full-reparse approach could spend so
    /// long resyncing that it never returned to `socket.recv_from`,
    /// starving the receive loop and dropping more datagrams under load.
    ///
    /// Position 0 is known bad (that's why the caller is here), so the scan
    /// starts at 1: this guarantees at least 1 byte is dropped, so the
    /// caller can't spin forever retrying the exact same failing state.
    /// Returns the number of bytes dropped.
    fn resync(&self) -> usize {
        let mut queue = self.0.lock().expect("stream ingest queue poisoned");
        let mut offset = 1usize;
        loop {
            let Some(&byte) = queue.get(offset) else {
                let dropped = queue.len();
                queue.clear();
                return dropped;
            };
            if byte == 0x47 {
                let next = queue.get(offset + TsPacket::SIZE);
                if next.is_none() || next == Some(&0x47) {
                    for _ in 0..offset {
                        queue.pop_front();
                    }
                    return offset;
                }
            }
            offset += 1;
        }
    }
}

impl Read for SharedQueue {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let mut queue = self.0.lock().expect("stream ingest queue poisoned");
        let n = out.len().min(queue.len());
        for slot in out.iter_mut().take(n) {
            *slot = queue.pop_front().expect("checked len above");
        }
        Ok(n)
    }
}

/// Receive MPEG-TS packets from `socket`, extract the H264 elementary
/// stream, and forward Annex-B access units to `rtsp`. Runs until the
/// socket errors (e.g. because it was dropped by `stream.stop`).
pub async fn run(socket: UdpSocket, rtsp: Arc<RtspServer>) -> Result<()> {
    let queue = SharedQueue::default();
    let mut ts_reader = TsPacketReader::new(queue.clone());
    let mut video_pid: Option<Pid> = None;
    let mut splitter = AnnexBSplitter::new();
    let mut pending_au: Vec<u8> = Vec::new();

    let mut recv_buf = vec![0u8; 65536];
    loop {
        let (n, _addr) = socket.recv_from(&mut recv_buf).await?;
        queue.extend(&recv_buf[..n]);

        // ffmpeg's UDP writes are fixed-size chunks (typically the local
        // MTU, e.g. 1472 bytes), *not* whole multiples of 188 -- so a TS
        // packet routinely straddles a datagram boundary. `queue` glues
        // consecutive datagrams into one continuous byte stream so that's
        // transparent; the `>= TsPacket::SIZE` guard just ensures each
        // `read_ts_packet` call has a full packet's worth of bytes queued
        // before the underlying `Read` impl could ever come up short.
        while queue.len() >= TsPacket::SIZE {
            match ts_reader.read_ts_packet() {
                Ok(Some(packet)) => {
                    handle_ts_packet(packet, &mut video_pid, &mut splitter);
                    for nal in splitter.drain_nals() {
                        process_nal(&nal, &mut pending_au, &rtsp).await;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let dropped = queue.resync();
                    debug!(%error, dropped, "malformed MPEG-TS packet, resynced");
                }
            }
        }
    }
}

fn handle_ts_packet(packet: TsPacket, video_pid: &mut Option<Pid>, splitter: &mut AnnexBSplitter) {
    let pid = packet.header.pid;
    match packet.payload {
        Some(TsPayload::Pmt(pmt)) => {
            if let Some(es) = pmt.es_info.iter().find(|es| es.stream_type == StreamType::H264) {
                if *video_pid != Some(es.elementary_pid) {
                    debug!(pid = es.elementary_pid.as_u16(), "stream: found H264 elementary stream");
                }
                *video_pid = Some(es.elementary_pid);
            }
        }
        Some(TsPayload::Pes(pes)) if Some(pid) == *video_pid => {
            splitter.push(&pes.data);
        }
        Some(TsPayload::Raw(bytes)) if Some(pid) == *video_pid => {
            splitter.push(&bytes);
        }
        _ => {}
    }
}

/// Buffers non-VCL NALs (SPS/PPS/SEI/AUD) and flushes a complete access unit
/// (mirroring an ffmpeg/libx264 frame: optional SPS/PPS/SEI followed by
/// exactly one slice NAL) to the RTSP server whenever a slice NAL arrives.
async fn process_nal(nal: &[u8], pending_au: &mut Vec<u8>, rtsp: &Arc<RtspServer>) {
    if nal.is_empty() {
        return;
    }
    let nal_type = nal[0] & 0x1F;
    match nal_type {
        NAL_TYPE_SPS => {
            rtsp.update_sps(nal.to_vec()).await;
            append_with_start_code(pending_au, nal);
        }
        NAL_TYPE_PPS => {
            rtsp.update_pps(nal.to_vec()).await;
            append_with_start_code(pending_au, nal);
        }
        NAL_TYPE_SEI | NAL_TYPE_AUD => {
            append_with_start_code(pending_au, nal);
        }
        NAL_TYPE_SLICE_NON_IDR | NAL_TYPE_SLICE_IDR => {
            append_with_start_code(pending_au, nal);
            rtsp.send_video_access_unit(pending_au).await;
            pending_au.clear();
        }
        _ => {
            // Other NAL types (e.g. filler) aren't forwarded; the RTP
            // payloader would drop AUD/filler on its own too.
        }
    }
}

fn append_with_start_code(buf: &mut Vec<u8>, nal: &[u8]) {
    buf.extend_from_slice(&[0, 0, 0, 1]);
    buf.extend_from_slice(nal);
}

/// Incrementally splits an Annex-B byte stream (`00 00 01` / `00 00 00 01`
/// start codes) into NAL units, buffering the trailing (possibly
/// incomplete) NAL across calls to [`Self::push`].
pub struct AnnexBSplitter {
    buf: Vec<u8>,
}

impl AnnexBSplitter {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Returns all fully-delimited NAL units (start code stripped) currently
    /// available, keeping any trailing partial NAL buffered for next time.
    pub fn drain_nals(&mut self) -> Vec<Vec<u8>> {
        let starts = find_start_codes(&self.buf);
        if starts.len() < 2 {
            return Vec::new();
        }

        let mut nals = Vec::with_capacity(starts.len() - 1);
        for pair in starts.windows(2) {
            let (pos0, len0) = pair[0];
            let (pos1, _) = pair[1];
            let nal_start = pos0 + len0;
            if nal_start < pos1 {
                nals.push(self.buf[nal_start..pos1].to_vec());
            }
        }

        let (last_pos, _) = *starts.last().expect("checked len above");
        self.buf.drain(0..last_pos);
        nals
    }
}

impl Default for AnnexBSplitter {
    fn default() -> Self {
        Self::new()
    }
}

/// Finds Annex-B start codes (`00 00 01` or `00 00 00 01`), returning
/// `(position, code_length)` pairs in ascending order.
fn find_start_codes(buf: &[u8]) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    let mut i = 0;
    while i + 3 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 {
            if buf[i + 2] == 1 {
                found.push((i, 3));
                i += 3;
                continue;
            } else if i + 4 <= buf.len() && buf[i + 2] == 0 && buf[i + 3] == 1 {
                found.push((i, 4));
                i += 4;
                continue;
            }
        }
        i += 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resync_skips_to_next_plausible_packet_boundary() {
        let queue = SharedQueue::default();
        // 5 junk bytes, then two back-to-back 188-byte "packets" (only the
        // sync byte matters for resync's own alignment check) so the
        // second sync byte can confirm the first one's position.
        let mut data = vec![0xAAu8; 5];
        data.push(0x47);
        data.extend(std::iter::repeat(0xBBu8).take(187));
        data.push(0x47);
        data.extend(std::iter::repeat(0xCCu8).take(187));
        queue.extend(&data);

        let dropped = queue.resync();
        assert_eq!(dropped, 5);
        assert_eq!(queue.len(), 2 * TsPacket::SIZE);
        assert_eq!(queue.0.lock().unwrap().front(), Some(&0x47));
    }

    #[test]
    fn resync_drops_whole_queue_when_no_sync_byte_present() {
        let queue = SharedQueue::default();
        queue.extend(&[0xAA; 300]);
        let dropped = queue.resync();
        assert_eq!(dropped, 300);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn splits_annex_b_stream_into_nals() {
        let mut splitter = AnnexBSplitter::new();
        let mut data = Vec::new();
        data.extend_from_slice(&[0, 0, 0, 1]);
        data.extend_from_slice(&[0x67, 0xAA, 0xBB]); // fake SPS
        data.extend_from_slice(&[0, 0, 1]);
        data.extend_from_slice(&[0x68, 0xCC]); // fake PPS
        data.extend_from_slice(&[0, 0, 0, 1]);
        data.extend_from_slice(&[0x65, 0x01, 0x02, 0x03]); // fake IDR slice (still "trailing")

        splitter.push(&data);
        let nals = splitter.drain_nals();

        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0], vec![0x67, 0xAA, 0xBB]);
        assert_eq!(nals[1], vec![0x68, 0xCC]);
    }

    #[test]
    fn buffers_trailing_partial_nal_across_pushes() {
        let mut splitter = AnnexBSplitter::new();
        splitter.push(&[0, 0, 0, 1, 0x67, 0xAA]);
        assert!(splitter.drain_nals().is_empty());

        splitter.push(&[0, 0, 0, 1, 0x68, 0xBB]);
        let nals = splitter.drain_nals();
        assert_eq!(nals, vec![vec![0x67, 0xAA]]);
    }

    #[test]
    fn handles_mixed_3_and_4_byte_start_codes() {
        let mut splitter = AnnexBSplitter::new();
        splitter.push(&[0, 0, 1, 0xAA, 0, 0, 0, 1, 0xBB, 0, 0, 1, 0xCC]);
        let nals = splitter.drain_nals();
        assert_eq!(nals, vec![vec![0xAA], vec![0xBB]]);
    }

    #[test]
    fn access_unit_grouping_flushes_on_slice_nal() {
        let mut pending = Vec::new();
        let sps = [NAL_TYPE_SPS, 0x42, 0x00];
        let pps = [NAL_TYPE_PPS, 0xCE];
        let slice = [NAL_TYPE_SLICE_IDR, 0x01, 0x02];

        append_with_start_code(&mut pending, &sps);
        append_with_start_code(&mut pending, &pps);
        append_with_start_code(&mut pending, &slice);

        // Grouped buffer should contain 3 start-coded NALs back to back.
        let found = find_start_codes(&pending);
        assert_eq!(found.len(), 3);
    }

    /// Builds a minimal synthetic MPEG-TS stream (PAT + PMT declaring a
    /// single H264 ES, plus one PES packet carrying a tiny Annex-B payload)
    /// and checks that [`handle_ts_packet`] finds the video PID and forwards
    /// the ES bytes to the splitter.
    #[test]
    fn demuxes_synthetic_ts_stream_to_h264_es() {
        use mpeg2ts::es::StreamType;
        use mpeg2ts::pes::PesHeader;
        use mpeg2ts::ts::payload::{Bytes as TsBytes, Pat, Pes, Pmt};
        use mpeg2ts::ts::{
            ContinuityCounter, EsInfo, ProgramAssociation, TsHeader, TsPacketWriter,
            TransportScramblingControl, VersionNumber, WriteTsPacket,
        };

        let video_pid = Pid::new(0x100).unwrap();
        let pmt_pid = Pid::new(0x1000).unwrap();

        let pat_packet = TsPacket {
            header: TsHeader {
                transport_error_indicator: false,
                transport_priority: false,
                pid: Pid::new(0).unwrap(),
                transport_scrambling_control: TransportScramblingControl::NotScrambled,
                continuity_counter: ContinuityCounter::new(),
            },
            adaptation_field: None,
            payload: Some(TsPayload::Pat(Pat {
                transport_stream_id: 1,
                version_number: VersionNumber::new(),
                table: vec![ProgramAssociation {
                    program_num: 1,
                    program_map_pid: pmt_pid,
                }],
            })),
        };

        let pmt_packet = TsPacket {
            header: TsHeader {
                transport_error_indicator: false,
                transport_priority: false,
                pid: pmt_pid,
                transport_scrambling_control: TransportScramblingControl::NotScrambled,
                continuity_counter: ContinuityCounter::new(),
            },
            adaptation_field: None,
            payload: Some(TsPayload::Pmt(Pmt {
                program_num: 1,
                pcr_pid: Some(video_pid),
                version_number: VersionNumber::new(),
                program_info: vec![],
                es_info: vec![EsInfo {
                    stream_type: StreamType::H264,
                    elementary_pid: video_pid,
                    descriptors: vec![],
                }],
            })),
        };

        let mut nal = vec![0, 0, 0, 1];
        nal.extend_from_slice(&[NAL_TYPE_SLICE_IDR, 0x01, 0x02]);
        let pes_packet = TsPacket {
            header: TsHeader {
                transport_error_indicator: false,
                transport_priority: false,
                pid: video_pid,
                transport_scrambling_control: TransportScramblingControl::NotScrambled,
                continuity_counter: ContinuityCounter::new(),
            },
            adaptation_field: None,
            payload: Some(TsPayload::Pes(Pes {
                header: PesHeader {
                    stream_id: mpeg2ts::es::StreamId::new(0xE0),
                    priority: false,
                    data_alignment_indicator: true,
                    copyright: false,
                    original_or_copy: false,
                    pts: None,
                    dts: None,
                    escr: None,
                },
                pes_packet_len: 0,
                data: TsBytes::new(&nal).unwrap(),
            })),
        };

        let mut writer = TsPacketWriter::new(Vec::new());
        writer.write_ts_packet(&pat_packet).unwrap();
        writer.write_ts_packet(&pmt_packet).unwrap();
        writer.write_ts_packet(&pes_packet).unwrap();
        let wire_bytes = writer.stream().clone();

        let mut reader = TsPacketReader::new(&wire_bytes[..]);
        let mut video_pid_found: Option<Pid> = None;
        let mut splitter = AnnexBSplitter::new();
        while let Some(packet) = reader.read_ts_packet().unwrap() {
            handle_ts_packet(packet, &mut video_pid_found, &mut splitter);
        }

        assert_eq!(video_pid_found, Some(video_pid));
        let nals = splitter.drain_nals();
        // Only one NAL was pushed with no subsequent start code, so it's
        // still buffered as a "trailing" NAL -- push a terminator to flush.
        assert!(nals.is_empty());
        splitter.push(&[0, 0, 0, 1]);
        let nals = splitter.drain_nals();
        assert_eq!(nals, vec![vec![NAL_TYPE_SLICE_IDR, 0x01, 0x02]]);
    }
}
