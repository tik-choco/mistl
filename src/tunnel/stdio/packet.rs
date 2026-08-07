//! Wire framing for stdio bytes: a one-byte stream-type tag prepended to the
//! payload, so a single `P2pPayload::Stdio` message (see
//! `crate::tunnel::rtc`'s wire payload) can carry stdin, stdout, or stderr
//! data and the receiving end knows which pipe to write it to. Ported
//! unchanged from the standalone `p2p` crate's `src/stdio/packet.rs` --
//! purely local byte-slicing, nothing to reseat on mistl's transport.

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum StreamType {
    Stdin = 0x00,
    Stdout = 0x01,
    Stderr = 0x02,
}

pub fn wrap_packet(stream_type: StreamType, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + data.len());
    out.push(stream_type as u8);
    out.extend_from_slice(data);
    out
}

pub fn unwrap_packet(data: &[u8]) -> (StreamType, &[u8]) {
    if data.is_empty() {
        return (StreamType::Stdin, &[]);
    }
    let stream_type = match data[0] {
        0x01 => StreamType::Stdout,
        0x02 => StreamType::Stderr,
        _ => StreamType::Stdin,
    };
    (stream_type, &data[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_stream_type_and_payload() {
        assert_eq!(
            wrap_packet(StreamType::Stdin, b"abc"),
            vec![0x00, b'a', b'b', b'c']
        );
        assert_eq!(
            wrap_packet(StreamType::Stdout, b"abc"),
            vec![0x01, b'a', b'b', b'c']
        );
        assert_eq!(
            wrap_packet(StreamType::Stderr, b"abc"),
            vec![0x02, b'a', b'b', b'c']
        );
    }

    #[test]
    fn unwraps_stream_type_and_payload() {
        assert_eq!(
            unwrap_packet(&[0x00, 1, 2]),
            (StreamType::Stdin, &[1, 2][..])
        );
        assert_eq!(
            unwrap_packet(&[0x01, 1, 2]),
            (StreamType::Stdout, &[1, 2][..])
        );
        assert_eq!(
            unwrap_packet(&[0x02, 1, 2]),
            (StreamType::Stderr, &[1, 2][..])
        );
    }

    #[test]
    fn unwrap_defaults_empty_and_unknown_streams_to_stdin() {
        assert_eq!(unwrap_packet(&[]), (StreamType::Stdin, &[][..]));
        assert_eq!(unwrap_packet(&[0xff, 9]), (StreamType::Stdin, &[9][..]));
    }
}
