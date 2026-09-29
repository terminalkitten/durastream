use crate::{Error, Result};

/// Bytes before each payload: length (u32 BE) + CRC-32 (u32 BE).
pub const HEADER_SIZE: usize = 8;
/// Width of a zero-padded offset token.
pub const TOKEN_WIDTH: usize = 20;

/// CRC-32/ISO-HDLC, same as `zlib.crc32`. crc-fast: PMULL/PCLMUL SIMD, ~2.5x crc32fast on big payloads.
pub(crate) fn crc32(data: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, data) as u32
}

/// `(payload length, crc)` from a frame header.
pub(crate) fn decode_header(header: [u8; HEADER_SIZE]) -> (usize, u32) {
    let [l0, l1, l2, l3, c0, c1, c2, c3] = header;
    (
        u32::from_be_bytes([l0, l1, l2, l3]) as usize,
        u32::from_be_bytes([c0, c1, c2, c3]),
    )
}

pub(crate) fn push_frame(buf: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::Invalid(format!("payload too large: {} bytes", payload.len())))?;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&crc32(payload).to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}

/// Yields `(payload, next_pos)` per intact frame; stops at the first torn/corrupt one.
pub fn iter_frames(data: &[u8]) -> impl Iterator<Item = (&[u8], usize)> {
    let mut pos = 0;
    std::iter::from_fn(move || {
        let header = data.get(pos..pos + HEADER_SIZE)?.try_into().ok()?;
        let (len, crc) = decode_header(header);
        let end = pos.checked_add(HEADER_SIZE + len)?;
        let payload = data.get(pos + HEADER_SIZE..end)?; // torn tail
        if crc32(payload) != crc {
            return None; // corruption: reject this record and everything after
        }
        pos = end;
        Some((payload, end))
    })
}

/// offset -> 20-char zero-padded token.
pub fn to_token(offset: u64) -> String {
    format!("{offset:0TOKEN_WIDTH$}")
}

/// token -> offset; handles "-1" (start) and "now" (tail).
pub fn from_token(token: &str, next_offset: u64) -> Result<i64> {
    let invalid = || Error::Invalid(format!("invalid token: {token:?}"));
    match token.trim() {
        "-1" => Ok(0),
        "now" => i64::try_from(next_offset).map_err(|_| invalid()),
        t => t.parse().map_err(|_| invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn frames(payloads: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        for p in payloads {
            push_frame(&mut buf, p).unwrap();
        }
        buf
    }

    #[test]
    fn crc_matches_zlib() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926); // CRC-32/ISO-HDLC check value
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn roundtrip_torn_and_corrupt() {
        let buf = frames(&[b"a", b"", b"ccc"]);
        let got: Vec<_> = iter_frames(&buf).map(|(p, _)| p).collect();
        assert_eq!(got, [&b"a"[..], b"", b"ccc"]);

        // torn tail: the last frame is dropped, the prefix survives
        let got: Vec<_> = iter_frames(&buf[..buf.len() - 1]).map(|(p, _)| p).collect();
        assert_eq!(got, [&b"a"[..], b""]);

        // a flipped payload byte rejects that record and everything after it
        let mut bad = buf.clone();
        bad[HEADER_SIZE] ^= 0xFF;
        assert_eq!(iter_frames(&bad).count(), 0);

        // a header that promises more bytes than exist
        assert_eq!(iter_frames(&[0xFF; HEADER_SIZE]).count(), 0);
    }

    #[test]
    fn tokens() {
        assert_eq!(to_token(1), "00000000000000000001");
        assert_eq!(from_token("-1", 5).unwrap(), 0);
        assert_eq!(from_token(" now ", 5).unwrap(), 5);
        assert_eq!(from_token("00000000000000000003", 5).unwrap(), 3);
        assert!(matches!(from_token("x", 5), Err(Error::Invalid(_))));
    }

    proptest! {
        #[test]
        fn any_truncation_yields_a_prefix(
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
            cut in any::<prop::sample::Index>(),
        ) {
            let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
            let buf = frames(&refs);
            let got: Vec<&[u8]> = iter_frames(&buf).map(|(p, _)| p).collect();
            prop_assert_eq!(&got, &refs);

            let cut = cut.index(buf.len() + 1);
            let got: Vec<&[u8]> = iter_frames(&buf[..cut]).map(|(p, _)| p).collect();
            prop_assert!(refs.starts_with(&got));
        }
    }
}
