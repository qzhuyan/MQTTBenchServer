// SPDX-License-Identifier: MPL-2.0

//! Private adapters for FlowSDK 0.7.0, which does not export the experimental
//! server helpers. Incoming packets still use its layered parser; cursors only
//! inspect acknowledgment-critical fields without full MQTT field validation.

use flowsdk::mqtt_serde::{
    control_packet::ControlPacketType,
    parser::{
        leveled::{parse_type_only, LeveledParseOk, ParsedPacket},
        parse_vbi, ParseError,
    },
};

pub const MAX_REMAINING_LENGTH: usize = 268_435_455;

#[derive(Clone, Copy)]
pub struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    pub fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    pub fn take(&mut self, len: usize) -> Result<&'a [u8], ParseError> {
        let bytes = self
            .remaining
            .get(..len)
            .ok_or(ParseError::BufferTooShort)?;
        self.remaining = &self.remaining[len..];
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, ParseError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    /// Reads a length-prefixed byte string without UTF-8 or content validation.
    pub fn byte_string(&mut self) -> Result<&'a [u8], ParseError> {
        let len = self.u16()? as usize;
        self.take(len)
    }
    /// Skips a VBI-delimited property block without interpreting its contents.
    pub fn skip_properties(&mut self) -> Result<(), ParseError> {
        let (len, used) = parse_vbi(self.remaining)?;
        self.take(used)?;
        self.take(len)?;
        Ok(())
    }
}

pub fn encoded_vbi_len(value: usize) -> Result<usize, ParseError> {
    match value {
        0..=127 => Ok(1),
        128..=16_383 => Ok(2),
        16_384..=2_097_151 => Ok(3),
        2_097_152..=MAX_REMAINING_LENGTH => Ok(4),
        _ => Err(ParseError::InvalidLength),
    }
}

pub fn frame_len(remaining: usize) -> Result<usize, ParseError> {
    Ok(1 + encoded_vbi_len(remaining)? + remaining)
}

pub fn encode_vbi(out: &mut Vec<u8>, mut value: usize) -> Result<(), ParseError> {
    encoded_vbi_len(value)?;
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            byte |= 128;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

/// Appends a fixed header. The caller supplies packet type/flags and body length.
pub fn encode_header(out: &mut Vec<u8>, first: u8, remaining: usize) -> Result<(), ParseError> {
    encoded_vbi_len(remaining)?;
    out.push(first);
    encode_vbi(out, remaining)
}

/// Encodes the minimal success form shared by PUBACK/PUBREC/PUBCOMP in 3.1.1/5.
pub fn encode_id_ack(out: &mut Vec<u8>, first: u8, id: u16) {
    out.extend_from_slice(&[first, 2]);
    out.extend_from_slice(&id.to_be_bytes());
}

/// Checks the announced size before admitting or reserving a frame body.
pub fn declared_packet_frame_len(buffer: &[u8]) -> Result<Option<usize>, ParseError> {
    if buffer.len() < 2 {
        return Ok(None);
    }
    let length_bytes = &buffer[1..];
    // Avoid allocating FlowSDK's More error for normally fragmented headers.
    if length_bytes.len() < 4 && length_bytes.iter().all(|byte| byte & 0x80 != 0) {
        return Ok(None);
    }
    let (remaining, used) = parse_vbi(length_bytes)?;
    Ok(Some(1 + used + remaining))
}

pub struct RawBodyRef<'a> {
    pub packet_type: ControlPacketType,
    pub flags: u8,
    pub remaining: &'a [u8],
}

/// Use the published layered parser for fixed-header validation, then borrow the
/// body from the engine's reusable storage. The engine calls this on whole frames,
/// so the TypeOnly result does not discard data or allocate a packet body.
pub fn parse_raw_body_ref(buffer: &[u8]) -> Result<Option<(RawBodyRef<'_>, usize)>, ParseError> {
    match parse_type_only(buffer)? {
        LeveledParseOk::Continue(_, _) => Ok(None),
        LeveledParseOk::Packet(ParsedPacket::TypeOnly(packet), consumed) => {
            let (_, used) = parse_vbi(&buffer[1..])?;
            Ok(Some((
                RawBodyRef {
                    packet_type: packet.packet_type,
                    flags: packet.flags,
                    remaining: &buffer[1 + used..consumed],
                },
                consumed,
            )))
        }
        _ => unreachable!("parse_type_only always returns the TypeOnly variant"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use flowsdk::mqtt_serde::parser::leveled::{packet_frame_len, parse_raw_body};

    #[test]
    fn declared_length_precedes_the_payload_and_rejects_invalid_vbi() {
        assert_eq!(declared_packet_frame_len(&[0x30, 0x80]).unwrap(), None);
        assert_eq!(
            declared_packet_frame_len(&[0x30, 0x80, 1]).unwrap(),
            Some(131)
        );
        assert_eq!(packet_frame_len(&[0x30, 0x80, 1]).unwrap(), None);
        assert!(declared_packet_frame_len(&[0x30, 255, 255, 255, 255]).is_err());
    }

    #[test]
    fn borrowed_adapter_matches_flowsdk_raw_body_for_every_type_and_flag() {
        for first in 0..=255 {
            let bytes = [first, 3, 0xff, 0, 0xff];
            let owned = parse_raw_body(Bytes::copy_from_slice(&bytes));
            let borrowed = parse_raw_body_ref(&bytes);
            match (owned, borrowed) {
                (Ok(LeveledParseOk::Packet(ParsedPacket::RawBody(a), n)), Ok(Some((b, m)))) => {
                    assert_eq!(n, m);
                    assert_eq!(a.packet_type, b.packet_type);
                    assert_eq!(a.flags, b.flags);
                    assert_eq!(a.remaining.as_ref(), b.remaining);
                    for prefix in 1..bytes.len() {
                        assert!(parse_raw_body_ref(&bytes[..prefix]).unwrap().is_none());
                    }
                }
                (Err(_), Err(_)) => {}
                _ => panic!("different raw-body behavior for first byte {first}"),
            }
        }
    }

    #[test]
    fn cursors_and_encoding_boundaries_are_checked() {
        let mut cursor = Cursor::new(&[0, 2, 0xff, 0, 2, 0xff, 0xff]);
        assert_eq!(cursor.byte_string().unwrap(), &[0xff, 0]);
        cursor.skip_properties().unwrap();
        assert!(cursor.take(usize::MAX).is_err());
        assert!(cursor.u16().is_err());
        let mut out = Vec::new();
        encode_header(&mut out, 0x90, 128).unwrap();
        assert_eq!(out, vec![0x90, 0x80, 1]);
        assert!(encode_header(&mut out, 0x90, usize::MAX).is_err());
        assert_eq!(out, vec![0x90, 0x80, 1]);
    }
}
