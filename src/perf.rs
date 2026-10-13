// SPDX-License-Identifier: MPL-2.0

use crate::wire::{self, declared_packet_frame_len, parse_raw_body_ref, Cursor, RawBodyRef};
use crate::{
    config::{FakeSessionRecovery, PerfServerConfig},
    stats::PerfServerStats,
};
use flowsdk::mqtt_serde::{control_packet::ControlPacketType as Kind, parser::ParseError};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
const ASSIGNED_ID_LEN: usize = 21; // "perf-" plus 16 hex digits
const INITIAL_INPUT_BYTES: usize = 64;
const INITIAL_OUTPUT_BYTES: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum PerfServerError {
    #[error("invalid configuration: {0}")]
    Config(&'static str),
    #[error("malformed MQTT frame: {0}")]
    Parse(#[from] ParseError),
    #[error("unsupported packet, protocol level, or connection phase")]
    Unsupported,
    #[error("declared frame exceeds the configured limit")]
    FrameTooLarge,
    #[error("response exceeds the configured output limit")]
    OutputTooLarge,
    #[error("output consumption exceeds the available bytes")]
    InvalidOutputConsumption,
    #[error("connection is closed")]
    Closed,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PerfProgress {
    /// Bytes accepted from this feed call. The caller retains the suffix.
    pub consumed_input: usize,
    /// A complete buffered frame can be retried with poll (possibly after writing).
    pub buffered_work: bool,
    pub needs_output: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    AwaitConnect,
    Active,
    Draining,
    Closed,
}

/// A bounded, synchronous acknowledgment engine. No socket or session state.
pub struct PerfServerEngine {
    config: PerfServerConfig,
    phase: Phase,
    version: u8,
    input: Vec<u8>,
    output: Vec<u8>,
    output_pos: usize,
    stats: PerfServerStats,
}

impl PerfServerEngine {
    pub fn new(config: PerfServerConfig) -> Result<Self, PerfServerError> {
        config.validate().map_err(PerfServerError::Config)?;
        Ok(Self {
            // Pending connections need no engine buffers. Allocate on first use.
            input: Vec::new(),
            output: Vec::new(),
            config,
            phase: Phase::AwaitConnect,
            version: 0,
            output_pos: 0,
            stats: PerfServerStats::default(),
        })
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<PerfProgress, PerfServerError> {
        let result = self.feed_inner(bytes);
        if result.is_err() {
            self.phase = Phase::Closed;
            self.input.clear();
            self.output.clear();
            self.output_pos = 0;
        }
        result
    }

    pub fn poll(&mut self) -> Result<PerfProgress, PerfServerError> {
        self.feed(&[])
    }
    pub fn output(&self) -> &[u8] {
        &self.output[self.output_pos..]
    }
    pub fn stats(&self) -> &PerfServerStats {
        &self.stats
    }
    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed || (self.phase == Phase::Draining && self.output().is_empty())
    }
    pub fn read_allowance(&self) -> usize {
        if matches!(self.phase, Phase::AwaitConnect | Phase::Active) {
            self.config.max_frame_bytes.saturating_sub(self.input.len())
        } else {
            0
        }
    }
    pub fn consume_output(&mut self, written: usize) -> Result<(), PerfServerError> {
        if written > self.output().len() {
            return Err(PerfServerError::InvalidOutputConsumption);
        }
        self.output_pos += written;
        self.stats.response_bytes_written += written as u64;
        if self.output_pos == self.output.len() {
            self.output.clear();
            self.output_pos = 0;
        }
        Ok(())
    }

    fn feed_inner(&mut self, bytes: &[u8]) -> Result<PerfProgress, PerfServerError> {
        if self.phase == Phase::Closed {
            return Err(PerfServerError::Closed);
        }
        let mut consumed = 0;
        let mut packets = 0;
        let mut buffered_work = false;
        while matches!(self.phase, Phase::AwaitConnect | Phase::Active) {
            let declared = declared_packet_frame_len(&self.input)?;
            if let Some(len) = declared {
                if len > self.config.max_frame_bytes {
                    return Err(PerfServerError::FrameTooLarge);
                }
                if self.input.len() == len {
                    if packets == self.config.max_packets_per_poll || !self.process_frame()? {
                        buffered_work = true;
                        break;
                    }
                    packets += 1;
                    continue;
                }
            } else if self.input.len() >= self.config.max_frame_bytes {
                // Even the unfinished fixed header cannot fit the frame limit.
                return Err(PerfServerError::FrameTooLarge);
            }
            if consumed == bytes.len() || packets == self.config.max_packets_per_poll {
                break;
            }
            // Read the fixed header a byte at a time, then admit only its bounded
            // body. Never copy an oversized body, even from a large caller slice.
            let room = declared.map_or(1, |len| len - self.input.len());
            let take = room.min(bytes.len() - consumed);
            if self.input.len() + take > self.input.capacity() {
                // Grow with received data, not the peer's announced size.
                // Doubling amortizes fragmented reads, bounded by this frame.
                let capacity = self
                    .input
                    .capacity()
                    .saturating_mul(2)
                    .max(self.input.len() + take)
                    .max(INITIAL_INPUT_BYTES)
                    .min(declared.unwrap_or(self.config.max_frame_bytes));
                self.input.reserve_exact(capacity - self.input.len());
            }
            self.input
                .extend_from_slice(&bytes[consumed..consumed + take]);
            consumed += take;
            Kind::from_first_byte(self.input[0])?;
        }
        Ok(PerfProgress {
            consumed_input: consumed,
            buffered_work,
            needs_output: !self.output().is_empty(),
        })
    }

    fn process_frame(&mut self) -> Result<bool, PerfServerError> {
        let Some((packet, _)) = parse_raw_body_ref(&self.input)? else {
            return Ok(false);
        };
        let action = Action::inspect(packet, self.phase, self.version, &self.config)?;
        let required = action.response_len()?;
        if required > self.config.max_output_bytes {
            return Err(PerfServerError::OutputTooLarge);
        }
        if required > self.config.max_output_bytes - (self.output.len() - self.output_pos) {
            return Ok(false);
        }
        // Compact only when necessary; normal full drains retain buffer capacity.
        if self.output.len() + required > self.config.max_output_bytes {
            self.output.copy_within(self.output_pos.., 0);
            self.output.truncate(self.output.len() - self.output_pos);
            self.output_pos = 0;
        }
        if self.output.len() + required > self.output.capacity() {
            let capacity = self
                .output
                .capacity()
                .saturating_mul(2)
                .max(INITIAL_OUTPUT_BYTES)
                .max(self.output.len() + required)
                .min(self.config.max_output_bytes);
            self.output.reserve_exact(capacity - self.output.len());
        }
        action.encode(&mut self.output)?;
        self.stats.packets_rcvd += 1;
        match action {
            Action::Connect { version, .. } => {
                self.version = version;
                self.phase = Phase::Active;
                self.stats.conn_rcvd += 1;
            }
            Action::Publish { qos, .. } => {
                self.stats.pub_rcvd += 1;
                self.stats.pub_rcvd_by_qos[qos as usize] += 1;
            }
            Action::Filters {
                count, unsubscribe, ..
            } => {
                if unsubscribe {
                    self.stats.unsub_rcvd += 1;
                } else {
                    self.stats.sub_rcvd += 1;
                    self.stats.subscribe_entries += count as u64;
                }
            }
            Action::Pubcomp(_) => self.stats.pubrel_rcvd += 1,
            Action::Disconnect => self.phase = Phase::Draining,
            _ => {}
        }
        self.input.clear();
        Ok(true)
    }
}

enum Action<'a> {
    Connect {
        version: u8,
        session_present: bool,
        assign_id: bool,
    },
    Publish {
        qos: u8,
        id: u16,
    },
    // Sub or UnSub
    Filters {
        id: u16,
        entries: &'a [u8],
        count: usize,
        v5: bool,
        unsubscribe: bool,
    },
    Pubcomp(u16),
    Ping,
    Disconnect,
    Ignore,
}

impl<'a> Action<'a> {
    fn inspect(
        packet: RawBodyRef<'a>,
        phase: Phase,
        version: u8,
        config: &PerfServerConfig,
    ) -> Result<Self, PerfServerError> {
        if (phase == Phase::AwaitConnect) != (packet.packet_type == Kind::CONNECT) {
            return Err(PerfServerError::Unsupported);
        }
        let mut body = Cursor::new(packet.remaining);
        Ok(match packet.packet_type {
            Kind::CONNECT => {
                body.byte_string()?; // Protocol name: only structural validation.
                let version = body.u8()?;
                if !((version == 4 && config.mqtt311) || (version == 5 && config.mqtt5)) {
                    return Err(PerfServerError::Unsupported);
                }
                let clean = body.u8()? & 2 != 0;
                body.take(2)?; // Keepalive is not enforced.
                if version == 5 {
                    body.skip_properties()?;
                }
                let client_id = body.byte_string()?;
                Self::Connect {
                    version,
                    session_present: !clean
                        && config.fake_session_recovery == FakeSessionRecovery::WhenRequested,
                    assign_id: version == 5 && client_id.is_empty(),
                }
            }
            Kind::PUBLISH => {
                let qos = (packet.flags >> 1) & 3;
                if qos == 3 {
                    return Err(PerfServerError::Unsupported);
                }
                let id = if qos == 0 {
                    0
                } else {
                    body.byte_string()?;
                    body.u16()?
                };
                Self::Publish { qos, id }
            }
            Kind::PUBREL => Self::Pubcomp(body.u16()?),
            Kind::SUBSCRIBE | Kind::UNSUBSCRIBE => {
                let id = body.u16()?;
                if version == 5 {
                    body.skip_properties()?;
                }
                let entries = body.remaining();
                let unsubscribe = packet.packet_type == Kind::UNSUBSCRIBE;
                let mut count = 0;
                while !body.remaining().is_empty() {
                    body.byte_string()?;
                    if !unsubscribe && body.u8()? & 3 == 3 {
                        return Err(PerfServerError::Unsupported);
                    }
                    count += 1;
                }
                if count == 0 {
                    return Err(ParseError::BufferTooShort.into());
                }
                Self::Filters {
                    id,
                    entries,
                    count,
                    v5: version == 5,
                    unsubscribe,
                }
            }
            Kind::PINGREQ => Self::Ping,
            Kind::DISCONNECT => Self::Disconnect,
            Kind::PUBACK | Kind::PUBREC | Kind::PUBCOMP => Self::Ignore,
            _ => return Err(PerfServerError::Unsupported),
        })
    }

    fn response_len(&self) -> Result<usize, ParseError> {
        Ok(match *self {
            Self::Connect {
                version: 5,
                assign_id: true,
                ..
            } => wire::frame_len(3 + 3 + ASSIGNED_ID_LEN)?,
            Self::Connect { version: 5, .. } => 5,
            Self::Connect { .. } => 4,
            Self::Publish { qos: 0, .. } | Self::Disconnect | Self::Ignore => 0,
            Self::Publish { .. } | Self::Pubcomp(_) => 4,
            Self::Ping => 2,
            Self::Filters {
                count,
                v5,
                unsubscribe,
                ..
            } => wire::frame_len(2 + usize::from(v5) + if unsubscribe && !v5 { 0 } else { count })?,
        })
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), ParseError> {
        match *self {
            Self::Connect {
                version,
                session_present,
                assign_id,
            } => {
                let properties = if assign_id { 3 + ASSIGNED_ID_LEN } else { 0 };
                wire::encode_header(out, 0x20, 2 + if version == 5 { 1 + properties } else { 0 })?;
                out.extend_from_slice(&[u8::from(session_present), 0]);
                if version == 5 {
                    wire::encode_vbi(out, properties)?;
                    if assign_id {
                        out.push(0x12); // Assigned Client Identifier
                        out.extend_from_slice(&(ASSIGNED_ID_LEN as u16).to_be_bytes());
                        out.extend_from_slice(b"perf-");
                        let id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                        for shift in (0..16).rev() {
                            out.push(b"0123456789abcdef"[((id >> (shift * 4)) & 15) as usize]);
                        }
                    }
                }
            }
            Self::Publish { qos: 0, .. } | Self::Disconnect | Self::Ignore => {}
            Self::Publish { qos, id } => {
                wire::encode_id_ack(out, if qos == 1 { 0x40 } else { 0x50 }, id)
            }
            Self::Pubcomp(id) => wire::encode_id_ack(out, 0x70, id),
            Self::Ping => out.extend_from_slice(&[0xd0, 0]),
            Self::Filters {
                id,
                entries,
                count,
                v5,
                unsubscribe,
            } => {
                let codes = if unsubscribe && !v5 { 0 } else { count };
                wire::encode_header(
                    out,
                    if unsubscribe { 0xb0 } else { 0x90 },
                    2 + usize::from(v5) + codes,
                )?;
                out.extend_from_slice(&id.to_be_bytes());
                if v5 {
                    out.push(0);
                }
                if unsubscribe {
                    out.resize(out.len() + codes, 0);
                } else {
                    let mut cursor = Cursor::new(entries);
                    for _ in 0..count {
                        cursor.byte_string()?;
                        out.push(cursor.u8()? & 3);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    #[test]
    fn idle_and_small_packet_connections_keep_small_buffers() {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        assert_eq!(engine.input.capacity() + engine.output.capacity(), 0);
        engine
            .feed(&[
                0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0, 0, 1, b'c',
            ])
            .unwrap();
        assert_eq!(engine.output(), &[0x20, 3, 0, 0, 0]);
        engine.consume_output(5).unwrap();
        // Steady small-packet traffic reuses the allocations; it does not grow
        // to the frame/output limits or shrink and allocate on every packet.
        let capacity = (engine.input.capacity(), engine.output.capacity());
        assert!(capacity.0 + capacity.1 <= 128);
        for _ in 0..1000 {
            engine.feed(&[0x32, 6, 0, 1, b't', 0, 1, 0]).unwrap();
            assert_eq!(engine.output(), &[0x40, 2, 0, 1]);
            engine.consume_output(4).unwrap();
            assert_eq!(
                (engine.input.capacity(), engine.output.capacity()),
                capacity
            );
        }
    }

    #[test]
    fn announced_length_does_not_allocate_the_whole_frame() {
        let mut engine = PerfServerEngine::new(PerfServerConfig {
            max_frame_bytes: 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            ..Default::default()
        })
        .unwrap();
        // Even when large frames are enabled, receiving only a header must not
        // reserve a megabyte. No CONNECT/body processing is needed yet.
        let mut header = Vec::new();
        wire::encode_header(&mut header, 0x10, 1024 * 1024 - 4).unwrap();
        engine.feed(&header).unwrap();
        assert_eq!(engine.input.len(), 4);
        assert!(engine.input.capacity() <= 64);
        engine.feed(&[0; 100]).unwrap();
        assert_eq!(engine.input.len(), 104);
        assert!(engine.input.capacity() <= 128);
    }

    #[test]
    fn default_parser_accepts_eight_kib_and_rejects_larger_frames_early() {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        engine
            .feed(&[
                0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0, 0, 1, b'c',
            ])
            .unwrap();
        engine.consume_output(5).unwrap();
        let mut frame = Vec::new();
        wire::encode_header(&mut frame, 0x30, 8192 - 3).unwrap();
        frame.resize(8192, b'p');
        for chunk in frame.chunks(257) {
            assert_eq!(engine.feed(chunk).unwrap().consumed_input, chunk.len());
            assert!(engine.input.capacity() <= 8192);
        }
        assert_eq!(engine.stats().pub_rcvd, 1);
        assert!(engine.input.is_empty());
        // One byte beyond the default whole-frame limit fails on its header.
        let mut header = Vec::new();
        wire::encode_header(&mut header, 0x30, 8192 - 2).unwrap();
        assert!(matches!(
            engine.feed(&header),
            Err(PerfServerError::FrameTooLarge)
        ));
        assert_eq!(engine.stats().pub_rcvd, 1);
    }
}
