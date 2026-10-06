// SPDX-License-Identifier: MPL-2.0
mod common;

use common::{connect, frame, publish, subscribe};
use mqtt_tcp_server::{FakeSessionRecovery, PerfServerConfig, PerfServerEngine, PerfServerError};

fn active(version: u8, config: PerfServerConfig) -> PerfServerEngine {
    let mut engine = PerfServerEngine::new(config).unwrap();
    engine.feed(&connect(version, true, b"test")).unwrap();
    take(&mut engine);
    engine
}
fn take(engine: &mut PerfServerEngine) -> Vec<u8> {
    let out = engine.output().to_vec();
    engine.consume_output(out.len()).unwrap();
    out
}

#[test]
fn versions_clean_flags_and_fake_recovery() {
    for version in [4, 5] {
        for clean in [false, true] {
            for recovery in [
                FakeSessionRecovery::WhenRequested,
                FakeSessionRecovery::Disabled,
            ] {
                let mut engine = PerfServerEngine::new(PerfServerConfig {
                    fake_session_recovery: recovery,
                    ..Default::default()
                })
                .unwrap();
                let request = connect(version, clean, b"fresh-id");
                for &byte in &request {
                    assert_eq!(engine.feed(&[byte]).unwrap().consumed_input, 1);
                }
                let sp = u8::from(!clean && recovery == FakeSessionRecovery::WhenRequested);
                assert_eq!(
                    take(&mut engine),
                    if version == 5 {
                        vec![0x20, 3, sp, 0, 0]
                    } else {
                        vec![0x20, 2, sp, 0]
                    }
                );
                assert_eq!(engine.stats().conn_rcvd, 1);
            }
        }
    }
}

#[test]
fn empty_v5_client_ids_are_assigned_distinct_wire_valid_identifiers() {
    let replies: Vec<_> = (0..2)
        .map(|_| {
            let mut engine = PerfServerEngine::new(Default::default()).unwrap();
            engine.feed(&connect(5, false, b"")).unwrap();
            take(&mut engine)
        })
        .collect();
    assert_ne!(replies[0], replies[1]);
    for reply in replies {
        assert_eq!(&reply[..8], &[0x20, 27, 1, 0, 24, 0x12, 0, 21]);
        assert!(std::str::from_utf8(&reply[8..])
            .unwrap()
            .starts_with("perf-"));
        let parsed =
            flowsdk::mqtt_serde::control_packet::MqttPacket::from_bytes_with_version(&reply, 5);
        assert!(parsed.is_ok(), "{parsed:?}");
    }
}

#[test]
fn pipelined_qos_and_duplicate_handshakes_are_stateless() {
    for version in [4, 5] {
        let mut engine = active(version, Default::default());
        let mut input = publish(version, 0, 0, false);
        for qos in [1, 2] {
            input.extend(publish(version, qos, 123, false));
            input.extend(publish(version, qos, 123, true));
        }
        // PUBREL succeeds even for an unknown ID, including a retransmission.
        input.extend_from_slice(&[0x62, 2, 0, 99, 0x6f, 2, 0, 99, 0xc0, 0]);
        assert_eq!(engine.feed(&input).unwrap().consumed_input, input.len());
        assert_eq!(
            take(&mut engine),
            vec![
                0x40, 2, 0, 123, 0x40, 2, 0, 123, 0x50, 2, 0, 123, 0x50, 2, 0, 123, 0x70, 2, 0, 99,
                0x70, 2, 0, 99, 0xd0, 0
            ]
        );
        assert_eq!(engine.stats().pub_rcvd, 5);
        assert_eq!(engine.stats().pub_rcvd_by_qos, [1, 2, 2]);
        assert_eq!(engine.stats().pubrel_rcvd, 2);
    }
}

#[test]
fn multi_filter_acks_cross_the_vbi_boundary() {
    for version in [4, 5] {
        let mut engine = active(version, Default::default());
        let options: Vec<_> = (0..140).map(|i| (i % 3) as u8 | 0xfc).collect();
        engine.feed(&subscribe(version, &options)).unwrap();
        let mut expected = vec![0, 7];
        if version == 5 {
            expected.push(0);
        }
        expected.extend((0..140).map(|i| (i % 3) as u8));
        assert_eq!(take(&mut engine), frame(0x90, &expected));
        assert_eq!(engine.stats().sub_rcvd, 1);
        assert_eq!(engine.stats().subscribe_entries, 140);
        let mut body = vec![0, 8];
        if version == 5 {
            body.push(0);
        }
        for _ in 0..140 {
            body.extend_from_slice(&[0, 1, b't']);
        }
        engine.feed(&frame(0xa2, &body)).unwrap();
        let mut expected = vec![0, 8];
        if version == 5 {
            expected.resize(143, 0);
        }
        assert_eq!(take(&mut engine), frame(0xb0, &expected));
        assert_eq!(engine.stats().sub_rcvd, 1);
    }
}

#[test]
fn ignored_fields_do_not_trigger_full_protocol_validation() {
    let mut engine = PerfServerEngine::new(Default::default()).unwrap();
    // Invalid protocol name, option bits, property IDs, credentials/Will tail.
    let request = frame(
        0x1f,
        &[0, 1, 0xff, 5, 0xff, 0, 0, 2, 0xff, 0xff, 0, 1, 0xff, 0xff],
    );
    engine.feed(&request).unwrap();
    assert_eq!(take(&mut engine), vec![0x20, 3, 0, 0, 0]);
    // Non-UTF8 filter, unknown properties, ignored option bits with valid QoS 2.
    engine
        .feed(&frame(0x8f, &[0, 0, 2, 0xff, 0xff, 0, 2, 0xff, b'#', 0xfe]))
        .unwrap();
    assert_eq!(take(&mut engine), vec![0x90, 4, 0, 0, 0, 2]);
    // No PUBLISH property decoding, topic syntax checks, or packet-ID checks.
    engine
        .feed(&frame(0x33, &[0, 1, 0xff, 0, 0, 0xff]))
        .unwrap();
    assert_eq!(take(&mut engine), vec![0x40, 2, 0, 0]);
    engine.feed(&[0x30, 0, 0x40, 0, 0x50, 0, 0x70, 0]).unwrap();
    assert!(engine.output().is_empty());
}

#[test]
fn stalled_output_and_partial_writes_never_duplicate_or_lose_counts() {
    let mut engine = active(
        5,
        PerfServerConfig {
            max_output_bytes: 5,
            ..Default::default()
        },
    );
    let requests = [publish(5, 1, 1, false), publish(5, 1, 2, false)].concat();
    let progress = engine.feed(&requests).unwrap();
    assert_eq!(progress.consumed_input, requests.len());
    assert!(progress.buffered_work);
    assert_eq!(engine.stats().pub_rcvd, 1);
    for _ in 0..3 {
        engine.poll().unwrap();
    }
    assert_eq!(engine.stats().pub_rcvd, 1);
    let mut received = engine.output()[..2].to_vec();
    engine.consume_output(2).unwrap();
    engine.poll().unwrap();
    assert_eq!(engine.stats().pub_rcvd, 1);
    received.push(engine.output()[0]);
    engine.consume_output(1).unwrap();
    engine.poll().unwrap(); // Compacts the one remaining byte before appending.
    assert_eq!(engine.stats().pub_rcvd, 2);
    received.extend(take(&mut engine));
    assert_eq!(received, vec![0x40, 2, 0, 1, 0x40, 2, 0, 2]);
    assert_eq!(engine.stats().response_bytes_written, 13); // CONNACK plus ACKs
}

#[test]
fn work_budget_returns_unconsumed_suffix_and_disconnect_drains() {
    let mut engine = PerfServerEngine::new(PerfServerConfig {
        max_packets_per_poll: 1,
        ..Default::default()
    })
    .unwrap();
    let request = [connect(4, true, b"c"), vec![0xc0, 0, 0xe0, 0, 0xc0, 0]].concat();
    let mut used = engine.feed(&request).unwrap().consumed_input;
    assert_eq!(used, request.len() - 6);
    used += engine.feed(&request[used..]).unwrap().consumed_input;
    used += engine.feed(&request[used..]).unwrap().consumed_input;
    assert_eq!(used, request.len() - 2); // Stop at DISCONNECT.
    assert_eq!(engine.read_allowance(), 0);
    assert!(!engine.is_closed());
    assert_eq!(take(&mut engine), vec![0x20, 2, 0, 0, 0xd0, 0]);
    assert!(engine.is_closed());
}

#[test]
fn limits_and_truncated_fields_close_without_counting_rejected_packets() {
    let mut tiny = PerfServerEngine::new(PerfServerConfig {
        max_frame_bytes: 2,
        max_input_bytes: 2,
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        tiny.feed(&[0x30, 0x80]),
        Err(PerfServerError::FrameTooLarge)
    ));
    assert_eq!(tiny.read_allowance(), 0);
    let mut engine = active(
        5,
        PerfServerConfig {
            max_frame_bytes: 64,
            max_input_bytes: 64,
            ..Default::default()
        },
    );
    assert!(matches!(
        engine.feed(&[0x30, 127]),
        Err(PerfServerError::FrameTooLarge)
    ));
    assert!(engine.is_closed());
    for packet in [
        vec![0x32, 1, 0],
        vec![0x62, 1, 0],
        vec![0x82, 3, 0, 1, 0],
        vec![0xa2, 4, 0, 1, 0, 0],
        vec![0x36, 0],
        vec![0x30, 255, 255, 255, 255],
    ] {
        let mut engine = active(5, Default::default());
        assert!(engine.feed(&packet).is_err(), "{packet:?}");
        assert!(engine.is_closed());
        assert_eq!(engine.stats().pub_rcvd + engine.stats().sub_rcvd, 0);
    }
    let mut engine = active(
        5,
        PerfServerConfig {
            max_output_bytes: 5,
            ..Default::default()
        },
    );
    assert!(matches!(
        engine.feed(&subscribe(5, &[0, 1, 2])),
        Err(PerfServerError::OutputTooLarge)
    ));
    assert_eq!(engine.stats().sub_rcvd, 0);
}

#[test]
fn unsupported_versions_directions_and_connection_phases_close() {
    for version in [4, 5] {
        // Reject the entire packet even if valid filters precede the reserved QoS.
        for packet in [
            publish(version, 3, 123, false),
            subscribe(version, &[3]),
            subscribe(version, &[0, 3, 2]),
            subscribe(version, &[0, 1, 0xff]),
        ] {
            let mut engine = active(version, Default::default());
            let accepted_packets = engine.stats().packets_rcvd;
            assert!(matches!(
                engine.feed(&packet),
                Err(PerfServerError::Unsupported)
            ));
            assert!(engine.is_closed());
            assert!(engine.output().is_empty());
            assert_eq!(engine.stats().packets_rcvd, accepted_packets);
            assert_eq!(engine.stats().pub_rcvd, 0);
            assert_eq!(engine.stats().sub_rcvd, 0);
            assert_eq!(engine.stats().subscribe_entries, 0);
        }
    }
    for version in [3, 6] {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        assert!(engine.feed(&connect(version, true, b"c")).is_err());
    }
    let mut engine = PerfServerEngine::new(PerfServerConfig {
        mqtt5: false,
        ..Default::default()
    })
    .unwrap();
    assert!(engine.feed(&connect(5, true, b"c")).is_err());
    for packet in [vec![0xc0, 0], vec![0x30, 0]] {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        assert!(engine.feed(&packet).is_err());
    }
    for packet in [
        connect(5, true, b"second"),
        vec![0xf0, 0],
        vec![0x20, 0],
        vec![0xd0, 0],
    ] {
        let mut engine = active(5, Default::default());
        assert!(engine.feed(&packet).is_err());
        assert_eq!(engine.stats().conn_rcvd, 1);
    }
}

#[test]
fn every_split_of_a_mixed_stream_produces_the_same_responses() {
    let request = [
        connect(5, false, b"c"),
        subscribe(5, &[0, 1, 2]),
        publish(5, 1, 8, false),
        vec![0xc0, 0],
    ]
    .concat();
    for split in 0..=request.len() {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        engine.feed(&request[..split]).unwrap();
        let mut out = take(&mut engine);
        engine.feed(&request[split..]).unwrap();
        out.extend(take(&mut engine));
        assert_eq!(
            out,
            vec![0x20, 3, 1, 0, 0, 0x90, 6, 0, 7, 0, 0, 1, 2, 0x40, 2, 0, 8, 0xd0, 0]
        );
    }
}
