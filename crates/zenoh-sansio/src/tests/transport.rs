use crate::{Transport, ZTransportRx, ZTransportTx, transport::establishment::State};
use core::{cell::RefCell, time::Duration};
use zenoh_proto::{exts::*, fields::*, keyexpr, msgs::*};

#[test]
fn transport_state_handshake() {
    let a_zid = ZenohIdProto::default();
    let mut a = State::WaitingInitSyn {
        mine_zid: a_zid,
        mine_batch_size: 512,
        mine_resolution: Resolution::default(),
        mine_lease: Duration::from_secs(30),
    };

    let b_zid = ZenohIdProto::default();
    let mut b = State::WaitingInitAck {
        mine_zid: b_zid,
        mine_batch_size: 1025,
        mine_resolution: Resolution::default(),
        mine_lease: Duration::from_secs(37),
    };

    let init = InitSyn {
        identifier: InitIdentifier {
            zid: b_zid,
            ..Default::default()
        },
        resolution: InitResolution {
            resolution: Resolution::default(),
            batch_size: BatchSize(1025),
        },
        ..Default::default()
    };

    let mut buff = [0u8; 128];

    macro_rules! buff {
        ($msg:expr) => {{
            let mut writer = &mut buff[..];
            <InitSyn as zenoh_proto::ZEncode>::z_encode($msg, &mut writer).unwrap();
            let len = 128 - writer.len();

            &buff[..len]
        }};
    }

    let mut buff = buff!(&init);
    let mut next = Some(TransportMessage::InitSyn(init));
    let mut desc = None;
    let mut current = &mut a;
    let mut other = &mut b;

    for _ in 0..4 {
        if let Some(response) = next {
            (next, desc) = current.poll((response, buff));
            core::mem::swap(&mut current, &mut other);

            buff = &[];
        }
    }

    assert!(desc.is_some());
    assert!(a.description().is_some() && b.description().is_some());
    assert_eq!(desc.unwrap().batch_size, 512);
    assert_eq!(desc.unwrap().resolution, Resolution::default());
}

#[test]
fn initial_sequence_number_uses_the_negotiated_resolution() {
    let mut offered = Resolution::default();
    offered.set(Field::FrameSN, Bits::U8);
    let mut initiator = State::WaitingInitAck {
        mine_zid: ZenohIdProto::default(),
        mine_batch_size: 512,
        mine_resolution: Resolution::default(),
        mine_lease: Duration::from_secs(30),
    };
    let peer = ZenohIdProto::default();
    let ack = InitAck {
        identifier: InitIdentifier {
            zid: peer,
            ..Default::default()
        },
        resolution: InitResolution {
            resolution: offered,
            batch_size: BatchSize(512),
        },
        ..Default::default()
    };

    let (reply, _) = initiator.poll((TransportMessage::InitAck(ack), &[]));
    let Some(TransportMessage::OpenSyn(open)) = reply else {
        panic!("InitAck must produce OpenSyn");
    };
    assert!(open.sn <= Bits::U8.transport_sn_mask());
}

#[test]
fn acceptor_selects_the_smaller_offered_resolution() {
    let mut offered = Resolution::default();
    offered.set(Field::FrameSN, Bits::U8);
    offered.set(Field::RequestID, Bits::U16);

    let mut acceptor = State::WaitingInitSyn {
        mine_zid: ZenohIdProto::default(),
        mine_batch_size: 512,
        mine_resolution: Resolution::default(),
        mine_lease: Duration::from_secs(30),
    };
    let syn = InitSyn {
        identifier: InitIdentifier::default(),
        resolution: InitResolution {
            resolution: offered,
            batch_size: BatchSize(1024),
        },
        ..Default::default()
    };

    let (reply, _) = acceptor.poll((TransportMessage::InitSyn(syn), b"encoded-init-syn"));
    let Some(TransportMessage::InitAck(ack)) = reply else {
        panic!("InitSyn must produce InitAck");
    };
    assert_eq!(ack.resolution.batch_size, BatchSize(512));
    assert_eq!(ack.resolution.resolution.get(Field::FrameSN), Bits::U8);
    assert_eq!(ack.resolution.resolution.get(Field::RequestID), Bits::U16);
}

#[test]
fn acceptor_rejects_a_tampered_cookie() {
    let mut acceptor = State::WaitingInitSyn {
        mine_zid: ZenohIdProto::default(),
        mine_batch_size: 512,
        mine_resolution: Resolution::default(),
        mine_lease: Duration::from_secs(30),
    };
    let syn = InitSyn {
        identifier: InitIdentifier::default(),
        resolution: InitResolution {
            resolution: Resolution::default(),
            batch_size: BatchSize(512),
        },
        ..Default::default()
    };
    let (reply, _) = acceptor.poll((TransportMessage::InitSyn(syn), b"encoded-init-syn"));
    assert!(matches!(reply, Some(TransportMessage::InitAck(_))));

    let open = OpenSyn {
        lease: Duration::from_secs(30),
        sn: 0,
        cookie: b"tampered-init-syn",
        ..Default::default()
    };
    let (reply, description) = acceptor.poll((TransportMessage::OpenSyn(open), &[]));

    assert!(reply.is_none());
    assert!(description.is_none());
    assert!(acceptor.description().is_none());
}

#[test]
fn transport_handshake() {
    let socket = ([0u8; 512], 0usize, 0usize);
    let socket_ref = RefCell::new(socket);

    let a = Transport::builder([0u8; 512]);
    let b = Transport::builder([0u8; 512]);

    let read = |socket: &mut &RefCell<([u8; 512], usize, usize)>,
                bytes: &mut [u8]|
     -> core::result::Result<usize, i32> {
        let mut borrow_mut = socket.borrow_mut();

        let to_read = bytes.len().min(borrow_mut.2);

        let slice = &borrow_mut.0[borrow_mut.1..(to_read + borrow_mut.1)];
        bytes[..slice.len()].copy_from_slice(slice);
        borrow_mut.1 += to_read;

        Ok(to_read)
    };

    let write = |socket: &mut &RefCell<([u8; 512], usize, usize)>,
                 bytes: &[u8]|
     -> core::result::Result<(), i32> {
        let mut borrow_mut = socket.borrow_mut();
        borrow_mut.0[..bytes.len()].copy_from_slice(bytes);
        borrow_mut.1 = 0;
        borrow_mut.2 = bytes.len();
        Ok(())
    };

    let mut ha = a.listen(&socket_ref, &read, &write);
    let mut hb = b.connect(&socket_ref, &read, &write);

    hb.poll().unwrap();

    for _ in 0..2 {
        ha.poll().unwrap();
        hb.poll().unwrap();
    }

    ha.poll()
        .expect("Unexpected Error")
        .expect("Transport A is not opened yet")
        .open();

    hb.poll()
        .expect("Unexpected Error")
        .expect("Transport B is not opened yet")
        .open();
}

#[test]
fn transport_handshake_streamed() {
    let socket = ([0u8; 512], 0usize, 0usize);
    let socket_ref = RefCell::new(socket);

    let a = Transport::builder([0u8; 512]);
    let b = Transport::builder([0u8; 512]);

    let read = |socket: &mut &RefCell<([u8; 512], usize, usize)>,
                bytes: &mut [u8]|
     -> core::result::Result<usize, i32> {
        let mut borrow_mut = socket.borrow_mut();

        let to_read = bytes.len().min(borrow_mut.2);

        let slice = &borrow_mut.0[borrow_mut.1..(to_read + borrow_mut.1)];
        bytes[..slice.len()].copy_from_slice(slice);
        borrow_mut.1 += to_read;

        Ok(to_read)
    };

    let write = |socket: &mut &RefCell<([u8; 512], usize, usize)>,
                 bytes: &[u8]|
     -> core::result::Result<(), i32> {
        let mut borrow_mut = socket.borrow_mut();
        borrow_mut.0[..bytes.len()].copy_from_slice(bytes);
        borrow_mut.1 = 0;
        borrow_mut.2 = bytes.len();
        Ok(())
    };

    let mut ha = a.listen(&socket_ref, &read, &write).prefixed();
    let mut hb = b.connect(&socket_ref, &read, &write).prefixed();

    hb.poll().unwrap();

    for _ in 0..2 {
        ha.poll().unwrap();
        hb.poll().unwrap();
    }

    ha.poll()
        .expect("Unexpected Error")
        .expect("Transport A is not opened yet")
        .open();

    hb.poll()
        .expect("Unexpected Error")
        .expect("Transport B is not opened yet")
        .open();
}

#[test]
fn transport_streamed_codec() {
    let mut transport = Transport::builder([0u8; 512]).codec();

    let msg = NetworkMessage {
        reliability: Reliability::Reliable,
        qos: QoS::declare(),
        body: NetworkBody::Push(Push {
            wire_expr: WireExpr::from(keyexpr::from_str_unchecked("abc/def")),
            payload: PushBody::Put(Put {
                payload: &[1, 2, 3, 4],
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    transport.tx.encode(msg.as_ref(), None).unwrap();
    transport
        .rx
        .decode_prefixed(transport.tx.flush(true).unwrap().unwrap())
        .unwrap();

    let mut flush = transport.rx.flush();
    let m = flush.next().unwrap().0;

    assert_eq!(flush.count(), 0);
    assert_eq!(m, msg);
}

#[test]
fn close_is_terminal_on_both_ends() {
    let mut sender = Transport::builder([0u8; 512]).codec();
    let mut receiver = Transport::builder([0u8; 512]).codec();

    sender.tx.close().unwrap();
    assert!(sender.tx.closed());

    let close = sender
        .tx
        .flush(false)
        .unwrap()
        .expect("Close must be emitted");
    receiver.rx.decode_raw(close).unwrap();
    assert_eq!(receiver.rx.flush().count(), 0);
    assert!(receiver.rx.closed());

    let msg = NetworkMessage {
        reliability: Reliability::Reliable,
        qos: QoS::declare(),
        body: NetworkBody::Push(Push {
            wire_expr: WireExpr::from(keyexpr::from_str_unchecked("after/close")),
            payload: PushBody::Put(Put {
                payload: &[1],
                ..Default::default()
            }),
            ..Default::default()
        }),
    };
    assert!(sender.tx.encode(msg.as_ref(), None).is_err());
    assert!(sender.tx.flush(false).unwrap().is_none());
}

#[test]
fn an_empty_transport_has_nothing_to_flush() {
    let mut transport = Transport::builder([0u8; 512]).codec();
    assert!(transport.tx.flush(false).unwrap().is_none());
    assert!(transport.tx.flush(true).unwrap().is_none());
}

#[test]
fn lease_deadlines_fire_at_the_deadline() {
    let lease = Duration::from_secs(8);
    let mut transport = Transport::builder([0u8; 512]).with_lease(lease).codec();

    transport.tx.sync(None, Duration::ZERO);
    transport.rx.sync(None, Duration::ZERO);

    assert!(
        !transport
            .tx
            .should_send_keepalive(lease / 4 - Duration::from_nanos(1))
    );
    assert!(transport.tx.should_send_keepalive(lease / 4));
    assert!(!transport.rx.should_close(lease - Duration::from_nanos(1)));
    assert!(transport.rx.should_close(lease));
}

#[test]
fn frame_sequence_numbers_wrap_at_the_negotiated_resolution() {
    let mut resolution = Resolution::default();
    resolution.set(Field::FrameSN, Bits::U8);
    let mut transport = Transport::builder([0u8; 512])
        .with_resolution(resolution)
        .codec();

    for value in 0..130u16 {
        let payload = value.to_le_bytes();
        let msg = NetworkMessage {
            reliability: Reliability::Reliable,
            qos: QoS::declare(),
            body: NetworkBody::Push(Push {
                wire_expr: WireExpr::from(keyexpr::from_str_unchecked("sequence/wrap")),
                payload: PushBody::Put(Put {
                    payload: &payload,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        transport.tx.encode(msg.as_ref(), None).unwrap();
        transport
            .rx
            .decode_raw(transport.tx.flush(false).unwrap().unwrap())
            .unwrap();
        assert_eq!(transport.rx.flush().next().unwrap().0, msg);
    }
}

#[test]
fn interleaved_reliability_channels_have_independent_frame_sequences() {
    use zenoh_proto::ZDecode;

    let mut resolution = Resolution::default();
    resolution.set(Field::FrameSN, Bits::U8);
    let mut transport = Transport::builder([0u8; 512])
        .with_resolution(resolution)
        .codec();
    for round in 0..260u32 {
        for reliability in [Reliability::Reliable, Reliability::BestEffort] {
            let payload = round.to_le_bytes();
            let message = NetworkMessage {
                reliability,
                qos: QoS::default(),
                body: NetworkBody::Push(Push {
                    wire_expr: WireExpr::from(keyexpr::from_str_unchecked("sequence/channels")),
                    payload: PushBody::Put(Put {
                        payload: &payload,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            };
            transport.tx.encode(message.as_ref(), None).unwrap();
            let wire = transport.tx.flush(false).unwrap().unwrap();
            let header = FrameHeader::z_decode(&mut &wire[..]).unwrap();
            assert_eq!(header.reliability, reliability);
            assert_eq!(header.sn, round & Bits::U8.transport_sn_mask());
            transport.rx.decode_raw(wire).unwrap();
            let mut received = transport.rx.flush();
            assert_eq!(received.next().unwrap().0, message);
            assert!(received.next().is_none());
        }
    }
}

mod fragmentation {
    extern crate std;

    use super::*;
    use std::{borrow::Cow, vec, vec::Vec};
    use zenoh_codec::{RCodec, WCodec, Zenoh080};
    use zenoh_protocol as mainline;

    #[test]
    fn integer_encoding_uses_exact_capacity_at_every_length_boundary() {
        use zenoh_proto::{ZEncode, ZLen};
        for bit in 0..64 {
            for value in [(1u64 << bit) - 1, 1u64 << bit, u64::MAX] {
                let mut buffer = [0u8; 9];
                let len = value.z_len();
                let mut writer = &mut buffer[..len];
                value.z_encode(&mut writer).unwrap();
                assert!(writer.is_empty());
                let decoded: u64 = Zenoh080.read(&mut &buffer[..len]).unwrap();
                assert_eq!(decoded, value);
                let mut short = [0xa5; 9];
                assert!(value.z_encode(&mut &mut short[..len - 1]).is_err());
                assert_eq!(short, [0xa5; 9]);
            }
        }
    }

    /// Encode the independent reference implementation's complete network reply.
    fn reply_bytes(payload: &[u8], rid: u32) -> Vec<u8> {
        let message = mainline::network::NetworkMessage::from(
            mainline::network::NetworkBody::Response(mainline::network::Response {
                rid,
                wire_expr: mainline::core::WireExpr {
                    scope: 0,
                    suffix: Cow::Borrowed("blob/chunk"),
                    mapping: mainline::network::Mapping::Sender,
                },
                payload: mainline::zenoh::ResponseBody::Reply(mainline::zenoh::Reply {
                    consolidation: Default::default(),
                    ext_unknown: Vec::new(),
                    payload: mainline::zenoh::PushBody::Put(mainline::zenoh::Put {
                        timestamp: None,
                        encoding: Default::default(),
                        ext_sinfo: None,
                        ext_attachment: None,
                        ext_unknown: Vec::new(),
                        payload: payload.to_vec().into(),
                    }),
                }),
                ext_qos: Default::default(),
                ext_tstamp: None,
                ext_respid: None,
                ext_ts_stack: None,
            }),
        );
        let mut encoded = Vec::new();
        Zenoh080.write(&mut encoded, &message).unwrap();
        encoded
    }

    fn fragment(bytes: &[u8], sn: u32, more: bool, first: bool) -> mainline::transport::Fragment {
        mainline::transport::Fragment {
            reliability: mainline::core::Reliability::Reliable,
            more,
            sn,
            payload: bytes.to_vec().into(),
            ext_qos: Default::default(),
            ext_first: first.then(Default::default),
            ext_drop: None,
        }
    }

    #[test]
    fn handshake_never_offers_more_batch_storage_than_it_owns() {
        use zenoh_proto::ZDecode;

        for (builder, expected) in [
            (Transport::builder(vec![0; 131_072]), u16::MAX),
            (Transport::builder(vec![0; 512]).with_batch_size(4_096), 512),
            (
                Transport::builder(vec![0; 4_096]).with_buff(vec![0; 512]),
                512,
            ),
        ] {
            let offered = core::cell::Cell::new(None);
            let mut handshake = builder.connect(
                (),
                |_: &mut (), _: &mut [u8]| Ok::<_, i32>(0),
                |_: &mut (), bytes: &[u8]| {
                    let syn = InitSyn::z_decode(&mut &bytes[..]).unwrap();
                    offered.set(Some(syn.resolution.batch_size.0));
                    Ok::<_, i32>(())
                },
            );
            handshake.poll().unwrap();
            assert_eq!(offered.get(), Some(expected));
        }
    }

    /// Feed real mainline wire bytes through the public receiver and copy only
    /// completed callback values, so each following batch can reuse its buffers.
    fn receive(
        receiver: &mut Transport<Vec<u8>>,
        fragment: mainline::transport::Fragment,
    ) -> Vec<(u32, Vec<u8>)> {
        let reliability = match fragment.reliability {
            mainline::core::Reliability::Reliable => Reliability::Reliable,
            mainline::core::Reliability::BestEffort => Reliability::BestEffort,
        };
        let mut wire = Vec::new();
        Zenoh080.write(&mut wire, &fragment).unwrap();
        receiver.rx.decode_raw(&wire).unwrap();
        receiver
            .rx
            .flush()
            .map(|(message, _)| {
                assert_eq!(message.reliability, reliability);
                let NetworkBody::Response(response) = message.body else {
                    panic!("expected the complete response");
                };
                let ResponseBody::Reply(reply) = response.payload else {
                    panic!("expected the complete reply");
                };
                let PushBody::Put(value) = reply.payload else {
                    panic!("expected the original value");
                };
                (response.rid, value.payload.to_vec())
            })
            .collect()
    }

    #[test]
    fn mainline_fragmented_reply_is_delivered_only_when_complete() {
        let payload = vec![0xa5; 65_536];
        let encoded = reply_bytes(&payload, 7);
        let mut receiver = Transport::builder(vec![0; 131_072])
            .with_batch_size(32_767)
            .codec();
        let count = encoded.len().div_ceil(1_024);
        let mut delivered = Vec::new();
        for (index, bytes) in encoded.chunks(1_024).enumerate() {
            let values = receive(
                &mut receiver,
                fragment(bytes, index as u32, index + 1 < count, index == 0),
            );
            if index + 1 < count {
                assert!(
                    values.is_empty(),
                    "partial data must never reach a callback"
                );
            }
            delivered.extend(values);
        }
        assert_eq!(delivered, vec![(7, payload)]);
    }

    #[test]
    fn outgoing_large_message_reassembles_with_the_mainline_codec() {
        let payload = vec![0x3c; 8 * 1024 * 1024];
        let message = NetworkMessage {
            reliability: Reliability::Reliable,
            qos: QoS::default(),
            body: NetworkBody::Push(Push {
                wire_expr: WireExpr::from(keyexpr::from_str_unchecked("large/value")),
                payload: PushBody::Put(Put {
                    payload: &payload,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let mut sender = Transport::builder(vec![0; payload.len() + 65_535])
            .with_batch_size(32_767)
            .codec();
        sender.tx.encode(message.as_ref(), None).unwrap();
        let mut assembled = Vec::new();
        let mut count = 0;
        let mut more = true;
        while let Some(wire) = sender.tx.flush(false).unwrap() {
            assert!(wire.len() <= 32_767);
            assert!(more, "the final fragment must end the transmission");
            let decoded: mainline::transport::TransportMessage =
                Zenoh080.read(&mut &wire[..]).unwrap();
            let mainline::transport::TransportBody::Fragment(fragment) = decoded.body else {
                panic!("a large message must use native transport fragments");
            };
            assert_eq!(fragment.sn, count);
            assert_eq!(fragment.ext_first.is_some(), count == 0);
            more = fragment.more;
            assembled.extend_from_slice(fragment.payload.as_slice());
            count += 1;
        }
        assert!(count > 1);
        assert!(!more);
        let decoded: mainline::network::NetworkMessage =
            Zenoh080.read(&mut &assembled[..]).unwrap();
        let mainline::network::NetworkBody::Push(push) = decoded.body else {
            panic!("expected the original publish");
        };
        let mainline::zenoh::PushBody::Put(put) = push.payload else {
            panic!("expected the original value");
        };
        assert_eq!(put.payload, payload.into());
    }

    #[test]
    fn a_full_batch_preserves_accepted_bytes_before_fragmenting_the_next_message() {
        use zenoh_proto::ZDecode;

        let first = reply_bytes(&[1; 8], 7);
        let second = reply_bytes(&[2; 2_048], 8);
        let first_message = NetworkMessage {
            reliability: Reliability::Reliable,
            qos: QoS::default(),
            body: NetworkBody::z_decode(&mut &first[..]).unwrap(),
        };
        let second_message = NetworkMessage {
            reliability: Reliability::Reliable,
            qos: QoS::default(),
            body: NetworkBody::z_decode(&mut &second[..]).unwrap(),
        };
        let mut sender = Transport::builder(vec![0; 4_096])
            .with_batch_size(64)
            .codec();
        let mut receiver = Transport::builder(vec![0; 4_096])
            .with_batch_size(64)
            .codec();
        sender
            .tx
            .encode(first_message.as_ref(), Some(&first))
            .unwrap();
        assert!(
            sender
                .tx
                .encode(second_message.as_ref(), Some(&second))
                .is_err()
        );
        receiver
            .rx
            .decode_raw(sender.tx.flush(false).unwrap().unwrap())
            .unwrap();
        let first_values: Vec<_> = receiver
            .rx
            .flush()
            .map(|(_, bytes)| bytes.to_vec())
            .collect();
        assert_eq!(first_values, vec![first]);
        sender
            .tx
            .encode(second_message.as_ref(), Some(&second))
            .unwrap();
        let mut second_values = Vec::new();
        while let Some(wire) = sender.tx.flush(false).unwrap() {
            receiver.rx.decode_raw(wire).unwrap();
            second_values.extend(receiver.rx.flush().map(|(_, bytes)| bytes.to_vec()));
        }
        assert_eq!(second_values, vec![second]);
    }

    #[test]
    fn length_prefixed_fragments_preserve_one_complete_network_reply() {
        let payload = vec![0x5a; 65_536];
        let encoded = reply_bytes(&payload, 11);
        let mut receiver = Transport::builder(vec![0; 131_072])
            .with_batch_size(32_767)
            .codec();
        let count = encoded.len().div_ceil(16_000);
        for (index, bytes) in encoded.chunks(16_000).enumerate() {
            let mut wire = vec![0; 2];
            Zenoh080
                .write(
                    &mut wire,
                    &fragment(bytes, index as u32, index + 1 < count, index == 0),
                )
                .unwrap();
            let len = u16::try_from(wire.len() - 2).unwrap();
            wire[..2].copy_from_slice(&len.to_le_bytes());
            receiver.rx.decode_prefixed(&wire).unwrap();
            let mut values = receiver.rx.flush();
            if index + 1 < count {
                assert!(values.next().is_none());
            } else {
                let (message, bytes) = values.next().expect("complete reply");
                assert_eq!(bytes, encoded);
                let NetworkBody::Response(response) = message.body else {
                    panic!("expected a response");
                };
                assert_eq!(response.rid, 11);
                assert!(values.next().is_none());
            }
        }
    }

    #[test]
    fn reassembly_capacity_is_exact_and_a_new_message_recovers_after_overflow() {
        let payload = vec![0xa5; 1_024];
        let encoded = reply_bytes(&payload, 7);
        for capacity in [encoded.len(), encoded.len() - 1] {
            let mut receiver = Transport::builder(vec![0; capacity])
                .with_batch_size(128)
                .codec();
            let count = encoded.len().div_ceil(64);
            let mut delivered = Vec::new();
            for (index, bytes) in encoded.chunks(64).enumerate() {
                delivered.extend(receive(
                    &mut receiver,
                    fragment(bytes, index as u32, index + 1 < count, index == 0),
                ));
            }
            assert_eq!(
                delivered,
                if capacity == encoded.len() {
                    vec![(7, payload.clone())]
                } else {
                    Vec::new()
                }
            );
            assert_eq!(
                receive(
                    &mut receiver,
                    fragment(&reply_bytes(&[42], 8), count as u32, false, true)
                ),
                vec![(8, vec![42])],
            );
        }
    }

    #[test]
    fn gaps_drop_markers_and_replaced_starts_never_publish_partial_data() {
        let old = reply_bytes(&[0xa5; 32], 7);
        let current = reply_bytes(&[42], 8);
        for interruption in ["missing_start", "gap", "drop", "new_start"] {
            let mut receiver = Transport::builder(vec![0; 512]).codec();
            if interruption == "missing_start" {
                assert!(receive(&mut receiver, fragment(&old[16..], 0, false, false)).is_empty());
            } else {
                assert!(receive(&mut receiver, fragment(&old[..16], 0, true, true)).is_empty());
            }
            let next = match interruption {
                "gap" => {
                    assert!(
                        receive(&mut receiver, fragment(&old[16..], 2, false, false)).is_empty()
                    );
                    3
                }
                "drop" => {
                    let mut drop = fragment(&[], 1, false, false);
                    drop.ext_drop = Some(Default::default());
                    assert!(receive(&mut receiver, drop).is_empty());
                    2
                }
                _ => 1,
            };
            assert_eq!(
                receive(&mut receiver, fragment(&current, next, false, true)),
                vec![(8, vec![42])]
            );
        }
    }

    #[test]
    fn a_duplicate_fragment_does_not_reset_or_extend_the_message() {
        let bytes = reply_bytes(&[42; 32], 7);
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        assert!(receive(&mut receiver, fragment(&bytes[..16], 0, true, true)).is_empty());
        assert!(receive(&mut receiver, fragment(&bytes[..16], 0, true, true)).is_empty());
        assert_eq!(
            receive(&mut receiver, fragment(&bytes[16..], 1, false, false)),
            vec![(7, vec![42; 32])]
        );
    }

    #[test]
    fn unnegotiated_markers_do_not_change_the_fragment_chain() {
        let bytes = reply_bytes(&[42; 32], 7);
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        receiver.rx = crate::TransportRx::new(
            vec![0; 512],
            512,
            0,
            Resolution::default(),
            Duration::from_secs(30),
            false,
        );
        assert!(receive(&mut receiver, fragment(&bytes[..16], 0, true, false)).is_empty());
        let mut last = fragment(&bytes[16..], 1, false, true);
        last.ext_drop = Some(Default::default());
        assert_eq!(receive(&mut receiver, last), vec![(7, vec![42; 32])]);
    }

    #[test]
    fn reliability_channels_reassemble_independently() {
        let reliable = reply_bytes(&[1; 32], 7);
        let best_effort = reply_bytes(&[2; 32], 8);
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        assert!(receive(&mut receiver, fragment(&reliable[..16], 0, true, true)).is_empty());
        let mut first = fragment(&best_effort[..16], 0, true, true);
        first.reliability = mainline::core::Reliability::BestEffort;
        assert!(receive(&mut receiver, first).is_empty());
        assert_eq!(
            receive(&mut receiver, fragment(&reliable[16..], 1, false, false)),
            vec![(7, vec![1; 32])]
        );
        let mut last = fragment(&best_effort[16..], 1, false, false);
        last.reliability = mainline::core::Reliability::BestEffort;
        assert_eq!(receive(&mut receiver, last), vec![(8, vec![2; 32])]);
    }

    #[test]
    fn fragment_sequence_numbers_wrap_at_the_negotiated_resolution() {
        let mut resolution = Resolution::default();
        resolution.set(Field::FrameSN, Bits::U8);
        let mask = Bits::U8.transport_sn_mask();
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        receiver.rx = crate::TransportRx::new(
            vec![0; 512],
            512,
            mask,
            resolution,
            Duration::from_secs(30),
            true,
        );
        let bytes = reply_bytes(&[42; 32], 7);
        assert!(receive(&mut receiver, fragment(&bytes[..16], mask, true, true)).is_empty());
        assert_eq!(
            receive(&mut receiver, fragment(&bytes[16..], 0, false, false)),
            vec![(7, vec![42; 32])]
        );
    }

    #[test]
    fn clear_and_close_discard_incomplete_messages() {
        let bytes = reply_bytes(&[42; 32], 7);
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        assert!(receive(&mut receiver, fragment(&bytes[..16], 0, true, true)).is_empty());
        receiver.rx.clear();
        assert!(receive(&mut receiver, fragment(&bytes[16..], 1, false, false)).is_empty());
        assert_eq!(
            receive(&mut receiver, fragment(&bytes, 2, false, true)),
            vec![(7, vec![42; 32])]
        );
        assert!(receive(&mut receiver, fragment(&bytes[..16], 3, true, true)).is_empty());
        let mut sender = Transport::builder(vec![0; 512]).codec();
        sender.tx.close().unwrap();
        receiver
            .rx
            .decode_raw(sender.tx.flush(false).unwrap().unwrap())
            .unwrap();
        assert_eq!(receiver.rx.flush().count(), 0);
        assert!(receiver.rx.closed());
        assert!(receive(&mut receiver, fragment(&bytes[16..], 4, false, false)).is_empty());
    }

    #[test]
    fn a_fragment_cannot_smuggle_two_network_messages() {
        let first = reply_bytes(&[1], 7);
        let mut combined = first.clone();
        combined.extend(reply_bytes(&[2], 8));
        let mut receiver = Transport::builder(vec![0; 512]).codec();
        assert!(receive(&mut receiver, fragment(&combined, 0, false, true)).is_empty());
        assert_eq!(
            receive(&mut receiver, fragment(&first, 1, false, true)),
            vec![(7, vec![1])]
        );
    }
}
