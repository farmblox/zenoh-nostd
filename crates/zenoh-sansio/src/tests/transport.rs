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

    transport.tx.encode_ref(core::iter::once(msg.as_ref()));
    transport
        .rx
        .decode_prefixed(transport.tx.flush_prefixed().unwrap())
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

    sender.tx.close();
    assert!(sender.tx.closed());

    let close = sender.tx.flush_raw().expect("Close must be emitted");
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
    sender.tx.encode_ref(core::iter::once(msg.as_ref()));
    assert!(sender.tx.flush_raw().is_none());
}

#[test]
fn an_empty_transport_has_nothing_to_flush() {
    let mut transport = Transport::builder([0u8; 512]).codec();
    assert!(transport.tx.flush_raw().is_none());
    assert!(transport.tx.flush_prefixed().is_none());
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

        transport.tx.encode_ref(core::iter::once(msg.as_ref()));
        transport
            .rx
            .decode_raw(transport.tx.flush_raw().unwrap())
            .unwrap();
        assert_eq!(transport.rx.flush().next().unwrap().0, msg);
    }
}
