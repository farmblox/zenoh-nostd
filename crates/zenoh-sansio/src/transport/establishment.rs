use core::time::Duration;

use sha3::{
    Shake128,
    digest::{ExtendableOutput, Update, XofReader},
};

use zenoh_proto::{TransportError, fields::*, msgs::*};

/// Everything that describes an Opened Transport between two peers
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) struct Description {
    pub mine_zid: ZenohIdProto,

    pub batch_size: u16,
    pub resolution: Resolution,

    pub mine_lease: Duration,
    pub other_lease: Duration,

    pub mine_sn: u32,
    pub other_sn: u32,

    pub other_zid: ZenohIdProto,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum State {
    WaitingInitSyn {
        /// Mine zid
        mine_zid: ZenohIdProto,
        /// Mine startup batch_size
        mine_batch_size: u16,
        /// Mine startup resolution
        mine_resolution: Resolution,
        /// Mine lease,
        mine_lease: Duration,
    },
    WaitingOpenSyn {
        /// Mine zid
        mine_zid: ZenohIdProto,
        /// Negotiated batch size.
        batch_size: u16,
        /// Negotiated resolution.
        resolution: Resolution,
        /// Mine lease,
        mine_lease: Duration,
        /// Peer zid from the InitSyn.
        other_zid: ZenohIdProto,
        /// Integrity check for the opaque cookie echoed in OpenSyn.
        cookie_digest: [u8; 32],
    },
    WaitingInitAck {
        /// Mine zid
        mine_zid: ZenohIdProto,
        /// Mine startup batch_size
        mine_batch_size: u16,
        /// Mine startup resolution
        mine_resolution: Resolution,
        /// Mine lease,
        mine_lease: Duration,
    },
    WaitingOpenAck {
        /// Mine zid
        mine_zid: ZenohIdProto,
        /// Negotiated batch_size
        batch_size: u16,
        /// Negotiated resolution
        resolution: Resolution,
        /// Computed sn,
        sn: u32,
        /// Mine lease,
        mine_lease: Duration,
        /// Peer zid
        other_zid: ZenohIdProto,
    },
    Opened(Description),
}

fn compute_sn(zid1: ZenohIdProto, zid2: ZenohIdProto, resolution: Resolution) -> u32 {
    let mut hasher = Shake128::default();
    hasher.update(&zid1.as_le_bytes()[..zid1.size()]);
    hasher.update(&zid2.as_le_bytes()[..zid2.size()]);
    let mut bytes = 0_u32.to_le_bytes();
    hasher.finalize_xof().read(&mut bytes);
    u32::from_le_bytes(bytes) & resolution.get(Field::FrameSN).transport_sn_mask()
}

fn select_resolution(mine: Resolution, offered: Resolution) -> Resolution {
    let mut selected = Resolution::default();
    selected.set(
        Field::FrameSN,
        mine.get(Field::FrameSN).min(offered.get(Field::FrameSN)),
    );
    selected.set(
        Field::RequestID,
        mine.get(Field::RequestID)
            .min(offered.get(Field::RequestID)),
    );
    selected
}

fn selected_resolution_is_valid(mine: Resolution, selected: Resolution) -> bool {
    selected.get(Field::FrameSN) <= mine.get(Field::FrameSN)
        && selected.get(Field::RequestID) <= mine.get(Field::RequestID)
}

fn cookie_digest(cookie: &[u8]) -> [u8; 32] {
    let mut hasher = Shake128::default();
    hasher.update(cookie);
    let mut digest = [0; 32];
    hasher.finalize_xof().read(&mut digest);
    digest
}

impl State {
    pub(crate) fn poll<'a>(
        &mut self,
        input: (TransportMessage<'a>, &'a [u8]),
    ) -> (Option<TransportMessage<'a>>, Option<Description>) {
        if let Self::Opened(description) = &self {
            return (None, Some(*description));
        }

        let (msg, buff) = input;

        match msg {
            // This state machine remains allocated for the connection between
            // InitSyn and OpenSyn. Keep the negotiated state here and use the
            // echoed cookie only as an opaque integrity token; unlike the
            // stateless mainline acceptor, no encrypted state cookie is needed.
            TransportMessage::InitSyn(syn) => match *self {
                Self::WaitingInitSyn {
                    mine_zid,
                    mine_batch_size,
                    mine_resolution,
                    mine_lease,
                } => {
                    zenoh_proto::debug!(
                        "Received InitSyn on transport {:?} -> NEW!({:?})",
                        mine_zid,
                        syn.identifier.zid
                    );

                    let batch_size = mine_batch_size.min(syn.resolution.batch_size.0);
                    let resolution = select_resolution(mine_resolution, syn.resolution.resolution);

                    *self = Self::WaitingOpenSyn {
                        mine_zid,
                        batch_size,
                        resolution,
                        mine_lease,
                        other_zid: syn.identifier.zid,
                        cookie_digest: cookie_digest(buff),
                    };

                    (
                        Some(TransportMessage::InitAck(InitAck {
                            identifier: InitIdentifier {
                                zid: mine_zid,
                                ..Default::default()
                            },
                            resolution: InitResolution {
                                resolution,
                                batch_size: BatchSize(batch_size),
                            },
                            cookie: buff,
                            ..Default::default()
                        })),
                        None,
                    )
                }
                _ => zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidState),
            },
            // Negotiate values, pass the cookie back
            TransportMessage::InitAck(ack) => match *self {
                Self::WaitingInitAck {
                    mine_zid,
                    mine_batch_size,
                    mine_resolution,
                    mine_lease,
                } => {
                    zenoh_proto::debug!(
                        "Received InitAck on transport {:?} -> ({:?})",
                        mine_zid,
                        ack.identifier.zid
                    );

                    let batch_size = mine_batch_size.min(ack.resolution.batch_size.0);
                    let resolution = ack.resolution.resolution;
                    if !selected_resolution_is_valid(mine_resolution, resolution) {
                        zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidAttribute);
                    }
                    let sn = compute_sn(mine_zid, ack.identifier.zid, resolution);

                    *self = Self::WaitingOpenAck {
                        mine_zid,
                        batch_size,
                        resolution,
                        sn,
                        mine_lease,
                        other_zid: ack.identifier.zid,
                    };

                    (
                        Some(TransportMessage::OpenSyn(OpenSyn {
                            lease: mine_lease,
                            sn,
                            cookie: ack.cookie,
                            ..Default::default()
                        })),
                        None,
                    )
                }
                _ => zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidState),
            },
            // Negotiate values, open the transport. Ack the open
            TransportMessage::OpenSyn(open) => match *self {
                Self::WaitingOpenSyn {
                    mine_zid,
                    batch_size,
                    resolution,
                    mine_lease,
                    other_zid,
                    cookie_digest: expected_cookie,
                } => {
                    if cookie_digest(open.cookie) != expected_cookie {
                        zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidAttribute);
                    }

                    zenoh_proto::debug!(
                        "Received OpenSyn on transport {:?} -> ({:?})",
                        mine_zid,
                        other_zid
                    );

                    let sn = compute_sn(mine_zid, other_zid, resolution);

                    let description = Description {
                        mine_zid,
                        batch_size,
                        resolution,
                        mine_lease,
                        other_lease: open.lease,
                        mine_sn: sn,
                        other_sn: open.sn,
                        other_zid,
                    };

                    *self = Self::Opened(description);

                    (
                        Some(TransportMessage::OpenAck(OpenAck {
                            lease: mine_lease,
                            sn,
                            ..Default::default()
                        })),
                        Some(description),
                    )
                }
                _ => zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidState),
            },
            // Open the transport
            TransportMessage::OpenAck(ack) => match *self {
                Self::WaitingOpenAck {
                    mine_zid,
                    batch_size,
                    resolution,
                    sn,
                    mine_lease,
                    other_zid,
                } => {
                    zenoh_proto::debug!(
                        "Received OpenAck on transport {:?} -> ({:?})",
                        mine_zid,
                        other_zid,
                    );

                    let description = Description {
                        mine_zid,
                        batch_size,
                        resolution,
                        mine_lease,
                        other_lease: ack.lease,
                        mine_sn: sn,
                        other_sn: ack.sn,
                        other_zid,
                    };

                    *self = Self::Opened(description);

                    (None, Some(description))
                }
                _ => zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidState),
            },
            _ => zenoh_proto::zbail!(@ret (None, None), TransportError::InvalidState),
        }
    }

    pub(crate) fn description(&self) -> Option<Description> {
        match self {
            Self::Opened(description) => Some(*description),
            _ => None,
        }
    }
}
