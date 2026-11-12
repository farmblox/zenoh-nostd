use core::fmt::Display;
use core::time::Duration;

use zenoh_proto::{
    EitherError, TransportError, ZBodyDecode, ZDecode, ZReadable,
    fields::{Field, Resolution},
    msgs::*,
};

use crate::{
    ZTransportRx,
    transport::{TransportTx, channel_index},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum State {
    Opened,
    Used,
    Synchronized { last_received: Duration },
    Closed,
}

#[derive(Debug)]
pub struct TransportRx<Buff> {
    buff: Buff,

    // Each reliability channel owns at most one bounded, incomplete message.
    // The input batch remains separate so borrowed network messages stay valid.
    reassembly: [Buff; 2],
    fragments: [Option<FragmentChain>; 2],
    fragment_markers: bool,

    cursor: usize,
    batch_size: usize,

    sn: [u32; 2],
    resolution: Resolution,
    lease: Duration,

    state: State,

    ignore_invalid_sn: bool,
}

#[derive(Debug, Clone, Copy)]
struct FragmentChain {
    len: usize,
    next_sn: u32,
    qos: zenoh_proto::exts::QoS,
}

/// Advance one channel only for a valid forward transport sequence number.
fn accept_sequence(next: &mut u32, sn: u32, resolution: Resolution, ignore: bool) -> bool {
    let mask = resolution.get(Field::FrameSN).transport_sn_mask();
    if !ignore {
        let gap = sn.wrapping_sub(*next) & mask;
        if (sn & !mask) != 0 || gap > (mask >> 1).saturating_sub(1) {
            zenoh_proto::error!("Invalid transport sequence {}, expected {}", sn, *next);
            return false;
        }
    }
    *next = sn.wrapping_add(1) & mask;
    true
}

/// Append only contiguous fragments. A new first marker, explicit drop, gap or
/// capacity failure discards the incomplete message before another can begin.
fn append_fragment(
    chain: &mut Option<FragmentChain>,
    buffer: &mut [u8],
    fragment: &Fragment<'_>,
    markers: bool,
    mask: u32,
) -> Option<usize> {
    if markers {
        if fragment.first.is_some() {
            *chain = None;
        } else if chain.is_none() {
            return None;
        }
        if fragment.drop.is_some() {
            *chain = None;
            return None;
        }
    }
    let held = chain.unwrap_or(FragmentChain {
        len: 0,
        next_sn: fragment.sn,
        qos: fragment.qos,
    });
    let end = held.len.checked_add(fragment.payload.len());
    if held.next_sn != fragment.sn
        || held.qos != fragment.qos
        || end.is_none_or(|end| end > buffer.len())
    {
        *chain = None;
        return None;
    }
    let end = end.expect("the bounded end was checked");
    buffer[held.len..end].copy_from_slice(fragment.payload);
    *chain = fragment.more.then_some(FragmentChain {
        len: end,
        next_sn: fragment.sn.wrapping_add(1) & mask,
        qos: fragment.qos,
    });
    (!fragment.more).then_some(end)
}

impl<Buff> TransportRx<Buff> {
    pub(crate) fn new(
        buff: Buff,

        batch_size: usize,
        sn: u32,
        resolution: Resolution,
        lease: Duration,
        fragment_markers: bool,
    ) -> Self
    where
        Buff: Clone,
    {
        Self {
            reassembly: [buff.clone(), buff.clone()],
            fragments: [None; 2],
            fragment_markers,
            buff,

            cursor: 0,
            batch_size,

            sn: [sn; 2],
            resolution,
            lease,

            state: State::Opened,
            ignore_invalid_sn: false,
        }
    }

    pub(crate) fn into_inner(self) -> Buff {
        self.buff
    }

    pub(crate) fn flush_transport(&mut self) -> impl Iterator<Item = (TransportMessage<'_>, &[u8])>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        let size = core::cmp::min(
            self.buff.as_ref().len(),
            core::cmp::min(self.batch_size, self.cursor),
        );
        self.cursor = 0;
        let mut reader = &self.buff.as_ref()[..size];
        let mut last_frame = None;
        let sn = &mut self.sn;
        let resolution = self.resolution;
        let ignore = self.ignore_invalid_sn;

        core::iter::from_fn(move || {
            Self::decode(&mut reader, &mut last_frame, sn, resolution, ignore)
        })
        .filter_map(|m| match m.0 {
            Message::Transport(msg) => Some((msg, m.1)),
            _ => None,
        })
    }

    pub fn ignore_invalid_sn(&mut self) {
        self.ignore_invalid_sn = true;
    }

    pub fn sync(&mut self, tx: Option<&TransportTx<Buff>>, now: Duration) {
        if let Some(tx) = tx
            && tx.closed()
        {
            self.state = State::Closed;
            return;
        }

        if self.should_close(now) {
            self.state = State::Closed;
        }

        if matches!(self.state, State::Used | State::Opened) {
            self.state = State::Synchronized { last_received: now };
        };
    }

    pub fn next_timeout(&self) -> Duration {
        match self.state {
            State::Opened | State::Closed | State::Used => Duration::from_secs(0),
            State::Synchronized { last_received } => last_received + self.lease,
        }
    }

    pub fn should_close(&self, now: Duration) -> bool {
        match self.state {
            State::Opened | State::Closed | State::Used => false,
            State::Synchronized { last_received } => now >= last_received + self.lease,
        }
    }

    pub fn closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    pub(crate) fn decode<'a>(
        reader: &mut &'a [u8],
        last_frame: &mut Option<FrameHeader>,
        sn: &mut [u32; 2],
        resolution: Resolution,
        ignore: bool,
    ) -> Option<(Message<'a>, &'a [u8])>
    where
        Buff: AsRef<[u8]>,
    {
        if !reader.can_read() {
            return None;
        }

        let (data, start) = (reader.as_ptr(), reader.len());

        let header = reader
            .read_u8()
            .expect("reader should not be empty at this stage");

        macro_rules! decode {
            ($ty:ty) => {
                match <$ty as ZBodyDecode>::z_body_decode(reader, header) {
                    Ok(msg) => msg,
                    Err(e) => {
                        zenoh_proto::error!(
                            "Failed to decode message of type {}: {}. Skipping the rest of the message - {}",
                            core::any::type_name::<$ty>(),
                            e,
                            zenoh_proto::zctx!()
                        );

                        return None;
                    }
                }
            };

            (@Transport $ty:ident) => {{
                last_frame.take();
                Message::Transport(TransportMessage:: $ty (decode!($ty)))
            }};

            (@Network $ty:ident) => {
                Message::Network(NetworkMessage {
                    reliability: last_frame.as_ref().expect("Should be in frame").reliability,
                    qos: last_frame.as_ref().expect("Should be in frame").qos,
                    body: NetworkBody:: $ty (decode!($ty))
                })
            };
        }

        let ack = header & 0b0010_0000 != 0;
        let net = last_frame.is_some();
        let ifinal = header & 0b0110_0000 == 0;
        let id = header & 0b0001_1111;

        let body = match id {
            FrameHeader::ID => {
                let header = decode!(FrameHeader);

                if !accept_sequence(
                    &mut sn[channel_index(header.reliability)],
                    header.sn,
                    resolution,
                    ignore,
                ) {
                    return None;
                }

                last_frame.replace(header);

                return Self::decode(reader, last_frame, sn, resolution, ignore);
            }
            InitAck::ID if ack => decode!(@Transport InitAck),
            InitSyn::ID => decode!(@Transport InitSyn),
            OpenAck::ID if ack => decode!(@Transport OpenAck),
            OpenSyn::ID => decode!(@Transport OpenSyn),
            Close::ID => decode!(@Transport Close),
            Fragment::ID => decode!(@Transport Fragment),
            KeepAlive::ID => decode!(@Transport KeepAlive),
            Push::ID if net => decode!(@Network Push),
            Request::ID if net => decode!(@Network Request),
            Response::ID if net => decode!(@Network Response),
            ResponseFinal::ID if net => decode!(@Network ResponseFinal),
            InterestFinal::ID if net && ifinal => decode!(@Network InterestFinal),
            Interest::ID if net => decode!(@Network Interest),
            Declare::ID if net => decode!(@Network Declare),
            _ => {
                zenoh_proto::error!(
                    "Unrecognized message header: {:08b}. Skipping the rest of the message - {}",
                    header,
                    zenoh_proto::zctx!()
                );
                return None;
            }
        };

        let len = start - reader.len();
        Some((body, unsafe { core::slice::from_raw_parts(data, len) }))
    }
}

impl<Buff> ZTransportRx for TransportRx<Buff>
where
    Buff: AsMut<[u8]> + AsRef<[u8]>,
{
    fn decode_prefixed(&mut self, mut read: &[u8]) -> core::result::Result<(), TransportError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        if read.is_empty() || self.state == State::Closed {
            return Ok(());
        }

        self.decode_prefixed_with(|data| {
            let size = data.len().min(read.len());
            let (ret, remain) = read.split_at(size);
            data[..size].copy_from_slice(ret);
            read = remain;
            Ok::<_, TransportError>(size)
        })
        .map_err(|e| e.flatten())
    }

    fn decode_raw(&mut self, read: &[u8]) -> core::result::Result<(), TransportError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        if read.is_empty() || self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read.len();
        if buff.len() < read.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        buff[..len].copy_from_slice(read);

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn decode_prefixed_with<E>(
        &mut self,
        mut read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        if 2 > buff.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        let mut len = [0u8; 2];
        let l = read(&mut len).map_err(EitherError::B)?;

        if l == 0 {
            return Ok(());
        } else if l != 2 {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        let len = u16::from_le_bytes(len) as usize;
        if len > u16::MAX as usize || len > buff.len() {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute);
        }

        if read(&mut buff[..len]).map_err(EitherError::B)? != len {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn decode_raw_with<E>(
        &mut self,
        mut read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read(buff).map_err(EitherError::B)?;

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    async fn decode_prefixed_with_async<E>(
        &mut self,
        mut read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        if 2 > buff.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        let mut len = [0u8; 2];
        let l = read(&mut len).await.map_err(EitherError::B)?;

        if l == 0 {
            return Ok(());
        } else if l != 2 {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        let len = u16::from_le_bytes(len) as usize;
        if len > u16::MAX as usize || len > buff.len() {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute);
        }

        if read(&mut buff[..len]).await.map_err(EitherError::B)? != len {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    async fn decode_raw_with_async<E>(
        &mut self,
        mut read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read(buff).await.map_err(EitherError::B)?;

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn flush(&mut self) -> impl Iterator<Item = (NetworkMessage<'_>, &'_ [u8])>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        let size = core::cmp::min(
            self.buff.as_ref().len(),
            core::cmp::min(self.batch_size, self.cursor),
        );
        // Draining a batch retains incomplete messages for their next fragment.
        self.cursor = 0;
        let mut reader = &self.buff.as_ref()[..size];
        let mut last_frame = None;
        let sn = &mut self.sn;
        let resolution = self.resolution;
        let ignore = self.ignore_invalid_sn;
        let state = &mut self.state;
        let chains = &mut self.fragments;
        let markers = self.fragment_markers;
        let [best_effort, reliable] = &mut self.reassembly;
        let mut buffers = [Some(best_effort.as_mut()), Some(reliable.as_mut())];

        core::iter::from_fn(move || {
            loop {
                let (message, bytes) =
                    Self::decode(&mut reader, &mut last_frame, sn, resolution, ignore)?;
                match message {
                    Message::Network(message) => return Some((message, bytes)),
                    Message::Transport(TransportMessage::Close(_)) => {
                        *state = State::Closed;
                        chains.fill(None);
                        return None;
                    }
                    Message::Transport(TransportMessage::Fragment(fragment)) => {
                        let channel = channel_index(fragment.reliability);
                        if !accept_sequence(&mut sn[channel], fragment.sn, resolution, ignore) {
                            continue;
                        }
                        let Some(len) = append_fragment(
                            &mut chains[channel],
                            buffers[channel].as_deref_mut()?,
                            &fragment,
                            markers,
                            resolution.get(Field::FrameSN).transport_sn_mask(),
                        ) else {
                            continue;
                        };
                        // A Fragment consumes the rest of its batch. Take this
                        // borrow once so the iterator can never overwrite bytes
                        // that it has already returned to its caller.
                        let bytes = &buffers[channel].take()?[..len];
                        let mut encoded = bytes;
                        let body = NetworkBody::z_decode(&mut encoded).ok()?;
                        if encoded.can_read() {
                            return None;
                        }
                        return Some((
                            NetworkMessage {
                                reliability: fragment.reliability,
                                qos: fragment.qos,
                                body,
                            },
                            bytes,
                        ));
                    }
                    Message::Transport(_) => {}
                }
            }
        })
    }

    fn clear(&mut self) {
        self.cursor = 0;
        self.fragments.fill(None);
    }
}
