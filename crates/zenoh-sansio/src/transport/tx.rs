use core::{ops::Range, time::Duration};

use zenoh_proto::{
    TransportError, ZEncode, ZLen, ZWriteable,
    fields::{Field, Resolution},
    msgs::{Close, Fragment, FrameHeader, KeepAlive, NetworkMessageRef, TransportMessageRef},
};

use crate::{
    ZTransportTx,
    transport::{TransportRx, channel_index},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum State {
    Opened,
    Used,
    Synchronized { last_sent: Duration },
    Closed,
}

#[derive(Debug)]
pub struct TransportTx<Buff> {
    buff: Buff,
    cursor: usize,
    batch_size: usize,

    sn: [u32; 2],
    resolution: Resolution,
    last_frame: Option<FrameHeader>,
    fragments: Option<Fragmented>,
    fragment_markers: bool,
    lease: Duration,

    state: State,
}

/// The remaining encoded body stays in the existing transmit buffer.
#[derive(Debug)]
struct Fragmented {
    remaining: Range<usize>,
    header: Fragment<'static>,
}

impl<Buff> TransportTx<Buff> {
    pub(crate) fn new(
        buff: Buff,

        batch_size: usize,
        sn: u32,
        resolution: Resolution,
        lease: Duration,
        fragment_markers: bool,
    ) -> Self {
        Self {
            buff,
            cursor: 2,
            batch_size,
            sn: [sn; 2],
            resolution,
            last_frame: None,
            fragments: None,
            fragment_markers,
            lease,
            state: State::Opened,
        }
    }

    pub(crate) fn into_inner(self) -> Buff {
        self.buff
    }

    pub fn sync(&mut self, rx: Option<&TransportRx<Buff>>, now: Duration) {
        if let Some(rx) = rx
            && rx.closed()
        {
            self.state = State::Closed;
            return;
        }

        if self.should_close(now) {
            self.state = State::Closed;
        }

        if matches!(self.state, State::Used | State::Opened) {
            self.state = State::Synchronized { last_sent: now };
        };
    }

    pub fn next_timeout(&self) -> Duration {
        match self.state {
            State::Opened | State::Closed | State::Used => Duration::from_secs(0),
            State::Synchronized { last_sent } => last_sent + self.lease / 4,
        }
    }

    pub fn should_send_keepalive(&self, now: Duration) -> bool {
        match self.state {
            State::Opened | State::Closed | State::Used => false,
            State::Synchronized { last_sent } => now >= last_sent + self.lease / 4,
        }
    }

    pub fn should_close(&self, now: Duration) -> bool {
        match self.state {
            State::Opened | State::Closed | State::Used => false,
            State::Synchronized { last_sent } => now >= last_sent + self.lease,
        }
    }

    pub fn closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }
}

impl<Buff> ZTransportTx for TransportTx<Buff>
where
    Buff: AsMut<[u8]> + AsRef<[u8]>,
{
    fn keepalive(&mut self) -> Result<(), TransportError> {
        self.transport(TransportMessageRef::KeepAlive(&KeepAlive))
    }

    fn close(&mut self) -> Result<(), TransportError> {
        // Closing abandons unsent bytes; a partial network message is never
        // completed by a later sender on this transport.
        self.clear();
        let result = self.transport(TransportMessageRef::Close(&Close::default()));
        self.state = State::Closed;
        result
    }

    fn transport(&mut self, msg: TransportMessageRef<'_>) -> Result<(), TransportError> {
        if self.closed() || self.fragments.is_some() {
            return Err(TransportError::InvalidState);
        }
        let end = self
            .cursor
            .checked_add(msg.z_len())
            .filter(|end| *end <= self.batch_size && *end <= self.buff.as_ref().len())
            .ok_or(TransportError::TransportTxFull)?;
        msg.z_encode(&mut &mut self.buff.as_mut()[self.cursor..end])?;
        self.cursor = end;
        self.last_frame = None;
        self.state = State::Used;
        Ok(())
    }

    fn encode(
        &mut self,
        msg: NetworkMessageRef<'_>,
        bytes: Option<&[u8]>,
    ) -> Result<(), TransportError> {
        if self.closed() || self.fragments.is_some() {
            return Err(TransportError::InvalidState);
        }
        let channel = channel_index(msg.reliability);
        let mask = self.resolution.get(Field::FrameSN).transport_sn_mask();
        let header = FrameHeader {
            reliability: msg.reliability,
            sn: self.sn[channel],
            qos: msg.qos,
        };
        let needs_header = self
            .last_frame
            .is_none_or(|last| last.reliability != header.reliability || last.qos != header.qos);
        let body_len = bytes.map_or_else(|| msg.body.z_len(), |bytes| bytes.len());
        let header_len = if needs_header { header.z_len() } else { 0 };
        let end = self
            .cursor
            .checked_add(header_len)
            .and_then(|end| end.checked_add(body_len))
            .ok_or(TransportError::TransportTxFull)?;
        if end <= self.batch_size && end <= self.buff.as_ref().len() {
            let mut writer = &mut self.buff.as_mut()[self.cursor..end];
            if needs_header {
                header.z_encode(&mut writer)?;
            }
            if let Some(bytes) = bytes {
                writer
                    .write_exact(bytes)
                    .map_err(zenoh_proto::CodecError::from)?;
            } else {
                msg.body.z_encode(&mut writer)?;
            }
            // Commit the frame and its sequence only after the entire message
            // fits. Failed encoding leaves the preceding batch intact.
            self.cursor = end;
            if needs_header {
                self.sn[channel] = self.sn[channel].wrapping_add(1) & mask;
                self.last_frame = Some(header);
            }
        } else {
            if self.cursor != 2 {
                return Err(TransportError::TransportTxFull);
            }
            let mut header = Fragment {
                reliability: msg.reliability,
                sn: mask,
                qos: msg.qos,
                first: self.fragment_markers.then(Default::default),
                ..Default::default()
            };
            // Reserve the largest header in this sequence space. Later headers
            // reuse already-consumed body bytes, so fragmentation needs neither
            // a second message buffer nor repeated serialization of the body.
            let start = 2 + header.z_len();
            let end = start
                .checked_add(body_len)
                .filter(|end| *end <= self.buff.as_ref().len() && start < self.batch_size)
                .ok_or(TransportError::TransportTxFull)?;
            let mut writer = &mut self.buff.as_mut()[start..end];
            if let Some(bytes) = bytes {
                writer
                    .write_exact(bytes)
                    .map_err(zenoh_proto::CodecError::from)?;
            } else {
                msg.body.z_encode(&mut writer)?;
            }
            header.sn = self.sn[channel];
            self.fragments = Some(Fragmented {
                remaining: start..end,
                header,
            });
        }
        self.state = State::Used;
        Ok(())
    }

    fn flush(&mut self, prefixed: bool) -> Result<Option<&[u8]>, TransportError> {
        if let Some(fragment) = &mut self.fragments {
            let channel = channel_index(fragment.header.reliability);
            fragment.header.sn = self.sn[channel];
            let header_len = fragment.header.z_len();
            let start = fragment.remaining.start - header_len;
            let count = fragment
                .remaining
                .len()
                .min(self.batch_size - 2 - header_len);
            let end = fragment.remaining.start + count;
            fragment.header.more = end < fragment.remaining.end;
            // The returned borrow prevents another flush from overwriting this
            // fragment until the link has finished using its bytes.
            fragment
                .header
                .z_encode(&mut &mut self.buff.as_mut()[start..fragment.remaining.start])?;
            self.buff.as_mut()[start - 2..start]
                .copy_from_slice(&((end - start) as u16).to_le_bytes());
            self.sn[channel] = self.sn[channel].wrapping_add(1)
                & self.resolution.get(Field::FrameSN).transport_sn_mask();
            fragment.remaining.start = end;
            fragment.header.first = None;
            if fragment.remaining.is_empty() {
                self.fragments = None;
            }
            return Ok(Some(
                &self.buff.as_ref()[if prefixed { start - 2 } else { start }..end],
            ));
        }
        if self.cursor == 2 {
            return Ok(None);
        }
        let end = self.cursor;
        self.buff.as_mut()[..2].copy_from_slice(&((end - 2) as u16).to_le_bytes());
        self.clear();
        Ok(Some(&self.buff.as_ref()[if prefixed { 0 } else { 2 }..end]))
    }

    fn clear(&mut self) {
        self.cursor = 2;
        self.last_frame = None;
        self.fragments = None;
    }
}
