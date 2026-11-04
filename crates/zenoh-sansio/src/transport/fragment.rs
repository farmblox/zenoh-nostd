use alloc::vec::Vec;
use zenoh_proto::{
    fields::Reliability,
    msgs::{Fragment, FrameHeader},
};

/// Bound one network message independently of the negotiated batch size.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
struct Partial {
    bytes: Vec<u8>,
    next_sn: u32,
}

/// The transport negotiates one priority with independent reliability channels.
#[derive(Debug, Default)]
pub(super) struct Reassembly {
    channels: [Option<Partial>; 2],
    pub complete: Option<(FrameHeader, Vec<u8>)>,
}

impl Reassembly {
    pub fn discard(&mut self, reliability: Reliability) {
        self.channels[reliability as usize] = None;
    }

    /// Only consecutive fragments beginning with a start marker can be delivered.
    pub fn push(&mut self, fragment: Fragment<'_>, mask: u32) {
        let channel = &mut self.channels[fragment.reliability as usize];
        if fragment.first.is_some() {
            *channel = Some(Partial {
                bytes: Vec::new(),
                next_sn: fragment.sn,
            });
        }
        if fragment.drop.is_some() {
            *channel = None;
            return;
        }
        let Some(partial) = channel.as_mut() else {
            return;
        };
        if fragment.sn != partial.next_sn
            || fragment.payload.is_empty()
            || fragment.payload.len() > MAX_MESSAGE_BYTES - partial.bytes.len()
        {
            *channel = None;
            return;
        }
        // Grow geometrically within the limit so large messages avoid repeated copies.
        let needed = partial.bytes.len() + fragment.payload.len();
        if needed > partial.bytes.capacity() {
            let capacity = needed.next_power_of_two().min(MAX_MESSAGE_BYTES);
            if partial
                .bytes
                .try_reserve_exact(capacity - partial.bytes.len())
                .is_err()
            {
                *channel = None;
                return;
            }
        }
        partial.bytes.extend_from_slice(fragment.payload);
        partial.next_sn = fragment.sn.wrapping_add(1) & mask;
        if !fragment.more {
            let partial = channel
                .take()
                .expect("the final fragment has a partial message");
            self.complete = Some((
                FrameHeader {
                    reliability: fragment.reliability,
                    sn: fragment.sn,
                    qos: fragment.qos,
                },
                partial.bytes,
            ));
        }
    }
}
