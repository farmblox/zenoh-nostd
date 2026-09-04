use core::str::FromStr;

use zenoh_proto::{CollectionError, exts::EntityGlobalId, keyexpr};

/// Whether a sample says a value exists, or that it is gone.
///
/// The distinction is load-bearing for liveliness: a token declaration and a
/// token undeclaration are delivered through the same callbacks, and without
/// this a receiver cannot tell "a peer appeared" from "a peer died".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SampleKind {
    /// A value was published, or a token declared.
    #[default]
    Put,
    /// A value was deleted, or a token undeclared.
    Delete,
}

#[derive(Debug)]
pub struct Sample<'a> {
    ke: &'a keyexpr,
    payload: &'a [u8],
    attachment: Option<&'a [u8]>,
    responder: Option<EntityGlobalId>,
    kind: SampleKind,
}

impl<'a> Sample<'a> {
    /// A sample carrying a value.
    pub fn new(ke: &'a keyexpr, payload: &'a [u8]) -> Self {
        Self {
            ke,
            payload,
            attachment: None,
            responder: None,
            kind: SampleKind::Put,
        }
    }

    /// A sample carrying a value and Zenoh's opaque attachment bytes.
    pub(crate) fn with_metadata(
        ke: &'a keyexpr,
        payload: &'a [u8],
        attachment: Option<&'a [u8]>,
        responder: Option<EntityGlobalId>,
    ) -> Self {
        Self {
            ke,
            payload,
            attachment,
            responder,
            kind: SampleKind::Put,
        }
    }

    /// A sample saying the value at `ke` is gone. Carries no payload.
    pub fn delete(ke: &'a keyexpr) -> Self {
        Self {
            ke,
            payload: &[],
            attachment: None,
            responder: None,
            kind: SampleKind::Delete,
        }
    }

    pub fn keyexpr(&self) -> &keyexpr {
        self.ke
    }

    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Opaque metadata attached to this publication or successful reply.
    pub fn attachment(&self) -> Option<&[u8]> {
        self.attachment
    }

    /// Zenoh entity that answered a query. Publications carry `None`.
    pub fn responder(&self) -> Option<EntityGlobalId> {
        self.responder
    }

    pub fn kind(&self) -> SampleKind {
        self.kind
    }
}

#[derive(Debug)]
pub struct FixedCapacitySample<
    const MAX_KEYEXPR: usize,
    const MAX_PAYLOAD: usize,
    const MAX_ATTACHMENT: usize,
> {
    ke: heapless::String<MAX_KEYEXPR>,
    payload: heapless::Vec<u8, MAX_PAYLOAD>,
    attachment: Option<heapless::Vec<u8, MAX_ATTACHMENT>>,
    responder: Option<EntityGlobalId>,
    kind: SampleKind,
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize, const MAX_ATTACHMENT: usize>
    FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>
{
    pub fn keyexpr(&self) -> &keyexpr {
        keyexpr::from_str_unchecked(self.ke.as_str())
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn attachment(&self) -> Option<&[u8]> {
        self.attachment.as_deref()
    }

    pub fn responder(&self) -> Option<EntityGlobalId> {
        self.responder
    }

    pub fn kind(&self) -> SampleKind {
        self.kind
    }

    pub fn as_ref(&self) -> Sample<'_> {
        Sample {
            ke: self.keyexpr(),
            payload: self.payload(),
            attachment: self.attachment(),
            responder: self.responder(),
            kind: self.kind,
        }
    }
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize, const MAX_ATTACHMENT: usize>
    TryFrom<&Sample<'_>> for FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>
{
    type Error = CollectionError;

    fn try_from(value: &Sample<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            ke: heapless::String::from_str(value.keyexpr().as_str())
                .map_err(|_| CollectionError::CollectionTooSmall)?,
            payload: heapless::Vec::from_slice(value.payload())
                .map_err(|_| CollectionError::CollectionTooSmall)?,
            attachment: value
                .attachment()
                .map(heapless::Vec::from_slice)
                .transpose()
                .map_err(|_| CollectionError::CollectionTooSmall)?,
            responder: value.responder(),
            kind: value.kind(),
        })
    }
}

#[cfg(feature = "alloc")]
#[derive(Debug)]
pub struct AllocSample {
    ke: alloc::string::String,
    payload: alloc::vec::Vec<u8>,
    attachment: Option<alloc::vec::Vec<u8>>,
    responder: Option<EntityGlobalId>,
    kind: SampleKind,
}

#[cfg(feature = "alloc")]
impl AllocSample {
    pub fn keyexpr(&self) -> &keyexpr {
        keyexpr::from_str_unchecked(self.ke.as_str())
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn attachment(&self) -> Option<&[u8]> {
        self.attachment.as_deref()
    }

    pub fn responder(&self) -> Option<EntityGlobalId> {
        self.responder
    }

    pub fn kind(&self) -> SampleKind {
        self.kind
    }

    pub fn as_ref(&self) -> Sample<'_> {
        Sample {
            ke: self.keyexpr(),
            payload: self.payload(),
            attachment: self.attachment(),
            responder: self.responder(),
            kind: self.kind,
        }
    }
}

#[cfg(feature = "alloc")]
impl TryFrom<&Sample<'_>> for AllocSample {
    type Error = CollectionError;

    fn try_from(value: &Sample<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            ke: alloc::string::String::from(value.keyexpr().as_str()),
            payload: alloc::vec::Vec::from(value.payload()),
            attachment: value.attachment().map(alloc::vec::Vec::from),
            responder: value.responder(),
            kind: value.kind(),
        })
    }
}

#[cfg(test)]
#[path = "sample_tests.rs"]
mod tests;
