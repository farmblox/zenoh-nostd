use core::str::FromStr;

use zenoh_proto::{CollectionError, keyexpr};

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
    kind: SampleKind,
}

impl<'a> Sample<'a> {
    /// A sample carrying a value.
    pub fn new(ke: &'a keyexpr, payload: &'a [u8]) -> Self {
        Self {
            ke,
            payload,
            kind: SampleKind::Put,
        }
    }

    /// A sample saying the value at `ke` is gone. Carries no payload.
    pub fn delete(ke: &'a keyexpr) -> Self {
        Self {
            ke,
            payload: &[],
            kind: SampleKind::Delete,
        }
    }

    pub fn keyexpr(&self) -> &keyexpr {
        self.ke
    }

    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    pub fn kind(&self) -> SampleKind {
        self.kind
    }
}

#[derive(Debug)]
pub struct FixedCapacitySample<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize> {
    ke: heapless::String<MAX_KEYEXPR>,
    payload: heapless::Vec<u8, MAX_PAYLOAD>,
    kind: SampleKind,
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize>
    FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD>
{
    pub fn keyexpr(&self) -> &keyexpr {
        keyexpr::from_str_unchecked(self.ke.as_str())
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn kind(&self) -> SampleKind {
        self.kind
    }

    pub fn as_ref(&self) -> Sample<'_> {
        Sample {
            ke: self.keyexpr(),
            payload: self.payload(),
            kind: self.kind,
        }
    }
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize> TryFrom<&Sample<'_>>
    for FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD>
{
    type Error = CollectionError;

    fn try_from(value: &Sample<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            ke: heapless::String::from_str(value.keyexpr().as_str())
                .map_err(|_| CollectionError::CollectionTooSmall)?,
            payload: heapless::Vec::from_slice(value.payload())
                .map_err(|_| CollectionError::CollectionTooSmall)?,
            kind: value.kind(),
        })
    }
}

#[cfg(feature = "alloc")]
#[derive(Debug)]
pub struct AllocSample {
    ke: alloc::string::String,
    payload: alloc::vec::Vec<u8>,
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

    pub fn kind(&self) -> SampleKind {
        self.kind
    }

    pub fn as_ref(&self) -> Sample<'_> {
        Sample {
            ke: self.keyexpr(),
            payload: self.payload(),
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
            kind: value.kind(),
        })
    }
}
