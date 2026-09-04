use super::sample::*;
use zenoh_proto::{CollectionError, keyexpr};

/// One thing a query hears back.
///
/// [`GetResponse::Final`] is not a reply — it is the peer saying there will be
/// no more of them. A caller that never sees it has no way to tell "still
/// arriving" from "done", and is left waiting out a timeout to find out.
#[derive(Debug)]
pub enum GetResponse<'a> {
    Ok(Sample<'a>),
    Err(Sample<'a>),
    Final,
}

impl<'a> GetResponse<'a> {
    pub fn ok(ke: &'a keyexpr, payload: &'a [u8]) -> Self {
        Self::Ok(Sample::new(ke, payload))
    }

    pub fn err(ke: &'a keyexpr, payload: &'a [u8]) -> Self {
        Self::Err(Sample::new(ke, payload))
    }
}

#[derive(Debug)]
pub enum FixedCapacityGetResponse<
    const MAX_KEYEXPR: usize,
    const MAX_PAYLOAD: usize,
    const MAX_ATTACHMENT: usize,
> {
    Ok(FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>),
    Err(FixedCapacitySample<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>),
    Final,
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize, const MAX_ATTACHMENT: usize>
    FixedCapacityGetResponse<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>
{
    pub fn as_ref(&self) -> GetResponse<'_> {
        match self {
            Self::Ok(sample) => GetResponse::Ok(sample.as_ref()),
            Self::Err(sample) => GetResponse::Err(sample.as_ref()),
            Self::Final => GetResponse::Final,
        }
    }
}

impl<const MAX_KEYEXPR: usize, const MAX_PAYLOAD: usize, const MAX_ATTACHMENT: usize>
    TryFrom<&GetResponse<'_>>
    for FixedCapacityGetResponse<MAX_KEYEXPR, MAX_PAYLOAD, MAX_ATTACHMENT>
{
    type Error = CollectionError;

    fn try_from(value: &GetResponse<'_>) -> Result<Self, Self::Error> {
        match value {
            GetResponse::Ok(sample) => Ok(Self::Ok(sample.try_into()?)),
            GetResponse::Err(sample) => Ok(Self::Err(sample.try_into()?)),
            GetResponse::Final => Ok(Self::Final),
        }
    }
}

#[cfg(feature = "alloc")]
#[derive(Debug)]
pub enum AllocGetResponse {
    Ok(AllocSample),
    Err(AllocSample),
    Final,
}

#[cfg(feature = "alloc")]
impl AllocGetResponse {
    pub fn as_ref(&self) -> GetResponse<'_> {
        match self {
            Self::Ok(sample) => GetResponse::Ok(sample.as_ref()),
            Self::Err(sample) => GetResponse::Err(sample.as_ref()),
            Self::Final => GetResponse::Final,
        }
    }
}

#[cfg(feature = "alloc")]
impl TryFrom<&GetResponse<'_>> for AllocGetResponse {
    type Error = CollectionError;

    fn try_from(value: &GetResponse<'_>) -> Result<Self, Self::Error> {
        match value {
            GetResponse::Ok(sample) => Ok(Self::Ok(sample.try_into()?)),
            GetResponse::Err(sample) => Ok(Self::Err(sample.try_into()?)),
            GetResponse::Final => Ok(Self::Final),
        }
    }
}
