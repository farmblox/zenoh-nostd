use crate::{exts::*, fields::*, *};

#[derive(ZStruct, Clone, Copy, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_|R|ID:5=0x05")]
pub struct FrameHeader {
    #[zenoh(header = R)]
    pub reliability: Reliability,
    pub sn: u32,

    #[zenoh(ext = 0x1, default = QoS::default())]
    pub qos: QoS,
}

/// One transport fragment ending at the batch boundary.
#[derive(ZStruct, Debug, PartialEq)]
#[zenoh(header = "Z|M|R|ID:5=0x06")]
pub struct Fragment<'a> {
    #[zenoh(header = R)]
    pub reliability: Reliability,
    #[zenoh(header = M)]
    pub more: bool,
    pub sn: u32,
    #[zenoh(ext = 0x1, mandatory, default = QoS::default())]
    pub qos: QoS,
    #[zenoh(ext = 0x2)]
    pub first: Option<FragmentFirst>,
    #[zenoh(ext = 0x3)]
    pub drop: Option<FragmentDrop>,
    #[zenoh(size = remain)]
    pub payload: &'a [u8],
}

#[derive(ZExt, Debug, PartialEq)]
pub struct FragmentFirst {}

#[derive(ZExt, Debug, PartialEq)]
pub struct FragmentDrop {}
