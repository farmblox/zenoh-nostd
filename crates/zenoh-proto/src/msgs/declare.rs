use crate::{exts::*, fields::*, *};

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_|I|ID:5=0x1e")]
pub struct Declare<'a> {
    #[zenoh(presence = header(I))]
    pub id: Option<u32>,

    #[zenoh(ext = 0x1, default = QoS::default())]
    pub qos: QoS,
    #[zenoh(ext = 0x2)]
    pub timestamp: Option<Timestamp>,
    #[zenoh(ext = 0x3, default = NodeId::default(), mandatory)]
    pub nodeid: NodeId,

    pub body: DeclareBody<'a>,
}

#[derive(ZEnum, Debug, PartialEq)]
pub enum DeclareBody<'a> {
    DeclareKeyExpr(DeclareKeyExpr<'a>),
    UndeclareKeyExpr(UndeclareKeyExpr),
    DeclareSubscriber(DeclareSubscriber<'a>),
    UndeclareSubscriber(UndeclareSubscriber<'a>),
    DeclareQueryable(DeclareQueryable<'a>),
    UndeclareQueryable(UndeclareQueryable<'a>),
    DeclareToken(DeclareToken<'a>),
    UndeclareToken(UndeclareToken<'a>),
    DeclareFinal(DeclareFinal),
}

impl Default for DeclareBody<'_> {
    fn default() -> Self {
        DeclareBody::DeclareFinal(DeclareFinal::default())
    }
}

/// The key expression an undeclaration names, as extension `0x0f`.
///
/// Not a bare [`WireExpr`], and the difference is what makes an undeclaration
/// readable. A `WireExpr` reads its `N` and `M` flags from the header of the
/// message carrying it, and length-prefixes its suffix. An extension has no
/// header of its own, so this shape writes those two flags as the extension's
/// first byte and lets the suffix run to the end of the extension, which the
/// extension's own length already delimits.
///
/// Decoding it as a plain `WireExpr` yields a key expression shifted by one
/// byte and truncated — a key that matches nothing, delivered as if it were
/// real.
#[derive(ZExt, Debug, PartialEq, Default)]
#[zenoh(header = "_:6|M|N")]
pub struct UndeclaredKeyExpr<'a> {
    pub scope: u16,
    #[zenoh(header = M)]
    pub mapping: Mapping,
    #[zenoh(presence = header(N), default = "", size = remain)]
    pub suffix: &'a str,
}

impl<'a> UndeclaredKeyExpr<'a> {
    /// The same expression in the shape the rest of the API speaks.
    pub fn as_wire_expr(&self) -> WireExpr<'a> {
        WireExpr {
            scope: self.scope,
            mapping: self.mapping,
            suffix: self.suffix,
        }
    }
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "_|M|N|ID:5=0x00")]
pub struct DeclareKeyExpr<'a> {
    pub id: u16,
    #[zenoh(flatten, shift = 5)]
    pub wire_expr: WireExpr<'a>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "_:3|ID:5=0x01")]
pub struct UndeclareKeyExpr {
    pub id: u16,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "_|M|N|ID:5=0x02")]
pub struct DeclareSubscriber<'a> {
    pub id: u32,
    #[zenoh(flatten, shift = 5)]
    pub wire_expr: WireExpr<'a>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_:2|ID:5=0x03")]
pub struct UndeclareSubscriber<'a> {
    pub id: u32,
    #[zenoh(ext = 0x0f)]
    pub wire_expr: Option<UndeclaredKeyExpr<'a>>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|M|N|ID:5=0x04")]
pub struct DeclareQueryable<'a> {
    pub id: u32,
    #[zenoh(flatten, shift = 5)]
    pub wire_expr: WireExpr<'a>,

    #[zenoh(ext = 0x01, default = QueryableInfo::default())]
    pub qinfo: QueryableInfo,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_:2|ID:5=0x05")]
pub struct UndeclareQueryable<'a> {
    pub id: u32,
    #[zenoh(ext = 0x0f)]
    pub wire_expr: Option<UndeclaredKeyExpr<'a>>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|M|N|ID:5=0x06")]
pub struct DeclareToken<'a> {
    pub id: u32,
    #[zenoh(flatten, shift = 5)]
    pub wire_expr: WireExpr<'a>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_:2|ID:5=0x07")]
pub struct UndeclareToken<'a> {
    pub id: u32,
    #[zenoh(ext = 0x0f)]
    pub wire_expr: Option<UndeclaredKeyExpr<'a>>,
}

#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_:2|ID:5=0x1A")]
pub struct DeclareFinal {}
