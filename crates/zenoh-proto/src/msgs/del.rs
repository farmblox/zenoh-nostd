//! Zenoh Delete value body.
//!
//! This is the 1.10 wire shape implemented by mainline Zenoh: message id 2,
//! optional timestamp, SourceInfo extension 1, and attachment extension 2.
//! Delete carries no value payload.

use crate::{exts::*, fields::*, *};

/// Remove a resource while preserving its optional timestamp and metadata.
///
/// Both live publications and successful query replies can carry this body.
/// Unlike Put, Delete has no encoding or value bytes after its extensions.
#[derive(ZStruct, Debug, PartialEq, Default)]
#[zenoh(header = "Z|_|T|ID:5=0x2")]
pub struct Del<'a> {
    /// Optional Hybrid Logical Clock timestamp carried by the sender.
    #[zenoh(presence = header(T))]
    pub timestamp: Option<Timestamp>,

    /// Optional identity and sequence metadata for the original source.
    #[zenoh(ext = 0x1)]
    pub sinfo: Option<SourceInfo>,
    /// Opaque application metadata preserved by Zenoh.
    #[zenoh(ext = 0x2)]
    pub attachment: Option<Attachment<'a>>,
}
