use crate::{
    api::{
        arg::{GetResponseRef, QueryableQueryRef, SampleRef},
        callbacks::ZCallbacks,
        session::declarations::ZDeclarations,
    },
    io::{link::ZLinkManager, transport::TransportLinkManager},
};
use zenoh_proto::nonwild_keyexpr;

pub trait ZSessionConfig: Sized {
    type Buff: AsMut<[u8]> + AsRef<[u8]> + Clone;
    type LinkManager: ZLinkManager;

    type SubCallbacks<'res>: ZCallbacks<'res, SampleRef>;
    type GetCallbacks<'res>: ZCallbacks<'res, GetResponseRef>;
    type QueryableCallbacks<'s, 'res>: ZCallbacks<'s, QueryableQueryRef<'s, 'res, Self>>
    where
        Self: 'res,
        'res: 's;
    type Declarations: ZDeclarations;

    fn transports(&self) -> &TransportLinkManager<Self::LinkManager>;
    fn buff(&self) -> Self::Buff;

    /// Optional non-wild key-expression prepended to every session operation.
    ///
    /// This matches mainline Zenoh's session namespace: application callbacks
    /// see relative keys, while every declaration, query, reply, and publish on
    /// the transport carries the namespace. Implementations should return a
    /// stable value that lives as long as the configuration.
    fn namespace(&self) -> Option<&nonwild_keyexpr> {
        None
    }
}

#[allow(dead_code)]
pub trait ZBrokerConfig {
    type Buff: AsMut<[u8]> + AsRef<[u8]> + Clone;
    type LinkManager: ZLinkManager;

    fn transports(&self) -> &TransportLinkManager<Self::LinkManager>;
    fn buff(&self) -> Self::Buff;
}
