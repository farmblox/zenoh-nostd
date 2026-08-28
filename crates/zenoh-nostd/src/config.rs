use crate::{
    api::{
        arg::{GetResponseRef, QueryableQueryRef, SampleRef},
        callbacks::ZCallbacks,
        session::declarations::ZDeclarations,
    },
    io::{link::ZLinkManager, transport::TransportLinkManager},
};

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
}

#[allow(dead_code)]
pub trait ZBrokerConfig {
    type Buff: AsMut<[u8]> + AsRef<[u8]> + Clone;
    type LinkManager: ZLinkManager;

    fn transports(&self) -> &TransportLinkManager<Self::LinkManager>;
    fn buff(&self) -> Self::Buff;
}
