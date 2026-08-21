use core::time::Duration;

use embassy_time::with_timeout;
use zenoh_proto::{
    Endpoint, TransportLinkError,
    fields::{Resolution, ZenohIdProto},
};
use zenoh_sansio::{Transport, ZTransportRx, ZTransportTx};

use crate::io::link::EmbeddedIOLink;

use super::link::{ZLink, ZLinkInfo, ZLinkManager, ZLinkRx, ZLinkTx};

mod rx;
mod traits;
mod tx;

pub use rx::*;
pub use traits::*;
pub use tx::*;

pub struct TransportLink<Link, Buff> {
    link: Link,
    transport: Transport<Buff>,
}

impl<Link, Buff> TransportLink<Link, Buff> {
    pub fn new(link: Link, transport: Transport<Buff>) -> Self {
        Self { link, transport }
    }

    pub fn split(
        &mut self,
    ) -> (
        TransportLinkTx<'_, Link::Tx<'_>, Buff>,
        TransportLinkRx<'_, Link::Rx<'_>, Buff>,
    )
    where
        Link: ZLink,
    {
        let (link_tx, link_rx) = self.link.split();
        let (transport_tx, transport_rx) = self.transport.split();

        (
            TransportLinkTx::new(link_tx, transport_tx),
            TransportLinkRx::new(link_rx, transport_rx),
        )
    }

    pub fn transport(&self) -> &Transport<Buff> {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut Transport<Buff> {
        &mut self.transport
    }
}

impl<Link, Buff> ZTransportLinkTx for TransportLink<Link, Buff>
where
    Link: ZLinkTx,
    Buff: AsMut<[u8]> + AsRef<[u8]>,
{
    fn tx(&mut self) -> (&mut impl ZLinkTx, &mut impl ZTransportTx) {
        (&mut self.link, &mut self.transport.tx)
    }
}

impl<Link, Buff> ZTransportLinkRx for TransportLink<Link, Buff>
where
    Link: ZLinkRx,
    Buff: AsMut<[u8]> + AsRef<[u8]>,
{
    fn rx(&mut self) -> (&mut impl ZLinkRx, &mut impl ZTransportRx) {
        (&mut self.link, &mut self.transport.rx)
    }
}

pub struct TransportLinkManager<LinkManager> {
    link_manager: LinkManager,

    open_timeout: Duration,
    zid: ZenohIdProto,
    lease: Duration,
    resolution: Resolution,
}

impl<LinkManager> From<LinkManager> for TransportLinkManager<LinkManager> {
    fn from(value: LinkManager) -> Self {
        Self::new(
            value,
            Duration::from_secs(10),
            ZenohIdProto::default(),
            Duration::from_secs(10),
            Resolution::default(),
        )
    }
}

impl<LinkManager> TransportLinkManager<LinkManager> {
    pub(crate) fn new(
        link_manager: LinkManager,
        open_timeout: Duration,
        zid: ZenohIdProto,
        lease: Duration,
        resolution: Resolution,
    ) -> Self {
        Self {
            link_manager,
            open_timeout,
            zid,
            lease,
            resolution,
        }
    }

    /// Set the transport lease advertised when this manager opens a session.
    ///
    /// Applications that retain an emit-only session without running its
    /// receive loop must keep that session's complete useful lifetime inside
    /// this lease. A zero lease is invalid because it would make a newly
    /// opened transport immediately stale.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        assert!(!lease.is_zero(), "a Zenoh transport lease must be non-zero");
        self.lease = lease;
        self
    }

    pub async fn bridge_connect<Tx: embedded_io_async::Write, Rx: embedded_io_async::Read, Buff>(
        &self,
        mut link: EmbeddedIOLink<Tx, Rx>,
        buff: Buff,
    ) -> core::result::Result<TransportLink<EmbeddedIOLink<Tx, Rx>, Buff>, TransportLinkError>
    where
        LinkManager: ZLinkManager,
        Buff: AsMut<[u8]> + AsRef<[u8]> + Clone,
    {
        let connect = async || {
            let streamed = link.is_streamed();
            Transport::builder(buff)
                .with_zid(self.zid)
                .with_lease(self.lease)
                .with_resolution(self.resolution)
                .connect_async(
                    &mut link,
                    async |link, bytes| {
                        if link.is_streamed() {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    async |link, bytes| link.write_all(bytes).await,
                )
                .with_prefixed(streamed)
                .finish_async()
                .await
        };

        let transport = with_timeout(self.open_timeout.try_into().unwrap(), connect())
            .await
            .map_err(|_| TransportLinkError::OpenTimeout)?
            .map_err(|e| e.flatten_map::<TransportLinkError>())?;

        Ok(TransportLink::new(link, transport))
    }

    pub async fn bridge_listen<Tx: embedded_io_async::Write, Rx: embedded_io_async::Read, Buff>(
        &self,
        mut link: EmbeddedIOLink<Tx, Rx>,
        buff: Buff,
    ) -> core::result::Result<TransportLink<EmbeddedIOLink<Tx, Rx>, Buff>, TransportLinkError>
    where
        LinkManager: ZLinkManager,
        Buff: AsMut<[u8]> + AsRef<[u8]> + Clone,
    {
        let connect = async || {
            let streamed = link.is_streamed();
            Transport::builder(buff)
                .with_zid(self.zid)
                .with_lease(self.lease)
                .with_resolution(self.resolution)
                .listen_async(
                    &mut link,
                    async |link, bytes| {
                        if link.is_streamed() {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    async |link, bytes| link.write_all(bytes).await,
                )
                .with_prefixed(streamed)
                .finish_async()
                .await
        };

        let transport = with_timeout(self.open_timeout.try_into().unwrap(), connect())
            .await
            .map_err(|_| TransportLinkError::OpenTimeout)?
            .map_err(|e| e.flatten_map::<TransportLinkError>())?;

        Ok(TransportLink::new(link, transport))
    }

    pub async fn connect<Buff>(
        &self,
        endpoint: Endpoint<'_>,
        buff: Buff,
    ) -> core::result::Result<TransportLink<LinkManager::Link<'_>, Buff>, TransportLinkError>
    where
        LinkManager: ZLinkManager,
        Buff: AsMut<[u8]> + AsRef<[u8]> + Clone,
    {
        // Match the reference implementation's open boundary: resolving the
        // endpoint and establishing the link are part of opening a transport,
        // not work that may wait forever before the open timeout begins. This
        // matters for browser WebSockets, whose connection future can remain
        // pending after the browser reports a failed network attempt.
        let connect = async {
            let mut link = self.link_manager.connect(endpoint).await?;
            let streamed = link.is_streamed();
            let transport = Transport::builder(buff)
                .with_zid(self.zid)
                .with_lease(self.lease)
                .with_resolution(self.resolution)
                .connect_async(
                    &mut link,
                    async |link, bytes| {
                        if link.is_streamed() {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    async |link, bytes| link.write_all(bytes).await,
                )
                .with_prefixed(streamed)
                .finish_async()
                .await
                .map_err(|e| e.flatten_map::<TransportLinkError>())?;

            Ok(TransportLink::new(link, transport))
        };

        with_timeout(self.open_timeout.try_into().unwrap(), connect)
            .await
            .map_err(|_| TransportLinkError::OpenTimeout)?
    }

    pub async fn listen<Buff>(
        &self,
        endpoint: Endpoint<'_>,
        buff: Buff,
    ) -> core::result::Result<TransportLink<LinkManager::Link<'_>, Buff>, TransportLinkError>
    where
        LinkManager: ZLinkManager,
        Buff: AsMut<[u8]> + AsRef<[u8]> + Clone,
    {
        // Listening has the same contract as connecting: the deadline covers
        // both acquiring the link and completing the Zenoh handshake.
        let listen = async {
            let mut link = self.link_manager.listen(endpoint).await?;
            let streamed = link.is_streamed();
            let transport = Transport::builder(buff)
                .with_zid(self.zid)
                .with_lease(self.lease)
                .with_resolution(self.resolution)
                .listen_async(
                    &mut link,
                    async |link, bytes| {
                        if link.is_streamed() {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    async |link, bytes| link.write_all(bytes).await,
                )
                .with_prefixed(streamed)
                .finish_async()
                .await
                .map_err(|e| e.flatten_map::<TransportLinkError>())?;

            Ok(TransportLink::new(link, transport))
        };

        with_timeout(self.open_timeout.try_into().unwrap(), listen)
            .await
            .map_err(|_| TransportLinkError::OpenTimeout)?
    }
}
