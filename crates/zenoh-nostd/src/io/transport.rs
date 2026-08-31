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

/// Bounded exponential backoff for opening a transport.
///
/// The connector owns this policy for both the first link and replacement
/// links. `maximum_delay` caps the interval between attempts, not the total
/// time spent connecting. API users must not wrap it in another timer or
/// replay loop.
#[cfg(feature = "alloc")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionRetryPolicy {
    /// Delay after the first failed connection attempt.
    pub initial_delay: embassy_time::Duration,
    /// Upper bound for subsequent delays.
    pub maximum_delay: embassy_time::Duration,
    /// Multiplier applied after each delay.
    pub increase_factor: u32,
}

#[cfg(feature = "alloc")]
impl Default for ConnectionRetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay: embassy_time::Duration::from_millis(250),
            maximum_delay: embassy_time::Duration::from_secs(60),
            increase_factor: 2,
        }
    }
}

#[cfg(feature = "alloc")]
impl ConnectionRetryPolicy {
    pub(crate) fn next_delay(self, current: embassy_time::Duration) -> embassy_time::Duration {
        let next = current
            .as_millis()
            .saturating_mul(u64::from(self.increase_factor.max(1)));
        embassy_time::Duration::from_millis(next.min(self.maximum_delay.as_millis()))
    }
}

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

    /// Connect immediately, then retry failed attempts with `policy`.
    ///
    /// Dropping this future cancels the pending link attempt and its backoff.
    /// This gives an executor or application runtime one cancellation boundary
    /// without moving retry ownership out of Zenoh.
    ///
    /// The method returns only after a complete Zenoh transport handshake. A
    /// failed link acquisition, handshake, or open timeout is logged and fed
    /// through the same backoff; no request or declaration exists yet to replay.
    #[cfg(feature = "alloc")]
    pub async fn connect_retrying<Buff>(
        &self,
        endpoint: Endpoint<'_>,
        buff: Buff,
        policy: ConnectionRetryPolicy,
    ) -> TransportLink<LinkManager::Link<'_>, Buff>
    where
        LinkManager: ZLinkManager,
        Buff: AsMut<[u8]> + AsRef<[u8]> + Clone,
    {
        let mut delay = policy.initial_delay;
        loop {
            match self.connect(endpoint.clone(), buff.clone()).await {
                Ok(transport) => return transport,
                Err(error) => {
                    zenoh_proto::debug!("connect attempt failed: {}", error);
                    embassy_time::Timer::after(delay).await;
                    delay = policy.next_delay(delay);
                }
            }
        }
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
