//! Replaceable transport storage for allocator-backed sessions.
//!
//! The wire driver borrows the transport halves it drives. Allocator-free
//! sessions keep that transport in caller-owned resource storage.
//! An allocator-backed session instead pins each transport in a box, allowing
//! the stable API session to drop a failed transport and install a replacement
//! without leaking either one.

use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, Ordering};

use embassy_sync::{blocking_mutex::raw::NoopRawMutex, mutex::Mutex, signal::Signal};
use zenoh_proto::{EitherError, TransportLinkError, msgs::NetworkMessage};

use crate::{
    config::ZSessionConfig,
    io::{
        driver::{OwnedDriver, RunControl},
        transport::TransportLink,
    },
    platform::ZLinkManager,
};

type Link<'res, Config> = <<Config as ZSessionConfig>::LinkManager as ZLinkManager>::Link<'res>;
type CurrentDriver<'res, Config> =
    Arc<OwnedDriver<'res, Link<'res, Config>, <Config as ZSessionConfig>::Buff>>;

/// The transport slot behind one stable allocator-backed Zenoh session.
pub(crate) struct ManagedDriver<'res, Config>
where
    Config: ZSessionConfig + 'res,
{
    current: Mutex<NoopRawMutex, Option<CurrentDriver<'res, Config>>>,
    closed: AtomicBool,
    stopped: AtomicBool,
    stop: Signal<NoopRawMutex, ()>,
}

impl<'res, Config> ManagedDriver<'res, Config>
where
    Config: ZSessionConfig + 'res,
{
    pub(crate) fn new(transport: TransportLink<Link<'res, Config>, Config::Buff>) -> Self {
        Self {
            current: Mutex::new(Some(Arc::new(OwnedDriver::new(transport)))),
            closed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            stop: Signal::new(),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn begin_close(&self) -> bool {
        let first = !self.stopped.swap(true, Ordering::AcqRel);
        self.closed.store(true, Ordering::Release);
        self.stop.signal(());
        first
    }

    pub(crate) fn should_stop(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_stop(&self) {
        self.stop.wait().await;
    }

    pub(crate) async fn install(&self, transport: TransportLink<Link<'res, Config>, Config::Buff>) {
        let mut current = self.current.lock().await;
        if self.should_stop() {
            return;
        }
        *current = Some(Arc::new(OwnedDriver::new(transport)));
        self.closed.store(false, Ordering::Release);
    }

    pub(crate) async fn reopen(
        &self,
        transport: TransportLink<Link<'res, Config>, Config::Buff>,
    ) -> core::result::Result<(), TransportLinkError> {
        let mut current = self.current.lock().await;
        if !self.should_stop() {
            return Err(TransportLinkError::TransportClosed);
        }
        self.stop.reset();
        *current = Some(Arc::new(OwnedDriver::new(transport)));
        self.stopped.store(false, Ordering::Release);
        self.closed.store(false, Ordering::Release);
        Ok(())
    }

    async fn driver(
        &self,
    ) -> core::result::Result<CurrentDriver<'res, Config>, TransportLinkError> {
        if self.is_closed() {
            return Err(TransportLinkError::TransportClosed);
        }
        self.current
            .lock()
            .await
            .clone()
            .ok_or(TransportLinkError::TransportClosed)
    }

    pub(crate) async fn send<'a>(
        &self,
        messages: impl Iterator<Item = NetworkMessage<'a>>,
    ) -> core::result::Result<(), TransportLinkError> {
        self.driver().await?.send(messages).await
    }

    pub(crate) async fn run<State, E, Update, Control>(
        &self,
        state: &Mutex<NoopRawMutex, State>,
        update: Update,
    ) -> core::result::Result<(), EitherError<TransportLinkError, E>>
    where
        Config::Buff: AsMut<[u8]> + AsRef<[u8]>,
        Update: for<'any> AsyncFnMut(
            zenoh_proto::fields::ZenohIdProto,
            &mut State,
            NetworkMessage<'any>,
            &'any [u8],
        ) -> core::result::Result<Control, E>,
        Control: RunControl,
    {
        self.driver()
            .await
            .map_err(EitherError::A)?
            .run(state, update)
            .await
    }

    pub(crate) async fn finish_close(&self) -> core::result::Result<(), TransportLinkError> {
        let current = self.current.lock().await.take();
        let Some(driver) = current else {
            return Ok(());
        };
        driver.begin_close();
        driver.finish_close().await
    }

    /// Drop a failed transport while retaining the API session's declarations.
    pub(crate) async fn discard_transport(&self) {
        self.closed.store(true, Ordering::Release);
        self.current.lock().await.take();
    }
}
