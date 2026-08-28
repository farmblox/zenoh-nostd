#[cfg(feature = "alloc")]
use core::marker::PhantomData;
use core::{
    ops::DerefMut,
    sync::atomic::{AtomicBool, Ordering},
};
use embassy_futures::select::{Either4, select4};
use embassy_sync::{
    blocking_mutex::raw::NoopRawMutex,
    mutex::{Mutex, MutexGuard},
    signal::Signal,
};
use embassy_time::{Duration, Instant, Timer};
use zenoh_proto::{EitherError, TransportLinkError, fields::ZenohIdProto, msgs::NetworkMessage};

use crate::{
    io::transport::{
        TransportLink, TransportLinkRx, TransportLinkTx, ZTransportLinkRx, ZTransportLinkTx,
    },
    platform::ZLink,
};

pub(crate) trait RunControl {
    fn should_stop(self) -> bool;
}

impl RunControl for () {
    fn should_stop(self) -> bool {
        false
    }
}

impl RunControl for bool {
    fn should_stop(self) -> bool {
        self
    }
}

pub struct Driver<'res, Link, Buff>
where
    Link: ZLink + 'res,
{
    zid: ZenohIdProto,
    tx: Mutex<NoopRawMutex, TransportLinkTx<'res, Link::Tx<'res>, Buff>>,
    rx: Mutex<NoopRawMutex, TransportLinkRx<'res, Link::Rx<'res>, Buff>>,
    closed: AtomicBool,
    shutdown: Signal<NoopRawMutex, ()>,
}

/// An allocator-backed driver that owns the stable transport it borrows.
///
/// This is used anywhere a transport must be replaced or stored in a
/// collection. The driver is destroyed before the pinned transport, so its
/// internal link-half references never dangle.
#[cfg(feature = "alloc")]
pub(crate) struct OwnedDriver<'res, Link, Buff>
where
    Link: ZLink + 'res,
{
    driver: core::mem::ManuallyDrop<Driver<'res, Link, Buff>>,
    transport: *mut TransportLink<Link, Buff>,
    _lifetime: PhantomData<&'res mut TransportLink<Link, Buff>>,
}

#[cfg(feature = "alloc")]
impl<'res, Link, Buff> OwnedDriver<'res, Link, Buff>
where
    Link: ZLink + 'res,
{
    pub(crate) fn new(transport: TransportLink<Link, Buff>) -> Self {
        let transport = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(transport));
        // SAFETY: the box is stable and this owner drops the driver before
        // reconstructing the box. The owner itself cannot outlive `'res`.
        let borrowed: &'res mut TransportLink<Link, Buff> =
            unsafe { core::mem::transmute(&mut *transport) };
        Self {
            driver: core::mem::ManuallyDrop::new(Driver::new(borrowed)),
            transport,
            _lifetime: PhantomData,
        }
    }
}

#[cfg(feature = "alloc")]
impl<'res, Link, Buff> core::ops::Deref for OwnedDriver<'res, Link, Buff>
where
    Link: ZLink + 'res,
{
    type Target = Driver<'res, Link, Buff>;

    fn deref(&self) -> &Self::Target {
        &self.driver
    }
}

#[cfg(feature = "alloc")]
impl<Link, Buff> Drop for OwnedDriver<'_, Link, Buff>
where
    Link: ZLink,
{
    fn drop(&mut self) {
        // SAFETY: `new` creates the unique owner and fixes this drop order.
        unsafe { core::mem::ManuallyDrop::drop(&mut self.driver) };
        unsafe { drop(alloc::boxed::Box::from_raw(self.transport)) };
    }
}

impl<'res, Link, Buff> Driver<'res, Link, Buff>
where
    Link: ZLink,
{
    pub fn new(transport: &'res mut TransportLink<Link, Buff>) -> Self {
        let zid = transport.transport().other_zid;

        let (tx, rx) = transport.split();

        Self {
            zid,
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
            closed: AtomicBool::new(false),
            shutdown: Signal::new(),
        }
    }

    #[allow(dead_code)]
    pub fn zid(&self) -> ZenohIdProto {
        self.zid
    }

    pub async fn tx(
        &self,
    ) -> core::result::Result<
        MutexGuard<'_, NoopRawMutex, TransportLinkTx<'res, Link::Tx<'res>, Buff>>,
        TransportLinkError,
    > {
        if self.is_closed() {
            return Err(TransportLinkError::TransportClosed);
        }
        let tx = self.tx.lock().await;
        if self.is_closed() {
            return Err(TransportLinkError::TransportClosed);
        }
        Ok(tx)
    }

    /// Send network messages through the current transport.
    pub(crate) async fn send<'a>(
        &self,
        messages: impl Iterator<Item = NetworkMessage<'a>>,
    ) -> core::result::Result<(), TransportLinkError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        self.tx().await?.send(messages).await
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Enter the closed state exactly once.
    pub(crate) fn begin_close(&self) -> bool {
        !self.closed.swap(true, Ordering::AcqRel)
    }

    /// Send Zenoh's transport Close and wake the receive loop immediately.
    pub(crate) async fn finish_close(&self) -> core::result::Result<(), TransportLinkError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        let result = self.tx.lock().await.close().await;
        self.shutdown.signal(());
        result
    }

    pub async fn run<State, E, Update, Control>(
        &self,
        state: &Mutex<NoopRawMutex, State>,
        mut update: Update,
    ) -> core::result::Result<(), EitherError<TransportLinkError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        Update: for<'any> AsyncFnMut(
            ZenohIdProto,
            &mut State,
            NetworkMessage<'any>,
            &'any [u8],
        ) -> core::result::Result<Control, E>,
        Control: RunControl,
    {
        if self.is_closed() {
            return Err(EitherError::A(TransportLinkError::TransportClosed));
        }

        let result = async {
            let mut rx = self.rx.lock().await;
            let start = Instant::now();

            loop {
                let now = start.elapsed();
                if rx.transport().closed() {
                    return Err(EitherError::A(TransportLinkError::TransportClosed));
                }
                if rx.transport().should_close(now.into()) {
                    let _ = self.tx.lock().await.close().await;
                    return Err(EitherError::A(TransportLinkError::RxLeaseExpired));
                }

                let (write_lease, read_lease) = self.sync(start, now, &mut rx).await;
                if self.is_closed() {
                    return Ok(());
                }

                match select4(write_lease, read_lease, rx.recv(), self.shutdown.wait()).await {
                    Either4::First(_) => {
                        let mut tx_guard = self.tx.lock().await;
                        let tx = tx_guard.deref_mut();

                        if tx.transport().should_close(start.elapsed().into()) {
                            let _ = tx.close().await;
                            break Err(EitherError::A(TransportLinkError::TxLeaseExpired));
                        }

                        if tx.transport().should_send_keepalive(start.elapsed().into()) {
                            zenoh_proto::trace!("Sending Keepalive");
                            tx.keepalive().await?;
                        }

                        continue;
                    }
                    Either4::Third(res) => {
                        let mut state = state.lock().await;

                        for msg in res? {
                            let stop = update(self.zid, &mut state, msg.0, msg.1)
                                .await
                                .map_err(EitherError::B)?;
                            if stop.should_stop() {
                                return Ok(());
                            }
                        }

                        continue;
                    }
                    Either4::Fourth(_) => return Ok(()),
                    _ => {}
                }

                if rx.transport().should_close(start.elapsed().into()) {
                    let _ = self.tx.lock().await.close().await;
                    break Err(EitherError::A(TransportLinkError::RxLeaseExpired));
                }
            }
        }
        .await;

        if result.is_err() {
            self.begin_close();
            self.shutdown.signal(());
        }

        result
    }

    pub async fn sync(
        &self,
        start: Instant,
        now: Duration,
        rx: &mut TransportLinkRx<'res, Link::Rx<'res>, Buff>,
    ) -> (Timer, Timer) {
        let mut tx_guard = self.tx.lock().await;
        let tx = tx_guard.deref_mut();

        rx.transport_mut().sync(Some(tx.transport()), now.into());
        tx.transport_mut().sync(Some(rx.transport()), now.into());

        let write_lease = start + tx.transport().next_timeout().try_into().unwrap();
        let read_lease = start + rx.transport().next_timeout().try_into().unwrap();

        (Timer::at(write_lease), Timer::at(read_lease))
    }
}
