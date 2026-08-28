use embassy_sync::{
    blocking_mutex::raw::NoopRawMutex,
    mutex::{Mutex, MutexGuard},
};
use zenoh_proto::{Endpoint, TransportLinkError};

use crate::{
    api::callbacks::ZCallbacks, api::session::declarations::ZDeclarations, config::ZSessionConfig,
    io::transport::TransportLink, platform::ZLinkManager,
};

#[cfg(not(feature = "alloc"))]
use crate::{io::driver::Driver, resources::Resources};

#[cfg(feature = "alloc")]
use self::managed::ManagedDriver;

mod run;

#[cfg(feature = "alloc")]
pub use run::{ReconnectPolicy, SessionEvent};

#[cfg(feature = "alloc")]
mod managed;

pub mod declarations;
pub mod get;
pub mod interest;
pub mod keyexprs;
pub mod r#pub;
pub mod put;
pub mod querier;
pub mod queryable;
pub mod sub;

pub(crate) struct SessionState<'s, 'res, Config>
where
    Config: ZSessionConfig + 'res,
    'res: 's,
{
    next: u32,
    sub_callbacks: Config::SubCallbacks<'res>,
    get_callbacks: Config::GetCallbacks<'res>,
    queryable_callbacks: Config::QueryableCallbacks<'s, 'res>,
    declarations: Config::Declarations,
    /// Numeric key-expression ids to the strings they stand for. A peer may
    /// declare a long key once and reference it by id afterwards; without this
    /// those references are unreadable (see [`keyexprs`]).
    pub(crate) keyexprs: keyexprs::KeyExprTable,
    /// Liveliness declaration ids to their resolved expressions. A token may
    /// later be undeclared by id alone.
    pub(crate) tokens: keyexprs::TokenTable,
}

impl<'s, 'res, Config> SessionState<'s, 'res, Config>
where
    Config: ZSessionConfig,
    'res: 's,
{
    pub fn new() -> Self {
        Self {
            next: 0,
            sub_callbacks: Config::SubCallbacks::empty(),
            get_callbacks: Config::GetCallbacks::empty(),
            queryable_callbacks: Config::QueryableCallbacks::empty(),
            declarations: Config::Declarations::empty(),
            keyexprs: keyexprs::KeyExprTable::new(),
            tokens: keyexprs::TokenTable::new(),
        }
    }

    pub(crate) fn next(&mut self) -> u32 {
        let next = self.next;
        self.next += 1;
        next
    }
}

/// A zenoh session.
///
/// The two lifetimes are deliberately distinct. `'a` is how long the session borrows the
/// [`Resources`] it was built from; `'res` is the lifetime of the link itself, which comes
/// from the [`ZLinkManager`] inside the config.
///
/// Conflating them — as a single `'res` did — forces `&'res mut Resources<'res, Config>`,
/// which is invariant and, because `Resources` has a destructor, makes the storage outlive
/// itself. `'static` is then the only lifetime that typechecks, so a session can only ever
/// be built once. Keeping them apart lets a session be scoped and rebuilt, which is what a
/// client needs in order to reconnect after a dropped link.
pub struct Session<'s, 'res, Config>
where
    Config: ZSessionConfig,
    'res: 's,
{
    #[cfg(not(feature = "alloc"))]
    driver: Driver<'s, <Config::LinkManager as ZLinkManager>::Link<'res>, Config::Buff>,
    #[cfg(feature = "alloc")]
    driver: ManagedDriver<'res, Config>,
    #[cfg(feature = "alloc")]
    config: &'res Config,
    #[cfg(feature = "alloc")]
    endpoint: Endpoint<'res>,
    state: Mutex<NoopRawMutex, SessionState<'s, 'res, Config>>,
}

impl<'s, 'res, Config> Session<'s, 'res, Config>
where
    Config: ZSessionConfig,
    'res: 's,
{
    #[cfg(not(feature = "alloc"))]
    pub fn new(
        transport: &'s mut TransportLink<
            <Config::LinkManager as ZLinkManager>::Link<'res>,
            Config::Buff,
        >,
    ) -> Self {
        Self {
            driver: Driver::new(transport),
            state: Mutex::new(SessionState::new()),
        }
    }

    #[cfg(feature = "alloc")]
    pub fn new(
        config: &'res Config,
        endpoint: Endpoint<'res>,
        transport: TransportLink<<Config::LinkManager as ZLinkManager>::Link<'res>, Config::Buff>,
    ) -> Self {
        Self {
            driver: ManagedDriver::new(transport),
            config,
            endpoint,
            state: Mutex::new(SessionState::new()),
        }
    }

    pub(crate) async fn state(
        &self,
    ) -> MutexGuard<'_, NoopRawMutex, SessionState<'s, 'res, Config>> {
        self.state.lock().await
    }

    pub(crate) async fn open_state(
        &self,
    ) -> core::result::Result<
        MutexGuard<'_, NoopRawMutex, SessionState<'s, 'res, Config>>,
        TransportLinkError,
    > {
        let state = self.state.lock().await;
        if self.is_closed() {
            return Err(TransportLinkError::TransportClosed);
        }
        Ok(state)
    }

    pub fn is_closed(&self) -> bool {
        self.driver.is_closed()
    }

    /// Reopen an explicitly closed allocator-backed session at its original
    /// endpoint.
    ///
    /// [`Self::close`] releases every declaration. Callers declare the new
    /// application scope after this returns. Transport-loss recovery uses
    /// [`Self::run_reconnecting`] and preserves declarations automatically.
    #[cfg(feature = "alloc")]
    pub async fn reopen(&self) -> core::result::Result<(), TransportLinkError> {
        let transport = self
            .config
            .transports()
            .connect(self.endpoint.clone(), self.config.buff())
            .await?;
        self.driver.reopen(transport).await
    }

    pub(crate) async fn discard_state(&self) {
        let discarded = {
            let mut state = self.state.lock().await;
            core::mem::replace(&mut *state, SessionState::new())
        };
        drop(discarded);
    }

    /// Close this Zenoh session and stop its driver.
    ///
    /// New operations are refused first, callback-bearing state is discarded,
    /// then transport Close is sent and the run loop is woken. This mirrors
    /// the ordering of the reference implementation: no callback can run once
    /// transport teardown begins.
    pub async fn close(&self) -> core::result::Result<(), TransportLinkError>
    where
        Config::Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        if !self.driver.begin_close() {
            return Ok(());
        }
        self.discard_state().await;
        self.driver.finish_close().await
    }
}

#[cfg(not(feature = "alloc"))]
pub async fn session_connect<'s, 'res, Config>(
    resources: &'s mut Resources<'res, Config>,
    config: &'res Config,
    endpoint: Endpoint<'_>,
) -> core::result::Result<Session<'s, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    Ok(Session::new(resources.init(
        config.transports().connect(endpoint, config.buff()).await?,
    )))
}

#[cfg(feature = "alloc")]
pub async fn session_connect<'res, Config>(
    config: &'res Config,
    endpoint: Endpoint<'res>,
) -> core::result::Result<Session<'res, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let transport = config
        .transports()
        .connect(endpoint.clone(), config.buff())
        .await?;
    Ok(Session::new(config, endpoint, transport))
}

#[cfg(not(feature = "alloc"))]
pub async fn session_listen<'s, 'res, Config>(
    resources: &'s mut Resources<'res, Config>,
    config: &'res Config,
    endpoint: Endpoint<'_>,
) -> core::result::Result<Session<'s, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    Ok(Session::new(resources.init(
        config.transports().listen(endpoint, config.buff()).await?,
    )))
}

#[cfg(feature = "alloc")]
pub async fn session_listen<'res, Config>(
    config: &'res Config,
    endpoint: Endpoint<'res>,
) -> core::result::Result<Session<'res, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let transport = config
        .transports()
        .listen(endpoint.clone(), config.buff())
        .await?;
    Ok(Session::new(config, endpoint, transport))
}

#[macro_export]
macro_rules! __session_connect {
    (
        $CONFIG:ty: $config:expr,
        $endpoint:expr
    ) => {{
        static CONFIG: static_cell::StaticCell<$CONFIG> = static_cell::StaticCell::new();
        let config = CONFIG.init($config);

        static SESSION: static_cell::StaticCell<
            $crate::session::Session<'static, 'static, $CONFIG>,
        > = static_cell::StaticCell::new();

        #[cfg(feature = "alloc")]
        let session = {
            let endpoint = $endpoint;
            let transport = config
                .transports()
                .connect(endpoint.clone(), config.buff())
                .await?;
            $crate::session::Session::new(config, endpoint, transport)
        };
        #[cfg(not(feature = "alloc"))]
        let session = {
            static RESOURCES: static_cell::StaticCell<
                $crate::session::Resources<'static, $CONFIG>,
            > = static_cell::StaticCell::new();
            $crate::session::Session::new(
                RESOURCES.init($crate::session::Resources::default()).init(
                    config
                        .transports()
                        .connect($endpoint, config.buff())
                        .await?,
                ),
            )
        };

        SESSION.init(session) as &$crate::session::Session<'static, 'static, $CONFIG>
    }};
}

#[macro_export]
macro_rules! __session_listen {
    (
        $CONFIG:ty: $config:expr,
        $endpoint:expr
    ) => {{
        static CONFIG: static_cell::StaticCell<$CONFIG> = static_cell::StaticCell::new();
        let config = CONFIG.init($config);

        static SESSION: static_cell::StaticCell<
            $crate::session::Session<'static, 'static, $CONFIG>,
        > = static_cell::StaticCell::new();

        #[cfg(feature = "alloc")]
        let session = {
            let endpoint = $endpoint;
            let transport = config
                .transports()
                .listen(endpoint.clone(), config.buff())
                .await?;
            $crate::session::Session::new(config, endpoint, transport)
        };
        #[cfg(not(feature = "alloc"))]
        let session = {
            static RESOURCES: static_cell::StaticCell<
                $crate::session::Resources<'static, $CONFIG>,
            > = static_cell::StaticCell::new();
            $crate::session::Session::new(
                RESOURCES
                    .init($crate::session::Resources::default())
                    .init(config.transports().listen($endpoint, config.buff()).await?),
            )
        };

        SESSION.init(session) as &$crate::session::Session<'static, 'static, $CONFIG>
    }};
}

#[cfg(not(feature = "alloc"))]
pub async fn session_connect_ignore_invalid_sn<'s, 'res, Config>(
    resources: &'s mut Resources<'res, Config>,
    config: &'res Config,
    endpoint: Endpoint<'_>,
) -> core::result::Result<Session<'s, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let mut transport = config.transports().connect(endpoint, config.buff()).await?;
    transport.transport_mut().rx.ignore_invalid_sn();

    Ok(Session::new(resources.init(transport)))
}

#[cfg(feature = "alloc")]
pub async fn session_connect_ignore_invalid_sn<'res, Config>(
    config: &'res Config,
    endpoint: Endpoint<'res>,
) -> core::result::Result<Session<'res, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let mut transport = config
        .transports()
        .connect(endpoint.clone(), config.buff())
        .await?;
    transport.transport_mut().rx.ignore_invalid_sn();
    Ok(Session::new(config, endpoint, transport))
}

#[cfg(not(feature = "alloc"))]
pub async fn session_listen_ignore_invalid_sn<'s, 'res, Config>(
    resources: &'s mut Resources<'res, Config>,
    config: &'res Config,
    endpoint: Endpoint<'_>,
) -> core::result::Result<Session<'s, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let mut transport = config.transports().listen(endpoint, config.buff()).await?;
    transport.transport_mut().rx.ignore_invalid_sn();
    Ok(Session::new(resources.init(transport)))
}

#[cfg(feature = "alloc")]
pub async fn session_listen_ignore_invalid_sn<'res, Config>(
    config: &'res Config,
    endpoint: Endpoint<'res>,
) -> core::result::Result<Session<'res, 'res, Config>, TransportLinkError>
where
    Config: ZSessionConfig,
{
    let mut transport = config
        .transports()
        .listen(endpoint.clone(), config.buff())
        .await?;
    transport.transport_mut().rx.ignore_invalid_sn();
    Ok(Session::new(config, endpoint, transport))
}
