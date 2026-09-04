use embassy_sync::{
    blocking_mutex::raw::NoopRawMutex,
    mutex::{Mutex, MutexGuard},
};
use zenoh_proto::{Endpoint, SessionError, TransportLinkError, fields::WireExpr, keyexpr};

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
pub use run::SessionEvent;

#[cfg(feature = "alloc")]
pub use crate::io::transport::ConnectionRetryPolicy;

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

#[cfg(test)]
#[path = "session/namespace_tests.rs"]
mod namespace_tests;

/// Maximum fully namespaced key accepted at the constrained session boundary.
///
/// This is separate from the smaller inbound mapping table: an ordinary
/// operation needs one temporary stack buffer, while retained remote mappings
/// remain independently bounded.
pub(crate) const MAX_SESSION_KEYEXPR: usize = 512;

/// Apply mainline Zenoh's namespace egress rule to one unoptimized key.
fn project_namespace<'a>(
    namespace: Option<&zenoh_proto::nonwild_keyexpr>,
    logical: &'a keyexpr,
    storage: &'a mut heapless::String<MAX_SESSION_KEYEXPR>,
) -> core::result::Result<WireExpr<'a>, SessionError> {
    let Some(namespace) = namespace else {
        return Ok(WireExpr::from(logical));
    };

    storage.clear();
    storage
        .push_str(namespace.as_str())
        .map_err(|_| zenoh_proto::CollectionError::CollectionTooSmall)?;
    if !logical.as_str().is_empty() {
        storage
            .push('/')
            .map_err(|_| zenoh_proto::CollectionError::CollectionTooSmall)?;
    }
    storage
        .push_str(logical.as_str())
        .map_err(|_| zenoh_proto::CollectionError::CollectionTooSmall)?;
    Ok(WireExpr::from(keyexpr::new(storage.as_str())?))
}

/// Apply mainline Zenoh's namespace ingress rule to one resolved key.
fn remove_namespace<'a>(
    namespace: Option<&zenoh_proto::nonwild_keyexpr>,
    wire_key: &'a str,
) -> Option<&'a str> {
    let Some(namespace) = namespace else {
        return Some(wire_key);
    };
    let tail = wire_key.strip_prefix(namespace.as_str())?;
    if tail.is_empty() {
        Some(tail)
    } else {
        tail.strip_prefix('/')
    }
}

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
/// the caller-owned `Resources` it was built from; `'res` is the lifetime of the link itself, which comes
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
        config: &'res Config,
        transport: &'s mut TransportLink<
            <Config::LinkManager as ZLinkManager>::Link<'res>,
            Config::Buff,
        >,
    ) -> Self {
        Self {
            driver: Driver::new(transport),
            config,
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

    /// Build the transport expression for one application-relative key.
    ///
    /// Copying through caller-owned fixed storage gives namespace handling one
    /// central, allocation-free boundary. Public operation builders cannot
    /// reach the transport driver without passing through this method.
    pub(crate) fn wire_expr<'a>(
        &self,
        logical: &'a keyexpr,
        storage: &'a mut heapless::String<MAX_SESSION_KEYEXPR>,
    ) -> core::result::Result<WireExpr<'a>, SessionError> {
        project_namespace(self.config.namespace(), logical, storage)
    }

    /// Remove this session's namespace from one resolved inbound expression.
    ///
    /// A namespaced session rejects rather than exposes traffic outside its
    /// prefix. The router may therefore share one physical link without a
    /// broad subscriber or queryable becoming an escape from containment.
    pub(crate) fn logical_key<'a>(&self, wire_key: &'a str) -> Option<&'a str> {
        remove_namespace(self.config.namespace(), wire_key)
    }

    /// Resolve a possibly mapped wire expression and enforce the namespace.
    pub(crate) fn resolve_wire_key<'a>(
        &self,
        table: &keyexprs::KeyExprTable,
        wire_expr: &WireExpr<'_>,
        storage: &'a mut heapless::String<MAX_SESSION_KEYEXPR>,
    ) -> core::result::Result<Option<&'a keyexpr>, SessionError> {
        let Some(resolved) = table.resolve(wire_expr, storage) else {
            return Ok(None);
        };
        let Some(logical) = self.logical_key(resolved) else {
            return Ok(None);
        };
        Ok(Some(keyexpr::new(logical)?))
    }

    /// Reopen an explicitly closed allocator-backed session at its original
    /// endpoint.
    ///
    /// [`Self::close`] releases every declaration. Callers declare the new
    /// application scope after this returns. Transport-loss recovery uses
    /// [`Self::run_reconnecting`] and preserves declarations automatically.
    /// Dropping the future cancels the current connection attempt or backoff
    /// and leaves the session closed.
    ///
    /// # Errors
    ///
    /// Returns [`TransportLinkError::TransportClosed`] if another owner has
    /// already reopened the session before this attempt installs its link.
    #[cfg(feature = "alloc")]
    pub async fn reopen(
        &self,
        policy: ConnectionRetryPolicy,
    ) -> core::result::Result<(), TransportLinkError> {
        let transport = self
            .config
            .transports()
            .connect_retrying(self.endpoint.clone(), self.config.buff(), policy)
            .await;
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
    Ok(Session::new(
        config,
        resources.init(config.transports().connect(endpoint, config.buff()).await?),
    ))
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

/// Open an allocator-backed session, retrying until its first transport opens.
///
/// Dropping the returned future cancels the current attempt and backoff. Once
/// open, drive the session with [`Session::run_reconnecting`] so the same
/// connector policy owns later replacement links and declaration replay.
/// The future returns only after the transport handshake has completed.
#[cfg(feature = "alloc")]
pub async fn session_connect_retrying<'res, Config>(
    config: &'res Config,
    endpoint: Endpoint<'res>,
    policy: ConnectionRetryPolicy,
) -> Session<'res, 'res, Config>
where
    Config: ZSessionConfig,
{
    let transport = config
        .transports()
        .connect_retrying(endpoint.clone(), config.buff(), policy)
        .await;
    Session::new(config, endpoint, transport)
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
    Ok(Session::new(
        config,
        resources.init(config.transports().listen(endpoint, config.buff()).await?),
    ))
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
                config,
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
                config,
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

    Ok(Session::new(config, resources.init(transport)))
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
    Ok(Session::new(config, resources.init(transport)))
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
