//! # Interest
//!
//! Declaration discovery: asking a peer which subscribers, queryables, key
//! expressions or liveliness tokens exist.
//!
//! ## Overview
//!
//! Zenoh 1.x does not push declarations unsolicited. A node sends an
//! `Interest` naming what it cares about, and the peer answers with the
//! matching `Declare` messages (see the specification's session/interests
//! page).
//!
//! - [`Session::declare_interest`]: send an interest for a key expression
//! - [`Session::declare_liveliness_interest`]: the liveliness form of the above
//! - [`InterestGuard`]: an outstanding interest, cancelled by
//!   [`InterestGuard::undeclare`]
//!
//! ## Modes
//!
//! - [`InterestMode::Current`] — what exists now. The peer replies with the
//!   matching declarations, then a `DeclareFinal` carrying the same interest
//!   id.
//! - [`InterestMode::Future`] — changes from here on. No snapshot, no
//!   `DeclareFinal`; it runs until cancelled.
//! - [`InterestMode::CurrentFuture`] — both. The snapshot arrives first,
//!   `DeclareFinal` marks where it ends, and updates follow.
//! - [`InterestMode::Final`] — cancels an outstanding `Future` or
//!   `CurrentFuture` by id. It carries no options and no key expression.
//!
//! ## Liveliness
//!
//! A liveliness subscriber *is* an interest: `history: true` is
//! `CurrentFuture` with `KEYEXPRS + TOKENS`, `history: false` is `Future`, and
//! undeclaring sends `Final`. A token is only ever delivered in answer to one,
//! so liveliness does not work without this module.
//!
//! ## Example
//!
//! ```ignore
//! let interest = session
//!     .declare_liveliness_interest(keyexpr::new("group/**")?, true)
//!     .await?;
//! // ... tokens arrive through the subscriber callbacks ...
//! interest.undeclare().await?;
//! ```

use zenoh_proto::{exts::QoS, fields::*, msgs::*, *};

use crate::{
    api::session::{
        Session,
        declarations::{Declaration, ZDeclarations},
    },
    config::ZSessionConfig,
};

/// An outstanding interest.
///
/// Dropping it does not cancel: cancelling is a message on the wire, and
/// sending one needs `await`. Call [`InterestGuard::undeclare`].
pub struct InterestGuard<'a, 's, 'res, Config>
where
    Config: ZSessionConfig,
{
    id: u32,
    session: &'a Session<'s, 'res, Config>,
}

impl<'a, 's, 'res, Config> InterestGuard<'a, 's, 'res, Config>
where
    Config: ZSessionConfig,
{
    /// The interest id.
    ///
    /// The peer echoes it in the `DeclareFinal` that closes this interest's
    /// snapshot, and [`Self::undeclare`] cancels by it.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Cancel this interest.
    ///
    /// Sends `InterestFinal`, which carries only the id: per the specification
    /// a `Final` has no options byte and no key expression, so it is a distinct
    /// message rather than an `Interest` with the mode set.
    pub async fn undeclare(self) -> core::result::Result<(), SessionError> {
        self.session.state().await.declarations.remove(self.id);
        if self.session.is_closed() {
            return Ok(());
        }
        self.session
            .driver
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::declare(),
                body: NetworkBody::InterestFinal(InterestFinal {
                    id: self.id,
                    qos: QoS::declare(),
                    ..Default::default()
                }),
            }))
            .await?;
        Ok(())
    }
}

impl<'s, 'res, Config> Session<'s, 'res, Config>
where
    Config: ZSessionConfig,
{
    /// Send an interest for `ke`, restricted to the declarations named by
    /// `options`.
    ///
    /// The key expression is always included, so the `R` flag is always set.
    /// An unrestricted interest asks a router for its entire declaration table,
    /// which a constrained peer should not do by accident; a caller that wants
    /// everything passes `**` explicitly.
    pub async fn declare_interest(
        &self,
        ke: &'static keyexpr,
        mode: InterestMode,
        options: InterestOptions,
    ) -> core::result::Result<InterestGuard<'_, 's, 'res, Config>, SessionError> {
        // Validate and project the complete transport key before retaining the
        // declaration. A local resource must not survive a failed declaration.
        let mut scoped = heapless::String::new();
        let wire_expr = self.wire_expr(ke, &mut scoped)?;
        let mut state = self.open_state().await?;
        let id = state.next();
        state.declarations.insert(Declaration::Interest {
            id,
            key: ke,
            mode,
            options,
        })?;
        drop(state);

        let msg = Interest {
            id,
            mode,
            qos: QoS::declare(),
            inner: InterestInner {
                options: options.options,
                wire_expr: Some(wire_expr),
            },
            ..Default::default()
        };

        if let Err(error) = self
            .driver
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::declare(),
                body: NetworkBody::Interest(msg),
            }))
            .await
        {
            self.state().await.declarations.remove(id);
            return Err(error.into());
        }

        Ok(InterestGuard { id, session: self })
    }

    /// Ask for the liveliness tokens matching `ke`.
    ///
    /// `history` chooses the mode: `true` asks for the tokens that already
    /// exist *and* the ones to come ([`InterestMode::CurrentFuture`]), `false`
    /// for only what happens next ([`InterestMode::Future`]).
    ///
    /// The options are `KEYEXPRS + TOKENS` either way — key expressions because
    /// the token declarations reference them, tokens because that is what is
    /// being asked for.
    ///
    /// Use `history: true` to answer "is anything alive at this key now".
    /// Without it the answer is silence until the next change, which is
    /// indistinguishable from nothing being there.
    pub async fn declare_liveliness_interest(
        &self,
        ke: &'static keyexpr,
        history: bool,
    ) -> core::result::Result<InterestGuard<'_, 's, 'res, Config>, SessionError> {
        self.declare_interest(
            ke,
            if history {
                InterestMode::CurrentFuture
            } else {
                InterestMode::Future
            },
            InterestOptions::KEYEXPRS + InterestOptions::TOKENS,
        )
        .await
    }
}
