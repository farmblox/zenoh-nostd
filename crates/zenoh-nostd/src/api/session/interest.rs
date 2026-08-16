//! The `Interest` protocol — asking a peer for declarations.
//!
//! Zenoh 1.x does not push declarations unsolicited. A node that wants to know
//! what subscribers, queryables, key expressions or **liveliness tokens** exist
//! sends an `Interest` naming what it cares about, and the peer answers with
//! the matching `Declare`s (see the spec's session/interests page).
//!
//! `zenoh-proto` has carried the whole message model for a while — the four
//! modes, the `A|M|N|R|T|Q|S|K` options byte, `InterestFinal`, `DeclareFinal`,
//! codecs and tests. What was missing is anyone sending one. This is that.
//!
//! ## The four modes, and when each is right
//!
//! - [`InterestMode::Current`] — "what exists right now". The peer replies with
//!   matching declarations, then a `DeclareFinal` carrying the same interest
//!   id, and is done.
//! - [`InterestMode::Future`] — "tell me about changes from here on". No
//!   snapshot, no `DeclareFinal`; it runs until cancelled.
//! - [`InterestMode::CurrentFuture`] — both. The snapshot arrives first,
//!   `DeclareFinal` marks where it ends, and updates keep coming after.
//! - [`InterestMode::Final`] — cancels an outstanding `Future` or
//!   `CurrentFuture` by id. It carries no options and no key expression.
//!
//! ## Why this matters here rather than as an optimization
//!
//! Queries and subscriptions work against a router without it, because a router
//! pushes what it knows. **Liveliness does not exist without it at all**: in
//! Zenoh's own session a liveliness subscriber *is* an interest —
//! `history: true` is `CurrentFuture` with `KEYEXPRS + TOKENS`, `history:
//! false` is `Future`, and undeclaring sends `Final`. A token is only ever
//! delivered in answer to one.

use zenoh_proto::{exts::QoS, fields::*, msgs::*, *};

use crate::{api::session::Session, config::ZSessionConfig, io::transport::ZTransportLinkTx};

/// An outstanding interest. Dropping it does **not** cancel — call
/// [`Interest::undeclare`], because cancelling is a message on the wire and
/// sending one needs `await`.
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
    /// The id the peer echoes in the `DeclareFinal` that closes this
    /// interest's snapshot, and the id [`Self::undeclare`] cancels.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Cancel: send `Interest` in `Final` mode for this id.
    ///
    /// Per the spec, a `Final` carries only the id — no options byte and no key
    /// expression — which is why this is `InterestFinal` and not an `Interest`
    /// with the mode set.
    pub async fn undeclare(self) -> core::result::Result<(), SessionError> {
        self.session
            .driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::InterestFinal(InterestFinal {
                    id: self.id,
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
    /// Send an `Interest` for `ke`, restricted to the declarations named by
    /// `options`.
    ///
    /// The key expression is always included, so the `R` flag is always set:
    /// an unrestricted interest asks a router for its entire declaration table,
    /// which is not something a constrained peer — a browser, a microcontroller
    /// — should ever do by accident. Callers that genuinely want everything
    /// pass `**`, and say so at the call site.
    pub async fn declare_interest(
        &self,
        ke: &'static keyexpr,
        mode: InterestMode,
        options: InterestOptions,
    ) -> core::result::Result<InterestGuard<'_, 's, 'res, Config>, SessionError> {
        let id = self.state().await.next();

        let msg = Interest {
            id,
            mode,
            inner: InterestInner {
                options: options.options,
                wire_expr: Some(WireExpr::from(ke)),
            },
            ..Default::default()
        };

        self.driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::Interest(msg),
            }))
            .await?;

        Ok(InterestGuard { id, session: self })
    }

    /// Ask for the liveliness tokens matching `ke`.
    ///
    /// `history` chooses the mode, exactly as Zenoh's own session does:
    /// `true` asks for the tokens that already exist *and* the ones to come
    /// (`CurrentFuture`), `false` for only what happens next (`Future`). The
    /// options are `KEYEXPRS + TOKENS` either way — key expressions because the
    /// token declarations reference them, tokens because that is the thing
    /// being asked for.
    ///
    /// A caller that wants to know "is anything alive at this key *right now*"
    /// — which is what reading an ownership token amounts to — wants
    /// `history: true`. Without it the answer is silence until the next change,
    /// which reads identically to "nothing is there".
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
