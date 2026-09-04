use embassy_futures::select::{Either, select};
use embassy_time::{Instant, Timer};
use zenoh_proto::{exts::Value, msgs::*, *};

use crate::{
    api::{
        callbacks::{ZCallbacks, ZDynCallback},
        query::QueryableQuery,
        session::{Session, keyexprs::TokenTable},
    },
    config::ZSessionConfig,
    session::{GetResponse, Sample},
};

#[cfg(feature = "alloc")]
use crate::api::session::{declarations::ZDeclarations, keyexprs::KeyExprTable};
#[cfg(feature = "alloc")]
use crate::io::transport::ConnectionRetryPolicy;

/// A transport transition reported by [`Session::run_reconnecting`].
#[cfg(feature = "alloc")]
pub enum SessionEvent<'a> {
    /// The old transport ended. In-flight queries have been finalized and are
    /// never replayed.
    Disconnected(&'a SessionError),
    /// A replacement transport is open and the session's declarations have
    /// been replayed.
    Reconnected,
}

impl<'s, 'res, Config> Session<'s, 'res, Config>
where
    Config: ZSessionConfig,
    'res: 's,
{
    pub async fn run(&self) -> core::result::Result<(), SessionError> {
        let result = self.run_inner(None).await;
        if result.is_err() {
            self.driver.begin_close();
            self.discard_state().await;
            let _ = self.driver.finish_close().await;
        }
        result
    }

    /// Drive the session until one executor-less query is complete.
    pub async fn run_until_response_final(
        &self,
        request_id: u32,
        deadline: Instant,
    ) -> core::result::Result<(), SessionError> {
        match select(self.run_inner(Some(request_id)), Timer::at(deadline)).await {
            Either::First(result) => {
                if result.is_err() {
                    self.driver.begin_close();
                    self.discard_state().await;
                    let _ = self.driver.finish_close().await;
                }
                result
            }
            Either::Second(_) => {
                self.state().await.get_callbacks.remove(request_id)?;
                Err(SessionError::RequestTimedout)
            }
        }
    }

    async fn run_inner(&self, stop_after: Option<u32>) -> core::result::Result<(), SessionError> {
        self.driver
            .run(&self.state, async |_, state, msg, _| {
                match msg.body {
                    NetworkBody::Push(Push {
                        wire_expr,
                        payload: PushBody::Put(Put {
                            payload,
                            attachment,
                            ..
                        }),
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(ke) =
                            self.resolve_wire_key(&state.keyexprs, &wire_expr, &mut resolved)?
                        else {
                            zenoh_proto::warn!(
                                "sample references an unknown key-expression mapping"
                            );
                            return Ok(false);
                        };
                        let sample = Sample::with_metadata(
                            ke,
                            payload,
                            attachment.as_ref().map(|attachment| attachment.buffer),
                            None,
                        );

                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    NetworkBody::Response(Response {
                        rid,
                        wire_expr,
                        payload,
                        respid,
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(ke) =
                            self.resolve_wire_key(&state.keyexprs, &wire_expr, &mut resolved)?
                        else {
                            zenoh_proto::warn!(
                                "response references an unknown key-expression mapping"
                            );
                            return Ok(false);
                        };
                        let response = match payload {
                            ResponseBody::Reply(Reply {
                                payload:
                                    PushBody::Put(Put {
                                        payload,
                                        attachment,
                                        ..
                                    }),
                                ..
                            }) => GetResponse::Ok(Sample::with_metadata(
                                ke,
                                payload,
                                attachment.as_ref().map(|attachment| attachment.buffer),
                                respid,
                            )),
                            ResponseBody::Err(Err { payload, .. }) => {
                                GetResponse::Err(Sample::with_metadata(
                                    ke, payload, None, respid,
                                ))
                            }
                        };

                        if let Some(cb) = state.get_callbacks.get(rid) {
                            cb.call(&response).await;
                        }
                    }
                    NetworkBody::ResponseFinal(ResponseFinal { rid, .. }) => {
                        // Told, then forgotten. The caller learns the query is
                        // spent — otherwise "no more replies" and "none yet"
                        // are the same silence, and the only way out is a
                        // timeout.
                        if let Some(cb) = state.get_callbacks.get(rid) {
                            cb.call(&GetResponse::Final).await;
                        }
                        state.get_callbacks.remove(rid)?;
                        if completes_requested_query(stop_after, rid) {
                            return Ok(true);
                        }
                    }
                    NetworkBody::Request(Request {
                        id,
                        wire_expr,
                        payload:
                            RequestBody::Query(Query {
                                parameters, body, ..
                            }),
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(ke) =
                            self.resolve_wire_key(&state.keyexprs, &wire_expr, &mut resolved)?
                        else {
                            zenoh_proto::warn!(
                                "query references an unknown key-expression mapping"
                            );
                            return Ok(false);
                        };
                        let query = QueryableQuery::new(
                            self,
                            id,
                            ke,
                            if parameters.is_empty() {
                                None
                            } else {
                                Some(parameters)
                            },
                            match body {
                                Some(Value { payload, .. }) => Some(payload),
                                None => None,
                            },
                        );

                        let count = state.queryable_callbacks.intersects(ke).count();
                        state.queryable_callbacks.set_counter(id, count)?;
                        for cb in state.queryable_callbacks.intersects(ke) {
                            cb.call(&query).await;
                        }
                    }
                    // Key-expression mappings. A peer declares a long key once
                    // and references it by id afterwards; every reference is
                    // unreadable until this is recorded (see
                    // `api::session::keyexprs`). Asking for these is what an
                    // interest's KEYEXPRS flag is for.
                    NetworkBody::Declare(Declare {
                        body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr { id, wire_expr }),
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(wire_key) = state.keyexprs.resolve(&wire_expr, &mut resolved) else {
                            zenoh_proto::warn!(
                                "key-expression mapping {} references an unknown mapping",
                                id
                            );
                            return Ok(false);
                        };
                        // A mapping may name only a prefix of this session's
                        // namespace (for example `fb`). Retain the mapping and
                        // enforce the namespace after a later WireExpr suffix
                        // resolves the complete operation key, as mainline
                        // Zenoh's namespace boundary does.
                        if !state.keyexprs.declare(id, wire_key) {
                            // Said once, here, rather than discovered later as
                            // a reference that resolves to nothing.
                            zenoh_proto::warn!(
                                "key-expression mapping {} refused (table full, or expression too long)",
                                id
                            );
                        }
                    }
                    NetworkBody::Declare(Declare {
                        body: DeclareBody::UndeclareKeyExpr(UndeclareKeyExpr { id }),
                        ..
                    }) => {
                        state.keyexprs.undeclare(id);
                    }

                    // Liveliness. A token is not its own message family — it
                    // arrives as a `Declare` the peer sends only in answer to
                    // an `Interest` carrying TOKENS (see
                    // `api::session::interest`). Without this arm the interest
                    // goes out, the peer answers, and the answer is dropped on
                    // the floor: indistinguishable from nothing being alive.
                    //
                    // Delivered through the subscriber callbacks, because a
                    // token is consumed exactly like a sample — "something is
                    // alive at this key" — and a second callback registry would
                    // mean a caller had to know which kind of aliveness it was
                    // subscribing to before it could ask.
                    NetworkBody::Declare(Declare {
                        id: interest_id,
                        body: DeclareBody::DeclareToken(DeclareToken { id, wire_expr }),
                        ..
                    }) => {
                        // Resolved, not read straight off the wire: a router
                        // answers an interest with the mapped form — a numeric
                        // scope and an empty suffix — and reading `suffix`
                        // alone yields "" and fails to parse.
                        let mut buf = heapless::String::new();
                        let Some(ke) =
                            self.resolve_wire_key(&state.keyexprs, &wire_expr, &mut buf)?
                        else {
                            zenoh_proto::warn!("token references an unknown key-expression mapping");
                            return Ok(false);
                        };
                        // A current snapshot is correlated by its interest id.
                        // It may repeat a nonzero token entity already learned
                        // by another interest; every history subscriber still
                        // receives that snapshot, while the one remembered
                        // entity remains responsible for the future drop.
                        if !record_token_declaration(
                            &mut state.tokens,
                            id,
                            interest_id,
                            ke.as_str(),
                        ) {
                            if id == 0 {
                                zenoh_proto::warn!(
                                    "liveliness token 0 arrived without an interest id"
                                );
                            } else {
                                zenoh_proto::warn!(
                                    "liveliness token {} refused (duplicate, table full, or expression too long)",
                                    id
                                );
                            }
                            return Ok(false);
                        }
                        // A token carries no payload: its existence is the
                        // whole message.
                        let sample = Sample::new(ke, &[]);
                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    NetworkBody::Declare(Declare {
                        body:
                            DeclareBody::UndeclareToken(UndeclareToken {
                                id,
                                wire_expr: Some(wire_expr),
                            }),
                        ..
                    }) => {
                        // The drop matters as much as the appearance — losing a
                        // token is how a peer's failure is observed.
                        //
                        let remembered = state.tokens.undeclare(id);
                        let mut buf = heapless::String::new();
                        let ke = match remembered.as_deref() {
                            Some(remembered) => keyexpr::new(remembered)?,
                            None => {
                                let wire_expr = wire_expr.as_wire_expr();
                                let Some(ke) =
                                    self.resolve_wire_key(&state.keyexprs, &wire_expr, &mut buf)?
                                else {
                                    return Ok(false);
                                };
                                ke
                            }
                        };
                        // `Delete`, so a receiver can tell a peer that appeared
                        // from one that died — both arrive through the same
                        // callbacks.
                        let sample = Sample::delete(ke);
                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    NetworkBody::Declare(Declare {
                        body:
                            DeclareBody::UndeclareToken(UndeclareToken {
                                id,
                                wire_expr: None,
                            }),
                        ..
                    }) => {
                        let Some(remembered) = state.tokens.undeclare(id) else {
                            zenoh_proto::warn!("undeclare references an unknown token id {}", id);
                            return Ok(false);
                        };
                        let ke = keyexpr::new(remembered.as_str())?;
                        let sample = Sample::delete(ke);
                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    _ => {}
                }

                Ok::<bool, SessionError>(false)
            })
            .await
            .map_err(|e| e.flatten_map())
    }

    /// Keep one API session alive while replacing failed transports.
    ///
    /// Subscriber, queryable, and interest declarations are replayed with
    /// their original ids. In-flight queries are finalized once and removed:
    /// replaying a request could repeat a non-idempotent operation. The first
    /// reconnect attempt is immediate; backoff starts only after a failed
    /// attempt, matching the reference runtime's connector behavior.
    #[cfg(feature = "alloc")]
    pub async fn run_reconnecting(
        &self,
        policy: ConnectionRetryPolicy,
        mut on_event: impl FnMut(SessionEvent<'_>),
    ) -> core::result::Result<(), SessionError> {
        loop {
            let error = match self.run_inner(None).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            self.driver.discard_transport().await;
            self.prepare_reconnect().await;
            if self.driver.should_stop() {
                return Ok(());
            }
            on_event(SessionEvent::Disconnected(&error));

            loop {
                let connect = self.config.transports().connect_retrying(
                    self.endpoint.clone(),
                    self.config.buff(),
                    policy,
                );
                let connected = match select(connect, self.driver.wait_stop()).await {
                    Either::First(transport) => transport,
                    Either::Second(_) => return Ok(()),
                };
                self.driver.install(connected).await;
                if self.driver.should_stop() {
                    return Ok(());
                }
                if self.replay_declarations().await.is_ok() {
                    on_event(SessionEvent::Reconnected);
                    break;
                }
                self.driver.discard_transport().await;
            }
        }
    }

    #[cfg(feature = "alloc")]
    async fn prepare_reconnect(&self) {
        let mut state = self.state().await;
        let all = keyexpr::new("**").expect("the Zenoh all-key expression is valid");
        for callback in state.get_callbacks.intersects(all) {
            callback.call(&GetResponse::Final).await;
        }
        state.get_callbacks = Config::GetCallbacks::empty();
        state.queryable_callbacks.clear_counters();
        state.keyexprs = KeyExprTable::new();
        state.tokens = TokenTable::new();
    }

    #[cfg(feature = "alloc")]
    async fn replay_declarations(&self) -> core::result::Result<(), SessionError> {
        let declarations = self
            .state()
            .await
            .declarations
            .iter()
            .collect::<alloc::vec::Vec<_>>();
        for declaration in declarations {
            // The temporary expression remains alive until this one replay
            // message has been serialized. Replaying sequentially also keeps
            // namespace enforcement identical to the original declaration.
            let mut scoped = heapless::String::new();
            let wire_expr = self.wire_expr(declaration.key(), &mut scoped)?;
            self.driver
                .send(core::iter::once(declaration.message(wire_expr)))
                .await?;
        }
        Ok(())
    }
}

/// Apply the part of a received token declaration that survives beyond this
/// message.
///
/// Snapshot declarations are correlated by their interest id. A second
/// history interest can replay a token entity already held in the table; that
/// replay must be delivered again so the second subscriber learns the current
/// state. An uncorrelated duplicate is malformed and remains rejected.
fn record_token_declaration(
    tokens: &mut TokenTable,
    token_id: u32,
    interest_id: Option<u32>,
    key: &str,
) -> bool {
    if token_id == 0 {
        return interest_id.is_some();
    }
    if interest_id.is_some() && tokens.matches(token_id, key) {
        return true;
    }
    tokens.declare(token_id, key)
}

fn completes_requested_query(stop_after: Option<u32>, response_id: u32) -> bool {
    stop_after == Some(response_id)
}

#[cfg(test)]
mod tests {
    use super::{TokenTable, completes_requested_query, record_token_declaration};

    #[cfg(feature = "alloc")]
    use crate::io::transport::ConnectionRetryPolicy;

    #[test]
    fn only_the_requested_final_stops_an_executorless_run() {
        assert!(completes_requested_query(Some(7), 7));
        assert!(!completes_requested_query(Some(7), 6));
        assert!(!completes_requested_query(None, 7));
    }

    #[test]
    fn current_snapshot_token_is_deliverable_but_not_remembered() {
        let mut tokens = TokenTable::new();
        assert!(record_token_declaration(
            &mut tokens,
            0,
            Some(7),
            "demo/token/current"
        ));
        assert!(tokens.undeclare(0).is_none());
    }

    #[test]
    fn zero_token_without_an_interest_is_rejected() {
        let mut tokens = TokenTable::new();
        assert!(!record_token_declaration(
            &mut tokens,
            0,
            None,
            "demo/token/malformed"
        ));
        assert!(tokens.undeclare(0).is_none());
    }

    #[test]
    fn each_history_interest_can_replay_the_same_live_token() {
        let mut tokens = TokenTable::new();
        assert!(record_token_declaration(
            &mut tokens,
            12,
            Some(7),
            "demo/token/current"
        ));
        assert!(record_token_declaration(
            &mut tokens,
            12,
            Some(8),
            "demo/token/current"
        ));
        assert_eq!(tokens.undeclare(12).as_deref(), Some("demo/token/current"));
    }

    #[test]
    fn an_uncorrelated_duplicate_token_is_rejected() {
        let mut tokens = TokenTable::new();
        assert!(record_token_declaration(
            &mut tokens,
            12,
            None,
            "demo/token/current"
        ));
        assert!(!record_token_declaration(
            &mut tokens,
            12,
            None,
            "demo/token/current"
        ));
        assert!(!record_token_declaration(
            &mut tokens,
            12,
            Some(8),
            "demo/token/different"
        ));
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn reconnect_backoff_is_bounded() {
        let policy = ConnectionRetryPolicy::default();
        let mut delay = policy.initial_delay;
        for _ in 0..32 {
            delay = policy.next_delay(delay);
        }
        assert_eq!(delay, policy.maximum_delay);
    }
}
