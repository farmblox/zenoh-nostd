use embassy_futures::select::{Either, select};
use embassy_time::{Instant, Timer};
use zenoh_proto::{exts::Value, msgs::*, *};

use crate::{
    api::{
        callbacks::{ZCallbacks, ZDynCallback},
        query::QueryableQuery,
        session::Session,
    },
    config::ZSessionConfig,
    session::{GetResponse, Sample},
};

impl<'s, 'res, Config> Session<'s, 'res, Config>
where
    Config: ZSessionConfig,
    'res: 's,
{
    pub async fn run(&self) -> core::result::Result<(), SessionError> {
        self.run_inner(None).await
    }

    /// Drive the session until one executor-less query is complete.
    pub async fn run_until_response_final(
        &self,
        request_id: u32,
        deadline: Instant,
    ) -> core::result::Result<(), SessionError> {
        match select(self.run_inner(Some(request_id)), Timer::at(deadline)).await {
            Either::First(result) => result,
            Either::Second(_) => {
                self.state().await.get_callbacks.remove(request_id)?;
                Err(SessionError::RequestTimedout)
            }
        }
    }

    async fn run_inner(&self, stop_after: Option<u32>) -> core::result::Result<(), SessionError> {
        let result = self
            .driver
            .run(&self.state, async |_, state, msg, _| {
                match msg.body {
                    NetworkBody::Push(Push {
                        wire_expr,
                        payload: PushBody::Put(Put { payload, .. }),
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(ke) = state
                            .keyexprs
                            .resolve_keyexpr(&wire_expr, &mut resolved)?
                        else {
                            zenoh_proto::warn!(
                                "sample references an unknown key-expression mapping"
                            );
                            return Ok(false);
                        };
                        let sample = Sample::new(ke, payload);

                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    NetworkBody::Response(Response {
                        rid,
                        wire_expr,
                        payload,
                        ..
                    }) => {
                        let mut resolved = heapless::String::new();
                        let Some(ke) = state
                            .keyexprs
                            .resolve_keyexpr(&wire_expr, &mut resolved)?
                        else {
                            zenoh_proto::warn!(
                                "response references an unknown key-expression mapping"
                            );
                            return Ok(false);
                        };
                        let response = match payload {
                            ResponseBody::Reply(Reply {
                                payload: PushBody::Put(Put { payload, .. }),
                                ..
                            }) => GetResponse::Ok(Sample::new(ke, payload)),
                            ResponseBody::Err(Err { payload, .. }) => {
                                GetResponse::Err(Sample::new(ke, payload))
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
                        let Some(ke) = state
                            .keyexprs
                            .resolve_keyexpr(&wire_expr, &mut resolved)?
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
                        if !state.keyexprs.declare(id, wire_expr.suffix) {
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
                        body: DeclareBody::DeclareToken(DeclareToken { id, wire_expr }),
                        ..
                    }) => {
                        // Resolved, not read straight off the wire: a router
                        // answers an interest with the mapped form — a numeric
                        // scope and an empty suffix — and reading `suffix`
                        // alone yields "" and fails to parse.
                        let mut buf = heapless::String::new();
                        let Some(ke) = state.keyexprs.resolve_keyexpr(&wire_expr, &mut buf)? else {
                            zenoh_proto::warn!("token references an unknown key-expression mapping");
                            return Ok(false);
                        };
                        if !state.tokens.declare(id, ke.as_str()) {
                            zenoh_proto::warn!(
                                "liveliness token {} refused (duplicate, table full, or expression too long)",
                                id
                            );
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
                                    state.keyexprs.resolve_keyexpr(&wire_expr, &mut buf)?
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
            .map_err(|e| e.flatten_map());

        if result.is_err() {
            self.driver.begin_close();
            self.discard_state().await;
            let _ = self.driver.finish_close().await;
        }

        result
    }
}

fn completes_requested_query(stop_after: Option<u32>, response_id: u32) -> bool {
    stop_after == Some(response_id)
}

#[cfg(test)]
mod tests {
    use super::completes_requested_query;

    #[test]
    fn only_the_requested_final_stops_an_executorless_run() {
        assert!(completes_requested_query(Some(7), 7));
        assert!(!completes_requested_query(Some(7), 6));
        assert!(!completes_requested_query(None, 7));
    }
}
