use zenoh_proto::{
    TransportLinkError,
    msgs::{NetworkMessage, NetworkMessageRef},
};
use zenoh_sansio::{ZTransportRx, ZTransportTx};

use super::{ZLinkInfo, ZLinkRx, ZLinkTx};

pub trait ZTransportLinkTx {
    fn tx(&mut self) -> (&mut impl ZLinkTx, &mut impl ZTransportTx);

    fn send<'a>(
        &mut self,
        msgs: impl Iterator<Item = NetworkMessage<'a>>,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        async move {
            for msg in msgs {
                transport.encode(msg.as_ref(), None)?;
                while let Some(bytes) = transport.flush(link.is_streamed())? {
                    link.write_all(bytes).await?;
                }
            }
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn send_optimized_ref<'a>(
        &mut self,
        msgs: impl Iterator<Item = (NetworkMessageRef<'a>, &'a [u8])>,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        async move {
            for (msg, bytes) in msgs {
                transport.encode(msg, Some(bytes))?;
                while let Some(bytes) = transport.flush(link.is_streamed())? {
                    link.write_all(bytes).await?;
                }
            }
            Ok(())
        }
    }

    fn keepalive(
        &mut self,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        async move {
            transport.keepalive()?;
            if let Some(bytes) = transport.flush(link.is_streamed())? {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }

    fn close(
        &mut self,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        async move {
            transport.close()?;
            if let Some(bytes) = transport.flush(link.is_streamed())? {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }
}

pub trait ZTransportLinkRx {
    fn rx(&mut self) -> (&mut impl ZLinkRx, &mut impl ZTransportRx);

    fn recv(
        &mut self,
    ) -> impl core::future::Future<
        Output = core::result::Result<
            impl Iterator<Item = (NetworkMessage<'_>, &'_ [u8])>,
            zenoh_proto::TransportLinkError,
        >,
    > {
        let (link, transport) = self.rx();
        let streamed = link.is_streamed();

        async move {
            transport
                .decode_with_async(
                    async |bytes| {
                        if streamed {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    streamed,
                )
                .await
                .map_err(|e| e.flatten_map::<TransportLinkError>())?;

            Ok(transport.flush())
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use crate::io::transport::TransportLink;
    use std::{
        cell::RefCell,
        task::{Context, Poll, Waker},
        vec,
        vec::Vec,
    };
    use zenoh_proto::{
        LinkError,
        exts::QoS,
        fields::{Reliability, WireExpr},
        keyexpr,
        msgs::*,
    };
    use zenoh_sansio::Transport;

    struct Link<'a> {
        writes: &'a RefCell<Vec<Vec<u8>>>,
        streamed: bool,
        fail_after: Option<usize>,
    }

    impl ZLinkInfo for Link<'_> {
        fn mtu(&self) -> u16 {
            128
        }
        fn is_streamed(&self) -> bool {
            self.streamed
        }
    }

    impl ZLinkTx for Link<'_> {
        async fn write_all(&mut self, bytes: &[u8]) -> Result<(), LinkError> {
            let mut writes = self.writes.borrow_mut();
            if self.fail_after == Some(writes.len()) {
                return Err(LinkError::LinkTxFailed);
            }
            writes.push(bytes.to_vec());
            Ok(())
        }
    }

    fn run<T>(future: impl Future<Output = T>) -> T {
        match std::pin::pin!(future)
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("the recording link never waits"),
        }
    }

    #[test]
    fn send_drains_fragments_and_propagates_capacity_and_link_failures() {
        for streamed in [false, true] {
            for (payload_len, fail_after) in [(4_096, None), (8_192, None), (4_096, Some(2))] {
                let payload = vec![0x5a; payload_len];
                let message = NetworkMessage {
                    reliability: Reliability::Reliable,
                    qos: QoS::default(),
                    body: NetworkBody::Push(Push {
                        wire_expr: WireExpr::from(keyexpr::from_str_unchecked("send/value")),
                        payload: PushBody::Put(Put {
                            payload: &payload,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                };
                let writes = RefCell::new(Vec::new());
                let link = Link {
                    writes: &writes,
                    streamed,
                    fail_after,
                };
                let mut sender = TransportLink::new(
                    link,
                    Transport::builder(vec![0; 8_192])
                        .with_batch_size(128)
                        .codec(),
                );
                let result = run(sender.send(core::iter::once(message)));
                let writes = writes.borrow();
                if payload_len == 8_192 {
                    assert!(result.is_err());
                    assert!(writes.is_empty(), "capacity failure must emit no prefix");
                } else {
                    assert_eq!(result.is_err(), fail_after.is_some());
                    if let Some(count) = fail_after {
                        assert_eq!(writes.len(), count);
                    } else {
                        assert!(writes.len() > 1);
                    }
                    let mut receiver = Transport::builder(vec![0; 8_192])
                        .with_batch_size(128)
                        .codec();
                    let mut delivered = Vec::new();
                    for wire in writes.iter() {
                        assert!(wire.len() <= 128);
                        receiver.rx.decode(wire, streamed).unwrap();
                        for (message, _) in receiver.rx.flush() {
                            let NetworkBody::Push(push) = message.body else {
                                panic!("expected push")
                            };
                            let PushBody::Put(put) = push.payload else {
                                panic!("expected put")
                            };
                            delivered.push(put.payload.to_vec());
                        }
                    }
                    assert_eq!(
                        delivered,
                        if fail_after.is_none() {
                            vec![payload]
                        } else {
                            Vec::new()
                        }
                    );
                }
            }
        }
    }
}
