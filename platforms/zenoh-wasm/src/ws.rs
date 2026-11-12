use {
    futures_util::{
        SinkExt as _, StreamExt as _,
        stream::{SplitSink, SplitStream},
    },
    yawc::{
        WebSocket,
        frame::{FrameView, OpCode},
    },
    zenoh_nostd::platform::*,
};

pub struct WasmWsLink {
    sink: SplitSink<WebSocket, FrameView>,
    stream: SplitStream<WebSocket>,
    mtu: u16,
}

impl WasmWsLink {
    pub fn new(stream: WebSocket) -> Self {
        let (sink, stream) = stream.split();
        Self {
            sink,
            stream,
            mtu: u16::MAX,
        }
    }
}

pub struct WasmWsLinkTx<'a> {
    sink: &'a mut SplitSink<WebSocket, FrameView>,
    mtu: u16,
}

pub struct WasmWsLinkRx<'a> {
    stream: &'a mut SplitStream<WebSocket>,
    mtu: u16,
}

impl ZLinkInfo for WasmWsLink {
    fn mtu(&self) -> u16 {
        self.mtu
    }

    fn is_streamed(&self) -> bool {
        false
    }
}

impl ZLinkInfo for WasmWsLinkTx<'_> {
    fn mtu(&self) -> u16 {
        self.mtu
    }

    fn is_streamed(&self) -> bool {
        false
    }
}

impl ZLinkInfo for WasmWsLinkRx<'_> {
    fn mtu(&self) -> u16 {
        self.mtu
    }

    fn is_streamed(&self) -> bool {
        false
    }
}

impl ZLinkTx for WasmWsLink {
    async fn write_all(&mut self, buffer: &[u8]) -> core::result::Result<(), LinkError> {
        self.split().0.write_all(buffer).await
    }
}

impl ZLinkTx for WasmWsLinkTx<'_> {
    async fn write_all(&mut self, buffer: &[u8]) -> core::result::Result<(), LinkError> {
        let item = FrameView::binary(buffer.to_vec());
        self.sink
            .send(item)
            .await
            .map_err(|_| LinkError::LinkTxFailed)
    }
}

impl ZLinkRx for WasmWsLink {
    async fn read(&mut self, buffer: &mut [u8]) -> core::result::Result<usize, LinkError> {
        self.split().1.read(buffer).await
    }

    async fn read_exact(&mut self, buffer: &mut [u8]) -> core::result::Result<(), LinkError> {
        self.split().1.read_exact(buffer).await
    }
}

impl ZLinkRx for WasmWsLinkRx<'_> {
    async fn read(&mut self, buffer: &mut [u8]) -> core::result::Result<usize, LinkError> {
        self.read_datagram(buffer, false).await
    }

    async fn read_exact(&mut self, buffer: &mut [u8]) -> core::result::Result<(), LinkError> {
        self.read_datagram(buffer, true).await.map(|_| ())
    }
}

impl WasmWsLinkRx<'_> {
    /// Validate the entire datagram before exposing any bytes to the transport.
    async fn read_datagram(&mut self, buffer: &mut [u8], exact: bool) -> Result<usize, LinkError> {
        #[cfg(target_arch = "wasm32")]
        let Some(Ok(frame)) = self.stream.next().await else {
            return Err(LinkError::LinkRxFailed);
        };
        #[cfg(not(target_arch = "wasm32"))]
        let Some(frame) = self.stream.next().await else {
            return Err(LinkError::LinkRxFailed);
        };
        let len = frame.payload.len();
        if frame.opcode != OpCode::Binary || len > buffer.len() || (exact && len != buffer.len()) {
            return Err(LinkError::LinkRxFailed);
        }
        buffer[..len].copy_from_slice(&frame.payload);
        Ok(len)
    }
}

impl ZLink for WasmWsLink {
    type Tx<'link>
        = WasmWsLinkTx<'link>
    where
        Self: 'link;

    type Rx<'link>
        = WasmWsLinkRx<'link>
    where
        Self: 'link;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        let tx = WasmWsLinkTx {
            sink: &mut self.sink,
            mtu: self.mtu,
        };
        let rx = WasmWsLinkRx {
            stream: &mut self.stream,
            mtu: self.mtu,
        };
        (tx, rx)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// Use the actual HTTP upgrade and WebSocket codecs without a listening port.
    async fn pair() -> (WasmWsLink, WebSocket) {
        let (client, server) = tokio::io::duplex(4_096);
        let (upgrades, mut upgraded) = tokio::sync::mpsc::channel(1);
        let serve = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    hyper_util::rt::TokioIo::new(server),
                    hyper::service::service_fn(move |mut request| {
                        let (response, upgrade) = WebSocket::upgrade(&mut request).unwrap();
                        upgrades.try_send(upgrade).unwrap();
                        async { Ok::<_, std::convert::Infallible>(response) }
                    }),
                )
                .with_upgrades()
                .await
                .unwrap();
        });
        let client = WebSocket::handshake(
            "ws://test.invalid/".parse().unwrap(),
            client,
            yawc::Options::default(),
        )
        .await
        .unwrap();
        let server = upgraded.recv().await.unwrap().await.unwrap();
        serve.await.unwrap();
        (WasmWsLink::new(client), server)
    }

    async fn verify_reads(link: &mut impl ZLinkRx, peer: &mut WebSocket) {
        for exact in [false, true] {
            for length in [0, 2, 4, 5] {
                let payload = vec![0x42; length];
                peer.send(FrameView::binary(payload.clone())).await.unwrap();
                let mut buffer = [0xa5; 4];
                let result = if exact {
                    link.read_exact(&mut buffer).await.map(|()| buffer.len())
                } else {
                    link.read(&mut buffer).await
                };
                if length > buffer.len() || (exact && length != buffer.len()) {
                    assert!(
                        result.is_err(),
                        "invalid datagram size {length}, exact={exact}"
                    );
                    assert_eq!(
                        buffer, [0xa5; 4],
                        "rejection must not expose a partial value"
                    );
                } else {
                    assert_eq!(result.unwrap(), length);
                    assert_eq!(&buffer[..length], &payload);
                    assert!(buffer[length..].iter().all(|byte| *byte == 0xa5));
                }
            }
        }
        peer.send(FrameView::text("not binary")).await.unwrap();
        assert!(link.read(&mut [0; 16]).await.is_err());
        peer.close().await.unwrap();
        assert!(link.read(&mut [0; 16]).await.is_err());
    }

    #[tokio::test]
    async fn whole_and_split_links_preserve_datagrams_or_refuse_them() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for split in [false, true] {
                let (mut link, mut peer) = pair().await;
                if split {
                    let (mut tx, mut rx) = link.split();
                    tx.write_all(&[1, 2, 3]).await.unwrap();
                    assert_eq!(&peer.next().await.unwrap().payload[..], &[1, 2, 3]);
                    verify_reads(&mut rx, &mut peer).await;
                } else {
                    link.write_all(&[1, 2, 3]).await.unwrap();
                    assert_eq!(&peer.next().await.unwrap().payload[..], &[1, 2, 3]);
                    verify_reads(&mut link, &mut peer).await;
                }
            }
        })
        .await
        .expect("WebSocket checks must finish without retries");
    }
}
