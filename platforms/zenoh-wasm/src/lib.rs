use yawc::WebSocket;
use zenoh_nostd::platform::*;

pub mod ws;

pub struct WasmLinkManager;

#[derive(ZLinkInfo, ZLinkTx, ZLinkRx, ZLink)]
#[zenoh(ZLink = (WasmLinkTx<'link>, WasmLinkRx<'link>))]
pub enum WasmLink {
    Ws(ws::WasmWsLink),
}

#[derive(ZLinkInfo, ZLinkTx)]
pub enum WasmLinkTx<'link> {
    Ws(ws::WasmWsLinkTx<'link>),
}

#[derive(ZLinkInfo, ZLinkRx)]
pub enum WasmLinkRx<'link> {
    Ws(ws::WasmWsLinkRx<'link>),
}

impl ZLinkManager for WasmLinkManager {
    type Link<'a>
        = WasmLink
    where
        Self: 'a;

    async fn connect(
        &self,
        endpoint: Endpoint<'_>,
    ) -> core::result::Result<Self::Link<'_>, LinkError> {
        let protocol = endpoint.protocol();
        let address = endpoint.address();

        // The address is passed through as written rather than parsed into a
        // `SocketAddr`. A browser resolves the host itself, so requiring a
        // numeric IP here would refuse every deployment that names one —
        // `ws/bridge.example.com:10000` is an ordinary endpoint, and on this
        // platform there is nothing to resolve it with anyway.
        //
        // `wss` is a distinct protocol rather than an option on `ws`: a page
        // served over HTTPS cannot open a plaintext socket at all, so the two
        // are not interchangeable at runtime.
        let scheme = match protocol.as_str() {
            "ws" => "ws",
            "wss" => "wss",
            _ => zenoh::zbail!(LinkError::CouldNotParseProtocol),
        };

        let url = format!("{}://{}", scheme, address.as_str());
        let socket = WebSocket::connect(url.parse().map_err(|_| LinkError::CouldNotConnect)?)
            .await
            .map_err(|_| LinkError::CouldNotConnect)?;

        Ok(Self::Link::Ws(ws::WasmWsLink::new(socket)))
    }

    async fn listen(&self, _: Endpoint<'_>) -> core::result::Result<Self::Link<'_>, LinkError> {
        zenoh::zbail!(LinkError::CouldNotListen)
    }
}
