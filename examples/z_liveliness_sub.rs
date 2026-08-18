//! Subscribe to liveliness tokens — the `Interest` protocol end to end.
//!
//! A liveliness token is not its own message family: it is a `Declare` a peer
//! only sends in answer to an `Interest` carrying `KEYEXPRS + TOKENS`.
//! `history` picks the mode — `CurrentFuture` for the tokens already alive plus
//! the ones to come, `Future` for only what happens next.
//!
//!   ENDPOINT=tcp/127.0.0.1:7447 \
//!   KEYEXPR='demo/group/*/item/*/owner/*' \
//!   cargo run --example z_liveliness_sub --features=std,log
#![cfg_attr(feature = "esp32s3", no_std)]
#![cfg_attr(feature = "esp32s3", no_main)]
#![cfg_attr(feature = "wasm", no_main)]

use zenoh_examples::*;
use zenoh_nostd::session::*;

async fn entry(spawner: embassy_executor::Spawner) -> zenoh::ZResult<()> {
    #[cfg(feature = "log")]
    env_logger::init();

    zenoh::info!("zenoh-nostd z_liveliness_sub example");

    // All channels that will be used must outlive `Resources`.
    // **Note**: as a direct implication, you may need to make static channels
    // if you want a `'static` session.
    use static_cell::StaticCell;
    static CHANNEL: StaticCell<
        embassy_sync::channel::Channel<
            embassy_sync::blocking_mutex::raw::NoopRawMutex,
            FixedCapacitySample<128, 128>,
            8,
        >,
    > = StaticCell::new();
    let channel: &'static _ = CHANNEL.init(embassy_sync::channel::Channel::new());

    let config = init_session_example(&spawner).await;
    let session = if LISTEN {
        zenoh::listen!(ExampleConfig: config, Endpoint::try_from(ENDPOINT)?)
    } else {
        zenoh::connect!(ExampleConfig: config, Endpoint::try_from(ENDPOINT)?)
    };

    let subscriber = session
        .declare_subscriber(zenoh::keyexpr::new(zenoh_examples::KEYEXPR)?)
        .channel(channel.dyn_sender(), channel.dyn_receiver())
        .finish()
        .await?;

    // The interest is what makes the peer send token declarations at all.
    // `history: true` asks for the tokens already alive as well as the ones to
    // come — the whole question when reading an ownership token.
    let _interest = session
        .declare_liveliness_interest(zenoh::keyexpr::new(zenoh_examples::KEYEXPR)?, true)
        .await?;
    zenoh::info!(
        "[Liveliness] interest declared on '{}'",
        zenoh_examples::KEYEXPR
    );

    embassy_futures::select::select(session.run(), async {
        while let Some(sample) = subscriber.recv().await {
            zenoh::info!(
                "[Liveliness] token alive at '{}' ({} bytes)",
                sample.keyexpr().as_str(),
                sample.payload().len()
            );
        }

        Ok::<(), Error>(())
    })
    .await;

    Ok(())
}

#[cfg_attr(feature = "std", embassy_executor::main)]
#[cfg_attr(feature = "wasm", embassy_executor::main)]
#[cfg_attr(feature = "esp32s3", esp_rtos::main)]

async fn main(spawner: embassy_executor::Spawner) {
    if let Err(e) = entry(spawner).await {
        zenoh::error!("Error in main: {}", e);
    }

    zenoh::info!("Exiting main");
}

#[cfg(feature = "esp32s3")]
mod esp32s3_app {
    use esp_hal::rng::Rng;
    pub use esp_println as _;
    use getrandom::{Error, register_custom_getrandom};

    #[panic_handler]
    fn panic(info: &core::panic::PanicInfo) -> ! {
        zenoh_nostd::session::zenoh::error!("Panic: {}", info);

        loop {}
    }

    extern crate alloc;

    esp_bootloader_esp_idf::esp_app_desc!();

    register_custom_getrandom!(getrandom_custom);
    pub fn getrandom_custom(bytes: &mut [u8]) -> Result<(), Error> {
        Rng::new().read(bytes);
        Ok(())
    }
}
