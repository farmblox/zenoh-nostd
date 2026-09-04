use std::{
    boxed::Box,
    cell::{Cell, RefCell},
    format,
    rc::Rc,
    string::{String, ToString},
    sync::mpsc::{self, Sender},
    thread,
    time::Duration as StdDuration,
};

use embassy_futures::{
    join::join,
    select::{Either, select},
};
use embassy_time::{Duration, Instant, Timer};

use super::*;

#[test]
fn first_connection_waits_for_a_late_listener_inside_zenoh() {
    run_embassy_test(delayed_listener_round_trip);
}

#[test]
fn replacement_connection_replays_the_existing_subscriber() {
    run_embassy_test(declaration_replay_round_trip);
}

#[test]
fn reply_and_put_metadata_survive_the_encoded_session_path() {
    run_embassy_test(metadata_round_trip);
}

#[embassy_executor::task]
async fn delayed_listener_round_trip(result: Sender<Result<(), String>>) {
    let outcome = delayed_listener_round_trip_inner().await;
    let _ = result.send(outcome);
}

#[embassy_executor::task]
async fn declaration_replay_round_trip(result: Sender<Result<(), String>>) {
    let outcome = declaration_replay_round_trip_inner().await;
    let _ = result.send(outcome);
}

#[embassy_executor::task]
async fn metadata_round_trip(result: Sender<Result<(), String>>) {
    let outcome = metadata_round_trip_inner().await;
    let _ = result.send(outcome);
}

async fn delayed_listener_round_trip_inner() -> Result<(), String> {
    let address = unused_tcp_endpoint();
    let endpoint = Endpoint::try_from(address.as_str()).map_err(|error| error.to_string())?;
    let client_config = ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    };
    let server_config = ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    };
    let policy = ConnectionRetryPolicy {
        initial_delay: Duration::from_millis(5),
        maximum_delay: Duration::from_millis(10),
        increase_factor: 2,
    };
    let started = Instant::now();

    let client = zenoh::connect_retrying(&client_config, endpoint.clone(), policy);
    let server = async {
        Timer::after(Duration::from_millis(50)).await;
        zenoh::listen(&server_config, endpoint)
            .await
            .map_err(|error| error.to_string())
    };
    let (client, server) = join(client, server).await;
    let server = server?;

    if client.is_closed() || server.is_closed() {
        return Err("connection returned a closed session".into());
    }
    if started.elapsed() < Duration::from_millis(50) {
        return Err("client returned before the delayed listener opened".into());
    }
    Ok(())
}

async fn declaration_replay_round_trip_inner() -> Result<(), String> {
    let address = unused_tcp_endpoint();
    let endpoint = Endpoint::try_from(address.as_str()).map_err(|error| error.to_string())?;
    let client_config = ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    };
    let first_server_config = ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    };
    let second_server_config = ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    };
    let policy = ConnectionRetryPolicy {
        initial_delay: Duration::from_millis(5),
        maximum_delay: Duration::from_millis(10),
        increase_factor: 2,
    };
    let (client, first_server) = join(
        zenoh::connect_retrying(&client_config, endpoint.clone(), policy),
        zenoh::listen(&first_server_config, endpoint.clone()),
    )
    .await;
    let first_server = first_server.map_err(|error| error.to_string())?;

    let samples = Rc::new(Cell::new(0));
    let sample_count = Rc::clone(&samples);
    client
        .declare_subscriber(
            zenoh::keyexpr::new("test/reconnect").map_err(|error| error.to_string())?,
        )
        .callback_sync(move |_| sample_count.set(sample_count.get() + 1))
        .finish()
        .await
        .map_err(|error| error.to_string())?;

    let reconnected = Rc::new(Cell::new(false));
    let saw_reconnect = Rc::clone(&reconnected);
    let runner = client.run_reconnecting(policy, move |event| {
        if matches!(event, SessionEvent::Reconnected) {
            saw_reconnect.set(true);
        }
    });
    let exercise = async {
        first_server
            .put(
                zenoh::keyexpr::new("test/reconnect").map_err(|error| error.to_string())?,
                b"before",
            )
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        wait_for(|| samples.get() == 1, "initial subscriber sample").await?;
        first_server
            .close()
            .await
            .map_err(|error| error.to_string())?;

        let second_server = zenoh::listen(&second_server_config, endpoint)
            .await
            .map_err(|error| error.to_string())?;
        wait_for(|| reconnected.get(), "replacement transport").await?;
        second_server
            .put(
                zenoh::keyexpr::new("test/reconnect").map_err(|error| error.to_string())?,
                b"after",
            )
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        wait_for(|| samples.get() == 2, "replayed subscriber sample").await?;
        client.close().await.map_err(|error| error.to_string())
    };
    let (run_result, exercise_result) = join(runner, exercise).await;
    exercise_result?;
    run_result.map_err(|error| error.to_string())
}

async fn metadata_round_trip_inner() -> Result<(), String> {
    const LIVE_KEY: &str = "test/metadata/live";
    const QUERY_KEY: &str = "test/metadata/query";
    const LIVE_ATTACHMENT: &[u8] = b"live-causal-stamp";
    const REPLY_ATTACHMENT: &[u8] = b"reply-causal-stamp";

    let address = Box::leak(unused_tcp_endpoint().into_boxed_str());
    let endpoint = Endpoint::try_from(&*address).map_err(|error| error.to_string())?;
    let client_config = Box::leak(Box::new(ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    }));
    let server_config = Box::leak(Box::new(ExampleConfig {
        transports: TransportLinkManager::from(LinkManager),
    }));
    let policy = ConnectionRetryPolicy {
        initial_delay: Duration::from_millis(5),
        maximum_delay: Duration::from_millis(10),
        increase_factor: 2,
    };
    let (client, server) = join(
        zenoh::connect_retrying(client_config, endpoint.clone(), policy),
        zenoh::listen(server_config, endpoint),
    )
    .await;
    let client = Box::leak(Box::new(client));
    let server = Box::leak(Box::new(server.map_err(|error| error.to_string())?));

    let live = Rc::new(RefCell::new(None));
    let live_sink = Rc::clone(&live);
    let _subscriber = client
        .declare_subscriber(zenoh::keyexpr::new(LIVE_KEY).map_err(|error| error.to_string())?)
        .callback_sync(move |sample| {
            *live_sink.borrow_mut() = Some((
                sample.payload().to_vec(),
                sample.attachment().map(<[u8]>::to_vec),
            ));
        })
        .finish()
        .await
        .map_err(|error| error.to_string())?;
    let _queryable = server
        .declare_queryable(zenoh::keyexpr::new(QUERY_KEY).map_err(|error| error.to_string())?)
        .callback(async |query| {
            query
                .reply(query.keyexpr(), b"query-value", Some(REPLY_ATTACHMENT))
                .await
                .expect("send attached query reply");
        })
        .finish()
        .await
        .map_err(|error| error.to_string())?;

    let reply = Rc::new(RefCell::new(None));
    let reply_sink = Rc::clone(&reply);
    let exercise = async {
        server
            .put(
                zenoh::keyexpr::new(LIVE_KEY).map_err(|error| error.to_string())?,
                b"live-value",
            )
            .attachment(LIVE_ATTACHMENT)
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        let _responses = client
            .get(zenoh::keyexpr::new(QUERY_KEY).map_err(|error| error.to_string())?)
            .callback_sync(move |response| {
                if let GetResponse::Ok(sample) = response {
                    *reply_sink.borrow_mut() = Some((
                        sample.payload().to_vec(),
                        sample.attachment().map(<[u8]>::to_vec),
                    ));
                }
            })
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        wait_for(
            || live.borrow().is_some() && reply.borrow().is_some(),
            "attached live sample and query reply",
        )
        .await?;

        let live = live.borrow();
        let (payload, attachment) = live.as_ref().ok_or("live sample was not delivered")?;
        if payload != b"live-value" || attachment.as_deref() != Some(LIVE_ATTACHMENT) {
            return Err("live Put metadata changed across Session::run_inner".into());
        }
        let reply = reply.borrow();
        let (payload, attachment) = reply.as_ref().ok_or("query reply was not delivered")?;
        if payload != b"query-value" || attachment.as_deref() != Some(REPLY_ATTACHMENT) {
            return Err("Reply Put metadata changed across Session::run_inner".into());
        }
        Ok(())
    };
    match select(join(client.run(), server.run()), exercise).await {
        Either::First((client, server)) => Err(format!(
            "a metadata session ended before delivery: client={client:?}, server={server:?}"
        )),
        Either::Second(result) => result,
    }
}

async fn wait_for(predicate: impl Fn() -> bool, label: &str) -> Result<(), String> {
    for _ in 0..200 {
        if predicate() {
            return Ok(());
        }
        Timer::after(Duration::from_millis(5)).await;
    }
    Err(format!("timed out waiting for {label}"))
}

fn unused_tcp_endpoint() -> String {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve local test port");
    let port = listener.local_addr().expect("read local test port").port();
    drop(listener);
    format!("tcp/127.0.0.1:{port}")
}

fn run_embassy_test<S>(
    task: impl FnOnce(Sender<Result<(), String>>) -> embassy_executor::SpawnToken<S> + Send + 'static,
) where
    S: 'static,
{
    let (result_tx, result_rx) = mpsc::channel();
    thread::spawn(move || {
        let executor = Box::leak(Box::new(embassy_executor::Executor::new()));
        executor.run(|spawner| {
            spawner
                .spawn(task(result_tx))
                .expect("spawn connection test");
        });
    });

    result_rx
        .recv_timeout(StdDuration::from_secs(5))
        .expect("connection retry test timed out")
        .expect("connection retry test failed");
}
