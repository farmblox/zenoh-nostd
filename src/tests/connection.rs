use std::{
    boxed::Box,
    cell::Cell,
    format,
    rc::Rc,
    string::{String, ToString},
    sync::mpsc::{self, Sender},
    thread,
    time::Duration as StdDuration,
};

use embassy_futures::join::join;
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
