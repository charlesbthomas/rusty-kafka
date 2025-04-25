use std::thread;
use std::time::Duration;

// https://crates.io/crates/async-channel for a
// Go-style multi-producer, multi-consumer channel.
use async_channel;

use clap::{value_t, App, Arg};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::info;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

fn setup_logger(verbose: bool, log_conf: Option<&str>) {
    let log_level = if verbose { "debug" } else { "info" };
    let log_conf = log_conf.unwrap_or("rdkafka=info");
    env_logger::Builder::new()
        .parse_filters(&format!("{},{}", log_level, log_conf))
        .init();
}

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    // Like `record_borrowed_message_receipt`, but takes an `OwnedMessage`
    // instead, as in a real-world use case  an `OwnedMessage` might be more
    // convenient than a `BorrowedMessage`.
}

// Emulates an expensive, synchronous computation.
fn expensive_computation(msg: OwnedMessage) -> String {
    info!("Starting expensive computation on message {}", msg.offset());
    thread::sleep(Duration::from_millis(100));
    info!(
        "Expensive computation completed on message {}",
        msg.offset()
    );
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => format!("Payload len for {} is {}", payload, payload.len()),
        Some(Err(_)) => "Message payload is not a string".to_owned(),
        None => "No payload".to_owned(),
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    output_topic: String,
) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    info!("Starting event loop");

    let (tx, rx) = async_channel::bounded::<OwnedMessage>(1); // Equivalent to a Go unbuffered channel.

    // We can have at most N expensive computations in flight.
    const N: usize = 1;
    for _ in 1..=N {
        // `tokio::spawn()` returns a `JoinHandle` that lets us
        // abort the spawned task, or wait for it to return.
        // Since our task just receives messages from the channel forever,
        // and doesn't return a value, we can discard the `JoinHandle`.
        //
        // Further, since the Tokio runtime is responsible for
        // running the future we pass it to completion, we don't need
        // to `.await` the `JoinHandle`.
        tokio::spawn({
            // Each task gets owning references to the receiver and producer.
            // Under the hood, both of these types might hold shared state in
            // an `Arc<Mutex<...>>`, but that's abstracted from us here.
            //
            // (Semantically, `.clone()` means "give me a dupe of this thing,"
            // not necessarily "give me a copy of this thing's bytes".
            // Depending on the semantics of the object, cloning might copy
            // its bytes, or it might bump a reference count).
            let rx = rx.clone();
            let producer = producer.clone();
            let output_topic = output_topic.clone();

            async move {
                // When the sender is closed, `rx.await()` will return an `Err`,
                // which will exit this loop.
                while let Ok(owned_message) = rx.recv().await {
                    let key = owned_message.key().expect("failed to get key").to_owned();
                    let computation_result =
                        tokio::task::spawn_blocking(|| expensive_computation(owned_message))
                            .await
                            .expect("failed to wait for expensive computation");
                    let produce_future = producer.send(
                        FutureRecord::to(&output_topic)
                            .key(&key)
                            .payload(&computation_result),
                        Duration::from_secs(0),
                    );
                    match produce_future.await {
                        Ok(delivery) => println!("Sent: {:?}", delivery),
                        Err((e, _)) => println!("Error: {:?}", e),
                    }
                }
            }
        });
    }

    let mut stream = consumer.stream();
    while let Some(borrowed_message) = stream
        .try_next()
        .await
        .expect("failed to read next message from consumer")
    {
        // Process each message
        record_borrowed_message_receipt(&borrowed_message).await;
        // Borrowed messages can't outlive the consumer they are received from, so they need to
        // be owned in order to be sent to a separate thread.
        let owned_message = borrowed_message.detach();
        record_owned_message_receipt(&owned_message).await;

        info!(
            "Sending message to channel, offset: {}, partition: {}",
            owned_message.offset(),
            owned_message.partition()
        );
        tx.send(owned_message)
            .await
            .expect("failed to send message to channel");
    }

    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value("1"),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let num_workers = value_t!(matches, "num-workers", usize).unwrap();

    (0..num_workers)
        .map(|_| {
            tokio::spawn(run_async_processor(
                brokers.to_owned(),
                group_id.to_owned(),
                input_topic.to_owned(),
                output_topic.to_owned(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async {})
        .await
}
