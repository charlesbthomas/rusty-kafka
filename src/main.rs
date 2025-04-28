use std::time::Duration;

use clap::{value_t, App, Arg};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::info;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

mod handlers;
mod util;

// Generate the event router module
event_router::generate_router!();

pub fn register_event_handler(
    event_type: &'static str,
    handler: fn(
        &serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    router::register_handler(event_type, handler);
}

fn setup_logger(verbose: bool, log_conf: Option<&str>) {
    let log_level = if verbose { "debug" } else { "info" };
    let log_conf = log_conf.unwrap_or("rdkafka=info");
    env_logger::Builder::new()
        .parse_filters(&format!("{},{}", log_level, log_conf))
        .init();
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

    let mut stream = consumer.stream();
    while let Some(borrowed_message) = stream
        .try_next()
        .await
        .expect("failed to read next message from consumer")
    {
        info!("Received message: {:?}", borrowed_message);
        router::route_kafka_message(&borrowed_message).await;
        let key = borrowed_message
            .key()
            .expect("failed to get key")
            .to_owned();
        let payload = borrowed_message
            .payload()
            .expect("failed to get payload")
            .to_owned();
        producer
            .send(
                FutureRecord::to(&output_topic).key(&key).payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .expect("failed to send message to producer");
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

    // List all registered event handlers on startup
    router::list_registered_handlers();

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
