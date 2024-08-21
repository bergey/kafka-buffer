use kafka_buffer::observability::{self, hist_time_since};
use rdkafka::message::BorrowedMessage;

// use std::borrow::Borrow;
use std::env;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::*;
#[macro_use]
extern crate lazy_static;
use prometheus::{self, register_histogram, register_int_counter, Histogram, IntCounter};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use sidekiq::{create_redis_pool, Client, Job, JobOpts};

lazy_static! {
    static ref KAFKA_MESSAGE_RECEIVED: IntCounter =
        register_int_counter!("kafka_message_received", "number of messages read").unwrap();
    static ref JOBS_WRITTEN: IntCounter =
        register_int_counter!("jobs_written", "number of Sidekiq jobs written to Redis").unwrap();
    static ref REDIS_DURATION_S: Histogram =
        register_histogram!("redis_duration_s", "duration of writes to Redis queues").unwrap();
}

async fn write_sidekiq_job<'a>(
    consumer: &StreamConsumer,
    sidekiq_client: &Client,
    message: BorrowedMessage<'a>,
) -> anyhow::Result<()> {
    // TODO look up in config file
    let class = "kafka-buffer".to_string();
    let job_opts = JobOpts {
        queue: "kafka-job-queue".to_string(),
        ..Default::default()
    };

    KAFKA_MESSAGE_RECEIVED.inc();
    let r_body = message.payload().map_or(Ok("".to_string()), |bytes| {
        String::from_utf8(bytes.to_vec())
    });
    match r_body {
        Err(err) => {
            error!("could not decode body as utf-8, skipping err={}", err);
            Ok(())
        }
        Ok(body) => {
            let job = Job {
                class: class.clone(),
                args: vec![sidekiq::Value::String(body)],
                retry: job_opts.retry,
                queue: job_opts.queue.clone(),
                jid: job_opts.jid.clone(),
                created_at: job_opts.created_at,
                enqueued_at: job_opts.enqueued_at,
            };
            let start = Instant::now();
            let r_push = sidekiq_client.push_async(job).await;
            hist_time_since(&REDIS_DURATION_S, start);
            match r_push {
                Ok(_) => {
                    JOBS_WRITTEN.inc();
                    consumer.commit_consumer_state(CommitMode::Async)?;
                    Ok(())
                }
                Err(err) => {
                    error!("Sidekiq push failed: {}", err);
                    Ok(()) // no commit, try again on next recv?
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    observability::init()?;
    let kafka_url = env::var("KAFKA_URL").unwrap_or("localhost:9092".to_string());
    let topic = Box::leak(Box::new(
        env::var("TOPIC").unwrap_or("buffer-topic".to_string()),
    ));
    let metrics_address = env::var("METRICS_ADDRESS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 9000)));

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-buffer")
        .set("bootstrap.servers", &kafka_url)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[&topic])?;

    let redis_pool = create_redis_pool()?;
    let sidekiq_client = Client::new(redis_pool, Default::default());

    // Create the outer pipeline on the message stream.
    info!("Starting event loop");
    loop {
        let r_message = consumer.recv().await;
        match r_message {
            Err(err) => {
                error!("kafka read error: {}", err);
                break;
            }
            Ok(message) => match write_sidekiq_job(&consumer, &sidekiq_client, message).await {
                Ok(()) => (),
                Err(_) => break,
            }
        }
    }
    warn!("Stream processing terminated");
    Ok(())
}
