use kafka_buffer::config::*;
use kafka_buffer::observability::{self, hist_time_since};
use rdkafka::message::BorrowedMessage;
use kafka_buffer::buffered_http_request_capnp::buffered_request;

use serde_json::map::Map;
use anyhow::{anyhow, Context};
use prometheus::{self, register_histogram, register_int_counter, Histogram, IntCounter};
use std::env;
use std::time::Instant;
use tracing::*;
use capnp::message::ReaderOptions;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use sidekiq::{create_redis_pool, Client, Job, JobOpts};

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[macro_use]
extern crate lazy_static;

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
    route: &Route,
    message: BorrowedMessage<'a>,
) -> anyhow::Result<()> {
    let job_opts = JobOpts {
        queue: route.queue.clone(),
        ..Default::default()
    };

    KAFKA_MESSAGE_RECEIVED.inc();
    match decode_capnp_message(message.payload()) {
        Err(err) => {
            error!("skipping topic={} offset={} could not decode payload: {}", message.topic(), message.offset(), err);
            Ok(())
        }
        Ok(job_args) => {
            debug!("received topic={} job_args={:?}", message.topic(), job_args);
            let job = Job {
                class: route.job_class.clone(),
                args: job_args,
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
    let metrics_address = env::var("METRICS_ADDRESS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 9000)));
    let config_file_name = env::var("CONFIG_FILE").unwrap_or(DEFAULT_CONFIG_FILE.to_string());
    let topics_map = parse_from_file(&config_file_name).by_topic();

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-buffer")
        .set("bootstrap.servers", &kafka_url)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .context("kafka consumer")?;
    let topics: Vec<&str> = topics_map.keys().map(|x| &**x).collect();
    info!("subscribing to {:?}", topics);
    consumer.subscribe(&topics)?;

    let redis_pool = create_redis_pool().context("redis_pool")?;
    let sidekiq_client = Client::new(redis_pool, Default::default());
    let metrics_listener = TcpListener::bind(metrics_address)
        .await
        .context("metrics_listener")?;

    // Create the outer pipeline on the message stream.
    info!("Starting event loop");
    loop {
        tokio::select! {
            r_message = consumer.recv() => match r_message {
                Err(err) => {
                    error!("kafka read error: {}", err);
                    break;
                }
                Ok(message) => {
                    let route = topics_map.get(message.topic()).expect("message came from a topic we subscribed to");
                    match write_sidekiq_job(&consumer, &sidekiq_client, &route, message).await {
                        Ok(()) => (),
                        Err(_) => break,
                    }
                }
            },
            r_stream = metrics_listener.accept() => match r_stream {
                Err(err) => {
                    error!("http error: {}", err);
                    break;
                }
                Ok((stream, _)) => {
                    let io = TokioIo::new(stream);
                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .serve_connection(io, service_fn(observability::prometheus_metrics))
                            .await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
            }
        };
    }
    warn!("Stream processing terminated");
    Ok(())
}

fn decode_capnp_message(o_bytes: Option<&[u8]>) -> anyhow::Result<Vec<sidekiq::Value>> {
    let mut job_args = Vec::new();
    let bytes = o_bytes.ok_or(anyhow!("kafka message has no payload"))?;
    let reader = capnp::serialize::read_message(bytes, ReaderOptions::new())?;
    // let typed_reader = TypedReader::<_, buffered_request::Owned>::new(reader);
    // let buf_req = typed_reader.get()?;
    let buf_req = reader.get_root::<buffered_request::Reader>()?;
    let body = buf_req.get_body()?;
    job_args.push(sidekiq::Value::String(String::from_utf8(body.to_vec())?));
    let mut headers = Map::new();
    for h in buf_req.get_headers()? {
        let name = h.get_name()?.to_str()?.to_owned();
        let value = String::from_utf8(h.get_value()?.to_vec())?;
        headers.insert(name, serde_json::Value::String(value));
    }
    job_args.push(sidekiq::Value::Object(headers));
    Ok(job_args)
}
