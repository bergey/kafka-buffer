use kafka_buffer::observability;

use futures::TryStreamExt;
use std::env;
use tracing::*;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use sidekiq::{create_redis_pool, Client, Job, JobOpts};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    observability::init()?;
    let kafka_url = env::var("KAFKA_URL").unwrap_or("localhost:9092".to_string());
    let topic = Box::leak(Box::new(
        env::var("TOPIC").unwrap_or("buffer-topic".to_string()),
    ));

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

    // TODO look up in config file
    let class = "kafka-buffer".to_string();
    let job_opts = JobOpts {
        queue: "kafka-job-queue".to_string(),
        ..Default::default()
    };
    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let r_body = borrowed_message
            .payload()
            .map_or(Ok("".to_string()), |bytes| {
                String::from_utf8(bytes.to_vec())
            });
        async {
            match r_body {
                Err(err) => error!(
                    "could not decode body as utf-8, skipping topic={} err={}",
                    topic, err
                ),
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
                    match sidekiq_client.push_async(job).await {
                        Ok(_) => (),
                        Err(err) => error!("Sidekiq push failed: {}", err),
                    }
                }
            }
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await?;
    warn!("Stream processing terminated");
    Ok(())
}
