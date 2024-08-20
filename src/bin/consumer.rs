use futures::TryStreamExt;
use std::env;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let kafka_url = env::var("KAFKA_URL").unwrap_or("localhost:9092".to_string());
    let topic = env::var("TOPIC").unwrap_or("buffer-topic".to_string());

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-buffer")
        .set("bootstrap.servers", &kafka_url)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[&topic])?;

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        async move {
            // Process each message
            println!("{:?}", borrowed_message);
            Ok(())
        }
    });

    println!("Starting event loop");
    stream_processor.await?;
    println!("Stream processing terminated");
    Ok(())
}
