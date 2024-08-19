/// read from stdin, send each line to Kafka
use std::env;
use std::io;
use std::io::BufRead;
use std::time::Duration;
use anyhow::Result;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

#[tokio::main]
async fn main() -> Result<()> {
    // let port = env::var("PORT").unwrap_or("3003".to_string());
    // let listen_ip = env::var("LISTEN_IP").unwrap_or("0.0.0.0".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or("localhost:9092".to_string());
    let topic = env::var("TOPIC").unwrap_or("buffer-topic".to_string());

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker_url)
        .set("message.timeout.ms", "1000")
        .create()?;

    // no http server yet, so read messages from stdin
    let stdin = io::stdin();
    let in_handle = stdin.lock();
    for r_line in in_handle.lines() {
        let line = r_line?;

        let produce_future = producer.send(
            FutureRecord::<(), String>::to(&topic)
                // .key("some key")
                .payload(&line),
            Duration::from_secs(0),
        );
        match produce_future.await {
            Ok(delivery) => println!("Sent: {:?}", delivery),
            Err((e, _)) => println!("Error: {:?}", e),
        }
    }
    Ok(())
}
