/// read from stdin, send each line to Kafka
use kafka::client::KafkaClient;
use kafka::producer::{Producer, Record, RequiredAcks};
use std::env;
use std::io;
use std::io::BufRead;
use std::time::Duration;
use anyhow::Result;

fn main() -> Result<()> {
    // let port = env::var("PORT").unwrap_or("3003".to_string());
    // let listen_ip = env::var("LISTEN_IP").unwrap_or("0.0.0.0".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or("localhost:9092".to_string());
    let topic = env::var("TOPIC").unwrap_or("buffer-topic".to_string());

    let mut client = KafkaClient::new(vec![broker_url.clone()]);
    client.load_metadata_all()?;
    println!(
        "topics: {}",
        client
            .topics()
            .names()
            .map(|s| s.to_owned())
            .collect::<Vec<String>>()
            .join(", ")
    );

    let mut producer = Producer::from_hosts(vec![broker_url])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();

    let stdin = io::stdin();
    let in_handle = stdin.lock();
    for r_line in in_handle.lines() {
        let line = r_line?;
        producer
            .send(&Record::from_value(&topic, line.as_bytes()))
            .unwrap();
    }
    Ok(())
}
