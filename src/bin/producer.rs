/// read from stdin, send each line to Kafka
use kafka::client::KafkaClient;
use kafka::producer::{Producer, Record, RequiredAcks};
use std::env;
use std::fmt::Write;
use std::time::Duration;

fn main() {
    // let port = env::var("PORT").unwrap_or("3003".to_string());
    // let listen_ip = env::var("LISTEN_IP").unwrap_or("0.0.0.0".to_string());
    let broker_url = env::var("BROKER_URL").unwrap_or("localhost:9092".to_string());
    let topic = env::var("TOPIC").unwrap_or("buffer-topic".to_string());

    let mut client = KafkaClient::new(vec![broker_url.clone()]);
    client.load_metadata_all().unwrap();
    println!("topics: {}", client.topics().names().map(|s| s.to_owned()).collect::<Vec<String>>().join(", "));

    let mut producer = Producer::from_hosts(vec![broker_url])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();

    let mut buf = String::with_capacity(2);
    for i in 0..10 {
        let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
        producer
            .send(&Record::from_value(&topic, buf.as_bytes()))
            .unwrap();
        buf.clear();
    }
}
