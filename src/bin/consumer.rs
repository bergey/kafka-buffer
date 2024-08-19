use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::env;

fn main() {
    let broker_url = env::var("BROKER_URL").unwrap_or("localhost:9092".to_string());
    let topic = env::var("TOPIC").unwrap_or("buffer-topic".to_string());

    let mut consumer = Consumer::from_hosts(vec![broker_url])
        .with_topic_partitions(topic, &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my-group".to_owned())
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .unwrap();
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                println!("{:?}", m);
            }
            consumer.consume_messageset(ms).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}
