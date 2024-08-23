kafka: docker
    docker run -d -p 9092:9092 apache/kafka-native:3.8.0 || true
    kafka_2.13-3.8.0/bin/kafka-topics.sh --create --topic  foo_topic --bootstrap-server localhost:9092 || true
    kafka_2.13-3.8.0/bin/kafka-topics.sh --create --topic  bar_queue_name__Bar --bootstrap-server localhost:9092 || true

redis: docker
    docker run -d -p 6379:6379 redis

docker:
    #!/usr/bin/env bash
    if ! docker system info &> /dev/null; then
        open -a docker
        echo 'starting docker'
        while ! docker system info &> /dev/null; do
            sleep 1
        done
    fi

test path='foo':
    <sample_data.txt xargs -I % curl -v http://localhost:3000/{{path}} -d %

producer:
    LOG_FORMAT=pretty RUST_LOG=debug METRICS_ADDRESS=0.0.0.0:9090 cargo run --bin producer

consumer:
   LOG_FORMAT=pretty RUST_LOG=debug METRICS_ADDRESS=0.0.0.0:9093 cargo run --bin consumer

# pop a job off the sidekiq queue
dequeue queue='foo_queue':
    redis-cli --raw rpop queue:{{queue}}
    redis-cli llen queue:{{queue}}
