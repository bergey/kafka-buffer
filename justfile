kafka: docker
    docker run -d -p 9092:9092 apache/kafka-native:3.8.0
    kafka_2.13-3.8.0/bin/kafka-topics.sh --create --topic  foo --bootstrap-server localhost:9092
    kafka_2.13-3.8.0/bin/kafka-topics.sh --create --topic  bar --bootstrap-server localhost:9092

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

test:
    <sample_data.txt xargs -I % curl -v http://localhost:3000/foo -d "'"%"'"

producer:
    LOG_FORMAT=pretty RUST_LOG=debug METRICS_ADDRESS=0.0.0.0:9090 cargo run --bin producer

consumer:
   LOG_FORMAT=pretty RUST_LOG=debug METRICS_ADDRESS=0.0.0.0:9093 cargo run --bin consumer
