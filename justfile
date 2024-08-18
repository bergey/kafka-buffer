kafka: docker
    docker run -p 9092:9092 apache/kafka-native:3.8.0

docker:
    #!/usr/bin/env bash 
    if ! docker system info &> /dev/null; then
        open -a docker
        echo 'starting docker'
        while ! docker system info &> /dev/null; do
            sleep 1
        done
    fi

