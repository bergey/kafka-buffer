/// read from stdin, send each line to Kafka
use kafka_buffer::config::*;
use kafka_buffer::observability;
use kafka_buffer::observability::hist_time_since;

use anyhow::Context;
use std::env;
use std::time::{Duration, Instant};
use tracing::*;
#[macro_use]
extern crate lazy_static;
use prometheus::{self, register_histogram, register_int_counter, Histogram, IntCounter};

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
struct Config {
    kafka_url: String,
    request_max_size: usize,
    topics_map: Routes,
}

lazy_static! {
    static ref HTTP_REQUEST: IntCounter =
        register_int_counter!("http_request", "HTTP requests started").unwrap();
    // TODO use Labels?
    static ref HTTP_200: IntCounter =
        register_int_counter!("http_200", "HTTP 200 responses sent").unwrap();
    static ref HTTP_4xx: IntCounter =
        register_int_counter!("http_4xx", "HTTP 4xx responses sent").unwrap();
    static ref HTTP_5xx: IntCounter =
        register_int_counter!("http_5xx", "HTTP 5xx responses sent").unwrap();
    // TODO better buckets
    static ref KAFKA_DURATION_S: Histogram =
        register_histogram!("kafka_duration_s", "duration of write requests to Kafka").unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    observability::init()?;
    let listen: SocketAddr = env::var("LISTEN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 3000)));
    let metrics_address = env::var("METRICS_ADDRESS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 9000)));

    let config_file_name = env::var("CONFIG_FILE").unwrap_or(DEFAULT_CONFIG_FILE.to_string());
    let topics_map = parse_from_file(&config_file_name);
    let config: &'static Config = Box::leak(Box::new(Config {
        kafka_url: env::var("KAFKA_URL").unwrap_or("localhost:9092".to_string()),
        request_max_size: env::var("REQUEST_MAX_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2 ^ 20),
        topics_map,
    }));

    let write_to_kafka = move |req: Request<hyper::body::Incoming>| {
        async {
            let path = req.uri().path();
            match config.topics_map.0.get(path) {
                None => {
                    HTTP_4xx.inc();
                    Ok::<Response<Empty<Bytes>>, anyhow::Error>(
                        Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Empty::<Bytes>::new())?,
                    )
                }
                Some(route) => {
                    let body =
                        http_body_util::Limited::new(req.into_body(), config.request_max_size);
                    match body.collect().await {
                        Ok(all) => {
                            // Create the `FutureProducer` to produce asynchronously.
                            let producer: FutureProducer = ClientConfig::new()
                                .set("bootstrap.servers", &config.kafka_url)
                                .set("message.timeout.ms", "1000")
                                .create()?;

                            let mut v: Vec<u8> = Vec::new();
                            v.extend(all.to_bytes().as_ref());
                            let start = Instant::now();
                            let produce_future = producer.send(
                                FutureRecord::<(), [u8]>::to(&route.topic).payload(&v),
                                Duration::from_secs(0),
                            );
                            let r_delivery = produce_future.await;
                            hist_time_since(&KAFKA_DURATION_S, start);
                            match r_delivery {
                                Ok(delivery) => debug!("Sent: {:?}", delivery),
                                Err((e, _)) => error!("Error: {:?}", e),
                            }
                        }
                        Err(err) => error!("error: {:?}", err),
                    }
                    HTTP_200.inc();
                    debug!("served request topic={}", route.topic);
                    Ok::<Response<Empty<Bytes>>, anyhow::Error>(
                        Response::new(Empty::<Bytes>::new()),
                    )
                }
            }
        }
    };

    let tcp_listener = TcpListener::bind(listen).await.context("tcp_listener")?;
    let metrics_listener = TcpListener::bind(metrics_address).await.context("metrics_listener")?;

    loop {
        tokio::select! {
                r_stream = tcp_listener.accept() => match r_stream {
                    Err(err) => {
                        error!("http error: {}", err);
                        break;
                    }
                    Ok((stream, _)) => {
                        HTTP_REQUEST.inc();

                        // Use an adapter to access something implementing `tokio::io` traits as if they implement
                        // `hyper::rt` IO traits.
                        let io = TokioIo::new(stream);

                        // Spawn a tokio task to serve multiple connections concurrently
                        tokio::task::spawn(async move {
                            if let Err(err) = http1::Builder::new()
                                .serve_connection(io, service_fn(write_to_kafka))
                                .await
                            {
                                error!("Error serving connection: {:?}", err);
                            }
                        });
                }
            },
            r_stream = metrics_listener.accept() => match r_stream {
                Err(err) => {
                    error!("http metrics error: {}", err);
                    break;
                }
                Ok((stream, _)) => {
                    // Use an adapter to access something implementing `tokio::io` traits as if they implement
                    // `hyper::rt` IO traits.
                    let io = TokioIo::new(stream);

                    // Spawn a tokio task to serve multiple connections concurrently
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
        }
    }
    Ok(())
}
