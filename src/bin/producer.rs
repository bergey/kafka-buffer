/// read from stdin, send each line to Kafka
use kafka_buffer::config::*;
use kafka_buffer::observability;
use kafka_buffer::observability::hist_time_since;
pub mod buffered_http_request_capnp {
    include!(concat!(env!("OUT_DIR"), "/buffered_http_request_capnp.rs"));
}
use crate::buffered_http_request_capnp::buffered_request;

use anyhow::Context;
use std::env;
use std::time::Instant;
use tracing::*;
#[macro_use]
extern crate lazy_static;
use prometheus::{self, register_histogram, register_int_counter, Histogram, IntCounter};

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::header::{HeaderMap, HeaderName};
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
    static ref KAFKA_DURATION_S: Histogram =
        register_histogram!("kafka_duration_s", "duration of write requests to Kafka",
                            vec![0.005, 0.0075, 0.010, 0.032, 0.100, 0.316, 1.0]
).unwrap();
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
            .unwrap_or(1 << 20),
        topics_map,
    }));

    // Create the `FutureProducer` to produce asynchronously.
    let producer: &'static FutureProducer = Box::leak(Box::new(
        ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_url)
            .set("message.timeout.ms", "1000")
            .set("connections.max.idle.ms", "30000")
            .set("linger.ms", "10")
            .set("compression.codec", "lz4")
            .create()?,
    ));

    let write_to_kafka = |req: Request<hyper::body::Incoming>| async {
        let path = req.uri().path();
        match config.topics_map.0.get(path) {
            None => {
                HTTP_4xx.inc();
                // I'd like to know which unknown URLs are requested, but not flood our logs
                empty_http_response(StatusCode::NOT_FOUND)
            }
            Some(route) => {
                let (headers, body) = {
                    let (parts, body) = req.into_parts();
                    let body = http_body_util::Limited::new(body, config.request_max_size);
                    (parts.headers, body)
                };
                match body.collect().await {
                    Err(err) => {
                        error!("http error: {:?}", err);
                        HTTP_4xx.inc();
                        empty_http_response(StatusCode::BAD_REQUEST)
                    }
                    Ok(all) => {
                        let payload = encode_request(&all.to_bytes(), &headers, &route.headers);
                        let start = Instant::now();
                        match producer.send_result(FutureRecord::<(), [u8]>::to(&route.topic).payload(&payload))
                        {
                            Err(err) => {
                                warn!("could not enqueue in rdkafka: {:?}", err);
                                HTTP_4xx.inc();
                               empty_http_response(StatusCode::TOO_MANY_REQUESTS)
                            }
                            Ok(produce_future) => {
                                let r_delivery = produce_future.await;
                                hist_time_since(&KAFKA_DURATION_S, start);
                                match r_delivery {
                                    Err(_cancelled) => {
                                        warn!("kafka message canceled");
                                        HTTP_5xx.inc();
                                        empty_http_response(StatusCode::INTERNAL_SERVER_ERROR)
                                    }
                                    Ok(Err((e, _))) => {
                                        error!("Kafka Error: {:?}", e);
                                        HTTP_5xx.inc();
                                        empty_http_response(StatusCode::SERVICE_UNAVAILABLE)
                                    }
                                    Ok(Ok((partition, offset))) => {
                                        debug!("served request topic={} partition={partition} offset={offset}", route.topic);
                                        HTTP_200.inc();
                                        empty_http_response(StatusCode::OK)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    let tcp_listener = TcpListener::bind(listen)
        .await
        .context(format!("tcp_listener {}", listen))?;
    let metrics_listener = TcpListener::bind(metrics_address)
        .await
        .context(format!("metrics_listener {}", metrics_address))?;

    loop {
        tokio::select! {
                r_stream = tcp_listener.accept() => match r_stream {
                    Err(err) => {
                        error!("fatal http error: {}", err);
                        std::process::exit(101);
                    }
                    Ok((stream, _)) => {
                        HTTP_REQUEST.inc();

                        // Use an adapter to access something implementing `tokio::io` traits as if they implement
                        // `hyper::rt` IO traits.
                        let io = TokioIo::new(stream);

                        // Spawn a tokio task to serve multiple connections concurrently
                        tokio::task::spawn(async move {
                            if let Err(err) = http1::Builder::new()
                                .serve_connection(io, service_fn(&write_to_kafka))
                                .await
                            {
                                error!("Error serving connection: {:?}", err);
                            }
                        });
                }
            },
            r_stream = metrics_listener.accept() => match r_stream {
                Err(err) => {
                    error!("fatal http metrics error: {}", err);
                    std::process::exit(102);
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
}

fn empty_http_response(status_code: StatusCode) -> Result<Response<Empty<Bytes>>, anyhow::Error> {
    Ok(Response::builder()
        .status(status_code)
        .body(Empty::<Bytes>::new())?)
}

fn encode_request(body: &[u8], headers: &HeaderMap, want: &Vec<HeaderName>) -> Vec<u8> {
    let mut message = ::capnp::message::Builder::new_default();
    let mut req = message.init_root::<buffered_request::Builder>();
    req.set_body(body);
    let mut hh = req.reborrow().init_headers(want.len().try_into().unwrap());
    for (i, name) in want.iter().enumerate() {
        if let Some(value) = headers.get(name) {
            hh.reborrow().get(i as u32).set_name(name);
            hh.reborrow().get(i as u32).set_value(value.as_bytes());
        }
    }
    let mut ret = Vec::new();
    // docs say: If you pass in a writer that never returns an error, then this function will never return an error.
    let _ = capnp::serialize::write_message(&mut ret, &message);
    ret
}
