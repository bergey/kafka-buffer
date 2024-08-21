use anyhow::Result;
use std::time::Instant;
use tracing::*;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};

pub fn init() -> Result<()> {
    if std::env::var("LOG_FORMAT") == Ok("pretty".to_string()) {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(fmt::layer().json().flatten_event(true))
            .with(EnvFilter::from_default_env())
            .init();
    }
    Ok(())
}

pub fn hist_time_since(hist: &prometheus::Histogram, start: Instant) {
    let elapsed = Instant::now() - start;
    hist.observe(elapsed.as_secs_f64());
}

pub async fn prometheus_metrics(
    req: Request<hyper::body::Incoming>,
) -> anyhow::Result<Response<Full<Bytes>>> {
    match req.uri().path() {
        "/metrics" => {
            let metrics = prometheus::gather();
            let encoder = prometheus::TextEncoder::new();
            let s = encoder.encode_to_string(&metrics)?;
            Ok(Response::new(Full::<Bytes>::from(s)))
        }
        path => {
            warn!("expected /metrics request to {}", path);
            Ok::<Response<Full<Bytes>>, anyhow::Error>(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::<Bytes>::from(""))?,
            )
        }
    }
}
