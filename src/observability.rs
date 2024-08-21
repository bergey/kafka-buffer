use anyhow::Result;
use std::time::Instant;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response};

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

pub async fn prometheus_metrics(_req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>> {
    let metrics = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();
    let s = encoder.encode_to_string(&metrics)?;
    Ok(Response::new(Full::<Bytes>::from(s)))
}
