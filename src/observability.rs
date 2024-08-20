use anyhow::Result;
use std::time::Instant;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

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

pub async fn prometheus_metrics() -> String {
    let metrics = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();
    encoder
        .encode_to_string(&metrics)
        .expect("failed to encode metrics")
}
