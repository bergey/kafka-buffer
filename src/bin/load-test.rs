#[macro_use]
extern crate lazy_static;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use prometheus::{self, Histogram, IntCounter};
use rand::prelude::*;
use rand_distr::{Distribution, Exp};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{sleep_until, Duration, Instant, sleep};
use tokio::{spawn, sync::mpsc};
use url;
use url::Url;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(long, short)]
    /// mean think time in seconds (exponentially distributed)
    think_time_s: Option<f64>,

    #[arg(long, short = 'u', default_value_t = 1)]
    num_users: u64,

    #[arg(long, default_value_t=Url::parse("http://localhost:3000").unwrap())]
    url: Url,

    #[arg(long)]
    /// stop after sending (approximately) this many messages
    stop_after: Option<u64>,

    #[arg(long)]
    /// time in seconds to ramp up to full number of users
    ramp_time: Option<f64>,

    #[arg(long, default_value_t)]
    /// time in seconds to wait after sending messages before exit
    sleep: f64
}

impl Cli {
    // this is very generous; in case the server totally fails to ack messages
    fn rw_channel_size(&self) -> usize {
        match self.stop_after {
            Some(num_msgs) => (num_msgs / self.num_users) as usize,
            None => 100,
        }
    }
}

lazy_static! {
    static ref ROUND_TRIP_TIME: Histogram = prometheus::register_histogram!(
        "round_trip_time",
        "client-side time to persist one new line to server"
    )
    .unwrap();
    static ref RECEIVED_COUNT: IntCounter =
        prometheus::register_int_counter!("received_count", "number of ws messages received").unwrap();
    // undecided whether to use this in place of the raw Atomic64 counter
    static ref SENT_COUNT: IntCounter =
        prometheus::register_int_counter!("sent_count", "number of lines sent to server").unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    env_logger::init();

    let count = Arc::new(AtomicU64::new(0));

    match cli.ramp_time {
        Some(ramp_s) => info!(
            "opening {} connections at rate {} per second",
            cli.num_users,
            cli.num_users as f64 / ramp_s
        ),
        None => info!(
            "opening {} connections as quickly as possible",
            cli.num_users
        ),
    }
    let mut joins = Vec::new();
    let mut time = Instant::now();
    for i in 1..=cli.num_users as usize {
        // TODO non-WS HTTP connections
        let ws = spawn(connect(cli.url.clone(), i));
        joins.push(ws);
        debug!("started {i} connection attempts");
        if let Some(ramp_s) = cli.ramp_time {
            time += Duration::from_secs_f64(ramp_s / cli.num_users as f64);
            sleep_until(time).await;
        }
    }
    let mut wss = Vec::new();
    for j in joins {
        wss.push(j.await?);
        if wss.len() as u64 % (std::cmp::max(1, cli.num_users / 10)) == 0 || wss.len() as u64 == cli.num_users {
            // approximate, we may have opened more if they complet out of order
            info!("opened {} connections", wss.len());
        } else {
            debug!("opened {} connections", wss.len());
        }
    }

    info!("starting to send lines");
    let mut joins = Vec::new();
    for (i, ws) in wss.into_iter().enumerate() {
        // TODO
        // joins.push(spawn(draw_random_lines(cli.clone(), ws, count.clone(), i)));
    }
    for j in joins {
        let _ = j.await;
    }

    sleep(Duration::from_secs_f64(cli.sleep)).await;

    Ok(())
}

// TODO loop over lines in file, staggering by number of users
// so ideally we send N lines in parallel, then the next N

// TODO non-WS HTTP connections
async fn connect(url: Url, user_id: usize) -> WSS {
    loop {
        match connect_async(url.clone()).await {
            Ok((ws, _)) => break ws,
            Err(err) => {
                // no backoff, trust whoever runs the load test to adjust the ramp if necessary
                warn!("user {user_id}: failed to connect, will retry: {err}");
            }
        }
    }
}

fn report_metrics_and_exit(exit_code: i32) {
    let metrics = prometheus::gather();
    let report = prometheus::TextEncoder::new()
        .encode_to_string(&metrics)
        .expect("failed to encode metrics");
    info!("{report}");
    // TODO graceful WS close
    std::process::exit(exit_code);
}
