#[macro_use]
extern crate lazy_static;

use clap::Parser;
use http_body_util::Full;
use http::uri::Uri;
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper::Request;
use hyper_util::rt::TokioIo;
use prometheus::{self, Histogram, IntCounter};
use rand::prelude::*;
use rand_distr::{Distribution, Exp};
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::time::{sleep, sleep_until, Duration, Instant};
use tracing::*;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(long, short)]
    /// mean think time in seconds (exponentially distributed)
    think_time_s: Option<f64>,

    #[arg(long, short = 'u', default_value_t = 1)]
    num_users: u64,

    #[arg(long, default_value_t=("http://localhost:3000".parse().unwrap()))]
    url: Uri,

    #[arg(long)]
    /// stop after sending (approximately) this many messages
    stop_after: Option<u64>,

    #[arg(long)]
    /// time in seconds to ramp up to full number of users
    ramp_time: Option<f64>,

    #[arg(long, default_value_t)]
    /// time in seconds to wait after sending messages before exit
    sleep: f64,

    #[arg(long, default_value = "sample_data.txt")]
    /// file with one line per message to send
    data: String,
}

lazy_static! {
    static ref ROUND_TRIP_TIME: Histogram = prometheus::register_histogram!(
        "round_trip_time",
        "client-side time to persist one new line to server"
    )
    .unwrap();
    static ref RECEIVED_COUNT: IntCounter =
        prometheus::register_int_counter!("received_count", "number of responses received").unwrap();
    static ref SENT_COUNT: IntCounter =
        prometheus::register_int_counter!("sent_count", "number of lines sent to server").unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    kafka_buffer::observability::init()?;

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
    let mut conns = Vec::new();
    for j in joins {
        conns.push(j.await?);
        if conns.len() as u64 % (std::cmp::max(1, cli.num_users / 10)) == 0
            || conns.len() as u64 == cli.num_users
        {
            // approximate, we may have opened more if they complet out of order
            info!("opened {} connections", conns.len());
        } else {
            debug!("opened {} connections", conns.len());
        }
    }

    info!("starting to send webhooks");
    let messages: &'static Vec<String> = {
        let mut messages = Vec::new();
        for line in std::fs::read_to_string(&cli.data)?.lines() {
            messages.push(line.to_owned());
        }
        Box::leak(Box::new(messages))
    };
    let mut joins = Vec::new();
    for (i, conn) in conns.into_iter().enumerate() {
        joins.push(spawn(one_client(
            cli.clone(),
            conn,
            i,
            &messages,
        )));
    }
    for j in joins {
        let _ = j.await;
    }

    sleep(Duration::from_secs_f64(cli.sleep)).await;

    Ok(())
}

// TODO loop over lines in file, staggering by number of users
// so ideally we send N lines in parallel, then the next N

// https://hyper.rs/guides/1/client/basic/
async fn connect(url: Uri, user_id: usize) -> SendRequest<Full<Bytes>> {
    let host = url.host().expect("uri has no host");
    let port = url.port().map_or(80, |p| p.as_u16());

    let address = format!("{}:{}", host, port);
    loop {
        match connect_once(&address).await {
            Ok(sender) => break sender,
            Err(err) => {
                // no backoff, trust whoever runs the load test to adjust the ramp if necessary
                warn!("user {user_id}: failed to connect, will retry: {err}");
            }
        }
    }
}

async fn connect_once(address: &str) -> anyhow::Result<SendRequest<Full<Bytes>>> {
    let stream = TcpStream::connect(address).await?;
    let io = TokioIo::new(stream);
    let (sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });
    Ok(sender)
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

async fn one_client(
    cli: Cli,
    mut conn: SendRequest<Full<Bytes>>,
    user_id: usize,
    messages: &Vec<String>,
) -> anyhow::Result<()> {
    let print_every_n = match cli.think_time_s {
        None => cli.num_users * 10,
        Some(s) => std::cmp::max(cli.num_users, (cli.num_users as f64 / s) as u64),
    };

    let think_distribution: Option<Exp<f64>> = cli.think_time_s.map(|λ| Exp::new(λ)).transpose()?;
    let mut deadline = Instant::now();
    let authority = cli.url.authority().expect("url has hostname");
    let mut msg_i = user_id * messages.len() % (cli.num_users as usize);

    loop {
        // Create an HTTP request with an empty body and a HOST header
        let req = Request::builder()
            .method("POST")
            .uri(&cli.url)
            .header(hyper::header::HOST, authority.as_str())
            .body(Full::<Bytes>::from(messages[msg_i].clone()))?;
        msg_i += 1;

        SENT_COUNT.inc();

        let i = SENT_COUNT.get();
        if i % print_every_n == 0 && i > 0 {
            info!("sent {i} messages so far");
        }
        if let Some(n) = cli.stop_after {
            if i == n - 1 {
                info!("stopping after sending {n} messages");
                report_metrics_and_exit(0);
            }
        }

        let request_time = Instant::now();
        let _ = conn.send_request(req).await?;
        RECEIVED_COUNT.inc();
        let elapsed = Instant::now().duration_since(request_time);
        ROUND_TRIP_TIME.observe(elapsed.as_secs_f64());

        // read until deadline or we've drained the queue
        match think_distribution {
            Some(think) => {
                deadline += Duration::from_secs_f64(think.sample(&mut thread_rng()));
                sleep_until(deadline).await;
            }
            None => (),
        }
    }
}
