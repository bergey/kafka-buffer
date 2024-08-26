use kafka_buffer::encode_request;

use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

use hyper::header::*;
use serde::{Deserialize, Serialize};
// use serde_json::Result;
use std::collections::HashMap;

const PAYLOAD_10: &str = "1234567890";
const PAYLOAD_100: &str = "abcdefghiklmnopqrstuvwxyzABCDEFGHIKLMNOPQRSTUVWXYZabcdefghiklmnopqrstuvwxyzABCDEFGHIKLMNOPQRSTUVWXYZ";

#[derive(Serialize, Deserialize)]
struct BufferedRequest {
    body: String,
    headers: HashMap<String, String>,
}

fn encode_json(body: &[u8], headers: &HeaderMap, want: &Vec<HeaderName>) -> anyhow::Result<Vec<u8>> {
    let mut hm: HashMap<String, String> = HashMap::new();
    for name in want {
        if let Some(value) = headers.get(name) {
            hm.insert(name.to_string(), value.to_str()?.to_owned());
        }
    }
    let br = BufferedRequest {
        body: String::from_utf8(body.to_vec())?,
        headers: hm,
    };
    Ok(serde_json::to_vec(&br)?)
}

fn criterion_benchmark(c: &mut Criterion) {
    let headers = {
        let mut hm = HeaderMap::new();
        let _ = hm.insert(HOST, HeaderValue::from_static("example.com"));
        let _ = hm.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let _ = hm.insert(ACCEPT_ENCODING, HeaderValue::from_static("utf-8"));
        let _ = hm.insert(USER_AGENT, HeaderValue::from_static("curl/8.7.1"));
        let _ = hm.insert(
            "x-forwarded-for",
            HeaderValue::from_static("127.0.0.1,8.8.8.8"),
        );
        hm
    };

    let payload_1k = {
        let mut s = String::new();
        for _ in 0..10 {
            s.push_str(PAYLOAD_100);
        }
        s
    };
    let payload_10k = {
        let mut s = String::new();
        for _ in 0..10 {
            s.push_str(&payload_1k);
        }
        s
    };

    c.bench_function("capnp encode 10", |b| {
        b.iter(|| {
            encode_request(
                black_box(PAYLOAD_10.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });
    c.bench_function("json encode 10", |b| {
        b.iter(|| {
            encode_json(
                black_box(PAYLOAD_10.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });

    c.bench_function("capnp encode 100", |b| {
        b.iter(|| {
            encode_request(
                black_box(PAYLOAD_100.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });
    c.bench_function("json encode 100", |b| {
        b.iter(|| {
            encode_json(
                black_box(PAYLOAD_100.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });

    c.bench_function("capnp encode 100 w/ headers", |b| {
        b.iter(|| {
            encode_request(
                black_box(PAYLOAD_100.as_bytes()),
                black_box(&headers),
                black_box(&vec![HOST, USER_AGENT]),
            )
        })
    });
    c.bench_function("json encode 100 w/ headers", |b| {
        b.iter(|| {
            encode_json(
                black_box(PAYLOAD_100.as_bytes()),
                black_box(&headers),
                black_box(&vec![HOST, USER_AGENT]),
            )
        })
    });

        c.bench_function("capnp encode 1k", |b| {
        b.iter(|| {
            encode_request(
                black_box(payload_1k.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });
    c.bench_function("json encode 1k", |b| {
        b.iter(|| {
            encode_json(
                black_box(payload_1k.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });

        c.bench_function("capnp encode 10k", |b| {
        b.iter(|| {
            encode_request(
                black_box(payload_10k.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });
    c.bench_function("json encode 10k", |b| {
        b.iter(|| {
            encode_json(
                black_box(payload_10k.as_bytes()),
                black_box(&HeaderMap::new()),
                black_box(&Vec::new()),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
