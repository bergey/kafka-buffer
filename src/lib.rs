pub mod config;
pub mod observability;

use hyper::header::{HeaderMap, HeaderName};

pub mod buffered_http_request_capnp {
    include!(concat!(env!("OUT_DIR"), "/buffered_http_request_capnp.rs"));
}
use buffered_http_request_capnp::buffered_request;

pub fn encode_request(body: &[u8], headers: &HeaderMap, want: &Vec<HeaderName>) -> Vec<u8> {
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
