pub mod config;
pub mod observability;

pub mod buffered_http_request_capnp {
    include!(concat!(env!("OUT_DIR"), "/buffered_http_request_capnp.rs"));
}
