fn main() {
    ::capnpc::CompilerCommand::new()
        .file("buffered_http_request.capnp")
        .run()
        .expect("compiling schema");
}
