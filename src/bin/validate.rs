use kafka_buffer::config::{parse_from_file, DEFAULT_CONFIG_FILE};

fn main() {
    let mut args = std::env::args();
    let _ = args.next(); // path to executable
    let config_file_name = args.next().unwrap_or(DEFAULT_CONFIG_FILE.to_string());
    let _ = parse_from_file(&config_file_name);
}
