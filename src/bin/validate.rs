use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "config.pest"]
pub struct ConfigParser;

fn print_parse(r: Rule, s: &str) {
    println!("{:?}", ConfigParser::parse(r, s));
}

fn print_parse_string(s: &str) {
    println!("{:?}", ConfigParser::parse(Rule::string, s));
}

fn main() {
    print_parse_string("\"hello\"");

    print_parse(Rule::attribute, "(job-class . \"Foo\")");
    print_parse(Rule::attribute, "(queue . \"sidekiq_q_name\")");
    print_parse(Rule::attribute, r#"(topic . "kafka_t_name")"#);

    print_parse(Rule::attribute_set, r#"'(
            (job-class . "Namespace::Foo")
            (queue . "foo_queue")
            (topic . "foo_topic")
            )"#);
    

    let unsuccessful_parse = ConfigParser::parse(Rule::string, "this is not a number");
    println!("{:?}", unsuccessful_parse);
}
