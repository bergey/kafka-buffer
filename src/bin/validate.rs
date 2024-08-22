use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "config.pest"]
pub struct ConfigParser;

fn print_parse(r: Rule, s: &str) {
    let r_parse = ConfigParser::parse(r, s);
    println!("{:?}", r_parse);
    if let Ok(mut parse) = r_parse {
        if let Some(pair) = parse.next() {
            println!(
                "{:?} : {}",
                pair.as_rule(),
                pair.into_inner()
                    .map(|p| format!("{:?} {}", p.as_rule(), p.as_span().as_str()))
                    .collect::<Vec<String>>()
                    .join(" ~ ")
            )
        }
    }
}

fn print_parse_string(s: &str) {
    println!("{:?}", ConfigParser::parse(Rule::string, s));
}
fn main() {
    print_parse_string("\"hello\"");

    print_parse(Rule::pair, "(job-class . \"Foo\")");
    print_parse(Rule::pair, "(queue . \"sidekiq_q_name\")");
    print_parse(Rule::pair, r#"(topic . "kafka_t_name")"#);
    print_parse(Rule::ident, r#"queue"#);
    print_parse(Rule::ident, r#"job-class"#);
    print_parse(Rule::list, r#"(no space)"#);

    print_parse(
        Rule::list,
        r#"'(
            (job-class . "Namespace::Foo")
            (queue . "foo_queue")
            (topic . "foo_topic")
            )"#,
    );

    let unsuccessful_parse = ConfigParser::parse(Rule::string, "this is not a number");
    println!("{:?}", unsuccessful_parse);
}
