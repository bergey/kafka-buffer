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

fn validate(s: &str) -> Vec<String> {
    let mut errors = Vec::new();
    match ConfigParser::parse(Rule::config, s) {
        Err(err) => errors.push(format!("{:?} {} {}", err.line_col, err.variant.message(), err.line())),
        Ok(config) => {
            for rule in config.into_inner() {
                match rule.as_rule() {
                    Rule::rule => {
                        for term in rule.into_inner() {
                            match term.as_rule() {
                                Rule::string => (),
                                Rule::attribute_set => {
                                    let mut job_class = 0;
                                    let mut queue = 0;
                                    let mut topic = 0;
                                    for attr in term {
                                        match att.
                                    }
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
   errors 
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
