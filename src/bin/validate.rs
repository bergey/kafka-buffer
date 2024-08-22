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

fn validate(s: &str) -> Vec<String> {
    let mut errors = Vec::new();
    match ConfigParser::parse(Rule::config, s) {
        Err(err) => errors.push(format!(
            "{:?} {} >> {}",
            err.line_col,
            err.variant.message(),
            err.line()
        )),
        Ok(mut pairs) => {
            if let Some(config) = pairs.next() {
                if config.as_rule() != Rule::list {
                    let (line, col) = config.line_col();
                    errors.push(format!(
                        "{}:{} config must be a list, found <{:?}>",
                        line,
                        col,
                        config.as_rule()
                    ));
                    return errors;
                }
                for route in config.into_inner() {
                    if route.as_rule() != Rule::pair {
                        let (line, col) = route.line_col();
                        errors.push(format!(
                            "{}:{} each route must be a pair (path . attribute_set) found <{:?}>",
                            line,
                            col,
                            route.as_rule()
                        ));
                        continue;
                    }
                    let mut pairs = route.into_inner();
                    let path = pairs.next().unwrap(); // every Rule::pair has two children
                    let attr_set = pairs.next().unwrap();
                    if path.as_rule() != Rule::string {
                        let (line, col) = path.line_col();
                        errors.push(format!(
                            "{}: {} each route must begin with a string url path.  found <{:?}>",
                            line,
                            col,
                            path.as_rule()
                        ));
                    }
                    if attr_set.as_rule() != Rule::list {
                        let (line, col) = attr_set.line_col();
                        errors.push(format!("{}: {} each route must end with an attribute set (a list of pairs).  found <{:?}>", line, col, attr_set.as_rule()));
                        continue;
                    }
                    let mut class: Option<String> = None;
                    let mut queue: Option<String> = None;
                    let mut topic: Option<String> = None;
                    for attr in attr_set.into_inner() {
                        if attr.as_rule() != Rule::pair {
                            let (line, col) = attr.line_col();
                            errors.push(format!(
                                "{}:{} each attribute must be a pair (key . \"value\"). found <{:?}>",
                                line,
                                col,
                                attr.as_rule()
                            ));
                            continue;
                        }
                        let mut pairs = attr.into_inner();
                        let key = pairs.next().unwrap(); // every Rule::pair has two children
                        let value = pairs.next().unwrap();
                        if key.as_rule() != Rule::ident {
                            let (line, col) = key.line_col();
                            errors.push(format!(
                            "{}: {} each attribute must begin with an unquoted key.  found <{:?}>",
                            line,
                            col,
                            key.as_rule()
                        ));
                        }
                        if value.as_rule() != Rule::string {
                            let (line, col) = key.line_col();
                            errors.push(format!(
                                "{}: {} each attribute must end with a string.  found <{:?}>",
                                line,
                                col,
                                value.as_rule()
                            ));
                        }
                        match key.as_str() {
                            "job-class" => match class {
                                None => class = Some(value.as_str().to_owned()),
                                Some(_) => {
                                    let (line, col) = key.line_col();
                                    errors.push(format!("{}:{} duplicate attribute job-class"))
                                }
                            },
                            "queue" => match queue {
                                None => queue = Some(value.as_str().to_owned()),
                                Some(_) => {
                                    let (line, col) = key.line_col();
                                    errors.push(format!("{}:{} duplicate attribute queue"))
                                }
                            },
                            "topic" => match topic {
                                None => topic = Some(value.as_str().to_owned()),
                                Some(_) => {
                                    let (line, col) = key.line_col();
                                    errors.push(format!("{}:{} duplicate attribute topic"))
                                }
                            },
                            k => {
                                let (line, col) = key.line_col();
                                errors.push(format!(
                                    "{}:{} valid attributes are job-class, queue, topic.  got {}",
                                    line, col, k
                                ));
                            }
                        }
                        // TODO use parsed attributes
                    }
                }
            }
            if let Some(_) = pairs.next() {
                errors.push(format!("config file should have only a single list"));
            }
        }
    }
    errors
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next(); // path to executable
    match args.next() {
        None => {
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
        Some(filename) => {
            let contents = std::fs::read_to_string(filename).unwrap();
            let errors = validate(&contents);
            if errors.len() > 0 {
                for err in errors {
                    println!("{}", err);
                }
                std::process::exit(1);
            }
        }
    }
}
