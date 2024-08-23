use pest::Parser;
use pest_derive::Parser;
use std::collections::HashMap;

#[derive(Parser)]
#[grammar = "config.pest"]
pub struct ConfigParser;

#[derive(Clone, Debug)]
pub struct Route {
    pub job_class: String,
    pub queue: String,
    pub topic: String,
}

#[derive(Clone, Debug)]
pub struct Routes(pub HashMap<String, Route>);

pub const DEFAULT_CONFIG_FILE: &str = "kafka_buffer.config";

/// the Vec will never be empty
pub fn parse(s: &str) -> Result<Routes, Vec<String>> {
    let mut rules = HashMap::new();
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
                    return Err(errors);
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
                                    errors.push(format!(
                                        "{}:{} duplicate attribute job-class",
                                        line, col
                                    ))
                                }
                            },
                            "queue" => match queue {
                                None => queue = Some(value.as_str().to_owned()),
                                Some(_) => {
                                    let (line, col) = key.line_col();
                                    errors
                                        .push(format!("{}:{} duplicate attribute queue", line, col))
                                }
                            },
                            "topic" => match topic {
                                None => topic = Some(value.as_str().to_owned()),
                                Some(_) => {
                                    let (line, col) = key.line_col();
                                    errors
                                        .push(format!("{}:{} duplicate attribute topic", line, col))
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
                    }
                    if let (Some(c), Some(q)) = (class, queue) {
                        let topic = topic.unwrap_or(format!("{}__{}", q, c));
                        rules.insert(
                            path.into_inner().next().unwrap().as_str().to_owned(),
                            Route {
                                job_class: c,
                                queue: q,
                                topic,
                            },
                        );
                    }
                }
            }
            if let Some(_) = pairs.next() {
                errors.push(format!("config file should have only a single list"));
            }
        }
    }
    if errors.len() == 0 {
        Ok(Routes(rules))
    } else {
        Err(errors)
    }
}

/// print errors and exit, or return valid Routes
pub fn parse_from_file(config_file_name: &str) -> Routes {
    let contents = std::fs::read_to_string(config_file_name).unwrap();
    match parse(&contents) {
        Ok(routes) => routes,
        Err(errors) => {
            for err in errors {
                println!("{}", err);
            }
            std::process::exit(1);
        }
    }
}
