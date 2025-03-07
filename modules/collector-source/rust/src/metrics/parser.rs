use chrono::prelude::*;
use nom::branch::alt;
use nom::bytes::complete::{
    is_not,
    tag,
    take_while,
};
use nom::character::complete::{
    char,
    digit1,
    line_ending,
    not_line_ending,
    one_of,
    space0,
    space1,
};
use nom::character::is_alphanumeric;
use nom::combinator::{
    map,
    opt,
    recognize,
};
use nom::error::Error as NomError;
use nom::multi::{
    many0,
    many1,
};
use nom::sequence::{
    delimited,
    preceded,
    separated_pair,
    terminated,
    tuple,
};
use nom::IResult;

use crate::core::{
    Error,
    Result,
};
use crate::util::*;

#[derive(Clone, Debug)]
pub struct Metric {
    pub name: String,
    pub labels: FnvMap<String, String>,
    pub value: f64,
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum MetricLine {
    Comment(String),
    Metric(Metric),
    Ignored,
}

/// Parses Metrics from raw metric format.
///
/// metric_name ["{" label_name "=" `"` label_value `"` { "," label_name"=" `"`
/// label_value `"` } [ "," ] "}"] value [ timestamp ]
///
/// In the sample syntax:
/// - metric_name and label_name carry the usual Prometheus expression language
///   restrictions.
/// - label_value can be any sequence of UTF-8 characters, but the backslash
///   (\), double-quote ("), and line feed (\n) characters have to be escaped as
///   \\, \", and \n, respectively.
/// - value is a float represented as required by Go's ParseFloat() function. In
///   addition to standard numerical values, NaN, +Inf, and -Inf are valid
///   values representing not a number, positive infinity, and negative
///   infinity, respectively.
/// - The timestamp is an int64 (milliseconds since epoch, i.e. 1970-01-01
///   00:00:00 UTC, excluding leap seconds), represented as required by Go's
///   ParseInt() function.
pub struct MetricParser<'a> {
    metrics: &'a FnvSet<String>,
}

impl<'a> MetricParser<'a> {
    /// Creates a new `MetricParser` provided the metrics whitelist.
    pub fn new(metrics: &'a FnvSet<String>) -> Self {
        Self { metrics }
    }

    /// Parses the variants of the raw metrics format into a `Vec` of
    /// `MetricLine` values.
    pub fn parse(&self, input: &'a str) -> Result<Vec<MetricLine>> {
        let metrics = self.metrics;

        // helper for checking whether an utf8 value is alpha, numeric, or _
        let is_alphanum_underscore = |c: u8| -> bool { is_alphanumeric(c) || c == b'_' };

        // helper for checking whether a char is alpha, numeric, or _
        let is_alphanum_underscore_char = |c: char| -> bool { is_alphanum_underscore(c as u8) };

        // string
        let string_value = |s: &'a str| -> IResult<&str, &str> {
            alt((
                delimited(
                    tag::<&str, &str, NomError<&str>>("\""),
                    is_not("\""),
                    tag("\""),
                ),
                tag("\"\""),
            ))(s)
        };

        // decimal number parser
        let decimal = |s: &'a str| -> IResult<&str, &str> {
            recognize(many1(terminated(one_of("0123456789"), many0(char('_')))))(s)
        };

        // floating point number parser
        let float = |input: &'a str| -> IResult<&str, &str> {
            // TODO: Handle NaN, +Inf, -Inf

            // Case one: .42
            let dot_value = recognize(tuple((
                char('.'),
                decimal,
                opt(tuple((one_of("eE"), opt(one_of("+-")), decimal))),
            )));

            // Case two: 42e42 and 42.42e42
            let exp = recognize(tuple((
                decimal,
                opt(preceded(char('.'), decimal)),
                one_of("eE"),
                opt(one_of("+-")),
                decimal,
            )));

            // Case three: 42. and 42.42
            let deci_deci = recognize(tuple((decimal, char('.'), opt(decimal))));

            alt((dot_value, exp, deci_deci))(input)
        };

        // metric value is either a decimal value or integer, parsed as an f64
        let metric_value = |s: &'a str| -> IResult<&str, f64> {
            map(
                preceded(space1::<&str, NomError<&str>>, alt((float, decimal))),
                |s| -> f64 { s.parse::<f64>().unwrap() },
            )(s)
        };

        // comment: starts with #
        let comment = |s: &'a str| -> IResult<&str, MetricLine> {
            map(
                preceded(
                    tag::<&str, &str, NomError<&str>>("#"),
                    terminated(not_line_ending, line_ending),
                ),
                |input| MetricLine::Comment(String::from(input)),
            )(s)
        };

        // label pairs: label="value",label2="value2",etc...
        let label_pairs = |s: &'a str| -> IResult<&str, FnvMap<String, String>> {
            let label_value = map(
                tuple((
                    opt(tag(",")),
                    separated_pair(
                        take_while(is_alphanum_underscore_char),
                        char('='),
                        string_value,
                    ),
                    opt(tag(",")),
                )),
                |o| (String::from(o.1 .0), String::from(o.1 .1)),
            );

            map(many0(label_value), |tuples| -> FnvMap<String, String> {
                tuples.into_iter().collect()
            })(s)
        };

        // label list: { <label_pairs> }
        let label_list = |s: &'a str| -> IResult<&str, FnvMap<String, String>> {
            delimited(char('{'), label_pairs, char('}'))(s)
        };

        let metric_name =
            |s: &'a str| -> IResult<&str, &str> { take_while(is_alphanum_underscore_char)(s) };

        // epoch timestamp: milliseconds
        let epoch_timestamp = |s: &'a str| -> IResult<&str, DateTime<Utc>> {
            map(
                preceded(space0::<&str, NomError<&str>>, digit1),
                |s| -> DateTime<Utc> {
                    let epoch = s.parse::<i64>().unwrap();
                    Utc.timestamp_millis_opt(epoch).unwrap()
                },
            )(s)
        };

        // full metric format
        let metric = |s: &'a str| -> IResult<&str, MetricLine> {
            let metric_line = terminated(
                tuple((
                    metric_name,
                    opt(label_list),
                    metric_value,
                    opt(epoch_timestamp),
                )),
                line_ending,
            );

            map(metric_line, |ml| {
                let name = String::from(ml.0);
                if !metrics.contains(&name) {
                    return MetricLine::Ignored;
                }

                MetricLine::Metric(Metric {
                    name,
                    labels: ml.1.unwrap_or_default(),
                    value: ml.2,
                    timestamp: ml.3,
                })
            })(s)
        };

        let parse_result = many0(alt((comment, metric)))(input);

        match parse_result {
            Ok(result) => Ok(result.1),
            Err(e) => Err(Error::Parse(e.to_string())),
        }
    }
}
