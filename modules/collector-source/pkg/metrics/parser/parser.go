package parser

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// MetricRecord is the definition of a single metric instance in time.
type MetricRecord struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp *time.Time
}

// Parse reads the input reader containing the raw metric format, and returns a slice of MetricRecord instances
// containing the data parsed from the input.
func Parse(reader io.Reader) ([]*MetricRecord, error) {
	return newParser(reader).parse()
}

// Parses Metrics from raw metric format.
//
// metric_name ["{" label_name "=" `"` label_value `"` { "," label_name"=" `"` label_value `"` } [ "," ] "}"] value [ timestamp ]
//
// In the sample syntax:
//   - metric_name and label_name carry the usual Prometheus expression language
//     restrictions.
//   - label_value can be any sequence of UTF-8 characters, but the backslash
//     (\), double-quote ("), and line feed (\n) characters have to be escaped as
//     \\, \", and \n, respectively.
//   - value is a float represented as required by Go's ParseFloat() function. In
//     addition to standard numerical values, NaN, +Inf, and -Inf are valid
//     values representing not a number, positive infinity, and negative
//     infinity, respectively.
//   - The timestamp is an int64 (milliseconds since epoch, i.e. 1970-01-01
//     00:00:00 UTC, excluding leap seconds), represented as required by Go's
//     ParseInt() function.
type parser struct {
	lex     *lexer
	current token
}

// creates a new parser, which is meant to be used once and discarded
func newParser(r io.Reader) *parser {
	return &parser{
		lex: newLexer(r),
	}
}

func (p *parser) advance() token {
	p.current = p.lex.next()
	return p.current
}

func (p *parser) parse() ([]*MetricRecord, error) {
	var metrics []*MetricRecord

	p.advance()
	for {
		if p.current.Type == Eof {
			break
		}

		if p.current.Type == Comment {
			p.advance()
			continue
		}

		metric, err := p.parseMetric()
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, metric)
	}

	if len(p.lex.errors) > 0 {
		var sb strings.Builder
		for _, err := range p.lex.errors {
			sb.WriteString(" * ")
			sb.WriteString(err.Error())
			sb.WriteRune('\n')
		}

		return nil, fmt.Errorf("lexical errors: \n%s", sb.String())
	}

	return metrics, nil
}

func (p *parser) parseMetric() (*MetricRecord, error) {
	// expected to be advanced to current token
	if p.current.Type != Literal {
		return nil, fmt.Errorf("[metric parse error] expected literal, got unexpected token %v", p.current)
	}

	metric := &MetricRecord{
		Name: p.current.Value,
	}

	p.advance()

	// No Bracket: Parse Value/Timestamp
	if p.current.Type != OpenBracket {
		v, ts, err := p.parseValueAndTimestamp()
		if err != nil {
			return nil, err
		}

		metric.Value = v
		metric.Timestamp = ts

		return metric, nil
	}

	// Parse Label Pairs
	p.advance()
	for {
		if p.current.Type == Comma {
			p.advance()
			continue
		}

		if p.current.Type == CloseBracket {
			break
		}

		labelName, labelValue, err := p.parseLabelValuePair()
		if err != nil {
			return nil, err
		}

		if metric.Labels == nil {
			metric.Labels = make(map[string]string)
		}

		metric.Labels[labelName] = labelValue
	}

	p.advance()

	// Value and Timestamp
	v, ts, err := p.parseValueAndTimestamp()
	if err != nil {
		return nil, err
	}

	metric.Value = v
	metric.Timestamp = ts

	return metric, nil
}

func (p *parser) parseValueAndTimestamp() (float64, *time.Time, error) {
	var ts *time.Time

	if p.current.Type != Value {
		return 0.0, nil, fmt.Errorf("[value and time parse error] expected value, got unexpected token %v", p.current)
	}

	v, err := strconv.ParseFloat(p.current.Value, 64)
	if err != nil {
		return 0.0, nil, fmt.Errorf("failed to parse value %v: %v", p.current.Value, err)
	}

	p.advance()

	if p.current.Type == Value {
		t, err := strconv.ParseInt(p.current.Value, 10, 64)
		if err != nil {
			return 0.0, nil, fmt.Errorf("failed to parse timestamp %v: %v", p.current.Value, err)
		}

		epoch := time.Unix(0, t*int64(time.Millisecond))
		ts = &epoch

		p.advance()
	}

	return v, ts, nil
}

func (p *parser) parseLabelValuePair() (string, string, error) {
	if p.current.Type != Literal {
		return "", "", fmt.Errorf("[label parse error] expected literal, got unexpected token %v", p.current)
	}

	// start with label name literal
	labelName := p.current.Value

	p.advance()

	// must be '='
	if p.current.Type != Equal {
		return "", "", fmt.Errorf("[label parse error] expected '=', got unexpected token %v", p.current)
	}

	p.advance()

	// must be string type
	if p.current.Type != String {
		return "", "", fmt.Errorf("[label parse error] expected string, got unexpected token %v", p.current)
	}

	// label value string
	labelValue := p.current.Value

	p.advance()

	return labelName, labelValue, nil
}
