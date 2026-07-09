package parser

import (
	"os"
	"strings"
	"testing"
)

const interestingFloatCases = `
# HELP random comment
test_metric{label1="value1", label2="value2"} .0123 1708014188740
test_metric{label1="value1", label2="value2"} 1.23e-2 1708014188740
test_metric{label1="value1", label2="value2"} 1.23e2 1708014188740
test_metric{label1="value1", label2="value2"} 1.23e+2 1708014188740
test_metric{label1="value1", label2="value2"} 0.23E-1 1708014188740
test_metric{label1="value1", label2="value2"} 0.23E1 1708014188740
test_metric{label1="value1", label2="value2"} 0.23E+1 1708014188740
test_metric{label1="value1", label2="value2"} 1_000_000.0 1708014188740
test_metric{label1="value1", label2="value2"} ___123 1708014188740
`

const cases = `
# HELP random comment 
test_metric{  , label1="value1"   , label2="value2" ,} 123 1708014188740
a_metric{} 0
another_metric{__foo="bar", } 15.2 1708014188740
spaced_metric
{
   label1="value1",
   label2="value2"
   
}
   123.52
   1708014188740
`

func TestParser(t *testing.T) {
	f, err := os.Open("scrape.txt")
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	p := newParser(f)
	metrics, err := p.parse()
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range metrics {
		t.Logf("Metric: %v", m)
	}
}

func TestInterestingFloatParsing(t *testing.T) {
	f := strings.NewReader(interestingFloatCases)
	p := newParser(f)

	metrics, err := p.parse()
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range metrics {
		t.Logf("Metric: %v", m)
	}
}

func TestMetricFormatResilience(t *testing.T) {
	f := strings.NewReader(cases)
	p := newParser(f)

	metrics, err := p.parse()
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range metrics {
		t.Logf("Metric: %v", m)
	}
}

// Metric names may contain colons per the Prometheus exposition format
// (recording rules, vLLM's vllm:* metrics). Regression test for the lexer
// treating ':' as an unexpected token.
func TestParserColonMetricNames(t *testing.T) {
	input := `# HELP vllm:kv_cache_usage_perc KV cache usage
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="Qwen3-32B"} 0.42
:leading_colon_total 3
vllm:num_requests_waiting{model_name="Qwen3-32B"} 7 1712000000
`

	records, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].Name != "vllm:kv_cache_usage_perc" || records[0].Value != 0.42 {
		t.Errorf("unexpected first record: %+v", records[0])
	}
	if records[0].Labels["model_name"] != "Qwen3-32B" {
		t.Errorf("unexpected labels on first record: %+v", records[0].Labels)
	}
	if records[1].Name != ":leading_colon_total" || records[1].Value != 3 {
		t.Errorf("unexpected second record: %+v", records[1])
	}
	if records[2].Name != "vllm:num_requests_waiting" || records[2].Value != 7 {
		t.Errorf("unexpected third record: %+v", records[2])
	}
}
