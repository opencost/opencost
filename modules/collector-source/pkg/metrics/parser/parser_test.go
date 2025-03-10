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
