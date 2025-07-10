package parser

import (
	"os"
	"slices"
	"strings"
	"testing"
)

const infNaNTest = `
# HELP test_metric testing 1 2 3
test_metric{label1="value1",label2="value2"} +Inf 1708014188740
test_metric{label1="value1",label2="value2"} Inf 1708014188740
test_metric{label1="value1",label2="value2"} -Inf 1708014188740
test_metric{label1="value1",label2="value2"} NaN 1708014188740
test_metric{label1="value1",label2="value2"} +NaN 1708014188740
test_metric{label1="value1",label2="value2"} -NaN 1708014188740
`

func TestInfNan(t *testing.T) {
	acceptable := []string{"+Inf", "Inf", "-Inf", "NaN", "+NaN", "-NaN", "1708014188740"}
	f := strings.NewReader(infNaNTest)

	l := newLexer(f)
	for {
		tok := l.next()
		if tok.Type == Eof {
			t.Logf("<EOF>")
			break
		}
		if tok.Type == Value {
			if !slices.Contains(acceptable, tok.Value) {
				t.Errorf("Unexpected value: %v", tok.Value)
			}
		}
	}
}

func TestLexer(t *testing.T) {
	f, err := os.Open("scrape.txt")
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	l := newLexer(f)
	for {
		tok := l.next()
		if tok.Type == Eof {
			t.Logf("EOF Encountered")
			break
		}
		t.Logf("Token: %v", tok)
	}
}
