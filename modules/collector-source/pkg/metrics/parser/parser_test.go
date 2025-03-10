package parser

import (
	"os"
	"testing"
)

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
