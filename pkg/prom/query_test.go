package prom

import "testing"

func TestWarningsFrom(t *testing.T) {
	var results interface{}

	results = map[string]interface{}{
		"status": "success",
		"warnings": []string{
			"Warning #1",
			"Warning #2",
		},
	}

	warnings := warningsFrom(results)
	if len(warnings) != 2 {
		t.Errorf("Unexpected warnings length: %d, Expected 2.", len(warnings))
	}

	if warnings[0] != "Warning #1" {
		t.Errorf("Unexpected first warning: %s", warnings[0])
	}
	if warnings[1] != "Warning #2" {
		t.Errorf("Unexpected second warning: %s", warnings[1])
	}
}
