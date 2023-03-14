package util

import (
	"fmt"
	"testing"
)

func TestCamelCaseSplitter(t *testing.T) {
	cases := []struct {
		input    string
		expected []string
	}{
		{
			input:    "labels",
			expected: []string{"labels"},
		},
		{
			input:    "helloWorld",
			expected: []string{"hello", "World"},
		},
		{
			input:    "TestCase",
			expected: []string{"Test", "Case"},
		},
		{
			input:    "controllerName",
			expected: []string{"controller", "Name"},
		},
		{
			input:    "underscored_testCase",
			expected: []string{"underscored", "test", "Case"},
		},
		{
			input:    "__underscored___testCase",
			expected: []string{"underscored", "test", "Case"},
		},
		{
			input:    "with123Digits",
			expected: []string{"with", "Digits"},
		},
		{
			input:    "test-case",
			expected: []string{"test", "case"},
		},
		{
			input:    "allocationETLString",
			expected: []string{"allocation", "ETL", "String"},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.input), func(t *testing.T) {
			result := SplitPropertyName(c.input)
			expected := c.expected

			cmp(t, result, expected)
		})
	}
}

func cmp(t *testing.T, a, b []string) {
	if len(a) != len(b) {
		t.Errorf("Length of Result: %d Not Equal to Expected: %d", len(a), len(b))
		return
	}

	for i := range a {
		if a[i] != b[i] {
			t.Errorf("String at Index: %d for Result: %s != Expected: %s", i, a[i], b[i])
			return
		}
	}
}
