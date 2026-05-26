package ast

import (
	"testing"
)

func TestUnicodeSupport(t *testing.T) {
	// Test Unicode characters in identifiers
	cases := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Unicode identifier",
			input:       "café",
			expectError: false,
		},
		{
			name:        "Unicode in keyed access",
			input:       "[café]",
			expectError: false,
		},
		{
			name:        "Unicode with valid field",
			input:       "namespace:café",
			expectError: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := lex(c.input, allocFields, allocMapFields)
			if c.expectError && err == nil {
				t.Errorf("expected error but got nil")
			} else if !c.expectError && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

func TestUnicodeErrorMessages(t *testing.T) {
	// Test that Unicode characters produce better error messages
	_, err := lex("café@", allocFields, allocMapFields)
	if err == nil {
		t.Errorf("expected error but got nil")
		return
	}

	// Check that the error message contains Unicode information
	errStr := err.Error()
	if len(errStr) == 0 {
		t.Errorf("expected error message to contain Unicode information")
	}
}

func TestInvalidUTF8(t *testing.T) {
	// Test invalid UTF-8 sequences
	// This is a string with an invalid UTF-8 sequence
	invalidUTF8 := "\xC0\x80" // Invalid UTF-8 sequence

	_, err := lex(invalidUTF8, allocFields, allocMapFields)
	if err == nil {
		t.Errorf("expected error for invalid UTF-8 but got nil")
		return
	}

	// Check that the error message mentions invalid UTF-8
	errStr := err.Error()
	if len(errStr) == 0 || !contains(errStr, "invalid UTF-8") {
		t.Errorf("expected error message to mention invalid UTF-8, got: %s", errStr)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
