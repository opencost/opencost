package ibm

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestAPIKey_Validate(t *testing.T) {
	testCases := map[string]struct {
		auth     APIKey
		expected error
	}{
		"valid": {
			auth:     APIKey{Key: "ibm-api-key"},
			expected: nil,
		},
		"missing key": {
			auth:     APIKey{Key: ""},
			expected: fmt.Errorf("APIKey: missing apiKey"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := tc.auth.Validate()
			actualString := "nil"
			if actual != nil {
				actualString = actual.Error()
			}
			expectedString := "nil"
			if tc.expected != nil {
				expectedString = tc.expected.Error()
			}
			if actualString != expectedString {
				t.Errorf("errors do not match: Actual: '%s', Expected: '%s'", actualString, expectedString)
			}
		})
	}
}

func TestAPIKey_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     APIKey
		right    cloud.Config
		expected bool
	}{
		"matching": {
			left:     APIKey{Key: "key"},
			right:    &APIKey{Key: "key"},
			expected: true,
		},
		"different key": {
			left:     APIKey{Key: "key"},
			right:    &APIKey{Key: "other"},
			expected: false,
		},
		"wrong type": {
			left:     APIKey{Key: "key"},
			right:    nil,
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.left.Equals(tc.right) != tc.expected {
				t.Errorf("incorrect Equals result")
			}
		})
	}
}

func TestAPIKey_Sanitize(t *testing.T) {
	auth := &APIKey{Key: "secret-key"}
	sanitized := auth.Sanitize().(*APIKey)
	if sanitized.Key != cloud.Redacted {
		t.Errorf("expected redacted key, got %q", sanitized.Key)
	}
}

func TestAPIKey_JSON(t *testing.T) {
	auth := &APIKey{Key: "secret-key"}
	b, err := json.Marshal(auth)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal map: %v", err)
	}
	if raw[cloud.AuthorizerTypeProperty] != APIKeyAuthorizerType {
		t.Errorf("authorizerType = %v, want %s", raw[cloud.AuthorizerTypeProperty], APIKeyAuthorizerType)
	}

	selected, err := SelectAuthorizerByType(APIKeyAuthorizerType)
	if err != nil {
		t.Fatalf("SelectAuthorizerByType: %v", err)
	}
	unmarshalled := selected.(*APIKey)
	if err := json.Unmarshal(b, unmarshalled); err != nil {
		t.Fatalf("unmarshal authorizer: %v", err)
	}
	if !auth.Equals(unmarshalled) {
		t.Error("round-trip authorizer mismatch")
	}
}

func TestSelectAuthorizerByType_Invalid(t *testing.T) {
	_, err := SelectAuthorizerByType("not-a-type")
	if err == nil {
		t.Fatal("expected error for invalid authorizer type")
	}
}
