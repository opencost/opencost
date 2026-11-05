package ibm

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestUsageConfiguration_Validate(t *testing.T) {
	validAuth := &APIKey{Key: "ibm-api-key"}
	testCases := map[string]struct {
		config   UsageConfiguration
		expected error
	}{
		"valid": {
			config: UsageConfiguration{
				AccountID:  "account-id",
				Authorizer: validAuth,
			},
			expected: nil,
		},
		"missing authorizer": {
			config: UsageConfiguration{
				AccountID:  "account-id",
				Authorizer: nil,
			},
			expected: fmt.Errorf("UsageConfiguration: missing Authorizer"),
		},
		"invalid authorizer": {
			config: UsageConfiguration{
				AccountID:  "account-id",
				Authorizer: &APIKey{Key: ""},
			},
			expected: fmt.Errorf("UsageConfiguration: APIKey: missing apiKey"),
		},
		"missing accountID": {
			config: UsageConfiguration{
				AccountID:  "",
				Authorizer: validAuth,
			},
			expected: fmt.Errorf("UsageConfiguration: missing accountID"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := tc.config.Validate()
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

func TestUsageConfiguration_Equals(t *testing.T) {
	auth := &APIKey{Key: "key"}
	testCases := map[string]struct {
		left     UsageConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching": {
			left: UsageConfiguration{
				AccountID:  "acct",
				Authorizer: auth,
			},
			right: &UsageConfiguration{
				AccountID:  "acct",
				Authorizer: &APIKey{Key: "key"},
			},
			expected: true,
		},
		"different account": {
			left: UsageConfiguration{
				AccountID:  "acct",
				Authorizer: auth,
			},
			right: &UsageConfiguration{
				AccountID:  "other",
				Authorizer: &APIKey{Key: "key"},
			},
			expected: false,
		},
		"different authorizer": {
			left: UsageConfiguration{
				AccountID:  "acct",
				Authorizer: auth,
			},
			right: &UsageConfiguration{
				AccountID:  "acct",
				Authorizer: &APIKey{Key: "other"},
			},
			expected: false,
		},
		"both nil authorizer": {
			left: UsageConfiguration{
				AccountID:  "acct",
				Authorizer: nil,
			},
			right: &UsageConfiguration{
				AccountID:  "acct",
				Authorizer: nil,
			},
			expected: true,
		},
		"left nil authorizer": {
			left: UsageConfiguration{
				AccountID:  "acct",
				Authorizer: nil,
			},
			right: &UsageConfiguration{
				AccountID:  "acct",
				Authorizer: auth,
			},
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

func TestUsageConfiguration_KeyProviderSanitize(t *testing.T) {
	cfg := &UsageConfiguration{
		AccountID:  "acct-123",
		Authorizer: &APIKey{Key: "secret"},
	}
	if cfg.Key() != "acct-123" {
		t.Errorf("Key() = %q, want acct-123", cfg.Key())
	}
	slashCfg := &UsageConfiguration{AccountID: "a/acct-123"}
	if slashCfg.Key() != "acct-123" {
		t.Errorf("Key() with a/ prefix = %q, want acct-123", slashCfg.Key())
	}
	if cfg.Provider() != opencost.IBMProvider {
		t.Errorf("Provider() = %q, want %q", cfg.Provider(), opencost.IBMProvider)
	}
	sanitized := cfg.Sanitize().(*UsageConfiguration)
	if sanitized.Authorizer.(*APIKey).Key != cloud.Redacted {
		t.Errorf("Sanitize did not redact api key")
	}
}

func TestUsageConfiguration_JSON(t *testing.T) {
	cfg := UsageConfiguration{
		AccountID:  "acct-123",
		Authorizer: &APIKey{Key: "secret"},
	}
	b, err := json.Marshal(&cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	unmarshalled := &UsageConfiguration{}
	if err := json.Unmarshal(b, unmarshalled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !cfg.Equals(unmarshalled) {
		t.Error("config does not equal unmarshalled config")
	}
}
