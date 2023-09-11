package aws

import (
	"testing"

	"github.com/opencost/opencost/pkg/cloud/config"
)

func TestAuthorizerJSON_Sanitize(t *testing.T) {

	testCases := map[string]struct {
		input    Authorizer
		expected Authorizer
	}{
		"Access Key": {
			input: &AccessKey{
				ID:     "ID",
				Secret: "Secret",
			},
			expected: &AccessKey{
				ID:     "ID",
				Secret: config.Redacted,
			},
		},
		"Service Account": {
			input:    &ServiceAccount{},
			expected: &ServiceAccount{},
		},
		"Master Payer Access Key": {
			input: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: "Secret",
				},
				RoleARN: "role arn",
			},
			expected: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: config.Redacted,
				},
				RoleARN: "role arn",
			},
		},
		"Master Payer Service Account": {
			input: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
			expected: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Convert to AuthorizerJSON for sanitization
			sanitizedAuthorizer := tc.input.Sanitize()

			if !tc.expected.Equals(sanitizedAuthorizer) {
				t.Error("Authorizer was not as expected after Sanitization")
			}

		})
	}
}
