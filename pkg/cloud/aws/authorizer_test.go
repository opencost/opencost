package aws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
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
				Secret: cloud.Redacted,
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
					Secret: cloud.Redacted,
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
		"Google Web Identity": {
			input: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
			},
			expected: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
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

func TestAuthorizerJSON_Encode(t *testing.T) {

	testCases := map[string]struct {
		authorizer Authorizer
	}{
		"Access Key": {
			authorizer: &AccessKey{
				ID:     "ID",
				Secret: "Secret",
			},
		},
		"Service Account": {
			authorizer: &ServiceAccount{},
		},
		"Master Payer Access Key": {
			authorizer: &AssumeRole{
				Authorizer: &AccessKey{
					ID:     "ID",
					Secret: "Secret",
				},
				RoleARN: "role arn",
			},
		},
		"Master Payer Service Account": {
			authorizer: &AssumeRole{
				Authorizer: &ServiceAccount{},
				RoleARN:    "role arn",
			},
		},
		"Google Web Identity": {
			authorizer: &WebIdentity{
				RoleARN:          "role arn",
				IdentityProvider: "Google",
				TokenRetriever: &GoogleIDTokenRetriever{
					Aud: "aud",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			b, err := tc.authorizer.MarshalJSON()
			if err != nil {
				t.Errorf("Failed to Marshal Authorizer: %s", err)
			}

			var f interface{}
			err = json.Unmarshal(b, &f)
			if err != nil {
				t.Errorf("Failed to Unmarshal Authorizer: %s", err)
			}

			authorizer, err := cloud.AuthorizerFromInterface(f, SelectAuthorizerByType)
			if err != nil {
				t.Errorf("Failed to Unmarshal Authorizer: %s", err)
			}

			if !tc.authorizer.Equals(authorizer) {
				t.Error("Authorizer was not as expected after Sanitization")
			}

		})
	}
}

// TestAssumeRole_CreateAWSConfig_BaseAuthorizerError verifies that AssumeRole.CreateAWSConfig
// propagates a failure from its base Authorizer instead of silently discarding it, and that the
// returned error identifies the target RoleARN.
func TestAssumeRole_CreateAWSConfig_BaseAuthorizerError(t *testing.T) {
	ara := &AssumeRole{
		// AccessKey.Validate fails when ID/Secret are empty, which makes AccessKey.CreateAWSConfig
		// return an error without any network calls.
		Authorizer: &AccessKey{},
		RoleARN:    "arn:aws:iam::123456789012:role/test-role",
	}

	_, err := ara.CreateAWSConfig("us-east-1")
	if err == nil {
		t.Fatal("expected an error when the base Authorizer fails to create an AWS config, got nil")
	}
	if !strings.Contains(err.Error(), ara.RoleARN) {
		t.Errorf("expected error to reference RoleARN %q, got: %s", ara.RoleARN, err.Error())
	}
}

// TestAssumeRole_CreateAWSConfig_AssumeRoleFailure verifies that a failure to assume the
// configured RoleARN (e.g. a broken cross-account trust policy) is surfaced with the RoleARN
// and underlying STS error, rather than a bare/generic SDK error, once credentials are resolved.
func TestAssumeRole_CreateAWSConfig_AssumeRoleFailure(t *testing.T) {
	// Minimal STS server that always rejects AssumeRole, simulating a broken trust policy.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`<ErrorResponse><Error><Code>AccessDenied</Code><Message>not authorized to perform sts:AssumeRole</Message></Error></ErrorResponse>`))
	}))
	defer server.Close()

	t.Setenv("AWS_ENDPOINT_URL_STS", server.URL)

	ara := &AssumeRole{
		Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
		RoleARN:    "arn:aws:iam::123456789012:role/test-role",
	}

	cfg, err := ara.CreateAWSConfig("us-east-1")
	if err != nil {
		t.Fatalf("CreateAWSConfig() returned an unexpected error building the config: %v", err)
	}

	_, err = cfg.Credentials.Retrieve(context.Background())
	if err == nil {
		t.Fatal("expected an error retrieving credentials for a rejected AssumeRole, got nil")
	}
	if !strings.Contains(err.Error(), ara.RoleARN) {
		t.Errorf("expected error to reference RoleARN %q, got: %s", ara.RoleARN, err.Error())
	}
	if !strings.Contains(err.Error(), "AssumeRole") {
		t.Errorf("expected error to identify the AssumeRole authorizer, got: %s", err.Error())
	}
}
