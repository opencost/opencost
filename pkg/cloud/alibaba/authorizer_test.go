package alibaba

import (
	"fmt"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestSelectAuthorizerByType(t *testing.T) {
	testCases := map[string]struct {
		typeStr       string
		expectedError bool
		expectedType  string
	}{
		"valid AccessKey type": {
			typeStr:       AccessKeyAuthorizerType,
			expectedError: false,
			expectedType:  "*alibaba.AccessKey",
		},
		"invalid type": {
			typeStr:       "InvalidType",
			expectedError: true,
			expectedType:  "",
		},
		"empty type": {
			typeStr:       "",
			expectedError: true,
			expectedType:  "",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			authorizer, err := SelectAuthorizerByType(testCase.typeStr)

			if testCase.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				if authorizer != nil {
					t.Errorf("expected nil authorizer but got %T", authorizer)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if authorizer == nil {
					t.Errorf("expected authorizer but got nil")
				}
				// Check the type
				if testCase.expectedType != "" {
					actualType := fmt.Sprintf("%T", authorizer)
					if actualType != testCase.expectedType {
						t.Errorf("expected type %s but got %s", testCase.expectedType, actualType)
					}
				}
			}
		})
	}
}

func TestAccessKey_MarshalJSON(t *testing.T) {
	testCases := map[string]struct {
		accessKey      AccessKey
		expectedFields map[string]interface{}
	}{
		"complete AccessKey": {
			accessKey: AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
			expectedFields: map[string]interface{}{
				cloud.AuthorizerTypeProperty: AccessKeyAuthorizerType,
				"accessKeyID":                "test-id",
				"accessKeySecret":            "test-secret",
			},
		},
		"empty AccessKey": {
			accessKey: AccessKey{},
			expectedFields: map[string]interface{}{
				cloud.AuthorizerTypeProperty: AccessKeyAuthorizerType,
				"accessKeyID":                "",
				"accessKeySecret":            "",
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := testCase.accessKey.MarshalJSON()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			if err != nil {
				t.Errorf("failed to unmarshal JSON: %v", err)
			}

			for key, expectedValue := range testCase.expectedFields {
				if actualValue, exists := result[key]; !exists {
					t.Errorf("missing field %s", key)
				} else if actualValue != expectedValue {
					t.Errorf("field %s: expected %v, got %v", key, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestAccessKey_Validate(t *testing.T) {
	testCases := map[string]struct {
		accessKey     AccessKey
		expectedError bool
		errorMessage  string
	}{
		"valid AccessKey": {
			accessKey: AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
			expectedError: false,
		},
		"missing AccessKeyID": {
			accessKey: AccessKey{
				AccessKeySecret: "test-secret",
			},
			expectedError: true,
			errorMessage:  "AccessKey: missing Access key ID",
		},
		"missing AccessKeySecret": {
			accessKey: AccessKey{
				AccessKeyID: "test-id",
			},
			expectedError: true,
			errorMessage:  "AccessKey: missing Access Key secret",
		},
		"both fields missing": {
			accessKey:     AccessKey{},
			expectedError: true,
			errorMessage:  "AccessKey: missing Access key ID",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			err := testCase.accessKey.Validate()

			if testCase.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if testCase.errorMessage != "" && err.Error() != testCase.errorMessage {
					t.Errorf("expected error message '%s' but got '%s'", testCase.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestAccessKey_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     AccessKey
		right    cloud.Config
		expected bool
	}{
		"matching AccessKey": {
			left: AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			right: &AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			expected: true,
		},
		"different AccessKeyID": {
			left: AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			right: &AccessKey{
				AccessKeyID:     "id2",
				AccessKeySecret: "secret1",
			},
			expected: false,
		},
		"different AccessKeySecret": {
			left: AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			right: &AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret2",
			},
			expected: false,
		},
		"nil config": {
			left: AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			right:    nil,
			expected: false,
		},
		"different config type": {
			left: AccessKey{
				AccessKeyID:     "id1",
				AccessKeySecret: "secret1",
			},
			right:    &BOAConfiguration{},
			expected: false,
		},
		"empty AccessKey": {
			left:     AccessKey{},
			right:    &AccessKey{},
			expected: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.left.Equals(testCase.right)
			if result != testCase.expected {
				t.Errorf("expected %t but got %t", testCase.expected, result)
			}
		})
	}
}

func TestAccessKey_Sanitize(t *testing.T) {
	original := AccessKey{
		AccessKeyID:     "original-id",
		AccessKeySecret: "original-secret",
	}

	sanitized := original.Sanitize()
	sanitizedAccessKey, ok := sanitized.(*AccessKey)
	if !ok {
		t.Fatalf("expected *AccessKey but got %T", sanitized)
	}

	// Check that AccessKeyID remains unchanged
	if sanitizedAccessKey.AccessKeyID != original.AccessKeyID {
		t.Errorf("AccessKeyID should remain unchanged: expected %s, got %s",
			original.AccessKeyID, sanitizedAccessKey.AccessKeyID)
	}

	// Check that AccessKeySecret is redacted
	if sanitizedAccessKey.AccessKeySecret != cloud.Redacted {
		t.Errorf("AccessKeySecret should be redacted: expected %s, got %s",
			cloud.Redacted, sanitizedAccessKey.AccessKeySecret)
	}

	// Verify original is not modified
	if original.AccessKeySecret != "original-secret" {
		t.Errorf("original AccessKey should not be modified")
	}
}

func TestAccessKey_GetCredentials(t *testing.T) {
	testCases := map[string]struct {
		accessKey     AccessKey
		expectedError bool
		checkCreds    func(*credentials.AccessKeyCredential) bool
	}{
		"valid credentials": {
			accessKey: AccessKey{
				AccessKeyID:     "test-id",
				AccessKeySecret: "test-secret",
			},
			expectedError: false,
			checkCreds: func(creds *credentials.AccessKeyCredential) bool {
				return creds.AccessKeyId == "test-id" && creds.AccessKeySecret == "test-secret"
			},
		},
		"invalid credentials - missing ID": {
			accessKey: AccessKey{
				AccessKeySecret: "test-secret",
			},
			expectedError: true,
		},
		"invalid credentials - missing secret": {
			accessKey: AccessKey{
				AccessKeyID: "test-id",
			},
			expectedError: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			creds, err := testCase.accessKey.GetCredentials()

			if testCase.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				if creds != nil {
					t.Errorf("expected nil credentials but got %v", creds)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if creds == nil {
					t.Errorf("expected credentials but got nil")
				}

				accessKeyCreds, ok := creds.(*credentials.AccessKeyCredential)
				if !ok {
					t.Errorf("expected *credentials.AccessKeyCredential but got %T", creds)
				}

				if testCase.checkCreds != nil && !testCase.checkCreds(accessKeyCreds) {
					t.Errorf("credentials validation failed")
				}
			}
		})
	}
}

func TestAccessKey_JSONRoundTrip(t *testing.T) {
	original := AccessKey{
		AccessKeyID:     "test-id",
		AccessKeySecret: "test-secret",
	}

	// Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Unmarshal back
	var unmarshaled AccessKey
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Check equality
	if !original.Equals(&unmarshaled) {
		t.Errorf("round-trip JSON marshaling failed: original %+v, unmarshaled %+v", original, unmarshaled)
	}
}
