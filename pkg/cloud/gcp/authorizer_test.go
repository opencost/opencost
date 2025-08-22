package gcp

import (
	"encoding/json"
	"testing"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectAuthorizerByType(t *testing.T) {
	tests := []struct {
		name        string
		authorizerType string
		expectError bool
	}{
		{
			name:        "ServiceAccountKey type",
			authorizerType: ServiceAccountKeyAuthorizerType,
			expectError: false,
		},
		{
			name:        "WorkloadIdentity type",
			authorizerType: WorkloadIdentityAuthorizerType,
			expectError: false,
		},
		{
			name:        "Invalid type",
			authorizerType: "InvalidType",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer, err := SelectAuthorizerByType(tt.authorizerType)
			
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, authorizer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, authorizer)
			}
		})
	}
}

func TestServiceAccountKey_MarshalJSON(t *testing.T) {
	key := &ServiceAccountKey{
		Key: map[string]string{
			"type": "service_account",
			"project_id": "test-project",
		},
	}

	data, err := json.Marshal(key)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, ServiceAccountKeyAuthorizerType, result["authorizerType"])
	assert.NotNil(t, result["key"])
}

func TestServiceAccountKey_Validate(t *testing.T) {
	tests := []struct {
		name        string
		key         map[string]string
		expectError bool
	}{
		{
			name: "Valid key",
			key: map[string]string{
				"type": "service_account",
			},
			expectError: false,
		},
		{
			name:        "Nil key",
			key:         nil,
			expectError: true,
		},
		{
			name:        "Empty key",
			key:         map[string]string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saKey := &ServiceAccountKey{Key: tt.key}
			err := saKey.Validate()
			
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestServiceAccountKey_Equals(t *testing.T) {
	key1 := &ServiceAccountKey{
		Key: map[string]string{"type": "service_account"},
	}
	key2 := &ServiceAccountKey{
		Key: map[string]string{"type": "service_account"},
	}
	key3 := &ServiceAccountKey{
		Key: map[string]string{"type": "different"},
	}
	workloadIdentity := &WorkloadIdentity{}

	tests := []struct {
		name     string
		config1  cloud.Config
		config2  cloud.Config
		expected bool
	}{
		{
			name:     "Same keys",
			config1:  key1,
			config2:  key2,
			expected: true,
		},
		{
			name:     "Different keys",
			config1:  key1,
			config2:  key3,
			expected: false,
		},
		{
			name:     "Different types",
			config1:  key1,
			config2:  workloadIdentity,
			expected: false,
		},
		{
			name:     "Nil config",
			config1:  key1,
			config2:  nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config1.Equals(tt.config2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestServiceAccountKey_Sanitize(t *testing.T) {
	key := &ServiceAccountKey{
		Key: map[string]string{
			"type": "service_account",
			"private_key": "secret-key",
		},
	}

	sanitized := key.Sanitize()
	require.NotNil(t, sanitized)

	saKey, ok := sanitized.(*ServiceAccountKey)
	require.True(t, ok)

	for _, value := range saKey.Key {
		assert.Equal(t, cloud.Redacted, value)
	}
}

func TestServiceAccountKey_CreateGCPClientOptions(t *testing.T) {
	tests := []struct {
		name        string
		key         map[string]string
		expectError bool
	}{
		{
			name: "Valid key",
			key: map[string]string{
				"type": "service_account",
			},
			expectError: false,
		},
		{
			name:        "Invalid key",
			key:         nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saKey := &ServiceAccountKey{Key: tt.key}
			options, err := saKey.CreateGCPClientOptions()
			
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, options)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, options)
				assert.Len(t, options, 1)
			}
		})
	}
}

func TestWorkloadIdentity_MarshalJSON(t *testing.T) {
	wi := &WorkloadIdentity{}

	data, err := json.Marshal(wi)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, WorkloadIdentityAuthorizerType, result["authorizerType"])
}

func TestWorkloadIdentity_Validate(t *testing.T) {
	wi := &WorkloadIdentity{}
	err := wi.Validate()
	assert.NoError(t, err)
}

func TestWorkloadIdentity_Equals(t *testing.T) {
	wi1 := &WorkloadIdentity{}
	wi2 := &WorkloadIdentity{}
	saKey := &ServiceAccountKey{Key: map[string]string{"type": "service_account"}}

	tests := []struct {
		name     string
		config1  cloud.Config
		config2  cloud.Config
		expected bool
	}{
		{
			name:     "Same workload identity",
			config1:  wi1,
			config2:  wi2,
			expected: true,
		},
		{
			name:     "Different types",
			config1:  wi1,
			config2:  saKey,
			expected: false,
		},
		{
			name:     "Nil config",
			config1:  wi1,
			config2:  nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config1.Equals(tt.config2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWorkloadIdentity_Sanitize(t *testing.T) {
	wi := &WorkloadIdentity{}
	sanitized := wi.Sanitize()
	
	_, ok := sanitized.(*WorkloadIdentity)
	assert.True(t, ok)
}

func TestWorkloadIdentity_CreateGCPClientOptions(t *testing.T) {
	wi := &WorkloadIdentity{}
	options, err := wi.CreateGCPClientOptions()
	
	assert.NoError(t, err)
	assert.NotNil(t, options)
	assert.Len(t, options, 0)
}
