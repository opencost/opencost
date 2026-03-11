package gcp

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigQueryConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   BigQueryConfiguration
		expected error
	}{
		"valid config GCP Key": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: nil,
		},
		"valid config WorkloadIdentity": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: nil,
		},
		"access Key invalid": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: nil,
				},
			},
			expected: fmt.Errorf("BigQueryConfig: issue with GCP Authorizer: ServiceAccountKey: missing Key"),
		},
		"missing configurer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: fmt.Errorf("BigQueryConfig: missing configurer"),
		},
		"missing projectID": {
			config: BigQueryConfiguration{
				ProjectID: "",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing ProjectID"),
		},
		"missing dataset": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing Dataset"),
		},
		"missing table": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: fmt.Errorf("BigQueryConfig: missing Table"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.config.Validate()
			actualString := "nil"
			if actual != nil {
				actualString = actual.Error()
			}
			expectedString := "nil"
			if testCase.expected != nil {
				expectedString = testCase.expected.Error()
			}
			if actualString != expectedString {
				t.Errorf("errors do not match: Actual: '%s', Expected: '%s", actualString, expectedString)
			}
		})
	}
}

func TestBigQueryConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     BigQueryConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: true,
		},
		"different configurer": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: false,
		},
		"missing both configurer": {
			left: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left configurer": {
			left: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
			expected: false,
		},
		"missing right configurer": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
			expected: false,
		},
		"different projectID": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID2",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different dataset": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset2",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different table": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table2",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			expected: false,
		},
		"different config": {
			left: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
			right: &ServiceAccountKey{

				Key: map[string]string{
					"Key":  "Key",
					"key1": "key2",
				},
			},
			expected: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.left.Equals(testCase.right)
			if actual != testCase.expected {
				t.Errorf("incorrect result: Actual: '%t', Expected: '%t", actual, testCase.expected)
			}
		})
	}
}

func TestBigQueryConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config BigQueryConfiguration
	}{
		"Empty Config": {
			config: BigQueryConfiguration{},
		},
		"Nil Authorizer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: nil,
			},
		},
		"ServiceAccountKeyConfigurer": {
			config: BigQueryConfiguration{
				ProjectID: "projectID",
				Dataset:   "dataset",
				Table:     "table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"Key":  "Key",
						"key1": "key2",
					},
				},
			},
		},
		"WorkLoadIdentityConfigurer": {
			config: BigQueryConfiguration{
				ProjectID:  "projectID",
				Dataset:    "dataset",
				Table:      "table",
				Authorizer: &WorkloadIdentity{},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			// test JSON Marshalling
			configJSON, err := json.Marshal(testCase.config)
			if err != nil {
				t.Errorf("failed to marshal configuration: %s", err.Error())
			}
			log.Info(string(configJSON))
			unmarshalledConfig := &BigQueryConfiguration{}
			err = json.Unmarshal(configJSON, unmarshalledConfig)
			if err != nil {
				t.Errorf("failed to unmarshal configuration: %s", err.Error())
			}
			if !testCase.config.Equals(unmarshalledConfig) {
				t.Error("config does not equal unmarshalled config")
			}
		})
	}
}

func TestBigQueryConfiguration_Key(t *testing.T) {
	bqc := &BigQueryConfiguration{
		ProjectID: "test-project",
		Dataset:   "test-dataset",
		Table:     "test-table",
	}

	key := bqc.Key()
	expected := "test-project/test-dataset.test-table"
	assert.Equal(t, expected, key)
}

func TestBigQueryConfiguration_Provider(t *testing.T) {
	bqc := &BigQueryConfiguration{}
	provider := bqc.Provider()
	assert.Equal(t, "GCP", provider)
}

func TestBigQueryConfiguration_GetBillingDataDataset(t *testing.T) {
	bqc := &BigQueryConfiguration{
		Dataset: "test-dataset",
		Table:   "test-table",
	}

	dataset := bqc.GetBillingDataDataset()
	expected := "test-dataset.test-table"
	assert.Equal(t, expected, dataset)
}

func TestBigQueryConfiguration_Sanitize(t *testing.T) {
	bqc := &BigQueryConfiguration{
		ProjectID: "test-project",
		Dataset:   "test-dataset",
		Table:     "test-table",
		Authorizer: &ServiceAccountKey{
			Key: map[string]string{
				"type":        "service_account",
				"private_key": "secret-key",
			},
		},
	}

	sanitized := bqc.Sanitize()
	require.NotNil(t, sanitized)

	sanitizedBQC, ok := sanitized.(*BigQueryConfiguration)
	require.True(t, ok)

	assert.Equal(t, "test-project", sanitizedBQC.ProjectID)
	assert.Equal(t, "test-dataset", sanitizedBQC.Dataset)
	assert.Equal(t, "test-table", sanitizedBQC.Table)
	assert.NotNil(t, sanitizedBQC.Authorizer)

	// Check that the authorizer is also sanitized
	saKey, ok := sanitizedBQC.Authorizer.(*ServiceAccountKey)
	require.True(t, ok)
	for _, value := range saKey.Key {
		assert.Equal(t, cloud.Redacted, value)
	}
}

func TestConvertBigQueryConfigToConfig(t *testing.T) {
	tests := []struct {
		name     string
		bqc      BigQueryConfig
		expected cloud.KeyedConfig
	}{
		{
			name:     "Empty config",
			bqc:      BigQueryConfig{},
			expected: nil,
		},
		{
			name: "Config with service account key",
			bqc: BigQueryConfig{
				ProjectID:          "test-project",
				BillingDataDataset: "test-dataset.test-table",
				Key: map[string]string{
					"type": "service_account",
				},
			},
			expected: &BigQueryConfiguration{
				ProjectID: "test-project",
				Dataset:   "test-dataset",
				Table:     "test-table",
				Authorizer: &ServiceAccountKey{
					Key: map[string]string{
						"type": "service_account",
					},
				},
			},
		},
		{
			name: "Config without service account key",
			bqc: BigQueryConfig{
				ProjectID:          "test-project",
				BillingDataDataset: "test-dataset.test-table",
				Key:                map[string]string{},
			},
			expected: &BigQueryConfiguration{
				ProjectID:  "test-project",
				Dataset:    "test-dataset",
				Table:      "test-table",
				Authorizer: &WorkloadIdentity{},
			},
		},
		{
			name: "Config with single part dataset",
			bqc: BigQueryConfig{
				ProjectID:          "test-project",
				BillingDataDataset: "test-dataset",
				Key:                map[string]string{},
			},
			expected: &BigQueryConfiguration{
				ProjectID:  "test-project",
				Dataset:    "test-dataset",
				Table:      "",
				Authorizer: &WorkloadIdentity{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertBigQueryConfigToConfig(tt.bqc)

			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				expectedBQC := tt.expected.(*BigQueryConfiguration)
				resultBQC := result.(*BigQueryConfiguration)

				assert.Equal(t, expectedBQC.ProjectID, resultBQC.ProjectID)
				assert.Equal(t, expectedBQC.Dataset, resultBQC.Dataset)
				assert.Equal(t, expectedBQC.Table, resultBQC.Table)
				assert.NotNil(t, resultBQC.Authorizer)
			}
		})
	}
}

func TestBigQueryConfiguration_UnmarshalJSON_Valid(t *testing.T) {
	jsonData := `{
		"projectID": "test-project",
		"dataset": "test-dataset",
		"table": "test-table",
		"authorizer": {
			"authorizerType": "GCPServiceAccountKey",
			"key": {
				"type": "service_account"
			}
		}
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.NoError(t, err)
	assert.Equal(t, "test-project", bqc.ProjectID)
	assert.Equal(t, "test-dataset", bqc.Dataset)
	assert.Equal(t, "test-table", bqc.Table)
	assert.NotNil(t, bqc.Authorizer)

	saKey, ok := bqc.Authorizer.(*ServiceAccountKey)
	assert.True(t, ok)
	assert.Equal(t, "service_account", saKey.Key["type"])
}

func TestBigQueryConfiguration_UnmarshalJSON_InvalidProjectID(t *testing.T) {
	jsonData := `{
		"dataset": "test-dataset",
		"table": "test-table",
		"authorizer": {
			"authorizerType": "GCPServiceAccountKey",
			"key": {
				"type": "service_account"
			}
		}
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "projectID")
}

func TestBigQueryConfiguration_UnmarshalJSON_InvalidDataset(t *testing.T) {
	jsonData := `{
		"projectID": "test-project",
		"table": "test-table",
		"authorizer": {
			"authorizerType": "GCPServiceAccountKey",
			"key": {
				"type": "service_account"
			}
		}
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dataset")
}

func TestBigQueryConfiguration_UnmarshalJSON_InvalidTable(t *testing.T) {
	jsonData := `{
		"projectID": "test-project",
		"dataset": "test-dataset",
		"authorizer": {
			"authorizerType": "GCPServiceAccountKey",
			"key": {
				"type": "service_account"
			}
		}
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table")
}

func TestBigQueryConfiguration_UnmarshalJSON_MissingAuthorizer(t *testing.T) {
	jsonData := `{
		"projectID": "test-project",
		"dataset": "test-dataset",
		"table": "test-table"
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing authorizer")
}

func TestBigQueryConfiguration_UnmarshalJSON_InvalidAuthorizer(t *testing.T) {
	jsonData := `{
		"projectID": "test-project",
		"dataset": "test-dataset",
		"table": "test-table",
		"authorizer": {
			"authorizerType": "InvalidType"
		}
	}`

	var bqc BigQueryConfiguration
	err := json.Unmarshal([]byte(jsonData), &bqc)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidType")
}
