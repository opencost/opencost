package aws

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestAthenaConfiguration_Validate(t *testing.T) {
	testCases := map[string]struct {
		config   AthenaConfiguration
		expected error
	}{
		"valid config access key": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: nil,
		},
		"valid config service account": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"valid missing catalog": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"access key invalid": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID: "id",
				},
			},
			expected: fmt.Errorf("AthenaConfiguration: AccessKey: missing Secret"),
		},
		"missing Authorizer": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: fmt.Errorf("AthenaConfiguration: missing Authorizer"),
		},
		"missing bucket": {
			config: AthenaConfiguration{
				Bucket:     "",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing bucket"),
		},
		"missing region": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing region"),
		},
		"missing database": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing database"),
		},
		"missing table": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Table:      "",
				Catalog:    "catalog",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing table"),
		},
		"missing workgroup": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: nil,
		},
		"missing account": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "",
				Authorizer: &ServiceAccount{},
			},
			expected: fmt.Errorf("AthenaConfiguration: missing account"),
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

func TestAthenaConfiguration_Equals(t *testing.T) {
	testCases := map[string]struct {
		left     AthenaConfiguration
		right    cloud.Config
		expected bool
	}{
		"matching config": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: true,
		},
		"different Authorizer": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing both Authorizer": {
			left: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: true,
		},
		"missing left Authorizer": {
			left: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
			expected: false,
		},
		"missing right Authorizer": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: nil,
			},
			expected: false,
		},
		"different bucket": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket2",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different region": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region2",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different database": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database2",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different table": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table2",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different catalog": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog2",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different workgroup": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup2",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different account": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account2",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			expected: false,
		},
		"different config": {
			left: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
			right: &AccessKey{
				ID:     "id",
				Secret: "secret",
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

func TestAthenaConfiguration_JSON(t *testing.T) {
	testCases := map[string]struct {
		config AthenaConfiguration
	}{
		"Empty Config": {
			config: AthenaConfiguration{},
		},
		"AccessKey": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AccessKey{
					ID:     "id",
					Secret: "secret",
				},
			},
		},

		"ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
			},
		},
		"AssumeRole with AccessKey": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &AccessKey{
						ID:     "id",
						Secret: "secret",
					},
					RoleARN: "12345",
				},
			},
		},
		"AssumeRole with ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &ServiceAccount{},
					RoleARN:    "12345",
				},
			},
		},
		"RoleArnNil": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: nil,
					RoleARN:    "12345",
				},
			},
		},
		"AssumeRole with AssumeRole with ServiceAccount": {
			config: AthenaConfiguration{
				Bucket:    "bucket",
				Region:    "region",
				Database:  "database",
				Catalog:   "catalog",
				Table:     "table",
				Workgroup: "workgroup",
				Account:   "account",
				Authorizer: &AssumeRole{
					Authorizer: &AssumeRole{
						RoleARN:    "12345",
						Authorizer: &ServiceAccount{},
					},
					RoleARN: "12345",
				},
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
			unmarshalledConfig := &AthenaConfiguration{}
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

func TestAthenaConfiguration_Sanitize(t *testing.T) {
	testCases := map[string]struct {
		config   AthenaConfiguration
		expected AthenaConfiguration
	}{
		"sanitize with access key": {
			config: AthenaConfiguration{
				Bucket:    "test-bucket",
				Region:    "us-west-2",
				Database:  "test-db",
				Catalog:   "test-catalog",
				Table:     "test-table",
				Workgroup: "test-workgroup",
				Account:   "123456789012",
				Authorizer: &AccessKey{
					ID:     "test-id",
					Secret: "test-secret",
				},
			},
			expected: AthenaConfiguration{
				Bucket:    "test-bucket",
				Region:    "us-west-2",
				Database:  "test-db",
				Catalog:   "test-catalog",
				Table:     "test-table",
				Workgroup: "test-workgroup",
				Account:   "123456789012",
				Authorizer: &AccessKey{
					ID:     "test-id",
					Secret: "test-secret",
				},
			},
		},
		"sanitize with service account": {
			config: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Workgroup:  "test-workgroup",
				Account:    "123456789012",
				Authorizer: &ServiceAccount{},
			},
			expected: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Workgroup:  "test-workgroup",
				Account:    "123456789012",
				Authorizer: &ServiceAccount{},
			},
		},
		"sanitize with assume role": {
			config: AthenaConfiguration{
				Bucket:    "test-bucket",
				Region:    "eu-west-1",
				Database:  "test-db",
				Table:     "test-table",
				Workgroup: "test-workgroup",
				Account:   "123456789012",
				Authorizer: &AssumeRole{
					Authorizer: &ServiceAccount{},
					RoleARN:    "arn:aws:iam::123456789012:role/test-role",
				},
			},
			expected: AthenaConfiguration{
				Bucket:    "test-bucket",
				Region:    "eu-west-1",
				Database:  "test-db",
				Table:     "test-table",
				Workgroup: "test-workgroup",
				Account:   "123456789012",
				Authorizer: &AssumeRole{
					Authorizer: &ServiceAccount{},
					RoleARN:    "arn:aws:iam::123456789012:role/test-role",
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.config.Sanitize()
			
			// Type assert the result
			athenaConfig, ok := result.(*AthenaConfiguration)
			if !ok {
				t.Fatalf("expected *AthenaConfiguration, got %T", result)
			}

			// Compare the sanitized config with expected
			if athenaConfig.Bucket != testCase.expected.Bucket {
				t.Errorf("Bucket mismatch: got %s, want %s", athenaConfig.Bucket, testCase.expected.Bucket)
			}
			if athenaConfig.Region != testCase.expected.Region {
				t.Errorf("Region mismatch: got %s, want %s", athenaConfig.Region, testCase.expected.Region)
			}
			if athenaConfig.Database != testCase.expected.Database {
				t.Errorf("Database mismatch: got %s, want %s", athenaConfig.Database, testCase.expected.Database)
			}
			if athenaConfig.Catalog != testCase.expected.Catalog {
				t.Errorf("Catalog mismatch: got %s, want %s", athenaConfig.Catalog, testCase.expected.Catalog)
			}
			if athenaConfig.Table != testCase.expected.Table {
				t.Errorf("Table mismatch: got %s, want %s", athenaConfig.Table, testCase.expected.Table)
			}
			if athenaConfig.Workgroup != testCase.expected.Workgroup {
				t.Errorf("Workgroup mismatch: got %s, want %s", athenaConfig.Workgroup, testCase.expected.Workgroup)
			}
			if athenaConfig.Account != testCase.expected.Account {
				t.Errorf("Account mismatch: got %s, want %s", athenaConfig.Account, testCase.expected.Account)
			}
			
			// Verify that the authorizer was also sanitized
			if athenaConfig.Authorizer == nil {
				t.Error("Authorizer should not be nil after sanitization")
			}
		})
	}
}

func TestAthenaConfiguration_Provider(t *testing.T) {
	config := AthenaConfiguration{
		Bucket:     "test-bucket",
		Region:     "us-west-2",
		Database:   "test-db",
		Table:      "test-table",
		Workgroup:  "test-workgroup",
		Account:    "123456789012",
		Authorizer: &ServiceAccount{},
	}

	provider := config.Provider()
	expectedProvider := "AWS"
	
	if provider != expectedProvider {
		t.Errorf("Provider() returned %s, expected %s", provider, expectedProvider)
	}
}

func TestAthenaConfiguration_Key(t *testing.T) {
	testCases := map[string]struct {
		config   AthenaConfiguration
		expected string
	}{
		"standard key": {
			config: AthenaConfiguration{
				Account: "123456789012",
				Bucket:  "test-bucket",
			},
			expected: "123456789012/test-bucket",
		},
		"empty account": {
			config: AthenaConfiguration{
				Account: "",
				Bucket:  "test-bucket",
			},
			expected: "/test-bucket",
		},
		"empty bucket": {
			config: AthenaConfiguration{
				Account: "123456789012",
				Bucket:  "",
			},
			expected: "123456789012/",
		},
		"both empty": {
			config: AthenaConfiguration{
				Account: "",
				Bucket:  "",
			},
			expected: "/",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.config.Key()
			if result != testCase.expected {
				t.Errorf("Key() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}
