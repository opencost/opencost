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
		"valid CUR version 1.0": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
				CURVersion: "1.0",
			},
			expected: nil,
		},
		"valid CUR version 2.0": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
				CURVersion: "2.0",
			},
			expected: nil,
		},
		"valid empty CUR version defaults to 2.0": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
				CURVersion: "",
			},
			expected: nil,
		},
		"invalid CUR version": {
			config: AthenaConfiguration{
				Bucket:     "bucket",
				Region:     "region",
				Database:   "database",
				Catalog:    "catalog",
				Table:      "table",
				Workgroup:  "workgroup",
				Account:    "account",
				Authorizer: &ServiceAccount{},
				CURVersion: "3.0",
			},
			expected: fmt.Errorf("AthenaConfiguration: invalid CURVersion '3.0', must be '1.0' or '2.0'"),
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
		"different CUR version": {
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
				CURVersion: "1.0",
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
				CURVersion: "2.0",
			},
			expected: false,
		},
		"matching CUR version": {
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
				CURVersion: "1.0",
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
				CURVersion: "1.0",
			},
			expected: true,
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
			config: AthenaConfiguration{
				CURVersion: "2.0", // Default value after JSON unmarshal
			},
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
				CURVersion: "2.0", // Default value after JSON unmarshal
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
				CURVersion: "2.0", // Default value after JSON unmarshal
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
				CURVersion: "2.0", // Default value after JSON unmarshal
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
				CURVersion: "2.0", // Default value after JSON unmarshal
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
				CURVersion: "2.0", // Default value after JSON unmarshal
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
				CURVersion: "2.0", // Default value after JSON unmarshal
			},
		},
		"CUR Version 1.0": {
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
				CURVersion: "1.0",
			},
		},
		"CUR Version 2.0": {
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
				CURVersion: "2.0",
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

func TestConvertAwsAthenaInfoToConfig(t *testing.T) {
	testCases := map[string]struct {
		input          AwsAthenaInfo
		expectNil      bool
		expectType     string
		expectAuthType string
	}{
		"empty config returns nil": {
			input:     AwsAthenaInfo{},
			expectNil: true,
		},
		"athena config with access key": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "access-key-id",
				ServiceKeySecret: "secret-key",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "AccessKey",
		},
		"athena config with service account (IRSA) - empty credentials": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "ServiceAccount",
		},
		"athena config with assume role wrapping access key": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "access-key-id",
				ServiceKeySecret: "secret-key",
				MasterPayerARN:   "arn:aws:iam::987654321098:role/cross-account-role",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "AssumeRole",
		},
		"athena config with assume role wrapping service account (IRSA)": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
				MasterPayerARN:   "arn:aws:iam::987654321098:role/cross-account-role",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "AssumeRole",
		},
		"s3 config (no table/database) with access key": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AccountID:        "123456789012",
				ServiceKeyName:   "access-key-id",
				ServiceKeySecret: "secret-key",
			},
			expectNil:      false,
			expectType:     "S3Configuration",
			expectAuthType: "AccessKey",
		},
		"s3 config with service account (IRSA)": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
			},
			expectNil:      false,
			expectType:     "S3Configuration",
			expectAuthType: "ServiceAccount",
		},
		"athena config with workgroup and catalog": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AthenaCatalog:    "test-catalog",
				AthenaWorkgroup:  "test-workgroup",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "ServiceAccount",
		},
		"athena config with CUR version 1.0": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
				CURVersion:       "1.0",
			},
			expectNil:      false,
			expectType:     "AthenaConfiguration",
			expectAuthType: "ServiceAccount",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ConvertAwsAthenaInfoToConfig(tc.input)

			if tc.expectNil {
				if result != nil {
					t.Errorf("expected nil result, got %T", result)
				}
				return
			}

			if result == nil {
				t.Fatal("expected non-nil result, got nil")
			}

			switch tc.expectType {
			case "AthenaConfiguration":
				ac, ok := result.(*AthenaConfiguration)
				if !ok {
					t.Errorf("expected *AthenaConfiguration, got %T", result)
					return
				}

				// Verify fields were copied correctly
				if ac.Bucket != tc.input.AthenaBucketName {
					t.Errorf("bucket mismatch: expected %s, got %s", tc.input.AthenaBucketName, ac.Bucket)
				}
				if ac.Region != tc.input.AthenaRegion {
					t.Errorf("region mismatch: expected %s, got %s", tc.input.AthenaRegion, ac.Region)
				}
				if ac.Database != tc.input.AthenaDatabase {
					t.Errorf("database mismatch: expected %s, got %s", tc.input.AthenaDatabase, ac.Database)
				}
				if ac.Table != tc.input.AthenaTable {
					t.Errorf("table mismatch: expected %s, got %s", tc.input.AthenaTable, ac.Table)
				}
				if ac.Catalog != tc.input.AthenaCatalog {
					t.Errorf("catalog mismatch: expected %s, got %s", tc.input.AthenaCatalog, ac.Catalog)
				}
				if ac.Workgroup != tc.input.AthenaWorkgroup {
					t.Errorf("workgroup mismatch: expected %s, got %s", tc.input.AthenaWorkgroup, ac.Workgroup)
				}
				if ac.Account != tc.input.AccountID {
					t.Errorf("account mismatch: expected %s, got %s", tc.input.AccountID, ac.Account)
				}

				// Check CUR version
				expectedCURVersion := tc.input.CURVersion
				if expectedCURVersion == "" {
					expectedCURVersion = "2.0"
				}
				if ac.CURVersion != expectedCURVersion {
					t.Errorf("CURVersion mismatch: expected %s, got %s", expectedCURVersion, ac.CURVersion)
				}

				// Verify authorizer type
				verifyAuthorizerType(t, ac.Authorizer, tc.expectAuthType, tc.input)

			case "S3Configuration":
				sc, ok := result.(*S3Configuration)
				if !ok {
					t.Errorf("expected *S3Configuration, got %T", result)
					return
				}

				// Verify fields were copied correctly
				if sc.Bucket != tc.input.AthenaBucketName {
					t.Errorf("bucket mismatch: expected %s, got %s", tc.input.AthenaBucketName, sc.Bucket)
				}
				if sc.Region != tc.input.AthenaRegion {
					t.Errorf("region mismatch: expected %s, got %s", tc.input.AthenaRegion, sc.Region)
				}
				if sc.Account != tc.input.AccountID {
					t.Errorf("account mismatch: expected %s, got %s", tc.input.AccountID, sc.Account)
				}

				// Verify authorizer type
				verifyAuthorizerType(t, sc.Authorizer, tc.expectAuthType, tc.input)

			default:
				t.Errorf("unexpected type: %s", tc.expectType)
			}
		})
	}
}

func verifyAuthorizerType(t *testing.T, auth Authorizer, expectType string, input AwsAthenaInfo) {
	t.Helper()

	switch expectType {
	case "AccessKey":
		ak, ok := auth.(*AccessKey)
		if !ok {
			t.Errorf("expected *AccessKey authorizer, got %T", auth)
			return
		}
		if ak.ID != input.ServiceKeyName {
			t.Errorf("access key ID mismatch: expected %s, got %s", input.ServiceKeyName, ak.ID)
		}
		if ak.Secret != input.ServiceKeySecret {
			t.Errorf("access key secret mismatch: expected %s, got %s", input.ServiceKeySecret, ak.Secret)
		}

	case "ServiceAccount":
		_, ok := auth.(*ServiceAccount)
		if !ok {
			t.Errorf("expected *ServiceAccount authorizer, got %T", auth)
		}

	case "AssumeRole":
		ar, ok := auth.(*AssumeRole)
		if !ok {
			t.Errorf("expected *AssumeRole authorizer, got %T", auth)
			return
		}
		if ar.RoleARN != input.MasterPayerARN {
			t.Errorf("role ARN mismatch: expected %s, got %s", input.MasterPayerARN, ar.RoleARN)
		}

		// Check the wrapped authorizer
		if input.ServiceKeyName == "" && input.ServiceKeySecret == "" {
			if _, ok := ar.Authorizer.(*ServiceAccount); !ok {
				t.Errorf("expected wrapped *ServiceAccount authorizer in AssumeRole, got %T", ar.Authorizer)
			}
		} else {
			if _, ok := ar.Authorizer.(*AccessKey); !ok {
				t.Errorf("expected wrapped *AccessKey authorizer in AssumeRole, got %T", ar.Authorizer)
			}
		}

	default:
		t.Errorf("unexpected authorizer type: %s", expectType)
	}
}

func TestConvertAwsAthenaInfoToConfig_AuthorizerCreateAWSConfig(t *testing.T) {
	// Test that the converted config's authorizer can create AWS configs
	testCases := map[string]struct {
		input       AwsAthenaInfo
		expectError bool
	}{
		"access key authorizer creates config": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "AKIAIOSFODNN7EXAMPLE",
				ServiceKeySecret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			expectError: false,
		},
		"service account authorizer creates config": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "",
				ServiceKeySecret: "",
			},
			expectError: false,
		},
		"invalid access key fails validation": {
			input: AwsAthenaInfo{
				AthenaBucketName: "test-bucket",
				AthenaRegion:     "us-east-1",
				AthenaDatabase:   "test-db",
				AthenaTable:      "test-table",
				AccountID:        "123456789012",
				ServiceKeyName:   "only-id-no-secret",
				ServiceKeySecret: "",
			},
			expectError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ConvertAwsAthenaInfoToConfig(tc.input)
			if result == nil {
				t.Fatal("expected non-nil result")
			}

			ac, ok := result.(*AthenaConfiguration)
			if !ok {
				t.Fatalf("expected *AthenaConfiguration, got %T", result)
			}

			_, err := ac.Authorizer.CreateAWSConfig(tc.input.AthenaRegion)
			if tc.expectError && err == nil {
				t.Error("expected error but got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
