package aws

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
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
	config := &AthenaConfiguration{
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
	}

	sanitized, ok := config.Sanitize().(*AthenaConfiguration)
	if !ok {
		t.Fatalf("Sanitize() did not return an *AthenaConfiguration")
	}

	if sanitized.Bucket != config.Bucket ||
		sanitized.Region != config.Region ||
		sanitized.Database != config.Database ||
		sanitized.Catalog != config.Catalog ||
		sanitized.Table != config.Table ||
		sanitized.Workgroup != config.Workgroup ||
		sanitized.Account != config.Account {
		t.Errorf("Sanitize() altered a non-secret field: %+v", sanitized)
	}

	ak, ok := sanitized.Authorizer.(*AccessKey)
	if !ok {
		t.Fatalf("Sanitize() did not preserve the *AccessKey authorizer type")
	}
	if ak.ID != "id" {
		t.Errorf("Sanitize() altered the authorizer ID: got %q, want %q", ak.ID, "id")
	}
	if ak.Secret != cloud.Redacted {
		t.Errorf("Sanitize() did not redact the authorizer secret: got %q, want %q", ak.Secret, cloud.Redacted)
	}

	if original, _ := config.Authorizer.(*AccessKey); original.Secret != "secret" {
		t.Errorf("Sanitize() mutated the original authorizer secret: got %q, want %q", original.Secret, "secret")
	}
}

func TestAthenaConfiguration_Key(t *testing.T) {
	testCases := map[string]struct {
		config   *AthenaConfiguration
		expected string
	}{
		"account and bucket set": {
			config:   &AthenaConfiguration{Account: "123456789012", Bucket: "my-bucket"},
			expected: "123456789012/my-bucket",
		},
		"empty fields": {
			config:   &AthenaConfiguration{},
			expected: "/",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if actual := tc.config.Key(); actual != tc.expected {
				t.Errorf("Key() = %q, want %q", actual, tc.expected)
			}
		})
	}
}

func TestAthenaConfiguration_Provider(t *testing.T) {
	config := &AthenaConfiguration{}
	if actual := config.Provider(); actual != opencost.AWSProvider {
		t.Errorf("Provider() = %q, want %q", actual, opencost.AWSProvider)
	}
}

func TestConvertAwsAthenaInfoToConfig(t *testing.T) {
	t.Run("empty info returns nil", func(t *testing.T) {
		if config := ConvertAwsAthenaInfoToConfig(AwsAthenaInfo{}); config != nil {
			t.Errorf("ConvertAwsAthenaInfoToConfig() = %v, want nil", config)
		}
	})

	t.Run("access key with athena table builds AthenaConfiguration", func(t *testing.T) {
		info := AwsAthenaInfo{
			AthenaBucketName: "bucket",
			AthenaRegion:     "region",
			AthenaDatabase:   "database",
			AthenaCatalog:    "catalog",
			AthenaTable:      "table",
			AthenaWorkgroup:  "workgroup",
			ServiceKeyName:   "id",
			ServiceKeySecret: "secret",
			AccountID:        "account",
		}

		config := ConvertAwsAthenaInfoToConfig(info)
		ac, ok := config.(*AthenaConfiguration)
		if !ok {
			t.Fatalf("ConvertAwsAthenaInfoToConfig() = %T, want *AthenaConfiguration", config)
		}
		if ac.Bucket != "bucket" || ac.Region != "region" || ac.Database != "database" ||
			ac.Catalog != "catalog" || ac.Table != "table" || ac.Workgroup != "workgroup" || ac.Account != "account" {
			t.Errorf("ConvertAwsAthenaInfoToConfig() mapped fields incorrectly: %+v", ac)
		}
		ak, ok := ac.Authorizer.(*AccessKey)
		if !ok {
			t.Fatalf("authorizer = %T, want *AccessKey", ac.Authorizer)
		}
		if ak.ID != "id" || ak.Secret != "secret" {
			t.Errorf("AccessKey = %+v, want {ID: id, Secret: secret}", ak)
		}
	})

	t.Run("no keys uses ServiceAccount authorizer", func(t *testing.T) {
		info := AwsAthenaInfo{
			AthenaBucketName: "bucket",
			AthenaDatabase:   "database",
			AccountID:        "account",
		}

		config := ConvertAwsAthenaInfoToConfig(info)
		ac, ok := config.(*AthenaConfiguration)
		if !ok {
			t.Fatalf("ConvertAwsAthenaInfoToConfig() = %T, want *AthenaConfiguration", config)
		}
		if _, ok := ac.Authorizer.(*ServiceAccount); !ok {
			t.Errorf("authorizer = %T, want *ServiceAccount", ac.Authorizer)
		}
	})

	t.Run("master payer arn wraps authorizer in AssumeRole", func(t *testing.T) {
		info := AwsAthenaInfo{
			AthenaBucketName: "bucket",
			AthenaTable:      "table",
			ServiceKeyName:   "id",
			ServiceKeySecret: "secret",
			MasterPayerARN:   "arn:aws:iam::123456789012:role/payer",
		}

		config := ConvertAwsAthenaInfoToConfig(info)
		ac, ok := config.(*AthenaConfiguration)
		if !ok {
			t.Fatalf("ConvertAwsAthenaInfoToConfig() = %T, want *AthenaConfiguration", config)
		}
		assumeRole, ok := ac.Authorizer.(*AssumeRole)
		if !ok {
			t.Fatalf("authorizer = %T, want *AssumeRole", ac.Authorizer)
		}
		if assumeRole.RoleARN != info.MasterPayerARN {
			t.Errorf("AssumeRole.RoleARN = %q, want %q", assumeRole.RoleARN, info.MasterPayerARN)
		}
		if _, ok := assumeRole.Authorizer.(*AccessKey); !ok {
			t.Errorf("wrapped authorizer = %T, want *AccessKey", assumeRole.Authorizer)
		}
	})

	t.Run("no table or database builds S3Configuration", func(t *testing.T) {
		info := AwsAthenaInfo{
			AthenaBucketName: "bucket",
			AthenaRegion:     "region",
			AccountID:        "account",
		}

		config := ConvertAwsAthenaInfoToConfig(info)
		s3, ok := config.(*S3Configuration)
		if !ok {
			t.Fatalf("ConvertAwsAthenaInfoToConfig() = %T, want *S3Configuration", config)
		}
		if s3.Bucket != "bucket" || s3.Region != "region" || s3.Account != "account" {
			t.Errorf("ConvertAwsAthenaInfoToConfig() mapped S3 fields incorrectly: %+v", s3)
		}
		if _, ok := s3.Authorizer.(*ServiceAccount); !ok {
			t.Errorf("authorizer = %T, want *ServiceAccount", s3.Authorizer)
		}
	})
}
