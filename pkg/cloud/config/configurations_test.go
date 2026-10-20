package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/oracle"
)

var (
	azureMultiCloudConf = MultiCloudConfig{
		AzureConfigs: []azure.AzureStorageConfig{
			{
				SubscriptionId: "subscriptionID",
				AccountName:    "accountName",
				AccessKey:      "accessKey",
				ContainerName:  "containerName",
				ContainerPath:  "containerPath",
				AzureCloud:     "azureCloud",
			},
		},
	}
	azureConfiguration = &Configurations{
		Azure: &AzureConfigs{
			Storage: []*azure.StorageConfiguration{
				{
					SubscriptionID: "subscriptionID",
					Account:        "accountName",
					Container:      "containerName",
					Path:           "containerPath",
					Cloud:          "azureCloud",
					Authorizer: &azure.SharedKeyCredential{
						AccessKey: "accessKey",
						Account:   "accountName",
					},
				},
			},
		},
	}

	GCPKeyMultiCloudConf = MultiCloudConfig{
		GCPConfigs: []gcp.BigQueryConfig{
			{
				ProjectID:          "projectID",
				BillingDataDataset: "dataset.table",
				Key: map[string]string{
					"key": "value",
				},
			},
		},
	}

	GCPKeyConfigurations = Configurations{
		GCP: &GCPConfigs{BigQuery: []*gcp.BigQueryConfiguration{{
			ProjectID: "projectID",
			Dataset:   "dataset",
			Table:     "table",
			Authorizer: &gcp.ServiceAccountKey{
				Key: map[string]string{
					"key": "value",
				},
			},
		},
		}},
	}

	GCPWIMultiCloudConf = MultiCloudConfig{
		GCPConfigs: []gcp.BigQueryConfig{
			{
				ProjectID:          "projectID",
				BillingDataDataset: "dataset.table",
				Key:                nil,
			},
		},
	}

	GCPWIConfigurations = Configurations{
		GCP: &GCPConfigs{BigQuery: []*gcp.BigQueryConfiguration{{
			ProjectID:  "projectID",
			Dataset:    "dataset",
			Table:      "table",
			Authorizer: &gcp.WorkloadIdentity{},
		},
		}},
	}

	AWSAthenaKeyMultiCloudConfig = MultiCloudConfig{
		AWSConfigs: []aws.AwsAthenaInfo{
			{
				AthenaBucketName: "bucket",
				AthenaRegion:     "region",
				AthenaDatabase:   "database",
				AthenaTable:      "table",
				AthenaWorkgroup:  "workgroup",
				ServiceKeyName:   "id",
				ServiceKeySecret: "secret",
				AccountID:        "account",
				MasterPayerARN:   "",
			},
		},
	}

	AWSAthenaKeyConfigurations = &Configurations{
		AWS: &AWSConfigs{
			Athena: []*aws.AthenaConfiguration{
				{
					Bucket:    "bucket",
					Region:    "region",
					Database:  "database",
					Table:     "table",
					Workgroup: "workgroup",
					Account:   "account",
					Authorizer: &aws.AccessKey{
						ID:     "id",
						Secret: "secret",
					},
				},
			},
		},
	}

	AWSAthenaAssumeRoleServiceAccountMultiCloudConfig = MultiCloudConfig{
		AWSConfigs: []aws.AwsAthenaInfo{
			{
				AthenaBucketName: "bucket",
				AthenaRegion:     "region",
				AthenaDatabase:   "database",
				AthenaTable:      "table",
				AthenaWorkgroup:  "workgroup",
				AccountID:        "account",
				MasterPayerARN:   "roleArn",
			},
		},
	}

	AWSAthenaAssumeRoleServiceAccountConfigurations = &Configurations{
		AWS: &AWSConfigs{
			Athena: []*aws.AthenaConfiguration{
				{
					Bucket:    "bucket",
					Region:    "region",
					Database:  "database",
					Table:     "table",
					Workgroup: "workgroup",
					Account:   "account",
					Authorizer: &aws.AssumeRole{
						Authorizer: &aws.ServiceAccount{},
						RoleARN:    "roleArn",
					},
				},
			},
		},
	}
	AWSS3ServiceAccountMultiCloudConfig = MultiCloudConfig{
		AWSConfigs: []aws.AwsAthenaInfo{
			{
				AthenaBucketName: "bucket",
				AthenaRegion:     "region",
				AccountID:        "account",
				MasterPayerARN:   "",
			},
		},
	}

	AWSS3ServiceAccountConfigurations = &Configurations{
		AWS: &AWSConfigs{
			S3: []*aws.S3Configuration{
				{
					Bucket:     "bucket",
					Region:     "region",
					Account:    "account",
					Authorizer: &aws.ServiceAccount{},
				},
			},
		},
	}

	AWSS3AssumeRoleAccessKeyMultiCloudConfig = MultiCloudConfig{
		AWSConfigs: []aws.AwsAthenaInfo{
			{
				AthenaBucketName: "bucket",
				AthenaRegion:     "region",
				AccountID:        "account",
				ServiceKeyName:   "id",
				ServiceKeySecret: "secret",
				MasterPayerARN:   "roleARN",
			},
		},
	}
	AWSS3AssumeRoleAccessKeyConfigurations = &Configurations{
		AWS: &AWSConfigs{
			S3: []*aws.S3Configuration{
				{
					Bucket:  "bucket",
					Region:  "region",
					Account: "account",
					Authorizer: &aws.AssumeRole{
						Authorizer: &aws.AccessKey{
							ID:     "id",
							Secret: "secret",
						},
						RoleARN: "roleARN",
					},
				},
			},
		},
	}

	OCIUsageAPIConfigurations = &Configurations{
		OCI: &OCIConfigs{
			UsageAPI: []*oracle.UsageApiConfiguration{
				{
					TenancyID: "ocid1.tenancy.oc1..tenancyID",
					Region:    "us-ashburn-1",
					Authorizer: &oracle.RawConfigProvider{
						TenancyID:   "ocid1.tenancy.oc1..tenancyID",
						UserID:      "ocid1.user.oc1..userID",
						Region:      "us-ashburn-1",
						Fingerprint: "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00",
						PrivateKey:  "dummy-private-key",
					},
				},
			},
		},
	}
)

func TestConfigurations_UnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		input    any
		expected *Configurations
	}{
		"Azure Storage SharedKeyCredential": {
			input:    azureConfiguration,
			expected: azureConfiguration,
		},
		"Azure Storage SharedKeyCredential Conversion": {
			input:    azureMultiCloudConf,
			expected: azureConfiguration,
		},
		"GCP BigQuery ServiceAccountKey": {
			input:    GCPKeyConfigurations,
			expected: &GCPKeyConfigurations,
		},
		"GCP BigQuery ServiceAccountKey Conversion": {
			input:    GCPKeyMultiCloudConf,
			expected: &GCPKeyConfigurations,
		},
		"GCP BigQuery Workload Identity ": {
			input:    &GCPWIConfigurations,
			expected: &GCPWIConfigurations,
		},
		"GCP BigQuery Workload Identity Conversion": {
			input:    GCPWIMultiCloudConf,
			expected: &GCPWIConfigurations,
		},
		"AWS Athena Access Key": {
			input:    AWSAthenaKeyConfigurations,
			expected: AWSAthenaKeyConfigurations,
		},
		"AWS Athena Access Key Conversion": {
			input:    AWSAthenaKeyMultiCloudConfig,
			expected: AWSAthenaKeyConfigurations,
		},
		"AWS Athena Assume Role Service Account": {
			input:    AWSAthenaAssumeRoleServiceAccountConfigurations,
			expected: AWSAthenaAssumeRoleServiceAccountConfigurations,
		},
		"AWS Athena Assume Role Service Account Conversion": {
			input:    AWSAthenaAssumeRoleServiceAccountMultiCloudConfig,
			expected: AWSAthenaAssumeRoleServiceAccountConfigurations,
		},
		"AWS S3 Service Account": {
			input:    AWSS3ServiceAccountConfigurations,
			expected: AWSS3ServiceAccountConfigurations,
		},
		"AWS S3 Service Account Conversion": {
			input:    AWSS3ServiceAccountMultiCloudConfig,
			expected: AWSS3ServiceAccountConfigurations,
		},
		"AWS S3 Assume Role Access Key": {
			input:    AWSS3AssumeRoleAccessKeyConfigurations,
			expected: AWSS3AssumeRoleAccessKeyConfigurations,
		},
		"AWS S3 Assume Role Service Access Key": {
			input:    AWSS3AssumeRoleAccessKeyMultiCloudConfig,
			expected: AWSS3AssumeRoleAccessKeyConfigurations,
		},
		"OCI UsageAPI RawConfigProvider": {
			input:    OCIUsageAPIConfigurations,
			expected: OCIUsageAPIConfigurations,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("failed to marshal input")
			}
			actual := &Configurations{}
			err = json.Unmarshal(b, actual)
			if err != nil && tt.expected != nil {
				t.Fatalf("Unmarshal failed with error %s", err.Error())
			}
			if !tt.expected.Equals(actual) {
				t.Fatalf("actual Configuration did not match expected")
			}
		})
	}
}

func TestConfigurations_UnmarshalJSON_Issue4048(t *testing.T) {
	expected := &Configurations{
		AWS: &AWSConfigs{
			Athena: []*aws.AthenaConfiguration{
				{
					Bucket:     "s3://my-cost-usage-report-bucket",
					Region:     "eu-west-1",
					Database:   "cost_and_usage_report",
					Catalog:    "AwsDataCatalog",
					Table:      "cost_and_usage_report",
					Workgroup:  "primary",
					Account:    "123456789012",
					Authorizer: &aws.ServiceAccount{},
				},
			},
		},
	}

	testCases := map[string]string{
		"legacy MultiCloudConfig format from issue 4048": `{
  "aws": [
    {
      "athenaBucketName": "s3://my-cost-usage-report-bucket",
      "athenaRegion": "eu-west-1",
      "athenaDatabase": "cost_and_usage_report",
      "athenaCatalog": "AwsDataCatalog",
      "athenaTable": "cost_and_usage_report",
      "athenaWorkgroup": "primary",
      "projectID": "123456789012"
    }
  ]
}`,
		"legacy MultiCloudConfig with empty provider arrays before active provider": `{
  "azure": [],
  "gcp": [],
  "aws": [
    {
      "athenaBucketName": "s3://my-cost-usage-report-bucket",
      "athenaRegion": "eu-west-1",
      "athenaDatabase": "cost_and_usage_report",
      "athenaCatalog": "AwsDataCatalog",
      "athenaTable": "cost_and_usage_report",
      "athenaWorkgroup": "primary",
      "projectID": "123456789012"
    }
  ],
  "alibaba": []
}`,
		"legacy MultiCloudConfig with uppercase provider keys": `{
  "Azure": [],
  "GCP": [],
  "AWS": [
    {
      "athenaBucketName": "s3://my-cost-usage-report-bucket",
      "athenaRegion": "eu-west-1",
      "athenaDatabase": "cost_and_usage_report",
      "athenaCatalog": "AwsDataCatalog",
      "athenaTable": "cost_and_usage_report",
      "athenaWorkgroup": "primary",
      "projectID": "123456789012"
    }
  ],
  "Alibaba": []
}`,
		"new Configurations format": `{
  "aws": {
    "athena": [
      {
        "bucket": "s3://my-cost-usage-report-bucket",
        "region": "eu-west-1",
        "database": "cost_and_usage_report",
        "catalog": "AwsDataCatalog",
        "table": "cost_and_usage_report",
        "workgroup": "primary",
        "account": "123456789012",
        "authorizer": {
          "authorizerType": "AWSServiceAccount"
        }
      }
    ]
  }
}`,
	}

	for name, rawJSON := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := &Configurations{
				Azure: azureConfiguration.Azure,
			}
			if err := json.Unmarshal([]byte(rawJSON), actual); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if !expected.Equals(actual) {
				t.Fatalf("unmarshalled Configurations did not match expected: got %+v, want %+v", actual, expected)
			}

			configPath := filepath.Join(t.TempDir(), "cloud-integration.json")
			if err := os.WriteFile(configPath, []byte(rawJSON), 0600); err != nil {
				t.Fatalf("failed to write temp cloud-integration.json: %v", err)
			}
			loaded := new(Configurations)
			if err := loadFile(configPath, loaded); err != nil {
				t.Fatalf("loadFile failed: %v", err)
			}
			if !expected.Equals(loaded) {
				t.Fatalf("loaded Configurations did not match expected: got %+v, want %+v", loaded, expected)
			}
		})
	}

	t.Run("invalid non-legacy provider returns error and preserves receiver", func(t *testing.T) {
		for _, payload := range []string{`{"oci":"invalid"}`, `{"aws":[{}],"oci":"invalid"}`} {
			actual := &Configurations{
				Azure: azureConfiguration.Azure,
			}
			err := json.Unmarshal([]byte(payload), actual)
			if err == nil {
				t.Fatalf("expected error unmarshalling %s, got nil", payload)
			}
			if !strings.Contains(err.Error(), "oci") {
				t.Fatalf("expected error for %s to identify oci field, got: %v", payload, err)
			}
			if !azureConfiguration.Equals(actual) {
				t.Fatalf("receiver was mutated on failed unmarshal (%s): got %+v, want %+v", payload, actual, azureConfiguration)
			}
		}
	})
}


