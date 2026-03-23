package config

import (
	"encoding/json"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
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
