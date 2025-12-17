package config

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
)

func Test_ParseConfig_InvalidType(t *testing.T) {
	body := strings.NewReader("{}")

	_, err := ParseConfig("invalid_type", body)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func Test_ParseConfig_S3(t *testing.T) {
	config := &aws.S3Configuration{
		Bucket:  "bucket",
		Region:  "region",
		Account: "account",
		Authorizer: &aws.AccessKey{
			ID:     "id",
			Secret: "secret",
		},
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(S3ConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !reflect.DeepEqual(config, parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}
}

func Test_ParseConfig_Athena(t *testing.T) {
	config := &aws.AthenaConfiguration{
		Bucket:    "bucket",
		Region:    "region",
		Database:  "database",
		Catalog:   "catalog",
		Table:     "table",
		Workgroup: "workgroup",
		Account:   "account",
		Authorizer: &aws.AccessKey{
			ID:     "id",
			Secret: "secret",
		},
		CURVersion: "curversion",
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(AthenaConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !reflect.DeepEqual(config, parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}
}

func Test_ParseConfig_BigQuery(t *testing.T) {
	config := &gcp.BigQueryConfiguration{
		ProjectID:            "projectid",
		Dataset:              "dataset",
		Table:                "table",
		ExcludePartitionTime: false,
		Authorizer: &gcp.ServiceAccountKey{
			Key: map[string]string{
				"key": "value",
			},
		},
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(BigQueryConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !reflect.DeepEqual(config, parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}
}

func Test_ParseConfig_Azure(t *testing.T) {
	config := &azure.StorageConfiguration{
		SubscriptionID: "subscriptionid",
		Account:        "account",
		Container:      "container",
		Path:           "path",
		Cloud:          "cloud",
		Authorizer: &azure.SharedKeyCredential{
			AccessKey: "accesskey",
			Account:   "account",
		},
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(AzureStorageConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !reflect.DeepEqual(config, parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}
}
