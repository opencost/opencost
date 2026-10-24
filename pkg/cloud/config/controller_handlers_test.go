package config

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/huawei"
	"github.com/opencost/opencost/pkg/cloud/ibm"
	"github.com/opencost/opencost/pkg/env"
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

func Test_ParseConfig_IBM(t *testing.T) {
	config := &ibm.UsageConfiguration{
		AccountID: "account-id",
		Authorizer: &ibm.APIKey{
			Key: "api-key",
		},
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(IBMUsageConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !config.Equals(parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}
}

func Test_GetAddConfigHandler(t *testing.T) {
	controller := &Controller{
		storage: &MemoryControllerStorage{},
	}

	handler := controller.GetAddConfigHandler()
	if handler == nil {
		t.Fatalf("expected handler, got nil")
	}

	// Test no type param
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler(w, req, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 status code, got %v: %v", w.Code, w.Body.String())
	}

	// Test no config body
	req = httptest.NewRequest("GET", "/?type="+S3ConfigType, nil)
	w = httptest.NewRecorder()
	handler(w, req, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 status code, got %v: %v", w.Code, w.Body.String())
	}

	// Test with config body
	mockConfig := aws.S3Configuration{
		Bucket:  "bucket",
		Region:  "region",
		Account: "account",
		Authorizer: &aws.AccessKey{
			ID:     "id",
			Secret: "secret",
		},
	}
	configBytes, err := json.Marshal(mockConfig)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	req = httptest.NewRequest("GET", "/?type="+S3ConfigType, bytes.NewReader(configBytes))
	w = httptest.NewRecorder()
	handler(w, req, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 status code, got %v: %v", w.Code, w.Body.String())
	}
}

func Test_ParseConfig_Huawei(t *testing.T) {
	config := &huawei.CostConfiguration{
		ProjectID: "project-id",
		Region:    "la-south-2",
		EnterpriseProjects: []huawei.EnterpriseProject{
			{ID: "0", Name: "default"},
		},
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	parsedConfig, err := ParseConfig(HuaweiCostConfigType, bytes.NewReader(configBytes))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}

	if !config.Equals(parsedConfig) {
		t.Fatalf("parsed config does not match original config:\n%+v\n%+v", parsedConfig, config)
	}

	// projectID is optional, so a region-only body must parse too.
	regionOnly, err := ParseConfig(HuaweiCostConfigType, strings.NewReader(`{"region":"la-south-2"}`))
	if err != nil {
		t.Fatalf("failed to parse a region-only config: %v", err)
	}
	if !regionOnly.Equals(&huawei.CostConfiguration{Region: "la-south-2"}) {
		t.Fatalf("unexpected region-only config: %+v", regionOnly)
	}
}

func Test_HuaweiConfigAddAndUpdateHandlers(t *testing.T) {
	t.Setenv(env.HuaweiDomainIDEnvVar, "domain-id")
	controller := &Controller{
		storage: &MemoryControllerStorage{},
	}

	send := func(handler func(http.ResponseWriter, *http.Request, httprouter.Params), config huawei.CostConfiguration) *httptest.ResponseRecorder {
		configBytes, err := json.Marshal(config)
		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}
		req := httptest.NewRequest("GET", "/?type="+HuaweiCostConfigType, bytes.NewReader(configBytes))
		w := httptest.NewRecorder()
		handler(w, req, nil)
		return w
	}

	add := controller.GetAddConfigHandler()
	if w := send(add, huawei.CostConfiguration{ProjectID: "project-a", Region: "la-south-2"}); w.Code != http.StatusOK {
		t.Fatalf("expected 200 adding a Huawei config, got %v: %v", w.Code, w.Body.String())
	}

	// Another project or region of the same account is the same integration:
	// the bill is account-wide, so it must not be added a second time.
	if w := send(add, huawei.CostConfiguration{ProjectID: "project-b", Region: "cn-north-4"}); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 adding a second config for the same account, got %v: %v", w.Code, w.Body.String())
	}

	update := controller.GetUpdateConfigHandler()
	if w := send(update, huawei.CostConfiguration{ProjectID: "project-b", Region: "cn-north-4"}); w.Code != http.StatusOK {
		t.Fatalf("expected 200 updating the Huawei config, got %v: %v", w.Code, w.Body.String())
	}
}

// Test_HuaweiConfigSurvivesDomainIDChange covers a Huawei configuration added
// through the API before HUAWEICLOUD_DOMAIN_ID is set, as when the domain ID
// is added on a restart: the controller must still find it under the key it
// was stored with, to update and to disable it.
func Test_HuaweiConfigSurvivesDomainIDChange(t *testing.T) {
	t.Setenv(env.HuaweiDomainIDEnvVar, "")
	controller := &Controller{
		storage: &MemoryControllerStorage{},
	}

	original := &huawei.CostConfiguration{Region: "la-south-2"}
	if err := controller.CreateConfig(original); err != nil {
		t.Fatalf("unexpected error creating the config: %v", err)
	}
	storedKey := original.Key()

	t.Setenv(env.HuaweiDomainIDEnvVar, "domain-id")

	if err := controller.UpdateConfig(&huawei.CostConfiguration{Region: "la-north-2"}); err != nil {
		t.Fatalf("expected the stored config to be found for update after the domain ID change: %v", err)
	}
	if err := controller.DisableConfig(storedKey, ConfigControllerSource.String()); err != nil {
		t.Fatalf("expected the stored config to be disabled after the domain ID change: %v", err)
	}
	if got := (&huawei.CostConfiguration{Region: "la-north-2"}).Key(); got != storedKey {
		t.Fatalf("the key changed with the domain ID: stored %q, now %q", storedKey, got)
	}
}
