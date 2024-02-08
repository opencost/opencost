package config

import (
	"os"
	"path/filepath"
	"testing"

	cloudconfig "github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/env"
)

// Baseline valid config
var validAthenaConf = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup",
	Account:    "account",
	Authorizer: &aws.ServiceAccount{},
}

// Config with the same key as the baseline but is not equal to it because of the change in the non-keyed property Workgroup
var validAthenaConfModifiedProperty = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup1",
	Account:    "account",
	Authorizer: &aws.ServiceAccount{},
}

// Config with the same key as baseline but is invalid due to missing Authorizer
var invalidAthenaConf = &aws.AthenaConfiguration{
	Bucket:     "bucket",
	Region:     "region",
	Database:   "database",
	Table:      "table",
	Workgroup:  "workgroup",
	Account:    "account",
	Authorizer: nil,
}

// A valid config with a different key from the baseline
var validBigQueryConf = &gcp.BigQueryConfiguration{
	ProjectID:  "projectID",
	Dataset:    "dataset",
	Table:      "table",
	Authorizer: &gcp.WorkloadIdentity{},
}

func TestIntegrationController_pullWatchers(t *testing.T) {
	testCases := map[string]struct {
		initialStatuses  []*Status
		configWatchers   map[ConfigSource]cloudconfig.KeyedConfigWatcher
		expectedStatuses []*Status
	}{
		// Helm Source
		"Helm Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source No Change": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source Update Config": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Helm Source Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Helm Source New Config": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false, // this value changed
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		// Config File
		"Config File Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File No Change": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File Update Config": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Config File Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Config File New Config": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false, // this value changed
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		// Multi Cloud
		"Multi Cloud Source init": {
			initialStatuses: []*Status{},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Multi Cloud No Change": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
		},
		"Multi Cloud Update Config": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
			},
		},
		"Multi Cloud Update Config Invalid": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Multi Cloud New Config": {
			initialStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				MultiCloudSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     MultiCloudSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     MultiCloudSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		// Watch Interaction
		"New Helm, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"Update Helm, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"New Helm Invalid, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Update Helm Invalid, Existing Config File": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     HelmSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"New Config File, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"Update Config File, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validAthenaConfModifiedProperty,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConfModifiedProperty.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConfModifiedProperty,
				},
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
		},
		"New Config File Invalid, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
		"Update Config File Invalid, Existing Helm": {
			initialStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        validAthenaConf.Key(),
					Active:     false,
					Valid:      true,
					ConfigType: AthenaConfigType,
					Config:     validAthenaConf,
				},
			},
			configWatchers: map[ConfigSource]cloudconfig.KeyedConfigWatcher{
				HelmSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						validBigQueryConf,
					},
				},
				ConfigFileSource: &MockKeyedConfigWatcher{
					Integrations: []cloudconfig.KeyedConfig{
						invalidAthenaConf,
					},
				},
			},
			expectedStatuses: []*Status{
				{
					Source:     HelmSource,
					Key:        validBigQueryConf.Key(),
					Active:     true,
					Valid:      true,
					ConfigType: BigQueryConfigType,
					Config:     validBigQueryConf,
				},
				{
					Source:     ConfigFileSource,
					Key:        invalidAthenaConf.Key(),
					Active:     false,
					Valid:      false,
					ConfigType: AthenaConfigType,
					Config:     invalidAthenaConf,
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Test set up and validation
			initialStatuses := Statuses{}
			for _, status := range tc.initialStatuses {
				if _, ok := initialStatuses.Get(status.Key, status.Source); ok {
					t.Errorf("invalid test, duplicate initial status with key: %s source: %s", status.Key, status.Source.String())
				}
				initialStatuses.Insert(status)
			}

			expectedStatuses := Statuses{}
			for _, status := range tc.expectedStatuses {
				if _, ok := expectedStatuses.Get(status.Key, status.Source); ok {
					t.Errorf("invalid test, duplicate expected status with key: %s source: %s", status.Key, status.Source.String())
				}
				expectedStatuses.Insert(status)
			}

			tempDir := os.TempDir()
			os.Setenv(env.ConfigPathEnvVar, tempDir)
			defer os.Remove(filepath.Join(tempDir, configFile))
			// Initialize controller
			icd := &Controller{
				watchers: tc.configWatchers,
			}
			icd.save(initialStatuses)
			icd.pullWatchers()
			status, err := icd.load()
			if err != nil {
				t.Errorf("failed to load status file: %s", err.Error())
			}
			if len(status.List()) != len(expectedStatuses.List()) {
				t.Errorf("integration statueses did not have the correct length actaul: %d, expected: %d", len(status), len(tc.expectedStatuses))
			}

			for _, actualStatus := range status.List() {
				expectedStatus, ok := expectedStatuses.Get(actualStatus.Key, actualStatus.Source)
				if !ok {
					t.Errorf("expected integration statuses is missing with integration key: %s, source: %s", actualStatus.Key, actualStatus.Source.String())
				}

				// failure here indicates an issue with the configID
				if actualStatus.Key != expectedStatus.Key {
					t.Errorf("integration status does not have the correct Key values actual: %s, expected: %s", actualStatus.Key, expectedStatus.Key)
				}

				// failure here indicates an issue with the configID
				if actualStatus.Key != expectedStatus.Key {
					t.Errorf("integration status does not have the correct Source values actual: %s, expected: %s", actualStatus.Source, expectedStatus.Source)
				}

				if actualStatus.Active != expectedStatus.Active {
					t.Errorf("integration status does not have the correct Active values actual: %v, expected: %v", actualStatus.Active, expectedStatus.Active)
				}

				if actualStatus.Valid != expectedStatus.Valid {
					t.Errorf("integration status does not have the correct Valid values actual: %v, expected: %v", actualStatus.Valid, expectedStatus.Valid)
				}

				if !actualStatus.Config.Equals(expectedStatus.Config) {
					t.Errorf("integration status does not have the correct config values actual: %v, expected: %v", actualStatus.Config, expectedStatus.Config)
				}
			}
		})
	}
}
