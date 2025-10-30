package azure

import (
	"reflect"
	"testing"

	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestStorageConnectionStringCredential_Validate(t *testing.T) {
	tests := map[string]struct {
		input   *StorageConnectionStringCredential
		wantErr bool
	}{
		"missing StorageConnectionString": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "",
			},
			wantErr: true,
		},
		"valid": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if err := tt.input.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStorageConnectionStringCredential_Sanitize(t *testing.T) {

	tests := map[string]struct {
		input *StorageConnectionStringCredential
		want  cloud.Config
	}{
		"Plain integration": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			want: &StorageConnectionStringCredential{
				StorageConnectionString: cloud.Redacted,
				HTTPConfig:              defaultHTTPConfig,
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.input.Sanitize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Sanitize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageConnectionStringCredential_Equals(t *testing.T) {
	tests := map[string]struct {
		input  *StorageConnectionStringCredential
		config cloud.Config
		want   bool
	}{
		"compare nil": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			config: nil,
			want:   false,
		},
		"different config": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			config: &StorageConnectionStringCredential{},
			want:   false,
		},
		"different StorageConnectionString": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			config: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString2",
				HTTPConfig:              defaultHTTPConfig,
			},
			want: false,
		},
		"different HTTPConfig": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			config: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig: storage.HTTPConfig{
					InsecureSkipVerify: true,
				},
			},
			want: false,
		},
		"equal": {
			input: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			config: &StorageConnectionStringCredential{
				StorageConnectionString: "StorageConnectionString",
				HTTPConfig:              defaultHTTPConfig,
			},
			want: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.input.Equals(tt.config); got != tt.want {
				t.Errorf("Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}