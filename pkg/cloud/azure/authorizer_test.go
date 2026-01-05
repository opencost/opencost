package azure

import (
	"reflect"
	"testing"

	"github.com/opencost/opencost/pkg/cloud"
)

func TestClientSecretCredential_Validate(t *testing.T) {
	tests := map[string]struct {
		csc     *ClientSecretCredential
		wantErr bool
	}{
		"missing TenantID": {
			csc: &ClientSecretCredential{
				TenantID:     "",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			wantErr: true,
		},
		"missing ClientID": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "",
				ClientSecret: "clientSecret",
			},
			wantErr: true,
		},
		"missing ClientSecret": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "",
			},
			wantErr: true,
		},
		"valid": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if err := tt.csc.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientSecretCredential_Sanitize(t *testing.T) {

	tests := map[string]struct {
		csc  *ClientSecretCredential
		want cloud.Config
	}{
		"Plain integration": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			want: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: cloud.Redacted,
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.csc.Sanitize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Sanitize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientSecretCredential_Equals(t *testing.T) {
	tests := map[string]struct {
		csc    *ClientSecretCredential
		config cloud.Config
		want   bool
	}{
		"compare nil": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			config: nil,
			want:   false,
		},
		"different config": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			config: &DefaultAzureCredentialHolder{},
			want:   false,
		},
		"different TenantID": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			config: &ClientSecretCredential{
				TenantID:     "tenantID2",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			want: false,
		},
		"different ClientID": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			config: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID2",
				ClientSecret: "clientSecret",
			},
			want: false,
		},
		"different ClientSecret": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret2",
			},
			config: &ClientSecretCredential{
				TenantID:     "tenantID2",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			want: false,
		},
		"equal": {
			csc: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			config: &ClientSecretCredential{
				TenantID:     "tenantID",
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
			},
			want: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.csc.Equals(tt.config); got != tt.want {
				t.Errorf("Equals() = %v, want %v", got, tt.want)
			}
		})
	}
}
