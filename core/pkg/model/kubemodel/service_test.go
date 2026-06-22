package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateService(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		service *Service
		wantErr string
	}{
		{
			name:    "empty UID",
			service: &Service{Name: "my-svc", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "UID is missing for Service with name 'my-svc'",
		},
		{
			name:    "empty Name",
			service: &Service{UID: "svc-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "Name is missing for Service 'svc-uid'",
		},
		{
			name:    "empty NamespaceUID",
			service: &Service{UID: "svc-uid", Name: "my-svc", Start: start, End: end},
			wantErr: "NamespaceUID is missing for Service 'svc-uid'",
		},
		{
			name:    "outside window",
			service: &Service{UID: "svc-uid", Name: "my-svc", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:    "valid",
			service: &Service{UID: "svc-uid", Name: "my-svc", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.service.ValidateService(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterService(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newService := func(uid, name string) *Service {
		return &Service{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}

	tests := []struct {
		name    string
		setup   func(*KubeModelSet)
		service *Service
		wantErr string
		want    *KubeModelSet
	}{
		{
			name:    "validation failure",
			service: &Service{UID: "", Name: "my-svc", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr: "RegisterService: invalid service: UID is missing for Service with name 'my-svc'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterService: invalid service: UID is missing for Service with name 'my-svc'"},
				}
				return kms
			}(),
		},
		{
			name:    "registers service",
			service: newService("svc-uid", "my-svc"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Services["svc-uid"] = newService("svc-uid", "my-svc")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterService(newService("svc-uid", "original"))
			},
			service: newService("svc-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Services["svc-uid"] = newService("svc-uid", "original")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			if tt.setup != nil {
				tt.setup(kms)
			}

			err := kms.RegisterService(tt.service)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}

func TestParseServiceType(t *testing.T) {
	tests := []struct {
		input    string
		expected ServiceType
	}{
		// Exact canonical values
		{"ClusterIP", ServiceTypeClusterIP},
		{"NodePort", ServiceTypeNodePort},
		{"LoadBalancer", ServiceTypeLoadBalancer},
		{"ExternalName", ServiceTypeExternalName},
		// Case-insensitive
		{"clusterip", ServiceTypeClusterIP},
		{"nodeport", ServiceTypeNodePort},
		{"loadbalancer", ServiceTypeLoadBalancer},
		{"externalname", ServiceTypeExternalName},
		{"CLUSTERIP", ServiceTypeClusterIP},
		{"LOADBALANCER", ServiceTypeLoadBalancer},
		// "lb" alias
		{"lb", ServiceTypeLoadBalancer},
		{"LB", ServiceTypeLoadBalancer},
		// Unknown input defaults to ClusterIP
		{"", ServiceTypeClusterIP},
		{"unknown", ServiceTypeClusterIP},
		{"ingress", ServiceTypeClusterIP},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseServiceType(tt.input)
			if got != tt.expected {
				t.Errorf("ParseServiceType(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
