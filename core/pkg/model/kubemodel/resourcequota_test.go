package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateResourceQuota(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name          string
		resourceQuota *ResourceQuota
		wantErr       string
	}{
		{
			name:          "empty UID",
			resourceQuota: &ResourceQuota{Name: "my-rq", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:       "UID is missing for ResourceQuota with name 'my-rq'",
		},
		{
			name:          "empty Name",
			resourceQuota: &ResourceQuota{UID: "rq-uid", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:       "Name is missing for ResourceQuota 'rq-uid'",
		},
		{
			name:          "empty NamespaceUID",
			resourceQuota: &ResourceQuota{UID: "rq-uid", Name: "my-rq", Start: start, End: end},
			wantErr:       "NamespaceUID is missing for ResourceQuota 'rq-uid'",
		},
		{
			name:          "outside window",
			resourceQuota: &ResourceQuota{UID: "rq-uid", Name: "my-rq", NamespaceUID: "ns-uid", Start: start.Add(-time.Hour), End: end},
			wantErr:       checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:          "valid",
			resourceQuota: &ResourceQuota{UID: "rq-uid", Name: "my-rq", NamespaceUID: "ns-uid", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.resourceQuota.ValidateResourceQuota(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterResourceQuota(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	newRQ := func(uid, name string) *ResourceQuota {
		return &ResourceQuota{UID: uid, Name: name, NamespaceUID: "ns-uid", Start: start, End: end}
	}
	// RegisterResourceQuota initializes nil Spec/Status on registration.
	newRegisteredRQ := func(uid, name string) *ResourceQuota {
		rq := newRQ(uid, name)
		rq.Spec = &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}}
		rq.Status = &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}}
		return rq
	}

	tests := []struct {
		name          string
		setup         func(*KubeModelSet)
		resourceQuota *ResourceQuota
		wantErr       string
		want          *KubeModelSet
	}{
		{
			name:          "validation failure",
			resourceQuota: &ResourceQuota{UID: "", Name: "my-rq", NamespaceUID: "ns-uid", Start: start, End: end},
			wantErr:       "RegisterResourceQuota: invalid resource quota: UID is missing for ResourceQuota with name 'my-rq'",
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.Metadata.Diagnostics = []Diagnostic{
					{Level: DiagnosticLevelError, Message: "RegisterResourceQuota: invalid resource quota: UID is missing for ResourceQuota with name 'my-rq'"},
				}
				return kms
			}(),
		},
		{
			name:          "registers resource quota",
			resourceQuota: newRQ("rq-uid", "my-rq"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.ResourceQuotas["rq-uid"] = newRegisteredRQ("rq-uid", "my-rq")
				kms.Metadata.ObjectCount = 1
				return kms
			}(),
		},
		{
			name: "duplicate registration is a no-op",
			setup: func(kms *KubeModelSet) {
				kms.RegisterResourceQuota(newRQ("rq-uid", "original"))
			},
			resourceQuota: newRQ("rq-uid", "duplicate"),
			want: func() *KubeModelSet {
				kms := NewKubeModelSet(start, end)
				kms.ResourceQuotas["rq-uid"] = newRegisteredRQ("rq-uid", "original")
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

			err := kms.RegisterResourceQuota(tt.resourceQuota)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			KubeModelSetEquals(t, tt.want, kms)
		})
	}
}
