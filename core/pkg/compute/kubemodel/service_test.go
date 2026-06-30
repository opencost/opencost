package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeServices(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Service
	}{
		{
			name:      "no data returns empty service map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Service{},
		},
		{
			name: "basic service info and uptime",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-service", NamespaceUID: "ns-1", ServiceType: "ClusterIP"},
				},
				source.QueryServiceUptime: []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Service{
				"svc-1": {
					UID:          "svc-1",
					Name:         "my-service",
					NamespaceUID: "ns-1",
					Type:         kubemodel.ServiceTypeClusterIP,
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "service without uptime is not registered",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-service", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.Service{},
		},
		{
			name: "service without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-service"},
				},
				source.QueryServiceUptime: []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Service{},
		},
		{
			name: "load balancer service with ingress address",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-lb", NamespaceUID: "ns-1", ServiceType: "LoadBalancer", LBIngressAddress: "1.2.3.4"},
				},
				source.QueryServiceUptime: []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Service{
				"svc-1": {
					UID:              "svc-1",
					Name:             "my-lb",
					NamespaceUID:     "ns-1",
					Type:             kubemodel.ServiceTypeLoadBalancer,
					LBIngressAddress: "1.2.3.4",
					Start:            start,
					End:              end,
				},
			},
		},
		{
			name: "service selector labels are attached",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-service", NamespaceUID: "ns-1", ServiceType: "ClusterIP"},
				},
				source.QueryServiceUptime: []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
				},
				source.QueryServiceSelectorLabels: []*source.ServiceLabelsResult{
					{UID: "svc-1", Labels: map[string]string{"app": "web", "tier": "frontend"}},
				},
			},
			want: map[string]*kubemodel.Service{
				"svc-1": {
					UID:          "svc-1",
					Name:         "my-service",
					NamespaceUID: "ns-1",
					Type:         kubemodel.ServiceTypeClusterIP,
					Start:        start,
					End:          end,
					Selector:     map[string]string{"app": "web", "tier": "frontend"},
				},
			},
		},
		{
			name: "uptime for unknown service is ignored",
			overrides: map[string]any{
				source.QueryServiceInfo: []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-service", NamespaceUID: "ns-1"},
				},
				source.QueryServiceUptime: []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
					{UID: "unknown-svc", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Service{
				"svc-1": {
					UID:          "svc-1",
					Name:         "my-service",
					NamespaceUID: "ns-1",
					Type:         kubemodel.ServiceTypeClusterIP,
					Start:        start,
					End:          end,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			seedCluster(ds, start, end)
			for method, result := range tt.overrides {
				ds.Querier.SetOverride(method, result)
			}

			km, err := NewKubeModel(testClusterUID, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Services)
		})
	}
}
