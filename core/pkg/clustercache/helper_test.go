package clustercache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

func TestGetLoadBalancerIngressAddress(t *testing.T) {
	tests := []struct {
		name     string
		service  *Service
		expected []string
	}{
		{
			name:     "no ingresses",
			service:  &Service{},
			expected: nil,
		},
		{
			name: "single IP ingress",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4"},
		},
		{
			name: "single hostname ingress",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{Hostname: "lb.example.com"},
						},
					},
				},
			},
			expected: []string{"lb.example.com"},
		},
		{
			name: "IP takes priority over hostname",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4", Hostname: "lb.example.com"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4"},
		},
		{
			name: "multiple ingresses",
			service: &Service{
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{
							{IP: "1.2.3.4"},
							{Hostname: "lb2.example.com"},
							{IP: "5.6.7.8"},
						},
					},
				},
			},
			expected: []string{"1.2.3.4", "lb2.example.com", "5.6.7.8"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetLoadBalancerIngressAddress(tt.service)
			if len(got) != len(tt.expected) {
				t.Fatalf("got %v, want %v", got, tt.expected)
			}
			for i := range tt.expected {
				if got[i] != tt.expected[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}
