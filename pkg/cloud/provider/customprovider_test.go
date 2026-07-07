package provider

import (
	"fmt"
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
)

type fakeProviderConfig struct {
	customPricing *models.CustomPricing
}

func (f *fakeProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	if f.customPricing != nil {
		return f.customPricing, nil
	}
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) Update(func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) UpdateFromMap(map[string]string) (*models.CustomPricing, error) {
	return nil, fmt.Errorf("no config")
}

func (f *fakeProviderConfig) ConfigFileManager() *config.ConfigFileManager { return nil }

func TestCustomProviderLoadBalancerPricing(t *testing.T) {
	cases := map[string]struct {
		pricing      *models.CustomPricing
		expectedCost float64
		expectErr    string
	}{
		"unset fields default to zero cost": {
			pricing:      &models.CustomPricing{},
			expectedCost: 0.0,
		},
		"forwarding rule cost is used when set": {
			pricing: &models.CustomPricing{
				FirstFiveForwardingRulesCost: "0.025",
				AdditionalForwardingRuleCost: "0.01",
				LBIngressDataCost:            "0.008",
			},
			expectedCost: 0.025,
		},
		"defaultLBPrice is used when forwarding rule cost is unset": {
			pricing: &models.CustomPricing{
				DefaultLBPrice: "0.05",
			},
			expectedCost: 0.05,
		},
		"forwarding rule cost takes precedence over defaultLBPrice": {
			pricing: &models.CustomPricing{
				FirstFiveForwardingRulesCost: "0.025",
				DefaultLBPrice:               "0.05",
			},
			expectedCost: 0.025,
		},
		"malformed value returns an error naming the field": {
			pricing: &models.CustomPricing{
				FirstFiveForwardingRulesCost: "not-a-number",
			},
			expectErr: "firstFiveForwardingRulesCost",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cp := &CustomProvider{Config: &fakeProviderConfig{customPricing: tc.pricing}}
			lb, err := cp.LoadBalancerPricing()
			if tc.expectErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectErr)
				}
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Fatalf("expected error containing %q, got %q", tc.expectErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadBalancerPricing returned error: %v", err)
			}
			if lb.Cost != tc.expectedCost {
				t.Fatalf("expected cost %f, got %f", tc.expectedCost, lb.Cost)
			}
		})
	}
}

func TestCustomProviderClusterInfoName(t *testing.T) {
	cases := map[string]struct {
		clusterName  string
		clusterIDEnv string
		expectedName string
	}{
		"configured cluster name is used": {
			clusterName:  "my-cluster",
			clusterIDEnv: "cluster-id",
			expectedName: "my-cluster",
		},
		"falls back to CLUSTER_ID when cluster name is unset": {
			clusterIDEnv: "cluster-id",
			expectedName: "cluster-id",
		},
		"falls back to default when cluster name and CLUSTER_ID are unset": {
			expectedName: "Custom Cluster",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CLUSTER_ID", tc.clusterIDEnv)
			cp := &CustomProvider{Config: &fakeProviderConfig{customPricing: &models.CustomPricing{ClusterName: tc.clusterName}}}
			info, err := cp.ClusterInfo()
			if err != nil {
				t.Fatalf("ClusterInfo returned error: %v", err)
			}
			if info["name"] != tc.expectedName {
				t.Fatalf("expected cluster name %q, got %q", tc.expectedName, info["name"])
			}
		})
	}
}
