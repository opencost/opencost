package costmodel

import (
	"io"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
)

// mockProvider is a mock implementation of the Provider interface for testing
type mockProvider struct {
	network *models.Network
	err     error
}

func (m *mockProvider) NetworkPricing() (*models.Network, error) {
	return m.network, m.err
}

func (m *mockProvider) GetKey(map[string]string, *clustercache.Node) models.Key {
	return nil
}

func (m *mockProvider) PVPricing(models.PVKey) (*models.PV, error) {
	return nil, nil
}

func (m *mockProvider) NodePricing(models.Key) (*models.Node, models.PricingMetadata, error) {
	return nil, models.PricingMetadata{}, nil
}

func (m *mockProvider) LoadBalancerPricing() (*models.LoadBalancer, error) {
	return nil, nil
}

func (m *mockProvider) AllNodePricing() (interface{}, error) {
	return nil, nil
}

func (m *mockProvider) ClusterInfo() (map[string]string, error) {
	return nil, nil
}

func (m *mockProvider) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (m *mockProvider) GetDisks() ([]byte, error) {
	return nil, nil
}

func (m *mockProvider) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, nil
}

func (m *mockProvider) GpuPricing(map[string]string) (string, error) {
	return "", nil
}

func (m *mockProvider) DownloadPricingData() error {
	return nil
}

func (m *mockProvider) GetPVKey(*clustercache.PersistentVolume, map[string]string, string) models.PVKey {
	return nil
}

func (m *mockProvider) UpdateConfig(io.Reader, string) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *mockProvider) UpdateConfigFromConfigMap(map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *mockProvider) GetConfig() (*models.CustomPricing, error) {
	return nil, nil
}

func (m *mockProvider) GetManagementPlatform() (string, error) {
	return "", nil
}

func (m *mockProvider) ApplyReservedInstancePricing(map[string]*models.Node) {
}

func (m *mockProvider) ServiceAccountStatus() *models.ServiceAccountStatus {
	return nil
}

func (m *mockProvider) PricingSourceStatus() map[string]*models.PricingSource {
	return nil
}

func (m *mockProvider) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (m *mockProvider) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	return 0.0
}

func (m *mockProvider) Regions() []string {
	return nil
}

func (m *mockProvider) PricingSourceSummary() interface{} {
	return nil
}

// TestGetNetworkUsageData tests the aggregation of NAT Gateway egress and ingress data
func TestGetNetworkUsageData(t *testing.T) {
	defaultClusterID := "default-cluster"

	testCases := []struct {
		name                     string
		zoneResults              []*source.NetZoneGiBResult
		regionResults            []*source.NetRegionGiBResult
		internetResults          []*source.NetInternetGiBResult
		natGatewayEgressResults  []*source.NetNatGatewayGiBResult
		natGatewayIngressResults []*source.NetNatGatewayIngressGiBResult
		expectedKeys             []string
		validateFunc             func(t *testing.T, result map[string]*NetworkUsageData)
	}{
		{
			name:            "NAT Gateway egress only",
			zoneResults:     nil,
			regionResults:   nil,
			internetResults: nil,
			natGatewayEgressResults: []*source.NetNatGatewayGiBResult{
				{
					Pod:       "pod1",
					Namespace: "ns1",
					Cluster:   "cluster1",
					Data: []*util.Vector{
						{Value: 10.5, Timestamp: 1000},
						{Value: 20.3, Timestamp: 2000},
					},
				},
			},
			natGatewayIngressResults: nil,
			expectedKeys:             []string{"ns1,pod1,cluster1"},
			validateFunc: func(t *testing.T, result map[string]*NetworkUsageData) {
				key := "ns1,pod1,cluster1"
				if data, ok := result[key]; ok {
					if len(data.NetworkNatGatewayEgress) != 2 {
						t.Errorf("expected 2 NAT Gateway egress vectors, got %d", len(data.NetworkNatGatewayEgress))
					}
					if data.NetworkNatGatewayEgress[0].Value != 10.5 {
						t.Errorf("expected first egress value 10.5, got %f", data.NetworkNatGatewayEgress[0].Value)
					}
					if len(data.NetworkNatGatewayIngress) != 0 {
						t.Errorf("expected 0 NAT Gateway ingress vectors, got %d", len(data.NetworkNatGatewayIngress))
					}
				} else {
					t.Errorf("expected key %s not found in result", key)
				}
			},
		},
		{
			name:                    "NAT Gateway ingress only",
			zoneResults:             nil,
			regionResults:           nil,
			internetResults:         nil,
			natGatewayEgressResults: nil,
			natGatewayIngressResults: []*source.NetNatGatewayIngressGiBResult{
				{
					Pod:       "pod2",
					Namespace: "ns2",
					Cluster:   "cluster2",
					Data: []*util.Vector{
						{Value: 5.2, Timestamp: 1000},
					},
				},
			},
			expectedKeys: []string{"ns2,pod2,cluster2"},
			validateFunc: func(t *testing.T, result map[string]*NetworkUsageData) {
				key := "ns2,pod2,cluster2"
				if data, ok := result[key]; ok {
					if len(data.NetworkNatGatewayIngress) != 1 {
						t.Errorf("expected 1 NAT Gateway ingress vector, got %d", len(data.NetworkNatGatewayIngress))
					}
					if data.NetworkNatGatewayIngress[0].Value != 5.2 {
						t.Errorf("expected ingress value 5.2, got %f", data.NetworkNatGatewayIngress[0].Value)
					}
					if len(data.NetworkNatGatewayEgress) != 0 {
						t.Errorf("expected 0 NAT Gateway egress vectors, got %d", len(data.NetworkNatGatewayEgress))
					}
				} else {
					t.Errorf("expected key %s not found in result", key)
				}
			},
		},
		{
			name:            "NAT Gateway egress and ingress for same pod",
			zoneResults:     nil,
			regionResults:   nil,
			internetResults: nil,
			natGatewayEgressResults: []*source.NetNatGatewayGiBResult{
				{
					Pod:       "pod3",
					Namespace: "ns3",
					Cluster:   "cluster3",
					Data: []*util.Vector{
						{Value: 15.0, Timestamp: 1000},
					},
				},
			},
			natGatewayIngressResults: []*source.NetNatGatewayIngressGiBResult{
				{
					Pod:       "pod3",
					Namespace: "ns3",
					Cluster:   "cluster3",
					Data: []*util.Vector{
						{Value: 8.5, Timestamp: 1000},
					},
				},
			},
			expectedKeys: []string{"ns3,pod3,cluster3"},
			validateFunc: func(t *testing.T, result map[string]*NetworkUsageData) {
				key := "ns3,pod3,cluster3"
				if data, ok := result[key]; ok {
					if len(data.NetworkNatGatewayEgress) != 1 {
						t.Errorf("expected 1 NAT Gateway egress vector, got %d", len(data.NetworkNatGatewayEgress))
					}
					if data.NetworkNatGatewayEgress[0].Value != 15.0 {
						t.Errorf("expected egress value 15.0, got %f", data.NetworkNatGatewayEgress[0].Value)
					}
					if len(data.NetworkNatGatewayIngress) != 1 {
						t.Errorf("expected 1 NAT Gateway ingress vector, got %d", len(data.NetworkNatGatewayIngress))
					}
					if data.NetworkNatGatewayIngress[0].Value != 8.5 {
						t.Errorf("expected ingress value 8.5, got %f", data.NetworkNatGatewayIngress[0].Value)
					}
				} else {
					t.Errorf("expected key %s not found in result", key)
				}
			},
		},
		{
			name: "Mixed network traffic with NAT Gateway",
			zoneResults: []*source.NetZoneGiBResult{
				{
					Pod:       "pod4",
					Namespace: "ns4",
					Cluster:   "cluster4",
					Data: []*util.Vector{
						{Value: 3.0, Timestamp: 1000},
					},
				},
			},
			regionResults: []*source.NetRegionGiBResult{
				{
					Pod:       "pod4",
					Namespace: "ns4",
					Cluster:   "cluster4",
					Data: []*util.Vector{
						{Value: 7.0, Timestamp: 1000},
					},
				},
			},
			internetResults: []*source.NetInternetGiBResult{
				{
					Pod:       "pod4",
					Namespace: "ns4",
					Cluster:   "cluster4",
					Data: []*util.Vector{
						{Value: 12.0, Timestamp: 1000},
					},
				},
			},
			natGatewayEgressResults: []*source.NetNatGatewayGiBResult{
				{
					Pod:       "pod4",
					Namespace: "ns4",
					Cluster:   "cluster4",
					Data: []*util.Vector{
						{Value: 18.0, Timestamp: 1000},
					},
				},
			},
			natGatewayIngressResults: []*source.NetNatGatewayIngressGiBResult{
				{
					Pod:       "pod4",
					Namespace: "ns4",
					Cluster:   "cluster4",
					Data: []*util.Vector{
						{Value: 9.0, Timestamp: 1000},
					},
				},
			},
			expectedKeys: []string{"ns4,pod4,cluster4"},
			validateFunc: func(t *testing.T, result map[string]*NetworkUsageData) {
				key := "ns4,pod4,cluster4"
				if data, ok := result[key]; ok {
					// Verify all network types are present
					if len(data.NetworkZoneEgress) != 1 {
						t.Errorf("expected 1 zone egress vector, got %d", len(data.NetworkZoneEgress))
					}
					if len(data.NetworkRegionEgress) != 1 {
						t.Errorf("expected 1 region egress vector, got %d", len(data.NetworkRegionEgress))
					}
					if len(data.NetworkInternetEgress) != 1 {
						t.Errorf("expected 1 internet egress vector, got %d", len(data.NetworkInternetEgress))
					}
					if len(data.NetworkNatGatewayEgress) != 1 {
						t.Errorf("expected 1 NAT Gateway egress vector, got %d", len(data.NetworkNatGatewayEgress))
					}
					if len(data.NetworkNatGatewayIngress) != 1 {
						t.Errorf("expected 1 NAT Gateway ingress vector, got %d", len(data.NetworkNatGatewayIngress))
					}

					// Verify values
					if data.NetworkNatGatewayEgress[0].Value != 18.0 {
						t.Errorf("expected NAT Gateway egress 18.0, got %f", data.NetworkNatGatewayEgress[0].Value)
					}
					if data.NetworkNatGatewayIngress[0].Value != 9.0 {
						t.Errorf("expected NAT Gateway ingress 9.0, got %f", data.NetworkNatGatewayIngress[0].Value)
					}
				} else {
					t.Errorf("expected key %s not found in result", key)
				}
			},
		},
		{
			name:            "Default cluster ID fallback for NAT Gateway",
			zoneResults:     nil,
			regionResults:   nil,
			internetResults: nil,
			natGatewayEgressResults: []*source.NetNatGatewayGiBResult{
				{
					Pod:       "pod5",
					Namespace: "ns5",
					Cluster:   "", // Empty cluster ID should use default
					Data: []*util.Vector{
						{Value: 5.0, Timestamp: 1000},
					},
				},
			},
			natGatewayIngressResults: nil,
			expectedKeys:             []string{"ns5,pod5,default-cluster"},
			validateFunc: func(t *testing.T, result map[string]*NetworkUsageData) {
				key := "ns5,pod5,default-cluster"
				if data, ok := result[key]; ok {
					if data.ClusterID != "default-cluster" {
						t.Errorf("expected cluster ID 'default-cluster', got %s", data.ClusterID)
					}
				} else {
					t.Errorf("expected key %s not found in result", key)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetNetworkUsageData(
				tc.zoneResults,
				tc.regionResults,
				tc.internetResults,
				tc.natGatewayEgressResults,
				tc.natGatewayIngressResults,
				defaultClusterID,
			)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result) != len(tc.expectedKeys) {
				t.Errorf("expected %d keys, got %d", len(tc.expectedKeys), len(result))
			}

			for _, key := range tc.expectedKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("expected key %s not found in result", key)
				}
			}

			if tc.validateFunc != nil {
				tc.validateFunc(t, result)
			}
		})
	}
}

// TestGetNetworkCost tests the calculation of NAT Gateway costs
func TestGetNetworkCost(t *testing.T) {
	testCases := []struct {
		name           string
		usage          *NetworkUsageData
		pricing        *models.Network
		expectedCost   float64
		expectedLength int
	}{
		{
			name: "NAT Gateway egress cost only",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkNatGatewayEgress: []*util.Vector{
					{Value: 10.0, Timestamp: 1000}, // 10 GiB
				},
			},
			pricing: &models.Network{
				NatGatewayEgressCost: 0.05, // $0.05 per GiB
			},
			expectedCost:   0.50, // 10 * 0.05 = 0.50
			expectedLength: 1,
		},
		{
			name: "NAT Gateway ingress cost only",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkNatGatewayIngress: []*util.Vector{
					{Value: 20.0, Timestamp: 1000}, // 20 GiB
				},
			},
			pricing: &models.Network{
				NatGatewayIngressCost: 0.02, // $0.02 per GiB
			},
			expectedCost:   0.40, // 20 * 0.02 = 0.40
			expectedLength: 1,
		},
		{
			name: "NAT Gateway egress and ingress costs",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkNatGatewayEgress: []*util.Vector{
					{Value: 10.0, Timestamp: 1000},
				},
				NetworkNatGatewayIngress: []*util.Vector{
					{Value: 5.0, Timestamp: 1000},
				},
			},
			pricing: &models.Network{
				NatGatewayEgressCost:  0.05, // $0.05 per GiB
				NatGatewayIngressCost: 0.02, // $0.02 per GiB
			},
			expectedCost:   0.60, // (10 * 0.05) + (5 * 0.02) = 0.50 + 0.10 = 0.60
			expectedLength: 1,
		},
		{
			name: "Mixed network costs with NAT Gateway",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkZoneEgress: []*util.Vector{
					{Value: 5.0, Timestamp: 1000},
				},
				NetworkRegionEgress: []*util.Vector{
					{Value: 8.0, Timestamp: 1000},
				},
				NetworkInternetEgress: []*util.Vector{
					{Value: 12.0, Timestamp: 1000},
				},
				NetworkNatGatewayEgress: []*util.Vector{
					{Value: 15.0, Timestamp: 1000},
				},
				NetworkNatGatewayIngress: []*util.Vector{
					{Value: 10.0, Timestamp: 1000},
				},
			},
			pricing: &models.Network{
				ZoneNetworkEgressCost:     0.01,
				RegionNetworkEgressCost:   0.02,
				InternetNetworkEgressCost: 0.09,
				NatGatewayEgressCost:      0.05,
				NatGatewayIngressCost:     0.02,
			},
			expectedCost:   2.24, // (5*0.01) + (8*0.02) + (12*0.09) + (15*0.05) + (10*0.02) = 0.05 + 0.16 + 1.08 + 0.75 + 0.20 = 2.24
			expectedLength: 1,
		},
		{
			name: "Multiple time points with NAT Gateway",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkNatGatewayEgress: []*util.Vector{
					{Value: 10.0, Timestamp: 1000},
					{Value: 15.0, Timestamp: 2000},
					{Value: 20.0, Timestamp: 3000},
				},
				NetworkNatGatewayIngress: []*util.Vector{
					{Value: 5.0, Timestamp: 1000},
					{Value: 8.0, Timestamp: 2000},
					{Value: 12.0, Timestamp: 3000},
				},
			},
			pricing: &models.Network{
				NatGatewayEgressCost:  0.05,
				NatGatewayIngressCost: 0.02,
			},
			expectedLength: 3,
		},
		{
			name: "Zero NAT Gateway costs",
			usage: &NetworkUsageData{
				ClusterID: "cluster1",
				PodName:   "pod1",
				Namespace: "ns1",
				NetworkNatGatewayEgress: []*util.Vector{
					{Value: 100.0, Timestamp: 1000},
				},
				NetworkNatGatewayIngress: []*util.Vector{
					{Value: 50.0, Timestamp: 1000},
				},
			},
			pricing: &models.Network{
				NatGatewayEgressCost:  0.0,
				NatGatewayIngressCost: 0.0,
			},
			expectedCost:   0.0,
			expectedLength: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &mockProvider{
				network: tc.pricing,
			}

			result, err := GetNetworkCost(tc.usage, provider)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result) != tc.expectedLength {
				t.Errorf("expected %d result vectors, got %d", tc.expectedLength, len(result))
			}

			if tc.expectedLength > 0 {
				totalCost := 0.0
				for _, v := range result {
					totalCost += v.Value
				}

				if tc.expectedCost > 0 {
					if diff := totalCost - tc.expectedCost; diff > 0.001 || diff < -0.001 {
						t.Errorf("expected total cost %f, got %f", tc.expectedCost, totalCost)
					}
				}
			}
		})
	}
}

// TestGetNetworkCost_NATGatewayMisalignedVectors tests NAT Gateway cost calculation with different vector lengths
func TestGetNetworkCost_NATGatewayMisalignedVectors(t *testing.T) {
	usage := &NetworkUsageData{
		ClusterID: "cluster1",
		PodName:   "pod1",
		Namespace: "ns1",
		NetworkZoneEgress: []*util.Vector{
			{Value: 5.0, Timestamp: 1000},
			{Value: 6.0, Timestamp: 2000},
		},
		NetworkNatGatewayEgress: []*util.Vector{
			{Value: 10.0, Timestamp: 1000},
			{Value: 15.0, Timestamp: 2000},
			{Value: 20.0, Timestamp: 3000}, // Extra NAT Gateway data point
		},
		NetworkNatGatewayIngress: []*util.Vector{
			{Value: 5.0, Timestamp: 1000}, // Only one ingress data point
		},
	}

	pricing := &models.Network{
		ZoneNetworkEgressCost: 0.01,
		NatGatewayEgressCost:  0.05,
		NatGatewayIngressCost: 0.02,
	}

	provider := &mockProvider{
		network: pricing,
	}

	result, err := GetNetworkCost(usage, provider)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 result vectors (max of all vector lengths including NAT Gateway)
	if len(result) != 3 {
		t.Errorf("expected 3 result vectors (max of all vectors including NAT Gateway), got %d", len(result))
	}

	// First vector: zone (5*0.01) + natEgress (10*0.05) + natIngress (5*0.02) = 0.05 + 0.50 + 0.10 = 0.65
	expectedFirst := (5.0 * 0.01) + (10.0 * 0.05) + (5.0 * 0.02)
	if diff := result[0].Value - expectedFirst; diff > 0.001 || diff < -0.001 {
		t.Errorf("expected first vector cost %f, got %f", expectedFirst, result[0].Value)
	}

	// Second vector: zone (6*0.01) + natEgress (15*0.05) = 0.06 + 0.75 = 0.81
	// (no NAT ingress for second time point)
	expectedSecond := (6.0 * 0.01) + (15.0 * 0.05)
	if diff := result[1].Value - expectedSecond; diff > 0.001 || diff < -0.001 {
		t.Errorf("expected second vector cost %f, got %f", expectedSecond, result[1].Value)
	}

	// Third vector: only natEgress (20*0.05) = 1.00
	// (no zone, region, internet, or NAT ingress for third time point)
	expectedThird := 20.0 * 0.05
	if diff := result[2].Value - expectedThird; diff > 0.001 || diff < -0.001 {
		t.Errorf("expected third vector cost %f, got %f", expectedThird, result[2].Value)
	}
}
