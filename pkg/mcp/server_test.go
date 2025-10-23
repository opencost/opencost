package mcp

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/opencost"
	models "github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryTypeConstants(t *testing.T) {
	assert.Equal(t, QueryType("allocation"), AllocationQueryType)
	assert.Equal(t, QueryType("asset"), AssetQueryType)
	assert.Equal(t, QueryType("cloudcost"), CloudCostQueryType)
}

func TestAllocationQueryStruct(t *testing.T) {
	query := AllocationQuery{
		Step:                                  1 * time.Hour,
		Accumulate:                            true,
		ShareIdle:                             true,
		Aggregate:                             "namespace",
		IncludeIdle:                           true,
		IdleByNode:                            true,
		IncludeProportionalAssetResourceCosts: true,
		IncludeAggregatedMetadata:             true,
		ShareLB:                               true,
	}

	assert.Equal(t, 1*time.Hour, query.Step)
	assert.True(t, query.Accumulate)
	assert.True(t, query.ShareIdle)
	assert.Equal(t, "namespace", query.Aggregate)
	assert.True(t, query.IncludeIdle)
	assert.True(t, query.IdleByNode)
	assert.True(t, query.IncludeProportionalAssetResourceCosts)
	assert.True(t, query.IncludeAggregatedMetadata)
	assert.True(t, query.ShareLB)
}

func TestAssetQueryStruct(t *testing.T) {
	query := AssetQuery{}

	// AssetQuery is currently empty, just test that it can be created
	assert.NotNil(t, query)
}

func TestCloudCostQueryStruct(t *testing.T) {
	query := CloudCostQuery{
		Aggregate:  "provider,service",
		Accumulate: "day",
		Filter:     "provider=aws",
		Provider:   "aws",
		Service:    "ec2",
		Category:   "compute",
		Region:     "us-east-1",
		AccountID:  "123456789",
	}

	assert.Equal(t, "provider,service", query.Aggregate)
	assert.Equal(t, "day", query.Accumulate)
	assert.Equal(t, "provider=aws", query.Filter)
	assert.Equal(t, "aws", query.Provider)
	assert.Equal(t, "ec2", query.Service)
	assert.Equal(t, "compute", query.Category)
	assert.Equal(t, "us-east-1", query.Region)
	assert.Equal(t, "123456789", query.AccountID)
}

func TestMCPRequestStruct(t *testing.T) {
	request := MCPRequest{
		SessionID: "test-session-123",
		Query: &OpenCostQueryRequest{
			QueryType: AllocationQueryType,
			Window:    "24h",
			AllocationParams: &AllocationQuery{
				Step:       1 * time.Hour,
				Accumulate: true,
				ShareIdle:  true,
			},
		},
	}

	assert.Equal(t, "test-session-123", request.SessionID)
	assert.NotNil(t, request.Query)
	assert.Equal(t, AllocationQueryType, request.Query.QueryType)
	assert.Equal(t, "24h", request.Query.Window)
	assert.NotNil(t, request.Query.AllocationParams)
	assert.Equal(t, 1*time.Hour, request.Query.AllocationParams.Step)
	assert.True(t, request.Query.AllocationParams.Accumulate)
	assert.True(t, request.Query.AllocationParams.ShareIdle)
}

func TestMCPResponseStruct(t *testing.T) {
	response := MCPResponse{
		Data: "test-data",
		QueryInfo: QueryMetadata{
			QueryID:        "query-123",
			Timestamp:      time.Now(),
			ProcessingTime: 100 * time.Millisecond,
		},
	}

	assert.Equal(t, "test-data", response.Data)
	assert.Equal(t, "query-123", response.QueryInfo.QueryID)
	assert.NotZero(t, response.QueryInfo.Timestamp)
	assert.Equal(t, 100*time.Millisecond, response.QueryInfo.ProcessingTime)
}

func TestQueryMetadataStruct(t *testing.T) {
	metadata := QueryMetadata{
		QueryID:        "query-456",
		Timestamp:      time.Now(),
		ProcessingTime: 250 * time.Millisecond,
	}

	assert.Equal(t, "query-456", metadata.QueryID)
	assert.NotZero(t, metadata.Timestamp)
	assert.Equal(t, 250*time.Millisecond, metadata.ProcessingTime)
}

func TestOpenCostQueryRequestStruct(t *testing.T) {
	request := OpenCostQueryRequest{
		QueryType:   AssetQueryType,
		Window:      "7d",
		AssetParams: &AssetQuery{},
	}

	assert.Equal(t, AssetQueryType, request.QueryType)
	assert.Equal(t, "7d", request.Window)
	assert.NotNil(t, request.AssetParams)
}

// Test helper functions
func createTestAllocation(name string) *Allocation {
	now := time.Now()
	return &Allocation{
		Name:         name,
		CPUCost:      10.0,
		RAMCost:      5.0,
		GPUCost:      0.0,
		PVCost:       2.0,
		NetworkCost:  1.0,
		SharedCost:   0.5,
		ExternalCost: 0.0,
		TotalCost:    18.5,
		CPUCoreHours: 100.0,
		RAMByteHours: 5000000000.0,
		GPUHours:     0.0,
		PVByteHours:  2000000000.0,
		Start:        now.Add(-24 * time.Hour),
		End:          now,
	}
}

func createTestAsset(name string) *Asset {
	now := time.Now()
	return &Asset{
		Type: "node",
		Properties: AssetProperties{
			Category: "compute",
			Provider: "aws",
			Name:     name,
		},
		CPUCost:      50.0,
		RAMCost:      25.0,
		GPUCost:      100.0,
		TotalCost:    175.0,
		CPUCoreHours: 500.0,
		RAMByteHours: 25000000000.0,
		GPUHours:     50.0,
		Start:        now.Add(-24 * time.Hour),
		End:          now,
	}
}

func createTestCloudCost(name string) *CloudCost {
	now := time.Now()
	return &CloudCost{
		Properties: CloudCostProperties{
			Provider: "aws",
			Service:  "ec2",
		},
		Window: TimeWindow{
			Start: now.Add(-24 * time.Hour),
			End:   now,
		},
		ListCost: CostMetric{
			Cost:              100.0,
			KubernetesPercent: 80.0,
		},
		NetCost: CostMetric{
			Cost:              95.0,
			KubernetesPercent: 80.0,
		},
	}
}

// Test MCP server response structures
func TestAllocationResponseStruct(t *testing.T) {
	allocation := createTestAllocation("test-namespace")
	allocationSet := &AllocationSet{
		Name: "test-namespace",
		Properties: map[string]string{
			"namespace": "test-namespace",
		},
		Allocations: []*Allocation{allocation},
	}

	response := AllocationResponse{
		Allocations: map[string]*AllocationSet{
			"test-namespace": allocationSet,
		},
	}

	require.NotNil(t, response.Allocations)
	assert.Len(t, response.Allocations, 1)
	assert.Contains(t, response.Allocations, "test-namespace")

	allocSet := response.Allocations["test-namespace"]
	assert.Equal(t, "test-namespace", allocSet.Name)
	assert.Len(t, allocSet.Allocations, 1)

	alloc := allocSet.Allocations[0]
	assert.Equal(t, "test-namespace", alloc.Name)
	assert.Equal(t, 10.0, alloc.CPUCost)
	assert.Equal(t, 5.0, alloc.RAMCost)
	assert.Equal(t, 18.5, alloc.TotalCost)
}

func TestAssetResponseStruct(t *testing.T) {
	asset := createTestAsset("test-node")
	assetSet := &AssetSet{
		Name:   "test-node",
		Assets: []*Asset{asset},
	}

	response := AssetResponse{
		Assets: map[string]*AssetSet{
			"test-node": assetSet,
		},
	}

	require.NotNil(t, response.Assets)
	assert.Len(t, response.Assets, 1)
	assert.Contains(t, response.Assets, "test-node")

	assetSetResult := response.Assets["test-node"]
	assert.Equal(t, "test-node", assetSetResult.Name)
	assert.Len(t, assetSetResult.Assets, 1)

	assetResult := assetSetResult.Assets[0]
	assert.Equal(t, "node", assetResult.Type)
	assert.Equal(t, 50.0, assetResult.CPUCost)
	assert.Equal(t, 25.0, assetResult.RAMCost)
	assert.Equal(t, 100.0, assetResult.GPUCost)
	assert.Equal(t, 175.0, assetResult.TotalCost)
}

func TestCloudCostResponseStruct(t *testing.T) {
	cloudCost := createTestCloudCost("aws-ec2")
	cloudCostSet := &CloudCostSet{
		Name:       "aws-ec2",
		CloudCosts: []*CloudCost{cloudCost},
		Window: &TimeWindow{
			Start: time.Now().Add(-24 * time.Hour),
			End:   time.Now(),
		},
	}

	response := CloudCostResponse{
		CloudCosts: map[string]*CloudCostSet{
			"aws-ec2": cloudCostSet,
		},
		Summary: &CloudCostSummary{
			TotalNetCost:       95.0,
			TotalAmortizedCost: 90.0,
			TotalInvoicedCost:  100.0,
			KubernetesPercent:  80.0,
		},
	}

	require.NotNil(t, response.CloudCosts)
	assert.Len(t, response.CloudCosts, 1)
	assert.Contains(t, response.CloudCosts, "aws-ec2")

	costSet := response.CloudCosts["aws-ec2"]
	assert.Equal(t, "aws-ec2", costSet.Name)
	assert.Len(t, costSet.CloudCosts, 1)

	cost := costSet.CloudCosts[0]
	assert.Equal(t, "aws", cost.Properties.Provider)
	assert.Equal(t, "ec2", cost.Properties.Service)
	assert.Equal(t, 100.0, cost.ListCost.Cost)
	assert.Equal(t, 95.0, cost.NetCost.Cost)

	require.NotNil(t, response.Summary)
	assert.Equal(t, 95.0, response.Summary.TotalNetCost)
	assert.Equal(t, 80.0, response.Summary.KubernetesPercent)
}

// Test allocation set functionality
func TestAllocationSetTotalCost(t *testing.T) {
	alloc1 := createTestAllocation("alloc1")
	alloc1.TotalCost = 10.0

	alloc2 := createTestAllocation("alloc2")
	alloc2.TotalCost = 15.0

	allocSet := &AllocationSet{
		Name:        "test-set",
		Allocations: []*Allocation{alloc1, alloc2},
	}

	totalCost := allocSet.TotalCost()
	assert.Equal(t, 25.0, totalCost)
}

// Test asset properties
func TestAssetProperties(t *testing.T) {
	props := AssetProperties{
		Category:   "compute",
		Provider:   "aws",
		Account:    "123456789",
		Project:    "my-project",
		Service:    "ec2",
		Cluster:    "prod-cluster",
		Name:       "worker-node-1",
		ProviderID: "i-1234567890abcdef0",
	}

	assert.Equal(t, "compute", props.Category)
	assert.Equal(t, "aws", props.Provider)
	assert.Equal(t, "123456789", props.Account)
	assert.Equal(t, "my-project", props.Project)
	assert.Equal(t, "ec2", props.Service)
	assert.Equal(t, "prod-cluster", props.Cluster)
	assert.Equal(t, "worker-node-1", props.Name)
	assert.Equal(t, "i-1234567890abcdef0", props.ProviderID)
}

// Test cloud cost properties
func TestCloudCostProperties(t *testing.T) {
	props := CloudCostProperties{
		ProviderID:        "i-1234567890abcdef0",
		Provider:          "aws",
		AccountID:         "123456789",
		AccountName:       "my-account",
		InvoiceEntityID:   "entity-123",
		InvoiceEntityName: "My Company",
		RegionID:          "us-east-1",
		AvailabilityZone:  "us-east-1a",
		Service:           "ec2",
		Category:          "compute",
		Labels: map[string]string{
			"environment": "production",
			"team":        "platform",
		},
	}

	assert.Equal(t, "i-1234567890abcdef0", props.ProviderID)
	assert.Equal(t, "aws", props.Provider)
	assert.Equal(t, "123456789", props.AccountID)
	assert.Equal(t, "my-account", props.AccountName)
	assert.Equal(t, "entity-123", props.InvoiceEntityID)
	assert.Equal(t, "My Company", props.InvoiceEntityName)
	assert.Equal(t, "us-east-1", props.RegionID)
	assert.Equal(t, "us-east-1a", props.AvailabilityZone)
	assert.Equal(t, "ec2", props.Service)
	assert.Equal(t, "compute", props.Category)
	assert.Equal(t, "production", props.Labels["environment"])
	assert.Equal(t, "platform", props.Labels["team"])
}

// Test cost metric
func TestCostMetric(t *testing.T) {
	metric := CostMetric{
		Cost:              100.0,
		KubernetesPercent: 80.0,
	}

	assert.Equal(t, 100.0, metric.Cost)
	assert.Equal(t, 80.0, metric.KubernetesPercent)
}

// Test time window
func TestTimeWindow(t *testing.T) {
	now := time.Now()
	window := TimeWindow{
		Start: now.Add(-24 * time.Hour),
		End:   now,
	}

	assert.True(t, window.Start.Before(window.End))
	assert.Equal(t, 24*time.Hour, window.End.Sub(window.Start))
}

// Test node overhead
func TestNodeOverhead(t *testing.T) {
	overhead := NodeOverhead{
		RamOverheadFraction:  0.1,
		CpuOverheadFraction:  0.05,
		OverheadCostFraction: 0.15,
	}

	assert.Equal(t, 0.1, overhead.RamOverheadFraction)
	assert.Equal(t, 0.05, overhead.CpuOverheadFraction)
	assert.Equal(t, 0.15, overhead.OverheadCostFraction)
}

// Test asset breakdown
func TestAssetBreakdown(t *testing.T) {
	breakdown := AssetBreakdown{
		Idle:   10.0,
		Other:  5.0,
		System: 15.0,
		User:   70.0,
	}

	assert.Equal(t, 10.0, breakdown.Idle)
	assert.Equal(t, 5.0, breakdown.Other)
	assert.Equal(t, 15.0, breakdown.System)
	assert.Equal(t, 70.0, breakdown.User)
}

// Test cloud cost summary
func TestCloudCostSummary(t *testing.T) {
	summary := CloudCostSummary{
		TotalNetCost:       1000.0,
		TotalAmortizedCost: 950.0,
		TotalInvoicedCost:  1100.0,
		KubernetesPercent:  85.0,
		ProviderBreakdown: map[string]float64{
			"aws": 800.0,
			"gcp": 200.0,
		},
		ServiceBreakdown: map[string]float64{
			"ec2": 600.0,
			"s3":  200.0,
			"rds": 200.0,
		},
		RegionBreakdown: map[string]float64{
			"us-east-1": 600.0,
			"us-west-2": 400.0,
		},
	}

	assert.Equal(t, 1000.0, summary.TotalNetCost)
	assert.Equal(t, 950.0, summary.TotalAmortizedCost)
	assert.Equal(t, 1100.0, summary.TotalInvoicedCost)
	assert.Equal(t, 85.0, summary.KubernetesPercent)
	assert.Equal(t, 800.0, summary.ProviderBreakdown["aws"])
	assert.Equal(t, 200.0, summary.ProviderBreakdown["gcp"])
	assert.Equal(t, 600.0, summary.ServiceBreakdown["ec2"])
	assert.Equal(t, 200.0, summary.ServiceBreakdown["s3"])
	assert.Equal(t, 600.0, summary.RegionBreakdown["us-east-1"])
	assert.Equal(t, 400.0, summary.RegionBreakdown["us-west-2"])
}

// Test default values
func TestAllocationQueryDefaultValues(t *testing.T) {
	query := AllocationQuery{}

	// Test default values
	assert.Equal(t, time.Duration(0), query.Step)
	assert.False(t, query.Accumulate)
	assert.False(t, query.ShareIdle)
	assert.Empty(t, query.Aggregate)
	assert.False(t, query.IncludeIdle)
	assert.False(t, query.IdleByNode)
	assert.False(t, query.IncludeProportionalAssetResourceCosts)
	assert.False(t, query.IncludeAggregatedMetadata)
	assert.False(t, query.ShareLB)
}

func TestCloudCostQueryDefaultValues(t *testing.T) {
	query := CloudCostQuery{}

	// Test default values
	assert.Empty(t, query.Aggregate)
	assert.Empty(t, query.Accumulate)
	assert.Empty(t, query.Filter)
	assert.Empty(t, query.Provider)
	assert.Empty(t, query.Service)
	assert.Empty(t, query.Category)
	assert.Empty(t, query.Region)
	assert.Empty(t, query.AccountID)
}

// Test edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("zero duration step", func(t *testing.T) {
		query := AllocationQuery{
			Step: 0,
		}
		assert.Equal(t, time.Duration(0), query.Step)
	})

	t.Run("negative duration step", func(t *testing.T) {
		query := AllocationQuery{
			Step: -1 * time.Hour,
		}
		assert.Equal(t, -1*time.Hour, query.Step)
	})

	t.Run("very large duration step", func(t *testing.T) {
		query := AllocationQuery{
			Step: 365 * 24 * time.Hour, // 1 year
		}
		assert.Equal(t, 365*24*time.Hour, query.Step)
	})

	t.Run("empty aggregate string", func(t *testing.T) {
		query := AllocationQuery{
			Aggregate: "",
		}
		assert.Empty(t, query.Aggregate)
	})

	t.Run("comma separated aggregate", func(t *testing.T) {
		query := AllocationQuery{
			Aggregate: "namespace,cluster,node",
		}
		assert.Equal(t, "namespace,cluster,node", query.Aggregate)
	})
}

// dummyQuerier captures the last QueryRequest it received
type dummyQuerier struct {
	last cloudcost.QueryRequest
}

func (dq *dummyQuerier) Query(_ context.Context, req cloudcost.QueryRequest) (*opencost.CloudCostSetRange, error) {
	dq.last = req
	// Return empty set range
	ccsr, _ := opencost.NewCloudCostSetRange(time.Now().Add(-24*time.Hour), time.Now(), opencost.AccumulateOptionDay, "")
	return ccsr, nil
}

func TestBuildCloudCostQueryRequest_AccumulateParsing(t *testing.T) {
	s := &MCPServer{}
	req := cloudcost.QueryRequest{}
	params := &CloudCostQuery{
		Aggregate:  "provider,service",
		Accumulate: "week",
	}
	out := s.buildCloudCostQueryRequest(req, params)

	assert.Equal(t, []string{"provider", "service"}, out.AggregateBy)
	assert.NotEqual(t, opencost.AccumulateOptionNone, out.Accumulate)
}

func TestBuildCloudCostQueryRequest_FilterString(t *testing.T) {
	s := &MCPServer{}
	req := cloudcost.QueryRequest{}
	params := &CloudCostQuery{
		Filter: `provider:"gcp" and service:"Compute Engine"`,
	}
	out := s.buildCloudCostQueryRequest(req, params)
	assert.NotNil(t, out.Filter)
}

func TestBuildFilterFromParams_SupportedFieldsOnly(t *testing.T) {
	s := &MCPServer{}
	params := &CloudCostQuery{
		Provider:        "gcp",
		ProviderID:      "cluster-1",
		Service:         "Compute Engine",
		Category:        "compute",
		AccountID:       "acct-123",
		InvoiceEntityID: "inv-456",
		Region:          "us-central1", // intentionally set; ignored by builder
		Labels: map[string]string{
			"goog-k8s-cluster-name": "cluster-1",
		},
	}
	f := s.buildFilterFromParams(params)
	assert.NotNil(t, f)
}

func TestBuildFilterFromParams_LabelOnly(t *testing.T) {
	s := &MCPServer{}
	params := &CloudCostQuery{
		Labels: map[string]string{"environment": "prod"},
	}
	f := s.buildFilterFromParams(params)
	assert.NotNil(t, f)
}

func TestQueryCloudCosts_QuerierCapture(t *testing.T) {
	dq := &dummyQuerier{}
	s := &MCPServer{cloudQuerier: dq}

	req := &OpenCostQueryRequest{
		QueryType: CloudCostQueryType,
		Window:    "5d",
		CloudCostParams: &CloudCostQuery{
			Aggregate:  "provider,service",
			Accumulate: "week",
			Provider:   "gcp",
		},
	}

	_, err := s.QueryCloudCosts(req)
	require.NoError(t, err)

	assert.Equal(t, []string{"provider", "service"}, dq.last.AggregateBy)
	assert.NotEqual(t, opencost.AccumulateOptionNone, dq.last.Accumulate)
}

// ---- Tests for MCP server end-to-end behavior ----

func TestProcessMCPRequest_CloudCostDispatch(t *testing.T) {
	dq := &dummyQuerier{}
	s := &MCPServer{cloudQuerier: dq}

	req := &MCPRequest{
		Query: &OpenCostQueryRequest{
			QueryType: CloudCostQueryType,
			Window:    "3d",
			CloudCostParams: &CloudCostQuery{
				Aggregate:  "provider",
				Accumulate: "day",
				Provider:   "gcp",
			},
		},
	}

	resp, err := s.ProcessMCPRequest(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)
}

func TestProcessMCPRequest_UnsupportedType(t *testing.T) {
	s := &MCPServer{}

	req := &MCPRequest{
		Query: &OpenCostQueryRequest{
			QueryType: QueryType("unknown"),
			Window:    "1d",
		},
	}
	_, err := s.ProcessMCPRequest(req)
	require.Error(t, err)
}

func TestProcessMCPRequest_ValidationError(t *testing.T) {
	s := &MCPServer{}
	// Missing window
	req := &MCPRequest{
		Query: &OpenCostQueryRequest{
			QueryType: CloudCostQueryType,
			Window:    "",
		},
	}
	_, err := s.ProcessMCPRequest(req)
	require.Error(t, err)
}

// ---- Additional comprehensive tests for missing functionality ----

func TestNewMCPServer(t *testing.T) {
	costModel := &costmodel.CostModel{}
	provider := &mockProvider{}
	cloudQuerier := &dummyQuerier{}

	server := NewMCPServer(costModel, provider, cloudQuerier)

	require.NotNil(t, server)
	assert.Equal(t, costModel, server.costModel)
	assert.Equal(t, provider, server.provider)
	assert.Equal(t, cloudQuerier, server.cloudQuerier)
}

// Mock provider for testing
type mockProvider struct{}

func (mp *mockProvider) GetConfig() (*models.CustomPricing, error)                { return nil, nil }
func (mp *mockProvider) AllNodePricing() (interface{}, error)                     { return nil, nil }
func (mp *mockProvider) ClusterInfo() (map[string]string, error)                  { return nil, nil }
func (mp *mockProvider) GetAddresses() ([]byte, error)                            { return nil, nil }
func (mp *mockProvider) GetDisks() ([]byte, error)                                { return nil, nil }
func (mp *mockProvider) GetOrphanedResources() ([]models.OrphanedResource, error) { return nil, nil }
func (mp *mockProvider) NodePricing(models.Key) (*models.Node, models.PricingMetadata, error) {
	return nil, models.PricingMetadata{}, nil
}
func (mp *mockProvider) GpuPricing(map[string]string) (string, error)            { return "", nil }
func (mp *mockProvider) PVPricing(models.PVKey) (*models.PV, error)              { return nil, nil }
func (mp *mockProvider) NetworkPricing() (*models.Network, error)                { return nil, nil }
func (mp *mockProvider) LoadBalancerPricing() (*models.LoadBalancer, error)      { return nil, nil }
func (mp *mockProvider) DownloadPricingData() error                              { return nil }
func (mp *mockProvider) GetKey(map[string]string, *clustercache.Node) models.Key { return nil }
func (mp *mockProvider) GetPVKey(*clustercache.PersistentVolume, map[string]string, string) models.PVKey {
	return nil
}
func (mp *mockProvider) UpdateConfig(io.Reader, string) (*models.CustomPricing, error) {
	return nil, nil
}
func (mp *mockProvider) UpdateConfigFromConfigMap(map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}
func (mp *mockProvider) GetManagementPlatform() (string, error)                         { return "", nil }
func (mp *mockProvider) ApplyReservedInstancePricing(map[string]*models.Node)           {}
func (mp *mockProvider) ServiceAccountStatus() *models.ServiceAccountStatus             { return nil }
func (mp *mockProvider) PricingSourceStatus() map[string]*models.PricingSource          { return nil }
func (mp *mockProvider) ClusterManagementPricing() (string, float64, error)             { return "", 0, nil }
func (mp *mockProvider) CombinedDiscountForNode(string, bool, float64, float64) float64 { return 0 }
func (mp *mockProvider) Regions() []string                                              { return nil }
func (mp *mockProvider) PricingSourceSummary() interface{}                              { return nil }

func TestGenerateQueryID(t *testing.T) {
	// Test that generateQueryID returns a non-empty string
	id1 := generateQueryID()
	id2 := generateQueryID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2) // Should be different each time
	assert.Contains(t, id1, "query-")
}

func TestTransformAllocationSet_NilInput(t *testing.T) {
	result := transformAllocationSet(nil)

	require.NotNil(t, result)
	assert.NotNil(t, result.Allocations)
	assert.Len(t, result.Allocations, 0)
}

func TestTransformAllocationSet_EmptyInput(t *testing.T) {
	emptySet := &opencost.AllocationSet{
		Allocations: map[string]*opencost.Allocation{},
	}

	result := transformAllocationSet(emptySet)

	require.NotNil(t, result)
	assert.Contains(t, result.Allocations, "allocations")
	assert.Len(t, result.Allocations["allocations"].Allocations, 0)
}

func TestTransformAssetSet_NilInput(t *testing.T) {
	result := transformAssetSet(nil)

	require.NotNil(t, result)
	assert.NotNil(t, result.Assets)
	assert.Len(t, result.Assets, 0)
}

func TestTransformAssetSet_EmptyInput(t *testing.T) {
	emptySet := &opencost.AssetSet{
		Assets: map[string]opencost.Asset{},
	}

	result := transformAssetSet(emptySet)

	require.NotNil(t, result)
	assert.Contains(t, result.Assets, "assets")
	assert.Len(t, result.Assets["assets"].Assets, 0)
}

func TestBuildFilterFromParams_EmptyParams(t *testing.T) {
	s := &MCPServer{}
	params := &CloudCostQuery{}

	filter := s.buildFilterFromParams(params)
	assert.Nil(t, filter)
}

func TestBuildFilterFromParams_RegionIgnored(t *testing.T) {
	s := &MCPServer{}
	params := &CloudCostQuery{
		Region: "us-east-1", // Should be ignored
	}

	filter := s.buildFilterFromParams(params)
	assert.Nil(t, filter) // Should return nil since only region is set
}

func TestBuildFilterFromParams_EmptyLabelKey(t *testing.T) {
	s := &MCPServer{}
	params := &CloudCostQuery{
		Labels: map[string]string{
			"":      "value1", // Empty key should be ignored
			"valid": "value2",
		},
	}

	filter := s.buildFilterFromParams(params)
	assert.NotNil(t, filter)
}

func TestBuildCloudCostQueryRequest_EmptyParams(t *testing.T) {
	s := &MCPServer{}
	req := cloudcost.QueryRequest{}
	params := &CloudCostQuery{}

	result := s.buildCloudCostQueryRequest(req, params)

	assert.Equal(t, req, result) // Should return unchanged request
}

func TestBuildCloudCostQueryRequest_InvalidFilterString(t *testing.T) {
	s := &MCPServer{}
	req := cloudcost.QueryRequest{}
	params := &CloudCostQuery{
		Filter: "invalid filter syntax !!!",
	}

	result := s.buildCloudCostQueryRequest(req, params)

	// Should not panic and should return request with nil filter
	assert.Nil(t, result.Filter)
}

func TestQueryCloudCosts_NilCloudQuerier(t *testing.T) {
	s := &MCPServer{cloudQuerier: nil}

	req := &OpenCostQueryRequest{
		QueryType: CloudCostQueryType,
		Window:    "24h",
	}

	_, err := s.QueryCloudCosts(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloud cost querier not configured")
}

func TestQueryCloudCosts_InvalidWindow(t *testing.T) {
	s := &MCPServer{cloudQuerier: &dummyQuerier{}}

	req := &OpenCostQueryRequest{
		QueryType: CloudCostQueryType,
		Window:    "invalid-window",
	}

	_, err := s.QueryCloudCosts(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse window")
}

func TestQueryAssets_InvalidWindow(t *testing.T) {
	s := &MCPServer{}

	req := &OpenCostQueryRequest{
		QueryType: AssetQueryType,
		Window:    "invalid-window",
	}

	_, err := s.QueryAssets(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse window")
}

func TestQueryAllocations_InvalidWindow(t *testing.T) {
	s := &MCPServer{}

	req := &OpenCostQueryRequest{
		QueryType: AllocationQueryType,
		Window:    "invalid-window",
	}

	_, err := s.QueryAllocations(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse window")
}


func TestProcessMCPRequest_ResponseMetadata(t *testing.T) {
	dq := &dummyQuerier{}
	s := &MCPServer{cloudQuerier: dq}

	req := &MCPRequest{
		Query: &OpenCostQueryRequest{
			QueryType: CloudCostQueryType,
			Window:    "1h",
		},
	}

	resp, err := s.ProcessMCPRequest(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Check response metadata
	assert.NotEmpty(t, resp.QueryInfo.QueryID)
	assert.NotZero(t, resp.QueryInfo.Timestamp)
	assert.Greater(t, resp.QueryInfo.ProcessingTime, time.Duration(0))
}

func TestCloudCostQuery_NewFields(t *testing.T) {
	query := CloudCostQuery{
		InvoiceEntityID: "entity-123",
		ProviderID:      "provider-456",
		Labels: map[string]string{
			"environment": "prod",
			"team":        "platform",
		},
	}

	assert.Equal(t, "entity-123", query.InvoiceEntityID)
	assert.Equal(t, "provider-456", query.ProviderID)
	assert.Equal(t, "prod", query.Labels["environment"])
	assert.Equal(t, "platform", query.Labels["team"])
}
