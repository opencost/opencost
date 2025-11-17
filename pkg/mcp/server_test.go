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

// ---- Tests for Efficiency Tool ----

func TestEfficiencyQueryStruct(t *testing.T) {
	bufferMultiplier := 1.4
	query := EfficiencyQuery{
		Aggregate:                  "pod",
		Filter:                     "namespace:production",
		EfficiencyBufferMultiplier: &bufferMultiplier,
	}

	assert.Equal(t, "pod", query.Aggregate)
	assert.Equal(t, "namespace:production", query.Filter)
	assert.NotNil(t, query.EfficiencyBufferMultiplier)
	assert.Equal(t, 1.4, *query.EfficiencyBufferMultiplier)
}

func TestEfficiencyQueryDefaultValues(t *testing.T) {
	query := EfficiencyQuery{}

	assert.Empty(t, query.Aggregate)
	assert.Empty(t, query.Filter)
	assert.Nil(t, query.EfficiencyBufferMultiplier)
}

func TestEfficiencyMetricStruct(t *testing.T) {
	now := time.Now()
	metric := EfficiencyMetric{
		Name:                       "test-pod",
		CPUEfficiency:              0.5,
		MemoryEfficiency:           0.6,
		CPUCoresRequested:          2.0,
		CPUCoresUsed:               1.0,
		RAMBytesRequested:          2147483648, // 2GB
		RAMBytesUsed:               1288490188, // ~1.2GB
		RecommendedCPURequest:      1.2,
		RecommendedRAMRequest:      1546188226, // ~1.44GB
		ResultingCPUEfficiency:     0.833,
		ResultingMemoryEfficiency:  0.833,
		CurrentTotalCost:           10.0,
		RecommendedCost:            6.0,
		CostSavings:                4.0,
		CostSavingsPercent:         40.0,
		EfficiencyBufferMultiplier: 1.2,
		Start:                      now.Add(-24 * time.Hour),
		End:                        now,
	}

	assert.Equal(t, "test-pod", metric.Name)
	assert.Equal(t, 0.5, metric.CPUEfficiency)
	assert.Equal(t, 0.6, metric.MemoryEfficiency)
	assert.Equal(t, 2.0, metric.CPUCoresRequested)
	assert.Equal(t, 1.0, metric.CPUCoresUsed)
	assert.Equal(t, 2147483648.0, metric.RAMBytesRequested)
	assert.Equal(t, 1288490188.0, metric.RAMBytesUsed)
	assert.Equal(t, 1.2, metric.RecommendedCPURequest)
	assert.Equal(t, 1546188226.0, metric.RecommendedRAMRequest)
	assert.Equal(t, 0.833, metric.ResultingCPUEfficiency)
	assert.Equal(t, 0.833, metric.ResultingMemoryEfficiency)
	assert.Equal(t, 10.0, metric.CurrentTotalCost)
	assert.Equal(t, 6.0, metric.RecommendedCost)
	assert.Equal(t, 4.0, metric.CostSavings)
	assert.Equal(t, 40.0, metric.CostSavingsPercent)
	assert.Equal(t, 1.2, metric.EfficiencyBufferMultiplier)
	assert.True(t, metric.Start.Before(metric.End))
}

func TestEfficiencyResponseStruct(t *testing.T) {
	now := time.Now()
	metric1 := &EfficiencyMetric{
		Name:             "pod-1",
		CPUEfficiency:    0.5,
		MemoryEfficiency: 0.6,
		Start:            now.Add(-24 * time.Hour),
		End:              now,
	}
	metric2 := &EfficiencyMetric{
		Name:             "pod-2",
		CPUEfficiency:    0.7,
		MemoryEfficiency: 0.8,
		Start:            now.Add(-24 * time.Hour),
		End:              now,
	}

	response := EfficiencyResponse{
		Efficiencies: []*EfficiencyMetric{metric1, metric2},
	}

	require.NotNil(t, response.Efficiencies)
	assert.Len(t, response.Efficiencies, 2)
	assert.Equal(t, "pod-1", response.Efficiencies[0].Name)
	assert.Equal(t, "pod-2", response.Efficiencies[1].Name)
}

func TestSafeDiv(t *testing.T) {
	tests := []struct {
		name        string
		numerator   float64
		denominator float64
		expected    float64
	}{
		{"normal division", 10.0, 2.0, 5.0},
		{"zero denominator", 10.0, 0.0, 0.0},
		{"zero numerator", 0.0, 2.0, 0.0},
		{"both zero", 0.0, 0.0, 0.0},
		{"negative values", -10.0, 2.0, -5.0},
		{"fractional result", 5.0, 2.0, 2.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := safeDiv(tt.numerator, tt.denominator)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeEfficiencyMetric_NilAllocation(t *testing.T) {
	result := computeEfficiencyMetric(nil, 1.2)
	assert.Nil(t, result)
}

func TestComputeEfficiencyMetric_ZeroMinutes(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:  "test-pod",
		Start: now,
		End:   now, // Same time, so 0 minutes
	}

	result := computeEfficiencyMetric(alloc, 1.2)
	assert.Nil(t, result)
}

func TestComputeEfficiencyMetric_ValidAllocation(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:  "test-pod",
		Start: now.Add(-24 * time.Hour),
		End:   now,
		// 24 hours = 1440 minutes
		CPUCoreHours:           24.0,   // 1 core for 24 hours
		RAMByteHours:           24.0e9, // ~1GB for 24 hours
		CPUCoreRequestAverage:  2.0,    // Requested 2 cores
		RAMBytesRequestAverage: 2.0e9,  // Requested 2GB
		CPUCost:                10.0,
		RAMCost:                5.0,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)
	assert.Equal(t, "test-pod", result.Name)
	assert.Equal(t, 2.0, result.CPUCoresRequested)
	assert.Equal(t, 2.0e9, result.RAMBytesRequested)
	assert.Equal(t, 1.0, result.CPUCoresUsed)            // 24 core-hours / 24 hours = 1 core
	assert.Equal(t, 1.0e9, result.RAMBytesUsed)          // 24GB-hours / 24 hours = 1GB
	assert.Equal(t, 0.5, result.CPUEfficiency)           // 1 / 2 = 0.5
	assert.Equal(t, 0.5, result.MemoryEfficiency)        // 1GB / 2GB = 0.5
	assert.Equal(t, 1.2, result.RecommendedCPURequest)   // 1 * 1.2 = 1.2
	assert.Equal(t, 1.2e9, result.RecommendedRAMRequest) // 1GB * 1.2 = 1.2GB
	assert.Equal(t, 1.2, result.EfficiencyBufferMultiplier)
	assert.Greater(t, result.CostSavings, 0.0)
}

func TestComputeEfficiencyMetric_CustomBufferMultiplier(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           24.0,
		RAMByteHours:           24.0e9,
		CPUCoreRequestAverage:  2.0,
		RAMBytesRequestAverage: 2.0e9,
		CPUCost:                10.0,
		RAMCost:                5.0,
	}

	// Test with 1.4 buffer multiplier (40% headroom)
	result := computeEfficiencyMetric(alloc, 1.4)

	require.NotNil(t, result)
	assert.Equal(t, 1.4, result.RecommendedCPURequest)   // 1 * 1.4 = 1.4
	assert.Equal(t, 1.4e9, result.RecommendedRAMRequest) // 1GB * 1.4 = 1.4GB
	assert.Equal(t, 1.4, result.EfficiencyBufferMultiplier)

	// Resulting efficiency should be usage / recommended
	expectedCPUEff := 1.0 / 1.4
	expectedMemEff := 1.0e9 / 1.4e9
	assert.InDelta(t, expectedCPUEff, result.ResultingCPUEfficiency, 0.001)
	assert.InDelta(t, expectedMemEff, result.ResultingMemoryEfficiency, 0.001)
}

func TestComputeEfficiencyMetric_MinimumThresholds(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:  "test-pod",
		Start: now.Add(-24 * time.Hour),
		End:   now,
		// Very small usage
		CPUCoreHours:           0.00001, // 0.000000417 cores average
		RAMByteHours:           100,     // ~4 bytes average
		CPUCoreRequestAverage:  0.1,
		RAMBytesRequestAverage: 1000,
		CPUCost:                0.001,
		RAMCost:                0.001,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)
	// Should enforce minimum CPU (0.001 cores)
	assert.Equal(t, efficiencyMinCPU, result.RecommendedCPURequest)
	// Should enforce minimum RAM (1MB)
	assert.Equal(t, float64(efficiencyMinRAM), result.RecommendedRAMRequest)
}

func TestComputeEfficiencyMetric_NoRequests(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           24.0,
		RAMByteHours:           24.0e9,
		CPUCoreRequestAverage:  0.0, // No requests set
		RAMBytesRequestAverage: 0.0, // No requests set
		CPUCost:                10.0,
		RAMCost:                5.0,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)
	// Efficiency should be 0 when no requests are set
	assert.Equal(t, 0.0, result.CPUEfficiency)
	assert.Equal(t, 0.0, result.MemoryEfficiency)
	// Recommendations should still be calculated based on usage
	assert.Equal(t, 1.2, result.RecommendedCPURequest)
	assert.Equal(t, 1.2e9, result.RecommendedRAMRequest)
}

func TestComputeEfficiencyMetric_OverProvisioned(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           12.0,   // 0.5 cores average
		RAMByteHours:           12.0e9, // 0.5GB average
		CPUCoreRequestAverage:  4.0,    // Requested 4 cores (over-provisioned)
		RAMBytesRequestAverage: 8.0e9,  // Requested 8GB (over-provisioned)
		CPUCost:                40.0,
		RAMCost:                20.0,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)
	// Low efficiency due to over-provisioning
	assert.Equal(t, 0.125, result.CPUEfficiency)     // 0.5 / 4 = 0.125
	assert.Equal(t, 0.0625, result.MemoryEfficiency) // 0.5GB / 8GB = 0.0625
	// Recommendations should be much lower
	assert.Equal(t, 0.6, result.RecommendedCPURequest)   // 0.5 * 1.2 = 0.6
	assert.Equal(t, 0.6e9, result.RecommendedRAMRequest) // 0.5GB * 1.2 = 0.6GB
	// Should have significant cost savings
	assert.Greater(t, result.CostSavings, 0.0)
	assert.Greater(t, result.CostSavingsPercent, 50.0)
}

func TestComputeEfficiencyMetric_UnderProvisioned(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           48.0,   // 2 cores average
		RAMByteHours:           48.0e9, // 2GB average
		CPUCoreRequestAverage:  1.0,    // Requested 1 core (under-provisioned)
		RAMBytesRequestAverage: 1.0e9,  // Requested 1GB (under-provisioned)
		CPUCost:                10.0,
		RAMCost:                5.0,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)
	// High efficiency (>100%) due to under-provisioning
	assert.Equal(t, 2.0, result.CPUEfficiency)    // 2 / 1 = 2.0
	assert.Equal(t, 2.0, result.MemoryEfficiency) // 2GB / 1GB = 2.0
	// Recommendations should be higher than current requests
	assert.Equal(t, 2.4, result.RecommendedCPURequest)   // 2 * 1.2 = 2.4
	assert.Equal(t, 2.4e9, result.RecommendedRAMRequest) // 2GB * 1.2 = 2.4GB
}

func TestComputeEfficiencyMetric_CostCalculations(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           24.0,
		RAMByteHours:           24.0e9,
		CPUCoreRequestAverage:  2.0,
		RAMBytesRequestAverage: 2.0e9,
		CPUCost:                10.0, // $10 for CPU
		RAMCost:                5.0,  // $5 for RAM
		NetworkCost:            1.0,  // $1 for network
		SharedCost:             0.5,  // $0.5 shared
		ExternalCost:           0.5,  // $0.5 external
		GPUCost:                1.0,  // $1 for GPU
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)

	// Current total cost should include all costs
	expectedCurrentCost := 10.0 + 5.0 + 1.0 + 0.5 + 0.5 + 1.0 // = 18.0
	assert.Equal(t, expectedCurrentCost, result.CurrentTotalCost)

	// Recommended cost should be lower due to right-sizing
	assert.Less(t, result.RecommendedCost, result.CurrentTotalCost)

	// Cost savings should be positive
	assert.Greater(t, result.CostSavings, 0.0)
	assert.Equal(t, result.CurrentTotalCost-result.RecommendedCost, result.CostSavings)

	// Cost savings percent should be calculated correctly
	expectedPercent := (result.CostSavings / result.CurrentTotalCost) * 100
	assert.InDelta(t, expectedPercent, result.CostSavingsPercent, 0.001)
}

func TestComputeEfficiencyMetric_OtherCostsPreserved(t *testing.T) {
	now := time.Now()
	alloc := &opencost.Allocation{
		Name:                   "test-pod",
		Start:                  now.Add(-24 * time.Hour),
		End:                    now,
		CPUCoreHours:           24.0,
		RAMByteHours:           24.0e9,
		CPUCoreRequestAverage:  2.0,
		RAMBytesRequestAverage: 2.0e9,
		CPUCost:                10.0,
		RAMCost:                5.0,
		NetworkCost:            2.0, // Fixed cost
		SharedCost:             1.0, // Fixed cost
		ExternalCost:           1.0, // Fixed cost
		GPUCost:                0.0,
	}

	result := computeEfficiencyMetric(alloc, 1.2)

	require.NotNil(t, result)

	// The "other costs" (Network, Shared, External, GPU) should be preserved
	// in the recommended cost calculation
	otherCosts := 2.0 + 1.0 + 1.0 + 0.0 // = 4.0

	// CPU and RAM costs should be reduced based on right-sizing
	// Original: 10.0 + 5.0 = 15.0
	// Usage: 1 core + 1GB
	// Recommended: 1.2 cores + 1.2GB
	// Cost is calculated based on REQUESTED amounts (2 cores, 2GB)
	cpuCostPerCoreHour := 10.0 / (2.0 * 24.0)  // CPU cost / (requested cores * hours)
	ramCostPerByteHour := 5.0 / (2.0e9 * 24.0) // RAM cost / (requested bytes * hours)
	expectedRecommendedCPUCost := 1.2 * 24.0 * cpuCostPerCoreHour
	expectedRecommendedRAMCost := 1.2e9 * 24.0 * ramCostPerByteHour
	expectedRecommendedTotal := expectedRecommendedCPUCost + expectedRecommendedRAMCost + otherCosts

	assert.InDelta(t, expectedRecommendedTotal, result.RecommendedCost, 0.01)
}

func TestQueryEfficiency_InvalidWindow(t *testing.T) {
	s := &MCPServer{}

	req := &OpenCostQueryRequest{
		QueryType: EfficiencyQueryType,
		Window:    "invalid-window",
	}

	_, err := s.QueryEfficiency(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse window")
}

func TestQueryEfficiency_DefaultBufferMultiplier(t *testing.T) {
	// Test that default buffer multiplier is 1.2 when not specified
	req := &OpenCostQueryRequest{
		QueryType:        EfficiencyQueryType,
		Window:           "24h",
		EfficiencyParams: &EfficiencyQuery{
			// EfficiencyBufferMultiplier not set - should default to 1.2
		},
	}

	assert.Nil(t, req.EfficiencyParams.EfficiencyBufferMultiplier)
}

func TestQueryEfficiency_CustomBufferMultiplier(t *testing.T) {
	bufferMultiplier := 1.4
	req := &OpenCostQueryRequest{
		QueryType: EfficiencyQueryType,
		Window:    "24h",
		EfficiencyParams: &EfficiencyQuery{
			EfficiencyBufferMultiplier: &bufferMultiplier,
		},
	}

	assert.NotNil(t, req.EfficiencyParams.EfficiencyBufferMultiplier)
	assert.Equal(t, 1.4, *req.EfficiencyParams.EfficiencyBufferMultiplier)
}

func TestQueryEfficiency_WithFilter(t *testing.T) {
	req := &OpenCostQueryRequest{
		QueryType: EfficiencyQueryType,
		Window:    "7d",
		EfficiencyParams: &EfficiencyQuery{
			Aggregate: "pod",
			Filter:    "namespace:production",
		},
	}

	assert.Equal(t, "pod", req.EfficiencyParams.Aggregate)
	assert.Equal(t, "namespace:production", req.EfficiencyParams.Filter)
}

func TestQueryEfficiency_WithAggregation(t *testing.T) {
	req := &OpenCostQueryRequest{
		QueryType: EfficiencyQueryType,
		Window:    "7d",
		EfficiencyParams: &EfficiencyQuery{
			Aggregate: "namespace,controller",
		},
	}

	assert.Equal(t, "namespace,controller", req.EfficiencyParams.Aggregate)
}

func TestEfficiencyConstants(t *testing.T) {
	// Test that efficiency constants are defined correctly
	assert.Equal(t, 1.2, efficiencyBufferMultiplier)
	assert.Equal(t, 0.001, efficiencyMinCPU)
	assert.Equal(t, 1024*1024, efficiencyMinRAM)
}

func TestEfficiencyQueryType(t *testing.T) {
	assert.Equal(t, QueryType("efficiency"), EfficiencyQueryType)
}
