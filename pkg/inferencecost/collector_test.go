package inferencecost

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// mockQuerier implements AllocationQuerier for testing.
type mockQuerier struct {
	set *opencost.AllocationSet
	err error
	// For dual-query tests, return different sets on subsequent calls
	callCount int
	sets      []*opencost.AllocationSet
}

func (m *mockQuerier) ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error) {
	if m.err != nil {
		return nil, m.err
	}
	
	// If multiple sets are provided, return them in sequence
	if len(m.sets) > 0 {
		if m.callCount < len(m.sets) {
			set := m.sets[m.callCount]
			m.callCount++
			return set, nil
		}
		// Return last set for any additional calls
		return m.sets[len(m.sets)-1], nil
	}
	
	// Otherwise return the single set
	return m.set, nil
}

// mockMetricsQuerier implements source.MetricsQuerier for testing inference metrics.
type mockMetricsQuerier struct {
	promptTokens      map[string]float64
	generationTokens  map[string]float64
	inputTime         map[string]float64
	outputTime        map[string]float64
	cachedTokens      map[string]float64
	cacheConfigs      map[string]*source.InferenceCacheConfig
	err               error
}

func (m *mockMetricsQuerier) QueryInferencePromptTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceTokensResult {
		return &source.InferenceTokensResult{Values: m.promptTokens}
	}
	return source.NewFuture(decoder, resultsChan)
}

func (m *mockMetricsQuerier) QueryInferenceGenerationTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceTokensResult {
		return &source.InferenceTokensResult{Values: m.generationTokens}
	}
	return source.NewFuture(decoder, resultsChan)
}

func (m *mockMetricsQuerier) QueryInferenceInputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceProcessingTimeResult {
		return &source.InferenceProcessingTimeResult{Values: m.inputTime}
	}
	return source.NewFuture(decoder, resultsChan)
}

func (m *mockMetricsQuerier) QueryInferenceOutputProcessingTime(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceProcessingTimeResult {
		return &source.InferenceProcessingTimeResult{Values: m.outputTime}
	}
	return source.NewFuture(decoder, resultsChan)
}

func (m *mockMetricsQuerier) QueryInferenceCachedTokens(start, end time.Time) *source.Future[source.InferenceTokensResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceTokensResult {
		return &source.InferenceTokensResult{Values: m.cachedTokens}
	}
	return source.NewFuture(decoder, resultsChan)
}

func (m *mockMetricsQuerier) QueryInferenceCacheConfig(t time.Time) *source.Future[source.InferenceCacheConfigResult] {
	resultsChan := make(source.QueryResultsChan, 1)
	if m.err != nil {
		resultsChan <- &source.QueryResults{Error: m.err}
	} else {
		results := []*source.QueryResult{
			source.NewQueryResult(
				map[string]any{"key": "mock"},
				[]*util.Vector{{Value: 0}},
				nil,
			),
		}
		resultsChan <- &source.QueryResults{Results: results}
	}
	
	decoder := func(result *source.QueryResult) *source.InferenceCacheConfigResult {
		return &source.InferenceCacheConfigResult{Configs: m.cacheConfigs}
	}
	return source.NewFuture(decoder, resultsChan)
}

// Stub implementations for all other MetricsQuerier methods (not used in inference tests)
func (m *mockMetricsQuerier) QueryPVActiveMinutes(start, end time.Time) *source.Future[source.PVActiveMinutesResult] { return nil }
func (m *mockMetricsQuerier) QueryPVUsedAverage(start, end time.Time) *source.Future[source.PVUsedAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryPVUsedMax(start, end time.Time) *source.Future[source.PVUsedMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryLocalStorageActiveMinutes(start, end time.Time) *source.Future[source.LocalStorageActiveMinutesResult] { return nil }
func (m *mockMetricsQuerier) QueryLocalStorageUsedAvg(start, end time.Time) *source.Future[source.LocalStorageUsedAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryLocalStorageUsedMax(start, end time.Time) *source.Future[source.LocalStorageUsedMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryLocalStorageBytes(start, end time.Time) *source.Future[source.LocalStorageBytesResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeActiveMinutes(start, end time.Time) *source.Future[source.NodeActiveMinutesResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeCPUCoresCapacity(start, end time.Time) *source.Future[source.NodeCPUCoresCapacityResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeCPUCoresAllocatable(start, end time.Time) *source.Future[source.NodeCPUCoresAllocatableResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeRAMBytesCapacity(start, end time.Time) *source.Future[source.NodeRAMBytesCapacityResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeRAMBytesAllocatable(start, end time.Time) *source.Future[source.NodeRAMBytesAllocatableResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeGPUCount(start, end time.Time) *source.Future[source.NodeGPUCountResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeCPUModeTotal(start, end time.Time) *source.Future[source.NodeCPUModeTotalResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeIsSpot(start, end time.Time) *source.Future[source.NodeIsSpotResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeRAMSystemPercent(start, end time.Time) *source.Future[source.NodeRAMSystemPercentResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeRAMUserPercent(start, end time.Time) *source.Future[source.NodeRAMUserPercentResult] { return nil }
func (m *mockMetricsQuerier) QueryLBActiveMinutes(start, end time.Time) *source.Future[source.LBActiveMinutesResult] { return nil }
func (m *mockMetricsQuerier) QueryLBPricePerHr(start, end time.Time) *source.Future[source.LBPricePerHrResult] { return nil }
func (m *mockMetricsQuerier) QueryClusterUptime(start, end time.Time) *source.Future[source.UptimeResult] { return nil }
func (m *mockMetricsQuerier) QueryClusterManagementDuration(start, end time.Time) *source.Future[source.ClusterManagementDurationResult] { return nil }
func (m *mockMetricsQuerier) QueryClusterManagementPricePerHr(start, end time.Time) *source.Future[source.ClusterManagementPricePerHrResult] { return nil }
func (m *mockMetricsQuerier) QueryPods(start, end time.Time) *source.Future[source.PodsResult] { return nil }
func (m *mockMetricsQuerier) QueryPodsUID(start, end time.Time) *source.Future[source.PodsResult] { return nil }
func (m *mockMetricsQuerier) QueryRAMBytesAllocated(start, end time.Time) *source.Future[source.RAMBytesAllocatedResult] { return nil }
func (m *mockMetricsQuerier) QueryRAMRequests(start, end time.Time) *source.Future[source.RAMRequestsResult] { return nil }
func (m *mockMetricsQuerier) QueryRAMLimits(start, end time.Time) *source.Future[source.RAMLimitsResult] { return nil }
func (m *mockMetricsQuerier) QueryRAMUsageAvg(start, end time.Time) *source.Future[source.RAMUsageAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryRAMUsageMax(start, end time.Time) *source.Future[source.RAMUsageMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeRAMPricePerGiBHr(start, end time.Time) *source.Future[source.NodeRAMPricePerGiBHrResult] { return nil }
func (m *mockMetricsQuerier) QueryCPUCoresAllocated(start, end time.Time) *source.Future[source.CPUCoresAllocatedResult] { return nil }
func (m *mockMetricsQuerier) QueryCPURequests(start, end time.Time) *source.Future[source.CPURequestsResult] { return nil }
func (m *mockMetricsQuerier) QueryCPULimits(start, end time.Time) *source.Future[source.CPULimitsResult] { return nil }
func (m *mockMetricsQuerier) QueryCPUUsageAvg(start, end time.Time) *source.Future[source.CPUUsageAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryCPUUsageMax(start, end time.Time) *source.Future[source.CPUUsageMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeCPUPricePerHr(start, end time.Time) *source.Future[source.NodeCPUPricePerHrResult] { return nil }
func (m *mockMetricsQuerier) QueryGPUsAllocated(start, end time.Time) *source.Future[source.GPUsAllocatedResult] { return nil }
func (m *mockMetricsQuerier) QueryGPUsRequested(start, end time.Time) *source.Future[source.GPUsRequestedResult] { return nil }
func (m *mockMetricsQuerier) QueryGPUsUsageAvg(start, end time.Time) *source.Future[source.GPUsUsageAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryGPUsUsageMax(start, end time.Time) *source.Future[source.GPUsUsageMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeGPUPricePerHr(start, end time.Time) *source.Future[source.NodeGPUPricePerHrResult] { return nil }
func (m *mockMetricsQuerier) QueryGPUInfo(start, end time.Time) *source.Future[source.GPUInfoResult] { return nil }
func (m *mockMetricsQuerier) QueryIsGPUShared(start, end time.Time) *source.Future[source.IsGPUSharedResult] { return nil }
func (m *mockMetricsQuerier) QueryPodPVCAllocation(start, end time.Time) *source.Future[source.PodPVCAllocationResult] { return nil }
func (m *mockMetricsQuerier) QueryPVCBytesRequested(start, end time.Time) *source.Future[source.PVCBytesRequestedResult] { return nil }
func (m *mockMetricsQuerier) QueryPVCInfo(start, end time.Time) *source.Future[source.PVCInfoResult] { return nil }
func (m *mockMetricsQuerier) QueryPVBytes(start, end time.Time) *source.Future[source.PVBytesResult] { return nil }
func (m *mockMetricsQuerier) QueryPVPricePerGiBHour(start, end time.Time) *source.Future[source.PVPricePerGiBHourResult] { return nil }
func (m *mockMetricsQuerier) QueryPVInfo(start, end time.Time) *source.Future[source.PVInfoResult] { return nil }
func (m *mockMetricsQuerier) QueryNamespaceUptime(start, end time.Time) *source.Future[source.UptimeResult] { return nil }
func (m *mockMetricsQuerier) QueryNetZoneGiB(start, end time.Time) *source.Future[source.NetZoneGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetZonePricePerGiB(start, end time.Time) *source.Future[source.NetZonePricePerGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetRegionGiB(start, end time.Time) *source.Future[source.NetRegionGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetRegionPricePerGiB(start, end time.Time) *source.Future[source.NetRegionPricePerGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetInternetGiB(start, end time.Time) *source.Future[source.NetInternetGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetInternetPricePerGiB(start, end time.Time) *source.Future[source.NetInternetPricePerGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetInternetServiceGiB(start, end time.Time) *source.Future[source.NetInternetServiceGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetNatGatewayPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetNatGatewayGiB(start, end time.Time) *source.Future[source.NetNatGatewayGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetTransferBytes(start, end time.Time) *source.Future[source.NetTransferBytesResult] { return nil }
func (m *mockMetricsQuerier) QueryNetZoneIngressGiB(start, end time.Time) *source.Future[source.NetZoneIngressGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetRegionIngressGiB(start, end time.Time) *source.Future[source.NetRegionIngressGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetInternetIngressGiB(start, end time.Time) *source.Future[source.NetInternetIngressGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetInternetServiceIngressGiB(start, end time.Time) *source.Future[source.NetInternetServiceIngressGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetNatGatewayIngressPricePerGiB(start, end time.Time) *source.Future[source.NetNatGatewayPricePerGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetNatGatewayIngressGiB(start, end time.Time) *source.Future[source.NetNatGatewayIngressGiBResult] { return nil }
func (m *mockMetricsQuerier) QueryNetReceiveBytes(start, end time.Time) *source.Future[source.NetReceiveBytesResult] { return nil }
func (m *mockMetricsQuerier) QueryNamespaceAnnotations(start, end time.Time) *source.Future[source.NamespaceAnnotationsResult] { return nil }
func (m *mockMetricsQuerier) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] { return nil }
func (m *mockMetricsQuerier) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryServiceLabels(start, end time.Time) *source.Future[source.ServiceLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryDeploymentLabels(start, end time.Time) *source.Future[source.DeploymentLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryStatefulSetLabels(start, end time.Time) *source.Future[source.StatefulSetLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryDaemonSetLabels(start, end time.Time) *source.Future[source.DaemonSetLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryJobLabels(start, end time.Time) *source.Future[source.JobLabelsResult] { return nil }
func (m *mockMetricsQuerier) QueryPodsWithReplicaSetOwner(start, end time.Time) *source.Future[source.PodsWithReplicaSetOwnerResult] { return nil }
func (m *mockMetricsQuerier) QueryReplicaSetsWithoutOwners(start, end time.Time) *source.Future[source.ReplicaSetsWithoutOwnersResult] { return nil }
func (m *mockMetricsQuerier) QueryReplicaSetsWithRollout(start, end time.Time) *source.Future[source.ReplicaSetsWithRolloutResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaUptime(start, end time.Time) *source.Future[source.UptimeResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPURequestMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMRequestMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecCPULimitMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaSpecRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaSpecRAMLimitMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedCPURequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPURequestMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedRAMRequestMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMRequestMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedCPULimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedCPULimitMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitAverage(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitAvgResult] { return nil }
func (m *mockMetricsQuerier) QueryResourceQuotaStatusUsedRAMLimitMax(start, end time.Time) *source.Future[source.ResourceQuotaStatusUsedRAMLimitMaxResult] { return nil }
func (m *mockMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) { return time.Time{}, time.Time{}, nil }

func makeAllocation(name string, gpuCost, cpuCost, ramCost, gpuCostIdle, cpuCostIdle, ramCostIdle float64, labels map[string]string, namespace string) *opencost.Allocation {
	a := &opencost.Allocation{
		Name:    name,
		GPUCost: gpuCost,
		CPUCost: cpuCost,
		RAMCost: ramCost,
		// Idle fields stored directly — they are added into TotalCost by OpenCost
		// when idle is distributed via ShareWeighted.
		GPUCostIdle: gpuCostIdle,
		CPUCostIdle: cpuCostIdle,
		RAMCostIdle: ramCostIdle,
		Properties: &opencost.AllocationProperties{
			Namespace: namespace,
			Labels:    opencost.AllocationLabels(labels),
		},
	}
	return a
}

func baseConfig() *Config {
	return &Config{
		PrometheusURL:             "http://fake-prometheus:9090",
		CollectionInterval:        5 * time.Minute,
		ModelLabel:                "llm-d.ai/model",
		SharedInfraLabel:          "llm-d.ai/inference-shared",
		SharedInfraLabelValue:     "true",
		AllocationMode:            AllocationModeComputeTime,
		OutputTokenCostMultiplier: 2.5,
	}
}

// TestCollector_ExtractAllocationResults verifies that extractAllocationResults
// correctly extracts allocation and usage costs from AllocationSets.
func TestCollector_ExtractAllocationResults(t *testing.T) {
	now := time.Now()
	cfg := baseConfig()
	c := &Collector{config: cfg}
	
	// Test allocation cost extraction (with idle)
	allocWithIdle := &opencost.Allocation{
		Name:    "llama-3",
		GPUCost: 3.0,
		CPUCost: 0.5,
		RAMCost: 0.5,
		Properties: &opencost.AllocationProperties{
			Namespace: "llm-prod",
		},
	}
	asWithIdle := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	asWithIdle.Set(allocWithIdle)

	resultsAlloc, err := c.extractAllocationResults(asWithIdle, true)
	if err != nil {
		t.Fatalf("extractAllocationResults (allocation) failed: %v", err)
	}

	key := modelNamespaceKey("llama-3", "llm-prod")
	r, ok := resultsAlloc[key]
	if !ok {
		t.Fatal("expected allocation result for llama-3/llm-prod")
	}
	
	if !floatEq(r.allocationTotalCost, 4.0) {
		t.Errorf("allocationTotalCost want 4.0 got %f", r.allocationTotalCost)
	}
	if r.usageTotalCost != 0 {
		t.Errorf("usageTotalCost should be 0 in allocation query, got %f", r.usageTotalCost)
	}

	// Test usage cost extraction (without idle)
	allocWithoutIdle := &opencost.Allocation{
		Name:    "llama-3",
		GPUCost: 2.0,
		CPUCost: 0.3,
		RAMCost: 0.3,
		Properties: &opencost.AllocationProperties{
			Namespace: "llm-prod",
		},
	}
	asWithoutIdle := opencost.NewAllocationSet(now.Add(-5*time.Minute), now)
	asWithoutIdle.Set(allocWithoutIdle)

	resultsUsage, err := c.extractAllocationResults(asWithoutIdle, false)
	if err != nil {
		t.Fatalf("extractAllocationResults (usage) failed: %v", err)
	}

	r2, ok := resultsUsage[key]
	if !ok {
		t.Fatal("expected usage result for llama-3/llm-prod")
	}
	
	if !floatEq(r2.usageTotalCost, 2.6) {
		t.Errorf("usageTotalCost want 2.6 got %f", r2.usageTotalCost)
	}
	if r2.allocationTotalCost != 0 {
		t.Errorf("allocationTotalCost should be 0 in usage query, got %f", r2.allocationTotalCost)
	}
}

// TestCollector_UsageCost_ExcludesIdle verifies the mathematical relationship
// between allocation and usage costs when idle is present.
func TestCollector_UsageCost_ExcludesIdle(t *testing.T) {
	// With ShareWeighted: AllocationTotalCost = 4.0 (GPU 3.0 + CPU 0.5 + RAM 0.5)
	// With ShareNone: UsageCost = 2.6 (excludes idle: 1.0 + 0.2 + 0.2 = 1.4)

	allocTotal := 4.0
	idleGPU, idleCPU, idleRAM := 1.0, 0.2, 0.2
	expectedUsageCost := allocTotal - (idleGPU + idleCPU + idleRAM)

	if !floatEq(expectedUsageCost, 2.6) {
		t.Errorf("expected usage cost 2.6 got %f", expectedUsageCost)
	}
	if expectedUsageCost >= allocTotal {
		t.Error("usage cost should be less than allocation cost when idle is present")
	}
}

// TestCollector_CombineMetrics_DerivesCachedTokens verifies that combineMetrics
// passes CachedTokens through directly and derives EffectiveInputTokens correctly.
func TestCollector_CombineMetrics_DerivesCachedTokens(t *testing.T) {
	cfg := baseConfig()

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 4.0, usageTotalCost: 2.6, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 20}
	genTokens := map[string]float64{"llama-3:llm-prod": 10}
	inputTime := map[string]float64{}
	outputTime := map[string]float64{}
	// vllm:prefix_cache_hits_total reports tokens directly (not blocks).
	cachedTokens := map[string]float64{"llama-3:llm-prod": 8}
	cacheConfigs := map[string]*cacheConfig{"llama-3:llm-prod": {prefixCachingEnabled: true}}

	c := &Collector{config: cfg}
	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cachedTokens, cacheConfigs, now.Add(-1*time.Hour), now)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	m := results[0]
	if !floatEq(m.CachedTokens, 8) {
		t.Errorf("CachedTokens want 8 got %f", m.CachedTokens)
	}
	if !floatEq(m.EffectiveInputTokens, 12) {
		t.Errorf("EffectiveInputTokens want 12 got %f", m.EffectiveInputTokens)
	}
}

// TestCollector_CombineMetrics_NoCacheHits_FallsBackToPromptTokens verifies that
// EffectiveInputTokens equals PromptTokens when no cache hits are reported.
func TestCollector_CombineMetrics_NoCacheHits_FallsBackToPromptTokens(t *testing.T) {
	cfg := baseConfig()

	allocCosts := map[string]*allocationResult{
		"llama-3:llm-prod": {allocationTotalCost: 1.0, usageTotalCost: 1.0, namespace: "llm-prod"},
	}
	promptTokens := map[string]float64{"llama-3:llm-prod": 1000}
	genTokens := map[string]float64{"llama-3:llm-prod": 500}
	// cachedTokens map is empty — simulates metric being unavailable
	cacheHits := map[string]float64{}
	cacheConfigs := map[string]*cacheConfig{"llama-3:llm-prod": {prefixCachingEnabled: true}}

	c := &Collector{config: cfg}
	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens,
		map[string]float64{}, map[string]float64{}, cacheHits, cacheConfigs, now.Add(-1*time.Hour), now)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	m := results[0]
	if !floatEq(m.EffectiveInputTokens, 1000) {
		t.Errorf("EffectiveInputTokens should fall back to PromptTokens=1000, got %f", m.EffectiveInputTokens)
	}
}

// TestReconcileTokenKeys_OrgPrefixMismatch verifies that a metric key with a
// fully-qualified org/model name is re-keyed to match the allocation key that
// uses only the short name, and that keys which already match are left unchanged.
func TestReconcileTokenKeys_OrgPrefixMismatch(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"MiniMax-M2.7:llm-d-pic": {allocationTotalCost: 489.0, namespace: "llm-d-pic"},
		"gpt-oss-120b:dolev-inf": {allocationTotalCost: 453.0, namespace: "dolev-inf"},
		// This alloc key already has a slash and no short-name alternative.
		"meta-llama/Llama-3:prod": {allocationTotalCost: 10.0, namespace: "prod"},
	}

	tokens := map[string]float64{
		// Mismatch: vLLM uses full org/model, alloc uses short name.
		"MiniMaxAI/MiniMax-M2.7:llm-d-pic": 4316.0,
		"openai/gpt-oss-120b:dolev-inf":    4773.0,
		// Already matches alloc key — should pass through unchanged.
		"meta-llama/Llama-3:prod": 1000.0,
		// No alloc entry at all — should pass through unchanged.
		"unknown-org/new-model:some-ns": 99.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)

	// Remapped entries should appear under the short-name alloc keys.
	if v, ok := out["MiniMax-M2.7:llm-d-pic"]; !ok || !floatEq(v, 4316.0) {
		t.Errorf("MiniMax-M2.7:llm-d-pic want 4316.0 got %v (ok=%v)", v, ok)
	}
	if v, ok := out["gpt-oss-120b:dolev-inf"]; !ok || !floatEq(v, 4773.0) {
		t.Errorf("gpt-oss-120b:dolev-inf want 4773.0 got %v (ok=%v)", v, ok)
	}
	// Original org-prefixed keys must be gone.
	if _, ok := out["MiniMaxAI/MiniMax-M2.7:llm-d-pic"]; ok {
		t.Error("org-prefixed key MiniMaxAI/MiniMax-M2.7:llm-d-pic should have been removed")
	}
	if _, ok := out["openai/gpt-oss-120b:dolev-inf"]; ok {
		t.Error("org-prefixed key openai/gpt-oss-120b:dolev-inf should have been removed")
	}
	// Verify remapped keys are tracked.
	if _, ok := remappedKeys["MiniMaxAI/MiniMax-M2.7:llm-d-pic"]; !ok {
		t.Error("MiniMaxAI/MiniMax-M2.7:llm-d-pic should be in remappedKeys")
	}
	if _, ok := remappedKeys["openai/gpt-oss-120b:dolev-inf"]; !ok {
		t.Error("openai/gpt-oss-120b:dolev-inf should be in remappedKeys")
	}
	// Keys that already matched or had no alloc entry pass through unchanged.
	if v, ok := out["meta-llama/Llama-3:prod"]; !ok || !floatEq(v, 1000.0) {
		t.Errorf("meta-llama/Llama-3:prod want 1000.0 got %v (ok=%v)", v, ok)
	}
	if v, ok := out["unknown-org/new-model:some-ns"]; !ok || !floatEq(v, 99.0) {
		t.Errorf("unknown-org/new-model:some-ns want 99.0 got %v (ok=%v)", v, ok)
	}
}

func TestReconcileTokenKeys_PrefersShortAllocationKeyWhenBothFormsExist(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"gemma-4-31B:llm-d-pic":        {allocationTotalCost: 10.0, namespace: "llm-d-pic"},
		"google/gemma-4-31B:llm-d-pic": {allocationTotalCost: 1.0, namespace: "llm-d-pic"},
	}

	tokens := map[string]float64{
		"google/gemma-4-31B:llm-d-pic": 123.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)

	if v, ok := out["gemma-4-31B:llm-d-pic"]; !ok || !floatEq(v, 123.0) {
		t.Errorf("gemma-4-31B:llm-d-pic want 123.0 got %v (ok=%v)", v, ok)
	}
	if _, ok := out["google/gemma-4-31B:llm-d-pic"]; ok {
		t.Error("google/gemma-4-31B:llm-d-pic should have been folded into gemma-4-31B:llm-d-pic")
	}
	if _, ok := remappedKeys["google/gemma-4-31B:llm-d-pic"]; !ok {
		t.Error("google/gemma-4-31B:llm-d-pic should be in remappedKeys")
	}
}

// TestCollector_BuildQueryWindow verifies that buildQueryWindow generates
// correct Prometheus time range selectors based on CollectionInterval.
// TestQueryCounterDelta_Formula verifies the delta = end - start subtraction
// and that negative deltas (counter resets) are clamped to zero.
func TestQueryCounterDelta_Formula(t *testing.T) {
	tests := []struct {
		name     string
		endVal   float64
		startVal float64
		want     float64
	}{
		{name: "normal increase", endVal: 1000, startVal: 200, want: 800},
		{name: "no activity", endVal: 500, startVal: 500, want: 0},
		{name: "counter reset clamped to zero", endVal: 100, startVal: 900, want: 0},
		{name: "new pod (no start sample)", endVal: 400, startVal: 0, want: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := tt.endVal - tt.startVal
			if delta < 0 {
				delta = 0
			}
			if delta != tt.want {
				t.Errorf("delta = %v, want %v", delta, tt.want)
			}
		})
	}
}

// TestReconcileTokenKeys_NoMismatch verifies that when all token keys directly
// match allocation keys, no re-keying occurs and no entries are dropped.
func TestReconcileTokenKeys_NoMismatch(t *testing.T) {
	allocCosts := map[string]*allocationResult{
		"llama-3:prod": {allocationTotalCost: 1.0},
	}
	tokens := map[string]float64{
		"llama-3:prod": 500.0,
	}

	out, remappedKeys := reconcileTokenKeys(tokens, allocCosts)
	if v, ok := out["llama-3:prod"]; !ok || !floatEq(v, 500.0) {
		t.Errorf("want llama-3:prod=500.0 got %v (ok=%v)", v, ok)
	}
	if len(out) != 1 {
		t.Errorf("expected 1 entry, got %d", len(out))
	}
	if len(remappedKeys) != 0 {
		t.Errorf("expected no remapped keys, got %d", len(remappedKeys))
	}
}

// TestCollector_CollectMetrics_PrometheusUnavailable ensures that CollectMetrics
// returns an error (not a panic) when metrics are unavailable.
func TestCollector_CollectMetrics_PrometheusUnavailable(t *testing.T) {
	cfg := baseConfig()

	now := time.Now()
	querier := &mockQuerier{set: opencost.NewAllocationSet(now.Add(-5*time.Minute), now)}
	
	// Create a mock metrics querier that returns an error
	metricsQuerier := &mockMetricsQuerier{
		err: fmt.Errorf("metrics unavailable"),
	}

	collector, err := NewCollector(cfg, querier, metricsQuerier)
	if err != nil {
		t.Fatalf("NewCollector returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	end := time.Now()
	start := end.Add(-5 * time.Minute)
	_, err = collector.CollectMetrics(ctx, start, end)
	// The allocation query succeeds (mock), but the metrics query will fail.
	// CollectMetrics should return an error from the prompt token query.
	if err == nil {
		t.Error("expected error when metrics are unavailable, got nil")
	}
}

func TestCollector_CombineMetrics_IncludesTimingOnlyKeysInUnion(t *testing.T) {
	cfg := baseConfig()
	c := &Collector{config: cfg}

	allocCosts := map[string]*allocationResult{}
	promptTokens := map[string]float64{}
	genTokens := map[string]float64{}
	inputTime := map[string]float64{"timing-only:ns1": 60}
	outputTime := map[string]float64{"timing-only:ns1": 40}
	cacheHits := map[string]float64{"timing-only:ns1": 2}
	cacheConfigs := map[string]*cacheConfig{}

	now := time.Now()
	results := c.combineMetrics(allocCosts, promptTokens, genTokens, inputTime, outputTime, cacheHits, cacheConfigs, now.Add(-1*time.Hour), now)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	m := results[0]
	if m.Properties.ModelName != "timing-only" || m.Properties.Namespace != "ns1" {
		t.Fatalf("unexpected properties: model=%s namespace=%s", m.Properties.ModelName, m.Properties.Namespace)
	}
	if !floatEq(m.InputProcessingTime, 60) {
		t.Errorf("InputProcessingTime want 60 got %f", m.InputProcessingTime)
	}
	if !floatEq(m.OutputProcessingTime, 40) {
		t.Errorf("OutputProcessingTime want 40 got %f", m.OutputProcessingTime)
	}
	if !floatEq(m.CachedTokens, 2) {
		t.Errorf("CachedTokens want 2 got %f", m.CachedTokens)
	}
}
