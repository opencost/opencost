package processors

import (
	"fmt"
	"math"
	"sort"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/opencost/opencost/pkg/mcp/utils"
)

// AllocationProcessor processes allocation data for AI consumption
type AllocationProcessor struct {
	efficiencyThreshold   float64
	topDriversCount      int
	insightConfidenceMin float64
}

// NewAllocationProcessor creates a new allocation processor
func NewAllocationProcessor() *AllocationProcessor {
	return &AllocationProcessor{
		efficiencyThreshold:   0.7,
		topDriversCount:      10,
		insightConfidenceMin: 0.6,
	}
}

// ProcessAllocations processes raw allocation data into AI-optimized format
func (p *AllocationProcessor) ProcessAllocations(data *opencost.AllocationSetRange, params *utils.AllocationQueryParams, ctx *types.QueryContext) (*types.AllocationResponseData, error) {
	if data == nil || len(data.Allocations) == 0 {
		return &types.AllocationResponseData{
			MCPResponseData: types.MCPResponseData{
				Summary:   "No allocation data found for the specified criteria",
				Data:      []types.ProcessedAllocation{},
				Metadata:  p.createMetadata(params, ctx, 0),
			},
			Allocations:    []types.ProcessedAllocation{},
			TotalCost:      0,
			TopCostDrivers: []types.CostDriver{},
			Breakdown:      types.AllocationBreakdown{},
		}, nil
	}

	// Process allocations
	processedAllocations := p.processAllocationSets(data.Allocations)
	
	// Calculate total cost
	totalCost := p.calculateTotalCost(processedAllocations)
	
	// Generate cost drivers
	topDrivers := p.generateTopCostDrivers(processedAllocations)
	
	// Generate breakdown
	breakdown := p.generateBreakdown(processedAllocations, params)
	
	// Generate insights
	insights := p.generateInsights(processedAllocations, totalCost)
	
	// Generate trends
	trends := p.generateTrends(data, processedAllocations)
	
	// Generate recommendations
	recommendations := p.generateRecommendations(processedAllocations, insights)

	response := &types.AllocationResponseData{
		MCPResponseData: types.MCPResponseData{
			Summary:         p.generateSummary(processedAllocations, totalCost, params),
			Data:            processedAllocations,
			Insights:        insights,
			Trends:          trends,
			Recommendations: recommendations,
			Metadata:        p.createMetadata(params, ctx, len(processedAllocations)),
		},
		Allocations:    processedAllocations,
		TotalCost:      totalCost,
		TopCostDrivers: topDrivers,
		Breakdown:      breakdown,
	}

	return response, nil
}

// processAllocationSets converts OpenCost allocations to processed format
func (p *AllocationProcessor) processAllocationSets(allocationSets []*opencost.AllocationSet) []types.ProcessedAllocation {
	var processed []types.ProcessedAllocation

	for _, set := range allocationSets {
		if set == nil {
			continue
		}

		for name, allocation := range set.Allocations {
			if allocation == nil {
				continue
			}

			processedAlloc := types.ProcessedAllocation{
				Name:             name,
				Properties:       p.extractProperties(allocation),
				Start:            set.Start(),
				End:              set.End(),
				CPUCoreHours:     allocation.CPUCoreHours,
				RAMByteHours:     allocation.RAMByteHours,
				GPUHours:         allocation.GPUHours,
				PVByteHours:      allocation.PVByteHours(),
				NetworkCost:      allocation.NetworkCost,
				LoadBalancerCost: allocation.LoadBalancerCost,
				TotalCost:        allocation.TotalCost(),
				CPUCost:          allocation.CPUCost,
				RAMCost:          allocation.RAMCost,
				GPUCost:          allocation.GPUCost,
				PVCost:           allocation.PVCost(),
				SharedCost:       allocation.SharedCost,
				ExternalCost:     allocation.ExternalCost,
				Efficiency:       p.calculateEfficiency(allocation),
				CostPerUnit:      p.calculateCostPerUnit(allocation),
				Tags:             p.generateTags(allocation),
			}

			processed = append(processed, processedAlloc)
		}
	}

	// Sort by total cost descending
	sort.Slice(processed, func(i, j int) bool {
		return processed[i].TotalCost > processed[j].TotalCost
	})

	return processed
}

// extractProperties extracts allocation properties
func (p *AllocationProcessor) extractProperties(allocation *opencost.Allocation) map[string]interface{} {
	props := make(map[string]interface{})

	if allocation.Properties != nil {
		// Extract standard properties
		if cluster := allocation.Properties.Cluster; cluster != "" {
			props["cluster"] = cluster
		}
		if namespace := allocation.Properties.Namespace; namespace != "" {
			props["namespace"] = namespace
		}
		if pod := allocation.Properties.Pod; pod != "" {
			props["pod"] = pod
		}
		if container := allocation.Properties.Container; container != "" {
			props["container"] = container
		}
		if node := allocation.Properties.Node; node != "" {
			props["node"] = node
		}
		if controller := allocation.Properties.Controller; controller != "" {
			props["controller"] = controller
		}
		if controllerKind := allocation.Properties.ControllerKind; controllerKind != "" {
			props["controller_kind"] = controllerKind
		}
		if len(allocation.Properties.Services) > 0 {
			props["services"] = allocation.Properties.Services
			// For compatibility, use the first service as "service"
			props["service"] = allocation.Properties.Services[0]
		}

		// Extract labels
		if len(allocation.Properties.Labels) > 0 {
			props["labels"] = allocation.Properties.Labels
		}

		// Extract annotations
		if len(allocation.Properties.Annotations) > 0 {
			props["annotations"] = allocation.Properties.Annotations
		}
	}

	return props
}

// calculateEfficiency calculates resource efficiency metrics
func (p *AllocationProcessor) calculateEfficiency(allocation *opencost.Allocation) *types.ResourceEfficiency {
	if allocation == nil {
		return nil
	}

	efficiency := &types.ResourceEfficiency{
		Recommendations: []string{},
	}

	// Calculate CPU efficiency (simplified - would need usage data in real implementation)
	if allocation.CPUCoreHours > 0 {
		// This is a placeholder - real implementation would compare against usage metrics
		efficiency.CPUEfficiency = 0.6 // Default assumption
	}

	// Calculate RAM efficiency
	if allocation.RAMByteHours > 0 {
		// This is a placeholder - real implementation would compare against usage metrics
		efficiency.RAMEfficiency = 0.65 // Default assumption
	}

	// Calculate overall efficiency
	if efficiency.CPUEfficiency > 0 && efficiency.RAMEfficiency > 0 {
		efficiency.Overall = (efficiency.CPUEfficiency + efficiency.RAMEfficiency) / 2
	} else if efficiency.CPUEfficiency > 0 {
		efficiency.Overall = efficiency.CPUEfficiency
	} else if efficiency.RAMEfficiency > 0 {
		efficiency.Overall = efficiency.RAMEfficiency
	}

	// Generate recommendations based on efficiency
	if efficiency.Overall < 0.5 {
		efficiency.Recommendations = append(efficiency.Recommendations, "Consider reducing resource requests")
		efficiency.Recommendations = append(efficiency.Recommendations, "Implement resource quotas")
	} else if efficiency.Overall < 0.7 {
		efficiency.Recommendations = append(efficiency.Recommendations, "Monitor resource usage patterns")
		efficiency.Recommendations = append(efficiency.Recommendations, "Consider implementing HPA")
	}

	return efficiency
}

// calculateCostPerUnit calculates various cost per unit metrics
func (p *AllocationProcessor) calculateCostPerUnit(allocation *opencost.Allocation) map[string]float64 {
	costPerUnit := make(map[string]float64)

	totalCost := allocation.TotalCost()
	if totalCost == 0 {
		return costPerUnit
	}

	// Cost per CPU core hour
	if allocation.CPUCoreHours > 0 {
		costPerUnit["cpu_core_hour"] = totalCost / allocation.CPUCoreHours
	}

	// Cost per GB RAM hour
	if allocation.RAMByteHours > 0 {
		ramGBHours := allocation.RAMByteHours / (1024 * 1024 * 1024)
		costPerUnit["ram_gb_hour"] = totalCost / ramGBHours
	}

	// Cost per GPU hour
	if allocation.GPUHours > 0 {
		costPerUnit["gpu_hour"] = totalCost / allocation.GPUHours
	}

	// Cost per PV GB hour
	pvByteHours := allocation.PVByteHours()
	if pvByteHours > 0 {
		pvGBHours := pvByteHours / (1024 * 1024 * 1024)
		costPerUnit["pv_gb_hour"] = totalCost / pvGBHours
	}

	return costPerUnit
}

// generateTags generates tags for categorization
func (p *AllocationProcessor) generateTags(allocation *opencost.Allocation) []string {
	var tags []string

	totalCost := allocation.TotalCost()

	// Cost-based tags
	if totalCost > 1000 {
		tags = append(tags, "high-cost")
	} else if totalCost > 100 {
		tags = append(tags, "medium-cost")
	} else {
		tags = append(tags, "low-cost")
	}

	// Resource-based tags
	if allocation.CPUCoreHours > 24*7 { // More than a week of continuous CPU
		tags = append(tags, "cpu-intensive")
	}

	if allocation.RAMByteHours > 8*1024*1024*1024*24*7 { // More than 8GB for a week
		tags = append(tags, "memory-intensive")
	}

	if allocation.GPUHours > 0 {
		tags = append(tags, "gpu-enabled")
	}

	pvByteHours := allocation.PVByteHours()
	if pvByteHours > 0 {
		tags = append(tags, "persistent-storage")
	}

	// Network-based tags
	if allocation.NetworkCost > 0 {
		tags = append(tags, "network-usage")
	}

	if allocation.LoadBalancerCost > 0 {
		tags = append(tags, "load-balanced")
	}

	return tags
}

// calculateTotalCost calculates the total cost across all allocations
func (p *AllocationProcessor) calculateTotalCost(allocations []types.ProcessedAllocation) float64 {
	total := 0.0
	for _, alloc := range allocations {
		total += alloc.TotalCost
	}
	return total
}

// generateTopCostDrivers identifies the top cost drivers
func (p *AllocationProcessor) generateTopCostDrivers(allocations []types.ProcessedAllocation) []types.CostDriver {
	if len(allocations) == 0 {
		return []types.CostDriver{}
	}

	totalCost := p.calculateTotalCost(allocations)
	if totalCost == 0 {
		return []types.CostDriver{}
	}

	var drivers []types.CostDriver
	count := p.topDriversCount
	if len(allocations) < count {
		count = len(allocations)
	}

	for i := 0; i < count; i++ {
		alloc := allocations[i]
		percentage := (alloc.TotalCost / totalCost) * 100

		driver := types.CostDriver{
			Name:        alloc.Name,
			Cost:        alloc.TotalCost,
			Percentage:  percentage,
			Type:        p.inferDriverType(alloc),
			Description: p.generateDriverDescription(alloc),
		}

		drivers = append(drivers, driver)
	}

	return drivers
}

// inferDriverType infers the type of cost driver
func (p *AllocationProcessor) inferDriverType(alloc types.ProcessedAllocation) string {
	if namespace, ok := alloc.Properties["namespace"].(string); ok && namespace != "" {
		return "namespace"
	}
	if pod, ok := alloc.Properties["pod"].(string); ok && pod != "" {
		return "pod"
	}
	if service, ok := alloc.Properties["service"].(string); ok && service != "" {
		return "service"
	}
	if controller, ok := alloc.Properties["controller"].(string); ok && controller != "" {
		return "controller"
	}
	return "allocation"
}

// generateDriverDescription generates a description for a cost driver
func (p *AllocationProcessor) generateDriverDescription(alloc types.ProcessedAllocation) string {
	driverType := p.inferDriverType(alloc)
	
	switch driverType {
	case "namespace":
		return fmt.Sprintf("Namespace with $%.2f in costs", alloc.TotalCost)
	case "pod":
		return fmt.Sprintf("Pod consuming $%.2f", alloc.TotalCost)
	case "service":
		return fmt.Sprintf("Service accounting for $%.2f", alloc.TotalCost)
	case "controller":
		return fmt.Sprintf("Controller managing $%.2f in resources", alloc.TotalCost)
	default:
		return fmt.Sprintf("Allocation with $%.2f in costs", alloc.TotalCost)
	}
}

// generateBreakdown generates cost breakdown by different dimensions
func (p *AllocationProcessor) generateBreakdown(allocations []types.ProcessedAllocation, params *utils.AllocationQueryParams) types.AllocationBreakdown {
	breakdown := types.AllocationBreakdown{}

	// Group by different dimensions
	namespaceGroups := make(map[string]float64)
	podGroups := make(map[string]float64)
	serviceGroups := make(map[string]float64)
	clusterGroups := make(map[string]float64)
	nodeGroups := make(map[string]float64)
	controllerGroups := make(map[string]float64)
	labelGroups := make(map[string]map[string]float64)

	for _, alloc := range allocations {
		// Namespace breakdown
		if namespace, ok := alloc.Properties["namespace"].(string); ok && namespace != "" {
			namespaceGroups[namespace] += alloc.TotalCost
		}

		// Pod breakdown
		if pod, ok := alloc.Properties["pod"].(string); ok && pod != "" {
			podGroups[pod] += alloc.TotalCost
		}

		// Service breakdown
		if service, ok := alloc.Properties["service"].(string); ok && service != "" {
			serviceGroups[service] += alloc.TotalCost
		}

		// Cluster breakdown
		if cluster, ok := alloc.Properties["cluster"].(string); ok && cluster != "" {
			clusterGroups[cluster] += alloc.TotalCost
		}

		// Node breakdown
		if node, ok := alloc.Properties["node"].(string); ok && node != "" {
			nodeGroups[node] += alloc.TotalCost
		}

		// Controller breakdown
		if controller, ok := alloc.Properties["controller"].(string); ok && controller != "" {
			controllerGroups[controller] += alloc.TotalCost
		}

		// Label breakdown
		if labels, ok := alloc.Properties["labels"].(map[string]string); ok {
			for key, value := range labels {
				if labelGroups[key] == nil {
					labelGroups[key] = make(map[string]float64)
				}
				labelGroups[key][value] += alloc.TotalCost
			}
		}
	}

	// Convert to cost drivers
	breakdown.ByNamespace = p.convertToCostDrivers(namespaceGroups, 10)
	breakdown.ByPod = p.convertToCostDrivers(podGroups, 10)
	breakdown.ByService = p.convertToCostDrivers(serviceGroups, 10)
	breakdown.ByCluster = p.convertToCostDrivers(clusterGroups, 10)
	breakdown.ByNode = p.convertToCostDrivers(nodeGroups, 10)
	breakdown.ByController = p.convertToCostDrivers(controllerGroups, 10)

	// Convert label groups
	breakdown.ByLabel = make(map[string][]types.CostDriver)
	for labelKey, labelValues := range labelGroups {
		breakdown.ByLabel[labelKey] = p.convertToCostDrivers(labelValues, 5)
	}

	return breakdown
}

// convertToCostDrivers converts a cost map to sorted cost drivers
func (p *AllocationProcessor) convertToCostDrivers(costMap map[string]float64, limit int) []types.CostDriver {
	var drivers []types.CostDriver
	totalCost := 0.0

	// Calculate total cost
	for _, cost := range costMap {
		totalCost += cost
	}

	// Convert to drivers
	for name, cost := range costMap {
		percentage := 0.0
		if totalCost > 0 {
			percentage = (cost / totalCost) * 100
		}

		drivers = append(drivers, types.CostDriver{
			Name:       name,
			Cost:       cost,
			Percentage: percentage,
			Type:       "breakdown",
		})
	}

	// Sort by cost descending
	sort.Slice(drivers, func(i, j int) bool {
		return drivers[i].Cost > drivers[j].Cost
	})

	// Limit results
	if len(drivers) > limit {
		drivers = drivers[:limit]
	}

	return drivers
}

// generateInsights generates AI-powered insights
func (p *AllocationProcessor) generateInsights(allocations []types.ProcessedAllocation, totalCost float64) []types.Insight {
	var insights []types.Insight

	if len(allocations) == 0 {
		return insights
	}

	// Cost concentration insight
	if len(allocations) > 0 {
		top1Cost := allocations[0].TotalCost
		concentration := (top1Cost / totalCost) * 100

		if concentration > 70 {
			insights = append(insights, types.Insight{
				Type:        "cost_concentration",
				Title:       "High Cost Concentration Risk",
				Description: fmt.Sprintf("Single allocation accounts for %.1f%% of total costs", concentration),
				Severity:    "high",
				Value:       concentration,
				Confidence:  0.95,
				ActionItems: []string{
					"Analyze the top cost driver for optimization opportunities",
					"Consider implementing cost budgets and alerts",
					"Review resource allocation policies",
				},
			})
		}
	}

	// Efficiency insights
	lowEfficiencyCount := 0
	totalEfficiency := 0.0
	efficiencyCount := 0

	for _, alloc := range allocations {
		if alloc.Efficiency != nil {
			totalEfficiency += alloc.Efficiency.Overall
			efficiencyCount++
			if alloc.Efficiency.Overall < 0.5 {
				lowEfficiencyCount++
			}
		}
	}

	if efficiencyCount > 0 {
		avgEfficiency := totalEfficiency / float64(efficiencyCount)
		if avgEfficiency < 0.6 {
			insights = append(insights, types.Insight{
				Type:        "efficiency",
				Title:       "Low Resource Efficiency Detected",
				Description: fmt.Sprintf("Average resource efficiency is %.1f%%, with %d allocations below 50%%", avgEfficiency*100, lowEfficiencyCount),
				Severity:    "medium",
				Value:       avgEfficiency,
				Confidence:  0.8,
				ActionItems: []string{
					"Review resource requests and limits",
					"Implement resource quotas",
					"Consider horizontal pod autoscaling",
					"Audit underutilized resources",
				},
			})
		}
	}

	// GPU usage insight
	gpuAllocations := 0
	gpuCost := 0.0
	for _, alloc := range allocations {
		if alloc.GPUHours > 0 {
			gpuAllocations++
			gpuCost += alloc.GPUCost
		}
	}

	if gpuAllocations > 0 {
		gpuCostPercentage := (gpuCost / totalCost) * 100
		insights = append(insights, types.Insight{
			Type:        "gpu_usage",
			Title:       "GPU Resource Usage",
			Description: fmt.Sprintf("%d allocations using GPUs, accounting for %.1f%% of costs", gpuAllocations, gpuCostPercentage),
			Severity:    "info",
			Value:       gpuCostPercentage,
			Confidence:  0.9,
			ActionItems: []string{
				"Monitor GPU utilization rates",
				"Consider GPU sharing for development workloads",
				"Evaluate GPU instance right-sizing",
			},
		})
	}

	return insights
}

// generateTrends generates trend analysis
func (p *AllocationProcessor) generateTrends(data *opencost.AllocationSetRange, allocations []types.ProcessedAllocation) []types.Trend {
	var trends []types.Trend

	// This is a simplified implementation - real trend analysis would require historical data
	if len(data.Allocations) > 1 {
		// Compare first and last periods
		firstPeriod := data.Allocations[0]
		lastPeriod := data.Allocations[len(data.Allocations)-1]

		firstCost := p.calculateSetTotalCost(firstPeriod)
		lastCost := p.calculateSetTotalCost(lastPeriod)

		if firstCost > 0 {
			changePercent := ((lastCost - firstCost) / firstCost) * 100
			direction := "stable"
			if changePercent > 5 {
				direction = "increasing"
			} else if changePercent < -5 {
				direction = "decreasing"
			}

			trends = append(trends, types.Trend{
				Type:        "cost_trend",
				Direction:   direction,
				Magnitude:   math.Abs(changePercent),
				Description: fmt.Sprintf("Cost %s by %.1f%% over the period", direction, math.Abs(changePercent)),
				Period:      "analyzed_window",
				Confidence:  0.8,
				Impact:      p.determineImpact(math.Abs(changePercent)),
			})
		}
	}

	return trends
}

// calculateSetTotalCost calculates total cost for an allocation set
func (p *AllocationProcessor) calculateSetTotalCost(set *opencost.AllocationSet) float64 {
	if set == nil {
		return 0
	}

	total := 0.0
	for _, allocation := range set.Allocations {
		if allocation != nil {
			total += allocation.TotalCost()
		}
	}
	return total
}

// determineImpact determines the impact level based on magnitude
func (p *AllocationProcessor) determineImpact(magnitude float64) string {
	if magnitude > 50 {
		return "high"
	} else if magnitude > 20 {
		return "medium"
	} else if magnitude > 5 {
		return "low"
	}
	return "minimal"
}

// generateRecommendations generates actionable recommendations
func (p *AllocationProcessor) generateRecommendations(allocations []types.ProcessedAllocation, insights []types.Insight) []types.Recommendation {
	var recommendations []types.Recommendation

	// Cost optimization recommendations based on insights
	for _, insight := range insights {
		switch insight.Type {
		case "cost_concentration":
			recommendations = append(recommendations, types.Recommendation{
				Type:         "cost_optimization",
				Title:        "Diversify Cost Distribution",
				Description:  "Reduce dependency on single high-cost allocation",
				Priority:     "high",
				Impact:       "high",
				Effort:       "medium",
				PotentialSavings: p.calculatePotentialSavings(allocations, 0.1),
				Tags:         []string{"cost-optimization", "risk-reduction"},
				Steps: []string{
					"Analyze the highest cost allocation for optimization opportunities",
					"Implement resource limits and quotas",
					"Consider workload distribution across multiple resources",
					"Set up cost monitoring and alerts",
				},
			})

		case "efficiency":
			recommendations = append(recommendations, types.Recommendation{
				Type:         "resource_optimization",
				Title:        "Improve Resource Efficiency",
				Description:  "Optimize resource requests and limits for better utilization",
				Priority:     "medium",
				Impact:       "medium",
				Effort:       "low",
				PotentialSavings: p.calculatePotentialSavings(allocations, 0.2),
				Tags:         []string{"efficiency", "resource-optimization"},
				Steps: []string{
					"Audit resource requests vs actual usage",
					"Implement Vertical Pod Autoscaler (VPA)",
					"Set appropriate resource quotas",
					"Monitor and adjust based on usage patterns",
				},
			})

		case "gpu_usage":
			if value, ok := insight.Value.(float64); ok && value > 30 {
				recommendations = append(recommendations, types.Recommendation{
					Type:         "gpu_optimization",
					Title:        "Optimize GPU Usage",
					Description:  "Review GPU utilization and consider optimization strategies",
					Priority:     "high",
					Impact:       "high",
					Effort:       "medium",
					PotentialSavings: p.calculatePotentialSavings(allocations, 0.15),
					Tags:         []string{"gpu", "cost-optimization"},
					Steps: []string{
						"Monitor GPU utilization metrics",
						"Implement GPU sharing where appropriate",
						"Consider spot instances for non-critical GPU workloads",
						"Evaluate GPU instance sizing",
					},
				})
			}
		}
	}

	// General recommendations based on allocation patterns
	highCostAllocations := 0
	for _, alloc := range allocations {
		if alloc.TotalCost > 100 {
			highCostAllocations++
		}
	}

	if highCostAllocations > 5 {
		recommendations = append(recommendations, types.Recommendation{
			Type:        "monitoring",
			Title:       "Implement Cost Monitoring",
			Description: "Set up comprehensive cost monitoring and alerting",
			Priority:    "medium",
			Impact:      "medium",
			Effort:      "low",
			Tags:        []string{"monitoring", "governance"},
			Steps: []string{
				"Set up cost budgets and alerts",
				"Implement cost allocation tags",
				"Create cost reporting dashboards",
				"Establish cost review processes",
			},
		})
	}

	return recommendations
}

// calculatePotentialSavings calculates potential savings based on optimization percentage
func (p *AllocationProcessor) calculatePotentialSavings(allocations []types.ProcessedAllocation, optimizationPercent float64) *float64 {
	totalCost := p.calculateTotalCost(allocations)
	savings := totalCost * optimizationPercent
	return &savings
}

// generateSummary generates a natural language summary
func (p *AllocationProcessor) generateSummary(allocations []types.ProcessedAllocation, totalCost float64, params *utils.AllocationQueryParams) string {
	if len(allocations) == 0 {
		return "No allocation data found for the specified criteria."
	}

	summary := fmt.Sprintf("Found %d allocations with a total cost of $%.2f", len(allocations), totalCost)

	if len(allocations) > 0 {
		topAllocation := allocations[0]
		topPercentage := (topAllocation.TotalCost / totalCost) * 100
		summary += fmt.Sprintf(". The highest cost allocation is '%s' at $%.2f (%.1f%% of total)", 
			topAllocation.Name, topAllocation.TotalCost, topPercentage)
	}

	// Add efficiency summary
	lowEfficiencyCount := 0
	for _, alloc := range allocations {
		if alloc.Efficiency != nil && alloc.Efficiency.Overall < 0.6 {
			lowEfficiencyCount++
		}
	}

	if lowEfficiencyCount > 0 {
		summary += fmt.Sprintf(". %d allocations show low resource efficiency (<60%%)", lowEfficiencyCount)
	}

	return summary
}

// createMetadata creates response metadata
func (p *AllocationProcessor) createMetadata(params *utils.AllocationQueryParams, ctx *types.QueryContext, dataPoints int) types.ResponseMetadata {
	metadata := types.ResponseMetadata{
		QueryType:     "allocation",
		ExecutionTime: ctx.GetDuration(),
		DataPoints:    dataPoints,
		TimeRange: types.TimeRange{
			Start: params.Window,
			Step:  params.Step,
		},
		Aggregation: params.Aggregate,
		Currency:    "USD",
	}

	// Add filters info
	if params.Filter != "" {
		metadata.Filters = []types.FilterInfo{
			{
				Field: "filter",
				Value: params.Filter,
				Type:  "expression",
			},
		}
	}

	return metadata
}