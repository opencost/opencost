//go:build mcp

package mcp

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// InsightEngine generates AI-powered insights and recommendations
type InsightEngine struct {
	// Configuration for insight generation
	costThresholds    CostThresholds
	efficiencyTargets map[string]float64
	anomalyDetection  AnomalyDetectionConfig
}

type AnomalyDetectionConfig struct {
	SensitivityLevel    string  // "low", "medium", "high"
	ThresholdMultiplier float64 // Multiplier for standard deviation
	MinDataPoints       int     // Minimum data points needed for analysis
}

// NewInsightEngine creates a new insight engine with default configuration
func NewInsightEngine() *InsightEngine {
	return &InsightEngine{
		costThresholds: CostThresholds{
			HighCostAlert:    1000.0, // $1000
			AnomalyThreshold: 0.3,    // 30% change
			EfficiencyTarget: 0.7,    // 70% efficiency
		},
		efficiencyTargets: map[string]float64{
			"cpu": 0.7,
			"ram": 0.8,
			"gpu": 0.9,
		},
		anomalyDetection: AnomalyDetectionConfig{
			SensitivityLevel:    "medium",
			ThresholdMultiplier: 2.0,
			MinDataPoints:       3,
		},
	}
}

// GenerateAllocationInsights analyzes allocation data and generates insights
func (ie *InsightEngine) GenerateAllocationInsights(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	// High cost analysis
	insights = append(insights, ie.analyzeHighCosts(allocations)...)

	// Efficiency analysis
	insights = append(insights, ie.analyzeEfficiency(allocations)...)

	// Utilization patterns
	insights = append(insights, ie.analyzeUtilizationPatterns(allocations)...)

	// Cost distribution analysis
	insights = append(insights, ie.analyzeCostDistribution(allocations)...)

	// Idle resource analysis
	insights = append(insights, ie.analyzeIdleResources(allocations)...)

	return insights
}

// GenerateAssetInsights analyzes asset data and generates insights
func (ie *InsightEngine) GenerateAssetInsights(assets []EnhancedAsset) []CostInsight {
	var insights []CostInsight

	// Asset utilization analysis
	insights = append(insights, ie.analyzeAssetUtilization(assets)...)

	// Cost per asset type analysis
	insights = append(insights, ie.analyzeAssetCosts(assets)...)

	// Capacity analysis
	insights = append(insights, ie.analyzeCapacity(assets)...)

	return insights
}

// GenerateCloudCostInsights analyzes cloud cost data and generates insights
func (ie *InsightEngine) GenerateCloudCostInsights(cloudCosts []EnhancedCloudCost) []CostInsight {
	var insights []CostInsight

	// Provider cost analysis
	insights = append(insights, ie.analyzeProviderCosts(cloudCosts)...)

	// Service cost analysis
	insights = append(insights, ie.analyzeServiceCosts(cloudCosts)...)

	// Regional cost analysis
	insights = append(insights, ie.analyzeRegionalCosts(cloudCosts)...)

	return insights
}

// GenerateAllocationRecommendations creates actionable recommendations
func (ie *InsightEngine) GenerateAllocationRecommendations(allocations []EnhancedAllocation, insights []CostInsight) []Recommendation {
	var recommendations []Recommendation

	// Rightsizing recommendations
	recommendations = append(recommendations, ie.generateRightsizingRecommendations(allocations)...)

	// Efficiency improvement recommendations
	recommendations = append(recommendations, ie.generateEfficiencyRecommendations(allocations)...)

	// Cost optimization recommendations
	recommendations = append(recommendations, ie.generateCostOptimizationRecommendations(allocations)...)

	// Based on insights
	recommendations = append(recommendations, ie.generateInsightBasedRecommendations(insights)...)

	return recommendations
}

// High cost analysis
func (ie *InsightEngine) analyzeHighCosts(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	totalCost := 0.0
	for _, alloc := range allocations {
		totalCost += alloc.TotalCost
	}

	// Find high-cost allocations (top 10% of total cost)
	threshold := totalCost * 0.1
	var highCostAllocations []string

	for _, alloc := range allocations {
		if alloc.TotalCost > threshold {
			highCostAllocations = append(highCostAllocations, alloc.Name)
		}
	}

	if len(highCostAllocations) > 0 {
		insight := CostInsight{
			Type:        "cost-concentration",
			Severity:    "high",
			Title:       "High Cost Concentration Detected",
			Description: fmt.Sprintf("Found %d allocations consuming >10%% of total cost ($%.2f)", len(highCostAllocations), threshold),
			Impact:      &totalCost,
			Confidence:  0.9,
			Actions: []string{
				"Review resource requests for high-cost allocations",
				"Consider workload optimization",
				"Evaluate if resources can be shared or consolidated",
			},
			Metadata: map[string]interface{}{
				"highCostAllocations": highCostAllocations,
				"threshold":           threshold,
				"totalCost":           totalCost,
			},
		}
		insights = append(insights, insight)
	}

	// Check for extremely high individual costs
	for _, alloc := range allocations {
		if alloc.TotalCost > ie.costThresholds.HighCostAlert {
			insight := CostInsight{
				Type:        "high-cost-alert",
				Severity:    "critical",
				Title:       fmt.Sprintf("High Cost Alert: %s", alloc.Name),
				Description: fmt.Sprintf("Allocation %s costs $%.2f, exceeding alert threshold", alloc.Name, alloc.TotalCost),
				Impact:      &alloc.TotalCost,
				Confidence:  1.0,
				Actions: []string{
					"Immediate review of resource allocation",
					"Check for runaway processes or resource leaks",
					"Consider implementing resource limits",
				},
				Metadata: map[string]interface{}{
					"allocation": alloc.Name,
					"cost":       alloc.TotalCost,
					"threshold":  ie.costThresholds.HighCostAlert,
				},
			}
			insights = append(insights, insight)
		}
	}

	return insights
}

// Efficiency analysis
func (ie *InsightEngine) analyzeEfficiency(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	var inefficientAllocations []string
	var lowCPUEfficiency []string
	var lowRAMEfficiency []string
	totalInefficient := 0.0

	for _, alloc := range allocations {
		if alloc.TotalEfficiency != nil && *alloc.TotalEfficiency < ie.costThresholds.EfficiencyTarget {
			inefficientAllocations = append(inefficientAllocations, alloc.Name)
			totalInefficient += alloc.TotalCost

			if alloc.CPUEfficiency != nil && *alloc.CPUEfficiency < ie.efficiencyTargets["cpu"] {
				lowCPUEfficiency = append(lowCPUEfficiency, alloc.Name)
			}
			if alloc.RAMEfficiency != nil && *alloc.RAMEfficiency < ie.efficiencyTargets["ram"] {
				lowRAMEfficiency = append(lowRAMEfficiency, alloc.Name)
			}
		}
	}

	if len(inefficientAllocations) > 0 {
		insight := CostInsight{
			Type:        "efficiency",
			Severity:    ie.determineSeverity(len(inefficientAllocations), len(allocations)),
			Title:       "Low Resource Efficiency Detected",
			Description: fmt.Sprintf("Found %d allocations with efficiency below %0.0f%% target", len(inefficientAllocations), ie.costThresholds.EfficiencyTarget*100),
			Impact:      &totalInefficient,
			Confidence:  0.85,
			Actions: []string{
				"Review and adjust resource requests",
				"Implement horizontal pod autoscaling",
				"Consider vertical pod autoscaling",
				"Optimize application resource usage",
			},
			Metadata: map[string]interface{}{
				"inefficientAllocations": inefficientAllocations,
				"lowCPUEfficiency":       lowCPUEfficiency,
				"lowRAMEfficiency":       lowRAMEfficiency,
				"potentialSavings":       totalInefficient * 0.3, // Estimated 30% savings
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Utilization pattern analysis
func (ie *InsightEngine) analyzeUtilizationPatterns(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	// Analyze cost distribution patterns
	if len(allocations) > 5 {
		// Sort by cost
		sortedAllocs := make([]EnhancedAllocation, len(allocations))
		copy(sortedAllocs, allocations)
		sort.Slice(sortedAllocs, func(i, j int) bool {
			return sortedAllocs[i].TotalCost > sortedAllocs[j].TotalCost
		})

		// Check for 80/20 rule (Pareto principle)
		totalCost := 0.0
		for _, alloc := range sortedAllocs {
			totalCost += alloc.TotalCost
		}

		top20PercentCount := int(math.Ceil(float64(len(sortedAllocs)) * 0.2))
		top20PercentCost := 0.0
		for i := 0; i < top20PercentCount && i < len(sortedAllocs); i++ {
			top20PercentCost += sortedAllocs[i].TotalCost
		}

		costRatio := top20PercentCost / totalCost
		if costRatio > 0.8 {
			insight := CostInsight{
				Type:        "pattern",
				Severity:    "medium",
				Title:       "Cost Concentration Pattern Detected",
				Description: fmt.Sprintf("Top 20%% of allocations account for %.1f%% of total cost", costRatio*100),
				Impact:      &top20PercentCost,
				Confidence:  0.9,
				Actions: []string{
					"Focus optimization efforts on top cost consumers",
					"Implement cost monitoring for high-impact services",
					"Consider workload consolidation opportunities",
				},
				Metadata: map[string]interface{}{
					"top20PercentCost": top20PercentCost,
					"costRatio":        costRatio,
					"topAllocations":   sortedAllocs[:top20PercentCount],
				},
			}
			insights = append(insights, insight)
		}
	}

	return insights
}

// Cost distribution analysis
func (ie *InsightEngine) analyzeCostDistribution(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	// Analyze resource type cost distribution
	resourceCosts := map[string]float64{
		"cpu":          0.0,
		"ram":          0.0,
		"gpu":          0.0,
		"network":      0.0,
		"storage":      0.0,
		"loadBalancer": 0.0,
		"shared":       0.0,
		"external":     0.0,
	}

	totalCost := 0.0
	for _, alloc := range allocations {
		resourceCosts["cpu"] += alloc.CPUCost
		resourceCosts["ram"] += alloc.RAMCost
		resourceCosts["gpu"] += alloc.GPUCost
		resourceCosts["network"] += alloc.NetworkCost
		resourceCosts["storage"] += alloc.PVCost
		resourceCosts["loadBalancer"] += alloc.LoadBalancerCost
		resourceCosts["shared"] += alloc.SharedCost
		resourceCosts["external"] += alloc.ExternalCost
		totalCost += alloc.TotalCost
	}

	// Find dominant cost type
	var dominantType string
	var dominantCost float64
	for resType, cost := range resourceCosts {
		if cost > dominantCost {
			dominantCost = cost
			dominantType = resType
		}
	}

	if dominantCost > totalCost*0.5 {
		insight := CostInsight{
			Type:        "cost-distribution",
			Severity:    "medium",
			Title:       fmt.Sprintf("%s Costs Dominate Allocation", strings.Title(dominantType)),
			Description: fmt.Sprintf("%s costs account for %.1f%% of total allocation costs", strings.Title(dominantType), (dominantCost/totalCost)*100),
			Impact:      &dominantCost,
			Confidence:  0.9,
			Actions:     ie.getResourceOptimizationActions(dominantType),
			Metadata: map[string]interface{}{
				"dominantType":    dominantType,
				"dominantCost":    dominantCost,
				"resourceCosts":   resourceCosts,
				"dominantPercent": (dominantCost / totalCost) * 100,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Idle resource analysis
func (ie *InsightEngine) analyzeIdleResources(allocations []EnhancedAllocation) []CostInsight {
	var insights []CostInsight

	var idleAllocations []string
	totalIdleCost := 0.0

	for _, alloc := range allocations {
		// Consider an allocation "idle" if both CPU and RAM efficiency are very low
		if alloc.CPUEfficiency != nil && alloc.RAMEfficiency != nil {
			if *alloc.CPUEfficiency < 0.1 && *alloc.RAMEfficiency < 0.1 {
				idleAllocations = append(idleAllocations, alloc.Name)
				totalIdleCost += alloc.TotalCost
			}
		}
	}

	if len(idleAllocations) > 0 {
		insight := CostInsight{
			Type:        "idle-resources",
			Severity:    "high",
			Title:       "Idle Resources Detected",
			Description: fmt.Sprintf("Found %d potentially idle allocations costing $%.2f", len(idleAllocations), totalIdleCost),
			Impact:      &totalIdleCost,
			Confidence:  0.8,
			Actions: []string{
				"Review idle allocations for termination",
				"Implement auto-scaling policies",
				"Set up idle resource monitoring and alerting",
				"Consider scheduled scaling for development environments",
			},
			Metadata: map[string]interface{}{
				"idleAllocations":  idleAllocations,
				"potentialSavings": totalIdleCost * 0.9, // 90% savings potential
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Asset utilization analysis
func (ie *InsightEngine) analyzeAssetUtilization(assets []EnhancedAsset) []CostInsight {
	var insights []CostInsight

	var underutilizedAssets []string
	totalUnderutilizedCost := 0.0

	for _, asset := range assets {
		if asset.UtilizationScore != nil && *asset.UtilizationScore < 0.3 {
			underutilizedAssets = append(underutilizedAssets, asset.Name)
			totalUnderutilizedCost += asset.TotalCost
		}
	}

	if len(underutilizedAssets) > 0 {
		insight := CostInsight{
			Type:        "asset-utilization",
			Severity:    "medium",
			Title:       "Underutilized Assets Detected",
			Description: fmt.Sprintf("Found %d assets with utilization below 30%%", len(underutilizedAssets)),
			Impact:      &totalUnderutilizedCost,
			Confidence:  0.75,
			Actions: []string{
				"Review asset sizing and requirements",
				"Consider consolidating workloads",
				"Implement auto-scaling for elastic workloads",
				"Schedule non-production workloads",
			},
			Metadata: map[string]interface{}{
				"underutilizedAssets": underutilizedAssets,
				"averageUtilization":  ie.calculateAverageUtilization(assets),
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Asset cost analysis
func (ie *InsightEngine) analyzeAssetCosts(assets []EnhancedAsset) []CostInsight {
	var insights []CostInsight

	// Group by asset type and analyze costs
	assetTypeCosts := make(map[string]float64)
	assetTypeCounts := make(map[string]int)

	for _, asset := range assets {
		assetTypeCosts[asset.Type] += asset.TotalCost
		assetTypeCounts[asset.Type]++
	}

	// Find most expensive asset type
	var expensiveType string
	var expensiveCost float64
	for assetType, cost := range assetTypeCosts {
		if cost > expensiveCost {
			expensiveCost = cost
			expensiveType = assetType
		}
	}

	totalCost := 0.0
	for _, cost := range assetTypeCosts {
		totalCost += cost
	}

	if expensiveCost > totalCost*0.6 {
		insight := CostInsight{
			Type:        "asset-cost-concentration",
			Severity:    "medium",
			Title:       fmt.Sprintf("%s Assets Drive Majority of Costs", expensiveType),
			Description: fmt.Sprintf("%s assets account for %.1f%% of total asset costs", expensiveType, (expensiveCost/totalCost)*100),
			Impact:      &expensiveCost,
			Confidence:  0.9,
			Actions: []string{
				fmt.Sprintf("Focus optimization efforts on %s assets", expensiveType),
				"Review sizing and utilization patterns",
				"Consider alternative instance types or configurations",
			},
			Metadata: map[string]interface{}{
				"assetTypeCosts":  assetTypeCosts,
				"assetTypeCounts": assetTypeCounts,
				"dominantType":    expensiveType,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Capacity analysis
func (ie *InsightEngine) analyzeCapacity(assets []EnhancedAsset) []CostInsight {
	var insights []CostInsight

	// This is a simplified capacity analysis
	// In a real implementation, this would use historical data and trends
	nodeAssets := 0
	for _, asset := range assets {
		if asset.Type == "Node" {
			nodeAssets++
		}
	}

	if nodeAssets > 0 {
		insight := CostInsight{
			Type:        "capacity",
			Severity:    "low",
			Title:       "Cluster Capacity Overview",
			Description: fmt.Sprintf("Cluster has %d nodes with current utilization patterns", nodeAssets),
			Confidence:  0.7,
			Actions: []string{
				"Monitor capacity trends over time",
				"Plan for future capacity needs",
				"Consider auto-scaling policies",
			},
			Metadata: map[string]interface{}{
				"nodeCount": nodeAssets,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Provider cost analysis
func (ie *InsightEngine) analyzeProviderCosts(cloudCosts []EnhancedCloudCost) []CostInsight {
	var insights []CostInsight

	providerCosts := make(map[string]float64)
	for _, cc := range cloudCosts {
		providerCosts[cc.Properties.Provider] += cc.AmortizedNetCost
	}

	// Find dominant provider
	var dominantProvider string
	var dominantCost float64
	totalCost := 0.0

	for provider, cost := range providerCosts {
		totalCost += cost
		if cost > dominantCost {
			dominantCost = cost
			dominantProvider = provider
		}
	}

	if dominantCost > totalCost*0.7 && len(providerCosts) > 1 {
		insight := CostInsight{
			Type:        "provider-concentration",
			Severity:    "medium",
			Title:       fmt.Sprintf("High Concentration in %s Provider", strings.ToUpper(dominantProvider)),
			Description: fmt.Sprintf("%s accounts for %.1f%% of cloud costs", strings.ToUpper(dominantProvider), (dominantCost/totalCost)*100),
			Impact:      &dominantCost,
			Confidence:  0.9,
			Actions: []string{
				"Consider multi-cloud strategy for cost optimization",
				"Evaluate reserved instances or committed use discounts",
				"Review regional pricing differences",
			},
			Metadata: map[string]interface{}{
				"providerCosts":    providerCosts,
				"dominantProvider": dominantProvider,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Service cost analysis
func (ie *InsightEngine) analyzeServiceCosts(cloudCosts []EnhancedCloudCost) []CostInsight {
	var insights []CostInsight

	serviceCosts := make(map[string]float64)
	for _, cc := range cloudCosts {
		serviceCosts[cc.Properties.Service] += cc.AmortizedNetCost
	}

	// Find top services by cost
	type serviceCost struct {
		service string
		cost    float64
	}

	var services []serviceCost
	totalCost := 0.0
	for service, cost := range serviceCosts {
		services = append(services, serviceCost{service, cost})
		totalCost += cost
	}

	sort.Slice(services, func(i, j int) bool {
		return services[i].cost > services[j].cost
	})

	if len(services) > 0 && services[0].cost > totalCost*0.4 {
		insight := CostInsight{
			Type:        "service-concentration",
			Severity:    "medium",
			Title:       fmt.Sprintf("High Costs in %s Service", services[0].service),
			Description: fmt.Sprintf("%s service accounts for %.1f%% of cloud costs", services[0].service, (services[0].cost/totalCost)*100),
			Impact:      &services[0].cost,
			Confidence:  0.9,
			Actions: []string{
				fmt.Sprintf("Deep dive into %s service usage and optimization", services[0].service),
				"Review service-specific cost optimization guides",
				"Consider alternative services or configurations",
			},
			Metadata: map[string]interface{}{
				"serviceCosts": serviceCosts,
				"topService":   services[0].service,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Regional cost analysis
func (ie *InsightEngine) analyzeRegionalCosts(cloudCosts []EnhancedCloudCost) []CostInsight {
	var insights []CostInsight

	regionCosts := make(map[string]float64)
	for _, cc := range cloudCosts {
		if cc.Properties.RegionID != "" {
			regionCosts[cc.Properties.RegionID] += cc.AmortizedNetCost
		}
	}

	if len(regionCosts) > 1 {
		// Find most expensive region
		var expensiveRegion string
		var expensiveCost float64
		totalCost := 0.0

		for region, cost := range regionCosts {
			totalCost += cost
			if cost > expensiveCost {
				expensiveCost = cost
				expensiveRegion = region
			}
		}

		insight := CostInsight{
			Type:        "regional-distribution",
			Severity:    "low",
			Title:       "Multi-Region Cost Distribution",
			Description: fmt.Sprintf("Costs distributed across %d regions, with %s being most expensive", len(regionCosts), expensiveRegion),
			Impact:      &totalCost,
			Confidence:  0.8,
			Actions: []string{
				"Review regional pricing differences",
				"Consider workload placement optimization",
				"Evaluate data transfer costs between regions",
			},
			Metadata: map[string]interface{}{
				"regionCosts":     regionCosts,
				"expensiveRegion": expensiveRegion,
			},
		}
		insights = append(insights, insight)
	}

	return insights
}

// Recommendation generators

func (ie *InsightEngine) generateRightsizingRecommendations(allocations []EnhancedAllocation) []Recommendation {
	var recommendations []Recommendation

	for _, alloc := range allocations {
		if alloc.CPUEfficiency != nil && alloc.RAMEfficiency != nil {
			if *alloc.CPUEfficiency < 0.5 || *alloc.RAMEfficiency < 0.5 {
				potentialSavings := alloc.TotalCost * 0.3 // Estimated 30% savings

				recommendation := Recommendation{
					ID:               fmt.Sprintf("rightsize_%s_%d", alloc.Name, time.Now().Unix()),
					Type:             "rightsizing",
					Title:            fmt.Sprintf("Rightsize %s", alloc.Name),
					Description:      fmt.Sprintf("Reduce resource requests for %s based on low utilization", alloc.Name),
					Effort:           "low",
					Impact:           "medium",
					PotentialSavings: &potentialSavings,
					Implementation: []string{
						"Reduce CPU requests by 30-50%",
						"Reduce memory requests by 20-40%",
						"Monitor performance after changes",
						"Implement gradual rollout",
					},
					RiskLevel: "low",
					Metadata: map[string]interface{}{
						"allocation":    alloc.Name,
						"cpuEfficiency": alloc.CPUEfficiency,
						"ramEfficiency": alloc.RAMEfficiency,
						"currentCost":   alloc.TotalCost,
					},
				}
				recommendations = append(recommendations, recommendation)
			}
		}
	}

	return recommendations
}

func (ie *InsightEngine) generateEfficiencyRecommendations(allocations []EnhancedAllocation) []Recommendation {
	var recommendations []Recommendation

	// Find allocations with consistently low efficiency
	var inefficientAllocations []EnhancedAllocation
	for _, alloc := range allocations {
		if alloc.TotalEfficiency != nil && *alloc.TotalEfficiency < 0.6 {
			inefficientAllocations = append(inefficientAllocations, alloc)
		}
	}

	if len(inefficientAllocations) > 3 {
		totalSavings := 0.0
		for _, alloc := range inefficientAllocations {
			totalSavings += alloc.TotalCost * 0.25
		}

		recommendation := Recommendation{
			ID:               fmt.Sprintf("efficiency_improvement_%d", time.Now().Unix()),
			Type:             "efficiency",
			Title:            "Implement Resource Efficiency Program",
			Description:      fmt.Sprintf("Systematic approach to improve efficiency across %d underperforming allocations", len(inefficientAllocations)),
			Effort:           "high",
			Impact:           "high",
			PotentialSavings: &totalSavings,
			Implementation: []string{
				"Implement Horizontal Pod Autoscaler (HPA)",
				"Deploy Vertical Pod Autoscaler (VPA)",
				"Set up resource quotas and limits",
				"Implement pod disruption budgets",
				"Regular efficiency monitoring and reporting",
			},
			RiskLevel: "medium",
			Metadata: map[string]interface{}{
				"inefficientCount": len(inefficientAllocations),
				"allocations":      inefficientAllocations,
			},
		}
		recommendations = append(recommendations, recommendation)
	}

	return recommendations
}

func (ie *InsightEngine) generateCostOptimizationRecommendations(allocations []EnhancedAllocation) []Recommendation {
	var recommendations []Recommendation

	// Sort by cost to focus on high-impact optimizations
	sortedAllocs := make([]EnhancedAllocation, len(allocations))
	copy(sortedAllocs, allocations)
	sort.Slice(sortedAllocs, func(i, j int) bool {
		return sortedAllocs[i].TotalCost > sortedAllocs[j].TotalCost
	})

	// Recommend optimization for top 20% of costs
	topCount := int(math.Ceil(float64(len(sortedAllocs)) * 0.2))
	if topCount > 10 {
		topCount = 10 // Limit to top 10
	}

	if topCount > 0 {
		totalTopCost := 0.0
		for i := 0; i < topCount; i++ {
			totalTopCost += sortedAllocs[i].TotalCost
		}

		potentialSavings := totalTopCost * 0.2

		recommendation := Recommendation{
			ID:               fmt.Sprintf("focus_optimization_%d", time.Now().Unix()),
			Type:             "optimization",
			Title:            "Focus on High-Impact Cost Optimization",
			Description:      fmt.Sprintf("Prioritize optimization efforts on top %d cost consumers", topCount),
			Effort:           "medium",
			Impact:           "high",
			PotentialSavings: &potentialSavings,
			Implementation: []string{
				"Conduct detailed analysis of top cost consumers",
				"Implement monitoring and alerting for high-cost resources",
				"Set up cost budgets and alerts",
				"Regular optimization reviews",
			},
			RiskLevel: "low",
			Metadata: map[string]interface{}{
				"topAllocations": sortedAllocs[:topCount],
				"totalTopCost":   totalTopCost,
			},
		}
		recommendations = append(recommendations, recommendation)
	}

	return recommendations
}

func (ie *InsightEngine) generateInsightBasedRecommendations(insights []CostInsight) []Recommendation {
	var recommendations []Recommendation

	for _, insight := range insights {
		switch insight.Type {
		case "idle-resources":
			if insight.Impact != nil {
				recommendation := Recommendation{
					ID:               fmt.Sprintf("idle_cleanup_%d", time.Now().Unix()),
					Type:             "termination",
					Title:            "Clean Up Idle Resources",
					Description:      "Remove or scale down identified idle resources",
					Effort:           "low",
					Impact:           "high",
					PotentialSavings: insight.Impact,
					Implementation: []string{
						"Review idle resource list",
						"Verify resources are truly unused",
						"Implement graceful shutdown procedures",
						"Set up monitoring to prevent future idle resources",
					},
					RiskLevel: "medium",
					Metadata:  insight.Metadata,
				}
				recommendations = append(recommendations, recommendation)
			}

		case "efficiency":
			if insight.Impact != nil {
				savings := *insight.Impact * 0.25
				recommendation := Recommendation{
					ID:               fmt.Sprintf("efficiency_fix_%d", time.Now().Unix()),
					Type:             "rightsizing",
					Title:            "Address Efficiency Issues",
					Description:      "Implement fixes for identified efficiency problems",
					Effort:           "medium",
					Impact:           "high",
					PotentialSavings: &savings,
					Implementation:   insight.Actions,
					RiskLevel:        "low",
					Metadata:         insight.Metadata,
				}
				recommendations = append(recommendations, recommendation)
			}
		}
	}

	return recommendations
}

// Helper methods

func (ie *InsightEngine) determineSeverity(affected, total int) string {
	ratio := float64(affected) / float64(total)
	if ratio > 0.5 {
		return "high"
	} else if ratio > 0.2 {
		return "medium"
	}
	return "low"
}

func (ie *InsightEngine) getResourceOptimizationActions(resourceType string) []string {
	switch resourceType {
	case "cpu":
		return []string{
			"Review CPU requests and limits",
			"Implement CPU-based autoscaling",
			"Optimize application CPU usage",
			"Consider CPU limit tuning",
		}
	case "ram":
		return []string{
			"Review memory requests and limits",
			"Implement memory-based autoscaling",
			"Optimize application memory usage",
			"Check for memory leaks",
		}
	case "gpu":
		return []string{
			"Review GPU utilization patterns",
			"Consider GPU sharing or fractional GPUs",
			"Optimize GPU workload scheduling",
			"Evaluate GPU instance types",
		}
	case "network":
		return []string{
			"Analyze network traffic patterns",
			"Optimize data transfer and caching",
			"Review network service configurations",
			"Consider regional data placement",
		}
	case "storage":
		return []string{
			"Review storage usage and types",
			"Implement storage lifecycle policies",
			"Optimize storage class selections",
			"Consider storage compression and deduplication",
		}
	default:
		return []string{
			"Review resource configuration",
			"Optimize resource utilization",
			"Consider alternative resource types",
		}
	}
}

func (ie *InsightEngine) calculateAverageUtilization(assets []EnhancedAsset) float64 {
	total := 0.0
	count := 0
	for _, asset := range assets {
		if asset.UtilizationScore != nil {
			total += *asset.UtilizationScore
			count++
		}
	}
	if count == 0 {
		return 0.0
	}
	return total / float64(count)
}
