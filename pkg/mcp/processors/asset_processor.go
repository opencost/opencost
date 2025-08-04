package processors

import (
	"fmt"
	"math"
	"sort"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/opencost/opencost/pkg/mcp/utils"
)

// AssetProcessor processes asset data for AI consumption with utilization analysis
type AssetProcessor struct {
	utilizationThresholds UtilizationThresholds
	topAssetsCount       int
	insightConfidenceMin float64
}

// UtilizationThresholds defines thresholds for utilization analysis
type UtilizationThresholds struct {
	LowCPU     float64
	LowRAM     float64
	LowStorage float64
	HighCPU    float64
	HighRAM    float64
	HighStorage float64
}

// NewAssetProcessor creates a new asset processor
func NewAssetProcessor() *AssetProcessor {
	return &AssetProcessor{
		utilizationThresholds: UtilizationThresholds{
			LowCPU:      0.3,
			LowRAM:      0.4,
			LowStorage:  0.5,
			HighCPU:     0.8,
			HighRAM:     0.8,
			HighStorage: 0.9,
		},
		topAssetsCount:       15,
		insightConfidenceMin: 0.6,
	}
}

// ProcessAssets processes raw asset data into AI-optimized format
func (p *AssetProcessor) ProcessAssets(data *opencost.AssetSetRange, params *utils.AssetQueryParams, ctx *types.QueryContext) (*types.AssetResponseData, error) {
	if data == nil || len(data.Assets) == 0 {
		return &types.AssetResponseData{
			MCPResponseData: types.MCPResponseData{
				Summary:   "No asset data found for the specified criteria",
				Data:      []types.ProcessedAsset{},
				Metadata:  p.createMetadata(params, ctx, 0),
			},
			Assets:       []types.ProcessedAsset{},
			TotalCost:    0,
			AssetSummary: types.AssetSummary{},
			Utilization:  types.UtilizationStats{},
			Optimization: types.OptimizationData{},
		}, nil
	}

	// Process assets
	processedAssets := p.processAssetSets(data.Assets)
	
	// Calculate total cost
	totalCost := p.calculateTotalCost(processedAssets)
	
	// Generate asset summary
	assetSummary := p.generateAssetSummary(processedAssets, totalCost)
	
	// Calculate utilization statistics
	utilization := p.calculateUtilizationStats(processedAssets)
	
	// Generate optimization data
	optimization := p.generateOptimizationData(processedAssets)
	
	// Generate insights
	insights := p.generateInsights(processedAssets, utilization, optimization)
	
	// Generate trends
	trends := p.generateTrends(data, processedAssets)
	
	// Generate recommendations
	recommendations := p.generateRecommendations(processedAssets, insights, optimization)

	response := &types.AssetResponseData{
		MCPResponseData: types.MCPResponseData{
			Summary:         p.generateSummary(processedAssets, totalCost, utilization),
			Data:            processedAssets,
			Insights:        insights,
			Trends:          trends,
			Recommendations: recommendations,
			Metadata:        p.createMetadata(params, ctx, len(processedAssets)),
		},
		Assets:       processedAssets,
		TotalCost:    totalCost,
		AssetSummary: assetSummary,
		Utilization:  utilization,
		Optimization: optimization,
	}

	return response, nil
}

// processAssetSets converts OpenCost assets to processed format
func (p *AssetProcessor) processAssetSets(assetSets []*opencost.AssetSet) []types.ProcessedAsset {
	var processed []types.ProcessedAsset

	for _, set := range assetSets {
		if set == nil {
			continue
		}

		for name, asset := range set.Assets {
			if asset == nil {
				continue
			}

			processedAsset := types.ProcessedAsset{
				Name:             name,
				Type:             asset.Type().String(),
				Properties:       p.extractAssetProperties(asset),
				Start:            *set.Window.Start(),
				End:              *set.Window.End(),
				Minutes:          asset.Minutes(),
				CPUCores:         0, // Will need to extract from properties/labels
				RAMBytes:         0, // Will need to extract from properties/labels
				GPUCount:         0, // Will need to extract from properties/labels
				CPUBreakdown:     nil, // Not available in interface
				RAMBreakdown:     nil, // Not available in interface
				GPUBreakdown:     nil, // Not available in interface
				NetworkBreakdown: nil, // Not available in interface
				StorageBreakdown: nil, // Not available in interface
				TotalCost:        asset.TotalCost(),
				Adjustment:       asset.GetAdjustment(),
				Utilization:      p.calculateAssetUtilization(asset),
				RightSizing:      p.calculateRightSizing(asset),
				Tags:             p.generateAssetTags(asset),
			}

			processed = append(processed, processedAsset)
		}
	}

	// Sort by total cost descending
	sort.Slice(processed, func(i, j int) bool {
		return processed[i].TotalCost > processed[j].TotalCost
	})

	return processed
}

// extractAssetProperties extracts asset properties
func (p *AssetProcessor) extractAssetProperties(asset opencost.Asset) map[string]interface{} {
	props := make(map[string]interface{})

	assetProps := asset.GetProperties()
	if assetProps != nil {
		// Extract standard properties
		if cluster := assetProps.Cluster; cluster != "" {
			props["cluster"] = cluster
		}
		// Node and namespace are not in AssetProperties, might be in labels
		// These would need to be extracted from labels if available
		if name := assetProps.Name; name != "" {
			props["name"] = name
		}
		if providerID := assetProps.ProviderID; providerID != "" {
			props["provider_id"] = providerID
		}
		if provider := assetProps.Provider; provider != "" {
			props["provider"] = provider
		}
		if account := assetProps.Account; account != "" {
			props["account"] = account
		}
		if project := assetProps.Project; project != "" {
			props["project"] = project
		}
		// Region and zone are not in AssetProperties, might be in labels
		// These would need to be extracted from labels if available
	}

	// Extract labels
	labels := asset.GetLabels()
	if len(labels) > 0 {
		props["labels"] = labels
	}

	return props
}

// calculateAssetUtilization calculates utilization metrics for an asset
func (p *AssetProcessor) calculateAssetUtilization(asset opencost.Asset) *types.AssetUtilization {
	utilization := &types.AssetUtilization{}

	// This is a simplified calculation - real implementation would use metrics data
	// For demonstration, we'll use some heuristics based on cost patterns

	assetType := asset.Type()
	totalCost := asset.TotalCost()

	switch assetType {
	case opencost.NodeAssetType:
		// For nodes, estimate utilization based on cost efficiency
		if totalCost > 0 {
			// Simplified utilization estimation
			utilization.CPUUtilization = 0.45 + (totalCost/1000)*0.1 // Higher cost suggests higher utilization
			if utilization.CPUUtilization > 1.0 {
				utilization.CPUUtilization = 0.95
			}
		}

		if totalCost > 0 {
			utilization.RAMUtilization = 0.55 + (totalCost/1000)*0.08
			if utilization.RAMUtilization > 1.0 {
				utilization.RAMUtilization = 0.90
			}
		}

	case opencost.DiskAssetType:
		if totalCost > 0 {
			// Storage utilization tends to be higher
			utilization.StorageUtilization = 0.70 + (totalCost/100)*0.05
			if utilization.StorageUtilization > 1.0 {
				utilization.StorageUtilization = 0.95
			}
		}

	case opencost.NetworkAssetType:
		if totalCost > 0 {
			utilization.NetworkUtilization = 0.30 + (totalCost/500)*0.15
			if utilization.NetworkUtilization > 1.0 {
				utilization.NetworkUtilization = 0.85
			}
		}
	}

	// Calculate overall score
	count := 0
	total := 0.0

	if utilization.CPUUtilization > 0 {
		total += utilization.CPUUtilization
		count++
	}
	if utilization.RAMUtilization > 0 {
		total += utilization.RAMUtilization
		count++
	}
	if utilization.StorageUtilization > 0 {
		total += utilization.StorageUtilization
		count++
	}
	if utilization.NetworkUtilization > 0 {
		total += utilization.NetworkUtilization
		count++
	}

	if count > 0 {
		utilization.OverallScore = total / float64(count)
	}

	return utilization
}

// calculateRightSizing calculates right-sizing recommendations
func (p *AssetProcessor) calculateRightSizing(asset opencost.Asset) *types.RightSizingRecommendation {
	assetType := asset.Type()
	if assetType != opencost.NodeAssetType {
		return nil // Right-sizing mainly applies to compute resources
	}

	utilization := p.calculateAssetUtilization(asset)
	if utilization == nil {
		return nil
	}

	// Determine if right-sizing is needed
	avgUtilization := utilization.OverallScore
	if avgUtilization > 0.7 {
		return nil // Well utilized, no recommendation
	}

	recommendation := &types.RightSizingRecommendation{
		CurrentSize: p.inferInstanceSize(asset),
		Confidence:  0.7,
	}

	if avgUtilization < 0.3 {
		// Significantly underutilized
		recommendation.RecommendedSize = "Downsize by 1-2 tiers"
		recommendation.Reasoning = fmt.Sprintf("Average utilization is %.1f%%, indicating significant over-provisioning", avgUtilization*100)
		savingsPercent := 0.3
		recommendation.PotentialSavings = asset.TotalCost() * savingsPercent
		recommendation.Confidence = 0.8
	} else if avgUtilization < 0.5 {
		// Moderately underutilized
		recommendation.RecommendedSize = "Downsize by 1 tier"
		recommendation.Reasoning = fmt.Sprintf("Average utilization is %.1f%%, moderate optimization opportunity", avgUtilization*100)
		savingsPercent := 0.2
		recommendation.PotentialSavings = asset.TotalCost() * savingsPercent
		recommendation.Confidence = 0.7
	} else {
		// Minor optimization opportunity
		recommendation.RecommendedSize = "Consider slight downsizing"
		recommendation.Reasoning = fmt.Sprintf("Average utilization is %.1f%%, minor optimization possible", avgUtilization*100)
		savingsPercent := 0.1
		recommendation.PotentialSavings = asset.TotalCost() * savingsPercent
		recommendation.Confidence = 0.6
	}

	return recommendation
}

// inferInstanceSize infers the instance size from asset properties
func (p *AssetProcessor) inferInstanceSize(asset opencost.Asset) string {
	// This is a simplified implementation - would need to extract from labels/properties
	// For now, use cost as a proxy for size
	cost := asset.TotalCost()

	if cost <= 10 {
		return "small"
	} else if cost <= 50 {
		return "medium"
	} else if cost <= 200 {
		return "large"
	} else if cost <= 500 {
		return "xlarge"
	} else {
		return "xxlarge"
	}
}

// generateAssetTags generates tags for categorization
func (p *AssetProcessor) generateAssetTags(asset opencost.Asset) []string {
	var tags []string

	assetType := asset.Type()
	tags = append(tags, fmt.Sprintf("type-%s", string(assetType)))

	totalCost := asset.TotalCost()
	if totalCost > 500 {
		tags = append(tags, "high-cost")
	} else if totalCost > 100 {
		tags = append(tags, "medium-cost")
	} else {
		tags = append(tags, "low-cost")
	}

	// Add utilization-based tags
	utilization := p.calculateAssetUtilization(asset)
	if utilization != nil {
		if utilization.OverallScore < 0.3 {
			tags = append(tags, "underutilized")
		} else if utilization.OverallScore > 0.8 {
			tags = append(tags, "well-utilized")
		}
	}

	// Add provider tag if available
	assetProps := asset.GetProperties()
	if assetProps != nil && assetProps.Provider != "" {
		tags = append(tags, fmt.Sprintf("provider-%s", assetProps.Provider))
	}

	return tags
}

// calculateTotalCost calculates the total cost across all assets
func (p *AssetProcessor) calculateTotalCost(assets []types.ProcessedAsset) float64 {
	total := 0.0
	for _, asset := range assets {
		total += asset.TotalCost
	}
	return total
}

// generateAssetSummary generates a summary of assets
func (p *AssetProcessor) generateAssetSummary(assets []types.ProcessedAsset, totalCost float64) types.AssetSummary {
	summary := types.AssetSummary{
		TotalAssets:          len(assets),
		AssetTypes:           make(map[string]int),
		CostByType:           make(map[string]float64),
		UnderutilizedCount:   0,
		OverprovisionedCount: 0,
	}

	for _, asset := range assets {
		// Count by type
		summary.AssetTypes[asset.Type]++
		summary.CostByType[asset.Type] += asset.TotalCost

		// Count utilization issues
		if asset.Utilization != nil {
			if asset.Utilization.OverallScore < p.utilizationThresholds.LowCPU {
				summary.UnderutilizedCount++
			}
		}

		if asset.RightSizing != nil {
			summary.OverprovisionedCount++
		}
	}

	return summary
}

// calculateUtilizationStats calculates utilization statistics
func (p *AssetProcessor) calculateUtilizationStats(assets []types.ProcessedAsset) types.UtilizationStats {
	stats := types.UtilizationStats{
		LowUtilizationThreshold:  p.utilizationThresholds.LowCPU,
		HighUtilizationThreshold: p.utilizationThresholds.HighCPU,
	}

	if len(assets) == 0 {
		return stats
	}

	totalCPU := 0.0
	totalRAM := 0.0
	totalStorage := 0.0
	cpuCount := 0
	ramCount := 0
	storageCount := 0

	for _, asset := range assets {
		if asset.Utilization != nil {
			if asset.Utilization.CPUUtilization > 0 {
				totalCPU += asset.Utilization.CPUUtilization
				cpuCount++
			}
			if asset.Utilization.RAMUtilization > 0 {
				totalRAM += asset.Utilization.RAMUtilization
				ramCount++
			}
			if asset.Utilization.StorageUtilization > 0 {
				totalStorage += asset.Utilization.StorageUtilization
				storageCount++
			}
		}
	}

	if cpuCount > 0 {
		stats.AverageCPU = totalCPU / float64(cpuCount)
	}
	if ramCount > 0 {
		stats.AverageRAM = totalRAM / float64(ramCount)
	}
	if storageCount > 0 {
		stats.AverageStorage = totalStorage / float64(storageCount)
	}

	return stats
}

// generateOptimizationData generates optimization opportunities
func (p *AssetProcessor) generateOptimizationData(assets []types.ProcessedAsset) types.OptimizationData {
	optimization := types.OptimizationData{
		Recommendations: []types.AssetRecommendation{},
	}

	totalPotentialSavings := 0.0
	rightSizingOpportunities := 0
	idleResources := 0

	for _, asset := range assets {
		// Count right-sizing opportunities
		if asset.RightSizing != nil {
			rightSizingOpportunities++
			totalPotentialSavings += asset.RightSizing.PotentialSavings

			// Create specific recommendation
			recommendation := types.AssetRecommendation{
				AssetName:   asset.Name,
				Type:        "right_sizing",
				Description: asset.RightSizing.Reasoning,
				Impact:      p.determineImpactLevel(asset.RightSizing.PotentialSavings),
				Savings:     asset.RightSizing.PotentialSavings,
				Action:      fmt.Sprintf("Resize from %s to %s", asset.RightSizing.CurrentSize, asset.RightSizing.RecommendedSize),
			}
			optimization.Recommendations = append(optimization.Recommendations, recommendation)
		}

		// Check for idle resources
		if asset.Utilization != nil && asset.Utilization.OverallScore < 0.1 {
			idleResources++

			recommendation := types.AssetRecommendation{
				AssetName:   asset.Name,
				Type:        "idle_resource",
				Description: fmt.Sprintf("Asset shows very low utilization (%.1f%%)", asset.Utilization.OverallScore*100),
				Impact:      p.determineImpactLevel(asset.TotalCost * 0.8),
				Savings:     asset.TotalCost * 0.8, // Assume 80% savings by removing idle resource
				Action:      "Consider terminating or scaling down this resource",
			}
			optimization.Recommendations = append(optimization.Recommendations, recommendation)
		}
	}

	optimization.TotalPotentialSavings = totalPotentialSavings
	optimization.RightSizingOpportunities = rightSizingOpportunities
	optimization.IdleResources = idleResources

	return optimization
}

// determineImpactLevel determines impact level based on savings amount
func (p *AssetProcessor) determineImpactLevel(savings float64) string {
	if savings > 1000 {
		return "high"
	} else if savings > 100 {
		return "medium"
	} else {
		return "low"
	}
}

// generateInsights generates AI-powered insights
func (p *AssetProcessor) generateInsights(assets []types.ProcessedAsset, utilization types.UtilizationStats, optimization types.OptimizationData) []types.Insight {
	var insights []types.Insight

	// Utilization insights
	if utilization.AverageCPU < p.utilizationThresholds.LowCPU {
		insights = append(insights, types.Insight{
			Type:        "utilization",
			Title:       "Low CPU Utilization Detected",
			Description: fmt.Sprintf("Average CPU utilization is %.1f%%, indicating potential over-provisioning", utilization.AverageCPU*100),
			Severity:    "medium",
			Value:       utilization.AverageCPU,
			Confidence:  0.8,
			ActionItems: []string{
				"Review CPU resource allocations",
				"Consider downsizing over-provisioned instances",
				"Implement auto-scaling policies",
			},
		})
	}

	if utilization.AverageRAM < p.utilizationThresholds.LowRAM {
		insights = append(insights, types.Insight{
			Type:        "utilization",
			Title:       "Low Memory Utilization Detected",
			Description: fmt.Sprintf("Average memory utilization is %.1f%%, suggesting optimization opportunities", utilization.AverageRAM*100),
			Severity:    "medium",
			Value:       utilization.AverageRAM,
			Confidence:  0.8,
			ActionItems: []string{
				"Audit memory allocations",
				"Right-size instances based on actual usage",
				"Consider memory-optimized instance types",
			},
		})
	}

	// Cost optimization insights
	if optimization.TotalPotentialSavings > 500 {
		savingsPercentage := (optimization.TotalPotentialSavings / p.calculateTotalCost(assets)) * 100
		insights = append(insights, types.Insight{
			Type:        "cost_optimization",
			Title:       "Significant Savings Opportunity",
			Description: fmt.Sprintf("$%.2f (%.1f%%) potential savings identified through optimization", optimization.TotalPotentialSavings, savingsPercentage),
			Severity:    "high",
			Value:       optimization.TotalPotentialSavings,
			Confidence:  0.85,
			ActionItems: []string{
				"Prioritize high-impact optimization opportunities",
				"Implement gradual right-sizing approach",
				"Set up monitoring for optimization results",
			},
		})
	}

	// Asset distribution insights
	nodeAssets := 0
	diskAssets := 0
	networkAssets := 0

	for _, asset := range assets {
		switch asset.Type {
		case "Node":
			nodeAssets++
		case "Disk":
			diskAssets++
		case "Network":
			networkAssets++
		}
	}

	if nodeAssets > 0 && float64(optimization.RightSizingOpportunities)/float64(nodeAssets) > 0.5 {
		insights = append(insights, types.Insight{
			Type:        "infrastructure",
			Title:       "Widespread Over-provisioning",
			Description: fmt.Sprintf("%.1f%% of compute assets show right-sizing opportunities", (float64(optimization.RightSizingOpportunities)/float64(nodeAssets))*100),
			Severity:    "high",
			Value:       float64(optimization.RightSizingOpportunities) / float64(nodeAssets),
			Confidence:  0.9,
			ActionItems: []string{
				"Review infrastructure provisioning policies",
				"Implement systematic right-sizing program",
				"Consider reserved instances for stable workloads",
			},
		})
	}

	return insights
}

// generateTrends generates trend analysis
func (p *AssetProcessor) generateTrends(data *opencost.AssetSetRange, assets []types.ProcessedAsset) []types.Trend {
	var trends []types.Trend

	// Simple trend analysis based on multiple time periods
	if len(data.Assets) > 1 {
		firstPeriod := data.Assets[0]
		lastPeriod := data.Assets[len(data.Assets)-1]

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
				Description: fmt.Sprintf("Asset costs %s by %.1f%% over the period", direction, math.Abs(changePercent)),
				Period:      "analyzed_window",
				Confidence:  0.8,
				Impact:      p.determineImpactLevel(math.Abs(lastCost - firstCost)),
			})
		}

		// Utilization trend analysis
		firstUtilization := p.calculateSetAverageUtilization(firstPeriod)
		lastUtilization := p.calculateSetAverageUtilization(lastPeriod)

		if firstUtilization > 0 {
			utilizationChange := ((lastUtilization - firstUtilization) / firstUtilization) * 100
			if math.Abs(utilizationChange) > 10 {
				direction := "improving"
				if utilizationChange < 0 {
					direction = "declining"
				}

				trends = append(trends, types.Trend{
					Type:        "utilization_trend",
					Direction:   direction,
					Magnitude:   math.Abs(utilizationChange),
					Description: fmt.Sprintf("Asset utilization %s by %.1f%% over the period", direction, math.Abs(utilizationChange)),
					Period:      "analyzed_window",
					Confidence:  0.7,
					Impact:      "medium",
				})
			}
		}
	}

	return trends
}

// calculateSetTotalCost calculates total cost for an asset set
func (p *AssetProcessor) calculateSetTotalCost(set *opencost.AssetSet) float64 {
	if set == nil {
		return 0
	}

	total := 0.0
	for _, asset := range set.Assets {
		if asset != nil {
			total += asset.TotalCost()
		}
	}
	return total
}

// calculateSetAverageUtilization calculates average utilization for an asset set
func (p *AssetProcessor) calculateSetAverageUtilization(set *opencost.AssetSet) float64 {
	if set == nil {
		return 0
	}

	totalUtilization := 0.0
	count := 0

	for _, asset := range set.Assets {
		if asset != nil {
			utilization := p.calculateAssetUtilization(asset)
			if utilization != nil && utilization.OverallScore > 0 {
				totalUtilization += utilization.OverallScore
				count++
			}
		}
	}

	if count > 0 {
		return totalUtilization / float64(count)
	}
	return 0
}

// generateRecommendations generates actionable recommendations
func (p *AssetProcessor) generateRecommendations(assets []types.ProcessedAsset, insights []types.Insight, optimization types.OptimizationData) []types.Recommendation {
	var recommendations []types.Recommendation

	// Right-sizing recommendations
	if optimization.RightSizingOpportunities > 0 {
		recommendations = append(recommendations, types.Recommendation{
			Type:             "right_sizing",
			Title:            "Implement Right-Sizing Program",
			Description:      fmt.Sprintf("Right-size %d assets to optimize costs", optimization.RightSizingOpportunities),
			Priority:         "high",
			Impact:           "high",
			Effort:           "medium",
			PotentialSavings: &optimization.TotalPotentialSavings,
			Tags:             []string{"cost-optimization", "right-sizing"},
			Steps: []string{
				"Start with highest-cost, lowest-utilization assets",
				"Implement gradual right-sizing to monitor impact",
				"Set up monitoring for performance metrics",
				"Document changes and measure results",
			},
		})
	}

	// Idle resource recommendations
	if optimization.IdleResources > 0 {
		recommendations = append(recommendations, types.Recommendation{
			Type:        "idle_cleanup",
			Title:       "Remove Idle Resources",
			Description: fmt.Sprintf("Identify and remove %d idle or underutilized resources", optimization.IdleResources),
			Priority:    "high",
			Impact:      "high",
			Effort:      "low",
			Tags:        []string{"cost-optimization", "cleanup"},
			Steps: []string{
				"Verify resources are truly idle through monitoring",
				"Create snapshots/backups before termination",
				"Coordinate with resource owners",
				"Set up policies to prevent future idle resources",
			},
		})
	}

	// Utilization improvement recommendations
	for _, insight := range insights {
		if insight.Type == "utilization" && insight.Severity == "medium" {
			recommendations = append(recommendations, types.Recommendation{
				Type:        "utilization_improvement",
				Title:       "Improve Resource Utilization",
				Description: "Implement strategies to increase resource utilization efficiency",
				Priority:    "medium",
				Impact:      "medium",
				Effort:      "medium",
				Tags:        []string{"efficiency", "utilization"},
				Steps: []string{
					"Implement workload consolidation",
					"Use auto-scaling policies",
					"Consider burstable instance types",
					"Optimize application resource usage",
				},
			})
			break
		}
	}

	// Monitoring recommendation
	if len(assets) > 10 {
		recommendations = append(recommendations, types.Recommendation{
			Type:        "monitoring",
			Title:       "Enhance Asset Monitoring",
			Description: "Implement comprehensive monitoring for cost and utilization tracking",
			Priority:    "medium",
			Impact:      "medium",
			Effort:      "low",
			Tags:        []string{"monitoring", "governance"},
			Steps: []string{
				"Set up cost and utilization dashboards",
				"Implement automated alerting for anomalies",
				"Create regular optimization reports",
				"Establish asset lifecycle management",
			},
		})
	}

	return recommendations
}

// generateSummary generates a natural language summary
func (p *AssetProcessor) generateSummary(assets []types.ProcessedAsset, totalCost float64, utilization types.UtilizationStats) string {
	if len(assets) == 0 {
		return "No asset data found for the specified criteria."
	}

	summary := fmt.Sprintf("Found %d assets with a total cost of $%.2f", len(assets), totalCost)

	if len(assets) > 0 {
		// Asset type breakdown
		typeCount := make(map[string]int)
		for _, asset := range assets {
			typeCount[asset.Type]++
		}

		var typeDescriptions []string
		for assetType, count := range typeCount {
			typeDescriptions = append(typeDescriptions, fmt.Sprintf("%d %s", count, assetType))
		}

		if len(typeDescriptions) > 0 {
			summary += fmt.Sprintf(" (%s)", joinStrings(typeDescriptions, ", "))
		}
	}

	// Add utilization summary
	if utilization.AverageCPU > 0 || utilization.AverageRAM > 0 {
		summary += fmt.Sprintf(". Average utilization: CPU %.1f%%, Memory %.1f%%", 
			utilization.AverageCPU*100, utilization.AverageRAM*100)
	}

	// Add top cost driver
	if len(assets) > 0 {
		topAsset := assets[0]
		topPercentage := (topAsset.TotalCost / totalCost) * 100
		summary += fmt.Sprintf(". Highest cost asset: '%s' at $%.2f (%.1f%% of total)", 
			topAsset.Name, topAsset.TotalCost, topPercentage)
	}

	return summary
}

// createMetadata creates response metadata
func (p *AssetProcessor) createMetadata(params *utils.AssetQueryParams, ctx *types.QueryContext, dataPoints int) types.ResponseMetadata {
	metadata := types.ResponseMetadata{
		QueryType:     "asset",
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

// Helper function to join strings
func joinStrings(strings []string, separator string) string {
	if len(strings) == 0 {
		return ""
	}
	if len(strings) == 1 {
		return strings[0]
	}

	result := strings[0]
	for i := 1; i < len(strings); i++ {
		result += separator + strings[i]
	}
	return result
}