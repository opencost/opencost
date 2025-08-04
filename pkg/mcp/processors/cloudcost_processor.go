package processors

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/opencost/opencost/pkg/mcp/utils"
)

// CloudCostProcessor processes cloud cost data for AI consumption with trend analysis
type CloudCostProcessor struct {
	anomalyThreshold     float64
	topServicesCount     int
	insightConfidenceMin float64
}

// NewCloudCostProcessor creates a new cloud cost processor
func NewCloudCostProcessor() *CloudCostProcessor {
	return &CloudCostProcessor{
		anomalyThreshold:     2.0, // 2x standard deviation
		topServicesCount:     15,
		insightConfidenceMin: 0.6,
	}
}

// ProcessCloudCosts processes raw cloud cost data into AI-optimized format
func (p *CloudCostProcessor) ProcessCloudCosts(data *opencost.CloudCostSetRange, params *utils.CloudCostQueryParams, ctx *types.QueryContext) (*types.CloudCostResponseData, error) {
	if data == nil || len(data.CloudCostSets) == 0 {
		return &types.CloudCostResponseData{
			MCPResponseData: types.MCPResponseData{
				Summary:   "No cloud cost data found for the specified criteria",
				Data:      []types.ProcessedCloudCost{},
				Metadata:  p.createMetadata(params, ctx, 0),
			},
			CloudCosts:      []types.ProcessedCloudCost{},
			TotalCost:       0,
			CostByProvider:  make(map[string]float64),
			CostByService:   make(map[string]float64),
			BillingAnalysis: types.BillingAnalysis{},
			Anomalies:       []types.CostAnomaly{},
		}, nil
	}

	// Process cloud costs
	processedCloudCosts := p.processCloudCostSets(data.CloudCostSets)
	
	// Calculate total cost
	totalCost := p.calculateTotalCost(processedCloudCosts)
	
	// Generate cost breakdowns
	costByProvider := p.generateCostByProvider(processedCloudCosts)
	costByService := p.generateCostByService(processedCloudCosts)
	
	// Generate billing analysis
	billingAnalysis := p.generateBillingAnalysis(data, processedCloudCosts, totalCost)
	
	// Detect anomalies
	anomalies := p.detectAnomalies(processedCloudCosts)
	
	// Generate insights
	insights := p.generateInsights(processedCloudCosts, billingAnalysis, anomalies)
	
	// Generate trends
	trends := p.generateTrends(data, processedCloudCosts)
	
	// Generate recommendations
	recommendations := p.generateRecommendations(processedCloudCosts, insights, billingAnalysis)

	response := &types.CloudCostResponseData{
		MCPResponseData: types.MCPResponseData{
			Summary:         p.generateSummary(processedCloudCosts, totalCost, billingAnalysis),
			Data:            processedCloudCosts,
			Insights:        insights,
			Trends:          trends,
			Recommendations: recommendations,
			Metadata:        p.createMetadata(params, ctx, len(processedCloudCosts)),
		},
		CloudCosts:      processedCloudCosts,
		TotalCost:       totalCost,
		CostByProvider:  costByProvider,
		CostByService:   costByService,
		BillingAnalysis: billingAnalysis,
		Anomalies:       anomalies,
	}

	return response, nil
}

// processCloudCostSets converts OpenCost cloud costs to processed format
func (p *CloudCostProcessor) processCloudCostSets(cloudCostSets []*opencost.CloudCostSet) []types.ProcessedCloudCost {
	var processed []types.ProcessedCloudCost

	for _, set := range cloudCostSets {
		if set == nil {
			continue
		}

		for _, cloudCost := range set.CloudCosts {
			if cloudCost == nil {
				continue
			}

			processedCloudCost := types.ProcessedCloudCost{
				Properties:       p.extractCloudCostProperties(cloudCost),
				Provider:         p.getProvider(cloudCost),
				AccountID:        p.getAccountID(cloudCost),
				InvoiceEntityID:  p.getInvoiceEntityID(cloudCost),
				Service:          p.getService(cloudCost),
				SKU:              p.getSKU(cloudCost),
				Region:           p.getRegion(cloudCost),
				UsageType:        p.getUsageType(cloudCost),
				UsageUnit:        p.getUsageUnit(cloudCost),
				Domain:           p.getDomain(cloudCost),
				Start:            *set.Window.Start(),
				End:              *set.Window.End(),
				ListCost:         cloudCost.ListCost.Cost,
				NetCost:          cloudCost.NetCost.Cost,
				AmortizedCost:    cloudCost.AmortizedCost.Cost,
				InvoicedCost:     cloudCost.InvoicedCost.Cost,
				AmortizedNetCost: cloudCost.AmortizedNetCost.Cost,
				Usage:            p.getUsage(cloudCost),
				CostPerUnit:      p.calculateCostPerUnit(cloudCost),
				Trend:            p.calculateCostTrend(cloudCost),
				Tags:             p.generateCloudCostTags(cloudCost),
			}

			processed = append(processed, processedCloudCost)
		}
	}

	// Sort by net cost descending
	sort.Slice(processed, func(i, j int) bool {
		return processed[i].NetCost > processed[j].NetCost
	})

	return processed
}

// Helper methods for extracting cloud cost properties
func (p *CloudCostProcessor) extractCloudCostProperties(cloudCost *opencost.CloudCost) map[string]interface{} {
	props := make(map[string]interface{})

	if cloudCost.Properties != nil {
		props["provider"] = cloudCost.Properties.Provider
		props["account_id"] = cloudCost.Properties.AccountID
		props["invoice_entity_id"] = cloudCost.Properties.InvoiceEntityID
		props["service"] = cloudCost.Properties.Service
		props["region"] = cloudCost.Properties.RegionID
		// Usage type may not be available in CloudCostProperties
		
		if len(cloudCost.Properties.Labels) > 0 {
			props["labels"] = cloudCost.Properties.Labels
		}
	}

	return props
}

func (p *CloudCostProcessor) getProvider(cloudCost *opencost.CloudCost) string {
	if cloudCost.Properties != nil {
		return cloudCost.Properties.Provider
	}
	return ""
}

func (p *CloudCostProcessor) getAccountID(cloudCost *opencost.CloudCost) string {
	if cloudCost.Properties != nil {
		return cloudCost.Properties.AccountID
	}
	return ""
}

func (p *CloudCostProcessor) getInvoiceEntityID(cloudCost *opencost.CloudCost) string {
	if cloudCost.Properties != nil {
		return cloudCost.Properties.InvoiceEntityID
	}
	return ""
}

func (p *CloudCostProcessor) getService(cloudCost *opencost.CloudCost) string {
	if cloudCost.Properties != nil {
		return cloudCost.Properties.Service
	}
	return ""
}

func (p *CloudCostProcessor) getSKU(cloudCost *opencost.CloudCost) string {
	// SKU might be in labels or properties
	if cloudCost.Properties != nil && cloudCost.Properties.Labels != nil {
		if sku, ok := cloudCost.Properties.Labels["sku"]; ok {
			return sku
		}
	}
	return ""
}

func (p *CloudCostProcessor) getRegion(cloudCost *opencost.CloudCost) string {
	if cloudCost.Properties != nil {
		return cloudCost.Properties.RegionID
	}
	return ""
}

func (p *CloudCostProcessor) getUsageType(cloudCost *opencost.CloudCost) string {
	// Usage type may be in labels
	if cloudCost.Properties != nil && cloudCost.Properties.Labels != nil {
		if usageType, ok := cloudCost.Properties.Labels["usage_type"]; ok {
			return usageType
		}
	}
	return ""
}

func (p *CloudCostProcessor) getUsageUnit(cloudCost *opencost.CloudCost) string {
	// Usage unit may be in labels
	if cloudCost.Properties != nil && cloudCost.Properties.Labels != nil {
		if usageUnit, ok := cloudCost.Properties.Labels["usage_unit"]; ok {
			return usageUnit
		}
	}
	return ""
}

func (p *CloudCostProcessor) getDomain(cloudCost *opencost.CloudCost) string {
	// Domain might be derived from service or other properties
	return ""
}

func (p *CloudCostProcessor) getUsage(cloudCost *opencost.CloudCost) float64 {
	// Usage might be stored in metrics or derived from costs
	return 0.0
}

func (p *CloudCostProcessor) calculateCostPerUnit(cloudCost *opencost.CloudCost) float64 {
	usage := p.getUsage(cloudCost)
	if usage > 0 && cloudCost.NetCost.Cost > 0 {
		return cloudCost.NetCost.Cost / usage
	}
	return 0.0
}

func (p *CloudCostProcessor) calculateCostTrend(cloudCost *opencost.CloudCost) *types.CostTrend {
	// Simplified trend calculation - would need historical data for real implementation
	return &types.CostTrend{
		Direction:     "stable",
		ChangePercent: 0.0,
		Period:        "unknown",
		Volatility:    "low",
	}
}

func (p *CloudCostProcessor) generateCloudCostTags(cloudCost *opencost.CloudCost) []string {
	var tags []string

	// Cost-based tags
	if cloudCost.NetCost.Cost > 1000 {
		tags = append(tags, "high-cost")
	} else if cloudCost.NetCost.Cost > 100 {
		tags = append(tags, "medium-cost")
	} else {
		tags = append(tags, "low-cost")
	}

	// Provider-based tags
	provider := p.getProvider(cloudCost)
	if provider != "" {
		tags = append(tags, fmt.Sprintf("provider-%s", strings.ToLower(provider)))
	}

	// Service-based tags
	service := p.getService(cloudCost)
	if service != "" {
		tags = append(tags, fmt.Sprintf("service-%s", strings.ToLower(strings.ReplaceAll(service, " ", "-"))))
	}

	return tags
}

// Analysis methods

func (p *CloudCostProcessor) calculateTotalCost(cloudCosts []types.ProcessedCloudCost) float64 {
	total := 0.0
	for _, cost := range cloudCosts {
		total += cost.NetCost
	}
	return total
}

func (p *CloudCostProcessor) generateCostByProvider(cloudCosts []types.ProcessedCloudCost) map[string]float64 {
	costByProvider := make(map[string]float64)
	for _, cost := range cloudCosts {
		if cost.Provider != "" {
			costByProvider[cost.Provider] += cost.NetCost
		}
	}
	return costByProvider
}

func (p *CloudCostProcessor) generateCostByService(cloudCosts []types.ProcessedCloudCost) map[string]float64 {
	costByService := make(map[string]float64)
	for _, cost := range cloudCosts {
		if cost.Service != "" {
			costByService[cost.Service] += cost.NetCost
		}
	}
	return costByService
}

func (p *CloudCostProcessor) generateBillingAnalysis(data *opencost.CloudCostSetRange, cloudCosts []types.ProcessedCloudCost, totalCost float64) types.BillingAnalysis {
	analysis := types.BillingAnalysis{
		TotalSpend: totalCost,
	}

	// Period comparison (simplified - would need historical data)
	if len(data.CloudCostSets) > 1 {
		firstPeriod := p.calculateSetTotalCost(data.CloudCostSets[0])
		lastPeriod := p.calculateSetTotalCost(data.CloudCostSets[len(data.CloudCostSets)-1])

		if firstPeriod > 0 {
			changePercent := ((lastPeriod - firstPeriod) / firstPeriod) * 100
			analysis.PeriodComparison = types.PeriodComparison{
				CurrentPeriod:  lastPeriod,
				PreviousPeriod: firstPeriod,
				Change:         lastPeriod - firstPeriod,
				ChangePercent:  changePercent,
				Trend:          p.determineTrend(changePercent),
			}
		}
	}

	// Spend distribution
	analysis.SpendDistribution = p.generateSpendDistribution(cloudCosts, totalCost)

	// Cost optimization
	analysis.CostOptimization = p.generateCostOptimization(cloudCosts)

	return analysis
}

func (p *CloudCostProcessor) calculateSetTotalCost(set *opencost.CloudCostSet) float64 {
	if set == nil {
		return 0
	}

	total := 0.0
	for _, cloudCost := range set.CloudCosts {
		if cloudCost != nil {
			total += cloudCost.NetCost.Cost
		}
	}
	return total
}

func (p *CloudCostProcessor) determineTrend(changePercent float64) string {
	if changePercent > 5 {
		return "increasing"
	} else if changePercent < -5 {
		return "decreasing"
	}
	return "stable"
}

func (p *CloudCostProcessor) generateSpendDistribution(cloudCosts []types.ProcessedCloudCost, totalCost float64) types.SpendDistribution {
	// Generate top services
	serviceCosts := make(map[string]float64)
	regionCosts := make(map[string]float64)
	accountCosts := make(map[string]float64)

	for _, cost := range cloudCosts {
		if cost.Service != "" {
			serviceCosts[cost.Service] += cost.NetCost
		}
		if cost.Region != "" {
			regionCosts[cost.Region] += cost.NetCost
		}
		if cost.AccountID != "" {
			accountCosts[cost.AccountID] += cost.NetCost
		}
	}

	return types.SpendDistribution{
		TopServices: p.convertToCostDrivers(serviceCosts, totalCost, 5),
		TopRegions:  p.convertToCostDrivers(regionCosts, totalCost, 5),
		TopAccounts: p.convertToCostDrivers(accountCosts, totalCost, 5),
		SpendPattern: p.determineSpendPattern(cloudCosts),
	}
}

func (p *CloudCostProcessor) convertToCostDrivers(costMap map[string]float64, totalCost float64, limit int) []types.CostDriver {
	var drivers []types.CostDriver

	for name, cost := range costMap {
		percentage := 0.0
		if totalCost > 0 {
			percentage = (cost / totalCost) * 100
		}

		drivers = append(drivers, types.CostDriver{
			Name:       name,
			Cost:       cost,
			Percentage: percentage,
			Type:       "cloud_cost",
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

func (p *CloudCostProcessor) determineSpendPattern(cloudCosts []types.ProcessedCloudCost) string {
	if len(cloudCosts) == 0 {
		return "unknown"
	}

	// Simple pattern detection based on distribution
	computeCost := 0.0
	storageCost := 0.0
	networkCost := 0.0
	otherCost := 0.0

	for _, cost := range cloudCosts {
		service := strings.ToLower(cost.Service)
		if strings.Contains(service, "compute") || strings.Contains(service, "ec2") || strings.Contains(service, "instance") {
			computeCost += cost.NetCost
		} else if strings.Contains(service, "storage") || strings.Contains(service, "s3") || strings.Contains(service, "disk") {
			storageCost += cost.NetCost
		} else if strings.Contains(service, "network") || strings.Contains(service, "bandwidth") || strings.Contains(service, "data transfer") {
			networkCost += cost.NetCost
		} else {
			otherCost += cost.NetCost
		}
	}

	total := computeCost + storageCost + networkCost + otherCost
	if total == 0 {
		return "unknown"
	}

	computePercent := (computeCost / total) * 100
	storagePercent := (storageCost / total) * 100
	networkPercent := (networkCost / total) * 100

	if computePercent > 60 {
		return "compute-heavy"
	} else if storagePercent > 40 {
		return "storage-heavy"
	} else if networkPercent > 30 {
		return "network-heavy"
	} else {
		return "balanced"
	}
}

func (p *CloudCostProcessor) generateCostOptimization(cloudCosts []types.ProcessedCloudCost) types.CostOptimization {
	optimization := types.CostOptimization{
		Opportunities: []types.OptimizationOpportunity{},
	}

	totalSavings := 0.0

	// Identify high-cost services for optimization
	serviceCosts := make(map[string]float64)
	for _, cost := range cloudCosts {
		if cost.Service != "" {
			serviceCosts[cost.Service] += cost.NetCost
		}
	}

	// Create optimization opportunities for high-cost services
	for service, cost := range serviceCosts {
		if cost > 500 { // Services with significant cost
			potentialSavings := cost * 0.15 // Assume 15% potential savings
			totalSavings += potentialSavings

			optimization.Opportunities = append(optimization.Opportunities, types.OptimizationOpportunity{
				Type:        "service_optimization",
				Service:     service,
				Description: fmt.Sprintf("Optimize %s usage and configuration", service),
				Savings:     potentialSavings,
				Confidence:  0.7,
				ActionPlan:  []string{
					fmt.Sprintf("Review %s usage patterns", service),
					"Consider reserved instances or committed use discounts",
					"Implement auto-scaling and right-sizing",
				},
			})
		}
	}

	optimization.PotentialSavings = totalSavings

	return optimization
}

func (p *CloudCostProcessor) detectAnomalies(cloudCosts []types.ProcessedCloudCost) []types.CostAnomaly {
	var anomalies []types.CostAnomaly

	// Group costs by service to detect anomalies
	serviceCosts := make(map[string][]float64)
	for _, cost := range cloudCosts {
		if cost.Service != "" {
			serviceCosts[cost.Service] = append(serviceCosts[cost.Service], cost.NetCost)
		}
	}

	// Simple anomaly detection based on cost distribution
	for service, costs := range serviceCosts {
		if len(costs) < 3 { // Need at least 3 data points
			continue
		}

		mean := p.calculateMean(costs)
		stdDev := p.calculateStdDev(costs, mean)

		for _, cost := range costs {
			if stdDev > 0 {
				score := math.Abs(cost-mean) / stdDev
				if score > p.anomalyThreshold {
					anomaly := types.CostAnomaly{
						Type:         "cost_spike",
						Service:      service,
						DetectedAt:   time.Now(),
						AnomalyScore: score,
						ExpectedCost: mean,
						ActualCost:   cost,
						Impact:       p.determineImpact(score),
						Description:  fmt.Sprintf("Unusual cost pattern detected for %s", service),
						PossibleCauses: []string{
							"Increased usage",
							"Configuration change",
							"Pricing tier change",
							"Data processing spike",
						},
						Recommendations: []string{
							"Review recent changes in service configuration",
							"Check for unusual usage patterns",
							"Verify pricing tier and billing settings",
						},
					}
					anomalies = append(anomalies, anomaly)
				}
			}
		}
	}

	return anomalies
}

func (p *CloudCostProcessor) calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (p *CloudCostProcessor) calculateStdDev(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0
	}
	sumSquares := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(values)-1))
}

func (p *CloudCostProcessor) determineImpact(score float64) string {
	if score > 4 {
		return "high"
	} else if score > 2.5 {
		return "medium"
	}
	return "low"
}

// Insight and trend generation

func (p *CloudCostProcessor) generateInsights(cloudCosts []types.ProcessedCloudCost, billingAnalysis types.BillingAnalysis, anomalies []types.CostAnomaly) []types.Insight {
	var insights []types.Insight

	// Cost trend insight
	if billingAnalysis.PeriodComparison.ChangePercent > 20 {
		insights = append(insights, types.Insight{
			Type:        "cost_trend",
			Title:       "Significant Cost Increase",
			Description: fmt.Sprintf("Cloud costs increased by %.1f%% compared to previous period", billingAnalysis.PeriodComparison.ChangePercent),
			Severity:    "high",
			Value:       billingAnalysis.PeriodComparison.ChangePercent,
			Confidence:  0.9,
			ActionItems: []string{
				"Analyze service-level cost changes",
				"Review recent infrastructure changes",
				"Implement cost monitoring and alerts",
			},
		})
	}

	// Anomaly insight
	if len(anomalies) > 0 {
		highImpactAnomalies := 0
		for _, anomaly := range anomalies {
			if anomaly.Impact == "high" {
				highImpactAnomalies++
			}
		}

		if highImpactAnomalies > 0 {
			insights = append(insights, types.Insight{
				Type:        "anomaly",
				Title:       "Cost Anomalies Detected",
				Description: fmt.Sprintf("%d high-impact cost anomalies detected", highImpactAnomalies),
				Severity:    "high",
				Value:       float64(highImpactAnomalies),
				Confidence:  0.85,
				ActionItems: []string{
					"Investigate anomaly root causes",
					"Review service usage patterns",
					"Set up automated anomaly detection",
				},
			})
		}
	}

	// Cost optimization insight
	if billingAnalysis.CostOptimization.PotentialSavings > 500 {
		insights = append(insights, types.Insight{
			Type:        "cost_optimization",
			Title:       "Significant Savings Opportunity",
			Description: fmt.Sprintf("$%.2f potential savings identified", billingAnalysis.CostOptimization.PotentialSavings),
			Severity:    "medium",
			Value:       billingAnalysis.CostOptimization.PotentialSavings,
			Confidence:  0.75,
			ActionItems: []string{
				"Implement reserved instance purchases",
				"Optimize resource utilization",
				"Review and right-size services",
			},
		})
	}

	return insights
}

func (p *CloudCostProcessor) generateTrends(data *opencost.CloudCostSetRange, cloudCosts []types.ProcessedCloudCost) []types.Trend {
	var trends []types.Trend

	// Cost trend analysis
	if len(data.CloudCostSets) > 1 {
		firstPeriod := p.calculateSetTotalCost(data.CloudCostSets[0])
		lastPeriod := p.calculateSetTotalCost(data.CloudCostSets[len(data.CloudCostSets)-1])

		if firstPeriod > 0 {
			changePercent := ((lastPeriod - firstPeriod) / firstPeriod) * 100
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
				Description: fmt.Sprintf("Cloud costs %s by %.1f%% over the analyzed period", direction, math.Abs(changePercent)),
				Period:      "analyzed_window",
				Confidence:  0.8,
				Impact:      p.determineImpactFromMagnitude(math.Abs(changePercent)),
			})
		}
	}

	return trends
}

func (p *CloudCostProcessor) determineImpactFromMagnitude(magnitude float64) string {
	if magnitude > 50 {
		return "high"
	} else if magnitude > 20 {
		return "medium"
	} else if magnitude > 5 {
		return "low"
	}
	return "minimal"
}

func (p *CloudCostProcessor) generateRecommendations(cloudCosts []types.ProcessedCloudCost, insights []types.Insight, billingAnalysis types.BillingAnalysis) []types.Recommendation {
	var recommendations []types.Recommendation

	// Cost optimization recommendations
	if billingAnalysis.CostOptimization.PotentialSavings > 0 {
		recommendations = append(recommendations, types.Recommendation{
			Type:             "cost_optimization",
			Title:            "Implement Cloud Cost Optimization",
			Description:      fmt.Sprintf("Reduce cloud spend by $%.2f through optimization strategies", billingAnalysis.CostOptimization.PotentialSavings),
			Priority:         "high",
			Impact:           "high",
			Effort:           "medium",
			PotentialSavings: &billingAnalysis.CostOptimization.PotentialSavings,
			Tags:             []string{"cost-optimization", "cloud"},
			Steps: []string{
				"Analyze highest-cost services for optimization opportunities",
				"Implement reserved instances or committed use discounts",
				"Set up auto-scaling and right-sizing policies",
				"Review and eliminate unused resources",
			},
		})
	}

	// Monitoring recommendations
	if len(cloudCosts) > 20 {
		recommendations = append(recommendations, types.Recommendation{
			Type:        "monitoring",
			Title:       "Enhance Cloud Cost Monitoring",
			Description: "Implement comprehensive cloud cost monitoring and governance",
			Priority:    "medium",
			Impact:      "medium",
			Effort:      "low",
			Tags:        []string{"monitoring", "governance"},
			Steps: []string{
				"Set up cost budgets and alerts",
				"Implement cost allocation tags",
				"Create automated cost reporting",
				"Establish regular cost review processes",
			},
		})
	}

	return recommendations
}

func (p *CloudCostProcessor) generateSummary(cloudCosts []types.ProcessedCloudCost, totalCost float64, billingAnalysis types.BillingAnalysis) string {
	if len(cloudCosts) == 0 {
		return "No cloud cost data found for the specified criteria."
	}

	summary := fmt.Sprintf("Found %d cloud cost entries with total spend of $%.2f", len(cloudCosts), totalCost)

	// Add period comparison
	if billingAnalysis.PeriodComparison.ChangePercent != 0 {
		trend := "increased"
		change := billingAnalysis.PeriodComparison.ChangePercent
		if change < 0 {
			trend = "decreased"
			change = -change
		}
		summary += fmt.Sprintf(". Costs %s by %.1f%% compared to previous period", trend, change)
	}

	// Add top service if available
	if len(cloudCosts) > 0 {
		serviceCosts := make(map[string]float64)
		for _, cost := range cloudCosts {
			if cost.Service != "" {
				serviceCosts[cost.Service] += cost.NetCost
			}
		}

		if len(serviceCosts) > 0 {
			var topService string
			var topCost float64
			for service, cost := range serviceCosts {
				if cost > topCost {
					topCost = cost
					topService = service
				}
			}

			if topService != "" {
				percentage := (topCost / totalCost) * 100
				summary += fmt.Sprintf(". Top service: %s at $%.2f (%.1f%% of total)", topService, topCost, percentage)
			}
		}
	}

	return summary
}

func (p *CloudCostProcessor) createMetadata(params *utils.CloudCostQueryParams, ctx *types.QueryContext, dataPoints int) types.ResponseMetadata {
	metadata := types.ResponseMetadata{
		QueryType:     "cloud_cost",
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