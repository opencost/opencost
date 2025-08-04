package utils

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/pkg/mcp/types"
)

// ResponseFormatter formats AI-optimized responses for natural conversation
type ResponseFormatter struct {
	includeMetadata      bool
	maxSummaryItems     int
	maxDetailItems      int
	includeRecommendations bool
}

// NewResponseFormatter creates a new response formatter
func NewResponseFormatter() *ResponseFormatter {
	return &ResponseFormatter{
		includeMetadata:        true,
		maxSummaryItems:       5,
		maxDetailItems:        20,
		includeRecommendations: true,
	}
}

// FormatAllocationResponse formats allocation response for AI conversation
func (f *ResponseFormatter) FormatAllocationResponse(data *types.AllocationResponseData, query string) string {
	var response strings.Builder

	// Start with natural language summary
	response.WriteString(f.formatAllocationSummary(data, query))
	
	// Add key insights if present
	if len(data.Insights) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatInsights(data.Insights))
	}

	// Add top cost drivers
	if len(data.TopCostDrivers) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatTopCostDrivers(data.TopCostDrivers))
	}

	// Add breakdown information
	response.WriteString("\n\n")
	response.WriteString(f.formatAllocationBreakdown(data.Breakdown))

	// Add trends if present
	if len(data.Trends) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatTrends(data.Trends))
	}

	// Add recommendations if enabled and present
	if f.includeRecommendations && len(data.Recommendations) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatRecommendations(data.Recommendations))
	}

	// Add follow-up questions
	if len(data.FollowUpQuestions) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatFollowUpQuestions(data.FollowUpQuestions))
	}

	// Add metadata if enabled
	if f.includeMetadata {
		response.WriteString("\n\n")
		response.WriteString(f.formatMetadata(data.Metadata))
	}

	return response.String()
}

// FormatAssetResponse formats asset response for AI conversation
func (f *ResponseFormatter) FormatAssetResponse(data *types.AssetResponseData, query string) string {
	var response strings.Builder

	// Start with natural language summary
	response.WriteString(f.formatAssetSummary(data, query))

	// Add utilization summary
	response.WriteString("\n\n")
	response.WriteString(f.formatUtilizationSummary(data.Utilization))

	// Add key insights if present
	if len(data.Insights) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatInsights(data.Insights))
	}

	// Add optimization opportunities
	if data.Optimization.TotalPotentialSavings > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatOptimizationSummary(data.Optimization))
	}

	// Add asset type breakdown
	response.WriteString("\n\n")
	response.WriteString(f.formatAssetTypeBreakdown(data.AssetSummary))

	// Add trends if present
	if len(data.Trends) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatTrends(data.Trends))
	}

	// Add recommendations if enabled and present
	if f.includeRecommendations && len(data.Recommendations) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatRecommendations(data.Recommendations))
	}

	// Add follow-up questions
	if len(data.FollowUpQuestions) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatFollowUpQuestions(data.FollowUpQuestions))
	}

	// Add metadata if enabled
	if f.includeMetadata {
		response.WriteString("\n\n")
		response.WriteString(f.formatMetadata(data.Metadata))
	}

	return response.String()
}

// FormatCloudCostResponse formats cloud cost response for AI conversation
func (f *ResponseFormatter) FormatCloudCostResponse(data *types.CloudCostResponseData, query string) string {
	var response strings.Builder

	// Start with natural language summary
	response.WriteString(f.formatCloudCostSummary(data, query))

	// Add billing analysis
	response.WriteString("\n\n")
	response.WriteString(f.formatBillingAnalysis(data.BillingAnalysis))

	// Add key insights if present
	if len(data.Insights) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatInsights(data.Insights))
	}

	// Add anomalies if present
	if len(data.Anomalies) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatAnomalies(data.Anomalies))
	}

	// Add cost breakdown
	response.WriteString("\n\n")
	response.WriteString(f.formatCloudCostBreakdown(data))

	// Add trends if present
	if len(data.Trends) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatTrends(data.Trends))
	}

	// Add recommendations if enabled and present
	if f.includeRecommendations && len(data.Recommendations) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatRecommendations(data.Recommendations))
	}

	// Add follow-up questions
	if len(data.FollowUpQuestions) > 0 {
		response.WriteString("\n\n")
		response.WriteString(f.formatFollowUpQuestions(data.FollowUpQuestions))
	}

	// Add metadata if enabled
	if f.includeMetadata {
		response.WriteString("\n\n")
		response.WriteString(f.formatMetadata(data.Metadata))
	}

	return response.String()
}

// Allocation-specific formatting methods

func (f *ResponseFormatter) formatAllocationSummary(data *types.AllocationResponseData, _ string) string {
	var summary strings.Builder

	summary.WriteString("## Allocation Cost Analysis\n\n")
	summary.WriteString(data.Summary)

	if len(data.Allocations) > 0 {
		// Add efficiency information
		lowEfficiencyCount := 0
		totalWithEfficiency := 0
		totalEfficiency := 0.0

		for _, alloc := range data.Allocations {
			if alloc.Efficiency != nil {
				totalWithEfficiency++
				totalEfficiency += alloc.Efficiency.Overall
				if alloc.Efficiency.Overall < 0.6 {
					lowEfficiencyCount++
				}
			}
		}

		if totalWithEfficiency > 0 {
			avgEfficiency := (totalEfficiency / float64(totalWithEfficiency)) * 100
			summary.WriteString(fmt.Sprintf("\n\n**Resource Efficiency**: Average %.1f%%", avgEfficiency))
			
			if lowEfficiencyCount > 0 {
				summary.WriteString(fmt.Sprintf(" (%d allocations below 60%% efficiency)", lowEfficiencyCount))
			}
		}
	}

	return summary.String()
}

func (f *ResponseFormatter) formatTopCostDrivers(drivers []types.CostDriver) string {
	var output strings.Builder
	output.WriteString("### Top Cost Drivers\n\n")

	maxItems := f.maxSummaryItems
	if len(drivers) < maxItems {
		maxItems = len(drivers)
	}

	for i := 0; i < maxItems; i++ {
		driver := drivers[i]
		output.WriteString(fmt.Sprintf("**%d. %s** - $%.2f (%.1f%%)\n", 
			i+1, driver.Name, driver.Cost, driver.Percentage))
		
		if driver.Description != "" {
			output.WriteString(fmt.Sprintf("   %s\n", driver.Description))
		}
		output.WriteString("\n")
	}

	if len(drivers) > maxItems {
		output.WriteString(fmt.Sprintf("*... and %d more cost drivers*\n", len(drivers)-maxItems))
	}

	return output.String()
}

func (f *ResponseFormatter) formatAllocationBreakdown(breakdown types.AllocationBreakdown) string {
	var output strings.Builder
	output.WriteString("### Cost Breakdown\n\n")

	// Show namespace breakdown if available
	if len(breakdown.ByNamespace) > 0 {
		output.WriteString("**By Namespace:**\n")
		for i, driver := range breakdown.ByNamespace {
			if i >= 3 { // Limit to top 3
				break
			}
			output.WriteString(fmt.Sprintf("- %s: $%.2f (%.1f%%)\n", 
				driver.Name, driver.Cost, driver.Percentage))
		}
		output.WriteString("\n")
	}

	// Show service breakdown if available
	if len(breakdown.ByService) > 0 {
		output.WriteString("**By Service:**\n")
		for i, driver := range breakdown.ByService {
			if i >= 3 { // Limit to top 3
				break
			}
			output.WriteString(fmt.Sprintf("- %s: $%.2f (%.1f%%)\n", 
				driver.Name, driver.Cost, driver.Percentage))
		}
		output.WriteString("\n")
	}

	return output.String()
}

// Asset-specific formatting methods

func (f *ResponseFormatter) formatAssetSummary(data *types.AssetResponseData, _ string) string {
	var summary strings.Builder

	summary.WriteString("## Asset Cost Analysis\n\n")
	summary.WriteString(data.Summary)

	// Add asset type distribution
	if len(data.AssetSummary.AssetTypes) > 1 {
		summary.WriteString("\n\n**Asset Distribution**: ")
		var typeStrs []string
		for assetType, count := range data.AssetSummary.AssetTypes {
			typeStrs = append(typeStrs, fmt.Sprintf("%d %s", count, assetType))
		}
		summary.WriteString(strings.Join(typeStrs, ", "))
	}

	// Add optimization summary
	if data.Optimization.TotalPotentialSavings > 0 {
		savingsPercent := (data.Optimization.TotalPotentialSavings / data.TotalCost) * 100
		summary.WriteString(fmt.Sprintf("\n\n**Optimization Opportunity**: $%.2f potential savings (%.1f%% of total cost)", 
			data.Optimization.TotalPotentialSavings, savingsPercent))
	}

	return summary.String()
}

func (f *ResponseFormatter) formatUtilizationSummary(utilization types.UtilizationStats) string {
	var output strings.Builder
	output.WriteString("### Resource Utilization\n\n")

	if utilization.AverageCPU > 0 {
		output.WriteString(fmt.Sprintf("**CPU**: %.1f%% average utilization", utilization.AverageCPU*100))
		if utilization.AverageCPU < utilization.LowUtilizationThreshold {
			output.WriteString(" (⚠️ Low)")
		} else if utilization.AverageCPU > utilization.HighUtilizationThreshold {
			output.WriteString(" (🔴 High)")
		} else {
			output.WriteString(" (✅ Good)")
		}
		output.WriteString("\n")
	}

	if utilization.AverageRAM > 0 {
		output.WriteString(fmt.Sprintf("**Memory**: %.1f%% average utilization", utilization.AverageRAM*100))
		if utilization.AverageRAM < utilization.LowUtilizationThreshold {
			output.WriteString(" (⚠️ Low)")
		} else if utilization.AverageRAM > utilization.HighUtilizationThreshold {
			output.WriteString(" (🔴 High)")
		} else {
			output.WriteString(" (✅ Good)")
		}
		output.WriteString("\n")
	}

	if utilization.AverageStorage > 0 {
		output.WriteString(fmt.Sprintf("**Storage**: %.1f%% average utilization", utilization.AverageStorage*100))
		if utilization.AverageStorage < 0.5 {
			output.WriteString(" (⚠️ Low)")
		} else if utilization.AverageStorage > 0.9 {
			output.WriteString(" (🔴 High)")
		} else {
			output.WriteString(" (✅ Good)")
		}
		output.WriteString("\n")
	}

	return output.String()
}

func (f *ResponseFormatter) formatOptimizationSummary(optimization types.OptimizationData) string {
	var output strings.Builder
	output.WriteString("### Optimization Opportunities\n\n")

	output.WriteString(fmt.Sprintf("**Total Potential Savings**: $%.2f\n\n", optimization.TotalPotentialSavings))

	if optimization.RightSizingOpportunities > 0 {
		output.WriteString(fmt.Sprintf("- **Right-sizing**: %d assets can be optimized\n", optimization.RightSizingOpportunities))
	}

	if optimization.IdleResources > 0 {
		output.WriteString(fmt.Sprintf("- **Idle Resources**: %d assets with very low utilization\n", optimization.IdleResources))
	}

	if len(optimization.Recommendations) > 0 {
		output.WriteString(fmt.Sprintf("- **Action Items**: %d specific recommendations available\n", len(optimization.Recommendations)))
	}

	return output.String()
}

func (f *ResponseFormatter) formatAssetTypeBreakdown(summary types.AssetSummary) string {
	var output strings.Builder
	output.WriteString("### Asset Type Breakdown\n\n")

	// Sort by cost
	type TypeCost struct {
		Type string
		Cost float64
		Count int
	}

	var typeCosts []TypeCost
	for assetType, cost := range summary.CostByType {
		count := summary.AssetTypes[assetType]
		typeCosts = append(typeCosts, TypeCost{
			Type:  assetType,
			Cost:  cost,
			Count: count,
		})
	}

	sort.Slice(typeCosts, func(i, j int) bool {
		return typeCosts[i].Cost > typeCosts[j].Cost
	})

	for _, tc := range typeCosts {
		avgCost := tc.Cost / float64(tc.Count)
		output.WriteString(fmt.Sprintf("**%s** (%d assets) - $%.2f total, $%.2f avg\n", 
			tc.Type, tc.Count, tc.Cost, avgCost))
	}

	return output.String()
}

// Cloud cost-specific formatting methods

func (f *ResponseFormatter) formatCloudCostSummary(data *types.CloudCostResponseData, _ string) string {
	var summary strings.Builder

	summary.WriteString("## Cloud Cost Analysis\n\n")
	summary.WriteString(data.Summary)

	// Add period comparison if available
	if data.BillingAnalysis.PeriodComparison.ChangePercent != 0 {
		change := data.BillingAnalysis.PeriodComparison.ChangePercent
		trend := "increased"
		if change < 0 {
			trend = "decreased"
			change = -change
		}
		summary.WriteString(fmt.Sprintf("\n\n**Period Comparison**: Costs %s by %.1f%% compared to previous period", trend, change))
	}

	return summary.String()
}

func (f *ResponseFormatter) formatBillingAnalysis(analysis types.BillingAnalysis) string {
	var output strings.Builder
	output.WriteString("### Billing Analysis\n\n")

	output.WriteString(fmt.Sprintf("**Total Spend**: $%.2f\n", analysis.TotalSpend))

	// Period comparison
	if analysis.PeriodComparison.ChangePercent != 0 {
		symbol := "📈"
		if analysis.PeriodComparison.ChangePercent < 0 {
			symbol = "📉"
		}
		output.WriteString(fmt.Sprintf("**Change from Previous Period**: %s %.1f%% ($%.2f → $%.2f)\n", 
			symbol, analysis.PeriodComparison.ChangePercent, 
			analysis.PeriodComparison.PreviousPeriod, analysis.PeriodComparison.CurrentPeriod))
	}

	// Budget tracking if available
	if analysis.BudgetTracking != nil {
		budget := analysis.BudgetTracking
		utilizationSymbol := "✅"
		if budget.IsOverBudget {
			utilizationSymbol = "🔴"
		} else if budget.UtilizationPercent > 80 {
			utilizationSymbol = "⚠️"
		}

		output.WriteString(fmt.Sprintf("**Budget Status**: %s %.1f%% utilized ($%.2f of $%.2f)\n", 
			utilizationSymbol, budget.UtilizationPercent, budget.SpentAmount, budget.BudgetAmount))
	}

	return output.String()
}

func (f *ResponseFormatter) formatAnomalies(anomalies []types.CostAnomaly) string {
	var output strings.Builder
	output.WriteString("### Cost Anomalies Detected\n\n")

	maxAnomalies := 3
	if len(anomalies) < maxAnomalies {
		maxAnomalies = len(anomalies)
	}

	for i := 0; i < maxAnomalies; i++ {
		anomaly := anomalies[i]
		impactSymbol := "⚠️"
		if anomaly.Impact == "high" {
			impactSymbol = "🔴"
		}

		output.WriteString(fmt.Sprintf("%s **%s** - %s\n", impactSymbol, anomaly.Service, anomaly.Description))
		output.WriteString(fmt.Sprintf("   Expected: $%.2f, Actual: $%.2f (Score: %.2f)\n", 
			anomaly.ExpectedCost, anomaly.ActualCost, anomaly.AnomalyScore))
		
		if len(anomaly.PossibleCauses) > 0 {
			output.WriteString(fmt.Sprintf("   Possible causes: %s\n", strings.Join(anomaly.PossibleCauses, ", ")))
		}
		output.WriteString("\n")
	}

	if len(anomalies) > maxAnomalies {
		output.WriteString(fmt.Sprintf("*... and %d more anomalies detected*\n", len(anomalies)-maxAnomalies))
	}

	return output.String()
}

func (f *ResponseFormatter) formatCloudCostBreakdown(data *types.CloudCostResponseData) string {
	var output strings.Builder
	output.WriteString("### Cost Breakdown\n\n")

	// Provider breakdown
	if len(data.CostByProvider) > 0 {
		output.WriteString("**By Provider:**\n")
		
		// Sort by cost
		type ProviderCost struct {
			Provider string
			Cost     float64
		}
		
		var providerCosts []ProviderCost
		for provider, cost := range data.CostByProvider {
			providerCosts = append(providerCosts, ProviderCost{Provider: provider, Cost: cost})
		}
		
		sort.Slice(providerCosts, func(i, j int) bool {
			return providerCosts[i].Cost > providerCosts[j].Cost
		})
		
		for _, pc := range providerCosts {
			percentage := (pc.Cost / data.TotalCost) * 100
			output.WriteString(fmt.Sprintf("- %s: $%.2f (%.1f%%)\n", pc.Provider, pc.Cost, percentage))
		}
		output.WriteString("\n")
	}

	// Service breakdown
	if len(data.CostByService) > 0 {
		output.WriteString("**By Service (Top 5):**\n")
		
		// Sort by cost
		type ServiceCost struct {
			Service string
			Cost    float64
		}
		
		var serviceCosts []ServiceCost
		for service, cost := range data.CostByService {
			serviceCosts = append(serviceCosts, ServiceCost{Service: service, Cost: cost})
		}
		
		sort.Slice(serviceCosts, func(i, j int) bool {
			return serviceCosts[i].Cost > serviceCosts[j].Cost
		})
		
		maxServices := 5
		if len(serviceCosts) < maxServices {
			maxServices = len(serviceCosts)
		}
		
		for i := 0; i < maxServices; i++ {
			sc := serviceCosts[i]
			percentage := (sc.Cost / data.TotalCost) * 100
			output.WriteString(fmt.Sprintf("- %s: $%.2f (%.1f%%)\n", sc.Service, sc.Cost, percentage))
		}
		
		if len(serviceCosts) > maxServices {
			output.WriteString(fmt.Sprintf("- *... and %d more services*\n", len(serviceCosts)-maxServices))
		}
	}

	return output.String()
}

// Common formatting methods

func (f *ResponseFormatter) formatInsights(insights []types.Insight) string {
	var output strings.Builder
	output.WriteString("### Key Insights\n\n")

	for _, insight := range insights {
		severitySymbol := "ℹ️"
		switch insight.Severity {
		case "high":
			severitySymbol = "🔴"
		case "medium":
			severitySymbol = "⚠️"
		case "low":
			severitySymbol = "💡"
		}

		output.WriteString(fmt.Sprintf("%s **%s** (%.0f%% confidence)\n", 
			severitySymbol, insight.Title, insight.Confidence*100))
		output.WriteString(fmt.Sprintf("   %s\n", insight.Description))

		if len(insight.ActionItems) > 0 {
			output.WriteString("   **Action Items:**\n")
			for _, action := range insight.ActionItems {
				output.WriteString(fmt.Sprintf("   - %s\n", action))
			}
		}
		output.WriteString("\n")
	}

	return output.String()
}

func (f *ResponseFormatter) formatTrends(trends []types.Trend) string {
	var output strings.Builder
	output.WriteString("### Trends\n\n")

	for _, trend := range trends {
		directionSymbol := "📊"
		switch trend.Direction {
		case "increasing":
			directionSymbol = "📈"
		case "decreasing":
			directionSymbol = "📉"
		case "stable":
			directionSymbol = "📊"
		}

		output.WriteString(fmt.Sprintf("%s **%s**: %s by %.1f%% (%s impact)\n", 
			directionSymbol, strings.Title(strings.ReplaceAll(trend.Type, "_", " ")), 
			trend.Direction, trend.Magnitude, trend.Impact))
		output.WriteString(fmt.Sprintf("   %s\n\n", trend.Description))
	}

	return output.String()
}

func (f *ResponseFormatter) formatRecommendations(recommendations []types.Recommendation) string {
	var output strings.Builder
	output.WriteString("### Recommendations\n\n")

	for i, rec := range recommendations {
		if i >= 3 { // Limit to top 3 recommendations
			break
		}

		prioritySymbol := "📌"
		switch rec.Priority {
		case "high":
			prioritySymbol = "🔴"
		case "medium":
			prioritySymbol = "⚠️"
		case "low":
			prioritySymbol = "💡"
		}

		output.WriteString(fmt.Sprintf("%s **%s** (%s priority, %s impact)\n", 
			prioritySymbol, rec.Title, rec.Priority, rec.Impact))
		output.WriteString(fmt.Sprintf("   %s\n", rec.Description))

		if rec.PotentialSavings != nil {
			output.WriteString(fmt.Sprintf("   **Potential Savings**: $%.2f\n", *rec.PotentialSavings))
		}

		if len(rec.Steps) > 0 {
			output.WriteString("   **Implementation Steps:**\n")
			for j, step := range rec.Steps {
				if j >= 3 { // Limit steps
					output.WriteString("   - *... additional steps available*\n")
					break
				}
				output.WriteString(fmt.Sprintf("   %d. %s\n", j+1, step))
			}
		}
		output.WriteString("\n")
	}

	if len(recommendations) > 3 {
		output.WriteString(fmt.Sprintf("*... and %d more recommendations available*\n", len(recommendations)-3))
	}

	return output.String()
}

func (f *ResponseFormatter) formatFollowUpQuestions(questions []string) string {
	var output strings.Builder
	output.WriteString("### Suggested Follow-up Questions\n\n")

	maxQuestions := 3
	if len(questions) < maxQuestions {
		maxQuestions = len(questions)
	}

	for i := 0; i < maxQuestions; i++ {
		output.WriteString(fmt.Sprintf("❓ %s\n", questions[i]))
	}

	return output.String()
}

func (f *ResponseFormatter) formatMetadata(metadata types.ResponseMetadata) string {
	var output strings.Builder
	output.WriteString("---\n")
	output.WriteString("### Query Details\n\n")

	output.WriteString(fmt.Sprintf("**Query Type**: %s\n", metadata.QueryType))
	output.WriteString(fmt.Sprintf("**Execution Time**: %v\n", metadata.ExecutionTime))
	output.WriteString(fmt.Sprintf("**Data Points**: %d\n", metadata.DataPoints))
	output.WriteString(fmt.Sprintf("**Time Range**: %s\n", metadata.TimeRange.Start))
	
	if metadata.Aggregation != "" {
		output.WriteString(fmt.Sprintf("**Aggregation**: %s\n", metadata.Aggregation))
	}
	
	if len(metadata.Filters) > 0 {
		output.WriteString("**Filters Applied**: ")
		var filterStrs []string
		for _, filter := range metadata.Filters {
			filterStrs = append(filterStrs, fmt.Sprintf("%s", filter.Value))
		}
		output.WriteString(strings.Join(filterStrs, ", "))
		output.WriteString("\n")
	}

	return output.String()
}

// Utility methods for response formatting

// FormatCurrency formats currency values consistently
func (f *ResponseFormatter) FormatCurrency(amount float64) string {
	if amount >= 1000000 {
		return fmt.Sprintf("$%.1fM", amount/1000000)
	} else if amount >= 1000 {
		return fmt.Sprintf("$%.1fK", amount/1000)
	} else {
		return fmt.Sprintf("$%.2f", amount)
	}
}

// FormatPercentage formats percentage values consistently
func (f *ResponseFormatter) FormatPercentage(value float64) string {
	return fmt.Sprintf("%.1f%%", value)
}

// FormatDuration formats duration values consistently
func (f *ResponseFormatter) FormatDuration(duration time.Duration) string {
	if duration >= time.Hour {
		return fmt.Sprintf("%.1fh", duration.Hours())
	} else if duration >= time.Minute {
		return fmt.Sprintf("%.1fm", duration.Minutes())
	} else {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	}
}

// TruncateText truncates text to specified length with ellipsis
func (f *ResponseFormatter) TruncateText(text string, maxLength int) string {
	if len(text) <= maxLength {
		return text
	}
	return text[:maxLength-3] + "..."
}

// Configuration methods

// SetMaxSummaryItems sets the maximum number of items in summary sections
func (f *ResponseFormatter) SetMaxSummaryItems(max int) {
	f.maxSummaryItems = max
}

// SetMaxDetailItems sets the maximum number of items in detail sections
func (f *ResponseFormatter) SetMaxDetailItems(max int) {
	f.maxDetailItems = max
}

// SetIncludeMetadata controls whether metadata is included in responses
func (f *ResponseFormatter) SetIncludeMetadata(include bool) {
	f.includeMetadata = include
}

// SetIncludeRecommendations controls whether recommendations are included
func (f *ResponseFormatter) SetIncludeRecommendations(include bool) {
	f.includeRecommendations = include
}

// JSON formatting for structured data when needed

// FormatAsJSON returns the response data as formatted JSON
func (f *ResponseFormatter) FormatAsJSON(data interface{}) (string, error) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to format as JSON: %w", err)
	}
	return string(jsonData), nil
}

// FormatAsCompactJSON returns the response data as compact JSON
func (f *ResponseFormatter) FormatAsCompactJSON(data interface{}) (string, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to format as compact JSON: %w", err)
	}
	return string(jsonData), nil
}