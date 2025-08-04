package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Query parameter types for different OpenCost endpoints

// AllocationQueryParams represents parameters for allocation queries
type AllocationQueryParams struct {
	Window     string `json:"window"`
	Step       string `json:"step"`
	Aggregate  string `json:"aggregate"`
	Accumulate bool   `json:"accumulate"`
	Filter     string `json:"filter"`
}

// AssetQueryParams represents parameters for asset queries
type AssetQueryParams struct {
	Window     string `json:"window"`
	Step       string `json:"step"`
	Aggregate  string `json:"aggregate"`
	Accumulate bool   `json:"accumulate"`
	Filter     string `json:"filter"`
}

// CloudCostQueryParams represents parameters for cloud cost queries
type CloudCostQueryParams struct {
	Window     string `json:"window"`
	Step       string `json:"step"`
	Aggregate  string `json:"aggregate"`
	Accumulate bool   `json:"accumulate"`
	Filter     string `json:"filter"`
}

// Natural Language Query Parser

// ParseAllocationQuery parses natural language queries for allocation data
func ParseAllocationQuery(nlQuery string, baseParams map[string]interface{}) (*AllocationQueryParams, error) {
	params := &AllocationQueryParams{
		Window:     "1d",
		Step:       "1d",
		Aggregate:  "namespace",
		Accumulate: true,
	}

	// Apply base parameters first
	if err := applyBaseParams(params, baseParams); err != nil {
		return nil, err
	}

	// Parse natural language components
	queryLower := strings.ToLower(nlQuery)

	// Parse time window
	if window := parseTimeWindow(queryLower); window != "" {
		params.Window = window
	}

	// Parse aggregation level
	if aggregate := parseAggregationLevel(queryLower, "allocation"); aggregate != "" {
		params.Aggregate = aggregate
	}

	// Parse filters
	filters := parseFilters(nlQuery)
	if len(filters) > 0 {
		if params.Filter != "" {
			params.Filter = fmt.Sprintf("(%s) AND (%s)", params.Filter, strings.Join(filters, " AND "))
		} else {
			params.Filter = strings.Join(filters, " AND ")
		}
	}

	// Parse step size
	if step := parseStepSize(queryLower, params.Window); step != "" {
		params.Step = step
	}

	return params, nil
}

// ParseAssetQuery parses natural language queries for asset data
func ParseAssetQuery(nlQuery string, baseParams map[string]interface{}) (*AssetQueryParams, error) {
	params := &AssetQueryParams{
		Window:     "1d",
		Step:       "1d",
		Aggregate:  "type",
		Accumulate: true,
	}

	// Apply base parameters first
	if err := applyBaseParams(params, baseParams); err != nil {
		return nil, err
	}

	// Parse natural language components
	queryLower := strings.ToLower(nlQuery)

	// Parse time window
	if window := parseTimeWindow(queryLower); window != "" {
		params.Window = window
	}

	// Parse aggregation level
	if aggregate := parseAggregationLevel(queryLower, "asset"); aggregate != "" {
		params.Aggregate = aggregate
	}

	// Parse asset-specific filters
	filters := parseAssetFilters(nlQuery)
	if len(filters) > 0 {
		if params.Filter != "" {
			params.Filter = fmt.Sprintf("(%s) AND (%s)", params.Filter, strings.Join(filters, " AND "))
		} else {
			params.Filter = strings.Join(filters, " AND ")
		}
	}

	// Parse step size
	if step := parseStepSize(queryLower, params.Window); step != "" {
		params.Step = step
	}

	return params, nil
}

// ParseCloudCostQuery parses natural language queries for cloud cost data
func ParseCloudCostQuery(nlQuery string, baseParams map[string]interface{}) (*CloudCostQueryParams, error) {
	params := &CloudCostQueryParams{
		Window:     "7d",
		Step:       "1d",
		Aggregate:  "service",
		Accumulate: true,
	}

	// Apply base parameters first
	if err := applyBaseParams(params, baseParams); err != nil {
		return nil, err
	}

	// Parse natural language components
	queryLower := strings.ToLower(nlQuery)

	// Parse time window
	if window := parseTimeWindow(queryLower); window != "" {
		params.Window = window
	}

	// Parse aggregation level
	if aggregate := parseAggregationLevel(queryLower, "cloudcost"); aggregate != "" {
		params.Aggregate = aggregate
	}

	// Parse cloud-specific filters
	filters := parseCloudCostFilters(nlQuery)
	if len(filters) > 0 {
		if params.Filter != "" {
			params.Filter = fmt.Sprintf("(%s) AND (%s)", params.Filter, strings.Join(filters, " AND "))
		} else {
			params.Filter = strings.Join(filters, " AND ")
		}
	}

	// Parse step size
	if step := parseStepSize(queryLower, params.Window); step != "" {
		params.Step = step
	}

	return params, nil
}

// Helper functions for parsing

// applyBaseParams applies base parameters to query params
func applyBaseParams(params interface{}, baseParams map[string]interface{}) error {
	// Use reflection-like approach with type switching
	switch p := params.(type) {
	case *AllocationQueryParams:
		if window, ok := baseParams["window"].(string); ok && window != "" {
			p.Window = window
		}
		if step, ok := baseParams["step"].(string); ok && step != "" {
			p.Step = step
		}
		if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
			p.Aggregate = aggregate
		}
		if filter, ok := baseParams["filter"].(string); ok && filter != "" {
			p.Filter = filter
		}
		if accumulate, ok := baseParams["accumulate"].(bool); ok {
			p.Accumulate = accumulate
		}

	case *AssetQueryParams:
		if window, ok := baseParams["window"].(string); ok && window != "" {
			p.Window = window
		}
		if step, ok := baseParams["step"].(string); ok && step != "" {
			p.Step = step
		}
		if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
			p.Aggregate = aggregate
		}
		if filter, ok := baseParams["filter"].(string); ok && filter != "" {
			p.Filter = filter
		}
		if accumulate, ok := baseParams["accumulate"].(bool); ok {
			p.Accumulate = accumulate
		}

	case *CloudCostQueryParams:
		if window, ok := baseParams["window"].(string); ok && window != "" {
			p.Window = window
		}
		if step, ok := baseParams["step"].(string); ok && step != "" {
			p.Step = step
		}
		if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
			p.Aggregate = aggregate
		}
		if filter, ok := baseParams["filter"].(string); ok && filter != "" {
			p.Filter = filter
		}
		if accumulate, ok := baseParams["accumulate"].(bool); ok {
			p.Accumulate = accumulate
		}

	default:
		return fmt.Errorf("unsupported params type: %T", params)
	}

	return nil
}

// parseTimeWindow extracts time window from natural language
func parseTimeWindow(query string) string {
	timePatterns := map[string]string{
		// Relative time patterns
		`last\s+hour|past\s+hour|1\s*h`:                      "1h",
		`last\s+(\d+)\s*hours?|past\s+(\d+)\s*hours?`:       "",  // Will be handled by regex
		`last\s+day|past\s+day|yesterday|1\s*d`:              "1d",
		`last\s+(\d+)\s*days?|past\s+(\d+)\s*days?`:         "",  // Will be handled by regex
		`last\s+week|past\s+week|7\s*d`:                      "7d",
		`last\s+(\d+)\s*weeks?|past\s+(\d+)\s*weeks?`:       "",  // Will be handled by regex
		`last\s+month|past\s+month|30\s*d`:                   "30d",
		`last\s+(\d+)\s*months?|past\s+(\d+)\s*months?`:     "",  // Will be handled by regex
		`today`:                                               "today",
		
		// Specific time patterns
		`this\s+hour`:   "1h",
		`this\s+day`:    "1d", 
		`this\s+week`:   "7d",
		`this\s+month`:  "30d",
	}

	// Check simple patterns first
	for pattern, window := range timePatterns {
		if window != "" {
			if matched, _ := regexp.MatchString(pattern, query); matched {
				return window
			}
		}
	}

	// Handle numbered patterns
	numberedPatterns := []struct {
		pattern string
		unit    string
	}{
		{`last\s+(\d+)\s*hours?|past\s+(\d+)\s*hours?`, "h"},
		{`last\s+(\d+)\s*days?|past\s+(\d+)\s*days?`, "d"},
		{`last\s+(\d+)\s*weeks?|past\s+(\d+)\s*weeks?`, "w"},
		{`last\s+(\d+)\s*months?|past\s+(\d+)\s*months?`, "mo"},
	}

	for _, np := range numberedPatterns {
		re := regexp.MustCompile(np.pattern)
		matches := re.FindStringSubmatch(query)
		if len(matches) > 1 {
			for i := 1; i < len(matches); i++ {
				if matches[i] != "" {
					if num, err := strconv.Atoi(matches[i]); err == nil {
						switch np.unit {
						case "h":
							return fmt.Sprintf("%dh", num)
						case "d":
							return fmt.Sprintf("%dd", num)
						case "w":
							return fmt.Sprintf("%dd", num*7)
						case "mo":
							return fmt.Sprintf("%dd", num*30)
						}
					}
				}
			}
		}
	}

	// Try to parse absolute date ranges
	if window := parseAbsoluteDateRange(query); window != "" {
		return window
	}

	return ""
}

// parseAbsoluteDateRange parses absolute date ranges
func parseAbsoluteDateRange(query string) string {
	// Look for ISO date patterns
	datePattern := `(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?)?)`
	rangePattern := datePattern + `\s*(?:to|,|-)\s*` + datePattern

	re := regexp.MustCompile(rangePattern)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) >= 3 {
		startDate := matches[1]
		endDate := matches[2]
		return fmt.Sprintf("%s,%s", startDate, endDate)
	}

	return ""
}

// parseAggregationLevel extracts aggregation level from natural language
func parseAggregationLevel(query, queryType string) string {
	aggregationPatterns := map[string]map[string]string{
		"allocation": {
			`namespace|namespaces|ns`:                    "namespace",
			`pod|pods`:                                   "pod",
			`container|containers`:                       "container",
			`service|services|svc`:                       "service",
			`deployment|deployments|deploy`:              "deployment",
			`statefulset|statefulsets|sts`:              "statefulset",
			`daemonset|daemonsets|ds`:                   "daemonset",
			`node|nodes`:                                "node",
			`cluster|clusters`:                          "cluster",
			`controller|controllers`:                    "controller",
			`job|jobs`:                                  "job",
			`label|labels`:                              "label",
		},
		"asset": {
			`type|types|asset.type`:                     "type",
			`name|names|asset.name`:                     "name",
			`cluster|clusters`:                          "cluster",
			`node|nodes`:                                "node",
			`provider|providers`:                        "provider",
			`region|regions`:                            "region",
			`zone|zones`:                                "zone",
			`account|accounts`:                          "account",
		},
		"cloudcost": {
			`service|services`:                          "service",
			`provider|providers`:                        "provider",
			`account|accounts`:                          "account",
			`region|regions`:                            "region",
			`usage.type|usagetype`:                      "usagetype",
			`invoice.entity.id|invoiceentityid`:         "invoiceentityid",
		},
	}

	patterns, exists := aggregationPatterns[queryType]
	if !exists {
		return ""
	}

	// Look for aggregation keywords
	for pattern, aggregate := range patterns {
		// Create regex to match "by X" or "group by X" patterns
		byPattern := fmt.Sprintf(`(?:by|group\s+by)\s+(?:%s)`, pattern)
		if matched, _ := regexp.MatchString(byPattern, query); matched {
			return aggregate
		}
		
		// Also check for direct mentions
		if matched, _ := regexp.MatchString(pattern, query); matched {
			return aggregate
		}
	}

	return ""
}

// parseFilters extracts general filters from natural language
func parseFilters(query string) []string {
	var filters []string

	// Namespace filters
	namespaceFilters := parseNamespaceFilters(query)
	filters = append(filters, namespaceFilters...)

	// Label filters
	labelFilters := parseLabelFilters(query)
	filters = append(filters, labelFilters...)

	// Cluster filters
	clusterFilters := parseClusterFilters(query)
	filters = append(filters, clusterFilters...)

	// Service filters
	serviceFilters := parseServiceFilters(query)
	filters = append(filters, serviceFilters...)

	return filters
}

// parseNamespaceFilters extracts namespace-specific filters
func parseNamespaceFilters(query string) []string {
	var filters []string

	// Direct namespace mentions
	if strings.Contains(strings.ToLower(query), "production") || strings.Contains(strings.ToLower(query), "prod") {
		filters = append(filters, `namespace:"production"`)
	}
	if strings.Contains(strings.ToLower(query), "staging") || strings.Contains(strings.ToLower(query), "stage") {
		filters = append(filters, `namespace:"staging"`)
	}
	if strings.Contains(strings.ToLower(query), "development") || strings.Contains(strings.ToLower(query), "dev") {
		filters = append(filters, `namespace:"development"`)
	}

	// Quoted namespace names
	quotedNamespaces := extractQuotedValues(query, []string{"namespace", "ns"})
	for _, ns := range quotedNamespaces {
		filters = append(filters, fmt.Sprintf(`namespace:"%s"`, ns))
	}

	return filters
}

// parseLabelFilters extracts label-based filters
func parseLabelFilters(query string) []string {
	var filters []string

	// Look for label patterns like "app=web" or "env:production"
	labelPatterns := []string{
		`(\w+)=(["\']?)(\w+)\2`,      // app=web or app="web"
		`(\w+):(["\']?)(\w+)\2`,      // app:web or app:"web"
		`label[:\s]+(\w+)=(["\']?)(\w+)\2`, // label app=web
	}

	for _, pattern := range labelPatterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindAllStringSubmatch(query, -1)
		for _, match := range matches {
			if len(match) >= 4 {
				key := match[1]
				value := match[3]
				if match[0] != key+"="+value { // Avoid duplicating the same pattern
					continue
				}
				filters = append(filters, fmt.Sprintf(`label[%s]:"%s"`, key, value))
			}
		}
	}

	return filters
}

// parseClusterFilters extracts cluster-specific filters
func parseClusterFilters(query string) []string {
	quotedClusters := extractQuotedValues(query, []string{"cluster"})
	var filters []string
	for _, cluster := range quotedClusters {
		filters = append(filters, fmt.Sprintf(`cluster:"%s"`, cluster))
	}
	return filters
}

// parseServiceFilters extracts service-specific filters
func parseServiceFilters(query string) []string {
	quotedServices := extractQuotedValues(query, []string{"service", "svc"})
	var filters []string
	for _, service := range quotedServices {
		filters = append(filters, fmt.Sprintf(`service:"%s"`, service))
	}
	return filters
}

// parseAssetFilters extracts asset-specific filters
func parseAssetFilters(query string) []string {
	var filters []string

	// Asset type filters
	queryLower := strings.ToLower(query)
	if strings.Contains(queryLower, "node") || strings.Contains(queryLower, "nodes") {
		filters = append(filters, `type:"Node"`)
	}
	if strings.Contains(queryLower, "disk") || strings.Contains(queryLower, "volume") || strings.Contains(queryLower, "storage") {
		filters = append(filters, `type:"Disk"`)
	}
	if strings.Contains(queryLower, "network") || strings.Contains(queryLower, "load balancer") {
		filters = append(filters, `type:"Network"`)
	}

	// Provider filters
	if strings.Contains(queryLower, "aws") || strings.Contains(queryLower, "amazon") {
		filters = append(filters, `provider:"AWS"`)
	}
	if strings.Contains(queryLower, "gcp") || strings.Contains(queryLower, "google") {
		filters = append(filters, `provider:"GCP"`)
	}
	if strings.Contains(queryLower, "azure") || strings.Contains(queryLower, "microsoft") {
		filters = append(filters, `provider:"Azure"`)
	}

	// Region filters
	quotedRegions := extractQuotedValues(query, []string{"region"})
	for _, region := range quotedRegions {
		filters = append(filters, fmt.Sprintf(`region:"%s"`, region))
	}

	return filters
}

// parseCloudCostFilters extracts cloud cost specific filters
func parseCloudCostFilters(query string) []string {
	var filters []string

	// Provider filters
	queryLower := strings.ToLower(query)
	if strings.Contains(queryLower, "aws") || strings.Contains(queryLower, "amazon") {
		filters = append(filters, `provider:"AWS"`)
	}
	if strings.Contains(queryLower, "gcp") || strings.Contains(queryLower, "google") {
		filters = append(filters, `provider:"GCP"`)
	}
	if strings.Contains(queryLower, "azure") || strings.Contains(queryLower, "microsoft") {
		filters = append(filters, `provider:"Azure"`)
	}

	// Service filters
	serviceMap := map[string]string{
		"ec2":         "EC2",
		"s3":          "S3",
		"rds":         "RDS",
		"lambda":      "Lambda",
		"cloudwatch":  "CloudWatch",
		"vpc":         "VPC",
		"compute":     "Compute Engine",
		"storage":     "Cloud Storage",
		"functions":   "Cloud Functions",
	}

	for keyword, service := range serviceMap {
		if strings.Contains(queryLower, keyword) {
			filters = append(filters, fmt.Sprintf(`service:"%s"`, service))
		}
	}

	// Account filters
	quotedAccounts := extractQuotedValues(query, []string{"account", "account.id"})
	for _, account := range quotedAccounts {
		filters = append(filters, fmt.Sprintf(`account:"%s"`, account))
	}

	// Region filters
	quotedRegions := extractQuotedValues(query, []string{"region"})
	for _, region := range quotedRegions {
		filters = append(filters, fmt.Sprintf(`region:"%s"`, region))
	}

	return filters
}

// parseStepSize determines appropriate step size based on window
func parseStepSize(query, window string) string {
	// If explicitly mentioned, use that
	stepPatterns := map[string]string{
		`hourly|1h|hour`:    "1h",
		`daily|1d|day`:      "1d",
		`weekly|7d|week`:    "7d",
		`monthly|30d|month`: "30d",
	}

	for pattern, step := range stepPatterns {
		if matched, _ := regexp.MatchString(pattern, query); matched {
			return step
		}
	}

	// Auto-determine based on window
	if strings.Contains(window, "h") {
		// For hour windows, use smaller steps
		return "1h"
	} else if strings.Contains(window, "d") {
		// Extract number of days
		re := regexp.MustCompile(`(\d+)d`)
		matches := re.FindStringSubmatch(window)
		if len(matches) > 1 {
			if days, err := strconv.Atoi(matches[1]); err == nil {
				if days <= 1 {
					return "1h"
				} else if days <= 7 {
					return "1d"
				} else {
					return "1d"
				}
			}
		}
	}

	return ""
}

// extractQuotedValues extracts quoted values for specific fields
func extractQuotedValues(text string, prefixes []string) []string {
	var values []string
	textLower := strings.ToLower(text)

	for _, prefix := range prefixes {
		patterns := []string{
			prefix + `\s*[:=]\s*"([^"]+)"`,
			prefix + `\s*[:=]\s*'([^']+)'`,
			prefix + `\s+"([^"]+)"`,
			prefix + `\s+'([^']+)'`,
		}

		for _, pattern := range patterns {
			re := regexp.MustCompile(pattern)
			matches := re.FindAllStringSubmatch(textLower, -1)
			for _, match := range matches {
				if len(match) > 1 {
					values = append(values, match[1])
				}
			}
		}
	}

	return values
}

// Additional utility functions

// ValidateTimeWindow validates that a time window is in correct format
func ValidateTimeWindow(window string) error {
	if window == "" {
		return fmt.Errorf("window cannot be empty")
	}

	// Check for special values
	if window == "today" {
		return nil
	}

	// Check for absolute date ranges
	if strings.Contains(window, ",") {
		parts := strings.Split(window, ",")
		if len(parts) != 2 {
			return fmt.Errorf("invalid date range format")
		}
		// Validate dates
		for _, part := range parts {
			if _, err := time.Parse(time.RFC3339, strings.TrimSpace(part)); err != nil {
				if _, err := time.Parse("2006-01-02", strings.TrimSpace(part)); err != nil {
					return fmt.Errorf("invalid date format in range: %s", part)
				}
			}
		}
		return nil
	}

	// Check for relative time format
	re := regexp.MustCompile(`^\d+[hdwm]$`)
	if !re.MatchString(window) {
		return fmt.Errorf("invalid window format: %s", window)
	}

	return nil
}

// ValidateAggregation validates aggregation parameter
func ValidateAggregation(aggregate, queryType string) error {
	validAggregations := map[string][]string{
		"allocation": {"cluster", "node", "namespace", "controllerKind", "controller", "service", "deployment", "statefulset", "daemonset", "job", "pod", "container", "label"},
		"asset":      {"type", "name", "cluster", "node", "namespace", "provider", "account", "region", "zone"},
		"cloudcost":  {"provider", "service", "account", "invoiceentityid", "region", "usagetype"},
	}

	validList, exists := validAggregations[queryType]
	if !exists {
		return fmt.Errorf("unknown query type: %s", queryType)
	}

	for _, valid := range validList {
		if aggregate == valid {
			return nil
		}
	}

	return fmt.Errorf("invalid aggregation '%s' for query type '%s'", aggregate, queryType)
}

// NormalizeFilter normalizes filter expressions
func NormalizeFilter(filter string) string {
	if filter == "" {
		return ""
	}

	// Remove extra whitespace
	filter = strings.TrimSpace(filter)
	
	// Normalize operators
	filter = strings.ReplaceAll(filter, " = ", ":")
	filter = strings.ReplaceAll(filter, "= ", ":")
	filter = strings.ReplaceAll(filter, " =", ":")
	
	return filter
}