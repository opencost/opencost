package env

import (
	"os"
	"strconv"
	"strings"
)

const (
	// MCP Server Configuration
	MCPEnabledEnvVar       = "OPENCOST_MCP_ENABLED"
	MCPPortEnvVar          = "OPENCOST_MCP_PORT"
	MCPLogLevelEnvVar      = "OPENCOST_MCP_LOG_LEVEL"
	MCPServerNameEnvVar    = "OPENCOST_MCP_SERVER_NAME"
	MCPServerVersionEnvVar = "OPENCOST_MCP_SERVER_VERSION"

	// MCP Features Configuration
	MCPInsightsEnabledEnvVar = "OPENCOST_MCP_INSIGHTS_ENABLED"
	MCPCacheEnabledEnvVar    = "OPENCOST_MCP_CACHE_ENABLED"
	MCPCacheTTLEnvVar        = "OPENCOST_MCP_CACHE_TTL"
	MCPSessionTimeoutEnvVar  = "OPENCOST_MCP_SESSION_TIMEOUT"

	// MCP Analysis Configuration
	MCPCostThresholdEnvVar       = "OPENCOST_MCP_COST_THRESHOLD"
	MCPEfficiencyThresholdEnvVar = "OPENCOST_MCP_EFFICIENCY_THRESHOLD"
	MCPAnomalySensitivityEnvVar  = "OPENCOST_MCP_ANOMALY_SENSITIVITY"

	// MCP Output Configuration
	MCPMaxRecommendationsEnvVar = "OPENCOST_MCP_MAX_RECOMMENDATIONS"
	MCPMaxInsightsEnvVar        = "OPENCOST_MCP_MAX_INSIGHTS"
	MCPDefaultWindowEnvVar      = "OPENCOST_MCP_DEFAULT_WINDOW"
)

// MCPConfig holds all MCP-related configuration
type MCPConfig struct {
	Enabled             bool
	Port                string
	LogLevel            string
	ServerName          string
	ServerVersion       string
	InsightsEnabled     bool
	CacheEnabled        bool
	CacheTTL            int
	SessionTimeout      int
	CostThreshold       float64
	EfficiencyThreshold float64
	AnomalySensitivity  string
	MaxRecommendations  int
	MaxInsights         int
	DefaultWindow       string
}

// GetMCPConfig returns the MCP configuration from environment variables
func GetMCPConfig() MCPConfig {
	return MCPConfig{
		Enabled:             GetMCPEnabled(),
		Port:                GetMCPPort(),
		LogLevel:            GetMCPLogLevel(),
		ServerName:          GetMCPServerName(),
		ServerVersion:       GetMCPServerVersion(),
		InsightsEnabled:     GetMCPInsightsEnabled(),
		CacheEnabled:        GetMCPCacheEnabled(),
		CacheTTL:            GetMCPCacheTTL(),
		SessionTimeout:      GetMCPSessionTimeout(),
		CostThreshold:       GetMCPCostThreshold(),
		EfficiencyThreshold: GetMCPEfficiencyThreshold(),
		AnomalySensitivity:  GetMCPAnomalySensitivity(),
		MaxRecommendations:  GetMCPMaxRecommendations(),
		MaxInsights:         GetMCPMaxInsights(),
		DefaultWindow:       GetMCPDefaultWindow(),
	}
}

// GetMCPEnabled returns true if MCP server should be enabled
func GetMCPEnabled() bool {
	enabled := os.Getenv(MCPEnabledEnvVar)
	if enabled == "" {
		return false // Disabled by default
	}

	enabledBool, err := strconv.ParseBool(enabled)
	if err != nil {
		return false
	}

	return enabledBool
}

// GetMCPPort returns the port for the MCP server
func GetMCPPort() string {
	port := os.Getenv(MCPPortEnvVar)
	if port == "" {
		return "9004" // Default MCP port
	}
	return port
}

// GetMCPLogLevel returns the log level for MCP operations
func GetMCPLogLevel() string {
	logLevel := os.Getenv(MCPLogLevelEnvVar)
	if logLevel == "" {
		return "info" // Default log level
	}

	// Validate log level
	switch strings.ToLower(logLevel) {
	case "debug", "info", "warn", "warning", "error", "fatal":
		return strings.ToLower(logLevel)
	default:
		return "info"
	}
}

// GetMCPServerName returns the MCP server name
func GetMCPServerName() string {
	name := os.Getenv(MCPServerNameEnvVar)
	if name == "" {
		return "opencost-mcp"
	}
	return name
}

// GetMCPServerVersion returns the MCP server version
func GetMCPServerVersion() string {
	version := os.Getenv(MCPServerVersionEnvVar)
	if version == "" {
		return "1.0.0"
	}
	return version
}

// GetMCPInsightsEnabled returns true if AI insights should be enabled
func GetMCPInsightsEnabled() bool {
	enabled := os.Getenv(MCPInsightsEnabledEnvVar)
	if enabled == "" {
		return true // Enabled by default
	}

	enabledBool, err := strconv.ParseBool(enabled)
	if err != nil {
		return true
	}

	return enabledBool
}

// GetMCPCacheEnabled returns true if caching should be enabled
func GetMCPCacheEnabled() bool {
	enabled := os.Getenv(MCPCacheEnabledEnvVar)
	if enabled == "" {
		return true // Enabled by default
	}

	enabledBool, err := strconv.ParseBool(enabled)
	if err != nil {
		return true
	}

	return enabledBool
}

// GetMCPCacheTTL returns the cache TTL in seconds
func GetMCPCacheTTL() int {
	ttl := os.Getenv(MCPCacheTTLEnvVar)
	if ttl == "" {
		return 300 // 5 minutes default
	}

	ttlInt, err := strconv.Atoi(ttl)
	if err != nil || ttlInt < 0 {
		return 300
	}

	return ttlInt
}

// GetMCPSessionTimeout returns the session timeout in seconds
func GetMCPSessionTimeout() int {
	timeout := os.Getenv(MCPSessionTimeoutEnvVar)
	if timeout == "" {
		return 3600 // 1 hour default
	}

	timeoutInt, err := strconv.Atoi(timeout)
	if err != nil || timeoutInt < 0 {
		return 3600
	}

	return timeoutInt
}

// GetMCPCostThreshold returns the cost threshold for alerts
func GetMCPCostThreshold() float64 {
	threshold := os.Getenv(MCPCostThresholdEnvVar)
	if threshold == "" {
		return 1000.0 // $1000 default
	}

	thresholdFloat, err := strconv.ParseFloat(threshold, 64)
	if err != nil || thresholdFloat < 0 {
		return 1000.0
	}

	return thresholdFloat
}

// GetMCPEfficiencyThreshold returns the efficiency threshold (0-1)
func GetMCPEfficiencyThreshold() float64 {
	threshold := os.Getenv(MCPEfficiencyThresholdEnvVar)
	if threshold == "" {
		return 0.7 // 70% default
	}

	thresholdFloat, err := strconv.ParseFloat(threshold, 64)
	if err != nil || thresholdFloat < 0 || thresholdFloat > 1 {
		return 0.7
	}

	return thresholdFloat
}

// GetMCPAnomalySensitivity returns the anomaly detection sensitivity
func GetMCPAnomalySensitivity() string {
	sensitivity := os.Getenv(MCPAnomalySensitivityEnvVar)
	if sensitivity == "" {
		return "medium"
	}

	// Validate sensitivity level
	switch strings.ToLower(sensitivity) {
	case "low", "medium", "high":
		return strings.ToLower(sensitivity)
	default:
		return "medium"
	}
}

// GetMCPMaxRecommendations returns the maximum number of recommendations to return
func GetMCPMaxRecommendations() int {
	max := os.Getenv(MCPMaxRecommendationsEnvVar)
	if max == "" {
		return 10 // Default max recommendations
	}

	maxInt, err := strconv.Atoi(max)
	if err != nil || maxInt < 1 {
		return 10
	}

	// Cap at reasonable limit
	if maxInt > 50 {
		return 50
	}

	return maxInt
}

// GetMCPMaxInsights returns the maximum number of insights to return
func GetMCPMaxInsights() int {
	max := os.Getenv(MCPMaxInsightsEnvVar)
	if max == "" {
		return 15 // Default max insights
	}

	maxInt, err := strconv.Atoi(max)
	if err != nil || maxInt < 1 {
		return 15
	}

	// Cap at reasonable limit
	if maxInt > 100 {
		return 100
	}

	return maxInt
}

// GetMCPDefaultWindow returns the default time window for queries
func GetMCPDefaultWindow() string {
	window := os.Getenv(MCPDefaultWindowEnvVar)
	if window == "" {
		return "7d" // 7 days default
	}

	// Validate common window formats
	validWindows := []string{
		"1h", "6h", "12h", "1d", "2d", "3d", "7d", "14d", "30d", "90d",
		"hour", "day", "week", "month", "quarter",
		"today", "yesterday", "this week", "last week", "this month", "last month",
	}

	windowLower := strings.ToLower(window)
	for _, valid := range validWindows {
		if windowLower == valid {
			return windowLower
		}
	}

	return "7d" // Default if invalid
}

// IsMCPEnabled is a convenience function to check if MCP is enabled
func IsMCPEnabled() bool {
	return GetMCPEnabled()
}

// ValidateMCPConfig validates the MCP configuration and returns any errors
func ValidateMCPConfig(config MCPConfig) []string {
	var errors []string

	// Validate port
	if config.Port != "" {
		port, err := strconv.Atoi(config.Port)
		if err != nil || port < 1 || port > 65535 {
			errors = append(errors, "Invalid MCP port: must be between 1 and 65535")
		}
	}

	// Validate thresholds
	if config.EfficiencyThreshold < 0 || config.EfficiencyThreshold > 1 {
		errors = append(errors, "Invalid efficiency threshold: must be between 0 and 1")
	}

	if config.CostThreshold < 0 {
		errors = append(errors, "Invalid cost threshold: must be positive")
	}

	// Validate timeouts and TTL
	if config.SessionTimeout < 60 {
		errors = append(errors, "Invalid session timeout: must be at least 60 seconds")
	}

	if config.CacheTTL < 10 {
		errors = append(errors, "Invalid cache TTL: must be at least 10 seconds")
	}

	return errors
}

// GetMCPConfigSummary returns a summary of the MCP configuration for logging
func GetMCPConfigSummary(config MCPConfig) map[string]interface{} {
	return map[string]interface{}{
		"enabled":             config.Enabled,
		"port":                config.Port,
		"logLevel":            config.LogLevel,
		"serverName":          config.ServerName,
		"serverVersion":       config.ServerVersion,
		"insightsEnabled":     config.InsightsEnabled,
		"cacheEnabled":        config.CacheEnabled,
		"cacheTTL":            config.CacheTTL,
		"sessionTimeout":      config.SessionTimeout,
		"costThreshold":       config.CostThreshold,
		"efficiencyThreshold": config.EfficiencyThreshold,
		"anomalySensitivity":  config.AnomalySensitivity,
		"maxRecommendations":  config.MaxRecommendations,
		"maxInsights":         config.MaxInsights,
		"defaultWindow":       config.DefaultWindow,
	}
}
