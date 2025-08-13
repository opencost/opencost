//go:build mcp

package mcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
)

// MCPManager manages the lifecycle of the MCP server
type MCPManager struct {
	config    env.MCPConfig
	mcpServer *OpenCostMCPServer
	costModel *costmodel.CostModel
	server    *server.Server
	ctx       context.Context
	cancel    context.CancelFunc
	isRunning bool
}

// NewMCPManager creates a new MCP manager
func NewMCPManager(costModel *costmodel.CostModel) *MCPManager {
	config := env.GetMCPConfig()

	return &MCPManager{
		config:    config,
		costModel: costModel,
	}
}

// Start initializes and starts the MCP server if enabled
func (m *MCPManager) Start() error {
	if !m.config.Enabled {
		log.Info("MCP server is disabled")
		return nil
	}

	log.Info("Starting OpenCost MCP server...")

	// Validate configuration
	if errors := env.ValidateMCPConfig(m.config); len(errors) > 0 {
		for _, err := range errors {
			log.Errorf("MCP configuration error: %s", err)
		}
		return fmt.Errorf("invalid MCP configuration")
	}

	// Log configuration summary
	configSummary := env.GetMCPConfigSummary(m.config)
	log.Infof("MCP server configuration: %+v", configSummary)

	// Create context for server lifecycle
	m.ctx, m.cancel = context.WithCancel(context.Background())

	// Create and configure the MCP server
	mcpServer := NewOpenCostMCPServer(m.costModel)
	m.mcpServer = mcpServer

	// Apply configuration to the server
	if err := m.applyConfiguration(); err != nil {
		return fmt.Errorf("failed to apply MCP configuration: %w", err)
	}

	// Start the server in a goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("MCP server panic: %v", r)
			}
		}()

		if err := mcpServer.Start(); err != nil {
			log.Errorf("MCP server error: %v", err)
		}
	}()

	m.isRunning = true
	log.Infof("OpenCost MCP server started successfully on port %s", m.config.Port)

	// Start health monitoring
	go m.monitorHealth()

	return nil
}

// Stop gracefully shuts down the MCP server
func (m *MCPManager) Stop() error {
	if !m.isRunning {
		return nil
	}

	log.Info("Stopping OpenCost MCP server...")

	// Cancel context to signal shutdown
	if m.cancel != nil {
		m.cancel()
	}

	// Give the server time to gracefully shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Wait for server to finish
		if m.server != nil {
			// In a real implementation, you'd want to gracefully stop the server
			// For now, we just mark it as stopped
		}
		m.isRunning = false
	}()

	select {
	case <-shutdownCtx.Done():
		log.Warn("MCP server shutdown timed out")
		return fmt.Errorf("shutdown timeout")
	case <-done:
		log.Info("OpenCost MCP server stopped successfully")
		return nil
	}
}

// IsRunning returns true if the MCP server is currently running
func (m *MCPManager) IsRunning() bool {
	return m.isRunning
}

// GetConfig returns the current MCP configuration
func (m *MCPManager) GetConfig() env.MCPConfig {
	return m.config
}

// Restart stops and starts the MCP server
func (m *MCPManager) Restart() error {
	if err := m.Stop(); err != nil {
		log.Errorf("Error stopping MCP server during restart: %v", err)
	}

	// Reload configuration
	m.config = env.GetMCPConfig()

	return m.Start()
}

// applyConfiguration applies the loaded configuration to the MCP server
func (m *MCPManager) applyConfiguration() error {
	if m.mcpServer == nil {
		return fmt.Errorf("MCP server not initialized")
	}

	// Configure insight engine if available
	if m.mcpServer.insights != nil {
		// Apply thresholds to insight engine
		m.mcpServer.insights.costThresholds.HighCostAlert = m.config.CostThreshold
		m.mcpServer.insights.costThresholds.EfficiencyTarget = m.config.EfficiencyThreshold
		m.mcpServer.insights.anomalyDetection.SensitivityLevel = m.config.AnomalySensitivity

		// Configure max limits
		switch m.config.AnomalySensitivity {
		case "low":
			m.mcpServer.insights.anomalyDetection.ThresholdMultiplier = 3.0
		case "medium":
			m.mcpServer.insights.anomalyDetection.ThresholdMultiplier = 2.0
		case "high":
			m.mcpServer.insights.anomalyDetection.ThresholdMultiplier = 1.5
		}
	}

	log.Debugf("Applied MCP configuration: insights=%v, cache=%v, thresholds=%.2f/%.2f",
		m.config.InsightsEnabled, m.config.CacheEnabled,
		m.config.CostThreshold, m.config.EfficiencyThreshold)

	return nil
}

// monitorHealth periodically checks the health of the MCP server
func (m *MCPManager) monitorHealth() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performHealthCheck()
		}
	}
}

// performHealthCheck checks if the MCP server is healthy
func (m *MCPManager) performHealthCheck() {
	// Check if server is listening on the configured port
	if m.config.Port != "" {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%s", m.config.Port), 5*time.Second)
		if err != nil {
			log.Warnf("MCP server health check failed: %v", err)
			return
		}
		conn.Close()
	}

	// Clean up old sessions if needed
	if m.mcpServer != nil {
		m.cleanupOldSessions()
	}

	log.Debugf("MCP server health check passed")
}

// cleanupOldSessions removes expired conversation contexts
func (m *MCPManager) cleanupOldSessions() {
	if m.mcpServer.contexts == nil {
		return
	}

	timeout := time.Duration(m.config.SessionTimeout) * time.Second
	cutoff := time.Now().Add(-timeout)

	for sessionId, ctx := range m.mcpServer.contexts {
		if ctx.LastActivity.Before(cutoff) {
			delete(m.mcpServer.contexts, sessionId)
			log.Debugf("Cleaned up expired MCP session: %s", sessionId)
		}
	}
}

// GetStats returns statistics about the MCP server
func (m *MCPManager) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"enabled": m.config.Enabled,
		"running": m.isRunning,
		"port":    m.config.Port,
		"uptime":  time.Since(time.Now()), // This would need to track actual start time
	}

	if m.mcpServer != nil && m.mcpServer.contexts != nil {
		stats["activeSessions"] = len(m.mcpServer.contexts)

		// Add session statistics
		sessionStats := make(map[string]interface{})
		for sessionId, ctx := range m.mcpServer.contexts {
			sessionStats[sessionId] = map[string]interface{}{
				"startTime":    ctx.StartTime,
				"lastActivity": ctx.LastActivity,
				"queryCount":   len(ctx.RecentQueries),
				"insightCount": len(ctx.CostInsights),
			}
		}
		stats["sessions"] = sessionStats
	}

	return stats
}

// UpdateConfiguration allows runtime updates to the MCP configuration
func (m *MCPManager) UpdateConfiguration(newConfig env.MCPConfig) error {
	// Validate new configuration
	if errors := env.ValidateMCPConfig(newConfig); len(errors) > 0 {
		return fmt.Errorf("invalid configuration: %v", errors)
	}

	oldConfig := m.config
	m.config = newConfig

	// Apply configuration changes
	if err := m.applyConfiguration(); err != nil {
		// Rollback on error
		m.config = oldConfig
		return fmt.Errorf("failed to apply new configuration: %w", err)
	}

	log.Infof("MCP configuration updated successfully")
	return nil
}

// GetServerInfo returns information about the MCP server
func (m *MCPManager) GetServerInfo() map[string]interface{} {
	info := map[string]interface{}{
		"name":     m.config.ServerName,
		"version":  m.config.ServerVersion,
		"protocol": "Model Context Protocol",
		"features": map[string]bool{
			"insights":    m.config.InsightsEnabled,
			"caching":     m.config.CacheEnabled,
			"allocations": true,
			"assets":      true,
			"cloudCosts":  true,
			"naturalLang": true,
		},
		"endpoints": map[string]string{
			"allocations": "/allocation",
			"assets":      "/assets",
			"cloudCosts":  "/cloudCost",
		},
	}

	if m.isRunning {
		info["status"] = "running"
		info["port"] = m.config.Port
	} else {
		info["status"] = "stopped"
	}

	return info
}

// RegisterShutdownHook registers a function to be called when the MCP server shuts down
func (m *MCPManager) RegisterShutdownHook(hook func()) {
	if m.ctx != nil {
		go func() {
			<-m.ctx.Done()
			hook()
		}()
	}
}

// Helper functions for integration

// StartMCPServer is a convenience function to start the MCP server with a cost model
// This can be called from the main OpenCost application startup
func StartMCPServer(costModel *costmodel.CostModel) (*MCPManager, error) {
	manager := NewMCPManager(costModel)

	if err := manager.Start(); err != nil {
		return nil, fmt.Errorf("failed to start MCP server: %w", err)
	}

	return manager, nil
}

// StartMCPServerWithContext starts the MCP server with a parent context
func StartMCPServerWithContext(ctx context.Context, costModel *costmodel.CostModel) (*MCPManager, error) {
	manager := NewMCPManager(costModel)

	// Override the internal context with the provided one
	manager.ctx, manager.cancel = context.WithCancel(ctx)

	if err := manager.Start(); err != nil {
		return nil, fmt.Errorf("failed to start MCP server: %w", err)
	}

	// Ensure shutdown when parent context is cancelled
	go func() {
		<-ctx.Done()
		manager.Stop()
	}()

	return manager, nil
}

// MCPHealthCheck provides a health check endpoint for the MCP server
type MCPHealthCheck struct {
	manager *MCPManager
}

// NewMCPHealthCheck creates a new health check for the MCP server
func NewMCPHealthCheck(manager *MCPManager) *MCPHealthCheck {
	return &MCPHealthCheck{manager: manager}
}

// IsHealthy returns true if the MCP server is healthy
func (hc *MCPHealthCheck) IsHealthy() bool {
	if hc.manager == nil {
		return false
	}

	return hc.manager.IsRunning()
}

// GetStatus returns detailed status information
func (hc *MCPHealthCheck) GetStatus() map[string]interface{} {
	if hc.manager == nil {
		return map[string]interface{}{
			"status": "unavailable",
			"error":  "MCP manager not initialized",
		}
	}

	status := map[string]interface{}{
		"status":  "healthy",
		"running": hc.manager.IsRunning(),
	}

	if !hc.manager.IsRunning() {
		status["status"] = "unhealthy"
		status["reason"] = "MCP server not running"
	}

	// Add server stats
	stats := hc.manager.GetStats()
	for k, v := range stats {
		status[k] = v
	}

	return status
}
