//go:build !mcp

package mcp

import "github.com/opencost/opencost/pkg/costmodel"

// Minimal stub to avoid import/use errors when MCP is disabled at build time.
// These functions/types are only referenced where guarded by env.IsMCPEnabled().

type MCPManager struct{}

func NewMCPManager(_ *costmodel.CostModel) *MCPManager { return &MCPManager{} }
func (m *MCPManager) Start() error                     { return nil }
func (m *MCPManager) Stop() error                      { return nil }
func (m *MCPManager) IsRunning() bool                  { return false }
func (m *MCPManager) GetServerInfo() map[string]interface{} {
	return map[string]interface{}{"status": "stopped"}
}

// RegisterShutdownHook is a no-op when MCP is disabled
func (m *MCPManager) RegisterShutdownHook(_ func()) {}

func StartMCPServer(_ *costmodel.CostModel) (*MCPManager, error) { return &MCPManager{}, nil }

type MCPHealthCheck struct{}

func NewMCPHealthCheck(_ *MCPManager) *MCPHealthCheck { return &MCPHealthCheck{} }
func (hc *MCPHealthCheck) GetStatus() map[string]interface{} {
	return map[string]interface{}{"status": "unavailable", "running": false}
}
