package costmodel

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
)

// Config contain configuration options that can be passed to the Execute() method
type Config struct {
	Port                   int
	KubernetesEnabled      bool
	CarbonEstimatesEnabled bool
	CloudCostEnabled       bool
	CustomCostEnabled      bool
	MCPServerEnabled       bool
}

func DefaultConfig() *Config {
	return &Config{
		Port:                   env.GetOpencostAPIPort(),
		KubernetesEnabled:      env.IsKubernetesEnabled(),
		CarbonEstimatesEnabled: env.IsCarbonEstimatesEnabled(),
		CloudCostEnabled:       env.IsCloudCostEnabled(),
		MCPServerEnabled:       env.IsMCPServerEnabled(),
		CustomCostEnabled:      env.IsCustomCostEnabled(),
	}
}

func (c *Config) log() {
	log.Infof("Kubernetes enabled: %t", c.KubernetesEnabled)
	log.Infof("Carbon Estimates enabled: %t", c.CarbonEstimatesEnabled)
	log.Infof("Cloud Costs enabled: %t", c.CloudCostEnabled)
	log.Infof("Custom Costs enabled: %t", c.CustomCostEnabled)
	log.Infof("MCP Server enabled: %t", c.MCPServerEnabled)
	log.Infof("Custom Costs enabled: %t", c.CustomCostEnabled)
}
