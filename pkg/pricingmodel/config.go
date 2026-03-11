package pricingmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
)

// PipelineConfig holds configuration for the pricing model pipeline.
type PipelineConfig struct {
	AppName           string
	CurrencyCode      string
	AWSRunnerConfig   AWSRunnerConfig
	AzureRunnerConfig AzureRunnerConfig
	GCPRunnerConfig   GCPRunnerConfig
}

type AWSRunnerConfig struct {
	Enabled         bool
	RefreshInterval time.Duration
}

type AzureRunnerConfig struct {
	Enabled         bool
	RefreshInterval time.Duration
}

type GCPRunnerConfig struct {
	Enabled         bool
	RefreshInterval time.Duration
	APIKey          string
}

func DefaultPipelineConfig(appName string) PipelineConfig {
	return PipelineConfig{
		AppName:      appName,
		CurrencyCode: "USD",
		AWSRunnerConfig: AWSRunnerConfig{
			Enabled:         true,
			RefreshInterval: timeutil.Day,
		},
		AzureRunnerConfig: AzureRunnerConfig{
			Enabled:         true,
			RefreshInterval: timeutil.Day,
		},
		GCPRunnerConfig: GCPRunnerConfig{
			Enabled:         true,
			RefreshInterval: timeutil.Day,
			APIKey:          env.GetCloudProviderAPIKey(),
		},
	}
}
