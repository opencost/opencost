package pricingmodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// PipelineConfig holds configuration for the pricing model pipeline.
// If nil is passed to NewPipeline, DefaultPipelineConfig is used.
type PipelineConfig struct {
	AppName         string
	AWSRunnerConfig AWSRunnerConfig
}

type AWSRunnerConfig struct {
	Enabled         bool
	RefreshInterval time.Duration
}

func DefaultPipelineConfig(appName string) PipelineConfig {
	return PipelineConfig{
		AppName: appName,
		AWSRunnerConfig: AWSRunnerConfig{
			Enabled:         true,
			RefreshInterval: timeutil.Day,
		},
	}
}
