package pricingmodel

import "time"

const defaultRefreshInterval = time.Hour

// PipelineConfig holds configuration for the pricing model pipeline.
// If nil is passed to NewPipeline, DefaultPipelineConfig is used.
type PipelineConfig struct {
	RefreshInterval time.Duration
}

func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		RefreshInterval: defaultRefreshInterval,
	}
}

func resolveConfig(cfg *PipelineConfig) PipelineConfig {
	if cfg == nil {
		return DefaultPipelineConfig()
	}
	return *cfg
}