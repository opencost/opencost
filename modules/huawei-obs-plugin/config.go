package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config is the plugin's on-disk configuration, loaded from the path OpenCost
// passes as the plugin's first CLI argument (see
// pkg/customcost/pipelineservice.go's getRegisteredPlugins, which names this file
// "huaweiobs_config.json" in the configured plugin config directory).
//
// Credentials (HUAWEICLOUD_ACCESS_KEY_ID, HUAWEICLOUD_SECRET_ACCESS_KEY,
// HUAWEICLOUD_DOMAIN_ID, HUAWEICLOUD_PROJECT_ID) are intentionally not part of this
// file -- they are read from the environment (inherited from the OpenCost process
// that launches this plugin as a subprocess), the same convention pkg/cloud/huawei
// uses, so secrets never need to be written to disk in a plugin config file.
type Config struct {
	// Region is the Huawei Cloud region the OBS buckets and BSS pricing queries
	// target, e.g. "la-south-2".
	Region string `json:"region"`
	// Buckets optionally restricts which buckets are priced. If empty, every
	// bucket visible to the configured credentials is priced.
	Buckets []string `json:"buckets,omitempty"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading plugin config %s: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing plugin config %s: %w", path, err)
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("plugin config %s: \"region\" is required", path)
	}
	return &cfg, nil
}
