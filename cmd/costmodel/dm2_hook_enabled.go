//go:build dm2emitter

package main

import (
	"context"
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/internal/dm2emitter"
)

// startDM2Emitter starts the DM2 emitter if enabled via environment variable
func startDM2Emitter(ctx context.Context, cache clustercache.ClusterCache, clusterInfo clusters.ClusterInfoProvider) {
	if os.Getenv("OPENCOST_DM2_EMITTER") != "on" {
		log.Infof("DM2 emitter is compiled in but not enabled (set OPENCOST_DM2_EMITTER=on to enable)")
		return
	}

	log.Infof("Starting DM2 emitter (UID-first protobuf export)")

	// Get output directory from environment or use default
	outDir := os.Getenv("OPENCOST_DM2_OUTPUT")
	if outDir == "" {
		outDir = "/tmp"
	}

	// Get period from environment or use default
	period := 5 * time.Minute
	if v := os.Getenv("OPENCOST_DM2_PERIOD"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			period = d
		} else {
			log.Warnf("Invalid OPENCOST_DM2_PERIOD value: %s, using default 5m", v)
		}
	}

	// Create inventory adapter using real caches
	inv := dm2emitter.NewKubeInventory(cache, clusterInfo)

	// Start emitter in background
	go func() {
		log.Infof("DM2 emitter starting: output=%s, period=%v", outDir, period)
		if err := dm2emitter.New(inv, outDir, period, false).Start(ctx); err != nil {
			log.Errorf("DM2 emitter error: %v", err)
		}
	}()
}