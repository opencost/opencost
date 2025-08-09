//go:build !dm2emitter

package main

import (
	"context"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
)

// startDM2Emitter is a no-op in normal builds (without dm2emitter build tag)
func startDM2Emitter(ctx context.Context, cache clustercache.ClusterCache, clusterInfo clusters.ClusterInfoProvider) {
	// no-op in normal builds
}