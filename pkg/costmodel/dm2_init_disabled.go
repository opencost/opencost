//go:build !dm2emitter

package costmodel

import (
	"context"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
)

// initDM2Emitter is a no-op when compiled without the dm2emitter build tag
func initDM2Emitter(ctx context.Context, cache clustercache.ClusterCache, clusterInfo clusters.ClusterInfoProvider) {
	// No-op when DM2 emitter is not compiled in
}