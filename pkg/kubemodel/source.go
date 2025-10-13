package kubemodel

import (
	"context"

	pb "github.com/opencost/opencost/core/pkg/model/pb"
)

// Source produces a model snapshot for the requested window.
// Implementations may call out to Prometheus, the Kubernetes API, or other
// systems to hydrate the model.
type Source interface {
	ComputeModel(ctx context.Context, window *pb.Window) (*Model, error)
}
