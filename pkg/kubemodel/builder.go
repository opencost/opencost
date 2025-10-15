package kubemodel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ModelHydrator is a function that hydrates a model using data from an OpenCostDataSource.
// It takes the model, datasource, start time, and end time, and populates the model's resources.
type ModelHydrator func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error

var (
	ErrNoDataSource = errors.New("kubemodel: no datasource configured")
	ErrNoHydrators  = errors.New("kubemodel: no hydrators configured")
)

// Builder uses an OpenCostDataSource and hydrators to assemble a complete model.
type Builder struct {
	datasource source.OpenCostDataSource
	hydrators  []ModelHydrator

	// Cluster metadata
	clusterID   string
	clusterName string
	account     string
	provider    kubepb.Provider
}

// Config contains the configuration for creating a Builder.
type Config struct {
	DataSource source.OpenCostDataSource
	Hydrators  []ModelHydrator

	ClusterID   string
	ClusterName string
	Account     string
	Provider    kubepb.Provider
}

// NewBuilder creates a new Builder with the given configuration.
// If ClusterID is not provided, it will attempt to fetch the kube-system namespace UID
// as a default, which is unique per cluster and stable across its lifetime.
func NewBuilder(cfg Config) (*Builder, error) {
	if cfg.DataSource == nil {
		return nil, ErrNoDataSource
	}
	if len(cfg.Hydrators) == 0 {
		return nil, ErrNoHydrators
	}

	// If ClusterID not provided, fetch kube-system namespace UID as default
	clusterID := cfg.ClusterID
	if clusterID == "" {
		uid, err := getKubeSystemNamespaceUID(cfg.DataSource)
		if err != nil {
			return nil, fmt.Errorf("kubemodel: cluster ID not provided and failed to get kube-system namespace UID: %w", err)
		}
		clusterID = uid
	}

	if cfg.ClusterName == "" {
		return nil, fmt.Errorf("kubemodel: cluster name must be provided")
	}

	return &Builder{
		datasource:  cfg.DataSource,
		hydrators:   cfg.Hydrators,
		clusterID:   clusterID,
		clusterName: cfg.ClusterName,
		account:     cfg.Account,
		provider:    cfg.Provider,
	}, nil
}

// getKubeSystemNamespaceUID fetches the UID of the kube-system namespace from the datasource.
// This UID is unique per cluster and stable across its lifetime, making it a good default cluster ID.
func getKubeSystemNamespaceUID(ds source.OpenCostDataSource) (string, error) {
	metrics := ds.Metrics()

	// Query namespace labels from the last hour to find kube-system
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	future := metrics.QueryNamespaceLabels(start, end)
	results, err := future.Await()
	if err != nil {
		return "", fmt.Errorf("failed to query namespace labels: %w", err)
	}

	// Find kube-system namespace and return its UID
	for _, ns := range results {
		if ns.Namespace == "kube-system" && ns.UID != "" {
			return ns.UID, nil
		}
	}

	return "", fmt.Errorf("kube-system namespace UID not found in metrics")
}

// ComputeModel builds a model for the supplied time window using the datasource and hydrators.
func (b *Builder) ComputeModel(ctx context.Context, start, end time.Time) (*Model, error) {
	if b == nil {
		return nil, fmt.Errorf("kubemodel: builder is nil")
	}
	if start.IsZero() {
		return nil, fmt.Errorf("kubemodel: start time must be set")
	}
	if end.IsZero() {
		return nil, fmt.Errorf("kubemodel: end time must be set")
	}
	if end.Before(start) {
		return nil, fmt.Errorf("kubemodel: end %s is before start %s", end, start)
	}

	start = start.UTC()
	end = end.UTC()

	resolution, err := DurationToResolution(end.Sub(start))
	if err != nil {
		return nil, err
	}

	window := &pb.Window{
		Resolution: resolution,
		Start:      timestamppb.New(start),
	}

	model := NewModel()
	model.Window = proto.Clone(window).(*pb.Window)
	model.Cluster = &kubepb.Cluster{
		ID:       b.clusterID,
		Provider: b.provider,
		Account:  b.account,
		Name:     b.clusterName,
		Window:   proto.Clone(window).(*pb.Window),
	}

	// Run all hydrators to populate the model
	for _, hydrator := range b.hydrators {
		if err := hydrator(ctx, model, b.datasource, start, end); err != nil {
			return nil, fmt.Errorf("kubemodel: hydrator failed: %w", err)
		}
	}

	return model, nil
}
