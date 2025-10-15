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
func NewBuilder(cfg Config) (*Builder, error) {
	if cfg.DataSource == nil {
		return nil, ErrNoDataSource
	}
	if len(cfg.Hydrators) == 0 {
		return nil, ErrNoHydrators
	}
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("kubemodel: cluster ID must be provided")
	}
	if cfg.ClusterName == "" {
		return nil, fmt.Errorf("kubemodel: cluster name must be provided")
	}

	return &Builder{
		datasource:  cfg.DataSource,
		hydrators:   cfg.Hydrators,
		clusterID:   cfg.ClusterID,
		clusterName: cfg.ClusterName,
		account:     cfg.Account,
		provider:    cfg.Provider,
	}, nil
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
