package kubemodel

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/opencost/opencost/core/pkg/model/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNoSources = errors.New("kubemodel: no sources configured")
)

// Builder coordinates one or more Sources to assemble a complete model.
type Builder struct {
	sources []Source
}

// NewBuilder wires the provided sources together. At least one source is
// required; each nil entry is ignored so callers can conditionally append.
func NewBuilder(sources ...Source) (*Builder, error) {
	filtered := make([]Source, 0, len(sources))
	for _, src := range sources {
		if src != nil {
			filtered = append(filtered, src)
		}
	}
	if len(filtered) == 0 {
		return nil, ErrNoSources
	}
	return &Builder{
		sources: filtered,
	}, nil
}

// ComputeModel requests each source to compute a model for the supplied window,
// merging the results into a single snapshot.
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

	result := NewModel()
	result.Window = proto.Clone(window).(*pb.Window)
	for _, src := range b.sources {
		sourceWindow := proto.Clone(window).(*pb.Window)
		model, err := src.ComputeModel(ctx, sourceWindow)
		if err != nil {
			return nil, fmt.Errorf("kubemodel: source compute failed: %w", err)
		}
		result.Merge(model)
	}

	return result, nil
}
