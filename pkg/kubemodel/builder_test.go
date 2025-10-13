package kubemodel

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
)

type fakeSource struct {
	model  *Model
	err    error
	window *pb.Window
}

func (f *fakeSource) ComputeModel(_ context.Context, window *pb.Window) (*Model, error) {
	f.window = window
	if f.err != nil {
		return nil, f.err
	}
	return f.model, nil
}

func TestNewBuilderValidation(t *testing.T) {
	if _, err := NewBuilder(); !errors.Is(err, ErrNoSources) {
		t.Fatalf("expected ErrNoSources, got %v", err)
	}

	var nilSource Source
	builder, err := NewBuilder(nilSource, &fakeSource{model: NewModel()})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(builder.sources) != 1 {
		t.Fatalf("nil sources should be filtered, got %d sources", len(builder.sources))
	}
}

func TestBuilderComputeModel(t *testing.T) {
	first := NewModel()
	first.Nodes["node-1"] = &kubepb.Node{ID: "node-1", Name: "first"}

	second := NewModel()
	second.Nodes["node-2"] = &kubepb.Node{ID: "node-2", Name: "second"}

	src1 := &fakeSource{model: first}
	src2 := &fakeSource{model: second}
	builder, err := NewBuilder(
		src1,
		src2,
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	ctx := context.Background()
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	model, err := builder.ComputeModel(ctx, start, end)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if model.Window == nil {
		t.Fatal("expected model window to be set")
	}
	if model.Window.Resolution != pb.Resolution_RESOLUTION_1H {
		t.Fatalf("unexpected resolution: %v", model.Window.Resolution)
	}
	if !model.Window.Start.AsTime().Equal(start) {
		t.Fatalf("expected window start %v, got %v", start, model.Window.Start.AsTime())
	}
	if len(model.Nodes) != 2 {
		t.Fatalf("expected merged nodes, got %d", len(model.Nodes))
	}
	for i, src := range []*fakeSource{src1, src2} {
		if src.window == nil {
			t.Fatalf("source %d did not receive window", i)
		}
		if src.window.Resolution != pb.Resolution_RESOLUTION_1H {
			t.Fatalf("source %d saw unexpected resolution %v", i, src.window.Resolution)
		}
		if !src.window.Start.AsTime().Equal(start) {
			t.Fatalf("source %d saw unexpected start %v", i, src.window.Start.AsTime())
		}
	}
	if src1.window == src2.window {
		t.Fatal("sources should receive distinct window instances")
	}
}

func TestBuilderComputeModelSourceError(t *testing.T) {
	builder, err := NewBuilder(&fakeSource{err: errors.New("boom")})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	_, err = builder.ComputeModel(context.Background(), time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("expected error from source")
	}
}

func TestBuilderComputeModelValidation(t *testing.T) {
	builder, err := NewBuilder(&fakeSource{model: NewModel()})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	end := time.Now()
	if _, err := builder.ComputeModel(context.Background(), time.Time{}, end); err == nil {
		t.Fatal("expected error when start is zero")
	}
	if _, err := builder.ComputeModel(context.Background(), end, time.Time{}); err == nil {
		t.Fatal("expected error when end is zero")
	}
	if _, err := builder.ComputeModel(context.Background(), end, end.Add(-time.Minute)); err == nil {
		t.Fatal("expected error when end precedes start")
	}
}

func TestBuilderUnsupportedDuration(t *testing.T) {
	builder, err := NewBuilder(&fakeSource{model: NewModel()})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = builder.ComputeModel(context.Background(), start, start.Add(30*time.Minute))
	if err == nil {
		t.Fatal("expected error for unsupported duration")
	}
}

func TestDurationToResolution(t *testing.T) {
	cases := []struct {
		duration   time.Duration
		resolution pb.Resolution
	}{
		{10 * time.Minute, pb.Resolution_RESOLUTION_10M},
		{time.Hour, pb.Resolution_RESOLUTION_1H},
		{24 * time.Hour, pb.Resolution_RESOLUTION_1D},
	}

	for _, tc := range cases {
		got, err := durationToResolution(tc.duration)
		if err != nil {
			t.Fatalf("unexpected err for %v: %v", tc.duration, err)
		}
		if got != tc.resolution {
			t.Fatalf("for %v expected %v, got %v", tc.duration, tc.resolution, got)
		}
	}
}
