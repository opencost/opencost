package kubemodel

import (
	"testing"
	"time"

	pb "github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelClone(t *testing.T) {
	original := NewModel()
	now := time.Now().UTC()
	original.Window = &pb.Window{
		Resolution: pb.Resolution_RESOLUTION_1H,
		Start:      timestamppb.New(now),
	}
	original.Cluster = &kubepb.Cluster{ID: "cluster"}
	original.Nodes["node"] = &kubepb.Node{ID: "node"}

	cloned := original.Clone()
	if cloned == original {
		t.Fatal("expected clone to allocate a new struct")
	}
	if cloned.Cluster == original.Cluster {
		t.Fatal("expected cluster to be deep copied")
	}
	if cloned.Window == original.Window {
		t.Fatal("expected window to be deep copied")
	}
	if cloned.Nodes["node"] == original.Nodes["node"] {
		t.Fatal("expected node map entry to be deep copied")
	}

	cloned.Cluster.ID = "mutated"
	originalStart := original.Window.Start.AsTime()
	cloned.Window.Start = timestamppb.New(originalStart.Add(time.Hour))
	cloned.Nodes["node"].ID = "mutated"
	if original.Cluster.ID != "cluster" || original.Nodes["node"].ID != "node" {
		t.Fatal("mutating clone must not change original")
	}
	if original.Window.Start.AsTime() != originalStart {
		t.Fatal("mutating cloned window should not affect original")
	}
	if cloned.Window.Start.AsTime() != originalStart.Add(time.Hour) {
		t.Fatal("expected cloned window start to change")
	}
	if original.Window.Start.AsTime() == cloned.Window.Start.AsTime() {
		t.Fatal("mutating cloned window should not affect original")
	}
}

func TestModelMerge(t *testing.T) {
	first := NewModel()
	first.Window = &pb.Window{
		Resolution: pb.Resolution_RESOLUTION_1H,
		Start:      timestamppb.New(time.Now().UTC()),
	}
	first.Cluster = &kubepb.Cluster{ID: "first"}
	first.Nodes["node"] = &kubepb.Node{ID: "node", Name: "first"}

	second := NewModel()
	nextStart := time.Now().UTC().Add(time.Hour)
	second.Window = &pb.Window{
		Resolution: pb.Resolution_RESOLUTION_1H,
		Start:      timestamppb.New(nextStart),
	}
	second.Nodes["node"] = &kubepb.Node{ID: "node", Name: "second"}
	second.Namespaces["ns"] = &kubepb.Namespace{ID: "ns"}

	first.Merge(second)

	if first.Window == nil || first.Window.Start.AsTime() != nextStart {
		t.Fatalf("expected window to be overridden, got %#v", first.Window)
	}
	if first.Cluster == nil || first.Cluster.ID != "first" {
		t.Fatalf("expected cluster to remain, got %#v", first.Cluster)
	}
	if got := first.Nodes["node"].Name; got != "second" {
		t.Fatalf("expected node to be overwritten, got %q", got)
	}
	if _, ok := first.Namespaces["ns"]; !ok {
		t.Fatalf("expected namespace from second to be merged")
	}
}
