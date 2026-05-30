package opencost

import (
	"bytes"
	"io"
	"testing"
	"time"
)

type UnmarshalFunc func(BingenUnmarshalable, []byte) error

func RunAllocation_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	// TODO niko
}

func RunAllocationSet_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day * 10)

	for start.Before(end) {
		set0 := GenerateMockAllocationSetClusterIdle(start)

		bytes, err := set0.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to AllocationSet.MarshalBinary: %s", err)
			return
		}

		set1 := new(AllocationSet)
		err = unmarshal(set1, bytes)
		if err != nil {
			t.Fatalf("Failed to AllocationSet.UnmarshalBinary: %s", err)
			return
		}

		for key, alloc := range set1.Allocations {
			other, ok := set0.Allocations[key]
			if !ok {
				t.Fatalf("Failed to match Allocation for key: %s", key)
				return
			}

			if !alloc.Equal(other) {
				t.Fatalf("allocations for key: %s did not match", key)
			}
		}

		start = start.Add(day)
	}
}

func BenchmarkAllocationSetRange_BinaryEncoding(b *testing.B) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AllocationSetRange
	var bs []byte
	var err error

	asr0 = NewAllocationSetRange(
		GenerateMockAllocationSetClusterIdle(startD0),
		GenerateMockAllocationSetClusterIdle(startD1),
		GenerateMockAllocationSetClusterIdle(startD2),
	)

	for it := 0; it < b.N; it++ {
		bs, err = asr0.MarshalBinary()
		if err != nil {
			b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			return
		}

		asr1 = &AllocationSetRange{}
		err = asr1.UnmarshalBinary(bs)
		if err != nil {
			b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			return
		}

		if asr0.Length() != asr1.Length() {
			b.Fatalf("AllocationSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
		}
		if !asr0.Window().Equal(asr1.Window()) {
			b.Fatalf("AllocationSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
		}

		for i, as0 := range asr0.Allocations {
			as1, err := asr1.Get(i)
			if err != nil {
				b.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
			}

			if as0.Length() != as1.Length() {
				b.Fatalf("AllocationSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
			}
			if !as0.Window.Equal(as1.Window) {
				b.Fatalf("AllocationSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
			}

			for k, a0 := range as0.Allocations {
				a1 := as1.Get(k)
				if a1 == nil {
					b.Fatalf("AllocationSetRange.Binary: missing Allocation: %s", a0)
				}

				if !a0.Equal(a1) {
					b.Fatalf("AllocationSetRange.Binary: unequal Allocations \"%s\": expected %s; found %s", k, a0, a1)
				}
			}
		}
	}
}

func RunAllocationSetRange_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AllocationSetRange
	var err error

	asr0 = NewAllocationSetRange(
		GenerateMockAllocationSetClusterIdle(startD0),
		GenerateMockAllocationSetClusterIdle(startD1),
		GenerateMockAllocationSetClusterIdle(startD2),
	)
	asrSets0 := [][]byte{}
	for _, as := range asr0.Allocations {
		bytes, err := as.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to marshal allocation set into []byte: %s", err)
			return
		}
		asrSets0 = append(asrSets0, bytes)
	}

	asrSets1 := []*AllocationSet{}
	for _, bytes := range asrSets0 {
		allocSet := new(AllocationSet)
		err = unmarshal(allocSet, bytes)
		if err != nil {
			t.Fatalf("AllocationSet.Binary: unexpected error: %s", err)
			return
		}
		asrSets1 = append(asrSets1, allocSet)
	}

	asr1 = NewAllocationSetRange(asrSets1...)

	if asr0.Length() != asr1.Length() {
		t.Fatalf("AllocationSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
	}
	if !asr0.Window().Equal(asr1.Window()) {
		t.Fatalf("AllocationSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
	}

	for i, as0 := range asr0.Allocations {
		as1, err := asr1.Get(i)
		if err != nil {
			t.Fatalf("AllocationSetRange.Binary: unexpected error: %s", err)
		}

		if as0.Length() != as1.Length() {
			t.Fatalf("AllocationSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
		}
		if !as0.Window.Equal(as1.Window) {
			t.Fatalf("AllocationSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
		}

		for k, a0 := range as0.Allocations {
			a1 := as1.Get(k)
			if a1 == nil {
				t.Fatalf("AllocationSetRange.Binary: missing Allocation: %s", a0)
			}

			// TODO Sean: fix JSON marshaling of PVs
			a1.PVs = a0.PVs
			if !a0.Equal(a1) {
				t.Fatalf("AllocationSetRange.Binary: unequal Allocations \"%s\": expected \"%s\"; found \"%s\"", k, a0, a1)
			}
		}
	}
}

func RunAny_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	start := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := NewWindow(&start, &end)

	var a0, a1 *Any
	var bs []byte
	var err error

	a0 = NewAsset(*window.start, *window.end, window)
	a0.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "世界",
	})
	a0.Cost = 123.45
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Any.Binary: unexpected error: %s", err)
	}

	a1 = &Any{}
	err = unmarshal(a1, bs)
	if err != nil {
		t.Fatalf("Any.Binary: unexpected error: %s", err)
	}

	if a1.Properties.Name != a0.Properties.Name {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.Name, a1.Properties.Name)
	}
	if a1.Properties.Cluster != a0.Properties.Cluster {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.Cluster, a1.Properties.Cluster)
	}
	if a1.Properties.ProviderID != a0.Properties.ProviderID {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Properties.ProviderID, a1.Properties.ProviderID)
	}
	if a1.Adjustment != a0.Adjustment {
		t.Fatalf("Any.Binary: expected %f, found %f", a0.Adjustment, a1.Adjustment)
	}
	if a1.TotalCost() != a0.TotalCost() {
		t.Fatalf("Any.Binary: expected %f, found %f", a0.TotalCost(), a1.TotalCost())
	}
	if !a1.Window.Equal(a0.Window) {
		t.Fatalf("Any.Binary: expected %s, found %s", a0.Window, a1.Window)
	}
}

func RunAsset_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	// TODO niko
}

func RunAssetSet_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	// TODO niko
}

func RunAssetSetRange_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	var asr0, asr1 *AssetSetRange
	var bs []byte
	var err error

	asr0 = NewAssetSetRange(
		GenerateMockAssetSet(startD0, day),
		GenerateMockAssetSet(startD1, day),
		GenerateMockAssetSet(startD2, day),
	)

	bs, err = asr0.MarshalBinary()
	if err != nil {
		t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		return
	}

	asr1 = &AssetSetRange{}
	err = unmarshal(asr1, bs)
	if err != nil {
		t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		return
	}

	if asr0.Length() != asr1.Length() {
		t.Fatalf("AssetSetRange.Binary: expected %d; found %d", asr0.Length(), asr1.Length())
	}
	if !asr0.Window().Equal(asr1.Window()) {
		t.Fatalf("AssetSetRange.Binary: expected %s; found %s", asr0.Window(), asr1.Window())
	}

	for i, as0 := range asr0.Assets {
		as1, err := asr1.Get(i)
		if err != nil {
			t.Fatalf("AssetSetRange.Binary: unexpected error: %s", err)
		}

		if as0.Length() != as1.Length() {
			t.Fatalf("AssetSetRange.Binary: expected %d; found %d", as0.Length(), as1.Length())
		}
		if !as0.Window.Equal(as1.Window) {
			t.Fatalf("AssetSetRange.Binary: expected %s; found %s", as0.Window, as1.Window)
		}

		for k, a0 := range as0.Assets {
			a1, ok := as1.Get(k)
			if !ok {
				t.Fatalf("AssetSetRange.Binary: missing Asset: %s", a0)
			}

			if !a0.Equal(a1) {
				t.Fatalf("AssetSetRange.Binary: unequal Assets \"%s\": expected %s; found %s", k, a0, a1)
			}
		}
	}
}

func RunBreakdown_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	var b0, b1 *Breakdown
	var bs []byte
	var err error

	b0 = &Breakdown{
		Idle:   0.75,
		Other:  0.1,
		System: 0.0,
		User:   0.15,
	}

	bs, err = b0.MarshalBinary()
	if err != nil {
		t.Fatalf("Breakdown.Binary: unexpected error: %s", err)
	}

	b1 = &Breakdown{}
	err = unmarshal(b1, bs)
	if err != nil {
		t.Fatalf("Breakdown.Binary: unexpected error: %s", err)
	}

	if b1.Idle != b0.Idle {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.Idle, b1.Idle)
	}
	if b1.Other != b0.Other {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.Other, b1.Other)
	}
	if b1.System != b0.System {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.System, b1.System)
	}
	if b1.User != b0.User {
		t.Fatalf("Breakdown.Binary: expected %f, found %f", b0.User, b1.User)
	}
}

func RunCloudAny_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *Cloud
	var bs []byte
	var err error

	a0 = NewCloud(ComputeCategory, "providerid1", *window.start, *window.end, window)
	a0.Cost = 6.09
	a0.SetAdjustment(-1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("CloudAny.Binary: unexpected error: %s", err)
	}

	a1 = &Cloud{}
	err = unmarshal(a1, bs)
	if err != nil {
		t.Fatalf("CloudAny.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("CloudAny.Binary: expected %v, found %v", a0, a1)
	}
}

func RunClusterManagement_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *ClusterManagement
	var bs []byte
	var err error

	a0 = NewClusterManagement(AWSProvider, "cluster1", window)
	a0.Cost = 4.003
	a0.SetAdjustment(-3.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("ClusterManagement.Binary: unexpected error: %s", err)
	}

	a1 = &ClusterManagement{}
	err = a1.UnmarshalBinary(bs)
	if err != nil {
		t.Fatalf("ClusterManagement.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("ClusterManagement.Binary: expected %v, found %v", a0, a1)
	}
}

func RunDisk_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)
	hours := window.Duration().Hours()

	start := time.Date(2020, time.September, 16, 3, 0, 0, 0, time.UTC)
	end := time.Date(2020, time.September, 16, 15, 12, 0, 0, time.UTC)

	var a0, a1 *Disk
	var bs []byte
	var err error

	a0 = NewDisk("any1", "cluster1", "世界", start, end, window)
	a0.ByteHours = 100 * 1024 * 1024 * 1024 * hours
	a0.Cost = 4.003
	a0.Local = 0.4
	a0.Breakdown = &Breakdown{
		Idle:   0.9,
		Other:  0.05,
		System: 0.05,
		User:   0.0,
	}
	a0.SetAdjustment(-3.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Disk.Binary: unexpected error: %s", err)
	}

	a1 = &Disk{}
	err = unmarshal(a1, bs)
	if err != nil {
		t.Fatalf("Disk.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("Disk.Binary: expected %v, found %v", a0, a1)
	}
}

func RunNode_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)
	hours := window.Duration().Hours()

	start := time.Date(2020, time.September, 16, 3, 0, 0, 0, time.UTC)
	end := time.Date(2020, time.September, 16, 15, 12, 0, 0, time.UTC)

	var a0, a1 *Node
	var bs []byte
	var err error

	a0 = NewNode("any1", "cluster1", "世界", start, end, window)
	a0.NodeType = "n2-standard"
	a0.Preemptible = 1.0
	a0.CPUCoreHours = 2.0 * hours
	a0.RAMByteHours = 12.0 * gb * hours
	a0.CPUCost = 1.50
	a0.GPUCost = 30.44
	a0.RAMCost = 15.0
	a0.Discount = 0.9
	a0.CPUBreakdown = &Breakdown{
		Idle:   0.9,
		Other:  0.05,
		System: 0.05,
		User:   0.0,
	}
	a0.RAMBreakdown = &Breakdown{
		Idle:   0.4,
		Other:  0.05,
		System: 0.05,
		User:   0.5,
	}
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("Node.Binary: unexpected error: %s", err)
	}

	a1 = &Node{}
	err = unmarshal(a1, bs)
	if err != nil {
		t.Fatalf("Node.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("Node.Binary: expected %v, found %v", a0, a1)
	}
}

func RunProperties_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	var p0, p1 *AllocationProperties
	var bs []byte
	var err error

	// empty properties
	p0 = &AllocationProperties{}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = unmarshal(p1, bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}

	// complete properties
	p0 = &AllocationProperties{}
	p0.Cluster = "cluster1"
	p0.Container = "container-abc-1"
	p0.Controller = "daemonset-abc"
	p0.ControllerKind = "daemonset"
	p0.Namespace = "namespace1"
	p0.NamespaceLabels = map[string]string{
		"app":                "cost-analyzer-namespace",
		"kubernetes.io/name": "cost-analyzer",
	}
	p0.NamespaceAnnotations = map[string]string{
		"com.kubernetes.io/managed-by":             "helm",
		"kubernetes.io/last-applied-configuration": "cost-analyzer",
	}
	p0.Node = "node1"
	p0.Pod = "daemonset-abc-123"
	p0.Labels = map[string]string{
		"app":  "cost-analyzer",
		"tier": "frontend",
	}
	p0.Services = []string{"kubecost-frontend"}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = unmarshal(p1, bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}

	// incomplete properties
	p0 = &AllocationProperties{}
	p0.Cluster = ("cluster1")
	p0.Controller = "daemonset-abc"
	p0.ControllerKind = "daemonset"
	p0.Namespace = "namespace1"
	p0.NamespaceAnnotations = map[string]string{
		"com.kubernetes.io/managed-by":             "helm",
		"kubernetes.io/last-applied-configuration": "cost-analyzer",
	}
	p0.Services = []string{}
	bs, err = p0.MarshalBinary()
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	p1 = &AllocationProperties{}
	err = unmarshal(p1, bs)
	if err != nil {
		t.Fatalf("AllocationProperties.Binary: unexpected error: %s", err)
	}

	if !p0.Equal(p1) {
		t.Fatalf("AllocationProperties.Binary: expected %s; found %s", p0, p1)
	}
}

func RunShared_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	ws := time.Date(2020, time.September, 16, 0, 0, 0, 0, time.UTC)
	we := ws.Add(24 * time.Hour)
	window := NewWindow(&ws, &we)

	var a0, a1 *SharedAsset
	var bs []byte
	var err error

	a0 = NewSharedAsset("any1", window)
	a0.Cost = 4.04
	a0.SetAdjustment(1.23)

	bs, err = a0.MarshalBinary()
	if err != nil {
		t.Fatalf("SharedAsset.Binary: unexpected error: %s", err)
	}

	a1 = &SharedAsset{}
	err = unmarshal(a1, bs)
	if err != nil {
		t.Fatalf("SharedAsset.Binary: unexpected error: %s", err)
	}

	if !a0.Equal(a1) {
		t.Fatalf("SharedAsset.Binary: expected %v, found %v", a0, a1)
	}
}

func RunWindow_BinaryEncodingTest(t *testing.T, unmarshal UnmarshalFunc) {
	var w0, w1 Window
	var bs []byte
	var err error

	// Window (nil, nil)
	w0 = NewWindow(nil, nil)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = unmarshal(&w1, bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if w1.Start() != w0.Start() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if w1.End() != w0.End() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (time, nil)
	ts := time.Now()
	w0 = NewWindow(&ts, nil)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = unmarshal(&w1, bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if !w1.Start().Equal(*w0.Start()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if w1.End() != w0.End() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (nil, time)
	te := time.Now()
	w0 = NewWindow(nil, &te)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = unmarshal(&w1, bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if w1.Start() != w0.Start() {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if !w1.End().Equal(*w0.End()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}

	// Window (time, time)
	ts, te = time.Now(), time.Now()
	w0 = NewWindow(&ts, &te)
	bs, err = w0.MarshalBinary()
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	err = unmarshal(&w1, bs)
	if err != nil {
		t.Fatalf("Window.Binary: unexpected error: %s", err)
	}

	if !w1.Start().Equal(*w0.Start()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.Start(), w1.Start())
	}
	if !w1.End().Equal(*w0.End()) {
		t.Fatalf("Window.Binary: expected %v; found %v", w0.End(), w1.End())
	}
}

type BingenUnmarshalable interface {
	UnmarshalBinary([]byte) error
	UnmarshalBinaryFromReader(io.Reader) error
}

func UnmarshalBingenBytes(value BingenUnmarshalable, b []byte) error {
	return value.UnmarshalBinary(b)
}

func UnmarshalBingenReader(value BingenUnmarshalable, b []byte) error {
	// convert bytes to reader in order to leverage io.Reader string table
	reader := bytes.NewReader(b)
	return value.UnmarshalBinaryFromReader(reader)
}

func RunAllOpencostBingenCodecTests(t *testing.T, unmarshal UnmarshalFunc) {
	tests := []struct {
		name string
		f    func(*testing.T, UnmarshalFunc)
	}{
		{
			name: "RunAllocation_BinaryEncodingTest",
			f:    RunAllocation_BinaryEncodingTest,
		},
		{
			name: "RunAllocationSet_BinaryEncodingTest",
			f:    RunAllocationSet_BinaryEncodingTest,
		},
		{
			name: "RunAllocationSetRange_BinaryEncodingTest",
			f:    RunAllocationSetRange_BinaryEncodingTest,
		},
		{
			name: "RunAny_BinaryEncodingTest",
			f:    RunAny_BinaryEncodingTest,
		},
		{
			name: "RunAsset_BinaryEncodingTest",
			f:    RunAsset_BinaryEncodingTest,
		},
		{
			name: "RunAssetSet_BinaryEncodingTest",
			f:    RunAssetSet_BinaryEncodingTest,
		},
		{
			name: "RunAssetSetRange_BinaryEncodingTest",
			f:    RunAssetSetRange_BinaryEncodingTest,
		},
		{
			name: "RunBreakdown_BinaryEncodingTest",
			f:    RunBreakdown_BinaryEncodingTest,
		},
		{
			name: "RunCloudAny_BinaryEncodingTest",
			f:    RunCloudAny_BinaryEncodingTest,
		},
		{
			name: "RunClusterManagement_BinaryEncodingTest",
			f:    RunClusterManagement_BinaryEncodingTest,
		},
		{
			name: "RunDisk_BinaryEncodingTest",
			f:    RunDisk_BinaryEncodingTest,
		},
		{
			name: "RunNode_BinaryEncodingTest",
			f:    RunNode_BinaryEncodingTest,
		},
		{
			name: "RunProperties_BinaryEncodingTest",
			f:    RunProperties_BinaryEncodingTest,
		},
		{
			name: "RunShared_BinaryEncodingTest",
			f:    RunShared_BinaryEncodingTest,
		},
		{
			name: "RunWindow_BinaryEncodingTest",
			f:    RunWindow_BinaryEncodingTest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			test.f(tt, unmarshal)
		})
	}
}

func TestOpencostBingenDefaultsWithBytes(t *testing.T) {
	config := DefaultBingenConfiguration()
	ConfigureBingen(config)

	RunAllOpencostBingenCodecTests(t, UnmarshalBingenBytes)
}

func TestOpencostBingenFileStringTableEnabledWithBytes(t *testing.T) {
	// This test _should_ still run the slice based string table because raw []byte
	// data always uses the string slice table
	config := DefaultBingenConfiguration()
	config.FileBackedStringTableEnabled = true
	config.FileBackedStringTableDir = t.TempDir()
	ConfigureBingen(config)

	// reset configuration to default on completion
	defer ConfigureBingen(DefaultBingenConfiguration())

	RunAllOpencostBingenCodecTests(t, UnmarshalBingenBytes)
}

func TestOpencostBingenDefaultsWithReader(t *testing.T) {
	// This test _should_ still run the slice based string table because we haven't configured
	// bingen to use the file string table
	config := DefaultBingenConfiguration()
	ConfigureBingen(config)

	// we use the reader to unmarshal instead of []bytes
	RunAllOpencostBingenCodecTests(t, UnmarshalBingenReader)
}

func TestOpencostBingenFileStringTableEnabledWithReader(t *testing.T) {
	// This test _should_ use the file backed string table because we have enabled it AND
	// we're using a reader
	config := DefaultBingenConfiguration()
	config.FileBackedStringTableEnabled = true
	config.FileBackedStringTableDir = t.TempDir()
	ConfigureBingen(config)

	// reset configuration to default on completion
	defer ConfigureBingen(DefaultBingenConfiguration())

	RunAllOpencostBingenCodecTests(t, UnmarshalBingenReader)
}
