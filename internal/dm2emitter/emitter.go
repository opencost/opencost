//go:build dm2emitter

package dm2emitter

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	dm2pb "github.com/opencost/opencost/protos/dm2"
	"google.golang.org/protobuf/proto"
)

// Inventory is the minimal interface we need from existing caches.
// Implement a real adapter in adapter.go that pulls from OpenCost's
// current k8s caches/types (namespaces, workloads/controllers, pods, containers).
type Inventory interface {
	ListNamespaces(ctx context.Context) []Namespace
	ListWorkloadsByNamespace(ctx context.Context, nsUID string) []Workload
	ListPodsByWorkload(ctx context.Context, wlUID string) []Pod
	ListContainersByPod(ctx context.Context, podUID string) []Container
	ClusterUID(ctx context.Context) string
	ClusterName(ctx context.Context) string
}

type Namespace struct{ UID, Name string }
type Workload struct{ UID, Name, Kind, NamespaceUID string }
type Pod struct{ UID, Name, NodeUID, WorkloadUID string }
type Container struct{ UID, Name, Image, PodUID string }

type Emitter struct {
	inv      Inventory
	outDir   string
	interval time.Duration
	once     bool // if true, emit once and return
}

func New(inv Inventory, outDir string, interval time.Duration, once bool) *Emitter {
	return &Emitter{inv: inv, outDir: outDir, interval: interval, once: once}
}

func (e *Emitter) Start(ctx context.Context) error {
	if _, err := os.Stat(e.outDir); os.IsNotExist(err) {
		if err := os.MkdirAll(e.outDir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", e.outDir, err)
		}
	}
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		if err := e.emitOnce(ctx); err != nil {
			// log in real impl; don't crash the app
		}
		if e.once {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Emitter) emitOnce(ctx context.Context) error {
	cluster := &dm2pb.Cluster{
		ClusterUid:  e.inv.ClusterUID(ctx),
		ClusterName: e.inv.ClusterName(ctx),
	}
	nsByUID := map[string]*dm2pb.Namespace{}
	for _, ns := range e.inv.ListNamespaces(ctx) {
		nsByUID[ns.UID] = &dm2pb.Namespace{Uid: ns.UID, Name: ns.Name}
		cluster.Namespaces = append(cluster.Namespaces, nsByUID[ns.UID])
	}
	wlByUID := map[string]*dm2pb.Workload{}
	for nsUID := range nsByUID {
		for _, wl := range e.inv.ListWorkloadsByNamespace(ctx, nsUID) {
			w := &dm2pb.Workload{Uid: wl.UID, Name: wl.Name, Kind: wl.Kind}
			nsByUID[nsUID].Workloads = append(nsByUID[nsUID].Workloads, w)
			wlByUID[wl.UID] = w
		}
	}
	podByUID := map[string]*dm2pb.Pod{}
	for wlUID := range wlByUID {
		for _, p := range e.inv.ListPodsByWorkload(ctx, wlUID) {
			pp := &dm2pb.Pod{Uid: p.UID, Name: p.Name, NodeUid: p.NodeUID}
			wlByUID[wlUID].Pods = append(wlByUID[wlUID].Pods, pp)
			podByUID[p.UID] = pp
		}
	}
	for podUID := range podByUID {
		for _, c := range e.inv.ListContainersByPod(ctx, podUID) {
			cc := &dm2pb.Container{Uid: c.UID, Name: c.Name, Image: c.Image}
			podByUID[podUID].Containers = append(podByUID[podUID].Containers, cc)
		}
	}

	b, err := proto.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(b); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}

	filename := filepath.Join(e.outDir, fmt.Sprintf("dm2_%d.pb.gz", time.Now().Unix()))
	return os.WriteFile(filename, gz.Bytes(), 0o644)
}
