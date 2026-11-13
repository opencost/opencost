package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// pod describes a running pod's start and end time within a Window and
// all the Allocations (i.e. containers) contained within it.
type pod struct {
	Window      opencost.Window
	Start       time.Time
	End         time.Time
	Key         podKey
	Node        string
	Allocations map[string]*opencost.Allocation
}

func (p *pod) equal(that *pod) bool {
	if p == nil {
		return that == nil
	}

	if !p.Window.Equal(that.Window) {
		return false
	}

	if !p.Start.Equal(that.Start) {
		return false
	}

	if !p.End.Equal(that.End) {
		return false
	}

	if p.Key != that.Key {
		return false
	}

	if p.Key != that.Key {
		return false
	}

	if len(p.Allocations) != len(that.Allocations) {
		return false
	}

	for container, thisAlloc := range p.Allocations {
		thatAlloc, ok := that.Allocations[container]
		if !ok || !thisAlloc.Equal(thatAlloc) {
			return false
		}
	}
	return true
}

// appendContainer adds an entry for the given container name to the pod.
func (p *pod) appendContainer(container string) {
	name := fmt.Sprintf("%s/%s/%s/%s", p.Key.Cluster, p.Key.Namespace, p.Key.Pod, container)

	alloc := &opencost.Allocation{
		Name:       name,
		Properties: &opencost.AllocationProperties{},
		Window:     p.Window.Clone(),
		Start:      p.Start,
		End:        p.End,
	}
	alloc.Properties.Container = container
	alloc.Properties.Pod = p.Key.Pod
	alloc.Properties.Namespace = p.Key.Namespace
	alloc.Properties.Cluster = p.Key.Cluster

	p.Allocations[container] = alloc
}

// pvc describes a PersistentVolumeClaim
// TODO:CLEANUP move to pkg/kubecost?
// TODO:CLEANUP add PersistentVolumeClaims field to type Allocation?
type pvc struct {
	Bytes     float64   `json:"bytes"`
	Name      string    `json:"name"`
	Cluster   string    `json:"cluster"`
	Namespace string    `json:"namespace"`
	Volume    *pv       `json:"persistentVolume"`
	Mounted   bool      `json:"mounted"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
}

// Cost computes the cumulative cost of the pvc
func (p *pvc) Cost() float64 {
	if p == nil || p.Volume == nil {
		return 0.0
	}

	gib := p.Bytes / 1024 / 1024 / 1024
	hrs := p.minutes() / 60.0

	return p.Volume.CostPerGiBHour * gib * hrs
}

// Minutes computes the number of minutes over which the pvc is defined
func (p *pvc) minutes() float64 {
	if p == nil {
		return 0.0
	}

	return p.End.Sub(p.Start).Minutes()
}

// interval returns the observed [Start, End] of the pvc
func (p *pvc) interval() observedInterval {
	if p == nil {
		return observedInterval{}
	}
	return observedInterval{Start: p.Start, End: p.End, Valid: true}
}

// String returns a string representation of the pvc
func (p *pvc) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s/%s{Bytes:%.2f, Cost:%.6f, Start,End:%s}", p.Cluster, p.Namespace, p.Name, p.Bytes, p.Cost(), opencost.NewWindow(&p.Start, &p.End))
}

// Key returns the pvcKey for the calling pvc
func (p *pvc) key() pvcKey {
	return newPVCKey(p.Cluster, p.Namespace, p.Name)
}

// pv describes a PersistentVolume
type pv struct {
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
	Bytes          float64   `json:"bytes"`
	CostPerGiBHour float64   `json:"costPerGiBHour"`
	Cluster        string    `json:"cluster"`
	Name           string    `json:"name"`
	StorageClass   string    `json:"storageClass"`
	ProviderID     string    `json:"providerID"`
}

func (p *pv) clone() *pv {
	if p == nil {
		return nil
	}
	return &pv{
		Start:          p.Start,
		End:            p.End,
		Bytes:          p.Bytes,
		CostPerGiBHour: p.CostPerGiBHour,
		Cluster:        p.Cluster,
		Name:           p.Name,
		StorageClass:   p.StorageClass,
	}
}

func (p *pv) equal(that *pv) bool {
	if p == nil {
		return that == nil
	}

	if !p.Start.Equal(that.Start) {
		return false
	}

	if !p.End.Equal(that.End) {
		return false
	}

	if p.Bytes != that.Bytes {
		return false
	}

	if p.CostPerGiBHour != that.CostPerGiBHour {
		return false
	}

	if p.Cluster != that.Cluster {
		return false
	}

	if p.Name != that.Name {
		return false
	}

	if p.StorageClass != that.StorageClass {
		return false
	}

	return true
}

// String returns a string representation of the pv
func (p *pv) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s{Bytes:%.2f, Cost/GiB*Hr:%.6f, StorageClass:%s, ProviderID: %s}", p.Cluster, p.Name, p.Bytes, p.CostPerGiBHour, p.StorageClass, p.ProviderID)
}

func (p *pv) minutes() float64 {
	if p == nil {
		return 0.0
	}

	return p.End.Sub(p.Start).Minutes()
}

// interval returns the observed [Start, End] of the pv
func (p *pv) interval() observedInterval {
	if p == nil {
		return observedInterval{}
	}
	return observedInterval{Start: p.Start, End: p.End, Valid: true}
}

// lbCost describes the start and end time of a Load Balancer along with cost
type lbCost struct {
	TotalCost float64
	Start     time.Time
	End       time.Time
	Private   bool
	Ip        string
}

// interval returns the observed [Start, End] of the load balancer, with End
// capped at the current time. getLoadBalancerCosts extends End by one
// resolution for interpolation, which can push it past now in an open window.
// l.End itself is left alone because the cost split between allocations uses it.
func (l *lbCost) interval() observedInterval {
	return l.intervalAsOf(time.Now().UTC())
}

// intervalAsOf is interval with an explicit current time.
func (l *lbCost) intervalAsOf(now time.Time) observedInterval {
	if l == nil {
		return observedInterval{}
	}
	end := l.End
	if end.After(now) {
		end = now
	}
	return observedInterval{Start: l.Start, End: end, Valid: true}
}

// observedInterval is the time span over which a resource (PV, PVC, load
// balancer) was observed. It bounds the Start and End of the __unmounted__
// allocations that carry that resource's cost. Valid is false when no
// interval is known.
type observedInterval struct {
	Start time.Time
	End   time.Time
	Valid bool
}
