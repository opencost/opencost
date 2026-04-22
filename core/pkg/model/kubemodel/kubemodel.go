package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate[stringtable,streamable]:KubeModelSet
type KubeModelSet struct {
	Metadata               *Metadata                         `json:"meta"`                   // @bingen:field[version=1]
	Window                 Window                            `json:"window"`                 // @bingen:field[version=1]
	Cluster                *Cluster                          `json:"cluster"`                // @bingen:field[version=1]
	Namespaces             map[string]*Namespace             `json:"namespaces"`             // @bingen:field[version=1]
	ResourceQuotas         map[string]*ResourceQuota         `json:"resourceQuotas"`         // @bingen:field[version=1]
	Containers             map[string]*Container             `json:"containers,omitempty"`   // @bingen:field[version=2]
	Deployments            map[string]*Deployment            `json:"deployments,omitempty"`  // @bingen:field[version=2]
	StatefulSets           map[string]*StatefulSet           `json:"statefulSets,omitempty"` // @bingen:field[version=2]
	DaemonSets             map[string]*DaemonSet             `json:"daemonSets,omitempty"`   // @bingen:field[version=2]
	Jobs                   map[string]*Job                   `json:"jobs,omitempty"`         // @bingen:field[version=2]
	CronJobs               map[string]*CronJob               `json:"cronJobs,omitempty"`     // @bingen:field[version=2]
	ReplicaSets            map[string]*ReplicaSet            `json:"replicaSets,omitempty"`  // @bingen:field[version=2]
	Nodes                  map[string]*Node                  `json:"nodes,omitempty"`        // @bingen:field[version=2]
	Pods                   map[string]*Pod                   `json:"pods,omitempty"`         // @bingen:field[version=2]
	PersistentVolumeClaims map[string]*PersistentVolumeClaim `json:"pvcs,omitempty"`         // @bingen:field[version=2]
	Services               map[string]*Service               `json:"services,omitempty"`     // @bingen:field[version=2]
	PersistentVolumes      map[string]*PersistentVolume      `json:"volumes,omitempty"`      // @bingen:field[version=2]
	DCGMDevices            map[string]*DCGMDevice            `json:"dcgmDevices,omitempty"`  // @bingen:field[version=2]
	idx                    *kubeModelSetIndexes              // @bingen:field[ignore]
}

func NewKubeModelSet(start time.Time, end time.Time) *KubeModelSet {
	now := time.Now().UTC()
	kms := &KubeModelSet{
		Metadata: &Metadata{
			CreatedAt:       now,
			CompletedAt:     now, // Will be updated when processing completes
			DiagnosticLevel: DefaultDiagnosticLevel,
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Containers:             map[string]*Container{},
		Deployments:            map[string]*Deployment{},
		StatefulSets:           map[string]*StatefulSet{},
		DaemonSets:             map[string]*DaemonSet{},
		Jobs:                   map[string]*Job{},
		CronJobs:               map[string]*CronJob{},
		ReplicaSets:            map[string]*ReplicaSet{},
		Namespaces:             map[string]*Namespace{},
		Nodes:                  map[string]*Node{},
		DCGMDevices:            map[string]*DCGMDevice{},
		Pods:                   map[string]*Pod{},
		PersistentVolumeClaims: map[string]*PersistentVolumeClaim{},
		ResourceQuotas:         map[string]*ResourceQuota{},
		Services:               map[string]*Service{},
		PersistentVolumes:      map[string]*PersistentVolume{},
		idx:                    newKubeModelSetIndexes(),
	}
	return kms
}

// GetNamespaceByName retrieves a namespace by its name using the index
func (kms *KubeModelSet) GetNamespaceByName(name string) (*Namespace, bool) {
	if kms.idx == nil {
		return nil, false
	}

	uid, ok := kms.idx.namespaceNameToID[name]
	if !ok {
		return nil, false
	}

	ns, ok := kms.Namespaces[uid]
	return ns, ok
}

// IsEmpty returns true if the KubeModelSet is nil, has no cluster, or contains no resources
func (kms *KubeModelSet) IsEmpty() bool {
	if kms == nil || kms.Cluster == nil {
		return true
	}

	// Check if all resource maps are empty
	return len(kms.Containers) == 0 &&
		len(kms.Deployments) == 0 &&
		len(kms.StatefulSets) == 0 &&
		len(kms.DaemonSets) == 0 &&
		len(kms.Jobs) == 0 &&
		len(kms.CronJobs) == 0 &&
		len(kms.ReplicaSets) == 0 &&
		len(kms.Namespaces) == 0 &&
		len(kms.Nodes) == 0 &&
		len(kms.DCGMDevices) == 0 &&
		len(kms.Pods) == 0 &&
		len(kms.PersistentVolumeClaims) == 0 &&
		len(kms.ResourceQuotas) == 0 &&
		len(kms.Services) == 0 &&
		len(kms.PersistentVolumes) == 0
}

// checkWindow validates that the given start/end times are fully contained within
// the KubeModelSet window. It records and returns an error if they are not.
func checkWindow(window Window, start, end time.Time) error {
	if window.Start.After(start) ||
		window.Start.After(end) ||
		window.End.Before(start) ||
		window.End.Before(end) {
		return fmt.Errorf(
			"start or end time (%s-%s) is outside of the window %s-%s",
			start.Format(time.RFC3339),
			end.Format(time.RFC3339),
			window.Start.Format(time.RFC3339),
			window.End.Format(time.RFC3339),
		)
	}
	return nil
}

type kubeModelSetIndexes struct {
	namespaceNameToID map[string]string
	namespaceByName   map[string]*Namespace
}

func newKubeModelSetIndexes() *kubeModelSetIndexes {
	return &kubeModelSetIndexes{
		namespaceNameToID: make(map[string]string),
		namespaceByName:   make(map[string]*Namespace),
	}
}
