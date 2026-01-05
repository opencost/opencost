package costmodel

import (
	"fmt"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func newResultPodKey(cluster string, namespace string, pod string) (podKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if namespace == "" {
		return podKey{}, fmt.Errorf("namespace is required")
	}

	if pod == "" {
		return podKey{}, fmt.Errorf("pod is required")
	}

	return newPodKey(cluster, namespace, pod), nil
}

type podKey struct {
	namespaceKey
	Pod string
}

func (k podKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.Pod)
}

func newPodKey(cluster, namespace, pod string) podKey {
	return podKey{
		namespaceKey: namespaceKey{
			Cluster:   cluster,
			Namespace: namespace,
		},
		Pod: pod,
	}
}

// getUnmountedPodKey while certain Unmounted costs can have a namespace, all unmounted costs for a single cluster will be represented by the same asset
func getUnmountedPodKey(cluster string) podKey {
	return newPodKey(cluster, opencost.UnmountedSuffix, opencost.UnmountedSuffix)
}

type namespaceKey struct {
	Cluster   string
	Namespace string
}

func (k namespaceKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Namespace)
}

func newNamespaceKey(cluster, namespace string) namespaceKey {
	return namespaceKey{
		Cluster:   cluster,
		Namespace: namespace,
	}
}

func newResultNamespaceKey(cluster string, namespace string) (namespaceKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if namespace == "" {
		return namespaceKey{}, fmt.Errorf("namespace is required")
	}

	return newNamespaceKey(cluster, namespace), nil
}

type controllerKey struct {
	Cluster        string
	Namespace      string
	ControllerKind string
	Controller     string
}

func (k controllerKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Cluster, k.Namespace, k.ControllerKind, k.Controller)
}

func newControllerKey(cluster, namespace, controllerKind, controller string) controllerKey {
	return controllerKey{
		Cluster:        cluster,
		Namespace:      namespace,
		ControllerKind: controllerKind,
		Controller:     controller,
	}
}

func newResultControllerKey(cluster, namespace, controller, controllerKind string) (controllerKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if namespace == "" {
		return controllerKey{}, fmt.Errorf("namespace is required")
	}

	if controller == "" {
		return controllerKey{}, fmt.Errorf("controller is required")
	}

	return newControllerKey(cluster, namespace, controllerKind, controller), nil
}

type serviceKey struct {
	Cluster   string
	Namespace string
	Service   string
}

func (k serviceKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.Service)
}

func newServiceKey(cluster, namespace, service string) serviceKey {
	return serviceKey{
		Cluster:   cluster,
		Namespace: namespace,
		Service:   service,
	}
}

func newResultServiceKey(cluster, namespace, service string) (serviceKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if namespace == "" {
		return serviceKey{}, fmt.Errorf("namespace is required")
	}

	if service == "" {
		return serviceKey{}, fmt.Errorf("service is required")
	}

	return newServiceKey(cluster, namespace, service), nil
}

type nodeKey struct {
	Cluster string
	Node    string
}

func (k nodeKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Node)
}

func newNodeKey(cluster, node string) nodeKey {
	return nodeKey{
		Cluster: cluster,
		Node:    node,
	}
}

func newResultNodeKey(cluster string, node string) (nodeKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if node == "" {
		return nodeKey{}, fmt.Errorf("node is required")
	}

	return newNodeKey(cluster, node), nil
}

type pvcKey struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string
}

func (k pvcKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.PersistentVolumeClaim)
}

func newPVCKey(cluster, namespace, persistentVolumeClaim string) pvcKey {
	return pvcKey{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: persistentVolumeClaim,
	}
}

// resultPVCKey converts a Prometheus query result to a pvcKey by
// looking up values associated with the given label names. For example,
// passing "cluster_id" for clusterLabel will use the value of the label
// "cluster_id" as the pvcKey's Cluster field. If a given field does not
// exist on the result, an error is returned. (The only exception to that is
// clusterLabel, which we expect may not exist, but has a default value.)
func newResultPVCKey(cluster, namespace, pvc string) (pvcKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}

	if namespace == "" {
		return pvcKey{}, fmt.Errorf("namespace is required")
	}

	if pvc == "" {
		return pvcKey{}, fmt.Errorf("persistentvolumeclaim is required")
	}

	return newPVCKey(cluster, namespace, pvc), nil
}

type pvKey struct {
	Cluster          string
	PersistentVolume string
}

func (k pvKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.PersistentVolume)
}

func newPVKey(cluster, persistentVolume string) pvKey {
	return pvKey{
		Cluster:          cluster,
		PersistentVolume: persistentVolume,
	}
}

func newResultPVKey(cluster, pv string) (pvKey, error) {
	if cluster == "" {
		cluster = coreenv.GetClusterID()
	}
	if pv == "" {
		return pvKey{}, fmt.Errorf("persistentvolume is required")
	}

	return newPVKey(cluster, pv), nil
}
