package kubemodel

import (
	"fmt"
	"time"
)

type OwnerKind string

const (
	OwnerKindDeployment  OwnerKind = "deployment"
	OwnerKindStatefulSet OwnerKind = "statefulset"
	OwnerKindDaemonSet   OwnerKind = "daemonset"
	OwnerKindJob         OwnerKind = "job"
	OwnerKindCronJob     OwnerKind = "cronjob"
	OwnerKindReplicaSet  OwnerKind = "replicaset"
)

// Owner represents a Kubernetes resource owner (workload controller)
// @bingen:generate:Owner
type Owner struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	Name         string            `json:"name"`
	Kind         OwnerKind         `json:"kind"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Start        time.Time         `json:"start,omitempty"`
	End          time.Time         `json:"end,omitempty"`
}

func (kms *KubeModelSet) RegisterOwner(uid, name, namespace, kind string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Owner '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Owners[uid]; !ok {
		namespaceUID := ""

		if ns, ok := kms.idx.namespaceByName[namespace]; !ok {
			kms.Warnf("RegisterOwner(%s, %s, %s, %s): missing namespace '%s'", uid, name, namespace, kind, namespace)
		} else {
			namespaceUID = ns.UID
		}

		kms.Owners[uid] = &Owner{
			UID:          uid,
			Name:         name,
			NamespaceUID: namespaceUID,
			Kind:         OwnerKind(kind),
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
