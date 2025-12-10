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

// Owner represents a Kubernetes resource owner
type Owner struct {
	UID         string            `json:"uid"`
	OwnerUID    string            `json:"ownerUid"`
	Name        string            `json:"name"`
	Kind        OwnerKind         `json:"kind"`
	Controller  bool              `json:"controller"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Start       time.Time         `json:"start"`
	End         time.Time         `json:"end"`
}

func (kms *KubeModelSet) RegisterOwner(uid, name, namespace, kind string, isController bool) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Owner '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Owners[uid]; !ok {
		namespaceUID := ""

		if ns, ok := kms.idx.namespaceByName[namespace]; !ok {
			kms.Warnf("RegisterOwner(%s, %s, %s, %s, %t): missing namespace '%s'", uid, name, namespace, kind, isController, namespace)
		} else {
			namespaceUID = ns.UID
		}

		kms.Owners[uid] = &Owner{
			UID:        uid,
			Name:       name,
			OwnerUID:   namespaceUID,
			Kind:       OwnerKind(kind),
			Controller: isController,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
