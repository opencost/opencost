package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:OwnerKind
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
// @bingen:generate:Owner
type Owner struct {
	UID         string            `json:"uid"`                   // @bingen:field[version=1]
	OwnerUID    string            `json:"ownerUid"`              // @bingen:field[version=1]
	Name        string            `json:"name"`                  // @bingen:field[version=1]
	Kind        OwnerKind         `json:"kind"`                  // @bingen:field[version=1]
	Controller  bool              `json:"controller"`            // @bingen:field[version=1]
	Labels      map[string]string `json:"labels,omitempty"`      // @bingen:field[version=1]
	Annotations map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Start       time.Time         `json:"start"`                 // @bingen:field[version=1]
	End         time.Time         `json:"end"`                   // @bingen:field[version=1]
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
