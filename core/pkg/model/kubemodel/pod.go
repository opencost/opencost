package kubemodel

import (
	"fmt"
	"time"
)

type Pod struct {
	UID                  string            `json:"uid"`
	NamespaceUID         string            `json:"namespaceUid"`
	OwnerUID             string            `json:"ownerUid"` // Reference to Owner (Deployment, StatefulSet, etc.)
	NodeUID              string            `json:"nodeUid"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	DurationSeconds      Measurement       `json:"durationSeconds"`
	NetworkTransferBytes Measurement       `json:"networkTransferBytes"`
	NetworkReceiveBytes  Measurement       `json:"networkReceiveBytes"`
	Start                time.Time         `json:"start,omitempty"` // Pod creation/start timestamp
	End                  time.Time         `json:"end,omitempty"`   // Pod deletion/end timestamp (nil if still running)
}

func (kms *KubeModelSet) RegisterPod(uid, name, namespace string) error {
	if uid == "" {
		err := fmt.Errorf("UID is nil for Pod '%s'", name)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Pods[uid]; !ok {
		namespaceUID := ""

		if ns, ok := kms.idx.namespaceByName[namespace]; !ok {
			kms.Warnf("RegisterPod(%s, %s, %s): missing namespace '%s'", uid, name, namespace, namespace)
		} else {
			namespaceUID = ns.UID
		}

		kms.Pods[uid] = &Pod{
			UID:          uid,
			Name:         name,
			NamespaceUID: namespaceUID,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
