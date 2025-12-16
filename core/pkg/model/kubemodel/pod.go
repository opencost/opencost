package kubemodel

import (
	"fmt"
	"time"
)

type Pod struct {
	UID                  string            `json:"uid"`
	NamespaceUID         string            `json:"namespaceUid"`
	OwnerUID             string            `json:"ownerUid"`
	NodeUID              string            `json:"nodeUid"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	Start                time.Time         `json:"start"`
	End                  time.Time         `json:"end"`
	CpuMillicoreUsageMax uint64            `json:"cpuMillicoreUsageMax"`
	RAMByteUsageMax      uint64            `json:"ramByteUsageMax"`
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`
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
