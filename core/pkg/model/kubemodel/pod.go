package kubemodel

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
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

func ParseOwnerKind(kind string) OwnerKind {
	switch strings.ToLower(kind) {
	case "deployment":
		return OwnerKindDeployment
	case "statefulset":
		return OwnerKindStatefulSet
	case "daemonset":
		return OwnerKindDaemonSet
	case "job":
		return OwnerKindJob
	case "cronjob":
		return OwnerKindCronJob
	case "replicaset":
		return OwnerKindReplicaSet
	default:
		log.Warnf("failed to find owner kind for '%s'", kind)
		return OwnerKind(strings.ToLower(kind))
	}
}

type Owner struct {
	UID  string
	Kind OwnerKind
}

type Pod struct {
	UID          string            `json:"uid"`
	NamespaceUID string            `json:"namespaceUid"`
	NodeUID      string            `json:"nodeUid"`
	Name         string            `json:"name"`
	Owners       []Owner           `json:"owners"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	//NetworkTransferBytes Measurement       `json:"networkTransferBytes"` // TODO  separate ticket
	//NetworkReceiveBytes  Measurement       `json:"networkReceiveBytes"`
	Start time.Time `json:"start,omitempty"` // Pod creation/start timestamp
	End   time.Time `json:"end,omitempty"`   // Pod deletion/end timestamp (nil if still running)
	//DurationSeconds      Measurement       `json:"durationSeconds"`
}

func (kms *KubeModelSet) RegisterPod(pod *Pod) error {
	// Check required fields
	if pod.UID == "" {
		err := fmt.Errorf("UID is missing for Pod with name '%s'", pod.Name)
		kms.Error(err)
		return err
	}

	if pod.Name == "" {
		err := fmt.Errorf("Name is missing for Pod '%s'", pod.UID)
		kms.Error(err)
		return err
	}

	if kms.Window.Start.After(pod.Start) ||
		kms.Window.Start.After(pod.End) ||
		kms.Window.End.Before(pod.Start) ||
		kms.Window.End.Before(pod.End) {
		err := fmt.Errorf(
			"Pod '%s' has a start or end time (%s-%s) outside of the window %s-%s",
			pod.Start.Format(time.RFC3339),
			pod.End.Format(time.RFC3339),
			kms.Window.Start.Format(time.RFC3339),
			kms.Window.End.Format(time.RFC3339),
		)
		kms.Error(err)
		return err
	}

	if _, ok := kms.Pods[pod.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterPod: Cluster is nil")
		}

		kms.Pods[pod.UID] = pod

		kms.Metadata.ObjectCount++
	}

	return nil
}
