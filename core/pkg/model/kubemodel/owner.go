package kubemodel

import "time"

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
