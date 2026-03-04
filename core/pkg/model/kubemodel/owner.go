package kubemodel

import (
	"strings"

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
