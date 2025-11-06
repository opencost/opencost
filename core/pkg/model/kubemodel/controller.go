package kubemodel

import (
	"time"
)

// ControllerKind represents the type of Kubernetes controller
type ControllerKind string

const (
	ControllerKindDeployment  ControllerKind = "deployment"
	ControllerKindStatefulSet ControllerKind = "statefulset"
	ControllerKindDaemonSet   ControllerKind = "daemonset"
	ControllerKindJob         ControllerKind = "job"
	ControllerKindCronJob     ControllerKind = "cronjob"
	ControllerKindReplicaSet  ControllerKind = "replicaset"
)

// Controller represents a Kubernetes workload controller
type Controller struct {
	ID          string            `json:"id"`
	NamespaceID string            `json:"namespaceId"`
	Name        string            `json:"name"`
	Kind        ControllerKind    `json:"kind"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Start       time.Time         `json:"start"`
	End         time.Time         `json:"end"`
	Diagnostic  *DiagnosticResult `json:"diagnostic,omitempty"`
}