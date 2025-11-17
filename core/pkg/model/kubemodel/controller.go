package kubemodel

import "time"

// @bingen:generate:ControllerKind
type ControllerKind string

const (
	ControllerKindDeployment  ControllerKind = "deployment"
	ControllerKindStatefulSet ControllerKind = "statefulset"
	ControllerKindDaemonSet   ControllerKind = "daemonset"
	ControllerKindJob         ControllerKind = "job"
	ControllerKindCronJob     ControllerKind = "cronjob"
	ControllerKindReplicaSet  ControllerKind = "replicaset"
)

// @bingen:generate:Controller
type Controller struct {
	UID          string            `json:"uid"`                   // @bingen:field[version=1]
	NamespaceUID string            `json:"namespaceUid"`          // @bingen:field[version=1]
	Name         string            `json:"name"`                  // @bingen:field[version=1]
	Kind         ControllerKind    `json:"kind"`                  // @bingen:field[version=1]
	Labels       map[string]string `json:"labels,omitempty"`      // @bingen:field[version=1]
	Annotations  map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Start        time.Time         `json:"start"`                 // @bingen:field[version=1]
	End          time.Time         `json:"end"`                   // @bingen:field[version=1]
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`  // @bingen:field[version=1]
}