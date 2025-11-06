package kubemodel

import "time"

// Cluster represents the top-level Kubernetes cluster
type Cluster struct {
	ID                string                `json:"id"`                // @bingen:field[version=1]
	Provider          Provider              `json:"provider"`          // @bingen:field[version=1]
	Account           string                `json:"account"`           // @bingen:field[version=1]
	Name              string                `json:"name"`              // @bingen:field[version=1]
	Start             time.Time             `json:"start"`             // @bingen:field[version=1]
	End               time.Time             `json:"end"`               // @bingen:field[version=1]
	Nodes             map[string]*Node      `json:"nodes"`             // @bingen:field[version=1]
	Namespaces        map[string]*Namespace `json:"namespaces"`        // @bingen:field[version=1]
	PersistentVolumes map[string]*Volume    `json:"persistentVolumes"` // @bingen:field[version=1]
	LoadBalancers     map[string]*Service   `json:"loadBalancers"`     // @bingen:field[version=1]
}
