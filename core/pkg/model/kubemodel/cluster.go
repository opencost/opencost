package kubemodel

import "time"

// Cluster represents the top-level Kubernetes cluster
type Cluster struct {
	ID                 string               `json:"id"`
	Provider           Provider             `json:"provider"`
	Account            string               `json:"account"`
	Name               string               `json:"name"`
	Start              time.Time            `json:"start"`
	End                time.Time            `json:"end"`
	Nodes              map[string]*Node     `json:"nodes"`
	Namespaces         map[string]*Namespace `json:"namespaces"`
	PersistentVolumes  map[string]*Volume   `json:"persistentVolumes"`
	LoadBalancers      map[string]*Service  `json:"loadBalancers"`
}
