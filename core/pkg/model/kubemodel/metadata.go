package kubemodel

import (
	"time"
)

// @bingen:generate:Metadata
type Metadata struct {
<<<<<<< HEAD
	CreatedAt   time.Time `json:"createdAt"`   // @bingen:field[version=1]
	CompletedAt time.Time `json:"completedAt"` // @bingen:field[version=1]
	ObjectCount int       `json:"objectCount"` // @bingen:field[version=1]
	Errors      []string  `json:"errors"`      // @bingen:field[version=1]
	Warnings    []string  `json:"warnings"`    // @bingen:field[version=1]
=======
	CreatedAt   time.Time           `json:"createdAt"`             // @bingen:field[version=1]
	CompletedAt time.Time           `json:"completedAt"`           // @bingen:field[version=1]
	ObjectCount int                 `json:"objectCount"`           // @bingen:field[version=1]
	Diagnostics []*DiagnosticResult `json:"diagnostics,omitempty"` // @bingen:field[version=1]
>>>>>>> 92af4761 (Introduce kubemodel with core Kubernetes resources (Cluster, Namespace, Node, Pod, Container, Owner, Service) (#3472))
}
