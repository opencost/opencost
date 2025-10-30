package kubemodel

import "time"

type KubeModel struct {
	Metadata KubeModelMetadata
	Cluster  Cluster
}

type KubeModelMetadata struct {
	CreatedAt  time.Time
	DataSource string
	Warnings   []string
	Errors     []error
}
