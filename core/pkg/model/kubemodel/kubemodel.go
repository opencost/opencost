package kubemodel

import "time"

type KubeModel struct {
	Metadata KubeModelMetadata
	Cluster  Cluster
	Window   Window
}

type KubeModelMetadata struct {
	CreatedAt  time.Time
	DataSource string
	Warnings   []string
	Errors     []error
}
