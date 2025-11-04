package kubemodel

import "time"

type Namespace struct {
	UID         string
	ClusterUID  string
	Name        string
	Labels      map[string]string
	Annotations map[string]string
	Start       time.Time
	End         time.Time
}
