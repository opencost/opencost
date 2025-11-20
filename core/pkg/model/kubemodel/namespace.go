package kubemodel

import "time"

// @bingen:generate:Namespace
type Namespace struct {
	UID         string            `json:"uid"`         // @bingen:field[version=1]
	ClusterUID  string            `json:"clusterUID"`  // @bingen:field[version=1]
	Name        string            `json:"name"`        // @bingen:field[version=1]
	Labels      map[string]string `json:"labels"`      // @bingen:field[version=1]
	Annotations map[string]string `json:"annotations"` // @bingen:field[version=1]
	Start       time.Time         `json:"start"`       // @bingen:field[version=1]
	End         time.Time         `json:"end"`         // @bingen:field[version=1]
}
