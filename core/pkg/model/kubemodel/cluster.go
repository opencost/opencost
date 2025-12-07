package kubemodel

import "time"

// @bingen:generate:Cluster
type Cluster struct {
	UID      string    `json:"uid"`      // @bingen:field[version=1]
	Provider Provider  `json:"provider"` // @bingen:field[version=1]
	Account  string    `json:"account"`  // @bingen:field[version=1]
	Name     string    `json:"name"`     // @bingen:field[version=1]
	Start    time.Time `json:"start"`    // @bingen:field[version=1]
	End      time.Time `json:"end"`      // @bingen:field[version=1]
}
