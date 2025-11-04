package kubemodel

import "time"

type Cluster struct {
	UID      string
	Provider Provider
	Account  string
	Name     string
	Start    time.Time
	End      time.Time
}
