package kubemodel

import "time"

// Window defines a unit of time by a resolution and a start time
type Window struct {
	Resolution Resolution `json:"resolution"`
	Start      time.Time  `json:"start"`
}
