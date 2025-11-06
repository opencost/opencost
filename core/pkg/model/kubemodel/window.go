package kubemodel

import "time"

// Window defines a period of time with a start and an end
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}
