package kubemodel

import "time"

// Window defines a period of time with a start and an end
type Window struct {
	Start time.Time `json:"start"` // @bingen:field[version=1]
	End   time.Time `json:"end"`   // @bingen:field[version=1]
}
