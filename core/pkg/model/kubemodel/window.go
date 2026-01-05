package kubemodel

import "time"

// @bingen:generate:Window
type Window struct {
	Start time.Time `json:"start"` // @bingen:field[version=1]
	End   time.Time `json:"end"`   // @bingen:field[version=1]
}
