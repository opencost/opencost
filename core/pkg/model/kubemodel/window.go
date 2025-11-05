package kubemodel

import "time"

type Window struct {
	Start time.Time `json:"start"` // @bingen:field[version=1]
	End   time.Time `json:"end"`   // @bingen:field[version=1]
}
