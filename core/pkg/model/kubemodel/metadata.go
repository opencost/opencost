package kubemodel

import "time"

// TODO can bingen support marshaling type `error`?
type Metadata struct {
	CreatedAt   time.Time `json:"createdAt"`   // @bingen:field[version=1]
	CompletedAt time.Time `json:"completedAt"` // @bingen:field[version=1]
	ObjectCount int       `json:"objectCount"` // @bingen:field[version=1]
	Errors      []string  `json:"errors"`      // @bingen:field[version=1]
	Warnings    []string  `json:"warnings"`    // @bingen:field[version=1]
}
