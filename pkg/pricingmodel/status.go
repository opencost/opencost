package pricingmodel

import "time"

// Status holds the diagnostic state of a runner and is suitable for HTTP serialization.
type Status struct {
	SourceKey   string    `json:"sourceKey"`
	CreatedAt   time.Time `json:"createdAt"`
	LastRun     time.Time `json:"lastRun"`
	NextRun     time.Time `json:"nextRun"`
	RefreshRate string    `json:"refreshRate"`
	Runs        int       `json:"runs"`
	LastError   string    `json:"lastError,omitempty"`
}
