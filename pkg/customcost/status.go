package customcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// Status gives the details and metadata of a CustomCost integration
type Status struct {
	Enabled           bool                       `json:"enabled"`
	Domains           []string                   `json:"domains"`
	Key               string                     `json:"key,omitempty"`
	Source            string                     `json:"source,omitempty"`
	Provider          string                     `json:"provider,omitempty"`
	Active            bool                       `json:"active,omitempty"`
	Valid             bool                       `json:"valid,omitempty"`
	LastRun           time.Time                  `json:"lastRun"`
	NextRun           time.Time                  `json:"nextRun"`
	RefreshRateDaily  string                     `json:"RefreshRateDaily,omitempty"`
	RefreshRateHourly string                     `json:"RefreshRateHourly,omitempty"`
	Created           time.Time                  `json:"created"`
	Runs              int                        `json:"runs,omitempty"`
	CoverageHourly    map[string]opencost.Window `json:"coverageHourly,omitempty"`
	CoverageDaily     map[string]opencost.Window `json:"coverageDaily,omitempty"`
	ConnectionStatus  string                     `json:"connectionStatus,omitempty"`
}
