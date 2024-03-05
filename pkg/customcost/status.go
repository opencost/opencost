package customcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	cloudconfig "github.com/opencost/opencost/pkg/cloud"
)

// Status gives the details and metadata of a CloudCost integration
type Status struct {
	Key               string                     `json:"key"`
	Source            string                     `json:"source"`
	Provider          string                     `json:"provider"`
	Active            bool                       `json:"active"`
	Valid             bool                       `json:"valid"`
	LastRun           time.Time                  `json:"lastRun"`
	NextRun           time.Time                  `json:"nextRun"`
	RefreshRateDaily  string                     `json:"RefreshRateDaily"`
	RefreshRateHourly string                     `json:"RefreshRateHourly"`
	Created           time.Time                  `json:"created"`
	Runs              int                        `json:"runs"`
	CoverageHourly    map[string]opencost.Window `json:"coverageHourly"`
	CoverageDaily     map[string]opencost.Window `json:"coverageDaily"`
	ConnectionStatus  string                     `json:"connectionStatus"`
	Config            cloudconfig.Config         `json:"config"`
}
