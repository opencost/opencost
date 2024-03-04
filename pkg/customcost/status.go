package customcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	cloudconfig "github.com/opencost/opencost/pkg/cloud"
)

// Status gives the details and metadata of a CloudCost integration
type Status struct {
	Key              string                     `json:"key"`
	Source           string                     `json:"source"`
	Provider         string                     `json:"provider"`
	Active           bool                       `json:"active"`
	Valid            bool                       `json:"valid"`
	LastRun          time.Time                  `json:"lastRun"`
	NextRun          time.Time                  `json:"nextRun"`
	RefreshRate      string                     `json:"RefreshRate"`
	Created          time.Time                  `json:"created"`
	Runs             int                        `json:"runs"`
	Coverage         map[string]opencost.Window `json:"coverage"`
	ConnectionStatus string                     `json:"connectionStatus"`
	Config           cloudconfig.Config         `json:"config"`
}
