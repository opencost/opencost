package cloudcost

import (
	"time"

	cloudconfig "github.com/opencost/opencost/pkg/cloud"
)

// Status gives the details and metadata of a CloudCost integration
type Status struct {
	Key              string             `json:"key"`
	Source           string             `json:"source"`
	Provider         string             `json:"provider"`
	Active           bool               `json:"active"`
	Valid            bool               `json:"valid"`
	LastRun          time.Time          `json:"lastRun"`
	NextRun          time.Time          `json:"nextRun"`
	RefreshRate      string             `json:"RefreshRate"`
	Created          time.Time          `json:"created"`
	Runs             int                `json:"runs"`
	Coverage         string             `json:"coverage"`
	ConnectionStatus string             `json:"connectionStatus"`
	Config           cloudconfig.Config `json:"config"`
}
