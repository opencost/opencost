package autocomplete

import (
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// Request is the shared autocomplete request shape used by allocation, asset, and cloud cost APIs.
type Request struct {
	TenantID    string
	Search      string
	Field       string
	Limit       int
	Window      opencost.Window
	Filter      filter.Filter
	LabelConfig *opencost.LabelConfig
}

// Response is the shared autocomplete response shape.
type Response struct {
	Data []string `json:"data"`
}
