package shared

// @bingen:generate:UsageType
type UsageType string

const (
	UsageTypeEmpty    UsageType = ""
	UsageTypeOnDemand UsageType = "OnDemand"
	UsageTypeSpot     UsageType = "Spot"
)
