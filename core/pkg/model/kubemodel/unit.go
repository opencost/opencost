package kubemodel

// @bingen:generate:Unit
type Unit string

type Measurement = float64

const (
	UnitMillicore       = "m"
	UnitByte            = "B"
	UnitSecond          = "s"
	UnitMillicoreSecond = "m-s"
	UnitByteSecond      = "B-s"
)
