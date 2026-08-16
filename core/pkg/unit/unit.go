package unit

import (
	"fmt"
	"strings"
)

type Unit string

const (
	// Durations of time
	Millisecond Unit = "ms"
	Second      Unit = "s"
	Minute      Unit = "min"
	Hour        Unit = "hr"

	// Data storage and transfer
	Byte Unit = "B"
	KiB  Unit = "KiB"
	MiB  Unit = "MiB"
	GiB  Unit = "GiB"

	// Compute resources
	MCPU Unit = "mCPU"
	VCPU Unit = "vCPU"
	GPU  Unit = "GPU"

	// Compute resources cumulative over time
	GiBHour  Unit = "GiB-hr"
	GPUHour  Unit = "GPU-hr"
	VCPUHour Unit = "vCPU-hr"
)

// validUnits is a map of all valid unit strings for quick lookup
var validUnits = map[string]Unit{
	string(Millisecond): Millisecond,
	string(Second):      Second,
	string(Minute):      Minute,
	string(Hour):        Hour,
	string(Byte):        Byte,
	string(KiB):         KiB,
	string(MiB):         MiB,
	string(GiB):         GiB,
	string(MCPU):        MCPU,
	string(VCPU):        VCPU,
	string(GPU):         GPU,
	string(GiBHour):     GiBHour,
	string(VCPUHour):    VCPUHour,
	string(GPUHour):     GPUHour,
}

// ParseUnit parses a string into a Unit type.
// It performs case-insensitive matching and returns an error if the string
// does not match any valid unit.
func ParseUnit(s string) (Unit, error) {
	for key, unit := range validUnits {
		if strings.EqualFold(key, s) {
			return unit, nil
		}
	}

	return "", fmt.Errorf("invalid unit: %q", s)
}
