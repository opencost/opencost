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
	KB   Unit = "KB"
	KiB  Unit = "KiB"
	MB   Unit = "MB"
	MiB  Unit = "MiB"
	GB   Unit = "GB"
	GiB  Unit = "GiB"
	TB   Unit = "TB"
	TiB  Unit = "TiB"
	PB   Unit = "PB"
	PiB  Unit = "PiB"

	// Compute resources
	MCPU Unit = "mCPU"
	VCPU Unit = "vCPU"
	GPU  Unit = "GPU"

	// Compute resources cumulative over time
	VCPUHour   Unit = "vCPU-hr"
	RAMGiBHour Unit = "RAM-GiB-hr"
	GPUHour    Unit = "GPU-hr"

	// Storage resources cumulative over time
	StorageGiBHour Unit = "storage-GiB-hr"
)

// validUnits is a map of all valid unit strings for quick lookup
var validUnits = map[string]Unit{
	string(Millisecond):    Millisecond,
	string(Second):         Second,
	string(Minute):         Minute,
	string(Hour):           Hour,
	string(Byte):           Byte,
	string(KB):             KB,
	string(KiB):            KiB,
	string(MB):             MB,
	string(MiB):            MiB,
	string(GB):             GB,
	string(GiB):            GiB,
	string(TB):             TB,
	string(TiB):            TiB,
	string(PB):             PB,
	string(PiB):            PiB,
	string(MCPU):           MCPU,
	string(VCPU):           VCPU,
	string(GPU):            GPU,
	string(VCPUHour):       VCPUHour,
	string(RAMGiBHour):     RAMGiBHour,
	string(GPUHour):        GPUHour,
	string(StorageGiBHour): StorageGiBHour,
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
