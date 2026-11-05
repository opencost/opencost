package unit

import (
	"testing"
)

func TestParseUnit_Strings(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expect    Unit
		expectErr bool
	}{
		// Duration units
		{name: "millisecond", input: "ms", expect: Millisecond, expectErr: false},
		{name: "second", input: "s", expect: Second, expectErr: false},
		{name: "minute", input: "min", expect: Minute, expectErr: false},
		{name: "hour", input: "hr", expect: Hour, expectErr: false},

		// Data storage units
		{name: "byte", input: "B", expect: Byte, expectErr: false},
		{name: "kibibyte", input: "KiB", expect: KiB, expectErr: false},
		{name: "mebibyte", input: "MiB", expect: MiB, expectErr: false},
		{name: "gibibyte", input: "GiB", expect: GiB, expectErr: false},

		// Compute resources
		{name: "mCPU", input: "mCPU", expect: MCPU, expectErr: false},
		{name: "vCPU", input: "vCPU", expect: VCPU, expectErr: false},
		{name: "GPU", input: "GPU", expect: GPU, expectErr: false},

		// Compute resources over time
		{name: "GiB-hr", input: "GiB-hr", expect: GiBHour, expectErr: false},
		{name: "vCPU-hr", input: "vCPU-hr", expect: VCPUHour, expectErr: false},
		{name: "GPU-hr", input: "GPU-hr", expect: GPUHour, expectErr: false},

		// Case insensitive tests
		{name: "uppercase ms", input: "MS", expect: Millisecond, expectErr: false},
		{name: "mixed case GiB", input: "gib", expect: GiB, expectErr: false},
		{name: "mixed case mCPU", input: "Mcpu", expect: MCPU, expectErr: false},
		{name: "mixed case vCPU-hr", input: "VCPU-HR", expect: VCPUHour, expectErr: false},

		// Invalid units
		{name: "invalid empty", input: "", expect: "", expectErr: true},
		{name: "invalid unknown", input: "xyz", expect: "", expectErr: true},
		{name: "invalid number", input: "123", expect: "", expectErr: true},
		{name: "invalid partial", input: "G", expect: "", expectErr: true},
		{name: "invalid with space", input: "G B", expect: "", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUnit(tt.input)
			if (err != nil) != tt.expectErr {
				t.Errorf("ParseUnit(%q) error = %v, expectErr %v", tt.input, err, tt.expectErr)
				return
			}
			if got != tt.expect {
				t.Errorf("ParseUnit(%q) = %v, expected %v", tt.input, got, tt.expect)
			}
		})
	}
}

func TestParseUnit_Constants(t *testing.T) {
	for _, unit := range validUnits {
		t.Run(string(unit), func(t *testing.T) {
			parsed, err := ParseUnit(string(unit))
			if err != nil {
				t.Errorf("ParseUnit(%q) unexpected error: %v", unit, err)
			}
			if parsed != unit {
				t.Errorf("ParseUnit(%q) = %v, expected %v", unit, parsed, unit)
			}
		})
	}
}
