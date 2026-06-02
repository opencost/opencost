package aws

import (
	"regexp"

	"github.com/opencost/opencost/core/pkg/pricing"
)

// usageTypeRegex extracts the EBS volume type from AWS UsageType strings
// Example: "USE1-EBS:VolumeUsage.gp3" -> "EBS:VolumeUsage.gp3"
var usageTypeRegex = regexp.MustCompile(".*(-|^)(EBS.+)")

// awsVolumeTypes maps AWS UsageType strings to VolumeType constants
var awsVolumeTypes = map[string]pricing.VolumeType{
	"EBS:VolumeUsage.gp2":    pricing.VolumeTypeGP2,
	"EBS:VolumeUsage.gp3":    pricing.VolumeTypeGP3,
	"EBS:VolumeUsage":        pricing.VolumeTypeStandard,
	"EBS:VolumeUsage.sc1":    pricing.VolumeTypeSC1,
	"EBS:VolumeP-IOPS.piops": pricing.VolumeTypeIO1,
	"EBS:VolumeUsage.st1":    pricing.VolumeTypeST1,
	"EBS:VolumeUsage.piops":  pricing.VolumeTypeIO1,
	"EBS:VolumeUsage.io2":    pricing.VolumeTypeIO2,
}

// nodeKey is used internally to track node metadata during product parsing
type nodeKey struct {
	Region       string
	InstanceType string
}

// volumeKey is used internally to track volume metadata during product parsing
type volumeKey struct {
	Region     string
	VolumeType pricing.VolumeType
	UsageType  string // Store original usage type for special handling (e.g., io1 per-IO costs)
}
