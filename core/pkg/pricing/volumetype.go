package pricing

type VolumeType string

const (
	VolumeTypeNil VolumeType = ""

	// AWS

	// General purpose SSD
	VolumeTypeGP2 VolumeType = "gp2"
	VolumeTypeGP3 VolumeType = "gp3"

	// Provisioned IOPS SSD
	VolumeTypeIO1 VolumeType = "io1"
	VolumeTypeIO2 VolumeType = "io2"

	// Throughput optimized HDD
	VolumeTypeST1 VolumeType = "st1"

	// Cold HDD
	VolumeTypeSC1 VolumeType = "sc1"

	// Magnetic (previous-generation / legacy)
	VolumeTypeStandard VolumeType = "standard"

	// Azure

	// HDD
	VolumeTypeStandardHDDLRS VolumeType = "Standard_LRS"

	// Standard SSD
	VolumeTypeStandardSSDLRS VolumeType = "StandardSSD_LRS"

	// Premium SSD
	VolumeTypePremiumLRS   VolumeType = "Premium_LRS"
	VolumeTypePremiumV2LRS VolumeType = "PremiumV2_LRS"

	// Ultra
	VolumeTypeUltraSSDLRS VolumeType = "UltraSSD_LRS"

	// GCP

	// Persistent Disk
	VolumeTypePDStandard VolumeType = "pd-standard"
	VolumeTypePDBalanced VolumeType = "pd-balanced"
	VolumeTypePDSSD      VolumeType = "pd-ssd"
	VolumeTypePDExtreme  VolumeType = "pd-extreme"

	// Hyperdisk
	VolumeTypeHyperdiskBalanced   VolumeType = "hyperdisk-balanced"
	VolumeTypeHyperdiskExtreme    VolumeType = "hyperdisk-extreme"
	VolumeTypeHyperdiskThroughput VolumeType = "hyperdisk-throughput"
)
