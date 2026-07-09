package azure

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// Azure managed disk size tiers (GiB), ordered ascending.
// Source: https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types
var (
	premiumSSDTiers = []diskTier{
		{Name: "P1", SizeGiB: 4},
		{Name: "P2", SizeGiB: 8},
		{Name: "P3", SizeGiB: 16},
		{Name: "P4", SizeGiB: 32},
		{Name: "P6", SizeGiB: 64},
		{Name: "P10", SizeGiB: 128},
		{Name: "P15", SizeGiB: 256},
		{Name: "P20", SizeGiB: 512},
		{Name: "P30", SizeGiB: 1024},
		{Name: "P40", SizeGiB: 2048},
		{Name: "P50", SizeGiB: 4096},
		{Name: "P60", SizeGiB: 8192},
		{Name: "P70", SizeGiB: 16384},
		{Name: "P80", SizeGiB: 32767},
	}

	standardSSDTiers = []diskTier{
		{Name: "E1", SizeGiB: 4},
		{Name: "E2", SizeGiB: 8},
		{Name: "E3", SizeGiB: 16},
		{Name: "E4", SizeGiB: 32},
		{Name: "E6", SizeGiB: 64},
		{Name: "E10", SizeGiB: 128},
		{Name: "E15", SizeGiB: 256},
		{Name: "E20", SizeGiB: 512},
		{Name: "E30", SizeGiB: 1024},
		{Name: "E40", SizeGiB: 2048},
		{Name: "E50", SizeGiB: 4096},
		{Name: "E60", SizeGiB: 8192},
		{Name: "E70", SizeGiB: 16384},
		{Name: "E80", SizeGiB: 32767},
	}

	standardHDDTiers = []diskTier{
		{Name: "S4", SizeGiB: 32},
		{Name: "S6", SizeGiB: 64},
		{Name: "S10", SizeGiB: 128},
		{Name: "S15", SizeGiB: 256},
		{Name: "S20", SizeGiB: 512},
		{Name: "S30", SizeGiB: 1024},
		{Name: "S40", SizeGiB: 2048},
		{Name: "S50", SizeGiB: 4096},
		{Name: "S60", SizeGiB: 8192},
		{Name: "S70", SizeGiB: 16384},
		{Name: "S80", SizeGiB: 32767},
	}
)

type diskTier struct {
	Name    string
	SizeGiB int
}

// managedDiskMeterRE matches Rate Card / Price Sheet managed disk capacity meters,
// e.g. "P4 LRS Disk", "E10 ZRS Disk". Disk Mount meters are excluded.
var managedDiskMeterRE = regexp.MustCompile(`^(P|E|S)(\d+)\s+(LRS|ZRS)\s+Disk$`)

const (
	azureDiskRedundancyLRS = "LRS"
	azureDiskRedundancyZRS = "ZRS"
)

type managedDiskSKU struct {
	StorageClass string // premium_ssd / standard_ssd / standard_hdd
	Redundancy   string // LRS / ZRS
}

// parseManagedDiskMeter parses a managed disk capacity meter name into storage class,
// redundancy, and tier (e.g. P4). Returns ok=false for non-matching names.
func parseManagedDiskMeter(meterName string) (storageClass, redundancy, tier string, ok bool) {
	matches := managedDiskMeterRE.FindStringSubmatch(strings.TrimSpace(meterName))
	if len(matches) != 4 {
		return "", "", "", false
	}
	prefix := matches[1]
	tier = prefix + matches[2]
	redundancy = matches[3]
	switch prefix {
	case "P":
		storageClass = AzureDiskPremiumSSDStorageClass
	case "E":
		storageClass = AzureDiskStandardSSDStorageClass
	case "S":
		storageClass = AzureDiskStandardStorageClass
	default:
		return "", "", "", false
	}
	return storageClass, redundancy, tier, true
}

// diskTierKey builds the Pricing map key for a managed disk tier monthly price.
// Format: region,storageClass,redundancy,tier (e.g. "centralus,premium_ssd,LRS,P4").
func diskTierKey(region, storageClass, redundancy, tier string) string {
	return fmt.Sprintf("%s,%s,%s,%s", region, storageClass, redundancy, tier)
}

// diskClassKey builds the legacy class-level Pricing key used for Azure Files and
// as a size-unknown fallback (linearized $/GiB-hour).
func diskClassKey(region, storageClass string) string {
	return region + "," + storageClass
}

// resolveDiskSKU maps StorageClass / Azure disk SKU parameters to an OpenCost
// storage class and redundancy. Returns ok=false for Azure Files SKUs handled separately.
func resolveDiskSKU(sku string) (managedDiskSKU, bool) {
	switch strings.ToLower(strings.TrimSpace(sku)) {
	case "premium_lrs":
		return managedDiskSKU{StorageClass: AzureDiskPremiumSSDStorageClass, Redundancy: azureDiskRedundancyLRS}, true
	case "premium_zrs":
		return managedDiskSKU{StorageClass: AzureDiskPremiumSSDStorageClass, Redundancy: azureDiskRedundancyZRS}, true
	case "standardssd_lrs":
		return managedDiskSKU{StorageClass: AzureDiskStandardSSDStorageClass, Redundancy: azureDiskRedundancyLRS}, true
	case "standardssd_zrs":
		return managedDiskSKU{StorageClass: AzureDiskStandardSSDStorageClass, Redundancy: azureDiskRedundancyZRS}, true
	case "standard_lrs":
		return managedDiskSKU{StorageClass: AzureDiskStandardStorageClass, Redundancy: azureDiskRedundancyLRS}, true
	case "standard_zrs":
		return managedDiskSKU{StorageClass: AzureDiskStandardStorageClass, Redundancy: azureDiskRedundancyZRS}, true
	default:
		return managedDiskSKU{}, false
	}
}

func tiersForStorageClass(storageClass string) []diskTier {
	switch storageClass {
	case AzureDiskPremiumSSDStorageClass:
		return premiumSSDTiers
	case AzureDiskStandardSSDStorageClass:
		return standardSSDTiers
	case AzureDiskStandardStorageClass:
		return standardHDDTiers
	default:
		return nil
	}
}

// selectDiskTier returns the smallest Azure disk tier that can hold sizeGiB.
// If sizeGiB is larger than the biggest tier, the largest tier is returned.
func selectDiskTier(storageClass string, sizeGiB float64) (diskTier, bool) {
	tiers := tiersForStorageClass(storageClass)
	if len(tiers) == 0 || sizeGiB <= 0 {
		return diskTier{}, false
	}
	for _, tier := range tiers {
		if float64(tier.SizeGiB) >= sizeGiB {
			return tier, true
		}
	}
	return tiers[len(tiers)-1], true
}

// smallestDiskTier returns the smallest tier for a storage class (used for
// linearized class-level fallback rates).
func smallestDiskTier(storageClass string) (diskTier, bool) {
	tiers := tiersForStorageClass(storageClass)
	if len(tiers) == 0 {
		return diskTier{}, false
	}
	return tiers[0], true
}

// tierHourlyFromMonthly converts a fixed monthly tier price to a whole-disk
// hourly cost. Stored in models.PV.Cost for tier keys so AllNodePricing stays
// in hourly units (class keys store $/GiB-hour).
func tierHourlyFromMonthly(monthlyTierPrice float64) float64 {
	if monthlyTierPrice <= 0 {
		return 0
	}
	return monthlyTierPrice / timeutil.HoursPerMonth
}

// effectiveGiBHourRate converts a fixed monthly tier price into the $/GiB-hour
// rate that, when multiplied by reportedSizeGiB, recovers the tier hourly cost.
func effectiveGiBHourRate(monthlyTierPrice, reportedSizeGiB float64) float64 {
	return effectiveGiBHourRateFromHourly(tierHourlyFromMonthly(monthlyTierPrice), reportedSizeGiB)
}

// effectiveGiBHourRateFromHourly converts a whole-disk hourly tier price into
// the $/GiB-hour rate used by allocation (rate × GiB × hours).
func effectiveGiBHourRateFromHourly(tierHourlyPrice, reportedSizeGiB float64) float64 {
	if tierHourlyPrice <= 0 || reportedSizeGiB <= 0 {
		return 0
	}
	return tierHourlyPrice / reportedSizeGiB
}

// nearestDiskTierIndex returns the index of preferred in the class tier table,
// or -1 if not found.
func diskTierIndex(storageClass, tierName string) int {
	tiers := tiersForStorageClass(storageClass)
	for i, tier := range tiers {
		if tier.Name == tierName {
			return i
		}
	}
	return -1
}

// pickSizedOrLargerAvailableTier chooses the first available priced tier at the
// preferred tier index or larger. It never selects a smaller tier to avoid
// underpricing fixed-tier Azure disks.
func pickSizedOrLargerAvailableTier(storageClass, preferredTier string, hasPrice func(tierName string) bool) (diskTier, bool) {
	tiers := tiersForStorageClass(storageClass)
	idx := diskTierIndex(storageClass, preferredTier)
	if idx < 0 {
		return diskTier{}, false
	}
	for i := idx; i < len(tiers); i++ {
		if hasPrice(tiers[i].Name) {
			return tiers[i], true
		}
	}
	return diskTier{}, false
}

func formatPrice(price float64) string {
	return fmt.Sprintf("%f", price)
}

func parsePrice(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// isManagedDiskTierKey returns true if the key is a valid managed disk tier pricing key.
func isManagedDiskTierKey(key string) bool {
	parts := strings.Split(key, ",")
	if len(parts) != 4 {
		return false
	}

	storageClass := parts[1]
	redundancy := parts[2]
	tierName := parts[3]

	if redundancy != azureDiskRedundancyLRS && redundancy != azureDiskRedundancyZRS {
		return false
	}

	tiers := tiersForStorageClass(storageClass)
	if len(tiers) == 0 {
		return false
	}

	for _, tier := range tiers {
		if tier.Name == tierName {
			return true
		}
	}

	return false
}

// pvSizeGiB returns the PersistentVolume capacity in GiB, or 0 if unavailable.
func pvSizeGiB(pv *clustercache.PersistentVolume) float64 {
	if pv == nil {
		return 0
	}
	qty := pv.Spec.Capacity.Storage()
	if qty == nil || qty.IsZero() {
		return 0
	}
	return float64(qty.Value()) / (1024 * 1024 * 1024)
}
