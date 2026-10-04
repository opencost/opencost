package azure

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseManagedDiskMeter(t *testing.T) {
	cases := []struct {
		name       string
		meter      string
		wantClass  string
		wantRedund string
		wantTier   string
		wantOK     bool
	}{
		{name: "p4 lrs", meter: "P4 LRS Disk", wantClass: AzureDiskPremiumSSDStorageClass, wantRedund: "LRS", wantTier: "P4", wantOK: true},
		{name: "p10 zrs", meter: "P10 ZRS Disk", wantClass: AzureDiskPremiumSSDStorageClass, wantRedund: "ZRS", wantTier: "P10", wantOK: true},
		{name: "e20 lrs", meter: "E20 LRS Disk", wantClass: AzureDiskStandardSSDStorageClass, wantRedund: "LRS", wantTier: "E20", wantOK: true},
		{name: "s4 lrs", meter: "S4 LRS Disk", wantClass: AzureDiskStandardStorageClass, wantRedund: "LRS", wantTier: "S4", wantOK: true},
		{name: "disk mount", meter: "P4 LRS Disk Mount", wantOK: false},
		{name: "files", meter: "LRS Provisioned", wantOK: false},
		{name: "garbage", meter: "P4 are good", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class, redund, tier, ok := parseManagedDiskMeter(tc.meter)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantClass, class)
				require.Equal(t, tc.wantRedund, redund)
				require.Equal(t, tc.wantTier, tier)
			}
		})
	}
}

func TestResolveDiskSKU(t *testing.T) {
	sku, ok := resolveDiskSKU("Premium_LRS")
	require.True(t, ok)
	require.Equal(t, AzureDiskPremiumSSDStorageClass, sku.StorageClass)
	require.Equal(t, azureDiskRedundancyLRS, sku.Redundancy)

	sku, ok = resolveDiskSKU("Premium_ZRS")
	require.True(t, ok)
	require.Equal(t, azureDiskRedundancyZRS, sku.Redundancy)

	_, ok = resolveDiskSKU("not-a-sku")
	require.False(t, ok)
}

func TestPickSizedOrLargerAvailableTier(t *testing.T) {
	available := map[string]bool{"P4": true, "P15": true}
	has := func(name string) bool { return available[name] }

	tier, ok := pickSizedOrLargerAvailableTier(AzureDiskPremiumSSDStorageClass, "P10", has)
	require.True(t, ok)
	require.Equal(t, "P15", tier.Name)

	tier, ok = pickSizedOrLargerAvailableTier(AzureDiskPremiumSSDStorageClass, "P4", has)
	require.True(t, ok)
	require.Equal(t, "P4", tier.Name)

	_, ok = pickSizedOrLargerAvailableTier(AzureDiskPremiumSSDStorageClass, "P10", func(string) bool { return false })
	require.False(t, ok)

	onlySmaller := map[string]bool{"P4": true}
	_, ok = pickSizedOrLargerAvailableTier(AzureDiskPremiumSSDStorageClass, "P10", func(name string) bool { return onlySmaller[name] })
	require.False(t, ok)
}

func TestDiskRateConversions(t *testing.T) {
	monthly := 5.2795
	require.InDelta(t, monthly/730.0, tierHourlyFromMonthly(monthly), 1e-12)
	require.InDelta(t, monthly/730.0/10.0, effectiveGiBHourRate(monthly, 10), 1e-12)
	require.Equal(t, 0.0, effectiveGiBHourRate(monthly, 0))
}

func TestIsManagedDiskTierKey(t *testing.T) {
	require.True(t, isManagedDiskTierKey("centralus,premium_ssd,LRS,P4"))
	require.True(t, isManagedDiskTierKey("centralus,standard_ssd,ZRS,E20"))
	require.True(t, isManagedDiskTierKey("centralus,premium_ssd,LRS,P123"))
	require.False(t, isManagedDiskTierKey("centralus,Standard_D2s_v3,ondemand,windows"))
	require.False(t, isManagedDiskTierKey("centralus,premium_ssd,LRS,E20"))
	require.False(t, isManagedDiskTierKey("centralus,standard_hdd,LRS,Sx"))
	require.False(t, isManagedDiskTierKey("centralus,premium_ssd,LRS,P4 Mount"))
	require.False(t, isManagedDiskTierKey("centralus,premium_ssd,LRS"))
}
