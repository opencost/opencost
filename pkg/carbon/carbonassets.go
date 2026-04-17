package carbon

import (
	"embed"
	"encoding/csv"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
)

//go:embed carbonlookupdata.csv
var f embed.FS

// averageRegionKey is the fallback region label used in the lookup CSV when a
// specific (provider, region, instanceType) tuple cannot be matched.
const averageRegionKey = "average-region"

type carbonLookupKeyRegion struct {
	provider string
	region   string
}

type carbonLookupKeyNode struct {
	provider     string
	region       string
	instanceType string
}

var (
	carbonLookupNode    map[carbonLookupKeyNode]float64
	carbonLookupDisk    map[carbonLookupKeyRegion]float64
	carbonLookupNetwork map[carbonLookupKeyRegion]float64
)

func init() {
	carbonData, err := f.ReadFile("carbonlookupdata.csv")
	if err != nil {
		log.Errorf("Error getting content of carbon lookup file: %s", err)
		return
	}

	reader := csv.NewReader(strings.NewReader(string(carbonData)))

	if _, err := reader.Read(); err != nil {
		log.Errorf("Error reading carbon lookup header: %s", err)
		return
	}

	rows, err := reader.ReadAll()
	if err != nil {
		log.Errorf("Error reading carbon lookup data: %s", err)
		return
	}

	carbonLookupNode = make(map[carbonLookupKeyNode]float64)
	carbonLookupDisk = make(map[carbonLookupKeyRegion]float64)
	carbonLookupNetwork = make(map[carbonLookupKeyRegion]float64)

	for _, row := range rows {
		// Skip blank records (e.g. a trailing newline in the CSV).
		if len(row) == 0 || (len(row) == 1 && strings.TrimSpace(row[0]) == "") {
			continue
		}
		if len(row) < 6 {
			log.Warnf("carbon: skipping malformed lookup row %v", row)
			continue
		}

		coeff, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			log.Warnf("carbon: skipping row with malformed carbon coefficient %q", row[5])
			continue
		}

		provider := row[0]
		region := row[1]
		instanceType := row[2]
		assetType := row[3]

		switch assetType {
		case "Node":
			carbonLookupNode[carbonLookupKeyNode{
				provider:     provider,
				region:       region,
				instanceType: instanceType,
			}] = coeff
		case "Disk":
			carbonLookupDisk[carbonLookupKeyRegion{
				provider: provider,
				region:   region,
			}] = coeff
		case "Network":
			carbonLookupNetwork[carbonLookupKeyRegion{
				provider: provider,
				region:   region,
			}] = coeff
		}
	}
}

type CarbonRow struct {
	Co2e float64 `json:"co2e"`
}

// RelateCarbonAssets returns an estimated CO2e value for each asset in the set.
// The returned value is in metric tonnes of CO2e, consistent with the units of
// the embedded lookup table (tonnes CO2e per hour of asset runtime).
func RelateCarbonAssets(as *opencost.AssetSet) (map[string]CarbonRow, error) {
	res := make(map[string]CarbonRow, len(as.Assets))

	for key, asset := range as.Assets {
		coeff := lookupCarbonCoeff(asset)
		res[key] = CarbonRow{
			Co2e: coeff * asset.Minutes() / 60,
		}
	}

	return res, nil
}

// lookupCarbonCoeff resolves the carbon coefficient (tonnes CO2e per hour) for
// the given asset, falling back to the provider-wide average-region value when
// a specific region or instance type is not present in the lookup table.
func lookupCarbonCoeff(asset opencost.Asset) float64 {
	props := asset.GetProperties()
	provider := resolveProvider(asset)
	if provider == "" {
		if isCarbonTrackedAsset(asset.Type()) {
			providerID := ""
			if props != nil {
				providerID = props.ProviderID
			}
			log.DedupedWarningf(10, "carbon: cannot infer provider for asset %q", providerID)
		}
		return 0
	}

	region, _ := util.GetRegion(asset.GetLabels())
	instanceType, _ := util.GetInstanceType(asset.GetLabels())

	switch asset.Type() {
	case opencost.NodeAssetType:
		if coeff, ok := carbonLookupNode[carbonLookupKeyNode{provider, region, instanceType}]; ok {
			return coeff
		}
		if coeff, ok := carbonLookupNode[carbonLookupKeyNode{provider, averageRegionKey, ""}]; ok {
			log.DedupedWarningf(10, "carbon: falling back to average-region for node (provider=%s region=%q instanceType=%q)", provider, region, instanceType)
			return coeff
		}
	case opencost.DiskAssetType:
		if coeff, ok := carbonLookupDisk[carbonLookupKeyRegion{provider, region}]; ok {
			return coeff
		}
		if coeff, ok := carbonLookupDisk[carbonLookupKeyRegion{provider, averageRegionKey}]; ok {
			log.DedupedWarningf(10, "carbon: falling back to average-region for disk (provider=%s region=%q)", provider, region)
			return coeff
		}
	case opencost.NetworkAssetType:
		if coeff, ok := carbonLookupNetwork[carbonLookupKeyRegion{provider, region}]; ok {
			return coeff
		}
		if coeff, ok := carbonLookupNetwork[carbonLookupKeyRegion{provider, averageRegionKey}]; ok {
			return coeff
		}
	}
	return 0
}

func isCarbonTrackedAsset(t opencost.AssetType) bool {
	switch t {
	case opencost.NodeAssetType, opencost.DiskAssetType, opencost.NetworkAssetType:
		return true
	}
	return false
}

// resolveProvider returns the canonical provider name for an asset. It prefers
// the canonical Provider property populated by the cost model, falling back to
// parsing the cloud provider ID when the property is missing.
func resolveProvider(asset opencost.Asset) string {
	props := asset.GetProperties()
	if props == nil {
		return ""
	}

	switch props.Provider {
	case opencost.AWSProvider, opencost.GCPProvider, opencost.AzureProvider:
		return props.Provider
	}

	return inferProviderFromProviderID(props.ProviderID)
}

// inferProviderFromProviderID is a best-effort fallback that matches the
// conventional shapes of Kubernetes Node `spec.providerID` values for the
// cloud providers present in the embedded lookup data (AWS, GCP, Azure).
//
// Real-world formats:
//   - AWS:   aws:///<availability-zone>/<instance-id>  (or raw "i-…")
//   - GCP:   gce://<project>/<zone>/<instance-name>
//   - Azure: azure:///subscriptions/<sub>/resourceGroups/<rg>/…
func inferProviderFromProviderID(providerID string) string {
	id := strings.ToLower(strings.TrimSpace(providerID))
	if id == "" {
		return ""
	}

	switch {
	case strings.HasPrefix(id, "aws:"), strings.HasPrefix(id, "i-"):
		return opencost.AWSProvider
	case strings.HasPrefix(id, "gce:"), strings.HasPrefix(id, "gke"):
		return opencost.GCPProvider
	case strings.HasPrefix(id, "azure:"):
		return opencost.AzureProvider
	}
	return ""
}
