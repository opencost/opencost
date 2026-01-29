package scrape

import (
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

type ClusterInfoScrapper struct {
	clusterUID          string
	clusterInfoProvider clusters.ClusterInfoProvider
}

func newClusterInfoScrapper(clusterUID string, clusterInfoProvider clusters.ClusterInfoProvider) Scraper {
	return &ClusterInfoScrapper{
		clusterUID:          clusterUID,
		clusterInfoProvider: clusterInfoProvider,
	}
}

func (cis *ClusterInfoScrapper) Scrape() []metric.Update {
	var scrapeResults []metric.Update

	// extract label values from cluster info provider
	clusterInfoMap := cis.clusterInfoProvider.GetClusterInfo()
	clusterName := clusterInfoMap[clusters.ClusterInfoIdKey]
	provider := clusterInfoMap[clusters.ClusterInfoProviderKey]

	accountID := clusterInfoMap[clusters.ClusterInfoAccountKey]
	// GCP special case
	if accountID == "" {
		accountID = clusterInfoMap[clusters.ClusterInfoProjectKey]
	}

	provisioner := clusterInfoMap[clusters.ClusterInfoProvisionerKey]

	region := clusterInfoMap[clusters.ClusterInfoRegionKey]

	clusterInfo := map[string]string{
		source.UIDLabel:             cis.clusterUID,
		source.ClusterNameLabel:     clusterName,
		source.ProviderLabel:        provider,
		source.AccountIDLabel:       accountID,
		source.ProvisionerNameLabel: provisioner,
		source.RegionLabel:          region,
	}

	scrapeResults = append(scrapeResults, metric.Update{
		Name:           metric.ClusterInfo,
		Labels:         clusterInfo,
		AdditionalInfo: clusterInfo,
		Value:          0,
	})
	return scrapeResults

}
