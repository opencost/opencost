package collector

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
)

type collectorClusterMap struct {
	clusterInfo clusters.ClusterInfoProvider
}

func newCollectorClusterMap(clusterInfo clusters.ClusterInfoProvider) *collectorClusterMap {
	return &collectorClusterMap{
		clusterInfo: clusterInfo,
	}
}

// getLocalClusterInfo returns the local cluster info in the event there does not exist a metric available.
func (c *collectorClusterMap) getLocalClusterInfo() (*clusters.ClusterInfo, error) {
	info := c.clusterInfo.GetClusterInfo()
	clusterInfo, err := clusters.MapToClusterInfo(info)
	if err != nil {
		return nil, fmt.Errorf("parsing local cluster info failed: %w", err)
	}

	return clusterInfo, nil
}

func (c *collectorClusterMap) GetClusterIDs() []string {
	info, err := c.getLocalClusterInfo()
	if err != nil {
		log.Errorf("%s", err.Error())
		return nil
	}
	return []string{info.ID}
}

func (c *collectorClusterMap) AsMap() map[string]*clusters.ClusterInfo {
	info, err := c.getLocalClusterInfo()
	if err != nil {
		log.Errorf("%s", err.Error())
		return nil
	}
	return map[string]*clusters.ClusterInfo{
		info.ID: info,
	}
}

func (c *collectorClusterMap) InfoFor(clusterID string) *clusters.ClusterInfo {
	info, err := c.getLocalClusterInfo()
	if err != nil {
		log.Errorf("%s", err.Error())
		return nil
	}

	if info.ID == clusterID {
		return info
	}
	return nil
}

func (c *collectorClusterMap) NameFor(clusterID string) string {
	info, err := c.getLocalClusterInfo()
	if err != nil {
		log.Errorf("%s", err.Error())
		return ""
	}
	if info.ID == clusterID {
		return info.Name
	}
	return ""
}

func (c *collectorClusterMap) NameIDFor(clusterID string) string {
	info, err := c.getLocalClusterInfo()
	if err != nil {
		log.Errorf("%s", err.Error())
		return clusterID
	}
	if info.ID == clusterID {
		return fmt.Sprintf("%s/%s", info.Name, clusterID)
	}
	return clusterID
}
