package kubemodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeCluster(kms *kubemodel.KubeModelSet, start, end time.Time) error {

	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()
	clusterInfoResultFuture := source.WithGroup(grp, metrics.QueryClusterInfo(start, end))
	clusterUptimeResultFuture := source.WithGroup(grp, metrics.QueryClusterUptime(start, end))

	clusterMap := make(map[string]*kubemodel.Cluster)

	clusterInfoResult, _ := clusterInfoResultFuture.Await()
	for _, res := range clusterInfoResult {
		clusterMap[res.UID] = &kubemodel.Cluster{
			UID:      res.UID,
			Provider: shared.ParseProvider(res.Provider),
			Account:  res.AccountID,
			Name:     res.Cluster,
			Region:   res.Region,
		}
	}

	clusterUptimeResult, _ := clusterUptimeResultFuture.Await()
	for _, res := range clusterUptimeResult {
		cluster, ok := clusterMap[res.UID]
		if !ok {
			log.Warnf("cluster with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		cluster.Start = s
		cluster.End = e
	}

	cluster, ok := clusterMap[km.clusterUID]
	if !ok {
		return fmt.Errorf("failed to compute cluster with UID '%s'", km.clusterUID)
	}

	kms.RegisterCluster(cluster)

	return nil
}
