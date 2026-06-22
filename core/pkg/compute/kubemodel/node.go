package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeNodes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nodeInfoResultFuture := source.WithGroup(grp, metrics.QueryNodeInfo(start, end))
	nodeUptimeResultFuture := source.WithGroup(grp, metrics.QueryNodeUptime(start, end))
	nodeLabelsResultFuture := source.WithGroup(grp, metrics.QueryNodeLabels(start, end))
	nodeResourceCapacitiesFuture := source.WithGroup(grp, metrics.QueryNodeResourceCapacities(start, end))
	nodeResourcesAllocatableFuture := source.WithGroup(grp, metrics.QueryNodeResourcesAllocatable(start, end))

	localStorageBytesFuture := source.WithGroup(grp, metrics.QueryKMLocalStorageBytes(start, end))
	localStorageUsedAvgFuture := source.WithGroup(grp, metrics.QueryKMLocalStorageUsedAvg(start, end))
	localStorageUsedMaxFuture := source.WithGroup(grp, metrics.QueryKMLocalStorageUsedMax(start, end))

	nodeMap := make(map[string]*kubemodel.Node)

	nodeInfoResult, _ := nodeInfoResultFuture.Await()
	for _, res := range nodeInfoResult {
		nodeMap[res.UID] = &kubemodel.Node{
			UID:                  res.UID,
			ProviderID:           res.ProviderID,
			Name:                 res.Node,
			ResourceCapacities:   make(kubemodel.ResourceQuantities),
			ResourcesAllocatable: make(kubemodel.ResourceQuantities),
		}
	}

	nodeUptimeResult, _ := nodeUptimeResultFuture.Await()
	for _, res := range nodeUptimeResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		node.Start = s
		node.End = e
	}

	nodeResourceCapacitiesResult, _ := nodeResourceCapacitiesFuture.Await()
	for _, res := range nodeResourceCapacitiesResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add resource capacities", res.UID)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		node.ResourceCapacities.Set(resource, unit, kubemodel.StatAvg, value)
	}

	nodeResourcesAllocatableResult, _ := nodeResourcesAllocatableFuture.Await()
	for _, res := range nodeResourcesAllocatableResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add resources allocatable", res.UID)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		node.ResourcesAllocatable.Set(resource, unit, kubemodel.StatAvg, value)
	}

	nodeLabelsResult, _ := nodeLabelsResultFuture.Await()
	for _, res := range nodeLabelsResult {
		node, ok := nodeMap[res.UID]
		if !ok {
			log.Warnf("node with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		node.Labels = res.Labels
	}

	localStorageBytesResult, _ := localStorageBytesFuture.Await()
	for _, res := range localStorageBytesResult {
		node, ok := nodeMap[res.UID]
		if ok {
			node.FileSystem.CapacityBytes = res.Value
		}
	}

	localStorageUsedAvgResult, _ := localStorageUsedAvgFuture.Await()
	for _, res := range localStorageUsedAvgResult {
		node, ok := nodeMap[res.UID]
		if ok {
			node.FileSystem.UsageByteAvg = res.Value
		}
	}

	localStorageUsedMaxResult, _ := localStorageUsedMaxFuture.Await()
	for _, res := range localStorageUsedMaxResult {
		node, ok := nodeMap[res.UID]
		if ok {
			node.FileSystem.UsageByteMax = res.Value
		}
	}

	for _, node := range nodeMap {
		err := kms.RegisterNode(node)
		if err != nil {
			log.Warnf("Failed to register node: %s", err.Error())
		}
	}

	return nil
}
