package costmodel

import (
	"errors"
	"fmt"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
	"time"
)

// ComputeAssets implements the asset.Source interface
func (cm *CostModel) ComputeAsset(start, end time.Time, resolution time.Duration) (*kubecost.AssetSet, error) {
	// If the duration is short enough, compute the AssetSet directly
	if end.Sub(start) <= cm.MaxPrometheusQueryDuration {
		return cm.computeAssets(start, end)
	}

	// If the duration exceeds the configured MaxPrometheusQueryDuration, then
	// query for maximum-sized AssetSets, collect them, and accumulate.

	// s and e track the coverage of the entire given window over multiple
	// internal queries.
	s, e := start, start

	// Collect AssetSets in a range, then accumulate
	// TODO optimize by collecting consecutive AssetSets, accumulating as we go
	asr := kubecost.NewAssetSetRange()

	for e.Before(end) {
		// By default, query for the full remaining duration. But do not let
		// any individual query duration exceed the configured max Prometheus
		// query duration.
		duration := end.Sub(e)
		if duration > cm.MaxPrometheusQueryDuration {
			duration = cm.MaxPrometheusQueryDuration
		}

		// Set start and end parameters (s, e) for next individual computation.
		e = s.Add(duration)

		// Compute the individual AssetSet for just (s, e)
		as, err := cm.computeAssets(s, e)
		if err != nil {
			return kubecost.NewAssetSet(start, end), fmt.Errorf("error computing assets for %s: %s", kubecost.NewClosedWindow(s, e), err)
		}

		// Append to the range
		asr.Append(as)

		// Set s equal to e to set up the next query, if one exists.
		s = e
	}

	// Record errors and warnings, then append them to the results later.
	errors := []string{}
	warnings := []string{}

	for _, as := range asr.Assets {
		errors = append(errors, as.Errors...)
		warnings = append(warnings, as.Warnings...)
	}

	// Accumulate to yield the result AssetSet. After this step, we will
	// be nearly complete, but without the raw asset data, which must be
	// recomputed.
	result, err := asr.Accumulate()
	if err != nil {
		return kubecost.NewAssetSet(start, end), fmt.Errorf("error accumulating data for %s: %s", kubecost.NewClosedWindow(s, e), err)
	}

	result.Errors = errors
	result.Warnings = warnings

	return result, nil
}

func (cm *CostModel) computeAssets(start, end time.Time) (*kubecost.AssetSet, error) {

	var nodeMap map[NodeIdentifier]*Node
	var diskMap map[DiskIdentifier]*Disk
	var lbMap map[LoadBalancerIdentifier]*LoadBalancer
	var clusterManagementMap map[ClusterManagementIdentifier]*ClusterManagement

	shouldTry := true
	numTries := 0
	maxTries := 13 // wait up to 650s (1^2 + 2^2 + ... + 12^2) between retries

	assetSet := kubecost.NewAssetSet(start, end)

	for shouldTry {
		time.Sleep(time.Duration(numTries*numTries) * time.Second)
		numTries++
		shouldTry = false

		// Run if value has not been set in a previous run
		if nodeMap == nil {
			clusterNodes, err := ClusterNodes(cm.Provider, cm.PrometheusClient, start, end)
			if err != nil {
				// The error.As() function will properly traverse ErrorCollections for the specific
				// error type, as well as check the top level error
				var pce prom.CommError
				if errors.As(err, &pce) && numTries < maxTries {
					log.Errorf("Asset ETL: ComputeAssets: ClusterNodes: %s: retrying", pce)
					shouldTry = true
					continue
				} else {
					return assetSet, err
				}
			}
			// set result if no error occurred, and it will not retry on subsequent attempts
			nodeMap = clusterNodes
		}

		// Run if value has not been set in a previous run
		if diskMap == nil {
			clusterDisks, err := ClusterDisks(cm.PrometheusClient, cm.Provider, start, end)
			if err != nil {
				// The error.As() function will properly traverse ErrorCollections for the specific
				// error type, as well as check the top level error
				var pce prom.CommError
				if errors.As(err, &pce) && numTries < maxTries {
					log.Errorf("Asset ETL: ComputeAssets: ClusterDisks: %s: retrying", pce)
					shouldTry = true
					continue
				} else {
					return assetSet, err
				}
			}
			// set result if no error occurred, and it will not retry on subsequent attempts
			diskMap = clusterDisks
		}

		// Run if value has not been set in a previous run
		if lbMap == nil {
			clusterLoadBalancers, err := ClusterLoadBalancers(cm.PrometheusClient, start, end)
			if err != nil {
				// The error.As() function will properly traverse ErrorCollections for the specific
				// error type, as well as check the top level error
				var pce prom.CommError
				if errors.As(err, &pce) && numTries < maxTries {
					log.Errorf("Asset ETL: ComputeAssets: ClusterLoadBalancerss: %s: retrying", pce)
					shouldTry = true
					continue
				} else {
					return assetSet, err
				}
			}
			// set result if no error occurred, and it will not retry on subsequent attempts
			lbMap = clusterLoadBalancers
		}

		// Run if value has not been set in a previous run
		if clusterManagementMap == nil {
			clusterManagement, err := ClusterManagements(cm.PrometheusClient, start, end)
			if err != nil {
				// The error.As() function will properly traverse ErrorCollections for the specific
				// error type, as well as check the top level error
				var pce prom.CommError
				if errors.As(err, &pce) && numTries < maxTries {
					log.Errorf("Asset ETL: ComputeAssets: clusterManagementQuery: %s: retrying", pce)
					shouldTry = true
					continue
				} else {
					return assetSet, err
				}
			}
			// set result if no error occurred, and it will not retry on subsequent attempts
			clusterManagementMap = clusterManagement
		}
	}

	// TODO network cost
	cm.insertNodeAssets(nodeMap, assetSet)

	cm.insertLBAssets(lbMap, assetSet)

	cm.insertDiskAssets(diskMap, assetSet)

	cm.insertClusterManagementAssets(clusterManagementMap, assetSet)

	// TODO IP addresses?

	return assetSet, nil
}

func (cm *CostModel) insertNodeAssets(nodeMap map[NodeIdentifier]*Node, assetSet *kubecost.AssetSet) {
	window := assetSet.GetWindow()
	start := *window.Start()
	end := *window.End()
	for _, n := range nodeMap {
		s := n.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("Asset ETL: node '%s' start outside window: %s not in [%s, %s]", n.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := n.End
		if e.Before(start) || e.After(end) {
			log.Debugf("Asset ETL: node '%s' end outside window: %s not in [%s, %s]", n.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		hours := e.Sub(s).Hours()

		node := kubecost.NewNode(n.Name, n.Cluster, n.ProviderID, s, e, kubecost.NewWindow(&start, &end))
		cm.propertiesFromCluster(node.Properties)
		node.NodeType = n.NodeType
		node.CPUCoreHours = n.CPUCores * hours
		node.RAMByteHours = n.RAMBytes * hours
		node.GPUHours = n.GPUCount * hours
		node.CPUBreakdown = &kubecost.Breakdown{
			Idle:   n.CPUBreakdown.Idle,
			System: n.CPUBreakdown.System,
			User:   n.CPUBreakdown.User,
			Other:  n.CPUBreakdown.Other,
		}
		node.RAMBreakdown = &kubecost.Breakdown{
			Idle:   n.RAMBreakdown.Idle,
			System: n.RAMBreakdown.System,
			User:   n.RAMBreakdown.User,
			Other:  n.RAMBreakdown.Other,
		}
		node.CPUCost = n.CPUCost
		node.GPUCost = n.GPUCost
		node.GPUCount = n.GPUCount
		node.RAMCost = n.RAMCost
		node.Discount = n.Discount
		if n.Preemptible {
			node.Preemptible = 1.0
		}
		node.SetLabels(kubecost.AssetLabels(n.Labels))
		assetSet.Insert(node, nil)
	}
}

func (cm *CostModel) insertLBAssets(lbMap map[LoadBalancerIdentifier]*LoadBalancer, assetSet *kubecost.AssetSet) {
	window := assetSet.GetWindow()
	start := *window.Start()
	end := *window.End()
	for _, lb := range lbMap {
		s := lb.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("Asset ETL: load balancer '%s' start outside window: %s not in [%s, %s]", lb.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := lb.End
		if e.Before(start) || e.After(end) {
			log.Debugf("Asset ETL: load balancer '%s' end outside window: %s not in [%s, %s]", lb.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		loadBalancer := kubecost.NewLoadBalancer(lb.Name, lb.Cluster, lb.ProviderID, s, e, kubecost.NewWindow(&start, &end))
		cm.propertiesFromCluster(loadBalancer.Properties)
		loadBalancer.Cost = lb.Cost
		assetSet.Insert(loadBalancer, nil)
	}
}

func (cm *CostModel) insertDiskAssets(diskMap map[DiskIdentifier]*Disk, assetSet *kubecost.AssetSet) {
	window := assetSet.GetWindow()
	start := *window.Start()
	end := *window.End()
	for _, d := range diskMap {
		s := d.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("Asset ETL: disk '%s' start outside window: %s not in [%s, %s]", d.Name, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := d.End
		if e.Before(start) || e.After(end) {
			log.Debugf("Asset ETL: disk '%s' end outside window: %s not in [%s, %s]", d.Name, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		hours := e.Sub(s).Hours()

		disk := kubecost.NewDisk(d.Name, d.Cluster, d.ProviderID, s, e, kubecost.NewWindow(&start, &end))
		cm.propertiesFromCluster(disk.Properties)
		disk.Cost = d.Cost
		disk.ByteHours = d.Bytes * hours
		if d.BytesUsedAvgPtr != nil {
			byteHours := *d.BytesUsedAvgPtr * hours
			disk.ByteHoursUsed = &byteHours
		}
		if d.BytesUsedMaxPtr != nil {
			usageMax := *d.BytesUsedMaxPtr
			disk.ByteUsageMax = &usageMax
		}

		if d.Local {
			disk.Local = 1.0
		}
		disk.Breakdown = &kubecost.Breakdown{
			Idle:   d.Breakdown.Idle,
			System: d.Breakdown.System,
			User:   d.Breakdown.User,
			Other:  d.Breakdown.Other,
		}
		disk.StorageClass = d.StorageClass
		disk.VolumeName = d.VolumeName
		disk.ClaimName = d.ClaimName
		disk.ClaimNamespace = d.ClaimNamespace
		assetSet.Insert(disk, nil)
	}
}

func (cm *CostModel) insertClusterManagementAssets(clusterManagementMap map[ClusterManagementIdentifier]*ClusterManagement, assetSet *kubecost.AssetSet) {
	window := assetSet.GetWindow()
	start := *window.Start()
	end := *window.End()
	for _, clusterManagement := range clusterManagementMap {
		s := clusterManagement.Start
		if s.Before(start) || s.After(end) {
			log.Debugf("Asset ETL: clusterManagement '%s' start outside window: %s not in [%s, %s]", clusterManagement.Cluster, s.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			s = start
		}

		e := clusterManagement.End
		if e.Before(start) || e.After(end) {
			log.Debugf("Asset ETL: clusterManagement '%s' end outside window: %s not in [%s, %s]", clusterManagement.Cluster, e.Format("2006-01-02T15:04:05"), start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05"))
			e = end
		}

		cmAsset := kubecost.NewClusterManagement(clusterManagement.Provisioner, clusterManagement.Cluster, assetSet.GetWindow().Clone())
		cm.propertiesFromCluster(cmAsset.Properties)
		cmAsset.Cost = clusterManagement.Cost
		assetSet.Insert(cmAsset, nil)
	}
}

// propertiesFromCluster populates static cluster properties to individual asset properties
func (cm *CostModel) propertiesFromCluster(props *kubecost.AssetProperties) {
	// If properties does not have cluster value, do nothing
	if props.Cluster == "" {
		return
	}
	clusterMap := cm.ClusterMap.AsMap()
	ci, ok := clusterMap[props.Cluster]
	if !ok {
		log.DedupedWarningf(5, "Asset Cluster \"%s\" was not found in Cluster Map", props.Cluster)
		return
	}

	props.Project = ci.Project
	props.Account = ci.Account
	props.Provider = ci.Provider
}
