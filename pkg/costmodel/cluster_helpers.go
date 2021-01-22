package costmodel

import (
	"time"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
)

// mergeTypeMaps takes two maps of (cluster name, node name) -> node type
// and combines them into a single map, preferring the k/v pairs in
// the first map.
func mergeTypeMaps(clusterAndNameToType1, clusterAndNameToType2 map[nodeIdentifierNoProviderID]string) map[nodeIdentifierNoProviderID]string {
	merged := map[nodeIdentifierNoProviderID]string{}
	for k, v := range clusterAndNameToType2 {
		merged[k] = v
	}

	// This ordering ensures the mappings in the first arg are preferred.
	for k, v := range clusterAndNameToType1 {
		merged[k] = v
	}

	return merged
}

func buildCPUCostMap(
	resNodeCPUCost []*prom.QueryResult,
	providerIDParser func(string) string,
) (
	map[NodeIdentifier]float64,
	map[nodeIdentifierNoProviderID]string,
) {

	cpuCostMap := make(map[NodeIdentifier]float64)
	clusterAndNameToType := make(map[nodeIdentifierNoProviderID]string)

	for _, result := range resNodeCPUCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: CPU cost data missing node")
			continue
		}

		nodeType, _ := result.GetString("instance_type")
		providerID, _ := result.GetString("provider_id")

		cpuCost := result.Values[0].Value

		key := NodeIdentifier{
			Cluster:    cluster,
			Name:       name,
			ProviderID: providerIDParser(providerID),
		}
		keyNon := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}

		clusterAndNameToType[keyNon] = nodeType

		cpuCostMap[key] = cpuCost
	}

	return cpuCostMap, clusterAndNameToType
}

func buildRAMCostMap(
	resNodeRAMCost []*prom.QueryResult,
	providerIDParser func(string) string,
) (
	map[NodeIdentifier]float64,
	map[nodeIdentifierNoProviderID]string,
) {

	ramCostMap := make(map[NodeIdentifier]float64)
	clusterAndNameToType := make(map[nodeIdentifierNoProviderID]string)

	for _, result := range resNodeRAMCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: RAM cost data missing node")
			continue
		}

		nodeType, _ := result.GetString("instance_type")
		providerID, _ := result.GetString("provider_id")

		ramCost := result.Values[0].Value

		key := NodeIdentifier{
			Cluster:    cluster,
			Name:       name,
			ProviderID: providerIDParser(providerID),
		}
		keyNon := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}

		clusterAndNameToType[keyNon] = nodeType
		ramCostMap[key] = ramCost
	}

	return ramCostMap, clusterAndNameToType
}

func buildGPUCostMap(
	resNodeGPUCost []*prom.QueryResult,
	providerIDParser func(string) string,
) (
	map[NodeIdentifier]float64,
	map[nodeIdentifierNoProviderID]string,
) {

	gpuCostMap := make(map[NodeIdentifier]float64)
	clusterAndNameToType := make(map[nodeIdentifierNoProviderID]string)

	for _, result := range resNodeGPUCost {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: GPU cost data missing node")
			continue
		}

		nodeType, _ := result.GetString("instance_type")
		providerID, _ := result.GetString("provider_id")

		gpuCost := result.Values[0].Value

		key := NodeIdentifier{
			Cluster:    cluster,
			Name:       name,
			ProviderID: providerIDParser(providerID),
		}
		keyNon := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}

		clusterAndNameToType[keyNon] = nodeType

		gpuCostMap[key] = gpuCost
	}

	return gpuCostMap, clusterAndNameToType
}

func buildCPUCoresMap(
	resNodeCPUCores []*prom.QueryResult,
	clusterAndNameToType map[nodeIdentifierNoProviderID]string,
) map[nodeIdentifierNoProviderID]float64 {

	m := make(map[nodeIdentifierNoProviderID]float64)

	for _, result := range resNodeCPUCores {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: CPU cores data missing node")
			continue
		}

		cpuCores := result.Values[0].Value

		key := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}
		if nodeType, ok := clusterAndNameToType[key]; ok {
			if v, ok := partialCPUMap[nodeType]; ok {
				m[key] = v
				if cpuCores > 0 {
					adjustmentFactor := v / cpuCores
					m[key] = m[key] * adjustmentFactor
				}
			} else {
				m[key] = cpuCores
			}
		} else {
			m[key] = cpuCores
		}
	}

	return m
}

func buildRAMBytesMap(resNodeRAMBytes []*prom.QueryResult) map[nodeIdentifierNoProviderID]float64 {

	m := make(map[nodeIdentifierNoProviderID]float64)

	for _, result := range resNodeRAMBytes {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: RAM bytes data missing node")
			continue
		}

		ramBytes := result.Values[0].Value

		key := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}
		m[key] = ramBytes
	}

	return m
}

// Mapping of cluster/node=cpu for computing resource efficiency
func buildCPUBreakdownMap(resNodeCPUModeTotal []*prom.QueryResult) map[nodeIdentifierNoProviderID]*ClusterCostsBreakdown {

	cpuBreakdownMap := make(map[nodeIdentifierNoProviderID]*ClusterCostsBreakdown)

	// Mapping of cluster/node=cpu for computing resource efficiency
	clusterNodeCPUTotal := map[nodeIdentifierNoProviderID]float64{}
	// Mapping of cluster/node:mode=cpu for computing resource efficiency
	clusterNodeModeCPUTotal := map[nodeIdentifierNoProviderID]map[string]float64{}

	// Build intermediate structures for CPU usage by (cluster, node) and by
	// (cluster, node, mode) for computing resouce efficiency
	for _, result := range resNodeCPUModeTotal {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := result.GetString("kubernetes_node")
		if err != nil {
			log.DedupedWarningf(5, "ClusterNodes: CPU mode data missing node")
			continue
		}

		mode, err := result.GetString("mode")
		if err != nil {
			log.Warningf("ClusterNodes: unable to read CPU mode: %s", err)
			mode = "other"
		}

		key := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    node,
		}

		total := result.Values[0].Value

		// Increment total
		clusterNodeCPUTotal[key] += total

		// Increment mode
		if _, ok := clusterNodeModeCPUTotal[key]; !ok {
			clusterNodeModeCPUTotal[key] = map[string]float64{}
		}
		clusterNodeModeCPUTotal[key][mode] += total
	}

	// Compute resource efficiency from intermediate structures
	for key, total := range clusterNodeCPUTotal {
		if modeTotals, ok := clusterNodeModeCPUTotal[key]; ok {
			for mode, subtotal := range modeTotals {
				// Compute percentage for the current cluster, node, mode
				pct := 0.0
				if total > 0 {
					pct = subtotal / total
				}

				// TODO: revisit what this access is supposed to do
				// if _, ok := clusterNodeModeCPUTotal[key]; !ok {
				// 	log.Warningf("ClusterNodes: CPU mode data for unidentified node")
				// 	continue
				// }

				if _, ok := cpuBreakdownMap[key]; !ok {
					cpuBreakdownMap[key] = &ClusterCostsBreakdown{}
				}

				switch mode {
				case "idle":
					cpuBreakdownMap[key].Idle += pct
				case "system":
					cpuBreakdownMap[key].System += pct
				case "user":
					cpuBreakdownMap[key].User += pct
				default:
					cpuBreakdownMap[key].Other += pct
				}
			}
		}
	}

	return cpuBreakdownMap
}

func buildRAMUserPctMap(resNodeRAMUserPct []*prom.QueryResult) map[nodeIdentifierNoProviderID]float64 {

	m := make(map[nodeIdentifierNoProviderID]float64)

	for _, result := range resNodeRAMUserPct {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("instance")
		if err != nil {
			log.Warningf("ClusterNodes: RAM user percent missing node")
			continue
		}

		pct := result.Values[0].Value

		key := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}

		m[key] = pct
	}

	return m
}

func buildRAMSystemPctMap(resNodeRAMSystemPct []*prom.QueryResult) map[nodeIdentifierNoProviderID]float64 {

	m := make(map[nodeIdentifierNoProviderID]float64)

	for _, result := range resNodeRAMSystemPct {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("instance")
		if err != nil {
			log.Warningf("ClusterNodes: RAM system percent missing node")
			continue
		}

		pct := result.Values[0].Value

		key := nodeIdentifierNoProviderID{
			Cluster: cluster,
			Name:    name,
		}

		m[key] = pct
	}

	return m
}

type activeData struct {
	start   time.Time
	end     time.Time
	minutes float64
}

func buildActiveDataMap(resActiveMins []*prom.QueryResult, resolution time.Duration, providerIDParser func(string) string) map[NodeIdentifier]activeData {

	m := make(map[NodeIdentifier]activeData)

	for _, result := range resActiveMins {
		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := result.GetString("node")
		if err != nil {
			log.Warningf("ClusterNodes: active mins missing node")
			continue
		}

		providerID, _ := result.GetString("provider_id")

		key := NodeIdentifier{
			Cluster:    cluster,
			Name:       name,
			ProviderID: providerIDParser(providerID),
		}

		if len(result.Values) == 0 {
			continue
		}

		s := time.Unix(int64(result.Values[0].Timestamp), 0)
		e := time.Unix(int64(result.Values[len(result.Values)-1].Timestamp), 0).Add(resolution)
		mins := e.Sub(s).Minutes()

		// TODO niko/assets if mins >= threshold, interpolate for missing data?
		m[key] = activeData{
			start:   s,
			end:     e,
			minutes: mins,
		}
	}

	return m
}

// Determine preemptibility with node labels
// node id -> is preemptible?
func buildPreemptibleMap(
	resNodeLabels []*prom.QueryResult,
	providerIDParser func(string) string,
) map[NodeIdentifier]bool {

	m := make(map[NodeIdentifier]bool)

	for _, result := range resNodeLabels {
		nodeName, err := result.GetString("node")
		if err != nil {
			continue
		}

		// GCP preemptible label
		pre := result.Values[0].Value

		cluster, err := result.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		providerID, _ := result.GetString("provider_id")

		key := NodeIdentifier{
			Cluster:    cluster,
			Name:       nodeName,
			ProviderID: providerIDParser(providerID),
		}

		// TODO(michaelmdresser): check this condition at merge time?
		// if node, ok := nodeMap[key]; pre > 0.0 && ok {
		// 	node.Preemptible = true
		// }
		m[key] = pre > 0.0

		// TODO AWS preemptible

		// TODO Azure preemptible
	}

	return m
}

// checkForKeyAndInitIfMissing inits a key in the provided nodemap if
// it does not exist. Intended to be called ONLY by buildNodeMap
func checkForKeyAndInitIfMissing(
	nodeMap map[NodeIdentifier]*Node,
	key NodeIdentifier,
	clusterAndNameToType map[nodeIdentifierNoProviderID]string,
) {
	if _, ok := nodeMap[key]; !ok {
		// default nodeType in case we don't have the mapping
		var nodeType string
		if t, ok := clusterAndNameToType[nodeIdentifierNoProviderID{
			Cluster: key.Cluster,
			Name:    key.Name,
		}]; ok {
			nodeType = t
		} else {
			log.Warningf("ClusterNodes: Type does not exist for node identifier %s", key)
		}

		nodeMap[key] = &Node{
			Cluster:      key.Cluster,
			Name:         key.Name,
			NodeType:     nodeType,
			ProviderID:   key.ProviderID,
			CPUBreakdown: &ClusterCostsBreakdown{},
			RAMBreakdown: &ClusterCostsBreakdown{},
		}
	}
}

func buildNodeMap(
	cpuCostMap, ramCostMap, gpuCostMap map[NodeIdentifier]float64,
	cpuCoresMap, ramBytesMap, ramUserPctMap,
	ramSystemPctMap map[nodeIdentifierNoProviderID]float64,
	cpuBreakdownMap map[nodeIdentifierNoProviderID]*ClusterCostsBreakdown,
	activeDataMap map[NodeIdentifier]activeData,
	preemptibleMap map[NodeIdentifier]bool,
	clusterAndNameToType map[nodeIdentifierNoProviderID]string,
) map[NodeIdentifier]*Node {

	nodeMap := make(map[NodeIdentifier]*Node)

	for id, cost := range cpuCostMap {
		checkForKeyAndInitIfMissing(nodeMap, id, clusterAndNameToType)
		nodeMap[id].CPUCost = cost
	}

	for id, cost := range ramCostMap {
		checkForKeyAndInitIfMissing(nodeMap, id, clusterAndNameToType)
		nodeMap[id].RAMCost = cost
	}

	for id, cost := range gpuCostMap {
		checkForKeyAndInitIfMissing(nodeMap, id, clusterAndNameToType)
		nodeMap[id].GPUCost = cost
	}

	for id, preemptible := range preemptibleMap {
		checkForKeyAndInitIfMissing(nodeMap, id, clusterAndNameToType)
		nodeMap[id].Preemptible = preemptible
	}

	for id, activeData := range activeDataMap {
		checkForKeyAndInitIfMissing(nodeMap, id, clusterAndNameToType)
		nodeMap[id].Start = activeData.start
		nodeMap[id].End = activeData.end
		nodeMap[id].Minutes = activeData.minutes
	}

	// We now merge in data that doesn't have a provider id by looping over
	// all keys already added and inserting data according to their
	// cluster name/node name combos.
	for id, nodePtr := range nodeMap {
		clusterAndNameID := nodeIdentifierNoProviderID{
			Cluster: id.Cluster,
			Name:    id.Name,
		}

		if cores, ok := cpuCoresMap[clusterAndNameID]; ok {
			nodePtr.CPUCores = cores
		}

		if ramBytes, ok := ramBytesMap[clusterAndNameID]; ok {
			nodePtr.RAMBytes = ramBytes
		}

		if ramUserPct, ok := ramUserPctMap[clusterAndNameID]; ok {
			nodePtr.RAMBreakdown.User = ramUserPct
		}

		if ramSystemPct, ok := ramSystemPctMap[clusterAndNameID]; ok {
			nodePtr.RAMBreakdown.System = ramSystemPct
		}

		if cpuBreakdown, ok := cpuBreakdownMap[clusterAndNameID]; ok {
			nodePtr.CPUBreakdown = cpuBreakdown
		}
	}

	return nodeMap
}
