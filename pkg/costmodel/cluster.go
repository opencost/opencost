package costmodel

import (
	"net"
	"strconv"
	"strings"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"golang.org/x/exp/slices"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

const MAX_LOCAL_STORAGE_SIZE = 1024 * 1024 * 1024 * 1024

// When ASSET_INCLUDE_LOCAL_DISK_COST is set to false, local storage
// provisioned by sig-storage-local-static-provisioner is excluded
// by checking if the volume is prefixed by "local-pv-".
//
// This is based on the sig-storage-local-static-provisioner implementation,
// which creates all PVs with the "local-pv-" prefix. For reference, see:
// https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/blob/b6f465027bd059e92c0032c81dd1e1d90e35c909/pkg/discovery/discovery.go#L410-L417
const SIG_STORAGE_LOCAL_PROVISIONER_PREFIX = "local-pv-"

// Costs represents cumulative and monthly cluster costs over a given duration. Costs
// are broken down by cores, memory, and storage.
type ClusterCosts struct {
	Start             *time.Time             `json:"startTime"`
	End               *time.Time             `json:"endTime"`
	CPUCumulative     float64                `json:"cpuCumulativeCost"`
	CPUMonthly        float64                `json:"cpuMonthlyCost"`
	CPUBreakdown      *ClusterCostsBreakdown `json:"cpuBreakdown"`
	GPUCumulative     float64                `json:"gpuCumulativeCost"`
	GPUMonthly        float64                `json:"gpuMonthlyCost"`
	RAMCumulative     float64                `json:"ramCumulativeCost"`
	RAMMonthly        float64                `json:"ramMonthlyCost"`
	RAMBreakdown      *ClusterCostsBreakdown `json:"ramBreakdown"`
	StorageCumulative float64                `json:"storageCumulativeCost"`
	StorageMonthly    float64                `json:"storageMonthlyCost"`
	StorageBreakdown  *ClusterCostsBreakdown `json:"storageBreakdown"`
	TotalCumulative   float64                `json:"totalCumulativeCost"`
	TotalMonthly      float64                `json:"totalMonthlyCost"`
	DataMinutes       float64
}

// ClusterCostsBreakdown provides percentage-based breakdown of a resource by
// categories: user for user-space (i.e. non-system) usage, system, and idle.
type ClusterCostsBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

type Disk struct {
	Cluster        string
	Name           string
	ProviderID     string
	StorageClass   string
	VolumeName     string
	ClaimName      string
	ClaimNamespace string
	Cost           float64
	Bytes          float64

	// These two fields may not be available at all times because they rely on
	// a new set of metrics that may or may not be available. Thus, they must
	// be nilable to represent the complete absence of the data.
	//
	// In other words, nilability here lets us distinguish between
	// "metric is not available" and "metric is available but is 0".
	//
	// They end in "Ptr" to distinguish from an earlier version in order to
	// ensure that all usages are checked for nil.
	BytesUsedAvgPtr *float64
	BytesUsedMaxPtr *float64

	Local     bool
	Start     time.Time
	End       time.Time
	Minutes   float64
	Breakdown *ClusterCostsBreakdown
}

type DiskIdentifier struct {
	Cluster string
	Name    string
}

func ClusterDisks(dataSource source.OpenCostDataSource, cp models.Provider, start, end time.Time) (map[DiskIdentifier]*Disk, error) {
	resolution := dataSource.Resolution()

	grp := source.NewQueryGroup()
	mq := dataSource.Metrics()

	resChPVCost := source.WithGroup(grp, mq.QueryPVPricePerGiBHour(start, end))
	resChPVSize := source.WithGroup(grp, mq.QueryPVBytes(start, end))
	resChActiveMins := source.WithGroup(grp, mq.QueryPVActiveMinutes(start, end))
	resChPVStorageClass := source.WithGroup(grp, mq.QueryPVInfo(start, end))
	resChPVUsedAvg := source.WithGroup(grp, mq.QueryPVUsedAverage(start, end))
	resChPVUsedMax := source.WithGroup(grp, mq.QueryPVUsedMax(start, end))
	resChPVCInfo := source.WithGroup(grp, mq.QueryPVCInfo(start, end))

	resPVCost, _ := resChPVCost.Await()
	resPVSize, _ := resChPVSize.Await()
	resActiveMins, _ := resChActiveMins.Await()
	resPVStorageClass, _ := resChPVStorageClass.Await()
	resPVUsedAvg, _ := resChPVUsedAvg.Await()
	resPVUsedMax, _ := resChPVUsedMax.Await()
	resPVCInfo, _ := resChPVCInfo.Await()

	// Cloud providers do not always charge for a node's local disk costs (i.e.
	// ephemeral storage). Provide an option to opt out of calculating &
	// allocating local disk costs. Note, that this does not affect
	// PersistentVolume costs.
	//
	// Ref:
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/RootDeviceStorage.html
	// https://learn.microsoft.com/en-us/azure/virtual-machines/managed-disks-overview#temporary-disk
	// https://cloud.google.com/compute/docs/disks/local-ssd
	resLocalStorageCost := []*source.LocalStorageCostResult{}
	resLocalStorageUsedCost := []*source.LocalStorageUsedCostResult{}
	resLocalStorageUsedAvg := []*source.LocalStorageUsedAvgResult{}
	resLocalStorageUsedMax := []*source.LocalStorageUsedMaxResult{}
	resLocalStorageBytes := []*source.LocalStorageBytesResult{}
	resLocalActiveMins := []*source.LocalStorageActiveMinutesResult{}

	if env.IsAssetIncludeLocalDiskCost() {
		resChLocalStorageCost := source.WithGroup(grp, mq.QueryLocalStorageCost(start, end))
		resChLocalStorageUsedCost := source.WithGroup(grp, mq.QueryLocalStorageUsedCost(start, end))
		resChLocalStoreageUsedAvg := source.WithGroup(grp, mq.QueryLocalStorageUsedAvg(start, end))
		resChLocalStoreageUsedMax := source.WithGroup(grp, mq.QueryLocalStorageUsedMax(start, end))
		resChLocalStorageBytes := source.WithGroup(grp, mq.QueryLocalStorageBytes(start, end))
		resChLocalActiveMins := source.WithGroup(grp, mq.QueryLocalStorageActiveMinutes(start, end))

		resLocalStorageCost, _ = resChLocalStorageCost.Await()
		resLocalStorageUsedCost, _ = resChLocalStorageUsedCost.Await()
		resLocalStorageUsedAvg, _ = resChLocalStoreageUsedAvg.Await()
		resLocalStorageUsedMax, _ = resChLocalStoreageUsedMax.Await()
		resLocalStorageBytes, _ = resChLocalStorageBytes.Await()
		resLocalActiveMins, _ = resChLocalActiveMins.Await()
	}

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	diskMap := buildAssetsPVCMap(resPVCInfo)

	pvCosts(diskMap, resolution, resActiveMins, resPVSize, resPVCost, resPVUsedAvg, resPVUsedMax, resPVCInfo, cp, opencost.NewClosedWindow(start, end))

	type localStorage struct {
		device string
		disk   *Disk
	}

	localStorageDisks := map[DiskIdentifier]localStorage{}

	// Start with local storage bytes so that the device with the largest size which has passed the
	// query filters can be determined
	for _, result := range resLocalStorageBytes {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Instance
		if name == "" {
			log.Warnf("ClusterDisks: local storage data missing instance")
			continue
		}

		device := result.Device
		if device == "" {
			log.Warnf("ClusterDisks: local storage data missing device")
			continue
		}

		bytes := result.Data[0].Value
		// Ignore disks that are larger than the max size
		if bytes > MAX_LOCAL_STORAGE_SIZE {
			continue
		}

		key := DiskIdentifier{cluster, name}

		// only keep the device with the most bytes per instance
		if current, ok := localStorageDisks[key]; !ok || current.disk.Bytes < bytes {
			localStorageDisks[key] = localStorage{
				device: device,
				disk: &Disk{
					Cluster:      cluster,
					Name:         name,
					Breakdown:    &ClusterCostsBreakdown{},
					Local:        true,
					StorageClass: opencost.LocalStorageClass,
					Bytes:        bytes,
				},
			}
		}
	}

	for _, result := range resLocalStorageCost {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Instance
		if name == "" {
			log.Warnf("ClusterDisks: local storage data missing instance")
			continue
		}

		device := result.Device
		if device == "" {
			log.Warnf("ClusterDisks: local storage data missing device")
			continue
		}

		cost := result.Data[0].Value
		key := DiskIdentifier{cluster, name}
		ls, ok := localStorageDisks[key]
		if !ok || ls.device != device {
			continue
		}
		ls.disk.Cost = cost

	}

	for _, result := range resLocalStorageUsedCost {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Instance
		if name == "" {
			log.Warnf("ClusterDisks: local storage data missing instance")
			continue
		}

		device := result.Device
		if device == "" {
			log.Warnf("ClusterDisks: local storage data missing device")
			continue
		}

		cost := result.Data[0].Value
		key := DiskIdentifier{cluster, name}
		ls, ok := localStorageDisks[key]
		if !ok || ls.device != device {
			continue
		}
		ls.disk.Breakdown.System = cost / ls.disk.Cost
	}

	for _, result := range resLocalStorageUsedAvg {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Instance
		if name == "" {
			log.Warnf("ClusterDisks: local storage data missing instance")
			continue
		}

		device := result.Device
		if device == "" {
			log.Warnf("ClusterDisks: local storage data missing device")
			continue
		}

		bytesAvg := result.Data[0].Value
		key := DiskIdentifier{cluster, name}
		ls, ok := localStorageDisks[key]
		if !ok || ls.device != device {
			continue
		}
		ls.disk.BytesUsedAvgPtr = &bytesAvg
	}

	for _, result := range resLocalStorageUsedMax {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Instance
		if name == "" {
			log.Warnf("ClusterDisks: local storage data missing instance")
			continue
		}

		device := result.Device
		if device == "" {
			log.Warnf("ClusterDisks: local storage data missing device")
			continue
		}

		bytesMax := result.Data[0].Value
		key := DiskIdentifier{cluster, name}
		ls, ok := localStorageDisks[key]
		if !ok || ls.device != device {
			continue
		}
		ls.disk.BytesUsedMaxPtr = &bytesMax
	}

	for _, result := range resLocalActiveMins {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.Node
		if name == "" {
			log.DedupedWarningf(5, "ClusterDisks: local active mins data missing instance")
			continue
		}

		providerID := result.ProviderID
		if providerID == "" {
			log.DedupedWarningf(5, "ClusterDisks: local active mins data missing provider_id")
			continue
		}

		key := DiskIdentifier{cluster, name}
		ls, ok := localStorageDisks[key]
		if !ok {
			continue
		}

		ls.disk.ProviderID = provider.ParseLocalDiskID(providerID)

		if len(result.Data) == 0 {
			continue
		}

		s := time.Unix(int64(result.Data[0].Timestamp), 0)
		e := time.Unix(int64(result.Data[len(result.Data)-1].Timestamp), 0)
		mins := e.Sub(s).Minutes()

		// TODO niko/assets if mins >= threshold, interpolate for missing data?

		ls.disk.End = e
		ls.disk.Start = s
		ls.disk.Minutes = mins
	}

	// move local storage disks to main disk map
	for key, ls := range localStorageDisks {
		diskMap[key] = ls.disk
	}

	var unTracedDiskLogData []DiskIdentifier
	//Iterating through Persistent Volume given by custom metrics kubecost_pv_info and assign the storage class if known and __unknown__ if not populated.
	for _, result := range resPVStorageClass {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.PersistentVolume
		key := DiskIdentifier{cluster, name}
		if _, ok := diskMap[key]; !ok {
			if !slices.Contains(unTracedDiskLogData, key) {
				unTracedDiskLogData = append(unTracedDiskLogData, key)
			}
			continue
		}

		if len(result.Data) == 0 {
			continue
		}

		storageClass := result.StorageClass
		if storageClass == "" {
			diskMap[key].StorageClass = opencost.UnknownStorageClass
		} else {
			diskMap[key].StorageClass = storageClass
		}
	}

	// Logging the unidentified disk information outside the loop

	for _, unIdentifiedDisk := range unTracedDiskLogData {
		log.Warnf("ClusterDisks: Cluster %s has Storage Class information for unidentified disk %s or disk deleted from analysis", unIdentifiedDisk.Cluster, unIdentifiedDisk.Name)
	}

	for _, disk := range diskMap {
		// Apply all remaining RAM to Idle
		disk.Breakdown.Idle = 1.0 - (disk.Breakdown.System + disk.Breakdown.Other + disk.Breakdown.User)

		// Set provider Id to the name for reconciliation
		if disk.ProviderID == "" {
			disk.ProviderID = disk.Name
		}
	}

	if !env.IsAssetIncludeLocalDiskCost() {
		return filterOutLocalPVs(diskMap), nil
	}

	return diskMap, nil
}

type NodeOverhead struct {
	CpuOverheadFraction float64
	RamOverheadFraction float64
}
type Node struct {
	Cluster         string
	Name            string
	ProviderID      string
	NodeType        string
	CPUCost         float64
	CPUCores        float64
	GPUCost         float64
	GPUCount        float64
	RAMCost         float64
	RAMBytes        float64
	Discount        float64
	Preemptible     bool
	CPUBreakdown    *ClusterCostsBreakdown
	RAMBreakdown    *ClusterCostsBreakdown
	Start           time.Time
	End             time.Time
	Minutes         float64
	Labels          map[string]string
	CostPerCPUHr    float64
	CostPerRAMGiBHr float64
	CostPerGPUHr    float64
	Overhead        *NodeOverhead
}

// GKE lies about the number of cores e2 nodes have. This table
// contains a mapping from node type -> actual CPU cores
// for those cases.
var partialCPUMap = map[string]float64{
	"e2-micro":  0.25,
	"e2-small":  0.5,
	"e2-medium": 1.0,
}

type NodeIdentifier struct {
	Cluster    string
	Name       string
	ProviderID string
}

type nodeIdentifierNoProviderID struct {
	Cluster string
	Name    string
}

type ClusterManagementIdentifier struct {
	Cluster     string
	Provisioner string
}

type ClusterManagementCost struct {
	Cluster     string
	Provisioner string

	Cost float64
}

func costTimesMinuteAndCount(activeDataMap map[NodeIdentifier]activeData, costMap map[NodeIdentifier]float64, resourceCountMap map[nodeIdentifierNoProviderID]float64) {
	for k, v := range activeDataMap {
		keyNon := nodeIdentifierNoProviderID{
			Cluster: k.Cluster,
			Name:    k.Name,
		}
		if cost, ok := costMap[k]; ok {
			minutes := v.minutes
			count := 1.0
			if c, ok := resourceCountMap[keyNon]; ok {
				count = c
			}
			costMap[k] = cost * (minutes / 60.0) * count
		}
	}
}

func costTimesMinute[T comparable](activeDataMap map[T]activeData, costMap map[T]float64) {
	for k, v := range activeDataMap {
		if cost, ok := costMap[k]; ok {
			minutes := v.minutes
			costMap[k] = cost * (minutes / 60)
		}
	}
}

func ClusterNodes(dataSource source.OpenCostDataSource, cp models.Provider, start, end time.Time) (map[NodeIdentifier]*Node, error) {
	mq := dataSource.Metrics()
	resolution := dataSource.Resolution()

	requiredGrp := source.NewQueryGroup()
	optionalGrp := source.NewQueryGroup()

	// return errors if these fail
	resChNodeCPUHourlyCost := source.WithGroup(requiredGrp, mq.QueryNodeCPUPricePerHr(start, end))
	resChNodeCPUCoresCapacity := source.WithGroup(requiredGrp, mq.QueryNodeCPUCoresCapacity(start, end))
	resChNodeCPUCoresAllocatable := source.WithGroup(requiredGrp, mq.QueryNodeCPUCoresAllocatable(start, end))
	resChNodeRAMHourlyCost := source.WithGroup(requiredGrp, mq.QueryNodeRAMPricePerGiBHr(start, end))
	resChNodeRAMBytesCapacity := source.WithGroup(requiredGrp, mq.QueryNodeRAMBytesCapacity(start, end))
	resChNodeRAMBytesAllocatable := source.WithGroup(requiredGrp, mq.QueryNodeRAMBytesAllocatable(start, end))
	resChNodeGPUCount := source.WithGroup(requiredGrp, mq.QueryNodeGPUCount(start, end))
	resChNodeGPUHourlyPrice := source.WithGroup(requiredGrp, mq.QueryNodeGPUPricePerHr(start, end))
	resChActiveMins := source.WithGroup(requiredGrp, mq.QueryNodeActiveMinutes(start, end))
	resChIsSpot := source.WithGroup(requiredGrp, mq.QueryNodeIsSpot(start, end))

	// Do not return errors if these fail, but log warnings
	resChNodeCPUModeTotal := source.WithGroup(optionalGrp, mq.QueryNodeCPUModeTotal(start, end))
	resChNodeRAMSystemPct := source.WithGroup(optionalGrp, mq.QueryNodeRAMSystemPercent(start, end))
	resChNodeRAMUserPct := source.WithGroup(optionalGrp, mq.QueryNodeRAMUserPercent(start, end))
	resChLabels := source.WithGroup(optionalGrp, mq.QueryNodeLabels(start, end))

	resNodeCPUHourlyCost, _ := resChNodeCPUHourlyCost.Await()
	resNodeCPUCoresCapacity, _ := resChNodeCPUCoresCapacity.Await()
	resNodeCPUCoresAllocatable, _ := resChNodeCPUCoresAllocatable.Await()
	resNodeGPUCount, _ := resChNodeGPUCount.Await()
	resNodeGPUHourlyPrice, _ := resChNodeGPUHourlyPrice.Await()
	resNodeRAMHourlyCost, _ := resChNodeRAMHourlyCost.Await()
	resNodeRAMBytesCapacity, _ := resChNodeRAMBytesCapacity.Await()
	resNodeRAMBytesAllocatable, _ := resChNodeRAMBytesAllocatable.Await()
	resIsSpot, _ := resChIsSpot.Await()
	resNodeCPUModeTotal, _ := resChNodeCPUModeTotal.Await()
	resNodeRAMSystemPct, _ := resChNodeRAMSystemPct.Await()
	resNodeRAMUserPct, _ := resChNodeRAMUserPct.Await()
	resActiveMins, _ := resChActiveMins.Await()
	resLabels, _ := resChLabels.Await()

	if optionalGrp.HasErrors() {
		for _, err := range optionalGrp.Errors() {
			log.Warnf("ClusterNodes: %s", err)
		}
	}
	if requiredGrp.HasErrors() {
		for _, err := range requiredGrp.Errors() {
			log.Errorf("ClusterNodes: %s", err)
		}

		return nil, requiredGrp.Error()
	}

	activeDataMap := buildActiveDataMap(resActiveMins, nodeKeyGen, nodeValues, resolution, opencost.NewClosedWindow(start, end))

	gpuCountMap := buildGPUCountMap(resNodeGPUCount)
	preemptibleMap := buildPreemptibleMap(resIsSpot)

	cpuCostMap, clusterAndNameToType1 := buildCPUCostMap(resNodeCPUHourlyCost, cp, preemptibleMap)
	ramCostMap, clusterAndNameToType2 := buildRAMCostMap(resNodeRAMHourlyCost, cp, preemptibleMap)
	gpuCostMap, clusterAndNameToType3 := buildGPUCostMap(resNodeGPUHourlyPrice, gpuCountMap, cp, preemptibleMap)

	clusterAndNameToTypeIntermediate := mergeTypeMaps(clusterAndNameToType1, clusterAndNameToType2)
	clusterAndNameToType := mergeTypeMaps(clusterAndNameToTypeIntermediate, clusterAndNameToType3)

	cpuCoresCapacityMap := buildCPUCoresMap(resNodeCPUCoresCapacity)
	ramBytesCapacityMap := buildRAMBytesMap(resNodeRAMBytesCapacity)

	cpuCoresAllocatableMap := buildCPUCoresMap(resNodeCPUCoresAllocatable)
	ramBytesAllocatableMap := buildRAMBytesMap(resNodeRAMBytesAllocatable)
	overheadMap := buildOverheadMap(ramBytesCapacityMap, ramBytesAllocatableMap, cpuCoresCapacityMap, cpuCoresAllocatableMap)

	ramUserPctMap := buildRAMUserPctMap(resNodeRAMUserPct)
	ramSystemPctMap := buildRAMSystemPctMap(resNodeRAMSystemPct)

	cpuBreakdownMap := buildCPUBreakdownMap(resNodeCPUModeTotal)

	labelsMap := buildLabelsMap(resLabels)

	costTimesMinuteAndCount(activeDataMap, cpuCostMap, cpuCoresCapacityMap)
	costTimesMinuteAndCount(activeDataMap, ramCostMap, ramBytesCapacityMap)
	costTimesMinute(activeDataMap, gpuCostMap) // there's no need to do a weird "nodeIdentifierNoProviderID" type match since gpuCounts have a providerID

	nodeMap := buildNodeMap(
		cpuCostMap, ramCostMap, gpuCostMap, gpuCountMap,
		cpuCoresCapacityMap, ramBytesCapacityMap, ramUserPctMap,
		ramSystemPctMap,
		cpuBreakdownMap,
		activeDataMap,
		preemptibleMap,
		labelsMap,
		clusterAndNameToType,
		overheadMap,
	)

	c, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}

	discount, err := ParsePercentString(c.Discount)
	if err != nil {
		return nil, err
	}

	negotiatedDiscount, err := ParsePercentString(c.NegotiatedDiscount)
	if err != nil {
		return nil, err
	}

	for _, node := range nodeMap {
		// TODO take GKE Reserved Instances into account
		node.Discount = cp.CombinedDiscountForNode(node.NodeType, node.Preemptible, discount, negotiatedDiscount)

		// Apply all remaining resources to Idle
		node.CPUBreakdown.Idle = 1.0 - (node.CPUBreakdown.System + node.CPUBreakdown.Other + node.CPUBreakdown.User)
		node.RAMBreakdown.Idle = 1.0 - (node.RAMBreakdown.System + node.RAMBreakdown.Other + node.RAMBreakdown.User)
	}

	return nodeMap, nil
}

type LoadBalancerIdentifier struct {
	Cluster   string
	Namespace string
	Name      string
	IngressIP string
}

type LoadBalancer struct {
	Cluster    string
	Namespace  string
	Name       string
	ProviderID string
	Cost       float64
	Start      time.Time
	End        time.Time
	Minutes    float64
	Private    bool
	Ip         string
}

func ClusterLoadBalancers(dataSource source.OpenCostDataSource, start, end time.Time) (map[LoadBalancerIdentifier]*LoadBalancer, error) {
	resolution := dataSource.Resolution()

	grp := source.NewQueryGroup()
	mq := dataSource.Metrics()

	resChLBCost := source.WithGroup(grp, mq.QueryLBPricePerHr(start, end))
	resChActiveMins := source.WithGroup(grp, mq.QueryLBActiveMinutes(start, end))

	resLBCost, _ := resChLBCost.Await()
	resActiveMins, _ := resChActiveMins.Await()

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	loadBalancerMap := make(map[LoadBalancerIdentifier]*LoadBalancer, len(resActiveMins))
	activeMap := buildActiveDataMap(resActiveMins, loadBalancerKeyGen, lbValues, resolution, opencost.NewClosedWindow(start, end))

	for _, result := range resLBCost {
		key, ok := loadBalancerKeyGen(result)
		if !ok {
			continue
		}

		lbPricePerHr := result.Data[0].Value

		lb := &LoadBalancer{
			Cluster:    key.Cluster,
			Namespace:  key.Namespace,
			Name:       key.Name,
			Cost:       lbPricePerHr, // default to hourly cost, overwrite if active entry exists
			Ip:         key.IngressIP,
			Private:    privateIPCheck(key.IngressIP),
			ProviderID: provider.ParseLBID(key.IngressIP),
		}

		if active, ok := activeMap[key]; ok {
			lb.Start = active.start
			lb.End = active.end
			lb.Minutes = active.minutes

			if lb.Minutes > 0 {
				lb.Cost = lbPricePerHr * (lb.Minutes / 60.0)
			} else {
				log.DedupedWarningf(20, "ClusterLoadBalancers: found zero minutes for key: %v", key)
			}
		}

		loadBalancerMap[key] = lb
	}

	return loadBalancerMap, nil
}

func ClusterManagement(dataSource source.OpenCostDataSource, start, end time.Time) (map[ClusterManagementIdentifier]*ClusterManagementCost, error) {
	resolution := dataSource.Resolution()

	grp := source.NewQueryGroup()
	mq := dataSource.Metrics()

	resChCMPrice := source.WithGroup(grp, mq.QueryClusterManagementPricePerHr(start, end))
	resChCMDur := source.WithGroup(grp, mq.QueryClusterManagementDuration(start, end))

	resCMPrice, _ := resChCMPrice.Await()
	resCMDur, _ := resChCMDur.Await()

	if grp.HasErrors() {
		return nil, grp.Error()
	}

	clusterManagementPriceMap := make(map[ClusterManagementIdentifier]*ClusterManagementCost, len(resCMDur))
	activeMap := buildActiveDataMap(resCMDur, clusterManagementKeyGen, clusterManagementValues, resolution, opencost.NewClosedWindow(start, end))

	for _, result := range resCMPrice {
		key, ok := clusterManagementKeyGen(result)
		if !ok {
			continue
		}

		cmPricePerHr := result.Data[0].Value
		cm := &ClusterManagementCost{
			Cluster:     key.Cluster,
			Provisioner: key.Provisioner,
			Cost:        cmPricePerHr, // default to hourly cost, overwrite if active entry exists
		}

		if active, ok := activeMap[key]; ok {
			if active.minutes > 0 {
				cm.Cost = cmPricePerHr * (active.minutes / 60.0)
			} else {
				log.DedupedWarningf(20, "ClusterManagement: found zero minutes for key: %v", key)
			}
		}

		clusterManagementPriceMap[key] = cm
	}

	return clusterManagementPriceMap, nil
}

// Check if an ip is private.
func privateIPCheck(ip string) bool {
	ipAddress := net.ParseIP(ip)
	return ipAddress.IsPrivate()
}

func pvCosts(
	diskMap map[DiskIdentifier]*Disk,
	resolution time.Duration,
	resActiveMins []*source.PVActiveMinutesResult,
	resPVSize []*source.PVBytesResult,
	resPVCost []*source.PVPricePerGiBHourResult,
	resPVUsedAvg []*source.PVUsedAvgResult,
	resPVUsedMax []*source.PVUsedMaxResult,
	resPVCInfo []*source.PVCInfoResult,
	cp models.Provider,
	window opencost.Window,
) {
	for _, result := range resActiveMins {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.PersistentVolume
		if name == "" {
			log.Warnf("ClusterDisks: active mins missing pv name")
			continue
		}

		if len(result.Data) == 0 {
			continue
		}

		key := DiskIdentifier{
			Cluster: cluster,
			Name:    name,
		}
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}

		s, e := calculateStartAndEnd(result.Data, resolution, window)
		mins := e.Sub(s).Minutes()

		diskMap[key].End = e
		diskMap[key].Start = s
		diskMap[key].Minutes = mins
	}

	for _, result := range resPVSize {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.PersistentVolume
		if name == "" {
			log.Warnf("ClusterDisks: PV size data missing persistentvolume")
			continue
		}

		// TODO niko/assets storage class

		bytes := result.Data[0].Value
		key := DiskIdentifier{cluster, name}
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}
		diskMap[key].Bytes = bytes
	}

	customPricingEnabled := provider.CustomPricesEnabled(cp)
	customPricingConfig, err := cp.GetConfig()
	if err != nil {
		log.Warnf("ClusterDisks: failed to load custom pricing: %s", err)
	}

	for _, result := range resPVCost {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		name := result.PersistentVolume
		if name == "" {
			log.Warnf("ClusterDisks: PV cost data missing persistentvolume")
			continue
		}

		// TODO niko/assets storage class

		var cost float64
		if customPricingEnabled && customPricingConfig != nil {
			customPVCostStr := customPricingConfig.Storage

			customPVCost, err := strconv.ParseFloat(customPVCostStr, 64)
			if err != nil {
				log.Warnf("ClusterDisks: error parsing custom PV price: %s", customPVCostStr)
			}

			cost = customPVCost
		} else {
			cost = result.Data[0].Value
		}

		key := DiskIdentifier{cluster, name}
		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      name,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}

		diskMap[key].Cost = cost * (diskMap[key].Bytes / 1024 / 1024 / 1024) * (diskMap[key].Minutes / 60)
		providerID := result.ProviderID // just put the providerID set up here, it's the simplest query.
		if providerID != "" {
			diskMap[key].ProviderID = provider.ParsePVID(providerID)
		}
	}

	for _, result := range resPVUsedAvg {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		claimName := result.PersistentVolumeClaim
		if claimName == "" {
			log.Debugf("ClusterDisks: pv usage data missing persistentvolumeclaim")
			continue
		}

		claimNamespace := result.Namespace
		if claimNamespace == "" {
			log.Debugf("ClusterDisks: pv usage data missing namespace")
			continue
		}

		var volumeName string

		for _, thatRes := range resPVCInfo {

			thatCluster := thatRes.Cluster
			if thatCluster == "" {
				thatCluster = coreenv.GetClusterID()
			}

			thatVolumeName := thatRes.VolumeName
			if thatVolumeName == "" {
				log.Debugf("ClusterDisks: pv claim data missing volumename")
				continue
			}

			thatClaimName := thatRes.PersistentVolumeClaim
			if thatClaimName == "" {
				log.Debugf("ClusterDisks: pv claim data missing persistentvolumeclaim")
				continue
			}

			thatClaimNamespace := thatRes.Namespace
			if thatClaimNamespace == "" {
				log.Debugf("ClusterDisks: pv claim data missing namespace")
				continue
			}

			if cluster == thatCluster && claimName == thatClaimName && claimNamespace == thatClaimNamespace {
				volumeName = thatVolumeName
			}
		}

		usage := result.Data[0].Value

		key := DiskIdentifier{
			Cluster: cluster,
			Name:    volumeName,
		}

		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      volumeName,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}
		diskMap[key].BytesUsedAvgPtr = &usage
	}

	for _, result := range resPVUsedMax {
		cluster := result.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		claimName := result.PersistentVolumeClaim
		if claimName == "" {
			log.Debugf("ClusterDisks: pv usage data missing persistentvolumeclaim")
			continue
		}

		claimNamespace := result.Namespace
		if claimNamespace == "" {
			log.Debugf("ClusterDisks: pv usage data missing namespace")
			continue
		}

		var volumeName string

		for _, thatRes := range resPVCInfo {
			thatCluster := thatRes.Cluster
			if thatCluster == "" {
				thatCluster = coreenv.GetClusterID()
			}

			thatVolumeName := thatRes.VolumeName
			if thatVolumeName == "" {
				log.Debugf("ClusterDisks: pv claim data missing volumename")
				continue
			}

			thatClaimName := thatRes.PersistentVolumeClaim
			if thatClaimName == "" {
				log.Debugf("ClusterDisks: pv claim data missing persistentvolumeclaim")
				continue
			}

			thatClaimNamespace := thatRes.Namespace
			if thatClaimNamespace == "" {
				log.Debugf("ClusterDisks: pv claim data missing namespace")
				continue
			}

			if cluster == thatCluster && claimName == thatClaimName && claimNamespace == thatClaimNamespace {
				volumeName = thatVolumeName
			}
		}

		usage := result.Data[0].Value

		key := DiskIdentifier{cluster, volumeName}

		if _, ok := diskMap[key]; !ok {
			diskMap[key] = &Disk{
				Cluster:   cluster,
				Name:      volumeName,
				Breakdown: &ClusterCostsBreakdown{},
			}
		}
		diskMap[key].BytesUsedMaxPtr = &usage
	}
}

// filterOutLocalPVs removes local Persistent Volumes (PVs) from the given disk map.
// Local PVs are identified by the prefix "local-pv-" in their names, which is the
// convention used by sig-storage-local-static-provisioner.
//
// Parameters:
//   - diskMap: A map of DiskIdentifier to Disk pointers, representing all PVs.
//
// Returns:
//   - A new map of DiskIdentifier to Disk pointers, containing only non-local PVs.
func filterOutLocalPVs(diskMap map[DiskIdentifier]*Disk) map[DiskIdentifier]*Disk {
	nonLocalPVDiskMap := map[DiskIdentifier]*Disk{}
	for key, val := range diskMap {
		if !strings.HasPrefix(key.Name, SIG_STORAGE_LOCAL_PROVISIONER_PREFIX) {
			nonLocalPVDiskMap[key] = val
		}
	}
	return nonLocalPVDiskMap
}
