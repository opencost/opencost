package costmodel

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/metrics"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util"

	promclient "github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"

	"k8s.io/client-go/kubernetes"

	"k8s.io/klog"
)

//--------------------------------------------------------------------------
//  ClusterInfoCollector
//--------------------------------------------------------------------------

// ClusterInfoCollector is a prometheus collector that generates ClusterInfoMetrics
type ClusterInfoCollector struct {
	Cloud         cloud.Provider
	KubeClientSet kubernetes.Interface
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (cic ClusterInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kubecost_cluster_info", "Kubecost Cluster Info", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (cic ClusterInfoCollector) Collect(ch chan<- prometheus.Metric) {
	clusterInfo := GetClusterInfo(cic.KubeClientSet, cic.Cloud)
	labels := prom.MapToLabels(clusterInfo)

	m := newClusterInfoMetric("kubecost_cluster_info", labels)
	ch <- m
}

//--------------------------------------------------------------------------
//  ClusterInfoMetric
//--------------------------------------------------------------------------

// ClusterInfoMetric is a prometheus.Metric used to encode the local cluster info
type ClusterInfoMetric struct {
	fqName string
	help   string
	labels map[string]string
}

// Creates a new ClusterInfoMetric, implementation of prometheus.Metric
func newClusterInfoMetric(fqName string, labels map[string]string) ClusterInfoMetric {
	return ClusterInfoMetric{
		fqName: fqName,
		labels: labels,
		help:   "kubecost_cluster_info ClusterInfo",
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (cim ClusterInfoMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{}
	return prometheus.NewDesc(cim.fqName, cim.help, prom.LabelNamesFrom(cim.labels), l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (cim ClusterInfoMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for k, v := range cim.labels {
		labels = append(labels, &dto.LabelPair{
			Name:  toStringPtr(k),
			Value: toStringPtr(v),
		})
	}
	m.Label = labels
	return nil
}

// returns a pointer to the string provided
func toStringPtr(s string) *string { return &s }

//--------------------------------------------------------------------------
//  Cost Model Metrics Initialization
//--------------------------------------------------------------------------

// Only allow the metrics to be instantiated and registered once
var metricsInit sync.Once

var (
	cpuGv                      *prometheus.GaugeVec
	ramGv                      *prometheus.GaugeVec
	gpuGv                      *prometheus.GaugeVec
	gpuCountGv                 *prometheus.GaugeVec
	pvGv                       *prometheus.GaugeVec
	spotGv                     *prometheus.GaugeVec
	totalGv                    *prometheus.GaugeVec
	ramAllocGv                 *prometheus.GaugeVec
	cpuAllocGv                 *prometheus.GaugeVec
	gpuAllocGv                 *prometheus.GaugeVec
	pvAllocGv                  *prometheus.GaugeVec
	networkZoneEgressCostG     prometheus.Gauge
	networkRegionEgressCostG   prometheus.Gauge
	networkInternetEgressCostG prometheus.Gauge
	clusterManagementCostGv    *prometheus.GaugeVec
	lbCostGv                   *prometheus.GaugeVec
)

// initCostModelMetrics uses a sync.Once to ensure that these metrics are only created once
func initCostModelMetrics(clusterCache clustercache.ClusterCache, provider cloud.Provider) {
	metricsInit.Do(func() {
		cpuGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_cpu_hourly_cost",
			Help: "node_cpu_hourly_cost hourly cost for each cpu on this node",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		ramGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_ram_hourly_cost",
			Help: "node_ram_hourly_cost hourly cost for each gb of ram on this node",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		gpuGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_gpu_hourly_cost",
			Help: "node_gpu_hourly_cost hourly cost for each gpu on this node",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		gpuCountGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_gpu_count",
			Help: "node_gpu_count count of gpu on this node",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		pvGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pv_hourly_cost",
			Help: "pv_hourly_cost Cost per GB per hour on a persistent disk",
		}, []string{"volumename", "persistentvolume", "provider_id"})

		spotGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kubecost_node_is_spot",
			Help: "kubecost_node_is_spot Cloud provider info about node preemptibility",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		totalGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_total_hourly_cost",
			Help: "node_total_hourly_cost Total node cost per hour",
		}, []string{"instance", "node", "instance_type", "region", "provider_id"})

		ramAllocGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "container_memory_allocation_bytes",
			Help: "container_memory_allocation_bytes Bytes of RAM used",
		}, []string{"namespace", "pod", "container", "instance", "node"})

		cpuAllocGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "container_cpu_allocation",
			Help: "container_cpu_allocation Percent of a single CPU used in a minute",
		}, []string{"namespace", "pod", "container", "instance", "node"})

		gpuAllocGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "container_gpu_allocation",
			Help: "container_gpu_allocation GPU used",
		}, []string{"namespace", "pod", "container", "instance", "node"})

		pvAllocGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pod_pvc_allocation",
			Help: "pod_pvc_allocation Bytes used by a PVC attached to a pod",
		}, []string{"namespace", "pod", "persistentvolumeclaim", "persistentvolume"})

		networkZoneEgressCostG = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubecost_network_zone_egress_cost",
			Help: "kubecost_network_zone_egress_cost Total cost per GB egress across zones",
		})

		networkRegionEgressCostG = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubecost_network_region_egress_cost",
			Help: "kubecost_network_region_egress_cost Total cost per GB egress across regions",
		})

		networkInternetEgressCostG = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubecost_network_internet_egress_cost",
			Help: "kubecost_network_internet_egress_cost Total cost per GB of internet egress.",
		})

		clusterManagementCostGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kubecost_cluster_management_cost",
			Help: "kubecost_cluster_management_cost Hourly cost paid as a cluster management fee.",
		}, []string{"provisioner_name"})

		lbCostGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // no differentiation between ELB and ALB right now
			Name: "kubecost_load_balancer_cost",
			Help: "kubecost_load_balancer_cost Hourly cost of load balancer",
		}, []string{"ingress_ip", "namespace", "service_name"}) // assumes one ingress IP per load balancer

		// Register cost-model metrics for emission
		prometheus.MustRegister(cpuGv, ramGv, gpuGv, gpuCountGv, totalGv, pvGv, spotGv)
		prometheus.MustRegister(ramAllocGv, cpuAllocGv, gpuAllocGv, pvAllocGv)
		prometheus.MustRegister(networkZoneEgressCostG, networkRegionEgressCostG, networkInternetEgressCostG)
		prometheus.MustRegister(clusterManagementCostGv, lbCostGv)

		// General Metric Collectors
		prometheus.MustRegister(ClusterInfoCollector{
			KubeClientSet: clusterCache.GetClient(),
			Cloud:         provider,
		})
	})
}

//--------------------------------------------------------------------------
//  CostModelMetricsEmitter
//--------------------------------------------------------------------------

// CostModelMetricsEmitter emits all cost-model specific metrics calculated by
// the CostModel.ComputeCostData() method.
type CostModelMetricsEmitter struct {
	PrometheusClient promclient.Client
	KubeClusterCache clustercache.ClusterCache
	CloudProvider    cloud.Provider
	Model            *CostModel

	// Metrics
	CPUPriceRecorder              *prometheus.GaugeVec
	RAMPriceRecorder              *prometheus.GaugeVec
	PersistentVolumePriceRecorder *prometheus.GaugeVec
	GPUPriceRecorder              *prometheus.GaugeVec
	GPUCountRecorder              *prometheus.GaugeVec
	PVAllocationRecorder          *prometheus.GaugeVec
	NodeSpotRecorder              *prometheus.GaugeVec
	NodeTotalPriceRecorder        *prometheus.GaugeVec
	RAMAllocationRecorder         *prometheus.GaugeVec
	CPUAllocationRecorder         *prometheus.GaugeVec
	GPUAllocationRecorder         *prometheus.GaugeVec
	ClusterManagementCostRecorder *prometheus.GaugeVec
	LBCostRecorder                *prometheus.GaugeVec
	NetworkZoneEgressRecorder     prometheus.Gauge
	NetworkRegionEgressRecorder   prometheus.Gauge
	NetworkInternetEgressRecorder prometheus.Gauge

	// Flow Control
	recordingLock     *sync.Mutex
	recordingStopping bool
	recordingStop     chan bool
}

// NewCostModelMetricsEmitter creates a new cost-model metrics emitter. Use Start() to begin metric emission.
func NewCostModelMetricsEmitter(promClient promclient.Client, clusterCache clustercache.ClusterCache, provider cloud.Provider, model *CostModel) *CostModelMetricsEmitter {
	// init will only actually execute once to register the custom gauges
	initCostModelMetrics(clusterCache, provider)

	metrics.InitKubeMetrics(clusterCache, &metrics.KubeMetricsOpts{
		EmitKubecostControllerMetrics: true,
		EmitNamespaceAnnotations:      env.IsEmitNamespaceAnnotationsMetric(),
		EmitPodAnnotations:            env.IsEmitPodAnnotationsMetric(),
		EmitKubeStateMetrics:          env.IsEmitKsmV1Metrics(),
		EmitNodeKubeStateMetrisOnly:   env.IsEmitNodeKubeStateMetrisOnly(),
	})

	return &CostModelMetricsEmitter{
		PrometheusClient:              promClient,
		KubeClusterCache:              clusterCache,
		CloudProvider:                 provider,
		Model:                         model,
		CPUPriceRecorder:              cpuGv,
		RAMPriceRecorder:              ramGv,
		GPUPriceRecorder:              gpuGv,
		GPUCountRecorder:              gpuCountGv,
		PersistentVolumePriceRecorder: pvGv,
		NodeSpotRecorder:              spotGv,
		NodeTotalPriceRecorder:        totalGv,
		RAMAllocationRecorder:         ramAllocGv,
		CPUAllocationRecorder:         cpuAllocGv,
		GPUAllocationRecorder:         gpuAllocGv,
		PVAllocationRecorder:          pvAllocGv,
		NetworkZoneEgressRecorder:     networkZoneEgressCostG,
		NetworkRegionEgressRecorder:   networkRegionEgressCostG,
		NetworkInternetEgressRecorder: networkInternetEgressCostG,
		ClusterManagementCostRecorder: clusterManagementCostGv,
		LBCostRecorder:                lbCostGv,
		recordingLock:                 new(sync.Mutex),
		recordingStopping:             false,
		recordingStop:                 nil,
	}
}

// Checks to see if there is a metric recording stop channel. If it exists, a new
// channel is not created and false is returned. If it doesn't exist, a new channel
// is created and true is returned.
func (cmme *CostModelMetricsEmitter) checkOrCreateRecordingChan() bool {
	cmme.recordingLock.Lock()
	defer cmme.recordingLock.Unlock()

	if cmme.recordingStop != nil {
		return false
	}

	cmme.recordingStop = make(chan bool, 1)
	return true
}

// IsRunning returns true if metric recording is running.
func (cmme *CostModelMetricsEmitter) IsRunning() bool {
	cmme.recordingLock.Lock()
	defer cmme.recordingLock.Unlock()

	return cmme.recordingStop != nil
}

// NodeCostAverages tracks a running average of a node's cost attributes.
// The averages are used to detect and discard spurrious outliers.
type NodeCostAverages struct {
	CpuCostAverage   float64
	RamCostAverage   float64
	NumCpuDataPoints float64
	NumRamDataPoints float64
}

// StartCostModelMetricRecording starts the go routine that emits metrics used to determine
// cluster costs.
func (cmme *CostModelMetricsEmitter) Start() bool {
	// Check to see if we're already recording
	// This function will create the stop recording channel and return true
	// if it doesn't exist.
	if !cmme.checkOrCreateRecordingChan() {
		log.Errorf("Attempted to start cost model metric recording when it's already running.")
		return false
	}

	go func() {
		defer errors.HandlePanic()

		containerSeen := make(map[string]bool)
		nodeSeen := make(map[string]bool)
		loadBalancerSeen := make(map[string]bool)
		pvSeen := make(map[string]bool)
		pvcSeen := make(map[string]bool)
		nodeCostAverages := make(map[string]NodeCostAverages)

		getKeyFromLabelStrings := func(labels ...string) string {
			return strings.Join(labels, ",")
		}
		getLabelStringsFromKey := func(key string) []string {
			return strings.Split(key, ",")
		}

		var defaultRegion string = ""
		nodeList := cmme.KubeClusterCache.GetAllNodes()
		if len(nodeList) > 0 {
			var ok bool
			defaultRegion, ok = util.GetRegion(nodeList[0].Labels)
			if !ok {
				log.DedupedWarningf(5, "Failed to locate default region")
			}
		}

		for {
			klog.V(4).Info("Recording prices...")
			podlist := cmme.KubeClusterCache.GetAllPods()
			podStatus := make(map[string]v1.PodPhase)
			for _, pod := range podlist {
				podStatus[pod.Name] = pod.Status.Phase
			}

			cfg, _ := cmme.CloudProvider.GetConfig()

			provisioner, clusterManagementCost, err := cmme.CloudProvider.ClusterManagementPricing()
			if err != nil {
				klog.V(1).Infof("Error getting cluster management cost %s", err.Error())
			}
			cmme.ClusterManagementCostRecorder.WithLabelValues(provisioner).Set(clusterManagementCost)

			// Record network pricing at global scope
			networkCosts, err := cmme.CloudProvider.NetworkPricing()
			if err != nil {
				klog.V(4).Infof("Failed to retrieve network costs: %s", err.Error())
			} else {
				cmme.NetworkZoneEgressRecorder.Set(networkCosts.ZoneNetworkEgressCost)
				cmme.NetworkRegionEgressRecorder.Set(networkCosts.RegionNetworkEgressCost)
				cmme.NetworkInternetEgressRecorder.Set(networkCosts.InternetNetworkEgressCost)
			}

			// TODO: Pass PrometheusClient and CloudProvider into CostModel on instantiation so this isn't so awkward
			data, err := cmme.Model.ComputeCostData(cmme.PrometheusClient, cmme.CloudProvider, "2m", "", "")
			if err != nil {
				// For an error collection, we'll just log the length of the errors (ComputeCostData already logs the
				// actual errors)
				if prom.IsErrorCollection(err) {
					if ec, ok := err.(prom.QueryErrorCollection); ok {
						log.Errorf("Error in price recording: %d errors occurred", len(ec.Errors()))
					}
				} else {
					log.Errorf("Error in price recording: " + err.Error())
				}

				// zero the for loop so the time.Sleep will still work
				data = map[string]*CostData{}
			}

			// TODO: Pass CloudProvider into CostModel on instantiation so this isn't so awkward
			nodes, err := cmme.Model.GetNodeCost(cmme.CloudProvider)
			if err != nil {
				log.Warningf("Metric emission: error getting Node cost: %s", err)
			}
			for nodeName, node := range nodes {
				// Emit costs, guarding against NaN inputs for custom pricing.
				cpuCost, _ := strconv.ParseFloat(node.VCPUCost, 64)
				if math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
					cpuCost, _ = strconv.ParseFloat(cfg.CPU, 64)
					if math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
						cpuCost = 0
					}
				}
				cpu, _ := strconv.ParseFloat(node.VCPU, 64)
				if math.IsNaN(cpu) || math.IsInf(cpu, 0) {
					cpu = 1 // Assume 1 CPU
				}
				ramCost, _ := strconv.ParseFloat(node.RAMCost, 64)
				if math.IsNaN(ramCost) || math.IsInf(ramCost, 0) {
					ramCost, _ = strconv.ParseFloat(cfg.RAM, 64)
					if math.IsNaN(ramCost) || math.IsInf(ramCost, 0) {
						ramCost = 0
					}
				}
				ram, _ := strconv.ParseFloat(node.RAMBytes, 64)
				if math.IsNaN(ram) || math.IsInf(ram, 0) {
					ram = 0
				}
				gpu, _ := strconv.ParseFloat(node.GPU, 64)
				if math.IsNaN(gpu) || math.IsInf(gpu, 0) {
					gpu = 0
				}
				gpuCost, _ := strconv.ParseFloat(node.GPUCost, 64)
				if math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
					gpuCost, _ = strconv.ParseFloat(cfg.GPU, 64)
					if math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
						gpuCost = 0
					}
				}
				nodeType := node.InstanceType
				nodeRegion := node.Region

				totalCost := cpu*cpuCost + ramCost*(ram/1024/1024/1024) + gpu*gpuCost

				labelKey := getKeyFromLabelStrings(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID)

				avgCosts, ok := nodeCostAverages[labelKey]

				// initialize average cost tracking for this node if there is none
				if !ok {
					avgCosts = NodeCostAverages{
						CpuCostAverage:   cpuCost,
						RamCostAverage:   ramCost,
						NumCpuDataPoints: 1,
						NumRamDataPoints: 1,
					}
					nodeCostAverages[labelKey] = avgCosts
				}

				cmme.GPUCountRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(gpu)

				const outlierFactor float64 = 30
				// don't record cpuCost, ramCost, or gpuCost in the case of wild outliers
				// k8s api sometimes causes cost spikes as described here:
				// https://github.com/kubecost/cost-model/issues/927
				if cpuCost < outlierFactor*avgCosts.CpuCostAverage {
					cmme.CPUPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(cpuCost)
					avgCosts.CpuCostAverage = (avgCosts.CpuCostAverage*avgCosts.NumCpuDataPoints + cpuCost) / (avgCosts.NumCpuDataPoints + 1)
					avgCosts.NumCpuDataPoints += 1
				} else {
					log.Warningf("CPU cost outlier detected; skipping data point.")
				}
				if ramCost < outlierFactor*avgCosts.RamCostAverage {
					cmme.RAMPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(ramCost)
					avgCosts.RamCostAverage = (avgCosts.RamCostAverage*avgCosts.NumRamDataPoints + ramCost) / (avgCosts.NumRamDataPoints + 1)
					avgCosts.NumRamDataPoints += 1
				} else {
					log.Warningf("RAM cost outlier detected; skipping data point.")
				}
				// skip redording totalCost if any constituent costs were outliers
				if cpuCost < outlierFactor*avgCosts.CpuCostAverage &&
					ramCost < outlierFactor*avgCosts.RamCostAverage {
					cmme.NodeTotalPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(totalCost)
				}

				nodeCostAverages[labelKey] = avgCosts

				if node.IsSpot() {
					cmme.NodeSpotRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(1.0)
				} else {
					cmme.NodeSpotRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(0.0)
				}
				nodeSeen[labelKey] = true
			}

			// TODO: Pass CloudProvider into CostModel on instantiation so this isn't so awkward
			loadBalancers, err := cmme.Model.GetLBCost(cmme.CloudProvider)
			if err != nil {
				log.Warningf("Metric emission: error getting LoadBalancer cost: %s", err)
			}
			for lbKey, lb := range loadBalancers {
				// TODO: parse (if necessary) and calculate cost associated with loadBalancer based on dynamic cloud prices fetched into each lb struct on GetLBCost() call
				keyParts := getLabelStringsFromKey(lbKey)
				namespace := keyParts[0]
				serviceName := keyParts[1]
				ingressIP := ""
				if len(lb.IngressIPAddresses) > 0 {
					ingressIP = lb.IngressIPAddresses[0] // assumes one ingress IP per load balancer
				}
				cmme.LBCostRecorder.WithLabelValues(ingressIP, namespace, serviceName).Set(lb.Cost)

				labelKey := getKeyFromLabelStrings(ingressIP, namespace, serviceName)
				loadBalancerSeen[labelKey] = true
			}

			for _, costs := range data {
				nodeName := costs.NodeName

				namespace := costs.Namespace
				podName := costs.PodName
				containerName := costs.Name

				if costs.PVCData != nil {
					for _, pvc := range costs.PVCData {
						if pvc.Volume != nil {
							timesClaimed := pvc.TimesClaimed
							if timesClaimed == 0 {
								timesClaimed = 1 // unallocated PVs are unclaimed but have a full allocation
							}
							cmme.PVAllocationRecorder.WithLabelValues(namespace, podName, pvc.Claim, pvc.VolumeName).Set(pvc.Values[0].Value / float64(timesClaimed))
							labelKey := getKeyFromLabelStrings(namespace, podName, pvc.Claim, pvc.VolumeName)
							pvcSeen[labelKey] = true
						}
					}
				}

				if len(costs.RAMAllocation) > 0 {
					cmme.RAMAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.RAMAllocation[0].Value)
				}
				if len(costs.CPUAllocation) > 0 {
					cmme.CPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.CPUAllocation[0].Value)
				}
				if len(costs.GPUReq) > 0 {
					// allocation here is set to the request because shared GPU usage not yet supported.
					cmme.GPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.GPUReq[0].Value)
				}
				labelKey := getKeyFromLabelStrings(namespace, podName, containerName, nodeName, nodeName)
				if podStatus[podName] == v1.PodRunning { // Only report data for current pods
					containerSeen[labelKey] = true
				} else {
					containerSeen[labelKey] = false
				}
			}

			storageClasses := cmme.KubeClusterCache.GetAllStorageClasses()
			storageClassMap := make(map[string]map[string]string)
			for _, storageClass := range storageClasses {
				params := storageClass.Parameters
				storageClassMap[storageClass.ObjectMeta.Name] = params
				if storageClass.GetAnnotations()["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.GetAnnotations()["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
					storageClassMap["default"] = params
					storageClassMap[""] = params
				}
			}

			pvs := cmme.KubeClusterCache.GetAllPersistentVolumes()
			for _, pv := range pvs {
				// Omit pv_hourly_cost if the volume status is failed
				if pv.Status.Phase == v1.VolumeFailed {
					continue
				}

				parameters, ok := storageClassMap[pv.Spec.StorageClassName]
				if !ok {
					klog.V(4).Infof("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
				}
				var region string
				if r, ok := util.GetRegion(pv.Labels); ok {
					region = r
				} else {
					region = defaultRegion
				}
				cacPv := &cloud.PV{
					Class:      pv.Spec.StorageClassName,
					Region:     region,
					Parameters: parameters,
				}

				// TODO: GetPVCost should be a method in CostModel?
				GetPVCost(cacPv, pv, cmme.CloudProvider, region)
				c, _ := strconv.ParseFloat(cacPv.Cost, 64)
				cmme.PersistentVolumePriceRecorder.WithLabelValues(pv.Name, pv.Name, cacPv.ProviderID).Set(c)
				labelKey := getKeyFromLabelStrings(pv.Name, pv.Name)
				pvSeen[labelKey] = true
			}

			for labelString, seen := range nodeSeen {
				if !seen {
					klog.V(4).Infof("Removing %s from nodes", labelString)
					labels := getLabelStringsFromKey(labelString)
					ok := cmme.NodeTotalPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from totalprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from totalprice", labelString)
					}
					ok = cmme.NodeSpotRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from spot records", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from spot records", labelString)
					}
					ok = cmme.CPUPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from cpuprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from cpuprice", labelString)
					}
					ok = cmme.GPUPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from gpuprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from gpuprice", labelString)
					}
					ok = cmme.GPUCountRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from gpucount", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from gpucount", labelString)
					}
					ok = cmme.RAMPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from ramprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from ramprice", labelString)
					}
					delete(nodeSeen, labelString)
					delete(nodeCostAverages, labelString)
				} else {
					nodeSeen[labelString] = false
				}
			}
			for labelString, seen := range loadBalancerSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					ok := cmme.LBCostRecorder.DeleteLabelValues(labels...)
					if !ok {
						log.Warningf("Metric emission: failed to delete LoadBalancer with labels: %v", labels)
					}
					delete(loadBalancerSeen, labelString)
				} else {
					loadBalancerSeen[labelString] = false
				}
			}
			for labelString, seen := range containerSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					cmme.RAMAllocationRecorder.DeleteLabelValues(labels...)
					cmme.CPUAllocationRecorder.DeleteLabelValues(labels...)
					cmme.GPUAllocationRecorder.DeleteLabelValues(labels...)
					delete(containerSeen, labelString)
				} else {
					containerSeen[labelString] = false
				}
			}
			for labelString, seen := range pvSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					cmme.PersistentVolumePriceRecorder.DeleteLabelValues(labels...)
					delete(pvSeen, labelString)
				} else {
					pvSeen[labelString] = false
				}
			}
			for labelString, seen := range pvcSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					cmme.PVAllocationRecorder.DeleteLabelValues(labels...)
					delete(pvcSeen, labelString)
				} else {
					pvcSeen[labelString] = false
				}
			}

			select {
			case <-time.After(time.Minute):
			case <-cmme.recordingStop:
				cmme.recordingLock.Lock()
				cmme.recordingStopping = false
				cmme.recordingStop = nil
				cmme.recordingLock.Unlock()
				return
			}
		}
	}()

	return true
}

// Stop halts the metrics emission loop after the current emission is completed
// or if the emission is paused.
func (cmme *CostModelMetricsEmitter) Stop() {
	cmme.recordingLock.Lock()
	defer cmme.recordingLock.Unlock()

	if !cmme.recordingStopping && cmme.recordingStop != nil {
		cmme.recordingStopping = true
		close(cmme.recordingStop)
	}
}
