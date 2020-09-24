package costmodel

import (
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"k8s.io/klog"
)

var (
	invalidLabelCharRE = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

//--------------------------------------------------------------------------
//  StatefulsetCollector
//--------------------------------------------------------------------------

// StatefulsetCollector is a prometheus collector that generates StatefulsetMetrics
type StatefulsetCollector struct {
	KubeClientSet kubernetes.Interface
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc StatefulsetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("statefulSet_match_labels", "statfulSet match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc StatefulsetCollector) Collect(ch chan<- prometheus.Metric) {
	ds, _ := sc.KubeClientSet.AppsV1().StatefulSets("").List(metav1.ListOptions{})
	for _, statefulset := range ds.Items {
		labels, values := kubeLabelsToPrometheusLabels(statefulset.Spec.Selector.MatchLabels)
		m := newStatefulsetMetric(statefulset.GetName(), statefulset.GetNamespace(), "statefulSet_match_labels", labels, values)
		ch <- m
	}
}

//--------------------------------------------------------------------------
//  StatefulsetMetric
//--------------------------------------------------------------------------

// StatefulsetMetric is a prometheus.Metric used to encode statefulset match labels
type StatefulsetMetric struct {
	fqName          string
	help            string
	labelNames      []string
	labelValues     []string
	statefulsetName string
	namespace       string
}

// Creates a new StatefulsetMetric, implementation of prometheus.Metric
func newStatefulsetMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) StatefulsetMetric {
	return StatefulsetMetric{
		fqName:          fqname,
		labelNames:      labelNames,
		labelValues:     labelvalues,
		help:            "statefulSet_match_labels StatefulSet Match Labels",
		statefulsetName: name,
		namespace:       namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s StatefulsetMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"statefulSet": s.statefulsetName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s StatefulsetMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "statefulSet"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.statefulsetName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  DeploymentCollector
//--------------------------------------------------------------------------

// DeploymentCollector is a prometheus collector that generates DeploymentMetrics
type DeploymentCollector struct {
	KubeClientSet kubernetes.Interface
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc DeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("deployment_match_labels", "deployment match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc DeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	ds, _ := sc.KubeClientSet.AppsV1().Deployments("").List(metav1.ListOptions{})
	for _, deployment := range ds.Items {
		labels, values := kubeLabelsToPrometheusLabels(deployment.Spec.Selector.MatchLabels)
		m := newDeploymentMetric(deployment.GetName(), deployment.GetNamespace(), "deployment_match_labels", labels, values)
		ch <- m
	}
}

//--------------------------------------------------------------------------
//  DeploymentMetric
//--------------------------------------------------------------------------

// DeploymentMetric is a prometheus.Metric used to encode deployment match labels
type DeploymentMetric struct {
	fqName         string
	help           string
	labelNames     []string
	labelValues    []string
	deploymentName string
	namespace      string
}

// Creates a new DeploymentMetric, implementation of prometheus.Metric
func newDeploymentMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) DeploymentMetric {
	return DeploymentMetric{
		fqName:         fqname,
		labelNames:     labelNames,
		labelValues:    labelvalues,
		help:           "deployment_match_labels Deployment Match Labels",
		deploymentName: name,
		namespace:      namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s DeploymentMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"deployment": s.deploymentName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s DeploymentMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "deployment"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.deploymentName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  ServiceCollector
//--------------------------------------------------------------------------

// ServiceCollector is a prometheus collector that generates ServiceMetrics
type ServiceCollector struct {
	KubeClientSet kubernetes.Interface
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc ServiceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("service_selector_labels", "service selector labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc ServiceCollector) Collect(ch chan<- prometheus.Metric) {
	svcs, _ := sc.KubeClientSet.CoreV1().Services("").List(metav1.ListOptions{})
	for _, svc := range svcs.Items {
		labels, values := kubeLabelsToPrometheusLabels(svc.Spec.Selector)
		m := newServiceMetric(svc.GetName(), svc.GetNamespace(), "service_selector_labels", labels, values)
		ch <- m
	}
}

//--------------------------------------------------------------------------
//  ServiceMetric
//--------------------------------------------------------------------------

// ServiceMetric is a prometheus.Metric used to encode service selector labels
type ServiceMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	serviceName string
	namespace   string
}

// Creates a new ServiceMetric, implementation of prometheus.Metric
func newServiceMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) ServiceMetric {
	return ServiceMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelvalues,
		help:        "service_selector_labels Service Selector Labels",
		serviceName: name,
		namespace:   namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s ServiceMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"service": s.serviceName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s ServiceMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "service"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.serviceName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  ClusterInfoCollector
//--------------------------------------------------------------------------

// ClusterInfoCollector is a prometheus collector that generates ClusterInfoMetrics
type ClusterInfoCollector struct {
	Cloud         costAnalyzerCloud.Provider
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

// toStringPtr is used to create a new string pointer from iteration vars
func toStringPtr(s string) *string {
	return &s
}

//--------------------------------------------------------------------------
//  Package Functions
//--------------------------------------------------------------------------

var (
	recordingLock     sync.Mutex
	recordingStopping bool
	recordingStop     chan bool
)

// Checks to see if there is a metric recording stop channel. If it exists, a new
// channel is not created and false is returned. If it doesn't exist, a new channel
// is created and true is returned.
func checkOrCreateRecordingChan() bool {
	recordingLock.Lock()
	defer recordingLock.Unlock()

	if recordingStop != nil {
		return false
	}

	recordingStop = make(chan bool, 1)
	return true
}

// IsCostModelMetricRecordingRunning returns true if metric recording is still running.
func IsCostModelMetricRecordingRunning() bool {
	recordingLock.Lock()
	defer recordingLock.Unlock()

	return recordingStop != nil
}

// StartCostModelMetricRecording starts the go routine that emits metrics used to determine
// cluster costs.
func StartCostModelMetricRecording(a *Accesses) bool {
	// Check to see if we're already recording
	// This function will create the stop recording channel and return true
	// if it doesn't exist.
	if !checkOrCreateRecordingChan() {
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

		getKeyFromLabelStrings := func(labels ...string) string {
			return strings.Join(labels, ",")
		}
		getLabelStringsFromKey := func(key string) []string {
			return strings.Split(key, ",")
		}

		var defaultRegion string = ""
		nodeList := a.Model.Cache.GetAllNodes()
		if len(nodeList) > 0 {
			defaultRegion = nodeList[0].Labels[v1.LabelZoneRegion]
		}

		for {
			klog.V(4).Info("Recording prices...")
			podlist := a.Model.Cache.GetAllPods()
			podStatus := make(map[string]v1.PodPhase)
			for _, pod := range podlist {
				podStatus[pod.Name] = pod.Status.Phase
			}

			cfg, _ := a.Cloud.GetConfig()

			provisioner, clusterManagementCost, err := a.Cloud.ClusterManagementPricing()
			if err != nil {
				klog.V(1).Infof("Error getting cluster management cost %s", err.Error())
			}
			a.ClusterManagementCostRecorder.WithLabelValues(provisioner).Set(clusterManagementCost)

			// Record network pricing at global scope
			networkCosts, err := a.Cloud.NetworkPricing()
			if err != nil {
				klog.V(4).Infof("Failed to retrieve network costs: %s", err.Error())
			} else {
				a.NetworkZoneEgressRecorder.Set(networkCosts.ZoneNetworkEgressCost)
				a.NetworkRegionEgressRecorder.Set(networkCosts.RegionNetworkEgressCost)
				a.NetworkInternetEgressRecorder.Set(networkCosts.InternetNetworkEgressCost)
			}

			data, err := a.Model.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, "2m", "", "")
			if err != nil {
				klog.V(1).Info("Error in price recording: " + err.Error())
				// zero the for loop so the time.Sleep will still work
				data = map[string]*CostData{}
			}

			nodes, err := a.Model.GetNodeCost(a.Cloud)
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

				a.CPUPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(cpuCost)
				a.RAMPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(ramCost)
				a.GPUPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(gpuCost)
				a.NodeTotalPriceRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(totalCost)
				if node.IsSpot() {
					a.NodeSpotRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(1.0)
				} else {
					a.NodeSpotRecorder.WithLabelValues(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID).Set(0.0)
				}
				labelKey := getKeyFromLabelStrings(nodeName, nodeName, nodeType, nodeRegion, node.ProviderID)
				nodeSeen[labelKey] = true
			}

			loadBalancers, err := a.Model.GetLBCost(a.Cloud)
			for lbKey, lb := range loadBalancers {
				// TODO: parse (if necessary) and calculate cost associated with loadBalancer based on dynamic cloud prices fetched into each lb struct on GetLBCost() call
				keyParts := getLabelStringsFromKey(lbKey)
				namespace := keyParts[0]
				serviceName := keyParts[1]
				ingressIP := lb.IngressIPAddresses[0] // assumes one ingress IP per load balancer
				a.LBCostRecorder.WithLabelValues(ingressIP, namespace, serviceName).Set(lb.Cost)

				labelKey := getKeyFromLabelStrings(namespace, serviceName)
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
							a.PVAllocationRecorder.WithLabelValues(namespace, podName, pvc.Claim, pvc.VolumeName).Set(pvc.Values[0].Value / float64(timesClaimed))
							labelKey := getKeyFromLabelStrings(namespace, podName, pvc.Claim, pvc.VolumeName)
							pvcSeen[labelKey] = true
						}
					}
				}

				if len(costs.RAMAllocation) > 0 {
					a.RAMAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.RAMAllocation[0].Value)
				}
				if len(costs.CPUAllocation) > 0 {
					a.CPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.CPUAllocation[0].Value)
				}
				if len(costs.GPUReq) > 0 {
					// allocation here is set to the request because shared GPU usage not yet supported.
					a.GPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.GPUReq[0].Value)
				}
				labelKey := getKeyFromLabelStrings(namespace, podName, containerName, nodeName, nodeName)
				if podStatus[podName] == v1.PodRunning { // Only report data for current pods
					containerSeen[labelKey] = true
				} else {
					containerSeen[labelKey] = false
				}

				storageClasses := a.Model.Cache.GetAllStorageClasses()
				storageClassMap := make(map[string]map[string]string)
				for _, storageClass := range storageClasses {
					params := storageClass.Parameters
					storageClassMap[storageClass.ObjectMeta.Name] = params
					if storageClass.GetAnnotations()["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.GetAnnotations()["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
						storageClassMap["default"] = params
						storageClassMap[""] = params
					}
				}

				pvs := a.Model.Cache.GetAllPersistentVolumes()
				for _, pv := range pvs {
					parameters, ok := storageClassMap[pv.Spec.StorageClassName]
					if !ok {
						klog.V(4).Infof("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
					}
					var region string
					if r, ok := pv.Labels[v1.LabelZoneRegion]; ok {
						region = r
					} else {
						region = defaultRegion
					}
					cacPv := &costAnalyzerCloud.PV{
						Class:      pv.Spec.StorageClassName,
						Region:     region,
						Parameters: parameters,
					}
					GetPVCost(cacPv, pv, a.Cloud, region)
					c, _ := strconv.ParseFloat(cacPv.Cost, 64)
					a.PersistentVolumePriceRecorder.WithLabelValues(pv.Name, pv.Name).Set(c)
					labelKey := getKeyFromLabelStrings(pv.Name, pv.Name)
					pvSeen[labelKey] = true
				}
			}
			for labelString, seen := range nodeSeen {
				if !seen {
					klog.V(4).Infof("Removing %s from nodes", labelString)
					labels := getLabelStringsFromKey(labelString)
					ok := a.NodeTotalPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from totalprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from totalprice", labelString)
					}
					ok = a.NodeSpotRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from spot records", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from spot records", labelString)
					}
					ok = a.CPUPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from cpuprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from cpuprice", labelString)
					}
					ok = a.GPUPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from gpuprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from gpuprice", labelString)
					}
					ok = a.RAMPriceRecorder.DeleteLabelValues(labels...)
					if ok {
						klog.V(4).Infof("removed %s from ramprice", labelString)
					} else {
						klog.Infof("FAILURE TO REMOVE %s from ramprice", labelString)
					}
					delete(nodeSeen, labelString)
				} else {
					nodeSeen[labelString] = false
				}
			}
			for labelString, seen := range loadBalancerSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.LBCostRecorder.DeleteLabelValues(labels...)
				} else {
					loadBalancerSeen[labelString] = false
				}
			}
			for labelString, seen := range containerSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.RAMAllocationRecorder.DeleteLabelValues(labels...)
					a.CPUAllocationRecorder.DeleteLabelValues(labels...)
					a.GPUAllocationRecorder.DeleteLabelValues(labels...)
					delete(containerSeen, labelString)
				} else {
					containerSeen[labelString] = false
				}
			}
			for labelString, seen := range pvSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.PersistentVolumePriceRecorder.DeleteLabelValues(labels...)
					delete(pvSeen, labelString)
				} else {
					pvSeen[labelString] = false
				}
			}
			for labelString, seen := range pvcSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.PVAllocationRecorder.DeleteLabelValues(labels...)
					delete(pvcSeen, labelString)
				} else {
					pvcSeen[labelString] = false
				}
			}

			select {
			case <-time.After(time.Minute):
			case <-recordingStop:
				recordingLock.Lock()
				recordingStopping = false
				recordingStop = nil
				recordingLock.Unlock()
				return
			}
		}
	}()

	return true
}

// StopCostModelMetricRecording halts the metrics emission loop after the current emission is completed
// or if the emission is paused.
func StopCostModelMetricRecording() {
	recordingLock.Lock()
	defer recordingLock.Unlock()

	if !recordingStopping && recordingStop != nil {
		recordingStopping = true
		close(recordingStop)
	}
}

// Converts kubernetes labels into prometheus labels.
func kubeLabelsToPrometheusLabels(labels map[string]string) ([]string, []string) {
	labelKeys := make([]string, 0, len(labels))
	for k := range labels {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	labelValues := make([]string, 0, len(labels))
	for i, k := range labelKeys {
		labelKeys[i] = "label_" + SanitizeLabelName(k)
		labelValues = append(labelValues, labels[k])
	}
	return labelKeys, labelValues
}

// Replaces all illegal prometheus label characters with _
func SanitizeLabelName(s string) string {
	return invalidLabelCharRE.ReplaceAllString(s, "_")
}
