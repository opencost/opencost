package costmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	prometheusClient "github.com/prometheus/client_golang/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

const (
	statusAPIError = 422

	apiPrefix         = "/api/v1"
	epAlertManagers   = apiPrefix + "/alertmanagers"
	epQuery           = apiPrefix + "/query"
	epQueryRange      = apiPrefix + "/query_range"
	epLabelValues     = apiPrefix + "/label/:name/values"
	epSeries          = apiPrefix + "/series"
	epTargets         = apiPrefix + "/targets"
	epSnapshot        = apiPrefix + "/admin/tsdb/snapshot"
	epDeleteSeries    = apiPrefix + "/admin/tsdb/delete_series"
	epCleanTombstones = apiPrefix + "/admin/tsdb/clean_tombstones"
	epConfig          = apiPrefix + "/status/config"
	epFlags           = apiPrefix + "/status/flags"

	clusterIDKey   = "CLUSTER_ID"
	remoteEnabled  = "REMOTE_WRITE_ENABLED"
	thanosEnabled  = "THANOS_ENABLED"
	thanosQueryUrl = "THANOS_QUERY_URL"
)

type CostModel struct {
	Cache ClusterCache

	stop chan struct{}
}

func NewCostModel(client kubernetes.Interface) *CostModel {
	stopCh := make(chan struct{})
	cache := NewKubernetesClusterCache(client)
	cache.Run(stopCh)

	return &CostModel{
		Cache: cache,
		stop:  stopCh,
	}
}

type CostData struct {
	Name            string                       `json:"name,omitempty"`
	PodName         string                       `json:"podName,omitempty"`
	NodeName        string                       `json:"nodeName,omitempty"`
	NodeData        *costAnalyzerCloud.Node      `json:"node,omitempty"`
	Namespace       string                       `json:"namespace,omitempty"`
	Deployments     []string                     `json:"deployments,omitempty"`
	Services        []string                     `json:"services,omitempty"`
	Daemonsets      []string                     `json:"daemonsets,omitempty"`
	Statefulsets    []string                     `json:"statefulsets,omitempty"`
	Jobs            []string                     `json:"jobs,omitempty"`
	RAMReq          []*Vector                    `json:"ramreq,omitempty"`
	RAMUsed         []*Vector                    `json:"ramused,omitempty"`
	RAMAllocation   []*Vector                    `json:"ramallocated,omitempty"`
	CPUReq          []*Vector                    `json:"cpureq,omitempty"`
	CPUUsed         []*Vector                    `json:"cpuused,omitempty"`
	CPUAllocation   []*Vector                    `json:"cpuallocated,omitempty"`
	GPUReq          []*Vector                    `json:"gpureq,omitempty"`
	PVCData         []*PersistentVolumeClaimData `json:"pvcData,omitempty"`
	NetworkData     []*Vector                    `json:"network,omitempty"`
	Labels          map[string]string            `json:"labels,omitempty"`
	NamespaceLabels map[string]string            `json:"namespaceLabels,omitempty"`
	ClusterID       string                       `json:"clusterId"`
}

func (cd *CostData) String() string {
	return fmt.Sprintf("\n\tName: %s; PodName: %s, NodeName: %s\n\tNamespace: %s\n\tDeployments: %s\n\tServices: %s\n\tCPU (req, used, alloc): %d, %d, %d\n\tRAM (req, used, alloc): %d, %d, %d",
		cd.Name, cd.PodName, cd.NodeName, cd.Namespace, strings.Join(cd.Deployments, ", "), strings.Join(cd.Services, ", "),
		len(cd.CPUReq), len(cd.CPUUsed), len(cd.CPUAllocation),
		len(cd.RAMReq), len(cd.RAMUsed), len(cd.RAMAllocation))
}

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

const (
	queryRAMRequestsStr = `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[%s] %s) 
					*  
					avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[%s] %s)
				) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		)
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryRAMUsageStr = `sort_desc(
		avg(
			label_replace(count_over_time(container_memory_working_set_bytes{container_name!="",container_name!="POD", instance!=""}[%s] %s), "node", "$1", "instance","(.+)") 
			* 
			label_replace(avg_over_time(container_memory_working_set_bytes{container_name!="",container_name!="POD", instance!=""}[%s] %s), "node", "$1", "instance","(.+)") 
		) by (namespace,container_name,pod_name,node,cluster_id)
	)`
	queryCPURequestsStr = `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[%s] %s) 
					*  
					avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[%s] %s)
				) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryCPUUsageStr = `avg(
		label_replace(
		rate( 
			container_cpu_usage_seconds_total{container_name!="",container_name!="POD",instance!=""}[%s] %s
		) , "node", "$1", "instance", "(.+)"
		)
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryGPURequestsStr = `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[%s] %s) 
					*  
					avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[%s] %s)
				) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryPVRequestsStr = `avg(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass, namespace, volumename, cluster_id) 
						* 
						on (persistentvolumeclaim, namespace, cluster_id) group_right(storageclass, volumename) 
				sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace, cluster_id)`
	queryZoneNetworkUsage     = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="true"}[%s] %s)) by (namespace,pod_name,cluster_id) / 1024 / 1024 / 1024`
	queryRegionNetworkUsage   = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="false"}[%s] %s)) by (namespace,pod_name,cluster_id) / 1024 / 1024 / 1024`
	queryInternetNetworkUsage = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true"}[%s] %s)) by (namespace,pod_name,cluster_id) / 1024 / 1024 / 1024`
	normalizationStr          = `max(count_over_time(kube_pod_container_resource_requests_memory_bytes{}[%s] %s))`
)

type PrometheusMetadata struct {
	Running            bool `json:"running"`
	KubecostDataExists bool `json:"kubecostDataExists"`
}

// ValidatePrometheus tells the model what data prometheus has on it.
func ValidatePrometheus(cli prometheusClient.Client) (*PrometheusMetadata, error) {
	data, err := Query(cli, "up")
	if err != nil {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, err
	}
	v, kcmetrics, err := getUptimeData(data)
	if err != nil {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, err
	}
	if len(v) > 0 {
		return &PrometheusMetadata{
			Running:            true,
			KubecostDataExists: kcmetrics,
		}, nil
	} else {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, fmt.Errorf("No running jobs found on Prometheus at %s", cli.URL(epQuery, nil).Path)
	}
}

func getUptimeData(qr interface{}) ([]*Vector, bool, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, false, err
		}
		return nil, false, fmt.Errorf(e)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return nil, false, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	jobData := []*Vector{}
	kubecostMetrics := false
	for _, val := range results {
		// For now, just do this for validation. TODO: This can be parsed to figure out the exact running jobs.
		metrics, ok := val.(map[string]interface{})["metric"].(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("Prometheus vector does not have metric labels")
		}
		jobname, ok := metrics["job"]
		if !ok {
			return nil, false, fmt.Errorf("up query does not have job names")
		}
		if jobname == "kubecost" {
			kubecostMetrics = true
		}
		value, ok := val.(map[string]interface{})["value"]
		if !ok {
			return nil, false, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
		}
		dataPoint, ok := value.([]interface{})
		if !ok || len(dataPoint) != 2 {
			return nil, false, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		strVal := dataPoint[1].(string)
		v, _ := strconv.ParseFloat(strVal, 64)
		toReturn := &Vector{
			Timestamp: dataPoint[0].(float64),
			Value:     v,
		}
		jobData = append(jobData, toReturn)
	}
	return jobData, kubecostMetrics, nil
}

func ComputeUptimes(cli prometheusClient.Client) (map[string]float64, error) {
	res, err := Query(cli, `container_start_time_seconds{container_name != "POD",container_name != ""}`)
	if err != nil {
		return nil, err
	}
	vectors, err := GetContainerMetricVector(res, false, 0, os.Getenv(clusterIDKey))
	if err != nil {
		return nil, err
	}
	results := make(map[string]float64)
	for key, vector := range vectors {
		if err != nil {
			return nil, err
		}
		val := vector[0].Value
		uptime := time.Now().Sub(time.Unix(int64(val), 0)).Seconds()
		results[key] = uptime
	}
	return results, nil
}

func (cm *CostModel) ComputeCostData(cli prometheusClient.Client, clientset kubernetes.Interface, cp costAnalyzerCloud.Provider, window string, offset string, filterNamespace string) (map[string]*CostData, error) {
	queryRAMRequests := fmt.Sprintf(queryRAMRequestsStr, window, offset, window, offset)
	queryRAMUsage := fmt.Sprintf(queryRAMUsageStr, window, offset, window, offset)
	queryCPURequests := fmt.Sprintf(queryCPURequestsStr, window, offset, window, offset)
	queryCPUUsage := fmt.Sprintf(queryCPUUsageStr, window, offset)
	queryGPURequests := fmt.Sprintf(queryGPURequestsStr, window, offset, window, offset)
	queryPVRequests := fmt.Sprintf(queryPVRequestsStr)
	queryNetZoneRequests := fmt.Sprintf(queryZoneNetworkUsage, window, "")
	queryNetRegionRequests := fmt.Sprintf(queryRegionNetworkUsage, window, "")
	queryNetInternetRequests := fmt.Sprintf(queryInternetNetworkUsage, window, "")
	normalization := fmt.Sprintf(normalizationStr, window, offset)

	// Cluster ID is specific to the source cluster
	clusterID := os.Getenv(clusterIDKey)

	var wg sync.WaitGroup
	wg.Add(11)

	var promErr error
	var resultRAMRequests interface{}
	go func() {
		resultRAMRequests, promErr = Query(cli, queryRAMRequests)
		defer wg.Done()
	}()
	var resultRAMUsage interface{}
	go func() {
		resultRAMUsage, promErr = Query(cli, queryRAMUsage)
		defer wg.Done()
	}()
	var resultCPURequests interface{}
	go func() {
		resultCPURequests, promErr = Query(cli, queryCPURequests)
		defer wg.Done()
	}()
	var resultCPUUsage interface{}
	go func() {
		resultCPUUsage, promErr = Query(cli, queryCPUUsage)
		defer wg.Done()
	}()
	var resultGPURequests interface{}
	go func() {
		resultGPURequests, promErr = Query(cli, queryGPURequests)
		defer wg.Done()
	}()
	var resultPVRequests interface{}
	go func() {
		resultPVRequests, promErr = Query(cli, queryPVRequests)
		defer wg.Done()
	}()
	var resultNetZoneRequests interface{}
	go func() {
		resultNetZoneRequests, promErr = Query(cli, queryNetZoneRequests)
		defer wg.Done()
	}()
	var resultNetRegionRequests interface{}
	go func() {
		resultNetRegionRequests, promErr = Query(cli, queryNetRegionRequests)
		defer wg.Done()
	}()
	var resultNetInternetRequests interface{}
	go func() {
		resultNetInternetRequests, promErr = Query(cli, queryNetInternetRequests)
		defer wg.Done()
	}()
	var normalizationResult interface{}
	go func() {
		normalizationResult, promErr = Query(cli, normalization)
		defer wg.Done()
	}()

	podDeploymentsMapping := make(map[string]map[string][]string)
	podServicesMapping := make(map[string]map[string][]string)
	namespaceLabelsMapping := make(map[string]map[string]string)
	podlist := cm.Cache.GetAllPods()
	var k8sErr error
	go func() {
		defer wg.Done()

		podDeploymentsMapping, k8sErr = getPodDeployments(cm.Cache, podlist)
		if k8sErr != nil {
			return
		}

		podServicesMapping, k8sErr = getPodServices(cm.Cache, podlist)
		if k8sErr != nil {
			return
		}
		namespaceLabelsMapping, k8sErr = getNamespaceLabels(cm.Cache)
		if k8sErr != nil {
			return
		}

	}()

	wg.Wait()

	if promErr != nil {
		return nil, fmt.Errorf("Error querying prometheus: %s", promErr.Error())
	}
	if k8sErr != nil {
		return nil, fmt.Errorf("Error querying the kubernetes api: %s", k8sErr.Error())
	}

	normalizationValue, err := getNormalization(normalizationResult)
	if err != nil {
		return nil, fmt.Errorf("Error parsing normalization values: " + err.Error())
	}

	nodes, err := getNodeCost(cm.Cache, cp)
	if err != nil {
		klog.V(1).Infof("Warning, no Node cost model available: " + err.Error())
		return nil, err
	}

	pvClaimMapping, err := getPVInfoVector(resultPVRequests, clusterID)
	if err != nil {
		klog.Infof("Unable to get PV Data: %s", err.Error())
	}
	if pvClaimMapping != nil {
		err = addPVData(cm.Cache, pvClaimMapping, cp)
		if err != nil {
			return nil, err
		}
	}

	networkUsageMap, err := GetNetworkUsageData(resultNetZoneRequests, resultNetRegionRequests, resultNetInternetRequests, clusterID, false)
	if err != nil {
		klog.V(1).Infof("Unable to get Network Cost Data: %s", err.Error())
		networkUsageMap = make(map[string]*NetworkUsageData)
	}

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)

	RAMReqMap, err := GetContainerMetricVector(resultRAMRequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMReqMap {
		containers[key] = true
	}

	RAMUsedMap, err := GetContainerMetricVector(resultRAMUsage, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMUsedMap {
		containers[key] = true
	}
	CPUReqMap, err := GetContainerMetricVector(resultCPURequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range CPUReqMap {
		containers[key] = true
	}
	GPUReqMap, err := GetContainerMetricVector(resultGPURequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range GPUReqMap {
		containers[key] = true
	}
	CPUUsedMap, err := GetContainerMetricVector(resultCPUUsage, false, 0, clusterID) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}
	currentContainers := make(map[string]v1.Pod)
	for _, pod := range podlist {
		if pod.Status.Phase != v1.PodRunning {
			continue
		}
		cs, err := newContainerMetricsFromPod(*pod, clusterID)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			containers[c.Key()] = true // captures any containers that existed for a time < a prometheus scrape interval. We currently charge 0 for this but should charge something.
			currentContainers[c.Key()] = *pod
		}
	}
	missingNodes := make(map[string]*costAnalyzerCloud.Node)
	missingContainers := make(map[string]*CostData)
	for key := range containers {
		if _, ok := containerNameCost[key]; ok {
			continue // because ordering is important for the allocation model (all PV's applied to the first), just dedupe if it's already been added.
		}
		if pod, ok := currentContainers[key]; ok {
			podName := pod.GetObjectMeta().GetName()
			ns := pod.GetObjectMeta().GetNamespace()

			nsLabels := namespaceLabelsMapping[ns]
			podLabels := pod.GetObjectMeta().GetLabels()
			if podLabels == nil {
				podLabels = make(map[string]string)
			}

			for k, v := range nsLabels {
				podLabels[k] = v
			}

			nodeName := pod.Spec.NodeName
			var nodeData *costAnalyzerCloud.Node
			if _, ok := nodes[nodeName]; ok {
				nodeData = nodes[nodeName]
			}
			var podDeployments []string
			if _, ok := podDeploymentsMapping[ns]; ok {
				if ds, ok := podDeploymentsMapping[ns][pod.GetObjectMeta().GetName()]; ok {
					podDeployments = ds
				} else {
					podDeployments = []string{}
				}
			}

			var podPVs []*PersistentVolumeClaimData
			podClaims := pod.Spec.Volumes
			for _, vol := range podClaims {
				if vol.PersistentVolumeClaim != nil {
					name := vol.PersistentVolumeClaim.ClaimName
					if pvClaim, ok := pvClaimMapping[ns+","+name+","+clusterID]; ok {
						podPVs = append(podPVs, pvClaim)
					}
				}
			}

			var podNetCosts []*Vector
			if usage, ok := networkUsageMap[ns+","+podName+","+clusterID]; ok {
				netCosts, err := GetNetworkCost(usage, cp)
				if err != nil {
					klog.V(3).Infof("Error pulling network costs: %s", err.Error())
				} else {
					podNetCosts = netCosts
				}
			}

			var podServices []string
			if _, ok := podServicesMapping[ns]; ok {
				if svcs, ok := podServicesMapping[ns][pod.GetObjectMeta().GetName()]; ok {
					podServices = svcs
				} else {
					podServices = []string{}
				}
			}

			for i, container := range pod.Spec.Containers {
				containerName := container.Name

				// recreate the key and look up data for this container
				newKey := newContainerMetricFromValues(ns, podName, containerName, pod.Spec.NodeName, clusterID).Key()

				RAMReqV, ok := RAMReqMap[newKey]
				if !ok {
					klog.V(4).Info("no RAM requests for " + newKey)
					RAMReqV = []*Vector{&Vector{}}
				}
				RAMUsedV, ok := RAMUsedMap[newKey]
				if !ok {
					klog.V(4).Info("no RAM usage for " + newKey)
					RAMUsedV = []*Vector{&Vector{}}
				}
				CPUReqV, ok := CPUReqMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU requests for " + newKey)
					CPUReqV = []*Vector{&Vector{}}
				}
				GPUReqV, ok := GPUReqMap[newKey]
				if !ok {
					klog.V(4).Info("no GPU requests for " + newKey)
					GPUReqV = []*Vector{&Vector{}}
				}
				CPUUsedV, ok := CPUUsedMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU usage for " + newKey)
					CPUUsedV = []*Vector{&Vector{}}
				}

				var pvReq []*PersistentVolumeClaimData
				var netReq []*Vector
				if i == 0 { // avoid duplicating by just assigning all claims to the first container.
					pvReq = podPVs
					netReq = podNetCosts
				}

				costs := &CostData{
					Name:            containerName,
					PodName:         podName,
					NodeName:        nodeName,
					Namespace:       ns,
					Deployments:     podDeployments,
					Services:        podServices,
					Daemonsets:      getDaemonsetsOfPod(pod),
					Jobs:            getJobsOfPod(pod),
					Statefulsets:    getStatefulSetsOfPod(pod),
					NodeData:        nodeData,
					RAMReq:          RAMReqV,
					RAMUsed:         RAMUsedV,
					CPUReq:          CPUReqV,
					CPUUsed:         CPUUsedV,
					GPUReq:          GPUReqV,
					PVCData:         pvReq,
					NetworkData:     netReq,
					Labels:          podLabels,
					NamespaceLabels: nsLabels,
					ClusterID:       clusterID,
				}
				costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
				costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
				if filterNamespace == "" {
					containerNameCost[newKey] = costs
				} else if costs.Namespace == filterNamespace {
					containerNameCost[newKey] = costs
				}
			}
		} else {
			// The container has been deleted. Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
			klog.V(4).Info("The container " + key + " has been deleted. Calculating allocation but resulting object will be missing data.")
			c, err := NewContainerMetricFromKey(key)
			if err != nil {
				return nil, err
			}
			RAMReqV, ok := RAMReqMap[key]
			if !ok {
				klog.V(4).Info("no RAM requests for " + key)
				RAMReqV = []*Vector{&Vector{}}
			}
			RAMUsedV, ok := RAMUsedMap[key]
			if !ok {
				klog.V(4).Info("no RAM usage for " + key)
				RAMUsedV = []*Vector{&Vector{}}
			}
			CPUReqV, ok := CPUReqMap[key]
			if !ok {
				klog.V(4).Info("no CPU requests for " + key)
				CPUReqV = []*Vector{&Vector{}}
			}
			GPUReqV, ok := GPUReqMap[key]
			if !ok {
				klog.V(4).Info("no GPU requests for " + key)
				GPUReqV = []*Vector{&Vector{}}
			}
			CPUUsedV, ok := CPUUsedMap[key]
			if !ok {
				klog.V(4).Info("no CPU usage for " + key)
				CPUUsedV = []*Vector{&Vector{}}
			}

			node, ok := nodes[c.NodeName]
			if !ok {
				klog.V(2).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
				if n, ok := missingNodes[c.NodeName]; ok {
					node = n
				} else {
					node = &costAnalyzerCloud.Node{}
					missingNodes[c.NodeName] = node
				}
			}
			namespacelabels, ok := namespaceLabelsMapping[c.Namespace]
			if !ok {
				klog.V(3).Infof("Missing data for namespace %s", c.Namespace)
			}
			costs := &CostData{
				Name:            c.ContainerName,
				PodName:         c.PodName,
				NodeName:        c.NodeName,
				NodeData:        node,
				Namespace:       c.Namespace,
				RAMReq:          RAMReqV,
				RAMUsed:         RAMUsedV,
				CPUReq:          CPUReqV,
				CPUUsed:         CPUUsedV,
				GPUReq:          GPUReqV,
				NamespaceLabels: namespacelabels,
				ClusterID:       c.ClusterID,
			}
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
			if filterNamespace == "" {
				containerNameCost[key] = costs
				missingContainers[key] = costs
			} else if costs.Namespace == filterNamespace {
				containerNameCost[key] = costs
				missingContainers[key] = costs
			}
		}
	}
	err = findDeletedNodeInfo(cli, missingNodes, window)

	if err != nil {
		klog.V(1).Infof("Error fetching historical node data: %s", err.Error())
	}
	err = findDeletedPodInfo(cli, missingContainers, window)
	if err != nil {
		klog.V(1).Infof("Error fetching historical pod data: %s", err.Error())
	}
	return containerNameCost, err
}

func findDeletedPodInfo(cli prometheusClient.Client, missingContainers map[string]*CostData, window string) error {
	if len(missingContainers) > 0 {
		queryHistoricalPodLabels := fmt.Sprintf(`kube_pod_labels{}[%s]`, window)

		podLabelsResult, err := Query(cli, queryHistoricalPodLabels)
		if err != nil {
			klog.V(1).Infof("Error parsing historical labels: %s", err.Error())
		}
		podLabels := make(map[string]map[string]string)
		if podLabelsResult != nil {
			podLabels, err = labelsFromPrometheusQuery(podLabelsResult)
			if err != nil {
				klog.V(1).Infof("Error parsing historical labels: %s", err.Error())
			}
		}
		for key, costData := range missingContainers {
			cm, _ := NewContainerMetricFromKey(key)
			labels, ok := podLabels[cm.PodName]
			if !ok {
				klog.V(1).Infof("Unable to find historical data for pod '%s'", cm.PodName)
				labels = make(map[string]string)
			}
			for k, v := range costData.NamespaceLabels {
				labels[k] = v
			}
			costData.Labels = labels
		}
	}

	return nil
}

func labelsFromPrometheusQuery(qr interface{}) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return toReturn, err
		}
		return toReturn, fmt.Errorf(e)
	}
	for _, val := range data.(map[string]interface{})["result"].([]interface{}) {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return toReturn, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return toReturn, fmt.Errorf("Metric field is improperly formatted")
		}
		pod, ok := metricMap["pod"]
		if !ok {
			return toReturn, fmt.Errorf("pod field does not exist in data result vector")
		}
		podName, ok := pod.(string)
		if !ok {
			return toReturn, fmt.Errorf("pod field is improperly formatted")
		}

		for labelName, labelValue := range metricMap {
			parsedLabelName := labelName
			parsedLv, ok := labelValue.(string)
			if !ok {
				return toReturn, fmt.Errorf("label value is improperly formatted")
			}
			if strings.HasPrefix(parsedLabelName, "label_") {
				l := strings.Replace(parsedLabelName, "label_", "", 1)
				if podLabels, ok := toReturn[podName]; ok {
					podLabels[l] = parsedLv
				} else {
					toReturn[podName] = make(map[string]string)
					toReturn[podName][l] = parsedLv
				}
			}
		}
	}
	return toReturn, nil
}

func findDeletedNodeInfo(cli prometheusClient.Client, missingNodes map[string]*costAnalyzerCloud.Node, window string) error {
	if len(missingNodes) > 0 {
		q := make([]string, 0, len(missingNodes))
		for nodename := range missingNodes {
			klog.V(3).Infof("Finding data for deleted node %v", nodename)
			q = append(q, nodename)
		}
		l := strings.Join(q, "|")

		queryHistoricalCPUCost := fmt.Sprintf(`avg_over_time(node_cpu_hourly_cost{instance=~"%s"}[%s])`, l, window)
		queryHistoricalRAMCost := fmt.Sprintf(`avg_over_time(node_ram_hourly_cost{instance=~"%s"}[%s])`, l, window)
		queryHistoricalGPUCost := fmt.Sprintf(`avg_over_time(node_gpu_hourly_cost{instance=~"%s"}[%s])`, l, window)

		cpuCostResult, err := Query(cli, queryHistoricalCPUCost)
		if err != nil {
			return fmt.Errorf("Error fetching cpu cost data: " + err.Error())
		}
		ramCostResult, err := Query(cli, queryHistoricalRAMCost)
		if err != nil {
			return fmt.Errorf("Error fetching ram cost data: " + err.Error())
		}
		gpuCostResult, err := Query(cli, queryHistoricalGPUCost)
		if err != nil {
			return fmt.Errorf("Error fetching gpu cost data: " + err.Error())
		}

		cpuCosts, err := getCost(cpuCostResult)
		if err != nil {
			return err
		}
		ramCosts, err := getCost(ramCostResult)
		if err != nil {
			return err
		}
		gpuCosts, err := getCost(gpuCostResult)
		if err != nil {
			return err
		}

		if len(cpuCosts) == 0 {
			klog.V(1).Infof("Historical data for node prices not available. Ingest this server's /metrics endpoint to get that data.")
		}

		for node, costv := range cpuCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].VCPUCost = fmt.Sprintf("%f", costv[0].Value)
			}
		}
		for node, costv := range ramCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].RAMCost = fmt.Sprintf("%f", costv[0].Value)
			}
		}
		for node, costv := range gpuCosts {
			if _, ok := missingNodes[node]; ok {
				missingNodes[node].GPUCost = fmt.Sprintf("%f", costv[0].Value)
			}
		}

	}
	return nil
}

func getContainerAllocation(req []*Vector, used []*Vector) []*Vector {
	if req == nil || len(req) == 0 {
		for _, usedV := range used {
			if usedV.Timestamp == 0 {
				continue
			}
			usedV.Timestamp = math.Round(usedV.Timestamp/10) * 10
		}
		return used
	}
	if used == nil || len(used) == 0 {
		for _, reqV := range req {
			if reqV.Timestamp == 0 {
				continue
			}
			reqV.Timestamp = math.Round(reqV.Timestamp/10) * 10
		}
		return req
	}
	var allocation []*Vector

	var timestamps []float64
	reqMap := make(map[float64]float64)
	for _, reqV := range req {
		if reqV.Timestamp == 0 {
			continue
		}
		reqV.Timestamp = math.Round(reqV.Timestamp/10) * 10
		reqMap[reqV.Timestamp] = reqV.Value
		timestamps = append(timestamps, reqV.Timestamp)
	}
	usedMap := make(map[float64]float64)
	for _, usedV := range used {
		if usedV.Timestamp == 0 {
			continue
		}
		usedV.Timestamp = math.Round(usedV.Timestamp/10) * 10
		usedMap[usedV.Timestamp] = usedV.Value
		if _, ok := reqMap[usedV.Timestamp]; !ok { // no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, usedV.Timestamp)
		}
	}

	sort.Float64s(timestamps)
	for _, t := range timestamps {
		rv, okR := reqMap[t]
		uv, okU := usedMap[t]
		allocationVector := &Vector{
			Timestamp: t,
		}
		if okR && okU {
			allocationVector.Value = math.Max(rv, uv)
		} else if okR {
			allocationVector.Value = rv
		} else if okU {
			allocationVector.Value = uv
		}
		allocation = append(allocation, allocationVector)
	}

	return allocation
}

func addPVData(cache ClusterCache, pvClaimMapping map[string]*PersistentVolumeClaimData, cloud costAnalyzerCloud.Provider) error {
	cfg, err := cloud.GetConfig()
	if err != nil {
		return err
	}
	storageClasses := cache.GetAllStorageClasses()
	storageClassMap := make(map[string]map[string]string)
	for _, storageClass := range storageClasses {
		params := storageClass.Parameters
		storageClassMap[storageClass.ObjectMeta.Name] = params
		if storageClass.GetAnnotations()["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.GetAnnotations()["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			storageClassMap["default"] = params
			storageClassMap[""] = params
		}
	}

	pvs := cache.GetAllPersistentVolumes()
	pvMap := make(map[string]*costAnalyzerCloud.PV)
	for _, pv := range pvs {
		parameters, ok := storageClassMap[pv.Spec.StorageClassName]
		if !ok {
			klog.V(4).Infof("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
		}
		cacPv := &costAnalyzerCloud.PV{
			Class:      pv.Spec.StorageClassName,
			Region:     pv.Labels[v1.LabelZoneRegion],
			Parameters: parameters,
		}
		err := GetPVCost(cacPv, pv, cloud)
		if err != nil {
			return err
		}
		pvMap[pv.Name] = cacPv
	}

	for _, pvc := range pvClaimMapping {
		if vol, ok := pvMap[pvc.VolumeName]; ok {
			pvc.Volume = vol
		} else {
			klog.V(1).Infof("PV not found, using default")
			pvc.Volume = &costAnalyzerCloud.PV{
				Cost: cfg.Storage,
			}
		}
	}
	return nil
}

func GetPVCost(pv *costAnalyzerCloud.PV, kpv *v1.PersistentVolume, cp costAnalyzerCloud.Provider) error {
	cfg, err := cp.GetConfig()
	if err != nil {
		return err
	}
	key := cp.GetPVKey(kpv, pv.Parameters)
	pvWithCost, err := cp.PVPricing(key)
	if err != nil {
		pv.Cost = cfg.Storage
		return err
	}
	if pvWithCost == nil || pvWithCost.Cost == "" {
		pv.Cost = cfg.Storage
		return nil // set default cost
	}
	pv.Cost = pvWithCost.Cost
	return nil
}

func getNodeCost(cache ClusterCache, cp costAnalyzerCloud.Provider) (map[string]*costAnalyzerCloud.Node, error) {
	cfg, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}

	nodeList := cache.GetAllNodes()
	nodes := make(map[string]*costAnalyzerCloud.Node)

	for _, n := range nodeList {
		name := n.GetObjectMeta().GetName()
		nodeLabels := n.GetObjectMeta().GetLabels()
		nodeLabels["providerID"] = n.Spec.ProviderID

		cnode, err := cp.NodePricing(cp.GetKey(nodeLabels))
		if err != nil {
			klog.V(1).Infof("Error getting node. Error: " + err.Error())
			nodes[name] = cnode
			continue
		}
		newCnode := *cnode

		var cpu float64
		if newCnode.VCPU == "" {
			cpu = float64(n.Status.Capacity.Cpu().Value())
			newCnode.VCPU = n.Status.Capacity.Cpu().String()
		} else {
			cpu, _ = strconv.ParseFloat(newCnode.VCPU, 64)
		}

		var ram float64
		if newCnode.RAM == "" {
			newCnode.RAM = n.Status.Capacity.Memory().String()
		}
		ram = float64(n.Status.Capacity.Memory().Value())
		newCnode.RAMBytes = fmt.Sprintf("%f", ram)

		if newCnode.GPU != "" && newCnode.GPUCost == "" {
			// We couldn't find a gpu cost, so fix cpu and ram, then accordingly
			klog.V(4).Infof("GPU without cost found for %s, calculating...", cp.GetKey(nodeLabels).Features())

			defaultCPU, err := strconv.ParseFloat(cfg.CPU, 64)
			if err != nil {
				klog.V(3).Infof("Could not parse default cpu price")
				return nil, err
			}

			defaultRAM, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				klog.V(3).Infof("Could not parse default ram price")
				return nil, err
			}

			defaultGPU, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				klog.V(3).Infof("Could not parse default gpu price")
				return nil, err
			}

			cpuToRAMRatio := defaultCPU / defaultRAM
			gpuToRAMRatio := defaultGPU / defaultRAM

			ramGB := ram / 1024 / 1024 / 1024
			ramMultiple := gpuToRAMRatio + cpu*cpuToRAMRatio + ramGB

			var nodePrice float64
			if newCnode.Cost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.Cost, 64)
				if err != nil {
					klog.V(3).Infof("Could not parse total node price")
					return nil, err
				}
			} else {
				nodePrice, err = strconv.ParseFloat(newCnode.VCPUCost, 64) // all the price was allocated the the CPU
				if err != nil {
					klog.V(3).Infof("Could not parse node vcpu price")
					return nil, err
				}
			}

			ramPrice := (nodePrice / ramMultiple)
			cpuPrice := ramPrice * cpuToRAMRatio
			gpuPrice := ramPrice * gpuToRAMRatio

			newCnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
			newCnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			newCnode.RAMBytes = fmt.Sprintf("%f", ram)
			newCnode.GPUCost = fmt.Sprintf("%f", gpuPrice)
		} else if newCnode.RAMCost == "" {
			// We couldn't find a ramcost, so fix cpu and allocate ram accordingly
			klog.V(4).Infof("No RAM cost found for %s, calculating...", cp.GetKey(nodeLabels).Features())

			defaultCPU, err := strconv.ParseFloat(cfg.CPU, 64)
			if err != nil {
				klog.V(3).Infof("Could not parse default cpu price")
				return nil, err
			}

			defaultRAM, err := strconv.ParseFloat(cfg.RAM, 64)
			if err != nil {
				klog.V(3).Infof("Could not parse default ram price")
				return nil, err
			}

			cpuToRAMRatio := defaultCPU / defaultRAM
			ramGB := ram / 1024 / 1024 / 1024
			ramMultiple := cpu*cpuToRAMRatio + ramGB

			var nodePrice float64
			if newCnode.Cost != "" {
				nodePrice, err = strconv.ParseFloat(newCnode.Cost, 64)
				if err != nil {
					klog.V(3).Infof("Could not parse total node price")
					return nil, err
				}
			} else {
				nodePrice, err = strconv.ParseFloat(newCnode.VCPUCost, 64) // all the price was allocated the the CPU
				if err != nil {
					klog.V(3).Infof("Could not parse node vcpu price")
					return nil, err
				}
			}

			ramPrice := (nodePrice / ramMultiple)
			cpuPrice := ramPrice * cpuToRAMRatio

			newCnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
			newCnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			newCnode.RAMBytes = fmt.Sprintf("%f", ram)

			klog.V(4).Infof("Computed \"%s\" RAM Cost := %v", name, newCnode.RAMCost)
		}

		nodes[name] = &newCnode
	}

	return nodes, nil
}

func getPodServices(cache ClusterCache, podList []*v1.Pod) (map[string]map[string][]string, error) {
	servicesList := cache.GetAllServices()
	podServicesMapping := make(map[string]map[string][]string)
	for _, service := range servicesList {
		namespace := service.GetObjectMeta().GetNamespace()
		name := service.GetObjectMeta().GetName()

		if _, ok := podServicesMapping[namespace]; !ok {
			podServicesMapping[namespace] = make(map[string][]string)
		}
		s := labels.Set(service.Spec.Selector).AsSelectorPreValidated()
		for _, pod := range podList {
			labelSet := labels.Set(pod.GetObjectMeta().GetLabels())
			if s.Matches(labelSet) && pod.GetObjectMeta().GetNamespace() == namespace {
				services, ok := podServicesMapping[namespace][pod.GetObjectMeta().GetName()]
				if ok {
					podServicesMapping[namespace][pod.GetObjectMeta().GetName()] = append(services, name)
				} else {
					podServicesMapping[namespace][pod.GetObjectMeta().GetName()] = []string{name}
				}
			}
		}
	}
	return podServicesMapping, nil
}

func getPodDeployments(cache ClusterCache, podList []*v1.Pod) (map[string]map[string][]string, error) {
	deploymentsList := cache.GetAllDeployments()
	podDeploymentsMapping := make(map[string]map[string][]string) // namespace: podName: [deploymentNames]
	for _, deployment := range deploymentsList {
		namespace := deployment.GetObjectMeta().GetNamespace()
		name := deployment.GetObjectMeta().GetName()
		if _, ok := podDeploymentsMapping[namespace]; !ok {
			podDeploymentsMapping[namespace] = make(map[string][]string)
		}
		s, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			klog.V(2).Infof("Error doing deployment label conversion: " + err.Error())
		}
		for _, pod := range podList {
			labelSet := labels.Set(pod.GetObjectMeta().GetLabels())
			if s.Matches(labelSet) && pod.GetObjectMeta().GetNamespace() == namespace {
				deployments, ok := podDeploymentsMapping[namespace][pod.GetObjectMeta().GetName()]
				if ok {
					podDeploymentsMapping[namespace][pod.GetObjectMeta().GetName()] = append(deployments, name)
				} else {
					podDeploymentsMapping[namespace][pod.GetObjectMeta().GetName()] = []string{name}
				}
			}
		}
	}
	return podDeploymentsMapping, nil
}

func costDataPassesFilters(costs *CostData, namespace string, cluster string) bool {
	passesNamespace := namespace == "" || costs.Namespace == namespace
	passesCluster := cluster == "" || costs.ClusterID == cluster

	return passesNamespace && passesCluster
}

func (cm *CostModel) ComputeCostDataRange(cli prometheusClient.Client, clientset kubernetes.Interface, cp costAnalyzerCloud.Provider,
	startString, endString, windowString string, filterNamespace string, filterCluster string, remoteEnabled bool) (map[string]*CostData, error) {
	queryRAMRequests := fmt.Sprintf(queryRAMRequestsStr, windowString, "", windowString, "")
	queryRAMUsage := fmt.Sprintf(queryRAMUsageStr, windowString, "", windowString, "")
	queryCPURequests := fmt.Sprintf(queryCPURequestsStr, windowString, "", windowString, "")
	queryCPUUsage := fmt.Sprintf(queryCPUUsageStr, windowString, "")
	queryGPURequests := fmt.Sprintf(queryGPURequestsStr, windowString, "", windowString, "")
	queryPVRequests := fmt.Sprintf(queryPVRequestsStr)
	queryNetZoneRequests := fmt.Sprintf(queryZoneNetworkUsage, windowString, "")
	queryNetRegionRequests := fmt.Sprintf(queryRegionNetworkUsage, windowString, "")
	queryNetInternetRequests := fmt.Sprintf(queryInternetNetworkUsage, windowString, "")
	normalization := fmt.Sprintf(normalizationStr, windowString, "")

	layout := "2006-01-02T15:04:05.000Z"

	start, err := time.Parse(layout, startString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + startString + ". Error: " + err.Error())
		return nil, err
	}
	end, err := time.Parse(layout, endString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + endString + ". Error: " + err.Error())
		return nil, err
	}
	window, err := time.ParseDuration(windowString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + windowString + ". Error: " + err.Error())
		return nil, err
	}
	clusterID := os.Getenv(clusterIDKey)

	if remoteEnabled == true {
		remoteLayout := "2006-01-02T15:04:05Z"
		remoteStartStr := start.Format(remoteLayout)
		remoteEndStr := end.Format(remoteLayout)
		klog.V(1).Infof("Using remote database for query from %s to %s with window %s", startString, endString, windowString)
		return CostDataRangeFromSQL("", "", windowString, remoteStartStr, remoteEndStr)
	}

	var wg sync.WaitGroup
	wg.Add(11)

	var promErr error
	var resultRAMRequests interface{}
	go func() {
		resultRAMRequests, promErr = QueryRange(cli, queryRAMRequests, start, end, window)
		defer wg.Done()
	}()
	var resultRAMUsage interface{}
	go func() {
		resultRAMUsage, promErr = QueryRange(cli, queryRAMUsage, start, end, window)
		defer wg.Done()
	}()
	var resultCPURequests interface{}
	go func() {
		resultCPURequests, promErr = QueryRange(cli, queryCPURequests, start, end, window)
		defer wg.Done()
	}()
	var resultCPUUsage interface{}
	go func() {
		resultCPUUsage, promErr = QueryRange(cli, queryCPUUsage, start, end, window)
		defer wg.Done()
	}()
	var resultGPURequests interface{}
	go func() {
		resultGPURequests, promErr = QueryRange(cli, queryGPURequests, start, end, window)
		defer wg.Done()
	}()
	var resultPVRequests interface{}
	go func() {
		resultPVRequests, promErr = QueryRange(cli, queryPVRequests, start, end, window)
		defer wg.Done()
	}()
	var resultNetZoneRequests interface{}
	go func() {
		resultNetZoneRequests, promErr = QueryRange(cli, queryNetZoneRequests, start, end, window)
		defer wg.Done()
	}()
	var resultNetRegionRequests interface{}
	go func() {
		resultNetRegionRequests, promErr = QueryRange(cli, queryNetRegionRequests, start, end, window)
		defer wg.Done()
	}()
	var resultNetInternetRequests interface{}
	go func() {
		resultNetInternetRequests, promErr = QueryRange(cli, queryNetInternetRequests, start, end, window)
		defer wg.Done()
	}()
	var normalizationResult interface{}
	go func() {
		normalizationResult, promErr = Query(cli, normalization)
		defer wg.Done()
	}()

	podDeploymentsMapping := make(map[string]map[string][]string)
	podServicesMapping := make(map[string]map[string][]string)
	namespaceLabelsMapping := make(map[string]map[string]string)
	podlist := cm.Cache.GetAllPods()
	var k8sErr error
	go func() {

		podDeploymentsMapping, k8sErr = getPodDeployments(cm.Cache, podlist)
		if k8sErr != nil {
			return
		}

		podServicesMapping, k8sErr = getPodServices(cm.Cache, podlist)
		if k8sErr != nil {
			return
		}
		namespaceLabelsMapping, k8sErr = getNamespaceLabels(cm.Cache)
		if k8sErr != nil {
			return
		}

		wg.Done()
	}()

	wg.Wait()

	if promErr != nil {
		return nil, fmt.Errorf("Error querying prometheus: %s", promErr.Error())
	}
	if k8sErr != nil {
		return nil, fmt.Errorf("Error querying the kubernetes api: %s", k8sErr.Error())
	}

	normalizationValue, err := getNormalization(normalizationResult)
	if err != nil {
		return nil, fmt.Errorf("Error parsing normalization values: " + err.Error())
	}

	nodes, err := getNodeCost(cm.Cache, cp)
	if err != nil {
		klog.V(1).Infof("Warning, no cost model available: " + err.Error())
		return nil, err
	}

	pvClaimMapping, err := getPVInfoVectors(resultPVRequests, clusterID)
	if err != nil {
		// Just log for compatibility with KSM less than 1.6
		klog.Infof("Unable to get PV Data: %s", err.Error())
	}
	if pvClaimMapping != nil {
		err = addPVData(cm.Cache, pvClaimMapping, cp)
		if err != nil {
			return nil, err
		}
	}

	networkUsageMap, err := GetNetworkUsageData(resultNetZoneRequests, resultNetRegionRequests, resultNetInternetRequests, clusterID, true)
	if err != nil {
		klog.V(1).Infof("Unable to get Network Cost Data: %s", err.Error())
		networkUsageMap = make(map[string]*NetworkUsageData)
	}

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)

	RAMReqMap, err := GetContainerMetricVectors(resultRAMRequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMReqMap {
		containers[key] = true
	}

	RAMUsedMap, err := GetContainerMetricVectors(resultRAMUsage, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMUsedMap {
		containers[key] = true
	}
	CPUReqMap, err := GetContainerMetricVectors(resultCPURequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range CPUReqMap {
		containers[key] = true
	}
	GPUReqMap, err := GetContainerMetricVectors(resultGPURequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range GPUReqMap {
		containers[key] = true
	}
	CPUUsedMap, err := GetContainerMetricVectors(resultCPUUsage, false, 0, clusterID) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}
	currentContainers := make(map[string]v1.Pod)
	for _, pod := range podlist {
		if pod.Status.Phase != v1.PodRunning {
			continue
		}
		cs, err := newContainerMetricsFromPod(*pod, clusterID)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			containers[c.Key()] = true // captures any containers that existed for a time < a prometheus scrape interval. We currently charge 0 for this but should charge something.
			currentContainers[c.Key()] = *pod
		}
	}

	missingNodes := make(map[string]*costAnalyzerCloud.Node)
	missingContainers := make(map[string]*CostData)
	for key := range containers {
		if _, ok := containerNameCost[key]; ok {
			continue // because ordering is important for the allocation model (all PV's applied to the first), just dedupe if it's already been added.
		}
		if pod, ok := currentContainers[key]; ok {
			podName := pod.GetObjectMeta().GetName()
			ns := pod.GetObjectMeta().GetNamespace()
			nodeName := pod.Spec.NodeName
			var nodeData *costAnalyzerCloud.Node
			if _, ok := nodes[nodeName]; ok {
				nodeData = nodes[nodeName]
			}
			var podDeployments []string
			if _, ok := podDeploymentsMapping[ns]; ok {
				if ds, ok := podDeploymentsMapping[ns][pod.GetObjectMeta().GetName()]; ok {
					podDeployments = ds
				} else {
					podDeployments = []string{}
				}
			}

			var podPVs []*PersistentVolumeClaimData
			podClaims := pod.Spec.Volumes
			for _, vol := range podClaims {
				if vol.PersistentVolumeClaim != nil {
					name := vol.PersistentVolumeClaim.ClaimName
					if pvClaim, ok := pvClaimMapping[ns+","+name+","+clusterID]; ok {
						podPVs = append(podPVs, pvClaim)
					}
				}
			}

			var podNetCosts []*Vector
			if usage, ok := networkUsageMap[ns+","+podName+","+clusterID]; ok {
				netCosts, err := GetNetworkCost(usage, cp)
				if err != nil {
					klog.V(3).Infof("Error pulling network costs: %s", err.Error())
				} else {
					podNetCosts = netCosts
				}
			}

			var podServices []string
			if _, ok := podServicesMapping[ns]; ok {
				if svcs, ok := podServicesMapping[ns][pod.GetObjectMeta().GetName()]; ok {
					podServices = svcs
				} else {
					podServices = []string{}
				}
			}

			nsLabels := namespaceLabelsMapping[ns]
			podLabels := pod.GetObjectMeta().GetLabels()

			if podLabels == nil {
				podLabels = make(map[string]string)
			}

			for k, v := range nsLabels {
				podLabels[k] = v
			}

			for i, container := range pod.Spec.Containers {
				containerName := container.Name

				newKey := newContainerMetricFromValues(ns, podName, containerName, pod.Spec.NodeName, clusterID).Key()

				RAMReqV, ok := RAMReqMap[newKey]
				if !ok {
					klog.V(4).Info("no RAM requests for " + newKey)
					RAMReqV = []*Vector{}
				}
				RAMUsedV, ok := RAMUsedMap[newKey]
				if !ok {
					klog.V(4).Info("no RAM usage for " + newKey)
					RAMUsedV = []*Vector{}
				}
				CPUReqV, ok := CPUReqMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU requests for " + newKey)
					CPUReqV = []*Vector{}
				}
				GPUReqV, ok := GPUReqMap[newKey]
				if !ok {
					klog.V(4).Info("no GPU requests for " + newKey)
					GPUReqV = []*Vector{}
				}
				CPUUsedV, ok := CPUUsedMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU usage for " + newKey)
					CPUUsedV = []*Vector{}
				}

				var pvReq []*PersistentVolumeClaimData
				var netReq []*Vector
				if i == 0 { // avoid duplicating by just assigning all claims to the first container.
					pvReq = podPVs
					netReq = podNetCosts
				}

				costs := &CostData{
					Name:            containerName,
					PodName:         podName,
					NodeName:        nodeName,
					Namespace:       ns,
					Deployments:     podDeployments,
					Services:        podServices,
					Daemonsets:      getDaemonsetsOfPod(pod),
					Jobs:            getJobsOfPod(pod),
					Statefulsets:    getStatefulSetsOfPod(pod),
					NodeData:        nodeData,
					RAMReq:          RAMReqV,
					RAMUsed:         RAMUsedV,
					CPUReq:          CPUReqV,
					CPUUsed:         CPUUsedV,
					GPUReq:          GPUReqV,
					PVCData:         pvReq,
					Labels:          podLabels,
					NetworkData:     netReq,
					NamespaceLabels: nsLabels,
					ClusterID:       clusterID,
				}
				costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
				costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)

				if costDataPassesFilters(costs, filterNamespace, filterCluster) {
					containerNameCost[newKey] = costs
				}
			}

		} else {
			// The container has been deleted. Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
			klog.V(4).Info("The container " + key + " has been deleted. Calculating allocation but resulting object will be missing data.")
			c, _ := NewContainerMetricFromKey(key)
			RAMReqV, ok := RAMReqMap[key]
			if !ok {
				klog.V(4).Info("no RAM requests for " + key)
				RAMReqV = []*Vector{}
			}
			RAMUsedV, ok := RAMUsedMap[key]
			if !ok {
				klog.V(4).Info("no RAM usage for " + key)
				RAMUsedV = []*Vector{}
			}
			CPUReqV, ok := CPUReqMap[key]
			if !ok {
				klog.V(4).Info("no CPU requests for " + key)
				CPUReqV = []*Vector{}
			}
			GPUReqV, ok := GPUReqMap[key]
			if !ok {
				klog.V(4).Info("no GPU requests for " + key)
				GPUReqV = []*Vector{}
			}
			CPUUsedV, ok := CPUUsedMap[key]
			if !ok {
				klog.V(4).Info("no CPU usage for " + key)
				CPUUsedV = []*Vector{}
			}

			node, ok := nodes[c.NodeName]
			if !ok {
				klog.V(2).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
				if n, ok := missingNodes[c.NodeName]; ok {
					node = n
				} else {
					node = &costAnalyzerCloud.Node{}
					missingNodes[c.NodeName] = node
				}
			}
			namespacelabels, ok := namespaceLabelsMapping[c.Namespace]
			if !ok {
				klog.V(3).Infof("Missing data for namespace %s", c.Namespace)
			}
			costs := &CostData{
				Name:            c.ContainerName,
				PodName:         c.PodName,
				NodeName:        c.NodeName,
				NodeData:        node,
				Namespace:       c.Namespace,
				RAMReq:          RAMReqV,
				RAMUsed:         RAMUsedV,
				CPUReq:          CPUReqV,
				CPUUsed:         CPUUsedV,
				GPUReq:          GPUReqV,
				NamespaceLabels: namespacelabels,
				ClusterID:       c.ClusterID,
			}
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)

			if costDataPassesFilters(costs, filterNamespace, filterCluster) {
				containerNameCost[key] = costs
				missingContainers[key] = costs
			}
		}
	}

	w := end.Sub(start)
	w += window
	if w.Minutes() > 0 {
		wStr := fmt.Sprintf("%dm", int(w.Minutes()))
		err = findDeletedNodeInfo(cli, missingNodes, wStr)
		if err != nil {
			klog.V(1).Infof("Error fetching historical node data: %s", err.Error())
		}
		err = findDeletedPodInfo(cli, missingContainers, wStr)
		if err != nil {
			klog.V(1).Infof("Error fetching historical pod data: %s", err.Error())
		}
	}

	return containerNameCost, err
}

func getNamespaceLabels(cache ClusterCache) (map[string]map[string]string, error) {
	nsToLabels := make(map[string]map[string]string)
	nss := cache.GetAllNamespaces()
	for _, ns := range nss {
		nsToLabels[ns.Name] = ns.Labels
	}
	return nsToLabels, nil
}

func getDaemonsetsOfPod(pod v1.Pod) []string {
	for _, ownerReference := range pod.ObjectMeta.OwnerReferences {
		if ownerReference.Kind == "DaemonSet" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

func getJobsOfPod(pod v1.Pod) []string {
	for _, ownerReference := range pod.ObjectMeta.OwnerReferences {
		if ownerReference.Kind == "Job" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

func getStatefulSetsOfPod(pod v1.Pod) []string {
	for _, ownerReference := range pod.ObjectMeta.OwnerReferences {
		if ownerReference.Kind == "StatefulSet" {
			return []string{ownerReference.Name}
		}
	}
	return []string{}
}

type PersistentVolumeClaimData struct {
	Class      string                `json:"class"`
	Claim      string                `json:"claim"`
	Namespace  string                `json:"namespace"`
	ClusterID  string                `json:"clusterId"`
	VolumeName string                `json:"volumeName"`
	Volume     *costAnalyzerCloud.PV `json:"persistentVolume"`
	Values     []*Vector             `json:"values"`
}

func getCost(qr interface{}) (map[string][]*Vector, error) {
	toReturn := make(map[string][]*Vector)
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		instance, ok := metricMap["instance"]
		if !ok {
			return nil, fmt.Errorf("Instance field does not exist in data result vector")
		}
		instanceStr, ok := instance.(string)
		if !ok {
			return nil, fmt.Errorf("Instance is improperly formatted")
		}
		dataPoint, ok := val.(map[string]interface{})["value"]
		if !ok {
			return nil, fmt.Errorf("Value field does not exist in data result vector")
		}
		value, ok := dataPoint.([]interface{})
		if !ok || len(value) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		var vectors []*Vector
		strVal := value[1].(string)
		v, _ := strconv.ParseFloat(strVal, 64)

		vectors = append(vectors, &Vector{
			Timestamp: value[0].(float64),
			Value:     v,
		})
		toReturn[instanceStr] = vectors
	}

	return toReturn, nil
}

func getPVInfoVectors(qr interface{}, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	pvmap := make(map[string]*PersistentVolumeClaimData)
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	result, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	results, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}
	for _, val := range results {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		pvclaim, ok := metricMap["persistentvolumeclaim"]
		if !ok {
			return nil, fmt.Errorf("Claim field does not exist in data result vector")
		}
		pvclaimStr, ok := pvclaim.(string)
		if !ok {
			return nil, fmt.Errorf("Claim field improperly formatted")
		}
		pvnamespace, ok := metricMap["namespace"]
		if !ok {
			return nil, fmt.Errorf("Namespace field does not exist in data result vector")
		}
		pvnamespaceStr, ok := pvnamespace.(string)
		if !ok {
			return nil, fmt.Errorf("Namespace field improperly formatted")
		}
		pv, ok := metricMap["volumename"]
		if !ok {
			klog.V(3).Infof("Warning: Unfulfilled claim %s: volumename field does not exist in data result vector", pvclaimStr)
			pv = ""
		}
		pvStr, ok := pv.(string)
		if !ok {
			return nil, fmt.Errorf("Volumename field improperly formatted")
		}
		pvclass, ok := metricMap["storageclass"]
		if !ok { // TODO: We need to look up the actual PV and PV capacity. For now just proceed with "".
			klog.V(2).Infof("Storage Class not found for claim \"%s/%s\".", pvnamespaceStr, pvclaimStr)
			pvclass = ""
		}
		pvclassStr, ok := pvclass.(string)
		if !ok {
			return nil, fmt.Errorf("StorageClass field improperly formatted")
		}
		cid, ok := metricMap["cluster_id"]
		if !ok {
			klog.V(4).Info("Prometheus vector does not have cluster id")
			cid = defaultClusterID
		}
		clusterID, ok := cid.(string)
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
		}

		values, ok := val.(map[string]interface{})["values"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("Values field is improperly formatted")
		}
		var vectors []*Vector
		for _, value := range values {
			dataPoint, ok := value.([]interface{})
			if !ok || len(dataPoint) != 2 {
				return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
			}

			strVal := dataPoint[1].(string)
			v, _ := strconv.ParseFloat(strVal, 64)
			vectors = append(vectors, &Vector{
				Timestamp: math.Round(dataPoint[0].(float64)/10) * 10,
				Value:     v,
			})
		}
		key := pvnamespaceStr + "," + pvclaimStr + "," + clusterID
		pvmap[key] = &PersistentVolumeClaimData{
			Class:      pvclassStr,
			Claim:      pvclaimStr,
			Namespace:  pvnamespaceStr,
			ClusterID:  clusterID,
			VolumeName: pvStr,
			Values:     vectors,
		}
	}
	return pvmap, nil
}

func getPVInfoVector(qr interface{}, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	pvmap := make(map[string]*PersistentVolumeClaimData)
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	result, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	results, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}
	for _, val := range results {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		pvclaim, ok := metricMap["persistentvolumeclaim"]
		if !ok {
			return nil, fmt.Errorf("Claim field does not exist in data result vector")
		}
		pvclaimStr, ok := pvclaim.(string)
		if !ok {
			return nil, fmt.Errorf("Claim field improperly formatted")
		}
		pvnamespace, ok := metricMap["namespace"]
		if !ok {
			return nil, fmt.Errorf("Namespace field does not exist in data result vector")
		}
		pvnamespaceStr, ok := pvnamespace.(string)
		if !ok {
			return nil, fmt.Errorf("Namespace field improperly formatted")
		}
		pv, ok := metricMap["volumename"]
		if !ok {
			klog.V(3).Infof("Warning: Unfulfilled claim %s: volumename field does not exist in data result vector", pvclaimStr)
			pv = ""
		}
		pvStr, ok := pv.(string)
		if !ok {
			return nil, fmt.Errorf("Volumename field improperly formatted")
		}
		pvclass, ok := metricMap["storageclass"]
		if !ok { // TODO: We need to look up the actual PV and PV capacity. For now just proceed with "".
			klog.V(2).Infof("Storage Class not found for claim \"%s/%s\".", pvnamespaceStr, pvclaimStr)
			pvclass = ""
		}
		pvclassStr, ok := pvclass.(string)
		if !ok {
			return nil, fmt.Errorf("StorageClass field improperly formatted")
		}
		cid, ok := metricMap["cluster_id"]
		if !ok {
			klog.V(4).Info("Prometheus vector does not have cluster id")
			cid = defaultClusterID
		}
		clusterID, ok := cid.(string)
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
		}
		dataPoint, ok := val.(map[string]interface{})["value"]
		if !ok {
			return nil, fmt.Errorf("Value field does not exist in data result vector")
		}
		value, ok := dataPoint.([]interface{})
		if !ok || len(value) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		var vectors []*Vector
		strVal := value[1].(string)
		v, _ := strconv.ParseFloat(strVal, 64)

		vectors = append(vectors, &Vector{
			Timestamp: value[0].(float64),
			Value:     v,
		})

		key := pvnamespaceStr + "," + pvclaimStr + "," + clusterID
		pvmap[key] = &PersistentVolumeClaimData{
			Class:      pvclassStr,
			Claim:      pvclaimStr,
			Namespace:  pvnamespaceStr,
			ClusterID:  clusterID,
			VolumeName: pvStr,
			Values:     vectors,
		}
	}
	return pvmap, nil
}

func QueryRange(cli prometheusClient.Client, query string, start, end time.Time, step time.Duration) (interface{}, error) {
	u := cli.URL(epQueryRange, nil)
	q := u.Query()
	q.Set("query", query)
	q.Set("start", start.Format(time.RFC3339Nano))
	q.Set("end", end.Format(time.RFC3339Nano))
	q.Set("step", strconv.FormatFloat(step.Seconds(), 'f', 3, 64))
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}

	_, body, warnings, err := cli.Do(context.Background(), req)
	for _, w := range warnings {
		klog.V(3).Infof("%s", w)
	}
	if err != nil {
		return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
	}
	return toReturn, err
}

func Query(cli prometheusClient.Client, query string) (interface{}, error) {
	u := cli.URL(epQuery, nil)
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, body, warnings, err := cli.Do(context.Background(), req)
	for _, w := range warnings {
		klog.V(3).Infof("%s", w)
	}
	if err != nil {
		if resp == nil {
			return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
		}

		return nil, fmt.Errorf("%d Error %s fetching query %s", resp.StatusCode, err.Error(), query)
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
	}
	return toReturn, nil
}

//todo: don't cast, implement unmarshaler interface
func getNormalization(qr interface{}) (float64, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf(e)
	}
	results, ok := data.(map[string]interface{})["result"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("Result field not found in normalization response, aborting")
	}
	if len(results) > 0 {
		dataPoint := results[0].(map[string]interface{})["value"].([]interface{})
		if len(dataPoint) == 2 {
			strNorm := dataPoint[1].(string)
			val, _ := strconv.ParseFloat(strNorm, 64)
			return val, nil
		}
		return 0, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}
	return 0, fmt.Errorf("Normalization data is empty, kube-state-metrics or node-exporter may not be running")
}

type ContainerMetric struct {
	Namespace     string
	PodName       string
	ContainerName string
	NodeName      string
	ClusterID     string
}

func (c *ContainerMetric) Key() string {
	return c.Namespace + "," + c.PodName + "," + c.ContainerName + "," + c.NodeName + "," + c.ClusterID
}

func NewContainerMetricFromKey(key string) (*ContainerMetric, error) {
	s := strings.Split(key, ",")
	if len(s) == 5 {
		return &ContainerMetric{
			Namespace:     s[0],
			PodName:       s[1],
			ContainerName: s[2],
			NodeName:      s[3],
			ClusterID:     s[4],
		}, nil
	}
	return nil, fmt.Errorf("Not a valid key")
}

func newContainerMetricFromValues(ns string, podName string, containerName string, nodeName string, clusterId string) *ContainerMetric {
	return &ContainerMetric{
		Namespace:     ns,
		PodName:       podName,
		ContainerName: containerName,
		NodeName:      nodeName,
		ClusterID:     clusterId,
	}
}

func newContainerMetricsFromPod(pod v1.Pod, clusterID string) ([]*ContainerMetric, error) {
	podName := pod.GetObjectMeta().GetName()
	ns := pod.GetObjectMeta().GetNamespace()
	node := pod.Spec.NodeName
	var cs []*ContainerMetric
	for _, container := range pod.Spec.Containers {
		containerName := container.Name
		cs = append(cs, &ContainerMetric{
			Namespace:     ns,
			PodName:       podName,
			ContainerName: containerName,
			NodeName:      node,
			ClusterID:     clusterID,
		})
	}
	return cs, nil
}

func newContainerMetricFromPrometheus(metrics map[string]interface{}, defaultClusterID string) (*ContainerMetric, error) {
	cName, ok := metrics["container_name"]
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have container name")
	}
	containerName, ok := cName.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string container name")
	}
	pName, ok := metrics["pod_name"]
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have pod name")
	}
	podName, ok := pName.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string pod name")
	}
	ns, ok := metrics["namespace"]
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have namespace")
	}
	namespace, ok := ns.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string namespace")
	}
	node, ok := metrics["node"]
	if !ok {
		klog.V(4).Info("Prometheus vector does not have node name")
		node = ""
	}
	nodeName, ok := node.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string node")
	}
	cid, ok := metrics["cluster_id"]
	if !ok {
		klog.V(4).Info("Prometheus vector does not have cluster id")
		cid = defaultClusterID
	}
	clusterID, ok := cid.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
	}
	return &ContainerMetric{
		ContainerName: containerName,
		PodName:       podName,
		Namespace:     namespace,
		NodeName:      nodeName,
		ClusterID:     clusterID,
	}, nil
}

func GetContainerMetricVector(qr interface{}, normalize bool, normalizationValue float64, defaultClusterID string) (map[string][]*Vector, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	containerData := make(map[string][]*Vector)
	for _, val := range results {
		metric, ok := val.(map[string]interface{})["metric"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have metric labels")
		}
		containerMetric, err := newContainerMetricFromPrometheus(metric, defaultClusterID)
		if err != nil {
			return nil, err
		}
		value, ok := val.(map[string]interface{})["value"]
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
		}
		dataPoint, ok := value.([]interface{})
		if !ok || len(dataPoint) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		strVal := dataPoint[1].(string)
		v, _ := strconv.ParseFloat(strVal, 64)
		if normalize && normalizationValue != 0 {
			v = v / normalizationValue
		}
		toReturn := &Vector{
			Timestamp: dataPoint[0].(float64),
			Value:     v,
		}
		klog.V(4).Info("key: " + containerMetric.Key())
		containerData[containerMetric.Key()] = []*Vector{toReturn}
	}
	return containerData, nil
}

func GetContainerMetricVectors(qr interface{}, normalize bool, normalizationValue float64, defaultClusterID string) (map[string][]*Vector, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	containerData := make(map[string][]*Vector)
	for _, val := range results {
		metric, ok := val.(map[string]interface{})["metric"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have metric labels")
		}
		containerMetric, err := newContainerMetricFromPrometheus(metric, defaultClusterID)
		if err != nil {
			return nil, err
		}
		vs, ok := val.(map[string]interface{})["values"]
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, values is not a field in the vector")
		}
		values, ok := vs.([]interface{})
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, values is not a slice")
		}
		var vectors []*Vector
		for _, value := range values {
			dataPoint, ok := value.([]interface{})
			if !ok || len(dataPoint) != 2 {
				return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
			}
			strVal := dataPoint[1].(string)
			v, _ := strconv.ParseFloat(strVal, 64)
			if normalize && normalizationValue != 0 {
				v = v / normalizationValue
			}
			vectors = append(vectors, &Vector{
				Timestamp: math.Round(dataPoint[0].(float64)/10) * 10,
				Value:     v,
			})
		}
		containerData[containerMetric.Key()] = vectors
	}
	return containerData, nil
}

func wrapPrometheusError(qr interface{}) (string, error) {
	e, ok := qr.(map[string]interface{})["error"]
	if !ok {
		return "", fmt.Errorf("Unexpected response from Prometheus")
	}
	eStr, ok := e.(string)
	return eStr, nil
}
