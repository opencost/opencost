package costmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	"github.com/kubecost/cost-model/clustercache"
	prometheusClient "github.com/prometheus/client_golang/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
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
	Cache        clustercache.ClusterCache
	RequestGroup *singleflight.Group
}

func NewCostModel(cache clustercache.ClusterCache) *CostModel {
	// request grouping to prevent over-requesting the same data prior to caching
	requestGroup := new(singleflight.Group)

	return &CostModel{
		Cache:        cache,
		RequestGroup: requestGroup,
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
	) by (namespace,container_name,pod_name,node,cluster_id) 
	* on (pod_name, namespace, node, cluster_id) group_right(container_name) label_replace(avg_over_time(kube_pod_status_phase{phase="Running"}[%s] %s), "pod_name","$1","pod","(.+)")`
	queryPVRequestsStr = `avg(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass, namespace, volumename, cluster_id) 
						* 
						on (persistentvolumeclaim, namespace, cluster_id) group_right(storageclass, volumename) 
				sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace, cluster_id)`
	queryRAMAllocation = `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(container_memory_allocation_bytes{container!="",container!="POD", node!=""}[%s] %s) 
					*  
					avg_over_time(container_memory_allocation_bytes{container!="",container!="POD", node!=""}[%s] %s)
				) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryCPUAllocation = `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(container_cpu_allocation{container!="",container!="POD", node!=""}[%s] %s) 
					*  
					avg_over_time(container_cpu_allocation{container!="",container!="POD", node!=""}[%s] %s)
				) by (namespace,container,pod,node,cluster_id) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node,cluster_id)`
	queryPVCAllocation        = `avg_over_time(pod_pvc_allocation[%s])`
	queryPVHourlyCost         = `avg_over_time(pv_hourly_cost[%s])`
	queryNSLabels             = `avg_over_time(kube_namespace_labels[%s])`
	queryPodLabels            = `avg_over_time(kube_pod_labels[%s])`
	queryDeploymentLabels     = `avg_over_time(deployment_match_labels[%s])`
	queryStatefulsetLabels    = `avg_over_time(statefulSet_match_labels[%s])`
	queryServiceLabels        = `avg_over_time(service_selector_labels[%s])`
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
func ValidatePrometheus(cli prometheusClient.Client, isThanos bool) (*PrometheusMetadata, error) {
	q := "up"
	if isThanos {
		q += " offset 3h"
	}
	data, err := Query(cli, q)
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
	queryGPURequests := fmt.Sprintf(queryGPURequestsStr, window, offset, window, offset, window, offset)
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

		podDeploymentsMapping, k8sErr = getPodDeployments(cm.Cache, podlist, clusterID)
		if k8sErr != nil {
			return
		}

		podServicesMapping, k8sErr = getPodServices(cm.Cache, podlist, clusterID)
		if k8sErr != nil {
			return
		}
		namespaceLabelsMapping, k8sErr = getNamespaceLabels(cm.Cache, clusterID)
		if k8sErr != nil {
			return
		}
	}()

	wg.Wait()

	defer measureTime(time.Now(), "ComputeCostData: Processing Query Data")

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

	nodes, err := cm.GetNodeCost(cp)
	if err != nil {
		klog.V(1).Infof("Warning, no Node cost model available: " + err.Error())
		return nil, err
	}

	pvClaimMapping, err := GetPVInfo(resultPVRequests, clusterID)
	if err != nil {
		klog.Infof("Unable to get PV Data: %s", err.Error())
	}
	if pvClaimMapping != nil {
		err = addPVData(cm.Cache, pvClaimMapping, cp)
		if err != nil {
			return nil, err
		}
	}

	networkUsageMap, err := GetNetworkUsageData(resultNetZoneRequests, resultNetRegionRequests, resultNetInternetRequests, clusterID)
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

			nsLabels := namespaceLabelsMapping[ns+","+clusterID]
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

			nsKey := ns + "," + clusterID

			var podDeployments []string
			if _, ok := podDeploymentsMapping[nsKey]; ok {
				if ds, ok := podDeploymentsMapping[nsKey][pod.GetObjectMeta().GetName()]; ok {
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
			if _, ok := podServicesMapping[nsKey]; ok {
				if svcs, ok := podServicesMapping[nsKey][pod.GetObjectMeta().GetName()]; ok {
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
				klog.V(4).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
				if n, ok := missingNodes[c.NodeName]; ok {
					node = n
				} else {
					node = &costAnalyzerCloud.Node{}
					missingNodes[c.NodeName] = node
				}
			}
			namespacelabels, ok := namespaceLabelsMapping[c.Namespace+","+c.ClusterID]
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
		defer measureTime(time.Now(), "Finding Deleted Node Info")

		q := make([]string, 0, len(missingNodes))
		for nodename := range missingNodes {
			klog.V(4).Infof("Finding data for deleted node %v", nodename)
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
	// The result of the normalize operation will be a new []*Vector to replace the requests
	allocationOp := func(r *Vector, x *float64, y *float64) bool {
		if x != nil && y != nil {
			r.Value = math.Max(*x, *y)
		} else if x != nil {
			r.Value = *x
		} else if y != nil {
			r.Value = *y
		}

		return true
	}

	return ApplyVectorOp(req, used, allocationOp)
}

func addPVData(cache clustercache.ClusterCache, pvClaimMapping map[string]*PersistentVolumeClaimData, cloud costAnalyzerCloud.Provider) error {
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
			klog.V(4).Infof("PV not found, using default")
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

func (cm *CostModel) GetNodeCost(cp costAnalyzerCloud.Provider) (map[string]*costAnalyzerCloud.Node, error) {
	cfg, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}

	nodeList := cm.Cache.GetAllNodes()
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

			defaultGPU, err := strconv.ParseFloat(cfg.GPU, 64)
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

	cp.ApplyReservedInstancePricing(nodes)

	return nodes, nil
}

func getPodServices(cache clustercache.ClusterCache, podList []*v1.Pod, clusterID string) (map[string]map[string][]string, error) {
	servicesList := cache.GetAllServices()
	podServicesMapping := make(map[string]map[string][]string)
	for _, service := range servicesList {
		namespace := service.GetObjectMeta().GetNamespace()
		name := service.GetObjectMeta().GetName()
		key := namespace + "," + clusterID
		if _, ok := podServicesMapping[key]; !ok {
			podServicesMapping[key] = make(map[string][]string)
		}
		s := labels.Set(service.Spec.Selector).AsSelectorPreValidated()
		for _, pod := range podList {
			labelSet := labels.Set(pod.GetObjectMeta().GetLabels())
			if s.Matches(labelSet) && pod.GetObjectMeta().GetNamespace() == namespace {
				services, ok := podServicesMapping[key][pod.GetObjectMeta().GetName()]
				if ok {
					podServicesMapping[key][pod.GetObjectMeta().GetName()] = append(services, name)
				} else {
					podServicesMapping[key][pod.GetObjectMeta().GetName()] = []string{name}
				}
			}
		}
	}
	return podServicesMapping, nil
}

func getPodStatefulsets(cache clustercache.ClusterCache, podList []*v1.Pod, clusterID string) (map[string]map[string][]string, error) {
	ssList := cache.GetAllStatefulSets()
	podSSMapping := make(map[string]map[string][]string) // namespace: podName: [deploymentNames]
	for _, ss := range ssList {
		namespace := ss.GetObjectMeta().GetNamespace()
		name := ss.GetObjectMeta().GetName()

		key := namespace + "," + clusterID
		if _, ok := podSSMapping[key]; !ok {
			podSSMapping[key] = make(map[string][]string)
		}
		s, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
		if err != nil {
			klog.V(2).Infof("Error doing deployment label conversion: " + err.Error())
		}
		for _, pod := range podList {
			labelSet := labels.Set(pod.GetObjectMeta().GetLabels())
			if s.Matches(labelSet) && pod.GetObjectMeta().GetNamespace() == namespace {
				sss, ok := podSSMapping[key][pod.GetObjectMeta().GetName()]
				if ok {
					podSSMapping[key][pod.GetObjectMeta().GetName()] = append(sss, name)
				} else {
					podSSMapping[key][pod.GetObjectMeta().GetName()] = []string{name}
				}
			}
		}
	}
	return podSSMapping, nil

}

func getPodDeployments(cache clustercache.ClusterCache, podList []*v1.Pod, clusterID string) (map[string]map[string][]string, error) {
	deploymentsList := cache.GetAllDeployments()
	podDeploymentsMapping := make(map[string]map[string][]string) // namespace: podName: [deploymentNames]
	for _, deployment := range deploymentsList {
		namespace := deployment.GetObjectMeta().GetNamespace()
		name := deployment.GetObjectMeta().GetName()

		key := namespace + "," + clusterID
		if _, ok := podDeploymentsMapping[key]; !ok {
			podDeploymentsMapping[key] = make(map[string][]string)
		}
		s, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			klog.V(2).Infof("Error doing deployment label conversion: " + err.Error())
		}
		for _, pod := range podList {
			labelSet := labels.Set(pod.GetObjectMeta().GetLabels())
			if s.Matches(labelSet) && pod.GetObjectMeta().GetNamespace() == namespace {
				deployments, ok := podDeploymentsMapping[key][pod.GetObjectMeta().GetName()]
				if ok {
					podDeploymentsMapping[key][pod.GetObjectMeta().GetName()] = append(deployments, name)
				} else {
					podDeploymentsMapping[key][pod.GetObjectMeta().GetName()] = []string{name}
				}
			}
		}
	}
	return podDeploymentsMapping, nil
}

func getPodDeploymentsWithMetrics(deploymentLabels map[string]map[string]string, podLabels map[string]map[string]string) (map[string]map[string][]string, error) {
	podDeploymentsMapping := make(map[string]map[string][]string)

	for depKey, depLabels := range deploymentLabels {
		kt, err := NewKeyTuple(depKey)
		if err != nil {
			continue
		}

		namespace := kt.Namespace
		name := kt.Key
		clusterID := kt.ClusterID

		key := namespace + "," + clusterID
		if _, ok := podDeploymentsMapping[key]; !ok {
			podDeploymentsMapping[key] = make(map[string][]string)
		}
		s := labels.Set(depLabels).AsSelectorPreValidated()
		for podKey, pLabels := range podLabels {
			pkey, err := NewKeyTuple(podKey)
			if err != nil {
				continue
			}
			podNamespace := pkey.Namespace
			podName := pkey.Key
			podClusterID := pkey.ClusterID

			labelSet := labels.Set(pLabels)
			if s.Matches(labelSet) && podNamespace == namespace && podClusterID == clusterID {
				deployments, ok := podDeploymentsMapping[key][podName]
				if ok {
					podDeploymentsMapping[key][podName] = append(deployments, name)
				} else {
					podDeploymentsMapping[key][podName] = []string{name}
				}
			}
		}
	}

	// Remove any duplicate data created by metric names
	pruneDuplicateData(podDeploymentsMapping)

	return podDeploymentsMapping, nil
}

func getPodServicesWithMetrics(serviceLabels map[string]map[string]string, podLabels map[string]map[string]string) (map[string]map[string][]string, error) {
	podServicesMapping := make(map[string]map[string][]string)

	for servKey, servLabels := range serviceLabels {
		kt, err := NewKeyTuple(servKey)
		if err != nil {
			continue
		}

		namespace := kt.Namespace
		name := kt.Key
		clusterID := kt.ClusterID

		key := namespace + "," + clusterID
		if _, ok := podServicesMapping[key]; !ok {
			podServicesMapping[key] = make(map[string][]string)
		}
		s := labels.Set(servLabels).AsSelectorPreValidated()

		for podKey, pLabels := range podLabels {
			pkey, err := NewKeyTuple(podKey)
			if err != nil {
				continue
			}
			podNamespace := pkey.Namespace
			podName := pkey.Key
			podClusterID := pkey.ClusterID

			labelSet := labels.Set(pLabels)
			if s.Matches(labelSet) && podNamespace == namespace && podClusterID == clusterID {
				services, ok := podServicesMapping[key][podName]
				if ok {
					podServicesMapping[key][podName] = append(services, name)
				} else {
					podServicesMapping[key][podName] = []string{name}
				}
			}
		}
	}

	// Remove any duplicate data created by metric names
	pruneDuplicateData(podServicesMapping)

	return podServicesMapping, nil
}

// This method alleviates an issue with metrics that used a '_' to replace '-' in deployment
// and service names. To avoid counting these as multiple deployments/services, we'll remove
// the '_' version. Not optimal, but takes care of the issue
func pruneDuplicateData(data map[string]map[string][]string) {
	for _, podMap := range data {
		for podName, values := range podMap {
			podMap[podName] = pruneDuplicates(values)
		}
	}
}

// Determine if there is an underscore in the value of a slice. If so, replace _ with -, and then
// check to see if the result exists in the slice. If both are true, then we DO NOT include that
// original value in the new slice.
func pruneDuplicates(s []string) []string {
	m := sliceToSet(s)

	for _, v := range s {
		if strings.Contains(v, "_") {
			name := strings.Replace(v, "_", "-", -1)
			if !m[name] {
				m[name] = true
			}
			delete(m, v)
		}
	}

	return setToSlice(m)
}

// Creates a map[string]bool containing the slice values as keys
func sliceToSet(s []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range s {
		m[v] = true
	}
	return m
}

func setToSlice(m map[string]bool) []string {
	var result []string
	for k, _ := range m {
		result = append(result, k)
	}
	return result
}

func costDataPassesFilters(costs *CostData, namespace string, cluster string) bool {
	passesNamespace := namespace == "" || costs.Namespace == namespace
	passesCluster := cluster == "" || costs.ClusterID == cluster

	return passesNamespace && passesCluster
}

// Attempt to create a key for the request. Reduce the times to minutes in order to more easily group requests based on
// real time ranges. If for any reason, the key generation fails, return a uuid to ensure uniqueness.
func requestKeyFor(startString string, endString string, windowString string, filterNamespace string, filterCluster string, remoteEnabled bool) string {
	fullLayout := "2006-01-02T15:04:05.000Z"
	keyLayout := "2006-01-02T15:04Z"

	sTime, err := time.Parse(fullLayout, startString)
	if err != nil {
		return uuid.New().String()
	}
	eTime, err := time.Parse(fullLayout, startString)
	if err != nil {
		return uuid.New().String()
	}

	startKey := sTime.Format(keyLayout)
	endKey := eTime.Format(keyLayout)

	return fmt.Sprintf("%s,%s,%s,%s,%s,%t", startKey, endKey, windowString, filterNamespace, filterCluster, remoteEnabled)
}

// Executes a range query for cost data
func (cm *CostModel) ComputeCostDataRange(cli prometheusClient.Client, clientset kubernetes.Interface, cp costAnalyzerCloud.Provider,
	startString, endString, windowString string, filterNamespace string, filterCluster string, remoteEnabled bool) (map[string]*CostData, error) {
	// Create a request key for request grouping. This key will be used to represent the cost-model result
	// for the specific inputs to prevent multiple queries for identical data.
	key := requestKeyFor(startString, endString, windowString, filterNamespace, filterCluster, remoteEnabled)

	klog.V(4).Infof("ComputeCostDataRange with Key: %s", key)

	// If there is already a request out that uses the same data, wait for it to return to share the results.
	// Otherwise, start executing.
	result, err, _ := cm.RequestGroup.Do(key, func() (interface{}, error) {
		return cm.costDataRange(cli, clientset, cp, startString, endString, windowString, filterNamespace, filterCluster, remoteEnabled)
	})

	data, ok := result.(map[string]*CostData)
	if !ok {
		return nil, fmt.Errorf("Failed to cast result as map[string]*CostData")
	}

	return data, err
}

func (cm *CostModel) costDataRange(cli prometheusClient.Client, clientset kubernetes.Interface, cp costAnalyzerCloud.Provider,
	startString, endString, windowString string, filterNamespace string, filterCluster string, remoteEnabled bool) (map[string]*CostData, error) {
	queryRAMRequests := fmt.Sprintf(queryRAMRequestsStr, windowString, "", windowString, "")
	queryRAMUsage := fmt.Sprintf(queryRAMUsageStr, windowString, "", windowString, "")
	queryCPURequests := fmt.Sprintf(queryCPURequestsStr, windowString, "", windowString, "")
	queryCPUUsage := fmt.Sprintf(queryCPUUsageStr, windowString, "")
	queryRAMAlloc := fmt.Sprintf(queryRAMAllocation, windowString, "", windowString, "")
	queryCPUAlloc := fmt.Sprintf(queryCPUAllocation, windowString, "", windowString, "")
	queryGPURequests := fmt.Sprintf(queryGPURequestsStr, windowString, "", windowString, "", windowString, "")
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

	numQueries := 20

	var wg sync.WaitGroup
	wg.Add(numQueries)

	queryProfileStart := time.Now()
	queryProfileCh := make(chan string, numQueries)

	var promErr error
	var resultRAMRequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "RAMRequests", queryProfileCh)

		resultRAMRequests, promErr = QueryRange(cli, queryRAMRequests, start, end, window)
	}()
	var resultRAMUsage interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "RAMUsage", queryProfileCh)

		resultRAMUsage, promErr = QueryRange(cli, queryRAMUsage, start, end, window)
	}()
	var resultCPURequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "CPURequests", queryProfileCh)

		resultCPURequests, promErr = QueryRange(cli, queryCPURequests, start, end, window)
	}()
	var resultCPUUsage interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "CPUUsage", queryProfileCh)

		resultCPUUsage, promErr = QueryRange(cli, queryCPUUsage, start, end, window)
	}()
	var resultRAMAllocations interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "RAMAllocations", queryProfileCh)

		resultRAMAllocations, promErr = QueryRange(cli, queryRAMAlloc, start, end, window)
	}()
	var resultCPUAllocations interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "CPUAllocations", queryProfileCh)

		resultCPUAllocations, promErr = QueryRange(cli, queryCPUAlloc, start, end, window)
	}()
	var resultGPURequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "GPURequests", queryProfileCh)

		resultGPURequests, promErr = QueryRange(cli, queryGPURequests, start, end, window)
	}()
	var resultPVRequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "PVRequests", queryProfileCh)

		resultPVRequests, promErr = QueryRange(cli, queryPVRequests, start, end, window)
	}()
	var resultNetZoneRequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "NetZoneRequests", queryProfileCh)

		resultNetZoneRequests, promErr = QueryRange(cli, queryNetZoneRequests, start, end, window)
	}()
	var resultNetRegionRequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "NetRegionRequests", queryProfileCh)

		resultNetRegionRequests, promErr = QueryRange(cli, queryNetRegionRequests, start, end, window)
	}()
	var resultNetInternetRequests interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "NetInternetRequests", queryProfileCh)

		resultNetInternetRequests, promErr = QueryRange(cli, queryNetInternetRequests, start, end, window)
	}()
	var pvPodAllocationResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "PVPodAllocation", queryProfileCh)

		pvPodAllocationResults, promErr = QueryRange(cli, fmt.Sprintf(queryPVCAllocation, windowString), start, end, window)
	}()
	var pvCostResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "PVCost", queryProfileCh)

		pvCostResults, promErr = QueryRange(cli, fmt.Sprintf(queryPVHourlyCost, windowString), start, end, window)
	}()
	var nsLabelsResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "NSLabels", queryProfileCh)

		nsLabelsResults, promErr = QueryRange(cli, fmt.Sprintf(queryNSLabels, windowString), start, end, window)
	}()
	var podLabelsResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "PodLabels", queryProfileCh)

		podLabelsResults, promErr = QueryRange(cli, fmt.Sprintf(queryPodLabels, windowString), start, end, window)
	}()
	var serviceLabelsResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "ServiceLabels", queryProfileCh)

		serviceLabelsResults, promErr = QueryRange(cli, fmt.Sprintf(queryServiceLabels, windowString), start, end, window)
	}()
	var deploymentLabelsResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "DeploymentLabels", queryProfileCh)

		deploymentLabelsResults, promErr = QueryRange(cli, fmt.Sprintf(queryDeploymentLabels, windowString), start, end, window)
	}()
	var statefulsetLabelsResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "StatefulSetLabels", queryProfileCh)

		statefulsetLabelsResults, promErr = QueryRange(cli, fmt.Sprintf(queryStatefulsetLabels, windowString), start, end, window)
	}()
	var normalizationResults interface{}
	go func() {
		defer wg.Done()
		defer measureTimeAsync(time.Now(), "Normalization", queryProfileCh)

		normalizationResults, promErr = QueryRange(cli, normalization, start, end, window)
	}()

	podDeploymentsMapping := make(map[string]map[string][]string)
	podStatefulsetsMapping := make(map[string]map[string][]string)
	podServicesMapping := make(map[string]map[string][]string)
	namespaceLabelsMapping := make(map[string]map[string]string)
	podlist := cm.Cache.GetAllPods()
	var k8sErr error
	go func() {
		defer wg.Done()

		podDeploymentsMapping, k8sErr = getPodDeployments(cm.Cache, podlist, clusterID)
		if k8sErr != nil {
			return
		}

		podStatefulsetsMapping, k8sErr = getPodStatefulsets(cm.Cache, podlist, clusterID)
		if k8sErr != nil {
			return
		}

		podServicesMapping, k8sErr = getPodServices(cm.Cache, podlist, clusterID)
		if k8sErr != nil {
			return
		}
		namespaceLabelsMapping, k8sErr = getNamespaceLabels(cm.Cache, clusterID)
		if k8sErr != nil {
			return
		}
	}()

	wg.Wait()

	// collect all query profiling messages
	close(queryProfileCh)
	queryProfileBreakdown := ""
	for msg := range queryProfileCh {
		queryProfileBreakdown += "\n - " + msg
	}
	measureTime(queryProfileStart, fmt.Sprintf("costDataRange(%s): Prom/k8s Queries: %s", windowString, queryProfileBreakdown))

	defer measureTime(time.Now(), fmt.Sprintf("costDataRange(%s): Processing Query Data", windowString))

	if promErr != nil {
		return nil, fmt.Errorf("Error querying prometheus: %s", promErr.Error())
	}
	if k8sErr != nil {
		return nil, fmt.Errorf("Error querying the kubernetes api: %s", k8sErr.Error())
	}

	profileStart := time.Now()

	normalizationValue, err := getNormalizations(normalizationResults)
	if err != nil {
		return nil, fmt.Errorf("error computing normalization for start=%s, end=%s, window=%s: %s",
			start, end, window, err.Error())
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): compute normalizations", windowString))

	profileStart = time.Now()

	nodes, err := cm.GetNodeCost(cp)
	if err != nil {
		klog.V(1).Infof("Warning, no cost model available: " + err.Error())
		return nil, err
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): GetNodeCost", windowString))

	profileStart = time.Now()

	pvClaimMapping, err := GetPVInfo(resultPVRequests, clusterID)
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

	pvCostMapping, err := GetPVCostMetrics(pvCostResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get PV Hourly Cost Data: %s", err.Error())
	}

	pvAllocationMapping, err := GetPVAllocationMetrics(pvPodAllocationResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get PV Allocation Cost Data: %s", err.Error())
	}
	if pvAllocationMapping != nil {
		addMetricPVData(pvAllocationMapping, pvCostMapping, cp)
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): process PV data", windowString))

	profileStart = time.Now()

	nsLabels, err := GetNamespaceLabelsMetrics(nsLabelsResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Namespace Labels for Metrics: %s", err.Error())
	}
	if nsLabels != nil {
		appendNamespaceLabels(namespaceLabelsMapping, nsLabels)
	}

	podLabels, err := GetPodLabelsMetrics(podLabelsResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Pod Labels for Metrics: %s", err.Error())
	}

	serviceLabels, err := GetServiceSelectorLabelsMetrics(serviceLabelsResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Service Selector Labels for Metrics: %s", err.Error())
	}

	deploymentLabels, err := GetDeploymentMatchLabelsMetrics(deploymentLabelsResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Deployment Match Labels for Metrics: %s", err.Error())
	}

	statefulsetLabels, err := GetStatefulsetMatchLabelsMetrics(statefulsetLabelsResults, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Deployment Match Labels for Metrics: %s", err.Error())
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): process labels", windowString))

	profileStart = time.Now()

	podStatefulsetMetricsMapping, err := getPodDeploymentsWithMetrics(statefulsetLabels, podLabels)
	if err != nil {
		klog.V(1).Infof("Unable to get match Statefulset Labels Metrics to Pods: %s", err.Error())
	}
	appendLabelsList(podStatefulsetsMapping, podStatefulsetMetricsMapping)

	podDeploymentsMetricsMapping, err := getPodDeploymentsWithMetrics(deploymentLabels, podLabels)
	if err != nil {
		klog.V(1).Infof("Unable to get match Deployment Labels Metrics to Pods: %s", err.Error())
	}
	appendLabelsList(podDeploymentsMapping, podDeploymentsMetricsMapping)

	podServicesMetricsMapping, err := getPodServicesWithMetrics(serviceLabels, podLabels)
	if err != nil {
		klog.V(1).Infof("Unable to get match Service Labels Metrics to Pods: %s", err.Error())
	}
	appendLabelsList(podServicesMapping, podServicesMetricsMapping)

	networkUsageMap, err := GetNetworkUsageData(resultNetZoneRequests, resultNetRegionRequests, resultNetInternetRequests, clusterID)
	if err != nil {
		klog.V(1).Infof("Unable to get Network Cost Data: %s", err.Error())
		networkUsageMap = make(map[string]*NetworkUsageData)
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): process deployments, services, and network usage", windowString))

	profileStart = time.Now()

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)
	otherClusterPVRecorded := make(map[string]bool)

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
	CPUUsedMap, err := GetContainerMetricVectors(resultCPUUsage, false, normalizationValue, clusterID) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}

	RAMAllocMap, err := GetContainerMetricVectors(resultRAMAllocations, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range RAMAllocMap {
		containers[key] = true
	}
	CPUAllocMap, err := GetContainerMetricVectors(resultCPUAllocations, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range CPUAllocMap {
		containers[key] = true
	}
	GPUReqMap, err := GetContainerMetricVectors(resultGPURequests, true, normalizationValue, clusterID)
	if err != nil {
		return nil, err
	}
	for key := range GPUReqMap {
		containers[key] = true
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): GetContainerMetricVectors", windowString))

	profileStart = time.Now()

	// Request metrics can show up after pod eviction and completion.
	// This method synchronizes requests to allocations such that when
	// allocation is 0, so are requests
	applyAllocationToRequests(RAMAllocMap, RAMReqMap)
	applyAllocationToRequests(CPUAllocMap, CPUReqMap)

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

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): applyAllocationToRequests", windowString))

	profileStart = time.Now()

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

			nsKey := ns + "," + clusterID
			var podDeployments []string
			if _, ok := podDeploymentsMapping[nsKey]; ok {
				if ds, ok := podDeploymentsMapping[nsKey][pod.GetObjectMeta().GetName()]; ok {
					podDeployments = ds
				} else {
					podDeployments = []string{}
				}
			}
			var podStatefulSets []string
			if _, ok := podStatefulsetsMapping[nsKey]; ok {
				if ds, ok := podStatefulsetsMapping[nsKey][pod.GetObjectMeta().GetName()]; ok {
					podStatefulSets = ds
				} else {
					podStatefulSets = []string{}
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
			if _, ok := podServicesMapping[nsKey]; ok {
				if svcs, ok := podServicesMapping[nsKey][pod.GetObjectMeta().GetName()]; ok {
					podServices = svcs
				} else {
					podServices = []string{}
				}
			}

			nsLabels := namespaceLabelsMapping[nsKey]
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
				CPUUsedV, ok := CPUUsedMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU usage for " + newKey)
					CPUUsedV = []*Vector{}
				}
				RAMAllocsV, ok := RAMAllocMap[newKey]
				if !ok {
					klog.V(4).Info("no RAM allocation for " + newKey)
					RAMAllocsV = []*Vector{}
				}
				CPUAllocsV, ok := CPUAllocMap[newKey]
				if !ok {
					klog.V(4).Info("no CPU allocation for " + newKey)
					CPUAllocsV = []*Vector{}
				}
				GPUReqV, ok := GPUReqMap[newKey]
				if !ok {
					klog.V(4).Info("no GPU requests for " + newKey)
					GPUReqV = []*Vector{}
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
					Statefulsets:    podStatefulSets,
					NodeData:        nodeData,
					RAMReq:          RAMReqV,
					RAMUsed:         RAMUsedV,
					CPUReq:          CPUReqV,
					CPUUsed:         CPUUsedV,
					RAMAllocation:   RAMAllocsV,
					CPUAllocation:   CPUAllocsV,
					GPUReq:          GPUReqV,
					PVCData:         pvReq,
					Labels:          podLabels,
					NetworkData:     netReq,
					NamespaceLabels: nsLabels,
					ClusterID:       clusterID,
				}

				if costDataPassesFilters(costs, filterNamespace, filterCluster) {
					containerNameCost[newKey] = costs
				}
			}

		} else {
			// The container has been deleted, or is from a different clusterID
			// Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
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
			CPUUsedV, ok := CPUUsedMap[key]
			if !ok {
				klog.V(4).Info("no CPU usage for " + key)
				CPUUsedV = []*Vector{}
			}
			RAMAllocsV, ok := RAMAllocMap[key]
			if !ok {
				klog.V(4).Info("no RAM allocation for " + key)
				RAMAllocsV = []*Vector{}
			}
			CPUAllocsV, ok := CPUAllocMap[key]
			if !ok {
				klog.V(4).Info("no CPU allocation for " + key)
				CPUAllocsV = []*Vector{}
			}
			GPUReqV, ok := GPUReqMap[key]
			if !ok {
				klog.V(4).Info("no GPU requests for " + key)
				GPUReqV = []*Vector{}
			}

			node, ok := nodes[c.NodeName]
			if !ok {
				klog.V(4).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
				if n, ok := missingNodes[c.NodeName]; ok {
					node = n
				} else {
					node = &costAnalyzerCloud.Node{}
					missingNodes[c.NodeName] = node
				}
			}

			nsKey := c.Namespace + "," + c.ClusterID
			podKey := c.Namespace + "," + c.PodName + "," + c.ClusterID

			namespaceLabels, ok := namespaceLabelsMapping[nsKey]
			if !ok {
				klog.V(3).Infof("Missing data for namespace %s", c.Namespace)
			}

			pLabels := podLabels[podKey]
			if pLabels == nil {
				pLabels = make(map[string]string)
			}

			for k, v := range namespaceLabels {
				pLabels[k] = v
			}

			var podDeployments []string
			if _, ok := podDeploymentsMapping[nsKey]; ok {
				if ds, ok := podDeploymentsMapping[nsKey][c.PodName]; ok {
					podDeployments = ds
				} else {
					podDeployments = []string{}
				}
			}

			var podServices []string
			if _, ok := podServicesMapping[nsKey]; ok {
				if svcs, ok := podServicesMapping[nsKey][c.PodName]; ok {
					podServices = svcs
				} else {
					podServices = []string{}
				}
			}

			var podPVs []*PersistentVolumeClaimData
			var podNetCosts []*Vector

			// For PVC data, we'll need to find the claim mapping and cost data. Will need to append
			// cost data since that was populated by cluster data previously. We do this with
			// the pod_pvc_allocation metric
			podPVData, ok := pvAllocationMapping[podKey]
			if !ok {
				klog.V(4).Infof("Failed to locate pv allocation mapping for missing pod.")
			}

			// For network costs, we'll use existing map since it should still contain the
			// correct data.
			var podNetworkCosts []*Vector
			if usage, ok := networkUsageMap[podKey]; ok {
				netCosts, err := GetNetworkCost(usage, cp)
				if err != nil {
					klog.V(3).Infof("Error pulling network costs: %s", err.Error())
				} else {
					podNetworkCosts = netCosts
				}
			}

			// Check to see if any other data has been recorded for this namespace, pod, clusterId
			// Follow the pattern of only allowing claims data per pod
			if !otherClusterPVRecorded[podKey] {
				otherClusterPVRecorded[podKey] = true

				podPVs = podPVData
				podNetCosts = podNetworkCosts
			}

			costs := &CostData{
				Name:            c.ContainerName,
				PodName:         c.PodName,
				NodeName:        c.NodeName,
				NodeData:        node,
				Namespace:       c.Namespace,
				Services:        podServices,
				Deployments:     podDeployments,
				RAMReq:          RAMReqV,
				RAMUsed:         RAMUsedV,
				CPUReq:          CPUReqV,
				CPUUsed:         CPUUsedV,
				RAMAllocation:   RAMAllocsV,
				CPUAllocation:   CPUAllocsV,
				GPUReq:          GPUReqV,
				Labels:          pLabels,
				NamespaceLabels: namespaceLabels,
				PVCData:         podPVs,
				NetworkData:     podNetCosts,
				ClusterID:       c.ClusterID,
			}

			if costDataPassesFilters(costs, filterNamespace, filterCluster) {
				containerNameCost[key] = costs
				missingContainers[key] = costs
			}
		}
	}

	measureTime(profileStart, fmt.Sprintf("costDataRange(%s): build CostData map", windowString))

	w := end.Sub(start)
	w += window
	if w.Minutes() > 0 {
		wStr := fmt.Sprintf("%dm", int(w.Minutes()))
		err = findDeletedNodeInfo(cli, missingNodes, wStr)
		if err != nil {
			klog.V(1).Infof("Error fetching historical node data: %s", err.Error())
		}
	}

	return containerNameCost, err
}

func applyAllocationToRequests(allocationMap map[string][]*Vector, requestMap map[string][]*Vector) {
	// The result of the normalize operation will be a new []*Vector to replace the requests
	normalizeOp := func(r *Vector, x *float64, y *float64) bool {
		// Omit data (return false) if both x and y inputs don't exist
		if x == nil || y == nil {
			return false
		}

		// If the allocation value is 0, 0 out request value
		if *x == 0 {
			r.Value = 0
		} else {
			r.Value = *y
		}

		return true
	}

	// Run normalization on all request vectors in the mapping
	for k, requests := range requestMap {

		// Only run normalization where there are valid allocations
		allocations, ok := allocationMap[k]
		if !ok {
			delete(requestMap, k)
			continue
		}

		// Replace request map with normalized
		requestMap[k] = ApplyVectorOp(allocations, requests, normalizeOp)
	}
}

func addMetricPVData(pvAllocationMap map[string][]*PersistentVolumeClaimData, pvCostMap map[string]*costAnalyzerCloud.PV, cp costAnalyzerCloud.Provider) {
	cfg, err := cp.GetConfig()
	if err != nil {
		klog.V(1).Infof("Failed to get provider config while adding pv metrics data.")
		return
	}

	for _, pvcDataArray := range pvAllocationMap {
		for _, pvcData := range pvcDataArray {
			costKey := fmt.Sprintf("%s,%s", pvcData.VolumeName, pvcData.ClusterID)

			pvCost, ok := pvCostMap[costKey]
			if !ok {
				pvcData.Volume = &costAnalyzerCloud.PV{
					Cost: cfg.Storage,
				}
				continue
			}

			pvcData.Volume = pvCost
		}
	}
}

// Append labels into nsLabels iff the ns key doesn't already exist
func appendNamespaceLabels(nsLabels map[string]map[string]string, labels map[string]map[string]string) {
	for k, v := range labels {
		if _, ok := nsLabels[k]; !ok {
			nsLabels[k] = v
		}
	}
}

func appendLabelsList(mainLabels map[string]map[string][]string, labels map[string]map[string][]string) {
	for k, v := range labels {
		mainLabels[k] = v
	}
}

func getNamespaceLabels(cache clustercache.ClusterCache, clusterID string) (map[string]map[string]string, error) {
	nsToLabels := make(map[string]map[string]string)
	nss := cache.GetAllNamespaces()
	for _, ns := range nss {
		nsToLabels[ns.Name+","+clusterID] = ns.Labels
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
	result, err := NewQueryResults(qr)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		instance, err := val.GetString("instance")
		if err != nil {
			return toReturn, err
		}

		toReturn[instance] = val.Values
	}

	return toReturn, nil
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
	queryResults, err := NewQueryResults(qr)
	if err != nil {
		return 0, err
	}

	if len(queryResults) > 0 {
		values := queryResults[0].Values

		if len(values) > 0 {
			return values[0].Value, nil
		}
		return 0, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}
	return 0, fmt.Errorf("Normalization data is empty, kube-state-metrics or node-exporter may not be running")
}

//todo: don't cast, implement unmarshaler interface
func getNormalizations(qr interface{}) ([]*Vector, error) {
	queryResults, err := NewQueryResults(qr)
	if err != nil {
		return nil, err
	}

	if len(queryResults) > 0 {
		vectors := []*Vector{}
		for _, value := range queryResults {
			vectors = append(vectors, value.Values...)
		}
		return vectors, nil
	}
	return nil, fmt.Errorf("normalization data is empty: time window may be invalid or kube-state-metrics or node-exporter may not be running")
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

type KeyTuple struct {
	Namespace string
	Key       string
	ClusterID string
}

func NewKeyTuple(key string) (*KeyTuple, error) {
	r := strings.Split(key, ",")
	if len(r) != 3 {
		return nil, fmt.Errorf("NewKeyTuple() Provided key not containing exactly 3 components.")
	}
	return &KeyTuple{
		Namespace: r[0],
		Key:       r[1],
		ClusterID: r[2],
	}, nil
}

func GetContainerMetricVector(qr interface{}, normalize bool, normalizationValue float64, defaultClusterID string) (map[string][]*Vector, error) {
	result, err := NewQueryResults(qr)
	if err != nil {
		return nil, err
	}

	containerData := make(map[string][]*Vector)
	for _, val := range result {
		containerMetric, err := newContainerMetricFromPrometheus(val.Metric, defaultClusterID)
		if err != nil {
			return nil, err
		}

		if normalize && normalizationValue != 0 {
			for _, v := range val.Values {
				v.Value = v.Value / normalizationValue
			}
		}
		containerData[containerMetric.Key()] = val.Values
	}
	return containerData, nil
}

func GetContainerMetricVectors(qr interface{}, normalize bool, normalizationValues []*Vector, defaultClusterID string) (map[string][]*Vector, error) {
	result, err := NewQueryResults(qr)
	if err != nil {
		return nil, err
	}

	containerData := make(map[string][]*Vector)
	for _, val := range result {
		containerMetric, err := newContainerMetricFromPrometheus(val.Metric, defaultClusterID)
		if err != nil {
			return nil, err
		}

		normalizedVectors := NormalizeVectorByVector(val.Values, normalizationValues)
		containerData[containerMetric.Key()] = normalizedVectors
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

func measureTime(start time.Time, name string) {
	elapsed := time.Since(start)

	klog.V(3).Infof("[Profiler] %s: %s", elapsed, name)
}

func measureTimeAsync(start time.Time, name string, ch chan string) {
	ch <- fmt.Sprintf("%s took %s", name, time.Since(start))
}
