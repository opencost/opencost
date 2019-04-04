package costmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	prometheusClient "github.com/prometheus/client_golang/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	statusAPIError = 422

	apiPrefix = "/api/v1"

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
)

type CostData struct {
	Name          string                  `json:"name"`
	PodName       string                  `json:"podName"`
	NodeName      string                  `json:"nodeName"`
	NodeData      *costAnalyzerCloud.Node `json:"node"`
	Namespace     string                  `json:"namespace"`
	Deployments   []string                `json:"deployments"`
	Services      []string                `json:"services"`
	Daemonsets    []string                `json:"daemonsets"`
	Statefulsets  []string                `json:"statefulsets"`
	Jobs          []string                `json:"jobs"`
	RAMReq        []*Vector               `json:"ramreq"`
	RAMUsed       []*Vector               `json:"ramused"`
	CPUReq        []*Vector               `json:"cpureq"`
	CPUUsed       []*Vector               `json:"cpuused"`
	RAMAllocation []*Vector               `json:"ramallocated"`
	CPUAllocation []*Vector               `json:"cpuallocated"`
	GPUReq        []*Vector               `json:"gpureq"`
	PVData        []*PersistentVolumeData `json:"pvData"`
	Labels        map[string]string       `json:"labels"`
}

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

func ComputeCostData(cli prometheusClient.Client, clientset *kubernetes.Clientset, cloud costAnalyzerCloud.Provider, window string) (map[string]*CostData, error) {
	queryRAMRequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD"}[` + window + `]) *  avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD"}[` + window + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryRAMUsage := `sort_desc(avg(count_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD"}[` + window + `]) * avg_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD"}[` + window + `])) by (namespace,container_name,pod_name,instance))`
	queryCPURequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD"}[` + window + `]) *  avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD"}[` + window + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryCPUUsage := `avg(rate(container_cpu_usage_seconds_total{container_name!="",container_name!="POD"}[` + window + `])) by (namespace,container_name,pod_name,instance)`
	queryGPURequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD"}[` + window + `]) *  avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD"}[` + window + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryPVRequests := `(sum(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass) + on (persistentvolumeclaim) group_right(storageclass) sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace))`
	normalization := `max(count_over_time(kube_pod_container_resource_requests_memory_bytes{}[` + window + `]))`
	resultRAMRequests, _ := query(cli, queryRAMRequests)
	resultRAMUsage, _ := query(cli, queryRAMUsage)
	resultCPURequests, _ := query(cli, queryCPURequests)
	resultCPUUsage, _ := query(cli, queryCPUUsage)
	resultGPURequests, _ := query(cli, queryGPURequests)
	resultPVRequests, _ := query(cli, queryPVRequests)
	normalizationResult, _ := query(cli, normalization)

	normalizationValue := getNormalization(normalizationResult)

	nodes, err := getNodeCost(clientset, cloud)
	if err != nil {
		log.Printf("Warning, no cost model available: " + err.Error())
	}

	podlist, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	podDeploymentsMapping, err := getPodDeployments(clientset, podlist)
	if err != nil {
		return nil, err
	}

	podServicesMapping, err := getPodServices(clientset, podlist)
	if err != nil {
		return nil, err
	}

	pvClaimMapping := getPVInfoVector(resultPVRequests)
	if err != nil {
		return nil, err
	}

	containerNameCost := make(map[string]*CostData)
	for _, pod := range podlist.Items {
		podName := pod.GetObjectMeta().GetName()
		ns := pod.GetObjectMeta().GetNamespace()
		labels := pod.GetObjectMeta().GetLabels()
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

		var podPVs []*PersistentVolumeData
		podClaims := pod.Spec.Volumes
		for _, vol := range podClaims {
			if vol.PersistentVolumeClaim != nil {
				name := vol.PersistentVolumeClaim.ClaimName
				if pvClaim, ok := pvClaimMapping[ns+","+name]; ok {
					podPVs = append(podPVs, pvClaim)
				}
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

			RAMReqV := findContainerMetric(resultRAMRequests, containerName, podName, ns)
			RAMReqV.Value = RAMReqV.Value / normalizationValue
			RAMUsedV := findContainerMetric(resultRAMUsage, containerName, podName, ns)
			RAMUsedV.Value = RAMUsedV.Value / normalizationValue
			CPUReqV := findContainerMetric(resultCPURequests, containerName, podName, ns)
			CPUReqV.Value = CPUReqV.Value / normalizationValue
			GPUReqV := findContainerMetric(resultGPURequests, containerName, podName, ns)
			GPUReqV.Value = GPUReqV.Value / normalizationValue

			var pvReq []*PersistentVolumeData
			if i == 0 { // avoid duplicating by just assigning all claims to the first container.
				pvReq = podPVs
			}

			costs := &CostData{
				Name:         containerName,
				PodName:      podName,
				NodeName:     nodeName,
				Namespace:    ns,
				Deployments:  podDeployments,
				Services:     podServices,
				Daemonsets:   getDaemonsetsOfPod(pod),
				Jobs:         getJobsOfPod(pod),
				Statefulsets: getStatefulSetsOfPod(pod),
				NodeData:     nodeData,
				RAMReq:       []*Vector{RAMReqV},
				RAMUsed:      []*Vector{RAMUsedV},
				CPUReq:       []*Vector{CPUReqV},
				CPUUsed:      []*Vector{findContainerMetric(resultCPUUsage, containerName, podName, ns)},
				GPUReq:       []*Vector{GPUReqV},
				PVData:       pvReq,
				Labels:       labels,
			}
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
			containerNameCost[ns+","+podName+","+containerName] = costs
		}
	}
	return containerNameCost, err
}

func getContainerAllocation(req []*Vector, used []*Vector) []*Vector {
	if req == nil || len(req) == 0 {
		return used
	}
	if used == nil || len(used) == 0 {
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
		usedV.Timestamp = math.Round(usedV.Timestamp/10) * 10
		usedMap[usedV.Timestamp] = usedV.Value
		timestamps = append(timestamps, usedV.Timestamp)
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

func getNodeCost(clientset *kubernetes.Clientset, cloud costAnalyzerCloud.Provider) (map[string]*costAnalyzerCloud.Node, error) {
	nodeList, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]*costAnalyzerCloud.Node)
	for _, n := range nodeList.Items {
		name := n.GetObjectMeta().GetName()
		labels := n.GetObjectMeta().GetLabels()
		cnode, err := cloud.NodePricing(cloud.GetKey(labels))
		if err != nil {
			log.Printf("Error getting node. Error: " + err.Error())
		}

		var cpu float64
		if cnode.VCPU == "" {
			cpu = float64(n.Status.Capacity.Cpu().Value())
			cnode.VCPU = n.Status.Capacity.Cpu().String()
		} else {
			cpu, _ = strconv.ParseFloat(cnode.VCPU, 64)
		}
		var ram float64
		if cnode.RAM == "" {
			cnode.RAM = n.Status.Capacity.Memory().String()
		}
		ram = float64(n.Status.Capacity.Memory().Value())
		if cnode.RAMCost == "" { // We couldn't find a ramcost, so fix cpu and allocate ram accordingly
			basePrice, _ := strconv.ParseFloat(cnode.BaseCPUPrice, 64)
			totalCPUPrice := basePrice * cpu
			var nodePrice float64
			if cnode.Cost != "" {
				log.Printf("Use given nodeprice as whole node price")
				nodePrice, _ = strconv.ParseFloat(cnode.Cost, 64)
			} else {
				log.Printf("Use cpuprice as whole node price")
				nodePrice, _ = strconv.ParseFloat(cnode.VCPUCost, 64) // all the price was allocated the the CPU
			}
			if totalCPUPrice >= nodePrice {
				totalCPUPrice = 0.9 * nodePrice // just allocate RAM costs to 10% of the node price here to avoid 0 or negative in the numerator
			}
			ramPrice := (nodePrice - totalCPUPrice) / (ram / 1024 / 1024 / 1024)
			cpuPrice := totalCPUPrice / cpu

			cnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
			cnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			log.Printf(cnode.RAMCost)
		}
		nodes[name] = cnode
	}
	return nodes, nil
}

func getPodServices(clientset *kubernetes.Clientset, podList *v1.PodList) (map[string]map[string][]string, error) {
	//servicesList, err := clientset.Core().Services("").List(metav1.ListOptions{})
	servicesList, err := clientset.CoreV1().Services("").List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	podServicesMapping := make(map[string]map[string][]string)
	for _, service := range servicesList.Items {
		namespace := service.GetObjectMeta().GetNamespace()
		name := service.GetObjectMeta().GetName()

		if _, ok := podServicesMapping[namespace]; !ok {
			podServicesMapping[namespace] = make(map[string][]string)
		}
		s := labels.Set(service.Spec.Selector).AsSelectorPreValidated()
		if err != nil {
			log.Printf("Error doing service label conversion: " + err.Error())
		}
		for _, pod := range podList.Items {
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

func getPodDeployments(clientset *kubernetes.Clientset, podList *v1.PodList) (map[string]map[string][]string, error) {
	deploymentsList, err := clientset.AppsV1().Deployments("").List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	podDeploymentsMapping := make(map[string]map[string][]string) // namespace: podName: [deploymentNames]
	for _, deployment := range deploymentsList.Items {
		namespace := deployment.GetObjectMeta().GetNamespace()
		name := deployment.GetObjectMeta().GetName()
		if _, ok := podDeploymentsMapping[namespace]; !ok {
			podDeploymentsMapping[namespace] = make(map[string][]string)
		}
		s, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			log.Printf("Error doing deployment label conversion: " + err.Error())
		}
		for _, pod := range podList.Items {
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

func ComputeCostDataRange(cli prometheusClient.Client, clientset *kubernetes.Clientset, cloud costAnalyzerCloud.Provider,
	startString, endString, windowString string) (map[string]*CostData, error) {
	queryRAMRequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD"}[` + windowString + `]) *  avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD"}[` + windowString + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryRAMUsage := `sort_desc(avg(count_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD"}[` + windowString + `]) * avg_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD"}[` + windowString + `])) by (namespace,container_name,pod_name,instance))`
	queryCPURequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD"}[` + windowString + `]) *  avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD"}[` + windowString + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryCPUUsage := `avg(rate(container_cpu_usage_seconds_total{container_name!="",container_name!="POD"}[` + windowString + `])) by (namespace,container_name,pod_name,instance)`
	queryGPURequests := `avg(label_replace(label_replace(avg((count_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD"}[` + windowString + `]) *  avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD"}[` + windowString + `]))) by (namespace,container,pod) , "container_name","$1","container","(.+)"), "pod_name","$1","pod","(.+)") ) by (namespace,container_name, pod_name)`
	queryPVRequests := `(sum(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass) + on (persistentvolumeclaim) group_right(storageclass) sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace))`
	normalization := `max(count_over_time(kube_pod_container_resource_requests_memory_bytes{}[` + windowString + `]))`

	layout := "2006-01-02T15:04:05.000Z"

	start, err := time.Parse(layout, startString)
	if err != nil {
		log.Printf("Error parsing time " + startString + ". Error: " + err.Error())
		return nil, err
	}
	end, err := time.Parse(layout, endString)
	if err != nil {
		log.Printf("Error parsing time " + endString + ". Error: " + err.Error())
		return nil, err
	}
	window, err := time.ParseDuration(windowString)
	if err != nil {
		log.Printf("Error parsing time " + windowString + ". Error: " + err.Error())
		return nil, err
	}
	resultRAMRequests, _ := queryRange(cli, queryRAMRequests, start, end, window)
	resultRAMUsage, _ := queryRange(cli, queryRAMUsage, start, end, window)
	resultCPURequests, _ := queryRange(cli, queryCPURequests, start, end, window)
	resultCPUUsage, _ := queryRange(cli, queryCPUUsage, start, end, window)
	resultGPURequests, _ := queryRange(cli, queryGPURequests, start, end, window)
	resultPVRequests, _ := queryRange(cli, queryPVRequests, start, end, window)

	normalizationResult, _ := query(cli, normalization)

	normalizationValue := getNormalization(normalizationResult)

	nodes, err := getNodeCost(clientset, cloud)
	if err != nil {
		//return nil, err
		log.Printf("Warning, no cost model available: " + err.Error())
	}

	podlist, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	podDeploymentsMapping, err := getPodDeployments(clientset, podlist)
	if err != nil {
		return nil, err
	}
	podServicesMapping, err := getPodServices(clientset, podlist)
	if err != nil {
		return nil, err
	}

	pvClaimMapping := getPVInfoVectors(resultPVRequests)
	if err != nil {
		return nil, err
	}

	containerNameCost := make(map[string]*CostData)

	for _, pod := range podlist.Items {
		podName := pod.GetObjectMeta().GetName()
		ns := pod.GetObjectMeta().GetNamespace()
		labels := pod.GetObjectMeta().GetLabels()
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
		var podServices []string
		if _, ok := podServicesMapping[ns]; ok {
			if svcs, ok := podServicesMapping[ns][pod.GetObjectMeta().GetName()]; ok {
				podServices = svcs
			} else {
				podServices = []string{}
			}
		}

		var podPVs []*PersistentVolumeData
		podClaims := pod.Spec.Volumes
		for _, vol := range podClaims {
			if vol.PersistentVolumeClaim != nil {
				name := vol.PersistentVolumeClaim.ClaimName
				if pvClaim, ok := pvClaimMapping[ns+","+name]; ok {
					podPVs = append(podPVs, pvClaim)
				}
			}
		}

		for i, container := range pod.Spec.Containers {
			containerName := container.Name

			RAMReqV := findContainerMetricVectors(resultRAMRequests, containerName, podName, ns)
			for _, v := range RAMReqV {
				v.Value = v.Value / normalizationValue
			}

			RAMUsedV := findContainerMetricVectors(resultRAMUsage, containerName, podName, ns)
			for _, v := range RAMUsedV {
				v.Value = v.Value / normalizationValue
			}

			CPUReqV := findContainerMetricVectors(resultCPURequests, containerName, podName, ns)
			for _, v := range CPUReqV {
				v.Value = v.Value / normalizationValue
			}

			GPUReqV := findContainerMetricVectors(resultGPURequests, containerName, podName, ns)
			for _, v := range GPUReqV {
				v.Value = v.Value / normalizationValue
			}

			var pvReq []*PersistentVolumeData
			if i == 0 { // avoid duplicating by just assigning all claims to the first container.
				pvReq = podPVs
			}

			costs := &CostData{
				Name:         containerName,
				PodName:      podName,
				NodeName:     nodeName,
				NodeData:     nodeData,
				Namespace:    ns,
				Deployments:  podDeployments,
				Services:     podServices,
				Daemonsets:   getDaemonsetsOfPod(pod),
				Jobs:         getJobsOfPod(pod),
				Statefulsets: getStatefulSetsOfPod(pod),
				RAMReq:       RAMReqV,
				RAMUsed:      RAMUsedV,
				CPUReq:       CPUReqV,
				CPUUsed:      findContainerMetricVectors(resultCPUUsage, containerName, podName, ns),
				GPUReq:       GPUReqV,
				PVData:       pvReq,
				Labels:       labels,
			}
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			containerNameCost[ns+","+podName+","+containerName] = costs
		}
	}
	return containerNameCost, err

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

type PersistentVolumeData struct {
	Class     string    `json:"class"`
	Claim     string    `json:"claim"`
	Namespace string    `json:"namespace"`
	Values    []*Vector `json:"values"`
}

func getPVInfoVectors(qr interface{}) map[string]*PersistentVolumeData {
	pvmap := make(map[string]*PersistentVolumeData)
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		pvclaim := val.(map[string]interface{})["metric"].(map[string]interface{})["persistentvolumeclaim"]
		pvclass := val.(map[string]interface{})["metric"].(map[string]interface{})["storageclass"]
		pvnamespace := val.(map[string]interface{})["metric"].(map[string]interface{})["namespace"]
		values := val.(map[string]interface{})["values"].([]interface{})
		var vectors []*Vector
		for _, value := range values {
			strVal := value.([]interface{})[1].(string)
			v, _ := strconv.ParseFloat(strVal, 64)
			vectors = append(vectors, &Vector{
				Timestamp: value.([]interface{})[0].(float64),
				Value:     v,
			})
		}
		key := pvnamespace.(string) + "," + pvclaim.(string)
		pvmap[key] = &PersistentVolumeData{
			Class:     pvclass.(string),
			Claim:     pvclaim.(string),
			Namespace: pvnamespace.(string),
			Values:    vectors,
		}
	}
	return pvmap
}

func getPVInfoVector(qr interface{}) map[string]*PersistentVolumeData {
	pvmap := make(map[string]*PersistentVolumeData)
	log.Printf("Interface %v. If the interface is nil, prometheus is not running!", qr)
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		pvclaim := val.(map[string]interface{})["metric"].(map[string]interface{})["persistentvolumeclaim"]
		pvclass := val.(map[string]interface{})["metric"].(map[string]interface{})["storageclass"]
		pvnamespace := val.(map[string]interface{})["metric"].(map[string]interface{})["namespace"]
		value := val.(map[string]interface{})["value"].([]interface{})
		var vectors []*Vector
		strVal := value[1].(string)
		v, _ := strconv.ParseFloat(strVal, 64)

		vectors = append(vectors, &Vector{
			Timestamp: value[0].(float64),
			Value:     v,
		})

		key := pvclaim.(string) + "," + pvnamespace.(string)
		pvmap[key] = &PersistentVolumeData{
			Class:     pvclass.(string),
			Claim:     pvclaim.(string),
			Namespace: pvnamespace.(string),
			Values:    vectors,
		}
	}
	return pvmap
}

func queryRange(cli prometheusClient.Client, query string, start, end time.Time, step time.Duration) (interface{}, error) {
	u := cli.URL(epQueryRange, nil)
	q := u.Query()
	q.Set("query", query)
	q.Set("start", start.Format(time.RFC3339Nano))
	q.Set("end", end.Format(time.RFC3339Nano))
	q.Set("step", strconv.FormatFloat(step.Seconds(), 'f', 3, 64))
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	_, body, err := cli.Do(context.Background(), req)
	if err != nil {
		log.Print("ERROR" + err.Error())
	}
	if err != nil {
		return nil, err
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		log.Print("ERROR" + err.Error())
	}
	return toReturn, err
}

func query(cli prometheusClient.Client, query string) (interface{}, error) {
	u := cli.URL(epQuery, nil)
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	_, body, err := cli.Do(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		log.Print("ERROR" + err.Error())
	}
	return toReturn, err
}

//todo: don't cast, implement unmarshaler interface
func getNormalization(qr interface{}) float64 {
	strNorm := qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[1].(string)
	val, _ := strconv.ParseFloat(strNorm, 64)
	return val
}

//todo: don't cast, implement unmarshaler interface...
func findContainerMetric(qr interface{}, cname string, podname string, namespace string) *Vector {
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		if val.(map[string]interface{})["metric"].(map[string]interface{})["container_name"] == cname &&
			val.(map[string]interface{})["metric"].(map[string]interface{})["pod_name"] == podname &&
			val.(map[string]interface{})["metric"].(map[string]interface{})["namespace"] == namespace {

			strVal := val.(map[string]interface{})["value"].([]interface{})[1].(string)
			value, _ := strconv.ParseFloat(strVal, 64)

			toReturn := &Vector{
				Timestamp: val.(map[string]interface{})["value"].([]interface{})[0].(float64),
				Value:     value,
			}
			return toReturn

		}
	}
	return &Vector{}
}

func findContainerMetricVectors(qr interface{}, cname string, podname string, namespace string) []*Vector {
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		if val.(map[string]interface{})["metric"].(map[string]interface{})["container_name"] == cname &&
			val.(map[string]interface{})["metric"].(map[string]interface{})["pod_name"] == podname &&
			val.(map[string]interface{})["metric"].(map[string]interface{})["namespace"] == namespace {
			values := val.(map[string]interface{})["values"].([]interface{})
			var vectors []*Vector
			for _, value := range values {
				strVal := value.([]interface{})[1].(string)
				v, _ := strconv.ParseFloat(strVal, 64)
				vectors = append(vectors, &Vector{
					Timestamp: value.([]interface{})[0].(float64),
					Value:     v,
				})
			}
			return vectors
		}
	}
	return []*Vector{}
}
