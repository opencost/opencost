package costmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
	Name            string                  `json:"name"`
	PodName         string                  `json:"podName"`
	NodeName        string                  `json:"nodeName"`
	NodeData        *costAnalyzerCloud.Node `json:"node"`
	Namespace       string                  `json:"namespace"`
	Deployments     []string                `json:"deployments"`
	Services        []string                `json:"services"`
	Daemonsets      []string                `json:"daemonsets"`
	Statefulsets    []string                `json:"statefulsets"`
	Jobs            []string                `json:"jobs"`
	RAMReq          []*Vector               `json:"ramreq"`
	RAMUsed         []*Vector               `json:"ramused"`
	CPUReq          []*Vector               `json:"cpureq"`
	CPUUsed         []*Vector               `json:"cpuused"`
	RAMAllocation   []*Vector               `json:"ramallocated"`
	CPUAllocation   []*Vector               `json:"cpuallocated"`
	GPUReq          []*Vector               `json:"gpureq"`
	PVData          []*PersistentVolumeData `json:"pvData"`
	Labels          map[string]string       `json:"labels"`
	NamespaceLabels map[string]string       `json:"namespaceLabels"`
}

type Vector struct {
	Timestamp float64 `json:"timestamp"`
	Value     float64 `json:"value"`
}

func ComputeCostData(cli prometheusClient.Client, clientset kubernetes.Interface, cloud costAnalyzerCloud.Provider, window string) (map[string]*CostData, error) {
	queryRAMRequests := `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[` + window + `]) 
					*  
					avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[` + window + `])
				) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		)
	) by (namespace,container_name,pod_name,node)`
	queryRAMUsage := `sort_desc(
		avg(
			label_replace(count_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD", instance!=""}[` + window + `]), "node", "$1", "instance","(.+)") 
			* 
			label_replace(avg_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD", instance!=""}[` + window + `]), "node", "$1", "instance","(.+)") 
		) by (namespace,container_name,pod_name,node)
	)`
	queryCPURequests := `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[` + window + `]) 
					*  
					avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[` + window + `])
				) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node)`
	queryCPUUsage := `avg(
		label_replace(
		  rate( 
			container_cpu_usage_seconds_total{container_name!="",container_name!="POD",instance!=""}[` + window + `]
		  ) , "node", "$1", "instance", "(.+)"
		)
	) by (namespace,container_name,pod_name,node)`
	queryGPURequests := `avg(
		label_replace(
			label_replace(
				avg(
					count_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[` + window + `]) 
					*  
					avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[` + window + `])
				) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
			), "pod_name","$1","pod","(.+)"
		) 
	) by (namespace,container_name,pod_name,node)`
	queryPVRequests := `avg(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass, namespace) 
	                    * 
	                    on (persistentvolumeclaim, namespace) group_right(storageclass) 
			    sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace)`
	normalization := `max(count_over_time(kube_pod_container_resource_requests_memory_bytes{}[` + window + `]))`
	resultRAMRequests, err := query(cli, queryRAMRequests)
	if err != nil {
		return nil, fmt.Errorf("Error fetching RAM requests: " + err.Error())
	}
	resultRAMUsage, err := query(cli, queryRAMUsage)
	if err != nil {
		return nil, fmt.Errorf("Error fetching RAM usage: " + err.Error())
	}
	resultCPURequests, err := query(cli, queryCPURequests)
	if err != nil {
		return nil, fmt.Errorf("Error fetching CPU requests: " + err.Error())
	}
	resultCPUUsage, err := query(cli, queryCPUUsage)
	if err != nil {
		return nil, fmt.Errorf("Error fetching CPUUsage requests: " + err.Error())
	}
	resultGPURequests, err := query(cli, queryGPURequests)
	if err != nil {
		return nil, fmt.Errorf("Error fetching GPU requests: " + err.Error())
	}
	resultPVRequests, err := query(cli, queryPVRequests)
	if err != nil {
		return nil, fmt.Errorf("Error fetching PV requests: " + err.Error())
	}
	normalizationResult, err := query(cli, normalization)
	if err != nil {
		return nil, fmt.Errorf("Error fetching normalization data: " + err.Error())
	}

	normalizationValue, err := getNormalization(normalizationResult)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodeCost(clientset, cloud)
	if err != nil {
		klog.V(1).Infof("Warning, no Node cost model available: " + err.Error())
		return nil, err
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
	namespaceLabelsMapping, err := getNamespaceLabels(clientset)
	if err != nil {
		return nil, err
	}

	pvClaimMapping, err := getPVInfoVector(resultPVRequests)
	if err != nil {
		return nil, err
	}

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)

	RAMReqMap, err := getContainerMetricVector(resultRAMRequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range RAMReqMap {
		containers[key] = true
	}

	RAMUsedMap, err := getContainerMetricVector(resultRAMUsage, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range RAMUsedMap {
		containers[key] = true
	}
	CPUReqMap, err := getContainerMetricVector(resultCPURequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range CPUReqMap {
		containers[key] = true
	}
	GPUReqMap, err := getContainerMetricVector(resultGPURequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range GPUReqMap {
		containers[key] = true
	}
	CPUUsedMap, err := getContainerMetricVector(resultCPUUsage, false, 0) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}
	currentContainers := make(map[string]v1.Pod)
	for _, pod := range podlist.Items {
		cs, err := newContainerMetricsFromPod(pod)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			containers[c.Key()] = true // captures any containers that existed for a time < a prometheus scrape interval. We currently charge 0 for this but should charge something.
			currentContainers[c.Key()] = pod
		}
	}
	for key := range containers {
		if _, ok := containerNameCost[key]; ok {
			continue // because ordering is important for the allocation model (all PV's applied to the first), just dedupe if it's already been added.
		}
		if pod, ok := currentContainers[key]; ok {
			podName := pod.GetObjectMeta().GetName()
			ns := pod.GetObjectMeta().GetNamespace()

			nsLabels := namespaceLabelsMapping[ns]

			podLabels := pod.GetObjectMeta().GetLabels()
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

				// recreate the key and look up data for this container
				newKey := newContainerMetricFromValues(ns, podName, containerName, pod.Spec.NodeName).Key()

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

				var pvReq []*PersistentVolumeData
				if i == 0 { // avoid duplicating by just assigning all claims to the first container.
					pvReq = podPVs
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
					PVData:          pvReq,
					Labels:          podLabels,
					NamespaceLabels: nsLabels,
				}
				costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
				costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
				containerNameCost[newKey] = costs
			}

		} else {
			// The container has been deleted. Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
			klog.V(4).Info("The container " + key + " has been deleted. Calculating allocation but resulting object will be missing data.")
			c, err := newContainerMetricFromKey(key)
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

			var node *costAnalyzerCloud.Node
			if n, ok := nodes[c.NodeName]; !ok {
				//TODO: The node has been deleted from kubernetes as well. You will need to query historical node data to get it.
				klog.V(2).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
			} else {
				node = n
			}
			costs := &CostData{
				Name:      c.ContainerName,
				PodName:   c.PodName,
				NodeName:  c.NodeName,
				NodeData:  node,
				Namespace: c.Namespace,
				RAMReq:    RAMReqV,
				RAMUsed:   RAMUsedV,
				CPUReq:    CPUReqV,
				CPUUsed:   CPUUsedV,
				GPUReq:    GPUReqV,
			}
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
			containerNameCost[key] = costs
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

func getNodeCost(clientset kubernetes.Interface, cloud costAnalyzerCloud.Provider) (map[string]*costAnalyzerCloud.Node, error) {
	nodeList, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]*costAnalyzerCloud.Node)
	for _, n := range nodeList.Items {
		name := n.GetObjectMeta().GetName()
		nodeLabels := n.GetObjectMeta().GetLabels()
		nodeLabels["providerID"] = n.Spec.ProviderID
		cnode, err := cloud.NodePricing(cloud.GetKey(nodeLabels))
		if err != nil {
			klog.V(1).Infof("Error getting node. Error: " + err.Error())
			continue
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

		if cnode.GPU != "" && cnode.GPUCost == "" { // We couldn't find a gpu cost, so fix cpu and ram, then accordingly
			klog.V(3).Infof("GPU without cost found, calculating...")
			basePrice, err := strconv.ParseFloat(cnode.BaseCPUPrice, 64)
			if err != nil {
				return nil, err
			}
			nodePrice, err := strconv.ParseFloat(cnode.Cost, 64)
			if err != nil {
				return nil, err
			}
			totalCPUPrice := basePrice * cpu
			totalRAMPrice := 0.1 * totalCPUPrice
			ramPrice := totalRAMPrice / (ram / 1024 / 1024 / 1024)
			gpuPrice := nodePrice - totalCPUPrice - totalRAMPrice
			cnode.VCPUCost = fmt.Sprintf("%f", basePrice)
			cnode.RAMCost = fmt.Sprintf("%f", ramPrice)
			cnode.RAMBytes = fmt.Sprintf("%f", ram)
			cnode.GPUCost = fmt.Sprintf("%f", gpuPrice)
			klog.V(2).Infof("Computed \"%s\" GPU Cost := %v", name, cnode.GPUCost)
		} else {
			if cnode.RAMCost == "" { // We couldn't find a ramcost, so fix cpu and allocate ram accordingly
				basePrice, err := strconv.ParseFloat(cnode.BaseCPUPrice, 64)
				if err != nil {
					return nil, err
				}
				totalCPUPrice := basePrice * cpu
				var nodePrice float64
				if cnode.Cost != "" {
					klog.V(3).Infof("Use given nodeprice as whole node price")
					nodePrice, err = strconv.ParseFloat(cnode.Cost, 64)
					if err != nil {
						return nil, err
					}
				} else {
					klog.V(3).Infof("Use cpuprice as whole node price")
					nodePrice, err = strconv.ParseFloat(cnode.VCPUCost, 64) // all the price was allocated the the CPU
					if err != nil {
						return nil, err
					}
				}
				if totalCPUPrice >= nodePrice {
					totalCPUPrice = 0.9 * nodePrice // just allocate RAM costs to 10% of the node price here to avoid 0 or negative in the numerator
				}
				ramPrice := (nodePrice - totalCPUPrice) / (ram / 1024 / 1024 / 1024)
				cpuPrice := totalCPUPrice / cpu

				cnode.VCPUCost = fmt.Sprintf("%f", cpuPrice)
				cnode.RAMCost = fmt.Sprintf("%f", ramPrice)
				cnode.RAMBytes = fmt.Sprintf("%f", ram)
				klog.V(3).Infof("Computed \"%s\" RAM Cost := %v", name, cnode.RAMCost)
			}
		}

		nodes[name] = cnode
	}
	return nodes, nil
}

func getPodServices(clientset kubernetes.Interface, podList *v1.PodList) (map[string]map[string][]string, error) {
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

func getPodDeployments(clientset kubernetes.Interface, podList *v1.PodList) (map[string]map[string][]string, error) {
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
			klog.V(2).Infof("Error doing deployment label conversion: " + err.Error())
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

func ComputeCostDataRange(cli prometheusClient.Client, clientset kubernetes.Interface, cloud costAnalyzerCloud.Provider,
	startString, endString, windowString string) (map[string]*CostData, error) {
	queryRAMRequests := `avg(
			label_replace(
				label_replace(
					avg(
						count_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[` + windowString + `]) 
						*  
						avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="",container!="POD", node!=""}[` + windowString + `])
					) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
				), "pod_name","$1","pod","(.+)"
			)
		) by (namespace,container_name,pod_name,node)`
	queryRAMUsage := `sort_desc(
		avg(
			label_replace(count_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD", instance!=""}[` + windowString + `]), "node", "$1", "instance","(.+)") 
			* 
			label_replace(avg_over_time(container_memory_usage_bytes{container_name!="",container_name!="POD", instance!=""}[` + windowString + `]), "node", "$1", "instance","(.+)") 
		) by (namespace,container_name,pod_name,node)
	)`
	queryCPURequests := `avg(
			label_replace(
				label_replace(
					avg(
						count_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[` + windowString + `]) 
						*  
						avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="",container!="POD", node!=""}[` + windowString + `])
					) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
				), "pod_name","$1","pod","(.+)"
			) 
		) by (namespace,container_name,pod_name,node)`
	queryCPUUsage := `avg(
			label_replace(
			  rate( 
				container_cpu_usage_seconds_total{container_name!="",container_name!="POD", instance!=""}[` + windowString + `]
			  ) , "node", "$1", "instance", "(.+)"
			)
		) by (namespace,container_name,pod_name,node)`
	queryGPURequests := `avg(
			label_replace(
				label_replace(
					avg(
						count_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[` + windowString + `]) 
						*  
						avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[` + windowString + `])
					) by (namespace,container,pod,node) , "container_name","$1","container","(.+)"
				), "pod_name","$1","pod","(.+)"
			) 
		) by (namespace,container_name,pod_name,node)`
	queryPVRequests := `avg(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass, namespace) 
							* 
							on (persistentvolumeclaim, namespace) group_right(storageclass) 
					sum(kube_persistentvolumeclaim_resource_requests_storage_bytes) by (persistentvolumeclaim, namespace)`
	normalization := `max(count_over_time(kube_pod_container_resource_requests_memory_bytes{}[` + windowString + `]))`

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
	resultRAMRequests, err := queryRange(cli, queryRAMRequests, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching RAM requests: " + err.Error())
	}
	resultRAMUsage, err := queryRange(cli, queryRAMUsage, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching RAM usage: " + err.Error())
	}
	resultCPURequests, err := queryRange(cli, queryCPURequests, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching CPU requests: " + err.Error())
	}
	resultCPUUsage, err := queryRange(cli, queryCPUUsage, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching CPU usage: " + err.Error())
	}
	resultGPURequests, err := queryRange(cli, queryGPURequests, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching GPU requests: " + err.Error())
	}
	resultPVRequests, err := queryRange(cli, queryPVRequests, start, end, window)
	if err != nil {
		return nil, fmt.Errorf("Error fetching PV requests: " + err.Error())
	}
	normalizationResult, err := query(cli, normalization)
	if err != nil {
		return nil, fmt.Errorf("Error fetching normalization data: " + err.Error())
	}

	normalizationValue, err := getNormalization(normalizationResult)
	if err != nil {
		return nil, err
	}

	nodes, err := getNodeCost(clientset, cloud)
	if err != nil {
		klog.V(1).Infof("Warning, no cost model available: " + err.Error())
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
	namespaceLabelsMapping, err := getNamespaceLabels(clientset)
	if err != nil {
		return nil, err
	}
	pvClaimMapping, err := getPVInfoVectors(resultPVRequests)
	if err != nil {
		return nil, err
	}

	containerNameCost := make(map[string]*CostData)
	containers := make(map[string]bool)

	RAMReqMap, err := getContainerMetricVectors(resultRAMRequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range RAMReqMap {
		containers[key] = true
	}

	RAMUsedMap, err := getContainerMetricVectors(resultRAMUsage, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range RAMUsedMap {
		containers[key] = true
	}
	CPUReqMap, err := getContainerMetricVectors(resultCPURequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range CPUReqMap {
		containers[key] = true
	}
	GPUReqMap, err := getContainerMetricVectors(resultGPURequests, true, normalizationValue)
	if err != nil {
		return nil, err
	}
	for key := range GPUReqMap {
		containers[key] = true
	}
	CPUUsedMap, err := getContainerMetricVectors(resultCPUUsage, false, 0) // No need to normalize here, as this comes from a counter
	if err != nil {
		return nil, err
	}
	for key := range CPUUsedMap {
		containers[key] = true
	}
	currentContainers := make(map[string]v1.Pod)
	for _, pod := range podlist.Items {
		cs, err := newContainerMetricsFromPod(pod)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			containers[c.Key()] = true // captures any containers that existed for a time < a prometheus scrape interval. We currently charge 0 for this but should charge something.
			currentContainers[c.Key()] = pod
		}
	}

	for key := range containers {
		if _, ok := containerNameCost[key]; ok {
			continue // because ordering is important for the allocation model (all PV's applied to the first), just dedupe if it's already been added.
		}
		if pod, ok := currentContainers[key]; ok {
			podName := pod.GetObjectMeta().GetName()
			ns := pod.GetObjectMeta().GetNamespace()
			podLabels := pod.GetObjectMeta().GetLabels()
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

			nsLabels := namespaceLabelsMapping[ns]

			for i, container := range pod.Spec.Containers {
				containerName := container.Name

				newKey := newContainerMetricFromValues(ns, podName, containerName, pod.Spec.NodeName).Key()

				RAMReqV, ok := RAMReqMap[newKey]
				if !ok {
					klog.V(2).Info("no RAM requests for " + newKey)
					RAMReqV = []*Vector{}
				}
				RAMUsedV, ok := RAMUsedMap[newKey]
				if !ok {
					klog.V(2).Info("no RAM usage for " + newKey)
					RAMUsedV = []*Vector{}
				}
				CPUReqV, ok := CPUReqMap[newKey]
				if !ok {
					klog.V(2).Info("no CPU requests for " + newKey)
					CPUReqV = []*Vector{}
				}
				GPUReqV, ok := GPUReqMap[newKey]
				if !ok {
					klog.V(2).Info("no GPU requests for " + newKey)
					GPUReqV = []*Vector{}
				}
				CPUUsedV, ok := CPUUsedMap[newKey]
				if !ok {
					klog.V(2).Info("no CPU usage for " + newKey)
					CPUUsedV = []*Vector{}
				}

				var pvReq []*PersistentVolumeData
				if i == 0 { // avoid duplicating by just assigning all claims to the first container.
					pvReq = podPVs
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
					PVData:          pvReq,
					Labels:          podLabels,
					NamespaceLabels: nsLabels,
				}
				costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
				costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
				containerNameCost[newKey] = costs
			}

		} else {
			// The container has been deleted. Not all information is sent to prometheus via ksm, so fill out what we can without k8s api
			klog.V(4).Info("The container " + key + " has been deleted. Calculating allocation but resulting object will be missing data.")
			c, _ := newContainerMetricFromKey(key)
			RAMReqV, ok := RAMReqMap[key]
			if !ok {
				klog.V(2).Info("no RAM requests for " + key)
				RAMReqV = []*Vector{}
			}
			RAMUsedV, ok := RAMUsedMap[key]
			if !ok {
				klog.V(2).Info("no RAM usage for " + key)
				RAMUsedV = []*Vector{}
			}
			CPUReqV, ok := CPUReqMap[key]
			if !ok {
				klog.V(2).Info("no CPU requests for " + key)
				CPUReqV = []*Vector{}
			}
			GPUReqV, ok := GPUReqMap[key]
			if !ok {
				klog.V(2).Info("no GPU requests for " + key)
				GPUReqV = []*Vector{}
			}
			CPUUsedV, ok := CPUUsedMap[key]
			if !ok {
				klog.V(2).Info("no CPU usage for " + key)
				CPUUsedV = []*Vector{}
			}
			var node *costAnalyzerCloud.Node
			if n, ok := nodes[c.NodeName]; !ok {
				//TODO: The node has been deleted from kubernetes as well. You will need to query historical node data to get it.
				klog.V(2).Infof("Node \"%s\" has been deleted from Kubernetes. Query historical data to get it.", c.NodeName)
			} else {
				node = n
			}
			costs := &CostData{
				Name:      c.ContainerName,
				PodName:   c.PodName,
				NodeName:  c.NodeName,
				NodeData:  node,
				Namespace: c.Namespace,
				RAMReq:    RAMReqV,
				RAMUsed:   RAMUsedV,
				CPUReq:    CPUReqV,
				CPUUsed:   CPUUsedV,
				GPUReq:    GPUReqV,
			}
			costs.CPUAllocation = getContainerAllocation(costs.CPUReq, costs.CPUUsed)
			costs.RAMAllocation = getContainerAllocation(costs.RAMReq, costs.RAMUsed)
			containerNameCost[key] = costs
		}
	}
	return containerNameCost, err
}

func getNamespaceLabels(clientset kubernetes.Interface) (map[string]map[string]string, error) {
	nsToLabels := make(map[string]map[string]string)
	nss, err := clientset.CoreV1().Namespaces().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, ns := range nss.Items {
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

type PersistentVolumeData struct {
	Class     string    `json:"class"`
	Claim     string    `json:"claim"`
	Namespace string    `json:"namespace"`
	Values    []*Vector `json:"values"`
}

func getPVInfoVectors(qr interface{}) (map[string]*PersistentVolumeData, error) {
	pvmap := make(map[string]*PersistentVolumeData)
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		pvclaim := metricMap["persistentvolumeclaim"]
		pvclass := metricMap["storageclass"]
		pvnamespace := metricMap["namespace"]
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
				Timestamp: dataPoint[0].(float64),
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
	return pvmap, nil
}

func getPVInfoVector(qr interface{}) (map[string]*PersistentVolumeData, error) {
	pvmap := make(map[string]*PersistentVolumeData)
	for _, val := range qr.(map[string]interface{})["data"].(map[string]interface{})["result"].([]interface{}) {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		pvclaim := metricMap["persistentvolumeclaim"]
		pvclass := metricMap["storageclass"]
		pvnamespace := metricMap["namespace"]
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

		key := pvclaim.(string) + "," + pvnamespace.(string)
		pvmap[key] = &PersistentVolumeData{
			Class:     pvclass.(string),
			Claim:     pvclaim.(string),
			Namespace: pvnamespace.(string),
			Values:    vectors,
		}
	}
	return pvmap, nil
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
		klog.V(1).Infof("ERROR" + err.Error())
	}
	if err != nil {
		return nil, err
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		klog.V(1).Infof("ERROR" + err.Error())
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
		klog.V(1).Infof("ERROR" + err.Error())
	}
	return toReturn, err
}

//todo: don't cast, implement unmarshaler interface
func getNormalization(qr interface{}) (float64, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		return 0, fmt.Errorf("Data field not found in normalization response, aborting")
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
}

func (c *ContainerMetric) Key() string {
	return c.Namespace + "," + c.PodName + "," + c.ContainerName + "," + c.NodeName
}

func newContainerMetricFromKey(key string) (*ContainerMetric, error) {
	s := strings.Split(key, ",")
	if len(s) == 4 {
		return &ContainerMetric{
			Namespace:     s[0],
			PodName:       s[1],
			ContainerName: s[2],
			NodeName:      s[3],
		}, nil
	}
	return nil, fmt.Errorf("Not a valid key")
}

func newContainerMetricFromValues(ns string, podName string, containerName string, nodeName string) *ContainerMetric {
	return &ContainerMetric{
		Namespace:     ns,
		PodName:       podName,
		ContainerName: containerName,
		NodeName:      nodeName,
	}
}

func newContainerMetricsFromPod(pod v1.Pod) ([]*ContainerMetric, error) {
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
		})
	}
	return cs, nil
}

func newContainerMetricFromPrometheus(metrics map[string]interface{}) (*ContainerMetric, error) {
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
		return nil, fmt.Errorf("Prometheus vector does not have node name")
	}
	nodeName, ok := node.(string)
	if !ok {
		return nil, fmt.Errorf("Prometheus vector does not have string nodename")
	}
	return &ContainerMetric{
		ContainerName: containerName,
		PodName:       podName,
		Namespace:     namespace,
		NodeName:      nodeName,
	}, nil
}

func getContainerMetricVector(qr interface{}, normalize bool, normalizationValue float64) (map[string][]*Vector, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted response from prometheus, response has no data field")
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
		containerMetric, err := newContainerMetricFromPrometheus(metric)
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

func getContainerMetricVectors(qr interface{}, normalize bool, normalizationValue float64) (map[string][]*Vector, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted response from prometheus, response has no data field")
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
		containerMetric, err := newContainerMetricFromPrometheus(metric)
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
				Timestamp: dataPoint[0].(float64),
				Value:     v,
			})
		}
		containerData[containerMetric.Key()] = vectors
	}
	return containerData, nil
}
