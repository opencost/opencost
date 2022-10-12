package prom

var queries = map[string]string{
	"RAMRequests": `
		avg(
			count_over_time(
				kube_pod_container_resource_requests{
					resource="memory",
					unit="byte",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
			*
			avg_over_time(
				kube_pod_container_resource_requests{
					resource="memory",
					unit="byte",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
				)
			) by (
				namespace,
				node,
				<< .container >>,
				<< .pod >>,
				<< .cluster >>
			)`,
	"RAMUsage": `
		sort_desc(
			avg(
				count_over_time(
					container_memory_working_set_bytes{
						node!="",
						<< .container >>!="",
						<< .container >>!="POD",
						<< .filter >>
					}[<< .duration >>] << .offset >>
				)
				*
				avg_over_time(
					container_memory_working_set_bytes{
						node!=""
						<< .container >>!="",
						<< .container >>!="POD",
						<< .filter >>
					}[<< .duration >>] << .offset >>
				)
			) by (
				namespace,
				node,
				<< .container >>,
				<< .pod >>,
				<< .cluster >>
			)
		)`,
	"CPURequests": `
		avg(
			count_over_time(
				kube_pod_container_resource_requests{
					resource="cpu",
					unit="core",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
			*
			avg_over_time(
				kube_pod_container_resource_requests{
					resource="cpu",
					unit="core",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			node,
			<< .container >>,
			<< .pod >>,
			<< .cluster >>
		)`,
	"CPUUsage": `
		avg(
			rate(
				container_cpu_usage_seconds_total{
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			node,
			<< .container >>,
			<< .pod >>,
			<< .cluster >>
		)`,
	"GPURequests": `
		avg(
			count_over_time(
				kube_pod_container_resource_requests{
					resource="nvidia_com_gpu",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
			*
			avg_over_time(
				kube_pod_container_resource_requests{
					resource="nvidia_com_gpu",
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
			* 
			<< .interval >>
		) by (
			namespace,
			node,
			<< .container >>,
			<< .pod >>,
			<< .cluster >>
		)
		* on (
			namespace,
			<< .pod >>,
			<< .cluster >>
		) group_left(
			<< .container >>
		) avg(
			avg_over_time(
				kube_pod_status_phase{
					phase="Running",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			<< .pod >>,
			<< .cluster >>
		)`,
	"PVRequests": `
		avg(
			avg(
				kube_persistentvolumeclaim_info{
					volumename!="",
					<< .filter >>
				}
			) by (
				persistentvolumeclaim,
				storageclass,
				namespace,
				volumename,
				kubernetes_node,
				<< .cluster >>
			)
			*
			on (
				persistentvolumeclaim,
				namespace,
				kubernetes_node,
				<< .cluster >>
			) group_right(
				storageclass,
				volumename
			) sum(
				kube_persistentvolumeclaim_resource_requests_storage_bytes{
					<< .filter >>
				}
			) by (
				persistentvolumeclaim,
				namespace,
				kubernetes_node,
				kubernetes_name,
				<< .cluster >>
			)
		) by (
			persistentvolumeclaim,
			storageclass,
			namespace,
			volumename,
			kubernetes_node,
			<< .cluster >>
		)`,
	"RAMAlloc": `
		sum(
			sum_over_time(
				container_memory_allocation_bytes{
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>]
			)
		) by (
			namespace,
			node,
			<< .pod >>,
			<< .container >>,
			<< .cluster >>
		)
		* 
		<< .interval >> / 60 / 60`,
	"CPUAlloc": `
		sum(
			sum_over_time(
				container_cpu_allocation{
					node!="",
					<< .container >>!="",
					<< .container >>!="POD",
					<< .filter >>
				}[<< .duration >>]
			)
		) by (
			namespace,
			node,
			<< .pod >>,
			<< .container >>,
			<< .cluster >>
		) 
		* 
		<< .interval >> / 60 / 60`,
	"PVCAlloc": `
		sum(
			sum_over_time(
				pod_pvc_allocation{
					<< .filter >>
				}[<< .duration >>]
			)
		) by (
			namespace,
			persistentvolume,
			persistentvolumeclaim,
			<< .cluster >>
		)
		*
		<< .interval >>/60/60`,
	"PVHourlyCost": `
		avg_over_time(
			pv_hourly_cost{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"NamespaceLabels": `
		avg_over_time(
			kube_namespace_labels{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"PodLabels": `
		avg_over_time(
			kube_pod_labels{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"NamespaceAnnotations": `
		avg_over_time(
			kube_namespace_annotations{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"PodAnnotations": `
		avg_over_time(
			kube_pod_annotations{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"DeploymentLabels": `
		avg_over_time(
			deployment_match_labels{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"StatefulSetLabels": `
		avg_over_time(
			statefulSet_match_labels{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"DaemonSetLabels": `
		sum(
			kube_pod_owner{
				owner_kind="DaemonSet",
				<< .filter >>
			}
		) by (
			namespace,
			owner_name,
			<< .pod >>,
			<< .cluster >>
		)`,
	"JobLabels": `
		sum(
			kube_pod_owner{
				owner_kind="Job",
				<< .filter >>
			}
		) by (
			namespace,
			owner_name,
			<< .pod >>,
			<< .cluster >>
		)`,
	"ServiceLabels": `
		avg_over_time(
			service_selector_labels{
				<< .filter >>
			}[<< .duration >>]
		)`,
	"NetZoneRequests": `
		sum(
			increase(
				kubecost_pod_network_egress_bytes_total{
					internet="false",
					sameZone="false",
					sameRegion="true",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			<< .pod >>,
			<< .cluster >>
		) / 1024 / 1024 / 1024`,
	"NetRegionRequests": `
		sum(
			increase(
				kubecost_pod_network_egress_bytes_total{
					internet="false",
					sameZone="false",
					sameRegion="false",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			<< .pod >>,
			<< .cluster >>
		) / 1024 / 1024 / 1024`,
	"NetInternetRequests": `
		sum(
			increase(
				kubecost_pod_network_egress_bytes_total{
					internet="true",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			namespace,
			<< .pod >>,
			<< .cluster >>
		) / 1024 / 1024 / 1024`,
	"Normalization": `
		max(
			count_over_time(
				kube_pod_container_resource_requests{
					resource="memory",
					unit="byte",
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		)`,
	"CPUCost": `
		avg(
			avg_over_time(
				node_cpu_hourly_cost{
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			node,
			instance,
			<< .cluster >>
		)`,
	"RAMCost": `
		avg(
			avg_over_time(
				node_ram_hourly_cost{
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			node,
			instance,
			<< .cluster >>
		)`,
	"GPUCost": `
		avg(
			avg_over_time(
				node_gpu_hourly_cost{
					<< .filter >>
				}[<< .duration >>] << .offset >>
			)
		) by (
			node,
			instance,
			<< .cluster >>
		)`,



	"Pods": `avg(kube_pod_container_status_running{<< .filter >>}) by (namespace, << .pod >>, << .cluster >>)[<< .duration >>: << .resolution >>]`,
	"PodsUID": `avg(kube_pod_container_status_running{<< .filter >>}) by (namespace, uid, << .pod >>, << .cluster >>)[<< .duration >>: << .resolution >>]`,
	"RAMBytesAllocated": `avg(avg_over_time(container_memory_allocation_bytes{<< .container >>!="", << .container >>!="POD", node!="", << .filter >>}[<< .duration >>])) by (namespace, provider_id, node, << .container >>, << .pod >>, << .cluster >>)`,
	"RAMReq": `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", << .container >>!="", << .container >>!="POD", node!="", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"RAMUsageAvg": `avg(avg_over_time(container_memory_working_set_bytes{<< .container >>!="", << .container >>!="POD", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"RAMUsageMax": `max(max_over_time(container_memory_working_set_bytes{<< .container >>!="", << .container >>!="POD", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"CPUCoresAllocated": `avg(avg_over_time(container_cpu_allocation{<< .container >>!="", << .container >>!="POD", node!="", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"CPUReq": `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", << .container >>!="", << .container >>!="POD", node!="", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster>>)`,
	"CPUUsageAvg": `avg(rate(container_cpu_usage_seconds_total{<< .container >>!="", << .container >>!="POD", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"CPUUsageMax": `max(rate(container_cpu_usage_seconds_total{<< .container >>!="", << .container >>!="POD", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"GPUsRequested": `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", << .container >>!="",<< .container >>!="POD", node!="", << .filter >>}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>,  << .cluster >>)`,
	"GPUsAllocated": `avg(avg_over_time(container_gpu_allocation{<< .container >>!="", << .container >>!="POD", node!=""}[<< .duration >>])) by (namespace, node, << .container >>, << .pod >>, << .cluster >>)`,
	"NodeCostPerCPUHr": `avg(avg_over_time(node_cpu_hourly_cost{<< .filter >>}[<< .duration >>])) by (instance_type, provider_id, node, << .cluster >>)`,
	"NodeCostPerRAMGiBHr": `avg(avg_over_time(node_ram_hourly_cost{<< .filter >>}[<< .duration >>])) by (instance_type, provider_id, node, << .cluster >>)`,
	"NodeCostPerGPUHr": `avg(avg_over_time(node_gpu_hourly_cost{<< .filter >>}[<< .duration >>])) by (instance_type, provider_id, node, << .cluster >>)`,
	"NodeIsSpot": `avg_over_time(kubecost_node_is_spot{<< .filter >>}[<< .duration >>])`,
	"PVCInfo": `avg(kube_persistentvolumeclaim_info{volumename != "", << .filter >>}) by (persistentvolumeclaim, storageclass, volumename, namespace, << .cluster >>)[<< .duration >>: << .resolution >>]`,
	"PVBytes": `avg(avg_over_time(kube_persistentvolume_capacity_bytes{<< .filter >>}[<< .duration >>])) by (persistentvolume, << .cluster >>)`,
	"PodPVCAllocation": `avg(avg_over_time(pod_pvc_allocation{<< .filter >>}[<< .duration >>])) by (persistentvolume, persistentvolumeclaim, namespace, << .pod >>, << .cluster >>)`,
	"PVCBytesRequested": `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{<< .filter >>}[<< .duration >>])) by (persistentvolumeclaim, namespace, << .cluster >>)`,
	"PVCostPerGiBHour": `avg(avg_over_time(pv_hourly_cost{<< .filter >>}[<< .duration >>])) by (volumename, << .cluster >>)`,
	"NetZoneGiB": `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="true", << .filter >>}[<< .duration >>])) by (namespace, << .pod >>, << .cluster >>) / 1024 / 1024 / 1024`,
	"NetZoneCostPerGiB": `avg(avg_over_time(kubecost_network_zone_egress_cost{<< .filter >>}[<< .duration >>])) by (<< .cluster >>)`,
	"NetRegionGiB": `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="false", << .filter >>}[<< .duration >>])) by (namespace, << .pod >>, << .cluster >>) / 1024 / 1024 / 1024`,
	"NetRegionCostPerGiB": `avg(avg_over_time(kubecost_network_region_egress_cost{<< .filter >>}[<< .duration >>])) by (<< .cluster >>)`,
	"NetInternetGiB": `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", << .filter >>}[<< .duration >>])) by (namespace, << .pod >>, << .cluster >>) / 1024 / 1024 / 1024`,
	"NetInternetCostPerGiB": `avg(avg_over_time(kubecost_network_internet_egress_cost{<< .filter >>}[<< .duration >>])) by (<< .cluster >>)`,
	"NetReceiveBytes": `sum(increase(container_network_receive_bytes_total{<< .pod >>!="", << .container >>="POD", << .filter >>}[<< .duration >>])) by (namespace, << .pod >>, << .cluster >>)`,
	"NetTransferBytes": `sum(increase(container_network_transmit_bytes_total{<< .pod >>!="", << .container >>="POD", << .filter >>}[<< .duration >>])) by (namespace, << .pod >>, << .cluster >>)`,
	"PodsWithReplicaSetOwner": `
		sum(
			avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", << .filter >>}[<< .duration >>])) by (owner_name, namespace, << .pod >>, << .cluster >>)`,
	"ReplicaSetsWithoutOwners": `
		avg(
			avg_over_time(
				kube_replicaset_owner{
					owner_kind="<none>",
					owner_name="<none>",
					<< .filter >>
				}[<< .duration >>]
			)
		) by (
			replicaset,
			namespace,
			<< .cluster >>
		)`,
	"LBCostPerHr": `
		avg(
			avg_over_time(
				kubecost_load_balancer_cost{
					<< .filter >>
				}[<< .duration >>]
			)
		) by (
			namespace,
			service_name,
			<< .cluster >>
		)`,
	"LBActiveMins": `
		count(
			kubecost_load_balancer_cost{
				<< .filter >>
			}
		) by (
			namespace,
			service_name,
			<< .cluster >>
		)[<< .duration >>: << .resolution >>]`,
}
