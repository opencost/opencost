package collector

import (
	"testing"

	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
)

const networkScape = `
# HELP kubecost_pod_network_egress_bytes kubecost_pod_network_egress_bytes_total egressed byte counts by pod.
# TYPE kubecost_pod_network_egress_bytes counter
kubecost_pod_network_egress_bytes_total{pod_name="pod1",namespace="namespace1",internet="false",same_region="true",same_zone="true",service="service1"} 3127969647
kubecost_pod_network_egress_bytes_total{pod_name="pod2",namespace="namespace1",internet="true",same_region="false",same_zone="false",service=""} 335188219
# HELP kubecost_pod_network_ingress_bytes kubecost_pod_network_ingress_bytes_total ingressed byte counts by pod.
# TYPE kubecost_pod_network_ingress_bytes counter
kubecost_pod_network_ingress_bytes_total{pod_name="pod1",namespace="namespace1",internet="true",same_region="false",same_zone="false",service="service1"} 17941460
kubecost_pod_network_ingress_bytes_total{pod_name="pod2",namespace="namespace1",internet="false",same_region="true",same_zone="false",service=""} 13948766
# HELP kubecost_network_costs_parsed_entries kubecost_network_costs_parsed_entries total parsed conntrack entries.
# TYPE kubecost_network_costs_parsed_entries gauge
# HELP kubecost_network_costs_parse_time kubecost_network_costs_parse_time total time in milliseconds it took to parse conntrack entries.
# TYPE kubecost_network_costs_parse_time gauge
# EOF
`

const kubecostScrape = `
# HELP container_cpu_allocation container_cpu_allocation Percent of a single CPU used in a minute
# TYPE container_cpu_allocation gauge
container_cpu_allocation{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-autoscaler-b5dd874d8-wxp5h"} 0.02
container_cpu_allocation{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt"} 0.01
# HELP container_gpu_allocation container_gpu_allocation GPU used
# TYPE container_gpu_allocation gauge
container_gpu_allocation{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-autoscaler-b5dd874d8-wxp5h"} 0
container_gpu_allocation{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt"} 0
# HELP container_memory_allocation_bytes container_memory_allocation_bytes Bytes of RAM used
# TYPE container_memory_allocation_bytes gauge
container_memory_allocation_bytes{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-autoscaler-b5dd874d8-wxp5h"} 1.1528192e+07
container_memory_allocation_bytes{container="autoscaler",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt"} 1e+07
# HELP deployment_match_labels deployment_match_labels Deployment Match Labels
# TYPE deployment_match_labels gauge
deployment_match_labels{deployment="dashboard-metrics-scraper",label_k8s_app="dashboard-metrics-scraper",namespace="kubernetes-dashboard"} 1
# HELP kube_namespace_labels kube_namespace_annotations Namespace Annotations
# TYPE kube_namespace_labels gauge
kube_namespace_labels{label_kubernetes_io_metadata_name="acme-fitness",namespace="acme-fitness"} 1
# HELP kube_node_labels kube_node_labels all labels for each node prefixed with label_
# TYPE kube_node_labels gauge
kube_node_labels{label_beta_kubernetes_io_arch="amd64",label_beta_kubernetes_io_instance_type="e2-standard-2",label_beta_kubernetes_io_os="linux",label_cloud_google_com_gke_boot_disk="pd-standard",label_cloud_google_com_gke_container_runtime="containerd",label_cloud_google_com_gke_cpu_scaling_level="2",label_cloud_google_com_gke_logging_variant="DEFAULT",label_cloud_google_com_gke_max_pods_per_node="110",label_cloud_google_com_gke_memory_gb_scaling_level="8",label_cloud_google_com_gke_nodepool="pool-2",label_cloud_google_com_gke_os_distribution="cos",label_cloud_google_com_gke_provisioning="standard",label_cloud_google_com_gke_stack_type="IPV4",label_cloud_google_com_machine_family="e2",label_cloud_google_com_private_node="false",label_failure_domain_beta_kubernetes_io_region="us-central1",label_failure_domain_beta_kubernetes_io_zone="us-central1-c",label_kubernetes_io_arch="amd64",label_kubernetes_io_hostname="gke-kc-demo-stage-pool-2-70aa2479-4ric",label_kubernetes_io_os="linux",label_node_kubernetes_io_instance_type="e2-standard-2",label_providerID="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",label_topology_gke_io_zone="us-central1-c",label_topology_kubernetes_io_region="us-central1",label_topology_kubernetes_io_zone="us-central1-c",node="gke-kc-demo-stage-pool-2-70aa2479-4ric"} 1
# HELP kube_node_status_allocatable_cpu_cores kube_node_status_allocatable_cpu_cores node allocatable cpu cores
# TYPE kube_node_status_allocatable_cpu_cores gauge
kube_node_status_allocatable_cpu_cores{node="gke-kc-demo-stage-pool-2-70aa2479-4ric"} 1.93
# HELP kube_node_status_allocatable_memory_bytes kube_node_status_allocatable_memory_bytes node allocatable memory in bytes
# TYPE kube_node_status_allocatable_memory_bytes gauge
kube_node_status_allocatable_memory_bytes{node="gke-kc-demo-stage-pool-2-70aa2479-4ric"} 6.32121344e+09
# HELP kube_node_status_capacity_cpu_cores kube_node_status_capacity_cpu_cores Node Capacity CPU Cores
# TYPE kube_node_status_capacity_cpu_cores gauge
kube_node_status_capacity_cpu_cores{node="gke-kc-demo-stage-pool-2-70aa2479-4ric"} 2
kube_node_status_capacity_cpu_cores{node="gke-kc-demo-stage-pool-2-70aa2479-r5b7"} 2
# HELP kube_node_status_capacity_memory_bytes kube_node_status_capacity_memory_bytes Node Capacity Memory Bytes
# TYPE kube_node_status_capacity_memory_bytes gauge
kube_node_status_capacity_memory_bytes{node="gke-kc-demo-stage-pool-2-70aa2479-4ric"} 8.333430784e+09
kube_node_status_capacity_memory_bytes{node="gke-kc-demo-stage-pool-2-70aa2479-r5b7"} 8.333430784e+09
# HELP kube_persistentvolume_capacity_bytes kube_persistentvolume_capacity_bytes pv storage capacity in bytes
# TYPE kube_persistentvolume_capacity_bytes gauge
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294"} 3.4359738368e+10
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e"} 3.4359738368e+10
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-56e397e0-4f93-4d09-8493-5dc644593d33"} 3.4359738368e+10
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb"} 2.147483648e+11
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148"} 3.4359738368e+10
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae"} 2.147483648e+11
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-af084eeb-792c-48f1-8150-885630ace62a"} 2.147483648e+11
kube_persistentvolume_capacity_bytes{persistentvolume="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669"} 1.073741824e+09
# HELP kube_persistentvolumeclaim_info kube_persistentvolumeclaim_info pvc storage resource requests in bytes
# TYPE kube_persistentvolumeclaim_info gauge
kube_persistentvolumeclaim_info{namespace="infra-costmanagement",persistentvolumeclaim="kc-infra-costmanagement-agent-cost-analyzer",storageclass="standard",volumename="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e"} 1
kube_persistentvolumeclaim_info{namespace="infra-costmanagement",persistentvolumeclaim="kc-infra-costmanagement-agent-prometheus-server",storageclass="standard",volumename="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294"} 1
kube_persistentvolumeclaim_info{namespace="kubecost",persistentvolumeclaim="kubecost-cost-analyzer",storageclass="standard",volumename="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148"} 1
kube_persistentvolumeclaim_info{namespace="kubecost",persistentvolumeclaim="kubecost-prometheus-server",storageclass="standard",volumename="pvc-56e397e0-4f93-4d09-8493-5dc644593d33"} 1
kube_persistentvolumeclaim_info{namespace="pacman",persistentvolumeclaim="mongo-storage",storageclass="standard",volumename="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669"} 1
# HELP kube_persistentvolumeclaim_resource_requests_storage_bytes kube_persistentvolumeclaim_resource_requests_storage_bytes pvc storage resource requests in bytes
# TYPE kube_persistentvolumeclaim_resource_requests_storage_bytes gauge
kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="infra-costmanagement",persistentvolumeclaim="kc-infra-costmanagement-agent-cost-analyzer"} 3.4359738368e+10
kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="infra-costmanagement",persistentvolumeclaim="kc-infra-costmanagement-agent-prometheus-server"} 3.4359738368e+10
kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="kubecost",persistentvolumeclaim="kubecost-cost-analyzer"} 3.4359738368e+10
kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="kubecost",persistentvolumeclaim="kubecost-prometheus-server"} 3.4359738368e+10
kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="pacman",persistentvolumeclaim="mongo-storage"} 1.073741824e+09
# HELP kube_pod_container_resource_requests kube_pod_container_resource_requests pods container resource requests
# TYPE kube_pod_container_resource_requests gauge
kube_pod_container_resource_requests{container="autoscaler",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-autoscaler-b5dd874d8-wxp5h",resource="cpu",uid="c114bc75-477f-48bc-9a3b-d5a3eabf476f",unit="core"} 0.02
kube_pod_container_resource_requests{container="autoscaler",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-autoscaler-b5dd874d8-wxp5h",resource="memory",uid="c114bc75-477f-48bc-9a3b-d5a3eabf476f",unit="byte"} 1.048576e+07
kube_pod_container_resource_requests{container="autoscaler",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt",resource="cpu",uid="567087c8-4dfe-4dc1-9078-fdc1c252eee2",unit="core"} 0.01
kube_pod_container_resource_requests{container="autoscaler",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt",resource="memory",uid="567087c8-4dfe-4dc1-9078-fdc1c252eee2",unit="byte"} 1e+07
kube_pod_container_resource_requests{container="cart",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="cart-65ddcdb87d-r8527",resource="cpu",uid="1b60aa55-1651-41bd-af19-9c884765e3a1",unit="core"} 0.1
kube_pod_container_resource_requests{container="cart",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="cart-65ddcdb87d-r8527",resource="memory",uid="1b60aa55-1651-41bd-af19-9c884765e3a1",unit="byte"} 6.7108864e+07
kube_pod_container_resource_requests{container="cart-redis",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="cart-redis-69d757ddbd-qplt2",resource="cpu",uid="0e61ae63-5853-4a32-9c60-640b19b8fa4a",unit="core"} 0.1
kube_pod_container_resource_requests{container="cart-redis",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="cart-redis-69d757ddbd-qplt2",resource="memory",uid="0e61ae63-5853-4a32-9c60-640b19b8fa4a",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="catalog",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="catalog-58bc8cfc6f-2fd42",resource="cpu",uid="e7344cfb-41c6-4aeb-94ce-57acbf188210",unit="core"} 0.1
kube_pod_container_resource_requests{container="catalog",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="catalog-58bc8cfc6f-2fd42",resource="memory",uid="e7344cfb-41c6-4aeb-94ce-57acbf188210",unit="byte"} 6.7108864e+07
kube_pod_container_resource_requests{container="config-reloader",namespace="monitoring",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="prometheus-prometheus-operator-kube-p-prometheus-0",resource="cpu",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb",unit="core"} 0.1
kube_pod_container_resource_requests{container="config-reloader",namespace="monitoring",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="prometheus-prometheus-operator-kube-p-prometheus-0",resource="memory",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb",unit="byte"} 5.24288e+07
kube_pod_container_resource_requests{container="container-watcher",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="container-watcher-vbtmc",resource="cpu",uid="9c2724fd-02b2-4ed9-a8cd-f056f8dcff99",unit="core"} 0.025
kube_pod_container_resource_requests{container="container-watcher",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="container-watcher-vbtmc",resource="memory",uid="9c2724fd-02b2-4ed9-a8cd-f056f8dcff99",unit="byte"} 5.24288e+07
kube_pod_container_resource_requests{container="container-watcher",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="container-watcher-gkxs4",resource="cpu",uid="f2e82e48-057f-4267-9705-b72a896cc88e",unit="core"} 0.025
kube_pod_container_resource_requests{container="container-watcher",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="container-watcher-gkxs4",resource="memory",uid="f2e82e48-057f-4267-9705-b72a896cc88e",unit="byte"} 5.24288e+07
kube_pod_container_resource_requests{container="controller",namespace="ingress-nginx",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="ingress-nginx-controller-5dfd6977dd-w2m54",resource="cpu",uid="33232d67-5086-4afd-bc1d-fac793d778e5",unit="core"} 0.1
kube_pod_container_resource_requests{container="controller",namespace="ingress-nginx",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="ingress-nginx-controller-5dfd6977dd-w2m54",resource="memory",uid="33232d67-5086-4afd-bc1d-fac793d778e5",unit="byte"} 9.437184e+07
kube_pod_container_resource_requests{container="core-metrics-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="cpu",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="core"} 0.003
kube_pod_container_resource_requests{container="core-metrics-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="memory",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="byte"} 4.194304e+07
kube_pod_container_resource_requests{container="core-metrics-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="cpu",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="core"} 0.003
kube_pod_container_resource_requests{container="core-metrics-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="memory",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="byte"} 4.194304e+07
kube_pod_container_resource_requests{container="cost-model",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v",resource="cpu",uid="0a5c9b3b-feb1-4985-b934-a69fff3c7587",unit="core"} 0.2
kube_pod_container_resource_requests{container="cost-model",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v",resource="memory",uid="0a5c9b3b-feb1-4985-b934-a69fff3c7587",unit="byte"} 5.767168e+07
kube_pod_container_resource_requests{container="cost-model",namespace="kubecost",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kubecost-cost-analyzer-96947bd8d-g8fjd",resource="cpu",uid="c22e4e0d-b66a-4a15-8115-a17a988e3765",unit="core"} 0.2
kube_pod_container_resource_requests{container="cost-model",namespace="kubecost",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kubecost-cost-analyzer-96947bd8d-g8fjd",resource="memory",uid="c22e4e0d-b66a-4a15-8115-a17a988e3765",unit="byte"} 5.767168e+07
kube_pod_container_resource_requests{container="csi-driver-registrar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="pdcsi-node-xphsp",resource="cpu",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a",unit="core"} 0.005
kube_pod_container_resource_requests{container="csi-driver-registrar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="pdcsi-node-xphsp",resource="memory",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a",unit="byte"} 1.048576e+07
kube_pod_container_resource_requests{container="csi-driver-registrar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="pdcsi-node-zs58l",resource="cpu",uid="33bb97ef-82f4-4480-8041-189009529f91",unit="core"} 0.005
kube_pod_container_resource_requests{container="csi-driver-registrar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="pdcsi-node-zs58l",resource="memory",uid="33bb97ef-82f4-4480-8041-189009529f91",unit="byte"} 1.048576e+07
kube_pod_container_resource_requests{container="default-http-backend",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="l7-default-backend-85758d674c-b7vf2",resource="cpu",uid="d0a248ca-1ccd-43ab-82ed-7609f0e525bd",unit="core"} 0.01
kube_pod_container_resource_requests{container="default-http-backend",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="l7-default-backend-85758d674c-b7vf2",resource="memory",uid="d0a248ca-1ccd-43ab-82ed-7609f0e525bd",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="dnsmasq",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="cpu",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="core"} 0.15
kube_pod_container_resource_requests{container="dnsmasq",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="memory",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="dnsmasq",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="cpu",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="core"} 0.15
kube_pod_container_resource_requests{container="dnsmasq",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="memory",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="event-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="event-exporter-gke-6b4f9cf548-xt4mf",resource="cpu",uid="fe0911a5-6b32-4f49-866d-0e710a279686",unit="core"} 0.003
kube_pod_container_resource_requests{container="event-exporter",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="event-exporter-gke-6b4f9cf548-xt4mf",resource="memory",uid="fe0911a5-6b32-4f49-866d-0e710a279686",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="fluentbit",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="cpu",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="core"} 0.05
kube_pod_container_resource_requests{container="fluentbit",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="memory",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="fluentbit",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="cpu",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="core"} 0.05
kube_pod_container_resource_requests{container="fluentbit",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="memory",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="fluentbit-gke",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="cpu",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="core"} 0.05
kube_pod_container_resource_requests{container="fluentbit-gke",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="memory",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="fluentbit-gke",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="cpu",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="core"} 0.05
kube_pod_container_resource_requests{container="fluentbit-gke",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="memory",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="byte"} 1.048576e+08
kube_pod_container_resource_requests{container="fluentbit-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="cpu",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="core"} 0.005
kube_pod_container_resource_requests{container="fluentbit-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="fluentbit-gke-tflm8",resource="memory",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="fluentbit-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="cpu",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="core"} 0.005
kube_pod_container_resource_requests{container="fluentbit-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="fluentbit-gke-d9dmk",resource="memory",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="gce-pd-driver",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="pdcsi-node-xphsp",resource="cpu",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a",unit="core"} 0.005
kube_pod_container_resource_requests{container="gce-pd-driver",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="pdcsi-node-xphsp",resource="memory",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a",unit="byte"} 1.048576e+07
kube_pod_container_resource_requests{container="gce-pd-driver",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="pdcsi-node-zs58l",resource="cpu",uid="33bb97ef-82f4-4480-8041-189009529f91",unit="core"} 0.005
kube_pod_container_resource_requests{container="gce-pd-driver",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="pdcsi-node-zs58l",resource="memory",uid="33bb97ef-82f4-4480-8041-189009529f91",unit="byte"} 1.048576e+07
kube_pod_container_resource_requests{container="gke-metrics-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="cpu",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="core"} 0.008
kube_pod_container_resource_requests{container="gke-metrics-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="memory",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="byte"} 1.1534336e+08
kube_pod_container_resource_requests{container="gke-metrics-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="cpu",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="core"} 0.008
kube_pod_container_resource_requests{container="gke-metrics-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="memory",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="byte"} 1.1534336e+08
kube_pod_container_resource_requests{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kc-infra-costmanagement-agent-network-costs-6nnzh",resource="cpu",uid="ec661291-52f4-40c3-ac70-6f77b3ec6cc6",unit="core"} 0.05
kube_pod_container_resource_requests{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kc-infra-costmanagement-agent-network-costs-6nnzh",resource="memory",uid="ec661291-52f4-40c3-ac70-6f77b3ec6cc6",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kc-infra-costmanagement-agent-network-costs-zn79c",resource="cpu",uid="97caaa49-9579-4ba2-8bf3-f5ebb6d868de",unit="core"} 0.05
kube_pod_container_resource_requests{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kc-infra-costmanagement-agent-network-costs-zn79c",resource="memory",uid="97caaa49-9579-4ba2-8bf3-f5ebb6d868de",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="konnectivity-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="konnectivity-agent-5f7f4497cc-24bln",resource="cpu",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06",unit="core"} 0.01
kube_pod_container_resource_requests{container="konnectivity-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="konnectivity-agent-5f7f4497cc-24bln",resource="memory",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="konnectivity-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-5f7f4497cc-7wg5t",resource="cpu",uid="fd563908-6596-48ef-b4ad-77c849c8bdac",unit="core"} 0.01
kube_pod_container_resource_requests{container="konnectivity-agent",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-5f7f4497cc-7wg5t",resource="memory",uid="fd563908-6596-48ef-b4ad-77c849c8bdac",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="konnectivity-agent-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="konnectivity-agent-5f7f4497cc-24bln",resource="cpu",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06",unit="core"} 0.005
kube_pod_container_resource_requests{container="konnectivity-agent-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="konnectivity-agent-5f7f4497cc-24bln",resource="memory",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="konnectivity-agent-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-5f7f4497cc-7wg5t",resource="cpu",uid="fd563908-6596-48ef-b4ad-77c849c8bdac",unit="core"} 0.005
kube_pod_container_resource_requests{container="konnectivity-agent-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="konnectivity-agent-5f7f4497cc-7wg5t",resource="memory",uid="fd563908-6596-48ef-b4ad-77c849c8bdac",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="kube-proxy",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-4ric",resource="cpu",uid="5a39612a-5a59-47c6-9092-c808e6e15f50",unit="core"} 0.1
kube_pod_container_resource_requests{container="kube-proxy",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-r5b7",resource="cpu",uid="011ee049-c7b5-4ea6-be2d-0214ce33976c",unit="core"} 0.1
kube_pod_container_resource_requests{container="kubedns",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="cpu",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="core"} 0.1
kube_pod_container_resource_requests{container="kubedns",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="memory",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="byte"} 7.340032e+07
kube_pod_container_resource_requests{container="kubedns",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="cpu",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="core"} 0.1
kube_pod_container_resource_requests{container="kubedns",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="memory",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="byte"} 7.340032e+07
kube_pod_container_resource_requests{container="kubedns-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="cpu",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="core"} 0.005
kube_pod_container_resource_requests{container="kubedns-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="memory",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="kubedns-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="cpu",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="core"} 0.005
kube_pod_container_resource_requests{container="kubedns-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="memory",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="byte"} 3.145728e+07
kube_pod_container_resource_requests{container="metrics-server",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="metrics-server-v1.31.0-8765f567-tnvbg",resource="cpu",uid="c5225894-a602-4905-bc61-480f466cb2f6",unit="core"} 0.043
kube_pod_container_resource_requests{container="metrics-server",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="metrics-server-v1.31.0-8765f567-tnvbg",resource="memory",uid="c5225894-a602-4905-bc61-480f466cb2f6",unit="byte"} 5.767168e+07
kube_pod_container_resource_requests{container="metrics-server-nanny",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="metrics-server-v1.31.0-8765f567-tnvbg",resource="cpu",uid="c5225894-a602-4905-bc61-480f466cb2f6",unit="core"} 0.005
kube_pod_container_resource_requests{container="metrics-server-nanny",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="metrics-server-v1.31.0-8765f567-tnvbg",resource="memory",uid="c5225894-a602-4905-bc61-480f466cb2f6",unit="byte"} 5.24288e+07
kube_pod_container_resource_requests{container="nginx-ingress",namespace="nginx-ingress",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="nginx-org-nginx-ingress-controller-c785694f7-h8lwr",resource="cpu",uid="4847c4aa-2960-42ec-8cc1-500af5e21e0c",unit="core"} 0.1
kube_pod_container_resource_requests{container="nginx-ingress",namespace="nginx-ingress",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="nginx-org-nginx-ingress-controller-c785694f7-h8lwr",resource="memory",uid="4847c4aa-2960-42ec-8cc1-500af5e21e0c",unit="byte"} 1.34217728e+08
kube_pod_container_resource_requests{container="opencost",namespace="opencost",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="opencost-585569b847-sxx7w",resource="cpu",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a",unit="core"} 0.01
kube_pod_container_resource_requests{container="opencost",namespace="opencost",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="opencost-585569b847-sxx7w",resource="memory",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a",unit="byte"} 5.767168e+07
kube_pod_container_resource_requests{container="opencost-ui",namespace="opencost",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="opencost-585569b847-sxx7w",resource="cpu",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a",unit="core"} 0.01
kube_pod_container_resource_requests{container="opencost-ui",namespace="opencost",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="opencost-585569b847-sxx7w",resource="memory",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a",unit="byte"} 5.767168e+07
kube_pod_container_resource_requests{container="order",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="order-d86cbd574-qvcd9",resource="cpu",uid="1a801c7d-7b57-4800-a688-4100b5f7b88d",unit="core"} 0.025
kube_pod_container_resource_requests{container="order",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="order-d86cbd574-qvcd9",resource="memory",uid="1a801c7d-7b57-4800-a688-4100b5f7b88d",unit="byte"} 6.7108864e+07
kube_pod_container_resource_requests{container="prometheus-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="cpu",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="core"} 0.01
kube_pod_container_resource_requests{container="prometheus-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="gke-metrics-agent-g4p65",resource="memory",uid="11773247-81ec-4b7a-982b-b039b118db23",unit="byte"} 4.718592e+07
kube_pod_container_resource_requests{container="prometheus-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="cpu",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="core"} 0.01
kube_pod_container_resource_requests{container="prometheus-metrics-collector",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="gke-metrics-agent-mwthl",resource="memory",uid="89f47827-bd8c-48d5-afa1-040aa9f478be",unit="byte"} 4.718592e+07
kube_pod_container_resource_requests{container="sidecar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="cpu",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="core"} 0.01
kube_pod_container_resource_requests{container="sidecar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-dns-584d54b8b5-nt6v7",resource="memory",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="sidecar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="cpu",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="core"} 0.01
kube_pod_container_resource_requests{container="sidecar",namespace="kube-system",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-dns-584d54b8b5-fmk79",resource="memory",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4",unit="byte"} 2.097152e+07
kube_pod_container_resource_requests{container="users",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="users-5446b88fb9-frmzk",resource="cpu",uid="470322c7-3a95-44b3-89f9-000eee0fce91",unit="core"} 0.1
kube_pod_container_resource_requests{container="users",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="users-5446b88fb9-frmzk",resource="memory",uid="470322c7-3a95-44b3-89f9-000eee0fce91",unit="byte"} 6.7108864e+07
kube_pod_container_resource_requests{container="users-redis",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="users-redis-cdd796598-pfc4n",resource="cpu",uid="8e62e323-0fa1-46d3-bc76-14a7dc1d7e40",unit="core"} 0.1
kube_pod_container_resource_requests{container="users-redis",namespace="acme-fitness",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="users-redis-cdd796598-pfc4n",resource="memory",uid="8e62e323-0fa1-46d3-bc76-14a7dc1d7e40",unit="byte"} 1.048576e+08
# HELP kube_pod_container_status_running kube_pod_container_status_running pods container status
# TYPE kube_pod_container_status_running gauge
kube_pod_container_status_running{container="autoscaler",namespace="kube-system",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt",uid="567087c8-4dfe-4dc1-9078-fdc1c252eee2"} 1
kube_pod_container_status_running{container="autoscaler",namespace="kube-system",pod="kube-dns-autoscaler-b5dd874d8-wxp5h",uid="c114bc75-477f-48bc-9a3b-d5a3eabf476f"} 1
kube_pod_container_status_running{container="cart",namespace="acme-fitness",pod="cart-65ddcdb87d-r8527",uid="1b60aa55-1651-41bd-af19-9c884765e3a1"} 1
kube_pod_container_status_running{container="cart-redis",namespace="acme-fitness",pod="cart-redis-69d757ddbd-qplt2",uid="0e61ae63-5853-4a32-9c60-640b19b8fa4a"} 1
kube_pod_container_status_running{container="catalog",namespace="acme-fitness",pod="catalog-58bc8cfc6f-2fd42",uid="e7344cfb-41c6-4aeb-94ce-57acbf188210"} 1
kube_pod_container_status_running{container="catalog-mongo",namespace="acme-fitness",pod="catalog-mongo-857649f774-fcxgm",uid="ba6a910c-0333-4ccf-bf32-9bf2b581396f"} 1
kube_pod_container_status_running{container="cert-manager",namespace="cert-manager",pod="cert-manager-7b8d75c477-hl8nt",uid="cdfc285f-fc5e-4d9e-9571-60e2c566b1ca"} 1
kube_pod_container_status_running{container="cert-manager",namespace="cert-manager",pod="cert-manager-cainjector-6cd8d7f84b-4pwwk",uid="4afc758d-1db8-4c75-81eb-03556d5b67fc"} 1
kube_pod_container_status_running{container="cert-manager",namespace="cert-manager",pod="cert-manager-webhook-64d76db6c-vb65g",uid="f6d34bad-4c42-4fc1-8d72-2a63e96bb236"} 1
kube_pod_container_status_running{container="config-reloader",namespace="monitoring",pod="prometheus-prometheus-operator-kube-p-prometheus-0",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb"} 1
kube_pod_container_status_running{container="container-watcher",namespace="kube-system",pod="container-watcher-gkxs4",uid="f2e82e48-057f-4267-9705-b72a896cc88e"} 1
kube_pod_container_status_running{container="container-watcher",namespace="kube-system",pod="container-watcher-vbtmc",uid="9c2724fd-02b2-4ed9-a8cd-f056f8dcff99"} 1
kube_pod_container_status_running{container="controller",namespace="ingress-nginx",pod="ingress-nginx-controller-5dfd6977dd-w2m54",uid="33232d67-5086-4afd-bc1d-fac793d778e5"} 1
kube_pod_container_status_running{container="core-metrics-exporter",namespace="kube-system",pod="gke-metrics-agent-g4p65",uid="11773247-81ec-4b7a-982b-b039b118db23"} 1
kube_pod_container_status_running{container="core-metrics-exporter",namespace="kube-system",pod="gke-metrics-agent-mwthl",uid="89f47827-bd8c-48d5-afa1-040aa9f478be"} 1
kube_pod_container_status_running{container="cost-model",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v",uid="0a5c9b3b-feb1-4985-b934-a69fff3c7587"} 1
kube_pod_container_status_running{container="cost-model",namespace="kubecost",pod="kubecost-cost-analyzer-96947bd8d-g8fjd",uid="c22e4e0d-b66a-4a15-8115-a17a988e3765"} 1
kube_pod_container_status_running{container="csi-driver-registrar",namespace="kube-system",pod="pdcsi-node-xphsp",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a"} 1
kube_pod_container_status_running{container="csi-driver-registrar",namespace="kube-system",pod="pdcsi-node-zs58l",uid="33bb97ef-82f4-4480-8041-189009529f91"} 1
kube_pod_container_status_running{container="dashboard-metrics-scraper",namespace="kubernetes-dashboard",pod="dashboard-metrics-scraper-7c857855d9-wf8gl",uid="776ad696-564a-4050-b9ea-5de78a795a4f"} 1
kube_pod_container_status_running{container="default-http-backend",namespace="kube-system",pod="l7-default-backend-85758d674c-b7vf2",uid="d0a248ca-1ccd-43ab-82ed-7609f0e525bd"} 1
kube_pod_container_status_running{container="dnsmasq",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_container_status_running{container="dnsmasq",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_container_status_running{container="event-exporter",namespace="kube-system",pod="event-exporter-gke-6b4f9cf548-xt4mf",uid="fe0911a5-6b32-4f49-866d-0e710a279686"} 1
kube_pod_container_status_running{container="falcon-node-sensor",namespace="falcon-system",pod="falcon-helm-falcon-sensor-tgp6w",uid="429bd9d7-fa36-43b7-a740-ec87c8b87fb5"} 1
kube_pod_container_status_running{container="falcon-node-sensor",namespace="falcon-system",pod="falcon-helm-falcon-sensor-tgztr",uid="7b80376e-3d71-448f-8797-3ddcd3532f62"} 1
kube_pod_container_status_running{container="fluentbit",namespace="kube-system",pod="fluentbit-gke-d9dmk",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad"} 1
kube_pod_container_status_running{container="fluentbit",namespace="kube-system",pod="fluentbit-gke-tflm8",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302"} 1
kube_pod_container_status_running{container="fluentbit-gke",namespace="kube-system",pod="fluentbit-gke-d9dmk",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad"} 1
kube_pod_container_status_running{container="fluentbit-gke",namespace="kube-system",pod="fluentbit-gke-tflm8",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302"} 1
kube_pod_container_status_running{container="fluentbit-metrics-collector",namespace="kube-system",pod="fluentbit-gke-d9dmk",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad"} 1
kube_pod_container_status_running{container="fluentbit-metrics-collector",namespace="kube-system",pod="fluentbit-gke-tflm8",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302"} 1
kube_pod_container_status_running{container="frontend",namespace="acme-fitness",pod="frontend-86c5ffb485-29g5f",uid="72fec697-4806-48a1-8def-0ce31d7fb71a"} 1
kube_pod_container_status_running{container="gce-pd-driver",namespace="kube-system",pod="pdcsi-node-xphsp",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a"} 1
kube_pod_container_status_running{container="gce-pd-driver",namespace="kube-system",pod="pdcsi-node-zs58l",uid="33bb97ef-82f4-4480-8041-189009529f91"} 1
kube_pod_container_status_running{container="gke-metrics-agent",namespace="kube-system",pod="gke-metrics-agent-g4p65",uid="11773247-81ec-4b7a-982b-b039b118db23"} 1
kube_pod_container_status_running{container="gke-metrics-agent",namespace="kube-system",pod="gke-metrics-agent-mwthl",uid="89f47827-bd8c-48d5-afa1-040aa9f478be"} 1
kube_pod_container_status_running{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-network-costs-6nnzh",uid="ec661291-52f4-40c3-ac70-6f77b3ec6cc6"} 1
kube_pod_container_status_running{container="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-network-costs-zn79c",uid="97caaa49-9579-4ba2-8bf3-f5ebb6d868de"} 1
kube_pod_container_status_running{container="konnectivity-agent",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-24bln",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06"} 1
kube_pod_container_status_running{container="konnectivity-agent",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-7wg5t",uid="fd563908-6596-48ef-b4ad-77c849c8bdac"} 1
kube_pod_container_status_running{container="konnectivity-agent-metrics-collector",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-24bln",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06"} 1
kube_pod_container_status_running{container="konnectivity-agent-metrics-collector",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-7wg5t",uid="fd563908-6596-48ef-b4ad-77c849c8bdac"} 1
kube_pod_container_status_running{container="kube-proxy",namespace="kube-system",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-4ric",uid="5a39612a-5a59-47c6-9092-c808e6e15f50"} 1
kube_pod_container_status_running{container="kube-proxy",namespace="kube-system",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-r5b7",uid="011ee049-c7b5-4ea6-be2d-0214ce33976c"} 1
kube_pod_container_status_running{container="kubedns",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_container_status_running{container="kubedns",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_container_status_running{container="kubedns-metrics-collector",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_container_status_running{container="kubedns-metrics-collector",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_container_status_running{container="kubernetes-dashboard",namespace="kubernetes-dashboard",pod="kubernetes-dashboard-658b66597c-55j9c",uid="7b4efbf2-8d7b-42ce-a02b-85c87a2d1d66"} 1
kube_pod_container_status_running{container="kubeturbo",namespace="turbo",pod="kubeturbo-6f55869c76-mj5bv",uid="bd444bf5-5431-41a0-b20e-cc755ca63df0"} 1
kube_pod_container_status_running{container="metrics-server",namespace="kube-system",pod="metrics-server-v1.31.0-8765f567-tnvbg",uid="c5225894-a602-4905-bc61-480f466cb2f6"} 1
kube_pod_container_status_running{container="metrics-server-nanny",namespace="kube-system",pod="metrics-server-v1.31.0-8765f567-tnvbg",uid="c5225894-a602-4905-bc61-480f466cb2f6"} 1
kube_pod_container_status_running{container="mongo",namespace="pacman",pod="mongo-766556897-6pnzq",uid="ec99bf05-1622-4170-a4aa-23e6ef98d75c"} 1
kube_pod_container_status_running{container="network-transfer-app",namespace="demo-app",pod="network-transfer-app-8484b8b89c-2b9f7",uid="840636ee-fb1e-4179-b2cf-beae5e0ebed3"} 1
kube_pod_container_status_running{container="nginx-ingress",namespace="nginx-ingress",pod="nginx-org-nginx-ingress-controller-c785694f7-h8lwr",uid="4847c4aa-2960-42ec-8cc1-500af5e21e0c"} 1
kube_pod_container_status_running{container="opencost",namespace="opencost",pod="opencost-585569b847-sxx7w",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a"} 1
kube_pod_container_status_running{container="opencost-ui",namespace="opencost",pod="opencost-585569b847-sxx7w",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a"} 1
kube_pod_container_status_running{container="order",namespace="acme-fitness",pod="order-d86cbd574-qvcd9",uid="1a801c7d-7b57-4800-a688-4100b5f7b88d"} 1
kube_pod_container_status_running{container="pacman",namespace="pacman",pod="pacman-7d76d7787b-4dgjk",uid="765cabe8-3aca-4358-b17c-4cfd2437ee75"} 1
kube_pod_container_status_running{container="payment",namespace="acme-fitness",pod="payment-784d9f88c7-fl49c",uid="4f9c6cb2-8105-4f04-a5eb-55ca049823a3"} 1
kube_pod_container_status_running{container="pos",namespace="acme-fitness",pod="pos-86d949fbc5-k4q7d",uid="0192e7f6-c835-4023-9b9d-c3c8e82b71f6"} 1
kube_pod_container_status_running{container="postgres",namespace="acme-fitness",pod="order-postgres-6cdb48cc6b-2zxm6",uid="f09f3972-acef-4772-a322-bcb3ef582001"} 1
kube_pod_container_status_running{container="prometheus",namespace="monitoring",pod="prometheus-prometheus-operator-kube-p-prometheus-0",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb"} 1
kube_pod_container_status_running{container="prometheus-metrics-collector",namespace="kube-system",pod="gke-metrics-agent-g4p65",uid="11773247-81ec-4b7a-982b-b039b118db23"} 1
kube_pod_container_status_running{container="prometheus-metrics-collector",namespace="kube-system",pod="gke-metrics-agent-mwthl",uid="89f47827-bd8c-48d5-afa1-040aa9f478be"} 1
kube_pod_container_status_running{container="prometheus-operator",namespace="monitoring",pod="prometheus-operator-kube-p-operator-8588b5df99-492mm",uid="36468476-6cfe-498c-9d9e-e16786c34503"} 1
kube_pod_container_status_running{container="prometheus-server",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-prometheus-server-86b88c6f752mrp4",uid="97c3347f-6b81-4e88-841f-fd02517d5c57"} 1
kube_pod_container_status_running{container="prometheus-server",namespace="kubecost",pod="kubecost-prometheus-server-7dd9f9db85-rhx4v",uid="bdd770ab-0aea-44f8-9362-d7fd801c38c5"} 1
kube_pod_container_status_running{container="prometheus-to-sd",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_container_status_running{container="prometheus-to-sd",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_container_status_running{container="prometheus-to-sd-exporter",namespace="kube-system",pod="event-exporter-gke-6b4f9cf548-xt4mf",uid="fe0911a5-6b32-4f49-866d-0e710a279686"} 1
kube_pod_container_status_running{container="proxy-logger",namespace="opencost",pod="proxy-logger-d889dfb87-8zxzg",uid="326c1372-9534-4d02-a1e4-bc8da21f5477"} 1
kube_pod_container_status_running{container="sidecar",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_container_status_running{container="sidecar",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_container_status_running{container="thanos-sidecar",namespace="monitoring",pod="prometheus-prometheus-operator-kube-p-prometheus-0",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb"} 1
kube_pod_container_status_running{container="users",namespace="acme-fitness",pod="users-5446b88fb9-frmzk",uid="470322c7-3a95-44b3-89f9-000eee0fce91"} 1
kube_pod_container_status_running{container="users-mongo",namespace="acme-fitness",pod="users-mongo-c46664f69-c65ht",uid="7bf635da-4822-4727-8222-d49514a9c461"} 1
kube_pod_container_status_running{container="users-redis",namespace="acme-fitness",pod="users-redis-cdd796598-pfc4n",uid="8e62e323-0fa1-46d3-bc76-14a7dc1d7e40"} 1
# HELP kube_pod_labels kube_pod_labels all labels for each pod prefixed with label_
# TYPE kube_pod_labels gauge
kube_pod_labels{label_k8s_app="dashboard-metrics-scraper",label_kubernetes_io_metadata_name="kubernetes-dashboard",label_pod_template_hash="7c857855d9",namespace="kubernetes-dashboard",pod="dashboard-metrics-scraper-7c857855d9-wf8gl",uid="776ad696-564a-4050-b9ea-5de78a795a4f"} 1
kube_pod_labels{label_k8s_app="konnectivity-agent",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="5f7f4497cc",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-24bln",uid="e13fb37f-fe14-4b9b-9ab8-6ecca1575e06"} 1
kube_pod_labels{label_k8s_app="konnectivity-agent",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="5f7f4497cc",namespace="kube-system",pod="konnectivity-agent-5f7f4497cc-7wg5t",uid="fd563908-6596-48ef-b4ad-77c849c8bdac"} 1
kube_pod_labels{label_k8s_app="konnectivity-agent-autoscaler",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="6c9f95d48d",namespace="kube-system",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt",uid="567087c8-4dfe-4dc1-9078-fdc1c252eee2"} 1
kube_pod_labels{label_k8s_app="kube-dns",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="584d54b8b5",namespace="kube-system",pod="kube-dns-584d54b8b5-fmk79",uid="a4d13c01-19af-4f1b-a76e-24fde9740df4"} 1
kube_pod_labels{label_k8s_app="kube-dns",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="584d54b8b5",namespace="kube-system",pod="kube-dns-584d54b8b5-nt6v7",uid="2e5295e1-ba07-47ac-bced-c31b5c29d1c2"} 1
kube_pod_labels{label_k8s_app="kube-dns-autoscaler",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="b5dd874d8",namespace="kube-system",pod="kube-dns-autoscaler-b5dd874d8-wxp5h",uid="c114bc75-477f-48bc-9a3b-d5a3eabf476f"} 1
kube_pod_labels{label_component="kube-proxy",label_kubernetes_io_metadata_name="kube-system",label_tier="node",namespace="kube-system",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-4ric",uid="5a39612a-5a59-47c6-9092-c808e6e15f50"} 1
kube_pod_labels{label_component="kube-proxy",label_kubernetes_io_metadata_name="kube-system",label_tier="node",namespace="kube-system",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-r5b7",uid="011ee049-c7b5-4ea6-be2d-0214ce33976c"} 1
kube_pod_labels{label_k8s_app="kubernetes-dashboard",label_kubernetes_io_metadata_name="kubernetes-dashboard",label_pod_template_hash="658b66597c",namespace="kubernetes-dashboard",pod="kubernetes-dashboard-658b66597c-55j9c",uid="7b4efbf2-8d7b-42ce-a02b-85c87a2d1d66"} 1
kube_pod_labels{label_app="network-transfer-app",label_kubernetes_io_metadata_name="demo-app",label_pod_template_hash="8484b8b89c",namespace="demo-app",pod="network-transfer-app-8484b8b89c-2b9f7",uid="840636ee-fb1e-4179-b2cf-beae5e0ebed3"} 1
kube_pod_labels{label_kubernetes_io_metadata_name="pacman",label_name="mongo",label_pod_template_hash="766556897",namespace="pacman",pod="mongo-766556897-6pnzq",uid="ec99bf05-1622-4170-a4aa-23e6ef98d75c"} 1
kube_pod_labels{label_app="proxy-logger",label_kubernetes_io_metadata_name="opencost",label_pod_template_hash="d889dfb87",namespace="opencost",pod="proxy-logger-d889dfb87-8zxzg",uid="326c1372-9534-4d02-a1e4-bc8da21f5477"} 1
kube_pod_labels{label_controller_revision_hash="787b9ff965",label_k8s_app="gcp-compute-persistent-disk-csi-driver",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="36",namespace="kube-system",pod="pdcsi-node-xphsp",uid="dea3f93a-5786-45b2-88c5-7b7c08437b6a"} 1
kube_pod_labels{label_controller_revision_hash="787b9ff965",label_k8s_app="gcp-compute-persistent-disk-csi-driver",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="36",namespace="kube-system",pod="pdcsi-node-zs58l",uid="33bb97ef-82f4-4480-8041-189009529f91"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="5446b88fb9",label_service="users",namespace="acme-fitness",pod="users-5446b88fb9-frmzk",uid="470322c7-3a95-44b3-89f9-000eee0fce91"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="58bc8cfc6f",label_service="catalog",namespace="acme-fitness",pod="catalog-58bc8cfc6f-2fd42",uid="e7344cfb-41c6-4aeb-94ce-57acbf188210"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="65ddcdb87d",label_service="cart",namespace="acme-fitness",pod="cart-65ddcdb87d-r8527",uid="1b60aa55-1651-41bd-af19-9c884765e3a1"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="69d757ddbd",label_service="cart-redis",namespace="acme-fitness",pod="cart-redis-69d757ddbd-qplt2",uid="0e61ae63-5853-4a32-9c60-640b19b8fa4a"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="6cdb48cc6b",label_service="order-db",namespace="acme-fitness",pod="order-postgres-6cdb48cc6b-2zxm6",uid="f09f3972-acef-4772-a322-bcb3ef582001"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="784d9f88c7",label_service="payment",namespace="acme-fitness",pod="payment-784d9f88c7-fl49c",uid="4f9c6cb2-8105-4f04-a5eb-55ca049823a3"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="857649f774",label_service="catalog-db",namespace="acme-fitness",pod="catalog-mongo-857649f774-fcxgm",uid="ba6a910c-0333-4ccf-bf32-9bf2b581396f"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="86c5ffb485",label_service="frontend",namespace="acme-fitness",pod="frontend-86c5ffb485-29g5f",uid="72fec697-4806-48a1-8def-0ce31d7fb71a"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="86d949fbc5",label_service="pos",namespace="acme-fitness",pod="pos-86d949fbc5-k4q7d",uid="0192e7f6-c835-4023-9b9d-c3c8e82b71f6"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="c46664f69",label_service="users-mongo",namespace="acme-fitness",pod="users-mongo-c46664f69-c65ht",uid="7bf635da-4822-4727-8222-d49514a9c461"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="cdd796598",label_service="users-redis",namespace="acme-fitness",pod="users-redis-cdd796598-pfc4n",uid="8e62e323-0fa1-46d3-bc76-14a7dc1d7e40"} 1
kube_pod_labels{label_app="acmefit",label_kubernetes_io_metadata_name="acme-fitness",label_pod_template_hash="d86cbd574",label_service="order",namespace="acme-fitness",pod="order-d86cbd574-qvcd9",uid="1a801c7d-7b57-4800-a688-4100b5f7b88d"} 1
kube_pod_labels{label_k8s_app="event-exporter",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="6b4f9cf548",label_version="v0.4.0",namespace="kube-system",pod="event-exporter-gke-6b4f9cf548-xt4mf",uid="fe0911a5-6b32-4f49-866d-0e710a279686"} 1
kube_pod_labels{label_k8s_app="glbc",label_kubernetes_io_metadata_name="kube-system",label_name="glbc",label_pod_template_hash="85758d674c",namespace="kube-system",pod="l7-default-backend-85758d674c-b7vf2",uid="d0a248ca-1ccd-43ab-82ed-7609f0e525bd"} 1
kube_pod_labels{label_app_kubernetes_io_instance="kubeturbo",label_app_kubernetes_io_name="kubeturbo",label_kubernetes_io_metadata_name="turbo",label_pod_template_hash="6f55869c76",namespace="turbo",pod="kubeturbo-6f55869c76-mj5bv",uid="bd444bf5-5431-41a0-b20e-cc755ca63df0"} 1
kube_pod_labels{label_k8s_app="metrics-server",label_kubernetes_io_metadata_name="kube-system",label_pod_template_hash="8765f567",label_version="v1.31.0",namespace="kube-system",pod="metrics-server-v1.31.0-8765f567-tnvbg",uid="c5225894-a602-4905-bc61-480f466cb2f6"} 1
kube_pod_labels{label_app_kubernetes_io_instance="opencost",label_app_kubernetes_io_name="opencost",label_kubernetes_io_metadata_name="opencost",label_pod_template_hash="585569b847",namespace="opencost",pod="opencost-585569b847-sxx7w",uid="fbdca7e6-3d51-4e80-9446-7a74996ac96a"} 1
kube_pod_labels{label_container_watcher_unique_id="80b26b52",label_controller_revision_hash="56f4744fc8",label_k8s_app="container-watcher",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="1",namespace="kube-system",pod="container-watcher-gkxs4",uid="f2e82e48-057f-4267-9705-b72a896cc88e"} 1
kube_pod_labels{label_container_watcher_unique_id="80b26b52",label_controller_revision_hash="56f4744fc8",label_k8s_app="container-watcher",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="1",namespace="kube-system",pod="container-watcher-vbtmc",uid="9c2724fd-02b2-4ed9-a8cd-f056f8dcff99"} 1
kube_pod_labels{label_app="cost-analyzer",label_app_kubernetes_io_instance="kubecost",label_app_kubernetes_io_name="cost-analyzer",label_kubernetes_io_metadata_name="kubecost",label_pod_template_hash="96947bd8d",namespace="kubecost",pod="kubecost-cost-analyzer-96947bd8d-g8fjd",uid="c22e4e0d-b66a-4a15-8115-a17a988e3765"} 1
kube_pod_labels{label_component="gke-metrics-agent",label_controller_revision_hash="58dbb64cdd",label_k8s_app="gke-metrics-agent",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="38",namespace="kube-system",pod="gke-metrics-agent-g4p65",uid="11773247-81ec-4b7a-982b-b039b118db23"} 1
kube_pod_labels{label_component="gke-metrics-agent",label_controller_revision_hash="58dbb64cdd",label_k8s_app="gke-metrics-agent",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="38",namespace="kube-system",pod="gke-metrics-agent-mwthl",uid="89f47827-bd8c-48d5-afa1-040aa9f478be"} 1
kube_pod_labels{label_app_kubernetes_io_instance="pacman",label_app_kubernetes_io_name="pacman",label_kubernetes_io_metadata_name="pacman",label_name="pacman",label_pod_template_hash="7d76d7787b",namespace="pacman",pod="pacman-7d76d7787b-4dgjk",uid="765cabe8-3aca-4358-b17c-4cfd2437ee75"} 1
kube_pod_labels{label_app="cost-analyzer",label_app_kubernetes_io_instance="kc-infra-costmanagement-agent",label_app_kubernetes_io_name="cost-analyzer",label_kubernetes_io_metadata_name="infra-costmanagement",label_name="infra-costmanagement",label_pod_template_hash="67655c79fb",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v",uid="0a5c9b3b-feb1-4985-b934-a69fff3c7587"} 1
kube_pod_labels{label_component="fluentbit-gke",label_controller_revision_hash="dc665b8c4",label_k8s_app="fluentbit-gke",label_kubernetes_io_cluster_service="true",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="35",namespace="kube-system",pod="fluentbit-gke-d9dmk",uid="7c485be5-57f0-4e43-9c6c-018eb0dabcad"} 1
kube_pod_labels{label_component="fluentbit-gke",label_controller_revision_hash="dc665b8c4",label_k8s_app="fluentbit-gke",label_kubernetes_io_cluster_service="true",label_kubernetes_io_metadata_name="kube-system",label_pod_template_generation="35",namespace="kube-system",pod="fluentbit-gke-tflm8",uid="5b219e7c-f0b8-4060-bbb9-03c598ee1302"} 1
kube_pod_labels{label_app="prometheus",label_component="server",label_heritage="Helm",label_kubernetes_io_metadata_name="kubecost",label_pod_template_hash="7dd9f9db85",label_release="kubecost",namespace="kubecost",pod="kubecost-prometheus-server-7dd9f9db85-rhx4v",uid="bdd770ab-0aea-44f8-9362-d7fd801c38c5"} 1
kube_pod_labels{label_app_kubernetes_io_instance="nginx-org",label_app_kubernetes_io_name="nginx-ingress",label_app_kubernetes_io_version="3.1.1",label_app_nginx_org_version="1.23.4",label_kubernetes_io_metadata_name="nginx-ingress",label_name="nginx-ingress",label_pod_template_hash="c785694f7",namespace="nginx-ingress",pod="nginx-org-nginx-ingress-controller-c785694f7-h8lwr",uid="4847c4aa-2960-42ec-8cc1-500af5e21e0c"} 1
kube_pod_labels{label_app_kubernetes_io_component="operator",label_app_kubernetes_io_instance="prometheus-operator",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="kube-prometheus",label_helm_sh_chart="kube-prometheus-8.1.5",label_kubernetes_io_metadata_name="monitoring",label_pod_template_hash="8588b5df99",namespace="monitoring",pod="prometheus-operator-kube-p-operator-8588b5df99-492mm",uid="36468476-6cfe-498c-9d9e-e16786c34503"} 1
kube_pod_labels{label_app="prometheus",label_component="server",label_heritage="Helm",label_kubernetes_io_metadata_name="infra-costmanagement",label_name="infra-costmanagement",label_pod_template_hash="86b88c6f75",label_release="kc-infra-costmanagement-agent",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-prometheus-server-86b88c6f752mrp4",uid="97c3347f-6b81-4e88-841f-fd02517d5c57"} 1
kube_pod_labels{label_app="kc-infra-costmanagement-agent-network-costs",label_app_kubernetes_io_instance="kubecost",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="network-costs",label_controller_revision_hash="bdd67b6d8",label_helm_sh_chart="cost-analyzer-v2025.04.12",label_kubernetes_io_metadata_name="infra-costmanagement",label_name="infra-costmanagement",label_pod_template_generation="9",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-network-costs-6nnzh",uid="ec661291-52f4-40c3-ac70-6f77b3ec6cc6"} 1
kube_pod_labels{label_app="kc-infra-costmanagement-agent-network-costs",label_app_kubernetes_io_instance="kubecost",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="network-costs",label_controller_revision_hash="bdd67b6d8",label_helm_sh_chart="cost-analyzer-v2025.04.12",label_kubernetes_io_metadata_name="infra-costmanagement",label_name="infra-costmanagement",label_pod_template_generation="9",namespace="infra-costmanagement",pod="kc-infra-costmanagement-agent-network-costs-zn79c",uid="97caaa49-9579-4ba2-8bf3-f5ebb6d868de"} 1
kube_pod_labels{label_app="cainjector",label_app_kubernetes_io_component="cainjector",label_app_kubernetes_io_instance="cert-manager",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="cainjector",label_app_kubernetes_io_version="v1.9.1",label_helm_sh_chart="cert-manager-v1.9.1",label_kubernetes_io_metadata_name="cert-manager",label_name="cert-manager",label_pod_template_hash="6cd8d7f84b",namespace="cert-manager",pod="cert-manager-cainjector-6cd8d7f84b-4pwwk",uid="4afc758d-1db8-4c75-81eb-03556d5b67fc"} 1
kube_pod_labels{label_app="cert-manager",label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="cert-manager",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="cert-manager",label_app_kubernetes_io_version="v1.9.1",label_helm_sh_chart="cert-manager-v1.9.1",label_kubernetes_io_metadata_name="cert-manager",label_name="cert-manager",label_pod_template_hash="7b8d75c477",namespace="cert-manager",pod="cert-manager-7b8d75c477-hl8nt",uid="cdfc285f-fc5e-4d9e-9571-60e2c566b1ca"} 1
kube_pod_labels{label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="ingress-nginx",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="ingress-nginx",label_app_kubernetes_io_part_of="ingress-nginx",label_app_kubernetes_io_version="1.12.1",label_helm_sh_chart="ingress-nginx-4.12.1",label_kubernetes_io_metadata_name="ingress-nginx",label_name="ingress-nginx",label_pod_template_hash="5dfd6977dd",namespace="ingress-nginx",pod="ingress-nginx-controller-5dfd6977dd-w2m54",uid="33232d67-5086-4afd-bc1d-fac793d778e5"} 1
kube_pod_labels{label_app="webhook",label_app_kubernetes_io_component="webhook",label_app_kubernetes_io_instance="cert-manager",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="webhook",label_app_kubernetes_io_version="v1.9.1",label_helm_sh_chart="cert-manager-v1.9.1",label_kubernetes_io_metadata_name="cert-manager",label_name="cert-manager",label_pod_template_hash="64d76db6c",namespace="cert-manager",pod="cert-manager-webhook-64d76db6c-vb65g",uid="f6d34bad-4c42-4fc1-8d72-2a63e96bb236"} 1
kube_pod_labels{label_app="falcon-sensor",label_app_kubernetes_io_component="kernel_sensor",label_app_kubernetes_io_instance="falcon-helm",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="falcon-sensor",label_controller_revision_hash="6bd5b4cd69",label_crowdstrike_com_provider="crowdstrike",label_helm_sh_chart="falcon-sensor-1.30.0",label_kubernetes_io_metadata_name="falcon-system",label_pod_security_kubernetes_io_enforce="privileged",label_pod_template_generation="6",namespace="falcon-system",pod="falcon-helm-falcon-sensor-tgp6w",uid="429bd9d7-fa36-43b7-a740-ec87c8b87fb5"} 1
kube_pod_labels{label_app="falcon-sensor",label_app_kubernetes_io_component="kernel_sensor",label_app_kubernetes_io_instance="falcon-helm",label_app_kubernetes_io_managed_by="Helm",label_app_kubernetes_io_name="falcon-sensor",label_controller_revision_hash="6bd5b4cd69",label_crowdstrike_com_provider="crowdstrike",label_helm_sh_chart="falcon-sensor-1.30.0",label_kubernetes_io_metadata_name="falcon-system",label_pod_security_kubernetes_io_enforce="privileged",label_pod_template_generation="6",namespace="falcon-system",pod="falcon-helm-falcon-sensor-tgztr",uid="7b80376e-3d71-448f-8797-3ddcd3532f62"} 1
kube_pod_labels{label_app_kubernetes_io_component="prometheus",label_app_kubernetes_io_instance="prometheus-operator-kube-p-prometheus",label_app_kubernetes_io_managed_by="prometheus-operator",label_app_kubernetes_io_name="prometheus",label_app_kubernetes_io_version="2.38.0",label_apps_kubernetes_io_pod_index="0",label_controller_revision_hash="prometheus-prometheus-operator-kube-p-prometheus-6dcc4957f4",label_kubernetes_io_metadata_name="monitoring",label_operator_prometheus_io_name="prometheus-operator-kube-p-prometheus",label_operator_prometheus_io_shard="0",label_prometheus="prometheus-operator-kube-p-prometheus",label_statefulset_kubernetes_io_pod_name="prometheus-prometheus-operator-kube-p-prometheus-0",namespace="monitoring",pod="prometheus-prometheus-operator-kube-p-prometheus-0",uid="b775e7a8-7d96-4e57-a70e-1522b84089eb"} 1
# HELP kube_pod_owner kube_pod_owner Information about the Pod's owner
# TYPE kube_pod_owner gauge
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="cart-65ddcdb87d",pod="cart-65ddcdb87d-r8527"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="cart-redis-69d757ddbd",pod="cart-redis-69d757ddbd-qplt2"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="catalog-58bc8cfc6f",pod="catalog-58bc8cfc6f-2fd42"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="catalog-mongo-857649f774",pod="catalog-mongo-857649f774-fcxgm"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="frontend-86c5ffb485",pod="frontend-86c5ffb485-29g5f"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="order-d86cbd574",pod="order-d86cbd574-qvcd9"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="order-postgres-6cdb48cc6b",pod="order-postgres-6cdb48cc6b-2zxm6"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="payment-784d9f88c7",pod="payment-784d9f88c7-fl49c"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="pos-86d949fbc5",pod="pos-86d949fbc5-k4q7d"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="users-5446b88fb9",pod="users-5446b88fb9-frmzk"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="users-mongo-c46664f69",pod="users-mongo-c46664f69-c65ht"} 1
kube_pod_owner{namespace="acme-fitness",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="users-redis-cdd796598",pod="users-redis-cdd796598-pfc4n"} 1
kube_pod_owner{namespace="cert-manager",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="cert-manager-7b8d75c477",pod="cert-manager-7b8d75c477-hl8nt"} 1
kube_pod_owner{namespace="cert-manager",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="cert-manager-cainjector-6cd8d7f84b",pod="cert-manager-cainjector-6cd8d7f84b-4pwwk"} 1
kube_pod_owner{namespace="cert-manager",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="cert-manager-webhook-64d76db6c",pod="cert-manager-webhook-64d76db6c-vb65g"} 1
kube_pod_owner{namespace="demo-app",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="network-transfer-app-8484b8b89c",pod="network-transfer-app-8484b8b89c-2b9f7"} 1
kube_pod_owner{namespace="falcon-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="falcon-helm-falcon-sensor",pod="falcon-helm-falcon-sensor-tgp6w"} 1
kube_pod_owner{namespace="falcon-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="falcon-helm-falcon-sensor",pod="falcon-helm-falcon-sensor-tgztr"} 1
kube_pod_owner{namespace="infra-costmanagement",owner_is_controller="true",owner_kind="DaemonSet",owner_name="kc-infra-costmanagement-agent-network-costs",pod="kc-infra-costmanagement-agent-network-costs-6nnzh"} 1
kube_pod_owner{namespace="infra-costmanagement",owner_is_controller="true",owner_kind="DaemonSet",owner_name="kc-infra-costmanagement-agent-network-costs",pod="kc-infra-costmanagement-agent-network-costs-zn79c"} 1
kube_pod_owner{namespace="infra-costmanagement",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v"} 1
kube_pod_owner{namespace="infra-costmanagement",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kc-infra-costmanagement-agent-prometheus-server-86b88c6f75",pod="kc-infra-costmanagement-agent-prometheus-server-86b88c6f752mrp4"} 1
kube_pod_owner{namespace="ingress-nginx",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="ingress-nginx-controller-5dfd6977dd",pod="ingress-nginx-controller-5dfd6977dd-w2m54"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="container-watcher",pod="container-watcher-gkxs4"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="container-watcher",pod="container-watcher-vbtmc"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="fluentbit-gke",pod="fluentbit-gke-d9dmk"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="fluentbit-gke",pod="fluentbit-gke-tflm8"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="gke-metrics-agent",pod="gke-metrics-agent-g4p65"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="gke-metrics-agent",pod="gke-metrics-agent-mwthl"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="pdcsi-node",pod="pdcsi-node-xphsp"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="DaemonSet",owner_name="pdcsi-node",pod="pdcsi-node-zs58l"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="Node",owner_name="gke-kc-demo-stage-pool-2-70aa2479-4ric",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-4ric"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="Node",owner_name="gke-kc-demo-stage-pool-2-70aa2479-r5b7",pod="kube-proxy-gke-kc-demo-stage-pool-2-70aa2479-r5b7"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="event-exporter-gke-6b4f9cf548",pod="event-exporter-gke-6b4f9cf548-xt4mf"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="konnectivity-agent-5f7f4497cc",pod="konnectivity-agent-5f7f4497cc-24bln"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="konnectivity-agent-5f7f4497cc",pod="konnectivity-agent-5f7f4497cc-7wg5t"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="konnectivity-agent-autoscaler-6c9f95d48d",pod="konnectivity-agent-autoscaler-6c9f95d48d-gj8bt"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kube-dns-584d54b8b5",pod="kube-dns-584d54b8b5-fmk79"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kube-dns-584d54b8b5",pod="kube-dns-584d54b8b5-nt6v7"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kube-dns-autoscaler-b5dd874d8",pod="kube-dns-autoscaler-b5dd874d8-wxp5h"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="l7-default-backend-85758d674c",pod="l7-default-backend-85758d674c-b7vf2"} 1
kube_pod_owner{namespace="kube-system",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="metrics-server-v1.31.0-8765f567",pod="metrics-server-v1.31.0-8765f567-tnvbg"} 1
kube_pod_owner{namespace="kubecost",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kubecost-cost-analyzer-96947bd8d",pod="kubecost-cost-analyzer-96947bd8d-g8fjd"} 1
kube_pod_owner{namespace="kubecost",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kubecost-prometheus-server-7dd9f9db85",pod="kubecost-prometheus-server-7dd9f9db85-rhx4v"} 1
kube_pod_owner{namespace="kubernetes-dashboard",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="dashboard-metrics-scraper-7c857855d9",pod="dashboard-metrics-scraper-7c857855d9-wf8gl"} 1
kube_pod_owner{namespace="kubernetes-dashboard",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kubernetes-dashboard-658b66597c",pod="kubernetes-dashboard-658b66597c-55j9c"} 1
kube_pod_owner{namespace="monitoring",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="prometheus-operator-kube-p-operator-8588b5df99",pod="prometheus-operator-kube-p-operator-8588b5df99-492mm"} 1
kube_pod_owner{namespace="monitoring",owner_is_controller="true",owner_kind="StatefulSet",owner_name="prometheus-prometheus-operator-kube-p-prometheus",pod="prometheus-prometheus-operator-kube-p-prometheus-0"} 1
kube_pod_owner{namespace="nginx-ingress",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="nginx-org-nginx-ingress-controller-c785694f7",pod="nginx-org-nginx-ingress-controller-c785694f7-h8lwr"} 1
kube_pod_owner{namespace="opencost",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="opencost-585569b847",pod="opencost-585569b847-sxx7w"} 1
kube_pod_owner{namespace="opencost",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="proxy-logger-d889dfb87",pod="proxy-logger-d889dfb87-8zxzg"} 1
kube_pod_owner{namespace="pacman",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="mongo-766556897",pod="mongo-766556897-6pnzq"} 1
kube_pod_owner{namespace="pacman",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="pacman-7d76d7787b",pod="pacman-7d76d7787b-4dgjk"} 1
kube_pod_owner{namespace="turbo",owner_is_controller="true",owner_kind="ReplicaSet",owner_name="kubeturbo-6f55869c76",pod="kubeturbo-6f55869c76-mj5bv"} 1
# HELP kubecost_allocation_data_status Kubecost Allocation data status monitoring metrics for errors, warnings, empty and total number of allocation sets.
# TYPE kubecost_allocation_data_status gauge
kubecost_allocation_data_status{resolution="daily",status="empty"} 0
kubecost_allocation_data_status{resolution="daily",status="error"} 0
kubecost_allocation_data_status{resolution="daily",status="success"} 92
kubecost_allocation_data_status{resolution="daily",status="warning"} 0
kubecost_allocation_data_status{resolution="hourly",status="empty"} 0
kubecost_allocation_data_status{resolution="hourly",status="error"} 0
kubecost_allocation_data_status{resolution="hourly",status="success"} 50
kubecost_allocation_data_status{resolution="hourly",status="warning"} 0
# HELP kubecost_asset_data_status Kubecost Asset data status monitoring metrics for errors, warnings, empty and total number of asset sets.
# TYPE kubecost_asset_data_status gauge
kubecost_asset_data_status{resolution="daily",status="empty"} 0
kubecost_asset_data_status{resolution="daily",status="error"} 0
kubecost_asset_data_status{resolution="daily",status="success"} 92
kubecost_asset_data_status{resolution="daily",status="warning"} 0
kubecost_asset_data_status{resolution="hourly",status="empty"} 0
kubecost_asset_data_status{resolution="hourly",status="error"} 0
kubecost_asset_data_status{resolution="hourly",status="success"} 50
kubecost_asset_data_status{resolution="hourly",status="warning"} 0
# HELP kubecost_cluster_info kubecost_cluster_info ClusterInfo
# TYPE kubecost_cluster_info gauge
kubecost_cluster_info{account="",clusterprofile="production",errorreporting="true",id="kc-demo-stage",logcollection="true",name="kc-demo-stage",productanalytics="true",project="guestbook-227502",provider="GCP",provisioner="GKE",region="us-central1",remotereadenabled="false",thanosenabled="false",valuesreporting="true",version="1.31"} 1
# HELP kubecost_cluster_management_cost kubecost_cluster_management_cost Hourly cost paid as a cluster management fee.
# TYPE kubecost_cluster_management_cost gauge
kubecost_cluster_management_cost{provisioner_name="GKE"} 0.1
# HELP kubecost_errored_allocation_ingestions number of errored ingested allocations. these do get retried
# TYPE kubecost_errored_allocation_ingestions counter
kubecost_errored_allocation_ingestions{version="nightly"} 0
# HELP kubecost_errored_asset_ingestions number of errored ingested assets. these do get retried
# TYPE kubecost_errored_asset_ingestions counter
kubecost_errored_asset_ingestions{version="nightly"} 0
# HELP kubecost_errored_cloud_cost_ingestions number of errored ingested cloud costs. these do get retried
# TYPE kubecost_errored_cloud_cost_ingestions counter
kubecost_errored_cloud_cost_ingestions{version="nightly"} 0
# HELP kubecost_errored_custom_cost_ingestions number of errored ingested custom costs. these do get retried
# TYPE kubecost_errored_custom_cost_ingestions counter
kubecost_errored_custom_cost_ingestions{version="nightly"} 0
# HELP kubecost_errored_network_insight_ingestions number of errored ingested network insights. these do get retried
# TYPE kubecost_errored_network_insight_ingestions counter
kubecost_errored_network_insight_ingestions{version="nightly"} 0
# HELP kubecost_etl_events_total Kubecost ETL events counted by kind.
# TYPE kubecost_etl_events_total counter
kubecost_etl_events_total{etl="allocation",event="AllocationSetDeletedEvent"} 50
kubecost_etl_events_total{etl="allocation",event="AllocationSetLoaded"} 138
kubecost_etl_events_total{etl="allocation",event="AllocationSetSaved"} 2573
kubecost_etl_events_total{etl="allocation",event="AllocationSetTotaled"} 2711
kubecost_etl_events_total{etl="asset",event="AssetSetDeletedEvent"} 50
kubecost_etl_events_total{etl="asset",event="AssetSetLoaded"} 138
kubecost_etl_events_total{etl="asset",event="AssetSetSaved"} 2573
kubecost_etl_events_total{etl="asset",event="AssetSetTotaled"} 2711
# HELP kubecost_etl_progress_percent Kubecost ETL build coverage progress by percentage of overall intended coverage.
# TYPE kubecost_etl_progress_percent gauge
kubecost_etl_progress_percent{etl="allocation",resolution="daily"} 1
kubecost_etl_progress_percent{etl="allocation",resolution="hourly"} 1
kubecost_etl_progress_percent{etl="asset",resolution="daily"} 1
kubecost_etl_progress_percent{etl="asset",resolution="hourly"} 1
# HELP kubecost_http_requests_total kubecost_http_requests_total Total number of HTTP requests
# TYPE kubecost_http_requests_total counter
kubecost_http_requests_total{code="200",handler="/healthz",method="GET"} 36450
kubecost_http_requests_total{code="200",handler="/metrics",method="GET"} 3644
kubecost_http_requests_total{code="200",handler="/prometheusQuery",method="GET"} 4249
# HELP kubecost_http_response_size_bytes kubecost_http_response_size_bytes Response size in bytes
# TYPE kubecost_http_response_size_bytes summary
kubecost_http_response_size_bytes_sum{code="200",handler="/healthz",method="GET"} 0
kubecost_http_response_size_bytes_count{code="200",handler="/healthz",method="GET"} 36450
kubecost_http_response_size_bytes_sum{code="200",handler="/metrics",method="GET"} 5.9905781e+07
kubecost_http_response_size_bytes_count{code="200",handler="/metrics",method="GET"} 3644
kubecost_http_response_size_bytes_sum{code="200",handler="/prometheusQuery",method="GET"} 267687
kubecost_http_response_size_bytes_count{code="200",handler="/prometheusQuery",method="GET"} 4249
# HELP kubecost_http_response_time_seconds kubecost_http_response_time_seconds Response time in seconds
# TYPE kubecost_http_response_time_seconds histogram
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="0.001"} 36245
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="0.01"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="0.1"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="0.3"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="0.6"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="1"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="3"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="6"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="9"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="20"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="30"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="60"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="90"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="120"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="240"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="360"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="720"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/healthz",method="GET",le="+Inf"} 36450
kubecost_http_response_time_seconds_sum{code="200",handler="/healthz",method="GET"} 1.9110984170000174
kubecost_http_response_time_seconds_count{code="200",handler="/healthz",method="GET"} 36450
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="0.001"} 0
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="0.01"} 0
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="0.1"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="0.3"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="0.6"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="1"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="3"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="6"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="9"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="20"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="30"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="60"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="90"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="120"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="240"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="360"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="720"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/metrics",method="GET",le="+Inf"} 3644
kubecost_http_response_time_seconds_sum{code="200",handler="/metrics",method="GET"} 53.405258262999915
kubecost_http_response_time_seconds_count{code="200",handler="/metrics",method="GET"} 3644
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="0.001"} 515
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="0.01"} 4152
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="0.1"} 4211
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="0.3"} 4243
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="0.6"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="1"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="3"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="6"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="9"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="20"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="30"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="60"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="90"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="120"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="240"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="360"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="720"} 4249
kubecost_http_response_time_seconds_bucket{code="200",handler="/prometheusQuery",method="GET",le="+Inf"} 4249
kubecost_http_response_time_seconds_sum{code="200",handler="/prometheusQuery",method="GET"} 17.939338128000035
kubecost_http_response_time_seconds_count{code="200",handler="/prometheusQuery",method="GET"} 4249
# HELP kubecost_load_balancer_cost kubecost_load_balancer_cost Hourly cost of load balancer
# TYPE kubecost_load_balancer_cost gauge
kubecost_load_balancer_cost{ingress_ip="35.202.61.109",namespace="ingress-nginx",service_name="ingress-nginx-controller"} 0.025
# HELP kubecost_network_internet_egress_cost kubecost_network_internet_egress_cost Total cost per GB of internet egress.
# TYPE kubecost_network_internet_egress_cost gauge
kubecost_network_internet_egress_cost 0.12
# HELP kubecost_network_region_egress_cost kubecost_network_region_egress_cost Total cost per GB egress across regions
# TYPE kubecost_network_region_egress_cost gauge
kubecost_network_region_egress_cost 0.01
# HELP kubecost_network_zone_egress_cost kubecost_network_zone_egress_cost Total cost per GB egress across zones
# TYPE kubecost_network_zone_egress_cost gauge
kubecost_network_zone_egress_cost 0.01
# HELP kubecost_node_is_spot kubecost_node_is_spot Cloud provider info about node preemptibility
# TYPE kubecost_node_is_spot gauge
kubecost_node_is_spot{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0
kubecost_node_is_spot{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0
# HELP kubecost_pv_info kubecost_pv_info pv info
# TYPE kubecost_pv_info gauge
kubecost_pv_info{persistentvolume="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294",provider_id="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e",provider_id="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-56e397e0-4f93-4d09-8493-5dc644593d33",provider_id="pvc-56e397e0-4f93-4d09-8493-5dc644593d33",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb",provider_id="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148",provider_id="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae",provider_id="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-af084eeb-792c-48f1-8150-885630ace62a",provider_id="pvc-af084eeb-792c-48f1-8150-885630ace62a",storageclass="standard"} 1
kubecost_pv_info{persistentvolume="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669",provider_id="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669",storageclass="standard"} 1
# HELP kubecost_read_db_size size of the read db in bytes
# TYPE kubecost_read_db_size gauge
kubecost_read_db_size{version="nightly"} 0
# HELP kubecost_successful_allocation_ingestions number of successfully ingested allocations
# TYPE kubecost_successful_allocation_ingestions counter
kubecost_successful_allocation_ingestions{version="nightly"} 0
# HELP kubecost_successful_asset_ingestions number of successfully ingested assets
# TYPE kubecost_successful_asset_ingestions counter
kubecost_successful_asset_ingestions{version="nightly"} 0
# HELP kubecost_successful_cloud_cost_ingestions number of successfully ingested cloud costs
# TYPE kubecost_successful_cloud_cost_ingestions counter
kubecost_successful_cloud_cost_ingestions{version="nightly"} 0
# HELP kubecost_successful_custom_cost_ingestions number of successfully ingested custom costs
# TYPE kubecost_successful_custom_cost_ingestions counter
kubecost_successful_custom_cost_ingestions{version="nightly"} 0
# HELP kubecost_successful_network_insight_ingestions number of successfully ingested network insights
# TYPE kubecost_successful_network_insight_ingestions counter
kubecost_successful_network_insight_ingestions{version="nightly"} 0
# HELP kubecost_write_db_size size of the write db in bytes
# TYPE kubecost_write_db_size gauge
kubecost_write_db_size{version="nightly"} 0
# HELP node_cpu_hourly_cost node_cpu_hourly_cost hourly cost for each cpu on this node
# TYPE node_cpu_hourly_cost gauge
node_cpu_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0.021811590000000002
node_cpu_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0.021811590000000002
# HELP node_gpu_count node_gpu_count count of gpu on this node
# TYPE node_gpu_count gauge
node_gpu_count{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0
node_gpu_count{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0
# HELP node_gpu_hourly_cost node_gpu_hourly_cost hourly cost for each gpu on this node
# TYPE node_gpu_hourly_cost gauge
node_gpu_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0
node_gpu_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0
# HELP node_ram_hourly_cost node_ram_hourly_cost hourly cost for each gb of ram on this node
# TYPE node_ram_hourly_cost gauge
node_ram_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0.00292353
node_ram_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0.00292353
# HELP node_total_hourly_cost node_total_hourly_cost Total node cost per hour
# TYPE node_total_hourly_cost gauge
node_total_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-4ric",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-4ric",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-4ric",region="us-central1"} 0.06631302438846588
node_total_hourly_cost{arch="amd64",instance="gke-kc-demo-stage-pool-2-70aa2479-r5b7",instance_type="e2-standard-2",node="gke-kc-demo-stage-pool-2-70aa2479-r5b7",provider_id="gce://guestbook-227502/us-central1-c/gke-kc-demo-stage-pool-2-70aa2479-r5b7",region="us-central1"} 0.06631302438846588
# HELP opencost_build_info opencost_build_info Build information
# TYPE opencost_build_info gauge
opencost_build_info{revision="0a60186",version="nightly"} 0
# HELP pod_pvc_allocation pod_pvc_allocation Bytes used by a PVC attached to a pod
# TYPE pod_pvc_allocation gauge
pod_pvc_allocation{namespace="infra-costmanagement",persistentvolume="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294",persistentvolumeclaim="kc-infra-costmanagement-agent-prometheus-server",pod="kc-infra-costmanagement-agent-prometheus-server-86b88c6f752mrp4"} 3.4359738368e+10
pod_pvc_allocation{namespace="infra-costmanagement",persistentvolume="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e",persistentvolumeclaim="kc-infra-costmanagement-agent-cost-analyzer",pod="kc-infra-costmanagement-agent-cost-analyzer-67655c79fb-dmq4v"} 3.4359738368e+10
pod_pvc_allocation{namespace="kubecost",persistentvolume="pvc-56e397e0-4f93-4d09-8493-5dc644593d33",persistentvolumeclaim="kubecost-prometheus-server",pod="kubecost-prometheus-server-7dd9f9db85-rhx4v"} 3.4359738368e+10
pod_pvc_allocation{namespace="kubecost",persistentvolume="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148",persistentvolumeclaim="kubecost-cost-analyzer",pod="kubecost-cost-analyzer-96947bd8d-g8fjd"} 3.4359738368e+10
pod_pvc_allocation{namespace="pacman",persistentvolume="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669",persistentvolumeclaim="mongo-storage",pod="mongo-766556897-6pnzq"} 1.073741824e+09
# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 641.98
# HELP process_max_fds Maximum number of open file descriptors.
# TYPE process_max_fds gauge
process_max_fds 1.048576e+06
# HELP process_network_receive_bytes_total Number of bytes received by the process over the network.
# TYPE process_network_receive_bytes_total counter
process_network_receive_bytes_total 3.93477347e+08
# HELP process_network_transmit_bytes_total Number of bytes sent by the process over the network.
# TYPE process_network_transmit_bytes_total counter
process_network_transmit_bytes_total 5.00354259e+08
# HELP process_open_fds Number of open file descriptors.
# TYPE process_open_fds gauge
process_open_fds 532
# HELP process_resident_memory_bytes Resident memory size in bytes.
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 1.67825408e+08
# HELP process_start_time_seconds Start time of the process since unix epoch in seconds.
# TYPE process_start_time_seconds gauge
process_start_time_seconds 1.74448597741e+09
# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 2.201174016e+09
# HELP process_virtual_memory_max_bytes Maximum amount of virtual memory available in bytes.
# TYPE process_virtual_memory_max_bytes gauge
process_virtual_memory_max_bytes 1.8446744073709552e+19
# HELP promhttp_metric_handler_requests_in_flight Current number of scrapes being served.
# TYPE promhttp_metric_handler_requests_in_flight gauge
promhttp_metric_handler_requests_in_flight 1
# HELP promhttp_metric_handler_requests_total Total number of scrapes by HTTP status code.
# TYPE promhttp_metric_handler_requests_total counter
promhttp_metric_handler_requests_total{code="200"} 3644
promhttp_metric_handler_requests_total{code="500"} 0
promhttp_metric_handler_requests_total{code="503"} 0
# HELP pv_hourly_cost pv_hourly_cost Cost per GB per hour on a persistent disk
# TYPE pv_hourly_cost gauge
pv_hourly_cost{persistentvolume="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294",provider_id="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294",volumename="pvc-0b0556e3-65f6-4fa0-807f-2cd96bd9b294"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e",provider_id="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e",volumename="pvc-4864123c-b4d5-4a5d-bc7a-eceae8a0bd4e"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-56e397e0-4f93-4d09-8493-5dc644593d33",provider_id="pvc-56e397e0-4f93-4d09-8493-5dc644593d33",volumename="pvc-56e397e0-4f93-4d09-8493-5dc644593d33"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb",provider_id="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb",volumename="pvc-7e6617f9-2247-400d-a0e7-4148a341d0bb"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148",provider_id="gke-kc-demo-stage-ae03-pvc-82c9507f-e4fe-44cd-aef6-34c95478d148",volumename="pvc-82c9507f-e4fe-44cd-aef6-34c95478d148"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae",provider_id="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae",volumename="pvc-ae832588-61bb-47fe-ba9b-e2487e0286ae"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-af084eeb-792c-48f1-8150-885630ace62a",provider_id="pvc-af084eeb-792c-48f1-8150-885630ace62a",volumename="pvc-af084eeb-792c-48f1-8150-885630ace62a"} 5.479452054794521e-05
pv_hourly_cost{persistentvolume="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669",provider_id="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669",volumename="pvc-ec6df12e-b0ec-4748-bbb4-fcea57c4f669"} 5.479452054794521e-05
# HELP service_selector_labels service_selector_labels Service Selector Labels
# TYPE service_selector_labels gauge
service_selector_labels{label_k8s_app="dashboard-metrics-scraper",namespace="kubernetes-dashboard",service="dashboard-metrics-scraper"} 1
service_selector_labels{label_k8s_app="glbc",namespace="kube-system",service="default-http-backend"} 1
service_selector_labels{label_app="kc-infra-costmanagement-agent-network-costs",namespace="infra-costmanagement",service="kc-infra-costmanagement-agent-network-costs"} 1
service_selector_labels{label_component="kube-controller-manager",namespace="kube-system",service="prometheus-operator-kube-p-kube-controller-manager"} 1
service_selector_labels{label_k8s_app="kube-dns",namespace="kube-system",service="kube-dns"} 1
service_selector_labels{label_k8s_app="kube-dns",namespace="kube-system",service="prometheus-operator-kube-p-coredns"} 1
service_selector_labels{label_k8s_app="kube-proxy",namespace="kube-system",service="prometheus-operator-kube-p-kube-proxy"} 1
service_selector_labels{label_component="kube-scheduler",namespace="kube-system",service="prometheus-operator-kube-p-kube-scheduler"} 1
service_selector_labels{label_app="kubecost-cache-proxy",namespace="kubecost",service="kubecost-cache-proxy"} 1
service_selector_labels{label_k8s_app="kubernetes-dashboard",namespace="kubernetes-dashboard",service="kubernetes-dashboard"} 1
service_selector_labels{label_k8s_app="metrics-server",namespace="kube-system",service="metrics-server"} 1
service_selector_labels{label_name="mongo",namespace="pacman",service="mongo"} 1
service_selector_labels{label_app="opencost",namespace="infra-costmanagement",service="opencost"} 1
service_selector_labels{label_app_kubernetes_io_name="prometheus",namespace="monitoring",service="prometheus-operated"} 1
service_selector_labels{label_app="proxy-logger",namespace="opencost",service="proxy-logger"} 1
service_selector_labels{label_app="acmefit",label_service="cart",namespace="acme-fitness",service="cart"} 1
service_selector_labels{label_app="acmefit",label_service="cart-redis",namespace="acme-fitness",service="cart-redis"} 1
service_selector_labels{label_app="acmefit",label_service="catalog",namespace="acme-fitness",service="catalog"} 1
service_selector_labels{label_app="acmefit",label_service="catalog-db",namespace="acme-fitness",service="catalog-mongo"} 1
service_selector_labels{label_app="acmefit",label_service="frontend",namespace="acme-fitness",service="frontend"} 1
service_selector_labels{label_app="acmefit",label_service="order",namespace="acme-fitness",service="order"} 1
service_selector_labels{label_app="acmefit",label_service="order-db",namespace="acme-fitness",service="order-postgres"} 1
service_selector_labels{label_app="acmefit",label_service="payment",namespace="acme-fitness",service="payment"} 1
service_selector_labels{label_app="acmefit",label_service="pos",namespace="acme-fitness",service="pos"} 1
service_selector_labels{label_app="acmefit",label_service="users",namespace="acme-fitness",service="users"} 1
service_selector_labels{label_app="acmefit",label_service="users-mongo",namespace="acme-fitness",service="users-mongo"} 1
service_selector_labels{label_app="acmefit",label_service="users-redis",namespace="acme-fitness",service="users-redis"} 1
service_selector_labels{label_app_kubernetes_io_instance="dcgm",label_app_kubernetes_io_name="dcgm-exporter",namespace="dcgm-exporter",service="dcgm-dcgm-exporter"} 1
service_selector_labels{label_app_kubernetes_io_instance="nginx-org",label_app_kubernetes_io_name="nginx-ingress",namespace="nginx-ingress",service="nginx-org-nginx-ingress-controller"} 1
service_selector_labels{label_app_kubernetes_io_instance="opencost",label_app_kubernetes_io_name="opencost",namespace="opencost",service="opencost"} 1
service_selector_labels{label_app_kubernetes_io_instance="pacman",label_app_kubernetes_io_name="pacman",namespace="pacman",service="pacman"} 1
service_selector_labels{label_app_kubernetes_io_name="prometheus",label_prometheus="prometheus-operator-kube-p-prometheus",namespace="monitoring",service="prometheus-operator-kube-p-prometheus"} 1
service_selector_labels{label_app_kubernetes_io_name="prometheus",label_prometheus="prometheus-operator-kube-p-prometheus",namespace="monitoring",service="prometheus-operator-kube-p-prometheus-thanos"} 1
service_selector_labels{label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="cert-manager",label_app_kubernetes_io_name="cert-manager",namespace="cert-manager",service="cert-manager"} 1
service_selector_labels{label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="ingress-nginx",label_app_kubernetes_io_name="ingress-nginx",namespace="ingress-nginx",service="ingress-nginx-controller"} 1
service_selector_labels{label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="ingress-nginx",label_app_kubernetes_io_name="ingress-nginx",namespace="ingress-nginx",service="ingress-nginx-controller-admission"} 1
service_selector_labels{label_app_kubernetes_io_component="controller",label_app_kubernetes_io_instance="ingress-nginx",label_app_kubernetes_io_name="ingress-nginx",namespace="ingress-nginx",service="ingress-nginx-controller-metrics"} 1
service_selector_labels{label_app="cost-analyzer",label_app_kubernetes_io_instance="kc-infra-costmanagement-agent",label_app_kubernetes_io_name="cost-analyzer",namespace="infra-costmanagement",service="kc-infra-costmanagement-agent-cost-analyzer"} 1
service_selector_labels{label_app="cost-analyzer",label_app_kubernetes_io_instance="kubecost",label_app_kubernetes_io_name="cost-analyzer",namespace="kubecost",service="kubecost-cost-analyzer"} 1
service_selector_labels{label_app_kubernetes_io_component="operator",label_app_kubernetes_io_instance="prometheus-operator",label_app_kubernetes_io_name="kube-prometheus",namespace="monitoring",service="prometheus-operator-kube-p-operator"} 1
service_selector_labels{label_app="prometheus",label_component="server",label_release="kc-infra-costmanagement-agent",namespace="infra-costmanagement",service="kc-infra-costmanagement-agent-prometheus-server"} 1
service_selector_labels{label_app="prometheus",label_component="server",label_release="kubecost",namespace="kubecost",service="kubecost-prometheus-server"} 1
service_selector_labels{label_app_kubernetes_io_component="webhook",label_app_kubernetes_io_instance="cert-manager",label_app_kubernetes_io_name="webhook",namespace="cert-manager",service="cert-manager-webhook"} 1
# HELP statefulSet_match_labels statefulSet_match_labels StatefulSet Match Labels
# TYPE statefulSet_match_labels gauge
statefulSet_match_labels{label_app_kubernetes_io_instance="prometheus-operator-kube-p-prometheus",label_app_kubernetes_io_managed_by="prometheus-operator",label_app_kubernetes_io_name="prometheus",label_operator_prometheus_io_name="prometheus-operator-kube-p-prometheus",label_operator_prometheus_io_shard="0",label_prometheus="prometheus-operator-kube-p-prometheus",namespace="monitoring",statefulSet="prometheus-prometheus-operator-kube-p-prometheus"} 1
`

func TestTargetScraper_Scrape(t *testing.T) {
	tests := []struct {
		name     string
		target   target.ScrapeTarget
		expected []UpdateArgs
	}{
		{
			name:   "Network Scrape",
			target: target.NewStringTarget(networkScape),
			expected: []UpdateArgs{
				{
					metricName: KubecostPodNetworkEgressBytesTotal,
					labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "true",
						"service":     "service1",
					},
					value:                 3127969647,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: KubecostPodNetworkEgressBytesTotal,
					labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "",
					},
					value:                 335188219,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: "kubecost_pod_network_ingress_bytes_total",
					labels: map[string]string{
						"pod_name":    "pod1",
						"namespace":   "namespace1",
						"internet":    "true",
						"same_region": "false",
						"same_zone":   "false",
						"service":     "service1",
					},
					value:                 17941460,
					timestamp:             nil,
					additionalInformation: nil,
				},
				{
					metricName: "kubecost_pod_network_ingress_bytes_total",
					labels: map[string]string{
						"pod_name":    "pod2",
						"namespace":   "namespace1",
						"internet":    "false",
						"same_region": "true",
						"same_zone":   "false",
						"service":     "",
					},
					value:                 13948766,
					timestamp:             nil,
					additionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			scrapper := &TargetScraper{
				targetProvider: NewMockTargetProvider(tt.target),
				collector:      &updateRecorder,
			}
			scrapper.Scrape()

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
