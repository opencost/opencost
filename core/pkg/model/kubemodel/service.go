package kubemodel

import "time"

type ServiceType string

const (
	ServiceTypeClusterIP    ServiceType = "ClusterIP"
	ServiceTypeNodePort     ServiceType = "NodePort"
	ServiceTypeLoadBalancer ServiceType = "LoadBalancer"
	ServiceTypeExternalName ServiceType = "ExternalName"
)

type ServicePort struct {
	Name       string `json:"name"`
	Port       uint16 `json:"port"`
	TargetPort uint16 `json:"targetPort"`
	NodePort   uint16 `json:"nodePort"`
	Protocol   string `json:"protocol"`
}

// @bingen:generate:Service
// Service represents a Kubernetes Service with network traffic tracking for cost allocation.
//
// Network Cost Allocation Strategy:
// Services expose applications and route traffic, incurring costs for:
// 1. Load Balancers (LoadBalancer type) - Cloud provider LB hourly cost + data transfer
// 2. Data Transfer - Egress charges based on NetworkTransferBytes
// 3. Public IPs (for LoadBalancer/NodePort with external IPs)
//
// Cost Attribution Flow:
// - LoadBalancer Services: Direct cloud resource cost (e.g., AWS ELB, GCP LB) allocated to service
// - Data Transfer: NetworkTransferBytes × cloud provider egress rate (varies by region/destination)
// - NetworkReceiveBytes: Typically free (ingress), tracked for visibility
// - Use Selector to map service costs to backing pods/containers proportionally
//
// Example: AWS Application Load Balancer
// - Fixed hourly cost: $0.0225/hour
// - LCU cost: $0.008/hour per LCU (based on connections, requests, bandwidth)
// - Data transfer: $0.09/GB for internet egress
// Total Service Cost = (LB hours × hourly rate) + (LCU hours × LCU rate) + (NetworkTransferBytes × transfer rate)
type Service struct {
	UID                  string            `json:"uid"`
	NamespaceUID         string            `json:"namespaceUid"`
	Name                 string            `json:"name"`
	Type                 ServiceType       `json:"type"`
	Hostname             string            `json:"hostname,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	Ports                []ServicePort     `json:"ports,omitempty"`
	Start                time.Time         `json:"start"`
	End                  time.Time         `json:"end"`
	NetworkTransferBytes Measurement       `json:"networkTransferBytes"`
	NetworkReceiveBytes  Measurement       `json:"networkReceiveBytes"`
	// Label selector to identify pods/containers targeted by this service
	// Maps label keys to values (e.g., {"app": "nginx", "tier": "frontend"})
	// Pods with matching labels will receive traffic from this service
	Selector map[string]string `json:"selector,omitempty"`
	// Lifecycle tracking
	DurationSeconds Measurement `json:"durationSeconds"` // Duration service existed within measurement window
}
