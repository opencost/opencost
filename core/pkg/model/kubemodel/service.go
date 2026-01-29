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

type Service struct {
	UID                  string            `json:"uid"`
	ClusterUID           string            `json:"clusterUid"`
	NamespaceUID         string            `json:"namespaceUid"`
	Name                 string            `json:"name"`
	Type                 ServiceType       `json:"type"`
	Hostname             string            `json:"hostname,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	Ports                []ServicePort     `json:"ports,omitempty"`
	Start                time.Time         `json:"start"`
	End                  time.Time         `json:"end"`
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`
}
