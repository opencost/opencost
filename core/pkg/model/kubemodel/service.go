package kubemodel

import "time"

// @bingen:generate:ServiceType
type ServiceType string

const (
	ServiceTypeClusterIP    ServiceType = "ClusterIP"
	ServiceTypeNodePort     ServiceType = "NodePort"
	ServiceTypeLoadBalancer ServiceType = "LoadBalancer"
	ServiceTypeExternalName ServiceType = "ExternalName"
)

// @bingen:generate:ServicePort
type ServicePort struct {
	Name       string `json:"name"`       // @bingen:field[version=1]
	Port       uint16 `json:"port"`       // @bingen:field[version=1]
	TargetPort uint16 `json:"targetPort"` // @bingen:field[version=1]
	NodePort   uint16 `json:"nodePort"`   // @bingen:field[version=1]
	Protocol   string `json:"protocol"`   // @bingen:field[version=1]
}

// @bingen:generate:Service
type Service struct {
	UID                  string            `json:"uid"`                   // @bingen:field[version=1]
	ClusterUID           string            `json:"clusterUid"`            // @bingen:field[version=1]
	NamespaceUID         string            `json:"namespaceUid"`          // @bingen:field[version=1]
	Name                 string            `json:"name"`                  // @bingen:field[version=1]
	Type                 ServiceType       `json:"type"`                  // @bingen:field[version=1]
	Hostname             string            `json:"hostname,omitempty"`    // @bingen:field[version=1]
	Labels               map[string]string `json:"labels,omitempty"`      // @bingen:field[version=1]
	Annotations          map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Ports                []ServicePort     `json:"ports,omitempty"`       // @bingen:field[version=1]
	Start                time.Time         `json:"start"`                 // @bingen:field[version=1]
	End                  time.Time         `json:"end"`                   // @bingen:field[version=1]
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`  // @bingen:field[version=1]
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`   // @bingen:field[version=1]
}
