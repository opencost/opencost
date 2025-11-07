package kubemodel

import (
	"time"
)

// ServicePort represents a service port
type ServicePort struct {
	Name       string `json:"name"`
	Port       uint16 `json:"port"`
	TargetPort uint16 `json:"targetPort"`
	NodePort   uint16 `json:"nodePort"`
	Protocol   string `json:"protocol"`
}

// Service represents a Kubernetes service
type Service struct {
	ID                   string            `json:"id"`
	ClusterID            string            `json:"clusterId"`
	NamespaceID          string            `json:"namespaceId"`
	Name                 string            `json:"name"`
	Type                 string            `json:"type"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	Ports                []ServicePort     `json:"ports,omitempty"`
	Start                *time.Time        `json:"start,omitempty"`
	End                  *time.Time        `json:"end,omitempty"`
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}