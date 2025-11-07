package kubemodel

import "time"

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
	ID                   string            `json:"id"`                   // @bingen:field[version=1]
	ClusterID            string            `json:"clusterId"`            // @bingen:field[version=1]
	NamespaceID          string            `json:"namespaceId"`          // @bingen:field[version=1]
	Name                 string            `json:"name"`                 // @bingen:field[version=1]
	Type                 string            `json:"type"`                 // @bingen:field[version=1]
	Labels               map[string]string `json:"labels,omitempty"`     // @bingen:field[version=1]
	Annotations          map[string]string `json:"annotations,omitempty"` // @bingen:field[version=1]
	Ports                []ServicePort     `json:"ports,omitempty"`      // @bingen:field[version=1]
	Start                time.Time         `json:"start"`                // @bingen:field[version=1]
	End                  time.Time         `json:"end"`                  // @bingen:field[version=1]
	NetworkTransferBytes uint64            `json:"networkTransferBytes"` // @bingen:field[version=1]
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`  // @bingen:field[version=1]
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"` // @bingen:field[version=1]
}