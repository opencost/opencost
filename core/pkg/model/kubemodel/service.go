package kubemodel

import (
	"fmt"
	"strings"
	"time"
)

// @bingen:generate:ServiceType
type ServiceType string

const (
	ServiceTypeClusterIP    ServiceType = "ClusterIP"
	ServiceTypeNodePort     ServiceType = "NodePort"
	ServiceTypeLoadBalancer ServiceType = "LoadBalancer"
	ServiceTypeExternalName ServiceType = "ExternalName"
)

// ParseServiceType converts a string to a ServiceType, performing case-insensitive matching.
// Returns ServiceTypeClusterIP (the default) if the service type string is not recognized.
func ParseServiceType(serviceType string) ServiceType {
	switch strings.ToLower(serviceType) {
	case "clusterip":
		return ServiceTypeClusterIP
	case "nodeport":
		return ServiceTypeNodePort
	case "loadbalancer", "lb":
		return ServiceTypeLoadBalancer
	case "externalname":
		return ServiceTypeExternalName
	default:
		return ServiceTypeClusterIP
	}
}

// @bingen:generate:Service
type Service struct {
	UID          string      `json:"uid"`
	NamespaceUID string      `json:"namespaceUid"`
	Name         string      `json:"name"`
	Type         ServiceType `json:"type"`
	Start        time.Time   `json:"start"`
	End          time.Time   `json:"end"`
	// Label selector to identify pods/containers targeted by this service
	// Maps label keys to values (e.g., {"app": "nginx", "tier": "frontend"})
	// Pods with matching labels will receive traffic from this service
	Selector map[string]string `json:"selector,omitempty"`
}

func (kms *KubeModelSet) RegisterService(service *Service) error {
	// Check required fields
	if service.UID == "" {
		err := fmt.Errorf("UID is missing for Service with name '%s'", service.Name)
		kms.Error(err)
		return err
	}

	if service.Name == "" {
		err := fmt.Errorf("Name is missing for Service '%s'", service.UID)
		kms.Error(err)
		return err
	}

	if service.NamespaceUID == "" {
		err := fmt.Errorf("NamespaceUID is missing for Service '%s'", service.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, service.Start, service.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.Services[service.UID]; !ok {
		kms.Services[service.UID] = service
		kms.Metadata.ObjectCount++
	}

	return nil
}
