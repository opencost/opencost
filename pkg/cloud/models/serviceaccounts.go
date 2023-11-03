package models

import "sync"

type ServiceAccountStatus struct {
	Checks []*ServiceAccountCheck `json:"checks"`
}

// ServiceAccountChecks is a thread safe map for holding ServiceAccountCheck objects
type ServiceAccountChecks struct {
	sync.RWMutex
	serviceAccountChecks map[string]*ServiceAccountCheck
}

// NewServiceAccountChecks initialize ServiceAccountChecks
func NewServiceAccountChecks() *ServiceAccountChecks {
	return &ServiceAccountChecks{
		serviceAccountChecks: make(map[string]*ServiceAccountCheck),
	}
}

func (sac *ServiceAccountChecks) Set(key string, check *ServiceAccountCheck) {
	sac.Lock()
	defer sac.Unlock()
	sac.serviceAccountChecks[key] = check
}

// getStatus extracts ServiceAccountCheck objects into a slice and returns them in a ServiceAccountStatus
func (sac *ServiceAccountChecks) GetStatus() *ServiceAccountStatus {
	sac.Lock()
	defer sac.Unlock()
	checks := []*ServiceAccountCheck{}
	for _, v := range sac.serviceAccountChecks {
		checks = append(checks, v)
	}
	return &ServiceAccountStatus{
		Checks: checks,
	}
}

type ServiceAccountCheck struct {
	Message        string `json:"message"`
	Status         bool   `json:"status"`
	AdditionalInfo string `json:"additionalInfo"`
}
