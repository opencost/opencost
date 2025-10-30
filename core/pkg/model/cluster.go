package model

import (
	"time"
)

// Provider represents the cloud provider or infrastructure type
type Provider string

const (
	ProviderAWS          Provider = "aws"
	ProviderGCP          Provider = "gcp"
	ProviderAzure        Provider = "azure"
	ProviderOnPremises   Provider = "on_premises"
	ProviderAlibaba      Provider = "alibaba"
	ProviderDigitalOcean Provider = "digitalocean"
	ProviderOracle       Provider = "oracle"
)

// Resolution represents the time granularity for data aggregation
type Resolution string

const (
	Resolution10M Resolution = "10m" // 10 minutes
	Resolution1H  Resolution = "1h"  // 1 hour
	Resolution1D  Resolution = "1d"  // 1 day
)

// ControllerKind represents the type of Kubernetes controller
type ControllerKind string

const (
	ControllerKindDeployment  ControllerKind = "deployment"
	ControllerKindStatefulSet ControllerKind = "statefulset"
	ControllerKindDaemonSet   ControllerKind = "daemonset"
	ControllerKindJob         ControllerKind = "job"
	ControllerKindCronJob     ControllerKind = "cronjob"
	ControllerKindReplicaSet  ControllerKind = "replicaset"
)

// Window defines a unit of time by a resolution and a start time
type Window struct {
	Resolution Resolution `json:"resolution"`
	Start      time.Time  `json:"start"`
}

// Container represents a container within a pod (allocated resource)
type Container struct {
	PodID                  string            `json:"podId"`
	Name                   string            `json:"name"`
	CreationTime           *time.Time        `json:"creationTime,omitempty"`
	DeletionTime           *time.Time        `json:"deletionTime,omitempty"`
	CpuCoreHours           float64           `json:"cpuCoreHours"`
	CpuCoreRequestAverage  float64           `json:"cpuCoreRequestAverage"`
	CpuCoreUsageAverage    float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax        float64           `json:"cpuCoreUsageMax"`
	RamByteHours              uint64            `json:"ramByteHours"`
	RamBytesRequestAverage    uint64            `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage      uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax          uint64            `json:"ramBytesUsageMax"`
	StorageByteHours          uint64            `json:"storageByteHours"`
	StorageBytesRequestAverage uint64           `json:"storageBytesRequestAverage"`
	StorageBytesUsageAverage  uint64            `json:"storageBytesUsageAverage"`
	StorageBytesUsageMax      uint64            `json:"storageBytesUsageMax"`
	GpuUsages                 []GPUUsage        `json:"gpuUsages,omitempty"`
	Diagnostic                *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Controller represents a Kubernetes workload controller
type Controller struct {
	ID           string            `json:"id"`
	NamespaceID  string            `json:"namespaceId"`
	Name         string            `json:"name"`
	Kind         ControllerKind    `json:"kind"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	CreationTime *time.Time        `json:"creationTime,omitempty"`
	DeletionTime *time.Time        `json:"deletionTime,omitempty"`
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`
}

// DiagnosticResult represents the result of a diagnostic run
type DiagnosticResult struct {
	// Unique Identifier for the diagnostic run result
	ID string `json:"id"`

	// Name of the diagnostic that ran
	Name string `json:"name"`

	// Description of the diagnostic run, human readable description
	Description string `json:"description"`

	// Category of the diagnostic run, used to group similar diagnostics
	Category string `json:"category"`

	// Timestamp when the diagnostic run was executed
	Timestamp time.Time `json:"timestamp"`

	// Error message if the diagnostic run failed
	Error string `json:"error,omitempty"`

	// Details contains additional custom information about the diagnostic run
	Details map[string]any `json:"details,omitempty"`
}

// GPUDevice represents a GPU device
type GPUDevice struct {
	ID                string            `json:"id"`
	NodeID            string            `json:"nodeId"`
	DeviceNumber      uint64            `json:"deviceNumber"`
	ModelName         string            `json:"modelName"`
	IsShared          bool              `json:"isShared"`
	SharePercentage   float64           `json:"sharePercentage"`
	GpuHours          float64           `json:"gpuHours"`
	GpuRequestAverage float64           `json:"gpuRequestAverage"`
	GpuUsageAverage   float64           `json:"gpuUsageAverage"`
	GpuUsageMax       float64           `json:"gpuUsageMax"`
	MemoryBytes       uint64            `json:"memoryBytes"`
	Diagnostic        *DiagnosticResult `json:"diagnostic,omitempty"`
}

// GPUUsage represents GPU usage metrics for a container
type GPUUsage struct {
	GpuDeviceID          string            `json:"gpuDeviceId"`
	GpuHours             float64           `json:"gpuHours"`
	GpuRequestPercentage float64           `json:"gpuRequestPercentage"`
	GpuUsageAverage      float64           `json:"gpuUsageAverage"`
	GpuUsageMax          float64           `json:"gpuUsageMax"`
	MemoryBytesUsed      uint64            `json:"memoryBytesUsed"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Namespace represents a Kubernetes namespace
type Namespace struct {
	ID           string            `json:"id"`
	ClusterID    string            `json:"clusterId"`
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	CreationTime *time.Time        `json:"creationTime,omitempty"`
	DeletionTime *time.Time        `json:"deletionTime,omitempty"`
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`
}

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
	CreationTime         *time.Time        `json:"creationTime,omitempty"`
	DeletionTime         *time.Time        `json:"deletionTime,omitempty"`
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Node represents a Kubernetes node
type Node struct {
	ID                   string            `json:"id"`
	ClusterID            string            `json:"clusterId"`
	ProviderResourceID   string            `json:"providerResourceId"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	CreationTime         *time.Time        `json:"creationTime,omitempty"`
	DeletionTime         *time.Time        `json:"deletionTime,omitempty"`
	CpuCores             uint64            `json:"cpuCores"`
	RamBytes             uint64            `json:"ramBytes"`
	CpuCost              float64           `json:"cpuCost"`
	RamCost              float64           `json:"ramCost"`
	GpuCost              float64           `json:"gpuCost"`
	CpuCoreUsageAverage  float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax      float64           `json:"cpuCoreUsageMax"`
	RamBytesUsageAverage uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax     uint64            `json:"ramBytesUsageMax"`
	Diagnostic           *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Pod represents a Kubernetes pod
type Pod struct {
	ID                     string            `json:"id"`
	NamespaceID            string            `json:"namespaceId"`
	ControllerID           string            `json:"controllerId"`
	NodeID                 string            `json:"nodeId"`
	Name                   string            `json:"name"`
	Labels                 map[string]string `json:"labels,omitempty"`
	Annotations            map[string]string `json:"annotations,omitempty"`
	CreationTime           *time.Time        `json:"creationTime,omitempty"`
	DeletionTime           *time.Time        `json:"deletionTime,omitempty"`
	CpuCoreHours           float64           `json:"cpuCoreHours"`
	CpuCoreRequestAverage  float64           `json:"cpuCoreRequestAverage"`
	CpuCoreUsageAverage    float64           `json:"cpuCoreUsageAverage"`
	CpuCoreUsageMax        float64           `json:"cpuCoreUsageMax"`
	RamByteHours           uint64            `json:"ramByteHours"`
	RamBytesRequestAverage uint64            `json:"ramBytesRequestAverage"`
	RamBytesUsageAverage   uint64            `json:"ramBytesUsageAverage"`
	RamBytesUsageMax       uint64            `json:"ramBytesUsageMax"`
	StorageByteHours       uint64            `json:"storageByteHours"`
	NetworkTransferBytes   uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes    uint64            `json:"networkReceiveBytes"`
	Diagnostic             *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Volume represents a Kubernetes volume
type Volume struct {
	ID           string            `json:"id"`
	ClusterID    string            `json:"clusterId"`
	Name         string            `json:"name"`
	Namespace    string            `json:"namespace"`
	Labels       map[string]string `json:"labels,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	CreationTime *time.Time        `json:"creationTime,omitempty"`
	DeletionTime *time.Time        `json:"deletionTime,omitempty"`
	StorageClass string            `json:"storageClass"`
	Size         uint64            `json:"size"`
	Cost         float64           `json:"cost"`
	Diagnostic   *DiagnosticResult `json:"diagnostic,omitempty"`
}

// PersistentVolumeClaim represents a Kubernetes persistent volume claim
type PersistentVolumeClaim struct {
	ID               string            `json:"id"`
	NamespaceID      string            `json:"namespaceId"`
	VolumeID         *string           `json:"volumeId,omitempty"`
	PodID            *string           `json:"podId,omitempty"`
	Name             string            `json:"name"`
	Labels           map[string]string `json:"labels,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	CreationTime     *time.Time        `json:"creationTime,omitempty"`
	DeletionTime     *time.Time        `json:"deletionTime,omitempty"`
	StorageClass     string            `json:"storageClass"`
	StorageByteHours uint64            `json:"storageByteHours"`
	RequestedBytes   uint64            `json:"requestedBytes"`
	Size             uint64            `json:"size"`
	VolumeName       string            `json:"volumeName"`
	Diagnostic       *DiagnosticResult `json:"diagnostic,omitempty"`
}

// Cluster represents the top-level Kubernetes cluster
type Cluster struct {
	ID       string   `json:"id"`
	Provider Provider `json:"provider"`
	Account  string   `json:"account"`
	Name     string   `json:"name"`
	Window   *Window  `json:"window,omitempty"`

	// Resources within the cluster
	Containers             []Container             `json:"containers,omitempty"`
	Controllers            []Controller            `json:"controllers,omitempty"`
	GpuDevices             []GPUDevice             `json:"gpuDevices,omitempty"`
	Namespaces             []Namespace             `json:"namespaces,omitempty"`
	Services               []Service               `json:"services,omitempty"`
	Nodes                  []Node                  `json:"nodes,omitempty"`
	Pods                   []Pod                   `json:"pods,omitempty"`
	Volumes                []Volume                `json:"volumes,omitempty"`
	PersistentVolumeClaims []PersistentVolumeClaim `json:"persistentVolumeClaims,omitempty"`
}
