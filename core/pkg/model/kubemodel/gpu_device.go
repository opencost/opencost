package kubemodel

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