package kubemodel

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