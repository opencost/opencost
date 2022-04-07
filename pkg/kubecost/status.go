package kubecost

import "time"

// ETLStatus describes ETL metadata
type ETLStatus struct {
	Coverage    Window           `json:"coverage"`
	LastRun     time.Time        `json:"lastRun"`
	Progress    float64          `json:"progress"`
	RefreshRate string           `json:"refreshRate"`
	Resolution  string           `json:"resolution"`
	MaxBatch    string           `json:"maxBatch"`
	StartTime   time.Time        `json:"startTime"`
	Backup      *DirectoryStatus `json:"backup,omitempty"`
}

// DirectoryStatus describes metadata of a directory of files
type DirectoryStatus struct {
	Path         string       `json:"path"`
	Size         string       `json:"size"`
	LastModified time.Time    `json:"lastModified"`
	FileCount    int          `json:"fileCount"`
	Files        []FileStatus `json:"files"`
}

// FileStatus describes the metadata of a single file
type FileStatus struct {
	Name         string            `json:"name"`
	Size         string            `json:"size"`
	LastModified time.Time         `json:"lastModified"`
	IsRepairing  bool              `json:"isRepairing"`
	Details      map[string]string `json:"details,omitempty"`
	Errors       []string          `json:"errors,omitempty"`
	Warnings     []string          `json:"warnings,omitempty"`
}

// CloudStatus describes CloudStore metadata
type CloudStatus struct {
	CloudConnectionStatus string              `json:"cloudConnectionStatus"`
	CloudUsage            *CloudProcessStatus `json:"cloudUsage,omitempty"`
	Reconciliation        *CloudProcessStatus `json:"reconciliation,omitempty"`
}

// CloudProcessStatus describes process metadata of a CloudStore
type CloudProcessStatus struct {
	Coverage    Window    `json:"coverage"`
	LastRun     time.Time `json:"lastRun"`
	NextRun     time.Time `json:"nextRun"`
	Progress    float64   `json:"progress"`
	RefreshRate string    `json:"refreshRate"`
	Resolution  string    `json:"resolution"`
	StartTime   time.Time `json:"startTime"`
}
