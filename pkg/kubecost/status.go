package kubecost

import "time"

// ETLStatus describes ETL metadata
type ETLStatus struct {
	LastRun                    time.Time        `json:"lastRun"`
	StartTime                  time.Time        `json:"startTime"`
	Coverage                   Window           `json:"coverage"`
	Backup                     *DirectoryStatus `json:"backup,omitempty"`
	RefreshRate                string           `json:"refreshRate"`
	Resolution                 string           `json:"resolution"`
	MaxPrometheusQueryDuration string           `json:"maxPrometheusQueryDuration"`
	UTCOffset                  string           `json:"utcOffset"`
	Progress                   float64          `json:"progress"`
}

// DirectoryStatus describes metadata of a directory of files
type DirectoryStatus struct {
	LastModified time.Time    `json:"lastModified"`
	Path         string       `json:"path"`
	Size         string       `json:"size"`
	Files        []FileStatus `json:"files"`
	FileCount    int          `json:"fileCount"`
}

// FileStatus describes the metadata of a single file
type FileStatus struct {
	LastModified time.Time         `json:"lastModified"`
	Details      map[string]string `json:"details,omitempty"`
	Name         string            `json:"name"`
	Size         string            `json:"size"`
	Errors       []string          `json:"errors,omitempty"`
	Warnings     []string          `json:"warnings,omitempty"`
	IsRepairing  bool              `json:"isRepairing"`
}

// CloudStatus describes CloudStore metadata
type CloudStatus struct {
	CloudUsage       *CloudAssetStatus     `json:"cloudUsage,omitempty"`
	Reconciliation   *ReconciliationStatus `json:"reconciliation,omitempty"`
	ConnectionStatus string                `json:"cloudConnectionStatus"`
	ProviderType     string                `json:"providerType"`
}

// CloudAssetStatus describes CloudAsset metadata of a CloudStore
type CloudAssetStatus struct {
	LastRun     time.Time `json:"lastRun"`
	NextRun     time.Time `json:"nextRun"`
	StartTime   time.Time `json:"startTime"`
	Coverage    Window    `json:"coverage"`
	RefreshRate string    `json:"refreshRate"`
	Resolution  string    `json:"resolution"`
	Progress    float64   `json:"progress"`
}

// ReconciliationStatus describes Reconciliation metadata of a CloudStore
type ReconciliationStatus struct {
	LastRun     time.Time `json:"lastRun"`
	NextRun     time.Time `json:"nextRun"`
	StartTime   time.Time `json:"startTime"`
	Coverage    Window    `json:"coverage"`
	RefreshRate string    `json:"refreshRate"`
	Resolution  string    `json:"resolution"`
	Progress    float64   `json:"progress"`
}
