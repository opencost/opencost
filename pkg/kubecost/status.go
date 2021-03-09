package kubecost

import "time"

// ETLStatus describes ETL metadata
type ETLStatus struct {
	Coverage    Window           `json:"coverage"`
	LastRun     time.Time        `json:"lastRun"`
	Progress    float64          `json:"progress"`
	RefreshRate string           `json:"refreshRate"`
	Resolution  time.Duration    `json:"resolution"`
	MaxBatch    time.Duration    `json:"maxBatch"`
	StartTime   time.Time        `json:"startTime"`
	UTCOffset   string           `json:"utcOffset"`
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
	Details      map[string]string `json:"details,omitempty"`
	Errors       []string          `json:"errors,omitempty"`
	Warnings     []string          `json:"warnings,omitempty"`
}
