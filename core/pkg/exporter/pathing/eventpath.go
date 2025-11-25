package pathing

import (
	"fmt"
	"path"
	"time"
)

// 2006-01-02T15:04:05Z07:00

// EventStorageTimeFormat is YYYYMMDDHHmmss
const EventStorageTimeFormat = "20060102150405"

// EventStoragePathFormatter is an implementation of the StoragePathFormatter interface for
// a cluster separated storage path of the format:
//
//	<root>/<cluster>/<event>/<sub-paths...>/YYYYMMDDHHmmss
type EventStoragePathFormatter struct {
	rootDir   string
	clusterId string
	event     string
	subPaths  []string
}

// NewEventStoragePathFormatter creates a StoragePathFormatter for a cluster separated storage path
// with the given root directory, cluster id, pipeline, and resolution. To omit the resolution directory
// structure, provide a `nil` resolution.
func NewEventStoragePathFormatter(rootDir, clusterId, event string, subPaths ...string) (StoragePathFormatter[time.Time], error) {
	if clusterId == "" {
		return nil, fmt.Errorf("cluster id cannot be empty")
	}

	if event == "" {
		return nil, fmt.Errorf("event cannot be empty")
	}

	for _, subPath := range subPaths {
		if subPath == "" {
			return nil, fmt.Errorf("subpaths cannot be empty")
		}
	}

	return &EventStoragePathFormatter{
		rootDir:   rootDir,
		clusterId: clusterId,
		event:     event,
		subPaths:  subPaths,
	}, nil
}

// RootDir returns the root directory of the storage path formatter.
func (espf *EventStoragePathFormatter) RootDir() string {
	return espf.rootDir
}

// Dir  returns the director that files will be placed in
func (espf *EventStoragePathFormatter) Dir() string {
	return path.Join(
		espf.rootDir,
		espf.clusterId,
		espf.event,
		path.Join(espf.subPaths...),
	)
}

// ToFullPath returns the full path to a file name within the storage directory using the format:
//
//	<root>/<cluster>/<event>/YYYYMMDDHHmm.json
func (espf *EventStoragePathFormatter) ToFullPath(prefix string, timestamp time.Time, fileExt string) string {
	fileName := toEventFileName(prefix, timestamp, fileExt)

	return path.Join(
		espf.rootDir,
		espf.clusterId,
		espf.event,
		path.Join(espf.subPaths...),
		fileName,
	)
}

// toEventFileName formats the file name as <prefix>.<timestamp>. if a non-empty fileExt is provided,
// then the file extension is appended to the file name.
func toEventFileName(prefix string, timestamp time.Time, fileExt string) string {
	suffix := timestamp.Format(EventStorageTimeFormat)
	if fileExt != "" {
		suffix = fmt.Sprintf("%s.%s", suffix, fileExt)
	}

	if prefix == "" {
		return suffix
	}

	return fmt.Sprintf("%s.%s", prefix, suffix)
}
