package pathing

import (
	"fmt"
	"path"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing/pathutils"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

const (
	DefaultRootDir string = "federated"
	BaseStorageDir string = "etl/bingen"
)

// BingenStoragePathFormatter is an implementation of the StoragePathFormatter interface for
// a cluster separated storage path of the format:
//
//	<root>/<cluster>/etl/bingen/<pipeline>/<resolution>/<epoch-start>-<epoch-end>
type BingenStoragePathFormatter struct {
	rootDir    string
	clusterId  string
	pipeline   string
	resolution string
}

func NewDefaultStoragePathFormatter(clusterId, pipeline string, resolution *time.Duration) (StoragePathFormatter[opencost.Window], error) {
	return NewBingenStoragePathFormatter(DefaultRootDir, clusterId, pipeline, resolution)
}

// NewBingenStoragePathFormatter creates a StoragePathFormatter for a cluster separated storage path
// with the given root directory, cluster id, pipeline, and resolution. To omit the resolution directory
// structure, provide a `nil` resolution.
func NewBingenStoragePathFormatter(rootDir, clusterId, pipeline string, resolution *time.Duration) (StoragePathFormatter[opencost.Window], error) {
	res := "."
	if resolution != nil {
		res = timeutil.FormatStoreResolution(*resolution)
	}

	if clusterId == "" {
		return nil, fmt.Errorf("cluster id cannot be empty")
	}

	if pipeline == "" {
		return nil, fmt.Errorf("pipeline cannot be empty")
	}

	return &BingenStoragePathFormatter{
		rootDir:    rootDir,
		clusterId:  clusterId,
		pipeline:   pipeline,
		resolution: res,
	}, nil
}

// RootDir returns the root directory of the storage path formatter.
func (bsf *BingenStoragePathFormatter) RootDir() string {
	return bsf.rootDir
}

// Dir returns the director that files will be placed in
func (bsf *BingenStoragePathFormatter) Dir() string {
	return path.Join(
		bsf.rootDir,
		bsf.clusterId,
		BaseStorageDir,
		bsf.pipeline,
		bsf.resolution,
	)
}

// ToFullPath returns the full path to a file name within the storage directory using the format:
//
//	<root>/<cluster>/etl/bingen/<pipeline>/<resolution>/<prefix>.<start-epoch>-<end-epoch>
func (bsf *BingenStoragePathFormatter) ToFullPath(prefix string, window opencost.Window, fileExt string) string {
	fileName := toBingenFileName(prefix, window, fileExt)

	return path.Join(
		bsf.rootDir,
		bsf.clusterId,
		BaseStorageDir,
		bsf.pipeline,
		bsf.resolution,
		fileName,
	)
}

// toBingenFileName formats the file name as <prefix>.<start-epoch>-<end-epoch> if a prefix is non-empty.
// If prefix is an empty string, then just the format <start-epoch>-<end-epoch> is returned.
func toBingenFileName(prefix string, window opencost.Window, fileExt string) string {
	start, end := derefTimeOrZero(window.Start()), derefTimeOrZero(window.End())

	suffix := pathutils.FormatEpochRange(start, end)
	if fileExt != "" {
		suffix = fmt.Sprintf("%s.%s", suffix, fileExt)
	}

	if prefix == "" {
		return suffix
	}

	return fmt.Sprintf("%s.%s", prefix, suffix)
}

// derefTimeOrZero dereferences a time.Time pointer and returns the zero value if the pointer is nil.
// This prevents nil pointer dereference errors when using windows. This is mostly an assertion, as
// generally windows for pathing will be pre-validated.
func derefTimeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
