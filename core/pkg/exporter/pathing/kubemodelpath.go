package pathing

import (
	"fmt"
	"path"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
)

const KubeModelStorageTimeFormat = "20060102150405"

// KubeModelStoragePathFormatter is an implementation of the StoragePathFormatter interface for
// a cluster separated storage path of the format:
//
//	<root>/<clusterid>/kubemodel/<resolution>/<YYYYMMDDHHiiSS>
//
// where <root> is, e.g., s3://<bucket>/<appid>
type KubeModelStoragePathFormatter struct {
	rootDir    string
	clusterId  string
	resolution string
}

func NewKubeModelStoragePathFormatter(rootDir, clusterId, resolution string) (StoragePathFormatter[opencost.Window], error) {
	if clusterId == "" {
		return nil, fmt.Errorf("cluster id cannot be empty")
	}

	return &KubeModelStoragePathFormatter{
		rootDir:    rootDir,
		clusterId:  clusterId,
		resolution: resolution,
	}, nil
}

// RootDir returns the root directory of the storage path formatter.
func (kmspf *KubeModelStoragePathFormatter) RootDir() string {
	return kmspf.rootDir
}

// Dir returns the director that files will be placed in
func (kmspf *KubeModelStoragePathFormatter) Dir() string {
	return path.Join(
		kmspf.rootDir,
		kmspf.clusterId,
		pipelines.KubeModelPipelineName,
		kmspf.resolution,
	)
}

// ToFullPath returns the full path to a file name within the storage directory using the format:
//
//	<root>/<clusterid>/kubemodel/<resolution>/<prefix>.<YYYYMMDDHHiiSS>.<fileExt>
func (kmspf *KubeModelStoragePathFormatter) ToFullPath(prefix string, window opencost.Window, fileExt string) string {
	return path.Join(
		kmspf.rootDir,
		kmspf.clusterId,
		pipelines.KubeModelPipelineName,
		kmspf.resolution,
		toKubeModelFileName(prefix, window.Start(), fileExt),
	)
}

func toKubeModelFileName(prefix string, start *time.Time, fileExt string) string {
	filename := derefTimeOrZero(start).Format(KubeModelStorageTimeFormat)

	if fileExt != "" {
		filename = fmt.Sprintf("%s.%s", filename, fileExt)
	}

	if prefix == "" {
		return filename
	}

	return fmt.Sprintf("%s.%s", prefix, filename)
}
