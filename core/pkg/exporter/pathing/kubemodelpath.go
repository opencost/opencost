package pathing

import (
	"fmt"
	"path"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
)

const (
	KubeModelDateDirTimeFormat = "2006/01/02"
	KubeModelStorageTimeFormat = "20060102150405"
)

// KubeModelStoragePathFormatter is an implementation of the StoragePathFormatter interface for
// a cluster separated storage path of the format:
//
//	<root>/<clusterid>/kubemodel/<resolution>/<YYYY>/<MM>/<DD>/<YYYYMMDDHHiiSS>
//
// where <root> is, e.g., s3://<bucket>/<appid>
type KubeModelStoragePathFormatter struct {
	dir string
}

func NewKubeModelStoragePathFormatter(rootDir, clusterId, resolution string) (StoragePathFormatter[opencost.Window], error) {
	if clusterId == "" {
		return nil, fmt.Errorf("cluster id cannot be empty")
	}

	return &KubeModelStoragePathFormatter{
		dir: path.Join(
			rootDir,
			clusterId,
			pipelines.KubeModelPipelineName,
			resolution,
		),
	}, nil
}

// Dir returns the director that files will be placed in
func (kmspf *KubeModelStoragePathFormatter) Dir() string {
	return kmspf.dir
}

// ToFullPath returns the full path to a file name within the storage directory using the format:
//
//	<root>/<clusterid>/kubemodel/<resolution>/<YYYY>/<MM>/<DD>/<prefix>.<YYYYMMDDHHiiSS>.<fileExt>
func (kmspf *KubeModelStoragePathFormatter) ToFullPath(prefix string, window opencost.Window, fileExt string) string {
	return path.Join(
		kmspf.dir,
		window.Start().Format(KubeModelDateDirTimeFormat),
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
