package kubemodel

import (
	"fmt"
	"time"

	coreexporter "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/log"
	coremodel "github.com/opencost/opencost/core/pkg/model/kubemodel"
	ocexporter "github.com/opencost/opencost/core/pkg/opencost/exporter"
	kmexporter "github.com/opencost/opencost/core/pkg/opencost/exporter/kubemodel"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

const exportInterval = 10 * time.Minute

var (
	janitorInterval    = timeutil.Day
	defaultResolutions = []time.Duration{time.Hour, timeutil.Day}
)

// Pipeline manages the KubeModel export controller group and the retention janitor.
type Pipeline struct {
	controllers *coreexporter.ComputeExportControllerGroup[coremodel.KubeModelSet]
	janitor     *Janitor
}

// NewPipeline creates a Pipeline with the default resolutions (1h, 1d).
func NewPipeline(store storage.Storage, clusterUID string, src kmexporter.KubeModelSource) (*Pipeline, error) {
	return NewPipelineWithResolutions(store, clusterUID, src, defaultResolutions)
}

// NewPipelineWithResolutions creates a Pipeline with the given resolutions.
func NewPipelineWithResolutions(store storage.Storage, clusterUID string, src kmexporter.KubeModelSource, resolutions []time.Duration) (*Pipeline, error) {
	if store == nil {
		return nil, fmt.Errorf("NewKubeModelPipeline: store cannot be nil")
	}
	if clusterUID == "" {
		return nil, fmt.Errorf("NewKubeModelPipeline: clusterUID cannot be empty")
	}

	computeSrc := kmexporter.NewKubeModelComputeSource(src)
	controllers := []*coreexporter.ComputeExportController[coremodel.KubeModelSet]{}

	for _, res := range resolutions {
		ctrl, err := ocexporter.NewComputePipelineExportController[coremodel.KubeModelSet](
			clusterUID, store, computeSrc, res,
		)
		if err != nil {
			log.Errorf("KubeModel pipeline: failed to create controller for resolution %s: %v", timeutil.FormatStoreResolution(res), err)
			continue
		}
		controllers = append(controllers, ctrl)
	}

	return &Pipeline{
		controllers: coreexporter.NewComputeExportControllerGroup(controllers...),
		janitor:     NewJanitor(store, clusterUID, resolutions),
	}, nil
}

// Start launches the export controllers and the retention janitor.
func (p *Pipeline) Start() {
	p.controllers.Start(exportInterval)
	p.janitor.Start(janitorInterval)
}

// Stop halts the export controllers and the retention janitor.
func (p *Pipeline) Stop() {
	p.controllers.Stop()
	p.janitor.Stop()
}
