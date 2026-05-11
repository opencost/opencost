package kubemodel

import (
	"fmt"
	"time"

	ocexporter "github.com/opencost/opencost/core/pkg/opencost/exporter"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

var (
	exportInterval     = 5 * time.Minute
	janitorInterval    = timeutil.Day
	defaultResolutions = []time.Duration{time.Hour, timeutil.Day}
)

// Pipeline manages the KubeModel export controller group and the retention janitor.
type Pipeline struct {
	controllers *ocexporter.PipelineExportControllers
	janitor     *Janitor
}

// NewPipeline creates a new pipeline with preset settings
func NewPipeline(appName, clusterUID string, store storage.Storage, cm ocexporter.ComputePipelineSource) (*Pipeline, error) {
	if store == nil {
		return nil, fmt.Errorf("NewPipeline: store cannot be nil")
	}
	if clusterUID == "" {
		return nil, fmt.Errorf("NewPipeline: clusterUID cannot be empty")
	}

	config := ocexporter.PipelinesExportConfig{
		AppName:                      appName,
		ClusterUID:                   clusterUID,
		KubeModelPipelineResolutions: defaultResolutions,
	}

	controllers := ocexporter.NewPipelineExportControllers(store, cm, config)

	return &Pipeline{
		controllers: controllers,
		janitor:     NewJanitor(store, appName, clusterUID, config.KubeModelPipelineResolutions),
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
