package pricingmodel

import (
	"net/http"

	proto "github.com/opencost/opencost/core/pkg/protocol"
)

var protocol = proto.HTTP()

// PipelineService exposes HTTP handlers for controlling and observing the pricing model pipeline.
type PipelineService struct {
	pipeline *Pipeline
}

// NewPipelineService creates a PipelineService wrapping the given Pipeline.
func NewPipelineService(pipeline *Pipeline) *PipelineService {
	return &PipelineService{pipeline: pipeline}
}

// GetStatusHandler returns an HTTP handler that serializes the status of all runners.
func (s *PipelineService) GetStatusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, s.pipeline.Status())
	}
}

// GetRebuildHandler returns an HTTP handler that triggers an immediate export
// outside the scheduled tick. If the "sourceKey" query parameter is provided,
// only that source is rebuilt; otherwise all sources are rebuilt.
func (s *PipelineService) GetRebuildHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sourceKey := r.URL.Query().Get("sourceKey")
		if sourceKey == "" {
			s.pipeline.Rebuild()
			protocol.WriteData(w, "Rebuild triggered for all pricing sources")
			return
		}
		if err := s.pipeline.RebuildSource(sourceKey); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, "Rebuild triggered for source: "+sourceKey)
	}
}