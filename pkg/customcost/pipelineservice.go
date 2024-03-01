package customcost

import (
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/go-plugin"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
	proto "github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/pkg/env"
)

var protocol = proto.HTTP()

// PipelineService exposes CloudCost pipeline controls and diagnostics endpoints
type PipelineService struct {
	hourlyIngestor, dailyIngestor *CustomCostIngestor
	hourlyStore, dailyStore       Repository
}

func getRegisteredPlugins(configDir string, execDir string) map[string]*plugin.Client {
	plugins := map[string]*plugin.Client{}
	return plugins
}

// NewPipelineService is a constructor for a PipelineService
func NewPipelineService(hourlyrepo, dailyrepo Repository, ingConf CustomCostIngestorConfig) (*PipelineService, error) {

	registeredPlugins := getRegisteredPlugins(ingConf.PluginConfigDir, ingConf.PluginExecutableDir)

	hourlyIngestor, err := NewCustomCostIngestor(&ingConf, hourlyrepo, registeredPlugins)
	if err != nil {
		return nil, err
	}

	hourlyIngestor.Start(false)

	dailyIngestor, err := NewCustomCostIngestor(&ingConf, dailyrepo, registeredPlugins)
	if err != nil {
		return nil, err
	}

	dailyIngestor.Start(false)
	return &PipelineService{
		hourlyIngestor: hourlyIngestor,
		hourlyStore:    hourlyrepo,
		dailyStore:     dailyrepo,
		dailyIngestor:  dailyIngestor,
	}, nil
}

// Status gives a combined view of the state of configs and the ingestior status
func (dp *PipelineService) Status() Status {

	// Pull config status from the config controller
	ingstatus := dp.hourlyIngestor.Status()
	dur, err := time.ParseDuration(env.GetCustomCostRefreshRateHours())
	if err != nil {
		log.Errorf("error parsing duration %s: %v", env.GetCustomCostRefreshRateHours(), err)
		return Status{}
	}
	refreshRate := time.Hour * dur

	// These are the statuses
	return Status{
		Coverage:    ingstatus.Coverage.String(),
		RefreshRate: refreshRate.String(),
	}

}

// GetCloudCostRebuildHandler creates a handler from a http request which initiates a rebuild of cloud cost pipeline, if an
// integrationKey is provided then it only rebuilds the specified billing integration
func (s *PipelineService) GetCloudCostRebuildHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If pipeline Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service is nil", http.StatusNotImplemented)
		}
	}
	if s.dailyIngestor == nil || s.hourlyIngestor == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service Ingestion Manager is nil", http.StatusNotImplemented)
		}
	}
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		commit := r.URL.Query().Get("commit") == "true" || r.URL.Query().Get("commit") == "1"

		if !commit {
			protocol.WriteData(w, "Pass parameter 'commit=true' to confirm Cloud Cost rebuild")
			return
		}

		domain := r.URL.Query().Get("domain")

		err := s.hourlyIngestor.Rebuild(domain)
		if err != nil {
			log.Errorf("error rebuilding hourly ingestor")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.dailyIngestor.Rebuild(domain)
		if err != nil {
			log.Errorf("error rebuilding daily ingestor")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, fmt.Sprintf("Rebuilding Custom Cost For Domain %s", domain))
		return

	}
}

// GetCloudCostStatusHandler creates a handler from a http request which returns a list of the billing integration status
func (s *PipelineService) GetCustomCostStatusHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If Reporting Service is nil, always return 501
	if s == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Reporting Service is nil", http.StatusNotImplemented)
		}
	}
	if s.hourlyIngestor == nil || s.dailyIngestor == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "Custom Cost Pipeline Service Ingestor is nil", http.StatusNotImplemented)
		}
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		protocol.WriteData(w, s.Status())
	}
}
