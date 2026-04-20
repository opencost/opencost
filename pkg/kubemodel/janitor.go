package kubemodel

import (
	"path"
	"sync/atomic"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

const (
	janitorDefault1dRetention  = 30 // days
	janitorDefault1hRetention  = 49 // hours
	janitorDefault10mRetention = 36 // 10-minute segments (6 hours)
)

// Janitor removes KubeModelSet files from storage that exceed the retention period for each resolution.
// Retention durations are read from the standard RESOLUTION_*_RETENTION env vars.
type Janitor struct {
	store       storage.Storage
	clusterId   string
	resolutions []time.Duration
	isRunning   atomic.Bool
	isStopping  atomic.Bool
	exitCh      chan struct{}
}

// NewJanitor creates a Janitor for the given storage backend, cluster, and active resolutions.
func NewJanitor(store storage.Storage, clusterId string, resolutions []time.Duration) *Janitor {
	return &Janitor{
		store:       store,
		clusterId:   clusterId,
		resolutions: resolutions,
	}
}

// Start launches the background retention cleanup loop. No-op if already running.
func (j *Janitor) Start(interval time.Duration) {
	if !j.isRunning.CompareAndSwap(false, true) {
		return
	}
	j.exitCh = make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-j.exitCh:
				j.isRunning.Store(false)
				j.isStopping.Store(false)
				return
			case <-ticker.C:
				j.cleanup()
			}
		}
	}()
}

// Stop halts the cleanup loop.
func (j *Janitor) Stop() {
	if !j.isStopping.CompareAndSwap(false, true) {
		return
	}
	close(j.exitCh)
}

func (j *Janitor) cleanup() {
	for _, res := range j.resolutions {
		retention := retentionFor(res)
		if retention == 0 {
			continue
		}
		resStr := timeutil.FormatStoreResolution(res)
		baseDir := path.Join(pathing.FinOpsAgentAppID, j.clusterId, pipelines.KubeModelPipelineName, resStr)
		cutoff := time.Now().UTC().Add(-retention).Truncate(timeutil.Day)
		j.pruneResolution(baseDir, cutoff)
	}
}

// retentionFor returns the total retention duration for a given export resolution, using the
// standard RESOLUTION_*_RETENTION env vars (without a prefix, so the same values apply across pipelines).
func retentionFor(res time.Duration) time.Duration {
	switch {
	case res >= timeutil.Day:
		n := coreenv.GetInt(coreenv.Resolution1dRetentionEnvVar, janitorDefault1dRetention)
		return timeutil.Day * time.Duration(n)
	case res >= time.Hour:
		n := coreenv.GetInt(coreenv.Resolution1hRetentionEnvVar, janitorDefault1hRetention)
		return time.Hour * time.Duration(n)
	default:
		n := coreenv.GetInt(coreenv.Resolution10mRetentionEnvVar, janitorDefault10mRetention)
		return 10 * time.Minute * time.Duration(n)
	}
}

func (j *Janitor) pruneResolution(baseDir string, cutoff time.Time) {
	years, err := j.store.ListDirectories(baseDir)
	if err != nil {
		if !storage.IsNotExist(err) {
			log.Errorf("KubeModel janitor: listing %s: %v", baseDir, err)
		}
		return
	}
	for _, yearInfo := range years {
		yearDir := path.Join(baseDir, yearInfo.Name)
		months, err := j.store.ListDirectories(yearDir)
		if err != nil {
			log.Warnf("KubeModel janitor: listing %s: %v", yearDir, err)
			continue
		}
		for _, monthInfo := range months {
			monthDir := path.Join(yearDir, monthInfo.Name)
			days, err := j.store.ListDirectories(monthDir)
			if err != nil {
				log.Warnf("KubeModel janitor: listing %s: %v", monthDir, err)
				continue
			}
			for _, dayInfo := range days {
				dayDir := path.Join(monthDir, dayInfo.Name)
				dateStr := yearInfo.Name + "/" + monthInfo.Name + "/" + dayInfo.Name
				date, err := time.Parse("2006/01/02", dateStr)
				if err != nil {
					log.Warnf("KubeModel janitor: cannot parse date from %s: %v", dateStr, err)
					continue
				}
				if date.Before(cutoff) {
					j.removeDay(dayDir)
				}
			}
		}
	}
}

func (j *Janitor) removeDay(dayDir string) {
	files, err := j.store.List(dayDir)
	if err != nil {
		log.Warnf("KubeModel janitor: listing files in %s: %v", dayDir, err)
		return
	}
	for _, f := range files {
		filePath := path.Join(dayDir, f.Name)
		if err := j.store.Remove(filePath); err != nil {
			log.Warnf("KubeModel janitor: removing %s: %v", filePath, err)
		} else {
			log.Infof("KubeModel janitor: removed expired file %s", filePath)
		}
	}
}