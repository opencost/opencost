package customcost

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-plugin"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/opencost"
	ocplugin "github.com/opencost/opencost/core/pkg/plugin"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
)

// IngestorStatus includes diagnostic values for a given Ingestor
type IngestorStatus struct {
	Created  time.Time
	LastRun  time.Time
	NextRun  time.Time
	Runs     int
	Coverage map[string]opencost.Window
}

// CustomCost IngestorConfig is a configuration struct for an Ingestor
type CustomCostIngestorConfig struct {
	MonthToDateRunInterval               int
	HourlyDuration, DailyDuration        time.Duration
	QueryWindow                          time.Duration
	RunWindow                            time.Duration
	PluginConfigDir, PluginExecutableDir string
	RefreshRate                          time.Duration
}

// DefaultIngestorConfiguration retrieves an CustomCostIngestorConfig from env variables
func DefaultIngestorConfiguration() CustomCostIngestorConfig {
	return CustomCostIngestorConfig{
		DailyDuration:          timeutil.Day * time.Duration(env.GetDataRetentionDailyResolutionDays()),
		HourlyDuration:         time.Hour * time.Duration(env.GetDataRetentionHourlyResolutionHours()),
		MonthToDateRunInterval: env.GetCloudCostMonthToDateInterval(),
		QueryWindow:            timeutil.Day * time.Duration(env.GetCloudCostQueryWindowDays()),
		PluginConfigDir:        env.GetPluginConfigDir(),
	}
}

type CustomCostIngestor struct {
	key          string
	config       *CustomCostIngestorConfig
	repo         Repository
	runID        string
	lastRun      time.Time
	runs         int
	creationTime time.Time
	coverage     map[string]opencost.Window
	coverageLock sync.Mutex
	isRunning    atomic.Bool
	isStopping   atomic.Bool
	exitBuildCh  chan string
	exitRunCh    chan string
	plugins      map[string]*plugin.ClientProtocol
	resolution   time.Duration
}

// NewIngestor is an initializer for ingestor
func NewCustomCostIngestor(ingestorConfig *CustomCostIngestorConfig, repo Repository, plugins map[string]*plugin.ClientProtocol, res time.Duration) (*CustomCostIngestor, error) {
	if repo == nil {
		return nil, fmt.Errorf("CustomCost: NewCustomCostIngestor: repository connot be nil")
	}
	if ingestorConfig == nil {
		return nil, fmt.Errorf("CustomCost: NewCustomCostIngestor: config connot be nil")
	}
	key := ""

	for name := range plugins {
		key += "," + name
	}

	key = strings.TrimPrefix(key, ",")

	now := time.Now().UTC()

	return &CustomCostIngestor{
		key:          key,
		config:       ingestorConfig,
		repo:         repo,
		creationTime: now,
		lastRun:      now,
		coverage:     map[string]opencost.Window{},
		plugins:      plugins,
		resolution:   res,
	}, nil
}

func (ing *CustomCostIngestor) LoadWindow(start, end time.Time) {
	var targets []opencost.Window
	if ing.resolution == time.Hour {
		oldestDailyDate := time.Now().UTC().Add(-1 * ing.config.DailyDuration)
		if !oldestDailyDate.After(start) {
			windows, err := opencost.GetWindows(start, end, timeutil.Day)
			if err != nil {
				log.Errorf("CustomCost[%s]: ingestor: invalid window %s", ing.key, opencost.NewWindow(&start, &end))
				return
			}
			targets = windows
		}
	} else {

		oldestHourlyDate := time.Now().UTC().Add(-1 * ing.config.HourlyDuration)
		if !oldestHourlyDate.After(start) {
			windows, err := opencost.GetWindows(start, end, time.Hour)
			if err != nil {
				log.Errorf("CustomCost[%s]: ingestor: invalid window %s", ing.key, opencost.NewWindow(&start, &end))
				return
			}
			targets = windows
		}
	}

	for _, window := range targets {
		allPluginsHave := true
		for domain := range ing.plugins {
			has, err2 := ing.repo.Has(*window.Start(), domain)
			if err2 != nil {
				log.Errorf("CustomCost[%s]: ingestor: error when loading window for plugin %s: %s", ing.key, domain, err2.Error())
			}
			if !has {
				allPluginsHave = false
				break
			}
		}
		if !allPluginsHave {
			ing.BuildWindow(start, end)
		} else {
			for domain := range ing.plugins {
				ing.expandCoverage(window, domain)
			}
			log.Debugf("CustomCost[%s]: ingestor: skipping build for window %s, coverage already exists", ing.key, window.String())
		}
	}

}

func (ing *CustomCostIngestor) BuildWindow(start, end time.Time) {

	for domain := range ing.plugins {
		ing.buildSingleDomain(start, end, domain)
	}
}

func (ing *CustomCostIngestor) buildSingleDomain(start, end time.Time, domain string) {
	target := opencost.NewWindow(&start, &end)
	req := model.CustomCostRequest{
		TargetWindow: &target,
		Resolution:   ing.resolution,
	}
	log.Infof("ingestor: building window %s for plugin %s", opencost.NewWindow(&start, &end), domain)
	// make RPC call via plugin
	pluginClient, found := ing.plugins[domain]
	if !found {
		log.Errorf("could not find plugin client for plugin %s. Did you initialize the plugin correctly?", domain)
		return
	}
	pluginClientDeref := *pluginClient
	// Request the plugin
	raw, err := pluginClientDeref.Dispense("CustomCostSource")
	if err != nil {
		log.Errorf("error creating new plugin client for plugin %s: %v", domain, err)
		return
	}

	custCostSrc := raw.(ocplugin.CustomCostSource)

	custCostResps := custCostSrc.GetCustomCosts(req)
	// loop through each customCostResponse, adding to repo
	for _, ccr := range custCostResps {

		// check for errors in response
		if len(ccr.Errors) > 0 {
			for _, errResp := range ccr.Errors {
				log.Errorf("error in getting custom costs for plugin %s: %v", domain, errResp)
			}
			log.Errorf("not adding any costs for window %v on plugin %s", req.TargetWindow, domain)
			continue
		}
		log.Debugf("BuildWindow[%s]: GetCustomCost: writing custom costs for window %s: %d", domain, ccr.Window, len(ccr.Costs))

		err2 := ing.repo.Put(&ccr)
		if err2 != nil {
			log.Errorf("CustomCost[%s]: ingestor: failed to save Custom Cost Set with window %s: %s", domain, ccr.GetWindow().String(), err2.Error())
		}

		ing.expandCoverage(ccr.Window, domain)
	}
}

func (ing *CustomCostIngestor) Start(rebuild bool) {

	// If already running, log that and return.
	if !ing.isRunning.CompareAndSwap(false, true) {
		log.Infof("CustomCost: ingestor: is already running")
		return
	}

	ing.runID = stringutil.RandSeq(5)

	ing.exitBuildCh = make(chan string)
	ing.exitRunCh = make(chan string)

	// Build the store once, advancing backward in time from the earliest
	// point of coverage.
	go ing.build(rebuild)

	go ing.run()

}

func (ing *CustomCostIngestor) Stop() {
	// If already stopping, log that and return.
	if !ing.isStopping.CompareAndSwap(false, true) {
		log.Infof("CloudCost: ingestor: is already stopping")
		return
	}

	msg := "Stopping"

	// If the processes are running (and thus there are channels available for
	// stopping them) then stop all sub-processes (i.e. build and run)
	var wg sync.WaitGroup

	if ing.exitBuildCh != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.exitBuildCh <- msg
		}()
	}

	if ing.exitRunCh != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.exitRunCh <- msg
		}()
	}

	wg.Wait()

	// Declare that the store is officially no longer running. This allows
	// Start to be called again, restarting the store from scratch.
	ing.isRunning.Store(false)
	ing.isStopping.Store(false)
}

// Status returns an IngestorStatus that describes the current state of the ingestor
func (ing *CustomCostIngestor) Status() IngestorStatus {
	return IngestorStatus{
		Created:  ing.creationTime,
		LastRun:  ing.lastRun,
		NextRun:  ing.lastRun.Add(ing.config.RefreshRate).UTC(),
		Runs:     ing.runs,
		Coverage: ing.coverage,
	}
}

func (ing *CustomCostIngestor) build(rebuild bool) {
	defer errors.HandlePanic()

	// Profile the full Duration of the build time
	buildStart := time.Now()

	targetDur := ing.config.DailyDuration
	if ing.resolution == time.Hour {
		targetDur = ing.config.HourlyDuration
	}
	// Build as far back as the configures build Duration
	limit := opencost.RoundBack(time.Now().UTC().Add(-1*targetDur), ing.resolution)

	queryWindowStr := timeutil.FormatStoreResolution(ing.config.QueryWindow)
	log.Infof("CustomCost[%s]: ingestor: build[%s]: Starting build back to %s in blocks of %s", ing.key, ing.runID, limit.String(), queryWindowStr)

	// Start with a window of the configured Duration and ending on the given
	// start time. Build windows repeating until the window reaches the
	// given limit time

	// Round end times back to nearest Resolution points in the past,
	// querying for exactly one interval
	e := opencost.RoundBack(time.Now().UTC(), ing.resolution)
	s := e.Add(-ing.config.QueryWindow)

	// Continue until limit is reached
	for limit.Before(e) {
		// If exit instruction is received, log and return
		select {
		case <-ing.exitBuildCh:
			log.Debugf("CustomCost[%s][%s]: ingestor: build[%s]: exiting", ing.key, ing.resolution, ing.runID)
			return
		default:
		}

		// Profile the current build step
		stepStart := time.Now()

		// if rebuild is not specified then check for existing coverage on window
		if rebuild {
			ing.BuildWindow(s, e)
		} else {
			ing.LoadWindow(s, e)
		}

		log.Infof("CustomCost[%s]: ingestor: build[%s]:  %s in %v", ing.key, ing.runID, opencost.NewClosedWindow(s, e), time.Since(stepStart))

		// Shift to next QueryWindow
		s = s.Add(-ing.config.QueryWindow)
		if s.Before(limit) {
			s = limit
		}
		e = e.Add(-ing.config.QueryWindow)
	}

	log.Infof(fmt.Sprintf("CusomCost[%s]: ingestor: build[%s]: completed in %v", ing.key, ing.runID, time.Since(buildStart)))

	// In order to be able to Stop, we have to wait on an exit message
	// here
	<-ing.exitBuildCh

}

func (ing *CustomCostIngestor) Rebuild(domain string) error {
	targetDur := ing.config.DailyDuration
	if ing.resolution == time.Hour {
		targetDur = ing.config.HourlyDuration
	}
	// Build as far back as the configures build Duration
	limit := opencost.RoundBack(time.Now().UTC().Add(-1*targetDur), ing.resolution)
	e := time.Now().UTC()

	ing.buildSingleDomain(limit, e, domain)
	return nil
}

func (ing *CustomCostIngestor) run() {
	defer errors.HandlePanic()

	ticker := timeutil.NewJobTicker()
	defer ticker.Close()
	ticker.TickIn(0)

	for {
		// If an exit instruction is received, break the run loop
		select {
		case <-ing.exitRunCh:
			log.Debugf("CustomCost[%s]: ingestor: Run[%s] exiting", ing.key, ing.runID)
			return
		case <-ticker.Ch:
			// Wait for next tick
		}

		// Start from the last covered time, minus the RunWindow
		start := ing.lastRun
		start = start.Add(-ing.config.RunWindow)

		// Round start time back to the nearest Resolution point in the past from the
		// last update to the QueryWindow
		s := opencost.RoundBack(start.UTC(), ing.resolution)
		e := s.Add(ing.config.QueryWindow)

		// Start with a window of the configured Duration and starting on the given
		// start time. Do the following, repeating until the window reaches the
		// current time:
		// 1. Instruct builder to build window
		// 2. Move window forward one Resolution
		for time.Now().After(s) {
			profStart := time.Now()
			ing.BuildWindow(s, e)

			log.Debugf("CUstomCost[%s]: ingestor: Run[%s]: completed %s in %v", ing.key, ing.runID, opencost.NewWindow(&s, &e), time.Since(profStart))

			s = s.Add(ing.config.QueryWindow)
			e = e.Add(ing.config.QueryWindow)
			// prevent builds into the future
			if e.After(time.Now().UTC()) {
				e = opencost.RoundForward(time.Now().UTC(), ing.resolution)
			}

		}
		ing.lastRun = time.Now().UTC()

		ing.runs++

		ticker.TickIn(ing.config.RefreshRate)
	}
}

func (ing *CustomCostIngestor) expandCoverage(window opencost.Window, plugin string) {
	if window.IsOpen() {
		return
	}
	ing.coverageLock.Lock()
	defer ing.coverageLock.Unlock()

	if _, hasCoverage := ing.coverage[plugin]; !hasCoverage {
		ing.coverage[plugin] = window.Clone()
	} else {
		// expand existing coverage
		ing.coverage[plugin] = ing.coverage[plugin].ExpandStart(*window.Start())
		ing.coverage[plugin] = ing.coverage[plugin].ExpandEnd(*window.End())
	}

}
