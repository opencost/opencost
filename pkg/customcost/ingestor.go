package customcost

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-plugin"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/env"
)

// IngestorStatus includes diagnostic values for a given Ingestor
type IngestorStatus struct {
	Created  time.Time
	LastRun  time.Time
	NextRun  time.Time
	Runs     int
	Coverage opencost.Window
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
	coverage     opencost.Window
	coverageLock sync.Mutex
	isRunning    atomic.Bool
	isStopping   atomic.Bool
	exitBuildCh  chan string
	exitRunCh    chan string
}

// NewIngestor is an initializer for ingestor
func NewCustomCostIngestor(ingestorConfig *CustomCostIngestorConfig, repo Repository, plugins map[string]*plugin.Client) (*CustomCostIngestor, error) {
	if repo == nil {
		return nil, fmt.Errorf("CustomCost: NewCustomCostIngestor: repository connot be nil")
	}
	if ingestorConfig == nil {
		return nil, fmt.Errorf("CustomCost: NewCustomCostIngestor: integration connot be nil")
	}

	now := time.Now().UTC()
	midnight := opencost.RoundForward(now, timeutil.Day)
	return &CustomCostIngestor{
		config:       ingestorConfig,
		repo:         repo,
		creationTime: now,
		lastRun:      now,
		coverage:     opencost.NewClosedWindow(midnight, midnight),
	}, nil
}

func (ing *CustomCostIngestor) LoadWindow(start, end time.Time) {
	windows, err := opencost.GetWindows(start, end, timeutil.Day)
	if err != nil {
		log.Errorf("CloudCost[%s]: ingestor: invalid window %s", ing.key, opencost.NewWindow(&start, &end))
		return
	}

	for _, window := range windows {
		has, err2 := ing.repo.Has(*window.Start(), ing.key)
		if err2 != nil {
			log.Errorf("CloudCost[%s]: ingestor: error when loading window: %s", ing.key, err2.Error())
		}
		if !has {
			ing.BuildWindow(start, end)
			return
		}
		ing.expandCoverage(window)
		log.Debugf("CloudCost[%s]: ingestor: skipping build for window %s, coverage already exists", ing.key, window.String())
	}

}

func (ing *CustomCostIngestor) BuildWindow(start, end time.Time) {
	// log.Infof("CloudCost[%s]: ingestor: building window %s", ing.key, opencost.NewWindow(&start, &end))
	// ccsr, err := ing.integration.GetCloudCost(start, end)
	// if err != nil {
	// 	log.Errorf("CloudCost[%s]: ingestor: build failed for window %s: %s", ing.key, opencost.NewWindow(&start, &end), err.Error())
	// 	return
	// }
	// for _, ccs := range ccsr.CloudCostSets {
	// 	log.Debugf("BuildWindow[%s]: GetCloudCost: writing cloud costs for window %s: %d", ccs.Integration, ccs.Window, len(ccs.CloudCosts))
	// 	err2 := ing.repo.Put(ccs)
	// 	if err2 != nil {
	// 		log.Errorf("CloudCost[%s]: ingestor: failed to save Cloud Cost Set with window %s: %s", ing.key, ccs.GetWindow().String(), err2.Error())
	// 	}
	// 	ing.expandCoverage(ccs.Window)
	// }
}

func (ing *CustomCostIngestor) Start(rebuild bool) {

	// // If already running, log that and return.
	// if !ing.isRunning.CompareAndSwap(false, true) {
	// 	log.Infof("CloudCost: ingestor: is already running")
	// 	return
	// }

	// ing.runID = stringutil.RandSeq(5)

	// ing.exitBuildCh = make(chan string)
	// ing.exitRunCh = make(chan string)

	// // Build the store once, advancing backward in time from the earliest
	// // point of coverage.
	// go ing.build(rebuild)

	// go ing.run()

	// TEMPORARY - load the repo with dummy data
	var resps []*model.CustomCostResponse
	err := json.Unmarshal([]byte(ddData), &resps)
	if err != nil {
		panic(err)
	}
	err = ing.repo.Put(resps)
	if err != nil {
		panic(err)
	}
	//2024-02-27T01:00:00
	target := time.Date(2024, 2, 27, 1, 0, 0, 0, time.UTC)
	stored, err := ing.repo.Get(target, "datadog")
	if err != nil {
		panic(err)
	}

	for _, storedResp := range stored {
		log.Debug("got stored object: ")
		spew.Dump(storedResp)
	}
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
	// defer errors.HandlePanic()

	// // Profile the full Duration of the build time
	// buildStart := time.Now()

	// // Build as far back as the configures build Duration
	// limit := opencost.RoundBack(time.Now().UTC().Add(-ing.config.Duration), ing.config.Resolution)

	// queryWindowStr := timeutil.FormatStoreResolution(ing.config.QueryWindow)
	// log.Infof("CloudCost[%s]: ingestor: build[%s]: Starting build back to %s in blocks of %s", ing.key, ing.runID, limit.String(), queryWindowStr)

	// // Start with a window of the configured Duration and ending on the given
	// // start time. Build windows repeating until the window reaches the
	// // given limit time

	// // Round end times back to nearest Resolution points in the past,
	// // querying for exactly one interval
	// e := opencost.RoundBack(time.Now().UTC(), ing.config.Resolution)
	// s := e.Add(-ing.config.QueryWindow)

	// // Continue until limit is reached
	// for limit.Before(e) {
	// 	// If exit instruction is received, log and return
	// 	select {
	// 	case <-ing.exitBuildCh:
	// 		log.Debugf("CloudCost[%s]: ingestor: build[%s]: exiting", ing.key, ing.runID)
	// 		return
	// 	default:
	// 	}

	// 	// Profile the current build step
	// 	stepStart := time.Now()

	// 	// if rebuild is not specified then check for existing coverage on window
	// 	if rebuild {
	// 		ing.BuildWindow(s, e)
	// 	} else {
	// 		ing.LoadWindow(s, e)
	// 	}

	// 	log.Infof("CloudCost[%s]: ingestor: build[%s]:  %s in %v", ing.key, ing.runID, opencost.NewClosedWindow(s, e), time.Since(stepStart))

	// 	// Shift to next QueryWindow
	// 	s = s.Add(-ing.config.QueryWindow)
	// 	if s.Before(limit) {
	// 		s = limit
	// 	}
	// 	e = e.Add(-ing.config.QueryWindow)
	// }

	// log.Infof(fmt.Sprintf("CloudCost[%s]: ingestor: build[%s]: completed in %v", ing.key, ing.runID, time.Since(buildStart)))

	// // In order to be able to Stop, we have to wait on an exit message
	// // here
	// <-ing.exitBuildCh

}

func (ing *CustomCostIngestor) Rebuild(domain string) error {
	return nil
}
func (ing *CustomCostIngestor) run() {
	// defer errors.HandlePanic()

	// ticker := timeutil.NewJobTicker()
	// defer ticker.Close()
	// ticker.TickIn(0)

	// for {
	// 	// If an exit instruction is received, break the run loop
	// 	select {
	// 	case <-ing.exitRunCh:
	// 		log.Debugf("CloudCost[%s]: ingestor: Run[%s] exiting", ing.key, ing.runID)
	// 		return
	// 	case <-ticker.Ch:
	// 		// Wait for next tick
	// 	}

	// 	// Start from the last covered time, minus the RunWindow
	// 	start := ing.lastRun
	// 	start = start.Add(-ing.config.RunWindow)

	// 	// Every Nth (determined by the MonthToDateRunInterval) run should be a month to date run. Where the start is
	// 	// truncated to the beginning of its current month this can mean that early in a new month we will build all of
	// 	// last month and the first few days of the current month.
	// 	if ing.runs%ing.config.MonthToDateRunInterval == 0 {
	// 		start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	// 		log.Infof("CloudCost[%s]: ingestor: Run[%s]: running month-to-date update starting at %s", ing.key, ing.runID, start.String())
	// 	}

	// 	// Round start time back to the nearest Resolution point in the past from the
	// 	// last update to the QueryWindow
	// 	s := opencost.RoundBack(start.UTC(), ing.config.Resolution)
	// 	e := s.Add(ing.config.QueryWindow)

	// 	// Start with a window of the configured Duration and starting on the given
	// 	// start time. Do the following, repeating until the window reaches the
	// 	// current time:
	// 	// 1. Instruct builder to build window
	// 	// 2. Move window forward one Resolution
	// 	for time.Now().After(s) {
	// 		profStart := time.Now()
	// 		ing.BuildWindow(s, e)

	// 		log.Debugf("CloudCost[%s]: ingestor: Run[%s]: completed %s in %v", ing.key, ing.runID, opencost.NewWindow(&s, &e), time.Since(profStart))

	// 		s = s.Add(ing.config.QueryWindow)
	// 		e = e.Add(ing.config.QueryWindow)
	// 		// prevent builds into the future
	// 		if e.After(time.Now().UTC()) {
	// 			e = opencost.RoundForward(time.Now().UTC(), ing.config.Resolution)
	// 		}

	// 	}
	// 	ing.lastRun = time.Now().UTC()

	// 	limit := opencost.RoundBack(time.Now().UTC(), ing.config.Resolution).Add(-ing.config.Duration)
	// 	err := ing.repo.Expire(limit)
	// 	if err != nil {
	// 		log.Errorf("CloudCost: Ingestor: failed to expire Data: %s", err)
	// 	}

	// 	ing.coverageLock.Lock()
	// 	ing.coverage = ing.coverage.ContractStart(limit)
	// 	ing.coverageLock.Unlock()

	// 	ing.runs++

	// 	ticker.TickIn(ing.config.RefreshRate)
	// }
}

func (ing *CustomCostIngestor) expandCoverage(window opencost.Window) {
	if window.IsOpen() {
		return
	}
	ing.coverageLock.Lock()
	defer ing.coverageLock.Unlock()

	coverage := ing.coverage.ExpandStart(*window.Start())
	coverage = coverage.ExpandEnd(*window.End())

	ing.coverage = coverage
}

// temporary mock data
const ddData = `
[
    {
        "Metadata": {
            "api_client_version": "v2"
        },
        "Costsource": "observability",
        "Domain": "datadog",
        "Version": "v1",
        "Currency": "USD",
        "Window": {
            "start": "2024-02-27T00:00:00Z",
            "end": "2024-02-27T01:00:00Z"
        },
        "Costs": [
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "350+ integrations, alerting, custom metrics \u0026 unlimited user accounts",
                "ListCost": 90,
                "ListUnitPrice": 18,
                "ResourceName": "agent_host_count",
                "ResourceType": "infra_hosts",
                "Id": "4bba680574ac970cfba52a5edc5b2d44541319b365fbcc45023b51fbe2572373",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/agent_host_count",
                "Window": {
                    "start": "2024-02-27T00:00:00Z",
                    "end": "2024-02-27T01:00:00Z"
                },
                "Labels": {},
                "UsageQty": 5,
                "UsageUnit": "Infra Hosts",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "Centralize your monitoring of systems and services (Per Container)",
                "ListCost": 236,
                "ListUnitPrice": 1,
                "ResourceName": "container_count",
                "ResourceType": "infra_hosts",
                "Id": "4bba680574ac970cfba52a5edc5b2d44541319b365fbcc45023b51fbe2572373",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/container_count",
                "Window": {
                    "start": "2024-02-27T00:00:00Z",
                    "end": "2024-02-27T01:00:00Z"
                },
                "Labels": {},
                "UsageQty": 236,
                "UsageUnit": "Containers",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "Centralize your monitoring of systems and services (Per Container)",
                "ListCost": 219,
                "ListUnitPrice": 1,
                "ResourceName": "container_count_excl_agent",
                "ResourceType": "infra_hosts",
                "Id": "4bba680574ac970cfba52a5edc5b2d44541319b365fbcc45023b51fbe2572373",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/container_count_excl_agent",
                "Window": {
                    "start": "2024-02-27T00:00:00Z",
                    "end": "2024-02-27T01:00:00Z"
                },
                "Labels": {},
                "UsageQty": 219,
                "UsageUnit": "Containers",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "350+ integrations, alerting, custom metrics \u0026 unlimited user accounts",
                "ListCost": 90,
                "ListUnitPrice": 18,
                "ResourceName": "host_count",
                "ResourceType": "infra_hosts",
                "Id": "4bba680574ac970cfba52a5edc5b2d44541319b365fbcc45023b51fbe2572373",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/host_count",
                "Window": {
                    "start": "2024-02-27T00:00:00Z",
                    "end": "2024-02-27T01:00:00Z"
                },
                "Labels": {},
                "UsageQty": 5,
                "UsageUnit": "Infra Hosts",
                "ExtendedAttributes": null
            }
        ],
        "Errors": null
    },
    {
        "Metadata": {
            "api_client_version": "v2"
        },
        "Costsource": "observability",
        "Domain": "datadog",
        "Version": "v1",
        "Currency": "USD",
        "Window": {
            "start": "2024-02-27T01:00:00Z",
            "end": "2024-02-27T02:00:00Z"
        },
        "Costs": [
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "350+ integrations, alerting, custom metrics \u0026 unlimited user accounts",
                "ListCost": 90,
                "ListUnitPrice": 18,
                "ResourceName": "agent_host_count",
                "ResourceType": "infra_hosts",
                "Id": "448c8561d845b42adb1d52ebc88b3c44385372e54bf117544442d25887e3c338",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/agent_host_count",
                "Window": {
                    "start": "2024-02-27T01:00:00Z",
                    "end": "2024-02-27T02:00:00Z"
                },
                "Labels": {},
                "UsageQty": 5,
                "UsageUnit": "Infra Hosts",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "Centralize your monitoring of systems and services (Per Container)",
                "ListCost": 235,
                "ListUnitPrice": 1,
                "ResourceName": "container_count",
                "ResourceType": "infra_hosts",
                "Id": "448c8561d845b42adb1d52ebc88b3c44385372e54bf117544442d25887e3c338",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/container_count",
                "Window": {
                    "start": "2024-02-27T01:00:00Z",
                    "end": "2024-02-27T02:00:00Z"
                },
                "Labels": {},
                "UsageQty": 235,
                "UsageUnit": "Containers",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "Centralize your monitoring of systems and services (Per Container)",
                "ListCost": 218,
                "ListUnitPrice": 1,
                "ResourceName": "container_count_excl_agent",
                "ResourceType": "infra_hosts",
                "Id": "448c8561d845b42adb1d52ebc88b3c44385372e54bf117544442d25887e3c338",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/container_count_excl_agent",
                "Window": {
                    "start": "2024-02-27T01:00:00Z",
                    "end": "2024-02-27T02:00:00Z"
                },
                "Labels": {},
                "UsageQty": 218,
                "UsageUnit": "Containers",
                "ExtendedAttributes": null
            },
            {
                "Metadata": null,
                "Zone": "us",
                "BilledCost": 0,
                "AccountName": "Kubecost",
                "ChargeCategory": "usage",
                "Description": "350+ integrations, alerting, custom metrics \u0026 unlimited user accounts",
                "ListCost": 90,
                "ListUnitPrice": 18,
                "ResourceName": "host_count",
                "ResourceType": "infra_hosts",
                "Id": "448c8561d845b42adb1d52ebc88b3c44385372e54bf117544442d25887e3c338",
                "ProviderId": "42c0ac62-8d80-11ed-96f3-da7ad0900005/host_count",
                "Window": {
                    "start": "2024-02-27T01:00:00Z",
                    "end": "2024-02-27T02:00:00Z"
                },
                "Labels": {},
                "UsageQty": 5,
                "UsageUnit": "Infra Hosts",
                "ExtendedAttributes": null
            }
        ],
        "Errors": null
    }
]
`
