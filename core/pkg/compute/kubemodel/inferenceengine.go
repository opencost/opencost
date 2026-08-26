package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

// computeInferenceEngines builds InferenceEngine entries from the model-server
// scheduler telemetry queries (Gateway API Inference Extension Model Server
// Protocol signals: KV-cache utilization, queue depth, running requests, and
// preemptions, with avg/p95/max summaries for the capacity gauges). Entries
// are keyed by (pod UID, engine index), so they join the rest of the
// KubeModel the same way every other entity does. Every query degrades
// gracefully: a data source with no model-server telemetry produces an empty
// map.
func (km *KubeModel) computeInferenceEngines(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	kvUsageAvgFuture := source.WithGroup(grp, metrics.QueryInferenceKVCacheUsageAvg(start, end))
	kvUsageMaxFuture := source.WithGroup(grp, metrics.QueryInferenceKVCacheUsageMax(start, end))
	queueDepthAvgFuture := source.WithGroup(grp, metrics.QueryInferenceQueueDepthAvg(start, end))
	queueDepthMaxFuture := source.WithGroup(grp, metrics.QueryInferenceQueueDepthMax(start, end))
	runningAvgFuture := source.WithGroup(grp, metrics.QueryInferenceRunningRequestsAvg(start, end))
	preemptionsFuture := source.WithGroup(grp, metrics.QueryInferencePreemptions(start, end))
	kvUsageP95Future := source.WithGroup(grp, metrics.QueryInferenceKVCacheUsageP95(start, end))
	queueDepthP95Future := source.WithGroup(grp, metrics.QueryInferenceQueueDepthP95(start, end))
	runningMaxFuture := source.WithGroup(grp, metrics.QueryInferenceRunningRequestsMax(start, end))
	runningP95Future := source.WithGroup(grp, metrics.QueryInferenceRunningRequestsP95(start, end))

	serverMap := make(map[string]*kubemodel.InferenceEngine)

	// apply merges one metric result into the server map, creating the entry
	// on first sight of an engine identity.
	apply := func(results []*source.InferenceEngineMetricResult, set func(s *kubemodel.InferenceEngine, value float64)) {
		for _, res := range results {
			if res.PodUID == "" || res.ModelName == "" {
				continue
			}

			// Identity is (pod UID, engine index): one pod can run several
			// engine cores under data parallelism, each with its own
			// scheduler, so the pod alone does not identify an engine.
			key := res.PodUID
			if res.EngineIndex != "" {
				key = res.PodUID + "/" + res.EngineIndex
			}

			server, ok := serverMap[key]
			if !ok {
				server = &kubemodel.InferenceEngine{
					PodUID:       res.PodUID,
					NamespaceUID: res.NamespaceUID,
					ModelName:    res.ModelName,
					EngineIndex:  res.EngineIndex,
					// The querier contract is currently implemented with the
					// vLLM metric mapping in both data sources; when further
					// Model Server Protocol mappings are added, engine
					// provenance must ride the query results instead.
					Engine: kubemodel.EngineVLLM,
				}
				serverMap[key] = server
			} else if server.ModelName != res.ModelName {
				// Identity comes from the first row seen for a key while the
				// value is taken from every row, so one engine reporting two
				// model names would stamp one model's measurement with the
				// other's identity, and collector results iterate a Go map, so
				// which one wins is not stable. One engine serves one model, so
				// this should be unreachable; say so loudly rather than
				// silently mis-attributing if that ever stops holding.
				log.Warnf("InferenceEngine: engine %s reported model %q and %q; keeping %q. "+
					"Measurements for this engine may be attributed to the wrong model.",
					key, server.ModelName, res.ModelName, server.ModelName)
			}

			set(server, res.Value)
		}
	}

	kvUsageAvgResult, _ := kvUsageAvgFuture.Await()
	apply(kvUsageAvgResult, func(s *kubemodel.InferenceEngine, v float64) { s.KVCacheUsageAvg = v })

	kvUsageMaxResult, _ := kvUsageMaxFuture.Await()
	apply(kvUsageMaxResult, func(s *kubemodel.InferenceEngine, v float64) { s.KVCacheUsageMax = v })

	queueDepthAvgResult, _ := queueDepthAvgFuture.Await()
	apply(queueDepthAvgResult, func(s *kubemodel.InferenceEngine, v float64) { s.QueueDepthAvg = v })

	queueDepthMaxResult, _ := queueDepthMaxFuture.Await()
	apply(queueDepthMaxResult, func(s *kubemodel.InferenceEngine, v float64) { s.QueueDepthMax = v })

	runningAvgResult, _ := runningAvgFuture.Await()
	apply(runningAvgResult, func(s *kubemodel.InferenceEngine, v float64) { s.RunningRequestsAvg = v })

	preemptionsResult, _ := preemptionsFuture.Await()
	apply(preemptionsResult, func(s *kubemodel.InferenceEngine, v float64) { s.Preemptions = v })

	kvUsageP95Result, _ := kvUsageP95Future.Await()
	apply(kvUsageP95Result, func(s *kubemodel.InferenceEngine, v float64) { s.KVCacheUsageP95 = v })

	queueDepthP95Result, _ := queueDepthP95Future.Await()
	apply(queueDepthP95Result, func(s *kubemodel.InferenceEngine, v float64) { s.QueueDepthP95 = v })

	runningMaxResult, _ := runningMaxFuture.Await()
	apply(runningMaxResult, func(s *kubemodel.InferenceEngine, v float64) { s.RunningRequestsMax = v })

	runningP95Result, _ := runningP95Future.Await()
	apply(runningP95Result, func(s *kubemodel.InferenceEngine, v float64) { s.RunningRequestsP95 = v })

	for _, server := range serverMap {
		if err := kms.RegisterInferenceEngine(server); err != nil {
			log.Warnf("Failed to register inference server: %s", err.Error())
		}
	}

	return nil
}
