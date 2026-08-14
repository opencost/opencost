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
// are keyed by the model-server pod's UID, so they join the rest of the
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
	// on first sight of a pod UID.
	apply := func(results []*source.InferenceEngineMetricResult, set func(s *kubemodel.InferenceEngine, value float64)) {
		for _, res := range results {
			if res.PodUID == "" || res.ModelName == "" {
				continue
			}

			server, ok := serverMap[res.PodUID]
			if !ok {
				server = &kubemodel.InferenceEngine{
					PodUID:       res.PodUID,
					NamespaceUID: res.NamespaceUID,
					ModelName:    res.ModelName,
					// The querier contract is currently implemented with the
					// vLLM metric mapping in both data sources; when further
					// Model Server Protocol mappings are added, engine
					// provenance must ride the query results instead.
					Engine: kubemodel.EngineVLLM,
				}
				serverMap[res.PodUID] = server
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
