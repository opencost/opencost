package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

// computeInferenceServers builds InferenceServer entries from the model-server
// scheduler telemetry queries (Gateway API Inference Extension Model Server
// Protocol gauges: KV-cache utilization, queue depth, running requests).
// Servers are keyed by "model_name:namespace" with one replica entry per pod.
// All five queries degrade gracefully: a data source with no model-server
// telemetry produces an empty map.
func (km *KubeModel) computeInferenceServers(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	kvUsageAvgFuture := source.WithGroup(grp, metrics.QueryInferenceKVCacheUsageAvg(start, end))
	kvUsageMaxFuture := source.WithGroup(grp, metrics.QueryInferenceKVCacheUsageMax(start, end))
	queueDepthAvgFuture := source.WithGroup(grp, metrics.QueryInferenceQueueDepthAvg(start, end))
	queueDepthMaxFuture := source.WithGroup(grp, metrics.QueryInferenceQueueDepthMax(start, end))
	runningAvgFuture := source.WithGroup(grp, metrics.QueryInferenceRunningRequestsAvg(start, end))

	serverMap := make(map[string]*kubemodel.InferenceServer)

	// apply merges one metric result into the server map, creating the server
	// and replica entries on first sight of a (model, namespace, pod) key.
	apply := func(results []*source.InferenceServerMetricResult, set func(r *kubemodel.InferenceServerReplica, value float64)) {
		for _, res := range results {
			if res.ModelName == "" || res.Namespace == "" || res.Pod == "" {
				continue
			}

			key := res.ModelName + ":" + res.Namespace
			server, ok := serverMap[key]
			if !ok {
				server = &kubemodel.InferenceServer{
					ModelName: res.ModelName,
					Namespace: res.Namespace,
					// The querier contract is currently implemented with the
					// vLLM metric mapping in both data sources; when further
					// Model Server Protocol mappings are added, engine
					// provenance must ride the query results instead.
					Engine:   kubemodel.EngineVLLM,
					Start:    start,
					End:      end,
					Replicas: make(map[string]kubemodel.InferenceServerReplica),
				}
				serverMap[key] = server
			}

			replica := server.Replicas[res.Pod]
			set(&replica, res.Value)
			server.Replicas[res.Pod] = replica
		}
	}

	kvUsageAvgResult, _ := kvUsageAvgFuture.Await()
	apply(kvUsageAvgResult, func(r *kubemodel.InferenceServerReplica, v float64) { r.KVCacheUsageAvg = v })

	kvUsageMaxResult, _ := kvUsageMaxFuture.Await()
	apply(kvUsageMaxResult, func(r *kubemodel.InferenceServerReplica, v float64) { r.KVCacheUsageMax = v })

	queueDepthAvgResult, _ := queueDepthAvgFuture.Await()
	apply(queueDepthAvgResult, func(r *kubemodel.InferenceServerReplica, v float64) { r.QueueDepthAvg = v })

	queueDepthMaxResult, _ := queueDepthMaxFuture.Await()
	apply(queueDepthMaxResult, func(r *kubemodel.InferenceServerReplica, v float64) { r.QueueDepthMax = v })

	runningAvgResult, _ := runningAvgFuture.Await()
	apply(runningAvgResult, func(r *kubemodel.InferenceServerReplica, v float64) { r.RunningRequestsAvg = v })

	for _, server := range serverMap {
		if err := kms.RegisterInferenceServer(server); err != nil {
			log.Warnf("Failed to register inference server: %s", err.Error())
		}
	}

	return nil
}
