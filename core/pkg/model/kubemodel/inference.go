package kubemodel

import (
	"fmt"
)

// InferenceServer holds window-aggregated model-server scheduler telemetry for
// one model-server pod. The signals are those standardized by the Gateway API
// Inference Extension Model Server Protocol (queue depth, running requests,
// KV-cache utilization) and are reported by serving engines such as vLLM.
//
// These metrics measure how much of a model server's serving capacity the
// workload actually consumes, which host-level GPU metrics (SM utilization,
// VRAM occupancy) cannot: a serving engine preallocates its memory budget and
// keeps the device busy at any batch size, so host metrics read high for
// every healthy deployment regardless of load. KV-cache utilization together
// with queue depth is the capacity signal, and it remains valid per MIG
// instance because each instance runs its own engine sized to its slice.
//
// Identity is the pod UID. Like DCGMDevice, this is split out from the
// universal k8s API structures, and like every other entity in the KubeModel
// it joins back by UID rather than by name: PodUID indexes kms.Pods directly
// and NamespaceUID indexes kms.Namespaces. There is deliberately no
// model-level grouping entity here. A rollup by served model is a view a
// consumer computes from ModelName; what the KubeModel stores is the
// measurement attached to the Kubernetes object it was measured on.
//
// Design note (kubemodel device direction): this follows the same shape as
// the planned per-source device types rather than introducing a generic
// paradigm. A model server is not a device; it is the capacity manager that
// sits between the workload and the device(s). The type is concrete and
// source-scoped: field semantics are normalized to the Model Server Protocol
// (an upstream contract that defines the per-engine metric mapping), and the
// Engine field preserves which engine's metrics populated the entry, so no
// per-engine meaning is erased by the normalization. The replica-to-device
// linkage (the GetParent analog of a MIG instance pointing at its physical
// device) is deliberately not collected here; it belongs to the DRA/device
// plugin requests join, which also relates replicas to MIG instances.
//
// Each gauge carries a window distribution summary (avg, p95, max) rather
// than per-bucket histograms, since quantiles compute identically from both
// data sources; preemption counts round out the pressure signals. KV-cache
// usage values are fractions in [0, 1] of the engine's configured KV block
// budget; queue depth and running requests are request counts. All three
// gauges carry the same (avg, p95, max) summary, so no gauge is reported with
// less resolution than the others.
//
// The measurement window is the KubeModelSet's window, so it is not repeated
// on each entry.
// @bingen:generate:InferenceServer
type InferenceServer struct {
	// PodUID is the UID of the model-server pod these measurements describe,
	// and the key this entry is stored under.
	PodUID string `json:"podUid"`
	// NamespaceUID is the UID of the pod's namespace.
	NamespaceUID string `json:"namespaceUid"`
	// ModelName is the model the engine reports serving (vLLM's model_name).
	ModelName string `json:"modelName"`
	// Engine identifies the serving engine whose metrics populated this
	// entry (see the Engine* constants). Field values follow the Model
	// Server Protocol semantics; Engine records which engine's mapping
	// produced them.
	Engine             string  `json:"engine"`
	KVCacheUsageAvg    float64 `json:"kvCacheUsageAvg"`
	KVCacheUsageP95    float64 `json:"kvCacheUsageP95"`
	KVCacheUsageMax    float64 `json:"kvCacheUsageMax"`
	QueueDepthAvg      float64 `json:"queueDepthAvg"`
	QueueDepthP95      float64 `json:"queueDepthP95"`
	QueueDepthMax      float64 `json:"queueDepthMax"`
	RunningRequestsAvg float64 `json:"runningRequestsAvg"`
	RunningRequestsP95 float64 `json:"runningRequestsP95"`
	// RunningRequestsMax carries information the average cannot: two
	// independent constraints bound a model server, the KV-cache budget
	// (reported directly by vllm:kv_cache_usage_perc) and the
	// concurrent-sequence limit (vLLM's max_num_seqs), and either can bind
	// first depending on context length. vLLM exposes no metric for
	// max_num_seqs, so the denominator of batch occupancy is not directly
	// collectible. It is recoverable by observation instead: while the queue
	// is non-empty the engine admits every sequence it can, so the running
	// gauge sits pinned at its effective concurrent-sequence limit, and the
	// window maximum is that ceiling. An average of running requests read
	// against an unknown ceiling says nothing about saturation.
	RunningRequestsMax float64 `json:"runningRequestsMax"`
	// Preemptions is the count of scheduler preemptions (requests evicted
	// from the running batch and recomputed) during the window. A pressure
	// and instability signal: sustained preemptions mean the engine is
	// thrashing its KV budget.
	Preemptions float64 `json:"preemptions"`
}

// EngineVLLM identifies vLLM as the serving engine that produced an
// InferenceServer entry. Additional engines (per the Model Server Protocol
// mappings, e.g. SGLang, Triton TensorRT-LLM) get constants as their metric
// mappings are implemented in the data sources.
const EngineVLLM = "vllm"

func (is *InferenceServer) ValidateInferenceServer() error {
	if is.PodUID == "" {
		return fmt.Errorf("PodUID is missing for InferenceServer with model '%s'", is.ModelName)
	}

	if is.ModelName == "" {
		return fmt.Errorf("ModelName is missing for InferenceServer on pod '%s'", is.PodUID)
	}

	return nil
}

// RegisterInferenceServer validates and adds an InferenceServer to the set,
// keyed by the model-server pod's UID.
func (kms *KubeModelSet) RegisterInferenceServer(server *InferenceServer) error {
	if err := server.ValidateInferenceServer(); err != nil {
		err = fmt.Errorf("RegisterInferenceServer: invalid inference server: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.InferenceServers[server.PodUID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterInferenceServer: Cluster is nil")
		}

		kms.InferenceServers[server.PodUID] = server

		kms.Metadata.ObjectCount++
	}

	return nil
}
