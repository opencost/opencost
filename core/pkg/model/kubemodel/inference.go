package kubemodel

import (
	"fmt"
	"time"
)

// InferenceServer holds window-aggregated model-server scheduler telemetry for
// one served model in one namespace, broken down per replica (pod). The
// signals are those standardized by the Gateway API Inference Extension Model
// Server Protocol (queue depth, running requests, KV-cache utilization) and
// are reported by serving engines such as vLLM.
//
// These metrics measure how much of a model server's serving capacity the
// workload actually consumes, which host-level GPU metrics (SM utilization,
// VRAM occupancy) cannot: a serving engine preallocates its memory budget and
// keeps the device busy at any batch size, so host metrics read high for
// every healthy deployment regardless of load. KV-cache utilization together
// with queue depth is the capacity signal, and it remains valid per MIG
// instance because each instance runs its own engine sized to its slice.
//
// Like DCGMDevice, this is split out from the universal k8s API structures;
// the join keys back to the rest of the KubeModel are the namespace and pod
// names carried on each replica entry.
// @bingen:generate:InferenceServer
type InferenceServer struct {
	ModelName string                            `json:"modelName"`
	Namespace string                            `json:"namespace"`
	Start     time.Time                         `json:"start"`
	End       time.Time                         `json:"end"`
	Replicas  map[string]InferenceServerReplica `json:"replicas"`
}

// InferenceServerReplica holds the window-aggregated scheduler gauges for a
// single model-server pod. KV-cache usage values are fractions in [0, 1] of
// the engine's configured KV block budget; queue depth and running requests
// are request counts.
// @bingen:generate:InferenceServerReplica
type InferenceServerReplica struct {
	KVCacheUsageAvg    float64 `json:"kvCacheUsageAvg"`
	KVCacheUsageMax    float64 `json:"kvCacheUsageMax"`
	QueueDepthAvg      float64 `json:"queueDepthAvg"`
	QueueDepthMax      float64 `json:"queueDepthMax"`
	RunningRequestsAvg float64 `json:"runningRequestsAvg"`
}

// Key returns the identifier used to store this InferenceServer in the
// KubeModelSet, matching the "model_name:namespace" keying used by the
// inference cost feature.
func (is *InferenceServer) Key() string {
	return is.ModelName + ":" + is.Namespace
}

func (is *InferenceServer) ValidateInferenceServer(window Window) error {
	if is.ModelName == "" {
		return fmt.Errorf("ModelName is missing for InferenceServer in namespace '%s'", is.Namespace)
	}

	if is.Namespace == "" {
		return fmt.Errorf("Namespace is missing for InferenceServer with model '%s'", is.ModelName)
	}

	if err := checkWindow(window, is.Start, is.End); err != nil {
		return err
	}

	return nil
}

// RegisterInferenceServer validates and adds an InferenceServer to the set,
// keyed by "model_name:namespace".
func (kms *KubeModelSet) RegisterInferenceServer(server *InferenceServer) error {
	if err := server.ValidateInferenceServer(kms.Window); err != nil {
		err = fmt.Errorf("RegisterInferenceServer: invalid inference server: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.InferenceServers[server.Key()]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterInferenceServer: Cluster is nil")
		}

		kms.InferenceServers[server.Key()] = server

		kms.Metadata.ObjectCount++
	}

	return nil
}
