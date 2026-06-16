package costmodel

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
)

// Scheduler-level GPU saturation metrics (USE method). Unlike the
// DCGM-derived device signals, these come from the Kubernetes scheduler's
// view: pods waiting for GPUs and how oversubscribed the schedulable GPU
// capacity is. Series are emitted per GPU resource name and only for
// resource names actually observed in the cluster (allocatable on a node or
// requested by a pod), so clusters without GPUs emit nothing.

var (
	gpuPendingPodCountGv          *prometheus.GaugeVec
	gpuPendingRequestTotalGv      *prometheus.GaugeVec
	gpuRequestedAllocatableRatGv  *prometheus.GaugeVec
	gpuSchedulerMetricsRegistered bool
)

// initGPUSchedulerMetrics creates and registers the scheduler-level GPU
// gauges. Called from initCostModelMetrics inside its sync.Once.
func initGPUSchedulerMetrics(disabledMetrics map[string]struct{}, register func(gv *prometheus.GaugeVec)) {
	gpuPendingPodCountGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_gpu_pending_pod_count",
		Help: "cluster_gpu_pending_pod_count Number of Pending pods requesting the GPU resource",
	}, []string{"resource"})
	if _, disabled := disabledMetrics["cluster_gpu_pending_pod_count"]; !disabled {
		register(gpuPendingPodCountGv)
	}

	gpuPendingRequestTotalGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_gpu_pending_request_total",
		Help: "cluster_gpu_pending_request_total Sum of GPU units requested by Pending pods",
	}, []string{"resource"})
	if _, disabled := disabledMetrics["cluster_gpu_pending_request_total"]; !disabled {
		register(gpuPendingRequestTotalGv)
	}

	gpuRequestedAllocatableRatGv = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_gpu_requested_allocatable_ratio",
		Help: "cluster_gpu_requested_allocatable_ratio GPU units requested by non-terminated pods divided by allocatable units; values near or above 1 indicate scheduler-level GPU saturation, including time-sliced or MPS-shared replica exhaustion",
	}, []string{"resource"})
	if _, disabled := disabledMetrics["cluster_gpu_requested_allocatable_ratio"]; !disabled {
		register(gpuRequestedAllocatableRatGv)
	}

	gpuSchedulerMetricsRegistered = true
}

// isGPUResourceName reports whether the resource name represents NVIDIA GPU
// capacity: whole GPUs, time-sliced/MPS shared replicas, or MIG profiles.
func isGPUResourceName(name v1.ResourceName) bool {
	resource := string(name)
	return resource == "nvidia.com/gpu" ||
		resource == "nvidia.com/gpu.shared" ||
		strings.HasPrefix(resource, "nvidia.com/mig-")
}

// gpuSchedulerStats accumulates scheduler-level saturation inputs for one
// GPU resource name.
type gpuSchedulerStats struct {
	PendingPodCount     float64
	PendingRequestTotal float64
	Allocatable         float64
	ActiveRequested     float64
}

// computeGPUSchedulerStats derives scheduler-level GPU saturation inputs
// from the cluster cache: per GPU resource name, the number of Pending pods
// requesting it, their total requested units, allocatable units across
// nodes, and units requested by all non-terminated pods.
func computeGPUSchedulerStats(pods []*clustercache.Pod, nodes []*clustercache.Node) map[v1.ResourceName]*gpuSchedulerStats {
	stats := make(map[v1.ResourceName]*gpuSchedulerStats)
	get := func(name v1.ResourceName) *gpuSchedulerStats {
		if _, ok := stats[name]; !ok {
			stats[name] = &gpuSchedulerStats{}
		}
		return stats[name]
	}

	for _, node := range nodes {
		for name, quantity := range node.Status.Allocatable {
			if isGPUResourceName(name) {
				get(name).Allocatable += quantity.AsApproximateFloat64()
			}
		}
	}

	for _, pod := range pods {
		phase := pod.Status.Phase
		if phase != v1.PodPending && phase != v1.PodRunning {
			continue
		}

		requests := make(map[v1.ResourceName]float64)
		for _, container := range pod.Spec.Containers {
			// GPUs are extended resources, so it is common to set only
			// Limits; mirror costmodel.go and fall back to the Limit when a
			// GPU resource is absent from Requests, else it is undercounted.
			for name, quantity := range container.Resources.Requests {
				if isGPUResourceName(name) {
					requests[name] += quantity.AsApproximateFloat64()
				}
			}
			for name, quantity := range container.Resources.Limits {
				if !isGPUResourceName(name) {
					continue
				}
				if _, ok := container.Resources.Requests[name]; ok {
					continue
				}
				requests[name] += quantity.AsApproximateFloat64()
			}
		}

		for name, requested := range requests {
			if requested <= 0 {
				continue
			}
			s := get(name)
			s.ActiveRequested += requested
			if phase == v1.PodPending {
				s.PendingPodCount++
				s.PendingRequestTotal += requested
			}
		}
	}

	return stats
}

// recordGPUSchedulerMetrics resets and re-emits the scheduler-level GPU
// gauges from the current cluster state. The requested/allocatable ratio is
// only emitted for resources with allocatable capacity, so a missing series
// is "no schedulable capacity observed" rather than zero pressure.
func recordGPUSchedulerMetrics(pods []*clustercache.Pod, nodes []*clustercache.Node) {
	if !gpuSchedulerMetricsRegistered {
		return
	}

	gpuPendingPodCountGv.Reset()
	gpuPendingRequestTotalGv.Reset()
	gpuRequestedAllocatableRatGv.Reset()

	for name, s := range computeGPUSchedulerStats(pods, nodes) {
		resource := string(name)
		gpuPendingPodCountGv.WithLabelValues(resource).Set(s.PendingPodCount)
		gpuPendingRequestTotalGv.WithLabelValues(resource).Set(s.PendingRequestTotal)
		if s.Allocatable > 0 {
			gpuRequestedAllocatableRatGv.WithLabelValues(resource).Set(s.ActiveRequested / s.Allocatable)
		}
	}
}
