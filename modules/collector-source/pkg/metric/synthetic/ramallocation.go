package synthetic

import (
	"maps"
	"math"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// ContainerMemoryAllocationMetric is the grouping unit for memory usage and request
type ContainerMemoryAllocationMetric struct {
	requestMetric *metric.Update
	usageMetric   *metric.Update
}

// Synthesize returns a new ContainerMemoryAllocationBytes metric update with the max(request, usage)
func (cmam *ContainerMemoryAllocationMetric) Synthesize() metric.Update {
	if cmam.requestMetric != nil && cmam.usageMetric != nil {
		req := cmam.requestMetric.Value
		if math.IsNaN(req) {
			log.Debugf("NaN value found during memory allocation synthesis for requests.")
			req = 0.0
		}

		used := cmam.usageMetric.Value
		if math.IsNaN(used) {
			log.Debugf("NaN value found during memory allocation synthesis for used.")
			used = 0.0
		}

		// TODO: validate and merge labels if they both have keys?
		labels := maps.Clone(cmam.usageMetric.Labels)

		return metric.Update{
			Name:   metric.ContainerMemoryAllocationBytes,
			Labels: labels,
			Value:  max(req, used),
		}
	} else if cmam.requestMetric != nil {
		req := cmam.requestMetric.Value
		if math.IsNaN(req) {
			log.Debugf("NaN value found during memory allocation synthesis for requests.")
			req = 0.0
		}

		// drop the "extra" labels
		labels := maps.Clone(cmam.requestMetric.Labels)
		delete(labels, source.ResourceLabel)
		delete(labels, source.UnitLabel)

		return metric.Update{
			Name:   metric.ContainerMemoryAllocationBytes,
			Labels: labels,
			Value:  req,
		}
	}

	// not possible for both request and usage to be nil, so we can assume only used is
	// valid here
	used := cmam.usageMetric.Value
	if math.IsNaN(used) {
		log.Debugf("NaN value found during memory allocation synthesis for used.")
		used = 0.0
	}

	labels := maps.Clone(cmam.usageMetric.Labels)

	return metric.Update{
		Name:   metric.ContainerMemoryAllocationBytes,
		Labels: labels,
		Value:  used,
	}
}

// ContainerMemoryAllocationSynthesizer is a MetricSynthesizer that leverages pod uid and container name grouping
// to match relevant request and usage metrics to build the memory allocation data.
type ContainerMemoryAllocationSynthesizer struct {
	byPod map[string]map[string]*ContainerMemoryAllocationMetric
}

// NewContainerMemoryAllocationSynthesizer creates a new ContainerMemoryAllocationSynthesizer which synthesizes
// metric updates for ContainerMemoryAllocationBytes from ram requests and ram usage metrics.
func NewContainerMemoryAllocationSynthesizer() *ContainerMemoryAllocationSynthesizer {
	return &ContainerMemoryAllocationSynthesizer{
		byPod: make(map[string]map[string]*ContainerMemoryAllocationMetric),
	}
}

// Process accepts metric updates and only records updates relevant to memory allocation.
func (cmas *ContainerMemoryAllocationSynthesizer) Process(t time.Time, update *metric.Update) {
	switch update.Name {
	case metric.KubePodContainerResourceRequests:
		cmas.addRequestsMetric(update)
	case metric.ContainerMemoryWorkingSetBytes:
		cmas.addUsageMetric(update)
	}
}

// Synthesize generates all new memory allocation metrics
func (cmas *ContainerMemoryAllocationSynthesizer) Synthesize() []metric.Update {
	var updates []metric.Update

	for _, pod := range cmas.byPod {
		for _, synthesizer := range pod {
			updates = append(updates, synthesizer.Synthesize())
		}
	}

	return updates
}

// Clear drops the current metric mapping and creates a new map ready to process next metrics collection.
func (cmas *ContainerMemoryAllocationSynthesizer) Clear() {
	cmas.byPod = make(map[string]map[string]*ContainerMemoryAllocationMetric)
}

func (cmas *ContainerMemoryAllocationSynthesizer) addRequestsMetric(update *metric.Update) {
	if !cmas.isValidRequests(update.Labels) {
		return
	}

	podUID := update.Labels[source.UIDLabel]
	container := update.Labels[source.ContainerLabel]
	if _, ok := cmas.byPod[podUID]; !ok {
		cmas.byPod[podUID] = make(map[string]*ContainerMemoryAllocationMetric)
	}

	if _, ok := cmas.byPod[podUID][container]; !ok {
		cmas.byPod[podUID][container] = &ContainerMemoryAllocationMetric{
			requestMetric: update,
		}
	} else {
		cmas.byPod[podUID][container].requestMetric = update
	}
}

func (cmas *ContainerMemoryAllocationSynthesizer) addUsageMetric(update *metric.Update) {
	if !cmas.isValidUsage(update.Labels) {
		return
	}

	podUID := update.Labels[source.UIDLabel]
	container := update.Labels[source.ContainerLabel]
	if _, ok := cmas.byPod[podUID]; !ok {
		cmas.byPod[podUID] = make(map[string]*ContainerMemoryAllocationMetric)
	}

	if _, ok := cmas.byPod[podUID][container]; !ok {
		cmas.byPod[podUID][container] = &ContainerMemoryAllocationMetric{
			usageMetric: update,
		}
	} else {
		cmas.byPod[podUID][container].usageMetric = update
	}
}

func (cmas *ContainerMemoryAllocationSynthesizer) isValidRequests(labels map[string]string) bool {
	return labels[source.ResourceLabel] == "memory" &&
		labels[source.UnitLabel] == "byte" &&
		labels[source.ContainerLabel] != "POD" &&
		labels[source.ContainerLabel] != "" &&
		labels[source.NodeLabel] != "" &&
		labels[source.UIDLabel] != ""
}

func (cmas *ContainerMemoryAllocationSynthesizer) isValidUsage(labels map[string]string) bool {
	return labels[source.ContainerLabel] != "POD" &&
		labels[source.ContainerLabel] != "" &&
		labels[source.UIDLabel] != ""
}
