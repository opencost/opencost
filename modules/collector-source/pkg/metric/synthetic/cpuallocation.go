package synthetic

import (
	"maps"
	"math"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// CpuUsageMetric contains the last two samples of a CPU instant metric.
type CpuUsageMetric struct {
	current *InstantMetric
	prev    *InstantMetric
}

// NewCpuUsageMetric creates a new cpu usage metric initialized to the provided instant metric
// data.
func NewCpuUsageMetric(t time.Time, m *metric.Update) *CpuUsageMetric {
	return new(CpuUsageMetric).Push(t, m)
}

// Push accepts new instant metric data, advances any current data to previous, and sets the new
// current to the provided metric.
func (usage *CpuUsageMetric) Push(t time.Time, m *metric.Update) *CpuUsageMetric {
	if usage.current == nil {
		usage.current = &InstantMetric{t, m}
		return usage
	}

	usage.prev = usage.current
	usage.current = &InstantMetric{t, m}
	return usage
}

// Labels returns the labels for any current if it exists first, then looks to any previous data next.
func (usage *CpuUsageMetric) Labels() map[string]string {
	if usage.current != nil {
		return usage.current.update.Labels
	}
	if usage.prev != nil {
		return usage.prev.update.Labels
	}

	return map[string]string{}
}

// IsValid returns true when usage is non-nil, the current instant metric is non-nil, and the previous
// instant metric is non-nil
func (usage *CpuUsageMetric) IsValid() bool {
	return usage != nil && usage.current != nil && usage.prev != nil
}

// IsEmpty returns true when there are no valid samples
func (usage *CpuUsageMetric) IsEmpty() bool {
	return usage == nil || (usage.current == nil && usage.prev == nil)
}

// Value returns the irate of the two metric samples if they exist, and 0 if they don't.
func (usage *CpuUsageMetric) Value() float64 {
	if usage.current == nil || usage.prev == nil {
		return 0.0
	}

	v1, t1 := usage.current.update.Value, usage.current.timestamp
	v2, t2 := usage.prev.update.Value, usage.prev.timestamp
	seconds := t1.Sub(t2).Seconds()
	if seconds <= 0.0 {
		return 0.0
	}

	irate := (v1 - v2) / seconds
	return irate
}

// Shift will set the previous to the current metric, and set the current metric to nil.
func (usage *CpuUsageMetric) Shift() {
	if usage == nil {
		return
	}

	usage.prev = usage.current
	usage.current = nil
}

// ContainerCpuAllocationMetric is the grouping unit for cpu usage and cpu request metrics.
type ContainerCpuAllocationMetric struct {
	requestMetric *metric.Update
	usageMetric   *CpuUsageMetric
}

// IsValid returns true if we can synthesize an update from the samples available
func (cmam *ContainerCpuAllocationMetric) IsValid() bool {
	return cmam.requestMetric != nil || cmam.usageMetric.IsValid()
}

// Synthesize returns a new CpuAllocation metric update with the max(request, usage)
func (cmam *ContainerCpuAllocationMetric) Synthesize() metric.Update {
	if cmam.requestMetric != nil && cmam.usageMetric.IsValid() {
		req := cmam.requestMetric.Value
		if math.IsNaN(req) {
			log.Debugf("NaN value found during cpu allocation synthesis for requests.")
			req = 0.0
		}

		used := cmam.usageMetric.Value()
		if math.IsNaN(used) {
			log.Debugf("NaN value found during cpu allocation synthesis for used.")
			used = 0.0
		}

		// TODO: validate and merge labels if they both have keys?
		labels := maps.Clone(cmam.usageMetric.Labels())

		return metric.Update{
			Name:   metric.ContainerCPUAllocation,
			Labels: labels,
			Value:  max(req, used),
		}
	} else if cmam.requestMetric != nil {
		req := cmam.requestMetric.Value
		if math.IsNaN(req) {
			log.Debugf("NaN value found during cpu allocation synthesis for requests.")
			req = 0.0
		}

		// drop the "extra" labels
		labels := maps.Clone(cmam.requestMetric.Labels)
		delete(labels, source.ResourceLabel)
		delete(labels, source.UnitLabel)

		return metric.Update{
			Name:   metric.ContainerCPUAllocation,
			Labels: labels,
			Value:  req,
		}
	}

	// not possible for both request and usage to be nil, so we can assume only used is
	// valid here
	used := cmam.usageMetric.Value()
	if math.IsNaN(used) {
		log.Debugf("NaN value found during cpu allocation synthesis for used.")
		used = 0.0
	}

	labels := maps.Clone(cmam.usageMetric.Labels())

	return metric.Update{
		Name:   metric.ContainerCPUAllocation,
		Labels: labels,
		Value:  used,
	}
}

// IsEmpty returns true if there are no valid samples to extract from
func (cmam *ContainerCpuAllocationMetric) IsEmpty() bool {
	return cmam.requestMetric == nil && cmam.usageMetric.IsEmpty()
}

// Cycle will advance the usage sample buffer and clear the request sample.
func (cmam *ContainerCpuAllocationMetric) Cycle() {
	cmam.requestMetric = nil
	cmam.usageMetric.Shift()
}

// ContainerCpuAllocationSynthesizer is a MetricSynthesizer that leverages pod uid and container name grouping
// to match relevant request and usage metrics to build the cpu allocation data.
type ContainerCpuAllocationSynthesizer struct {
	byPod map[string]map[string]*ContainerCpuAllocationMetric
}

// NewContainerCpuAllocationSynthesizer creates a new ContainerCpuAllocationSynthesizer which synthesizes
// metric updates for ContainerCPUAllocation from cpu requests and cpu usage metrics.
func NewContainerCpuAllocationSynthesizer() *ContainerCpuAllocationSynthesizer {
	return &ContainerCpuAllocationSynthesizer{
		byPod: make(map[string]map[string]*ContainerCpuAllocationMetric),
	}
}

// Process only processes cpu requests and cpu usage metrics
func (cmas *ContainerCpuAllocationSynthesizer) Process(t time.Time, update *metric.Update) {
	switch update.Name {
	case metric.KubePodContainerResourceRequests:
		cmas.addRequestsMetric(update)
	case metric.ContainerCPUUsageSecondsTotal:
		cmas.addUsageMetric(t, update)
	}
}

// Synthesize will synthesize all valid synthesizers within the pod/container mapping.
func (cmas *ContainerCpuAllocationSynthesizer) Synthesize() []metric.Update {
	var updates []metric.Update

	for _, pod := range cmas.byPod {
		for _, synthesizer := range pod {
			isValid := synthesizer.IsValid()
			if isValid {
				updates = append(updates, synthesizer.Synthesize())
			}
		}
	}

	return updates
}

// Clear for the CpuAllocationSynthesis must cycle the samples, and only remove them if there is no
// more valid sample data remaining.
func (cmas *ContainerCpuAllocationSynthesizer) Clear() {
	for podKey, pod := range cmas.byPod {
		for synthKey, synthesizer := range pod {
			synthesizer.Cycle()
			if synthesizer.IsEmpty() {
				delete(pod, synthKey)
			}
		}
		if len(pod) == 0 {
			delete(cmas.byPod, podKey)
		}
	}
}

func (cmas *ContainerCpuAllocationSynthesizer) addRequestsMetric(update *metric.Update) {
	if !cmas.isValidRequests(update.Labels) {
		return
	}

	podUID := update.Labels[source.UIDLabel]
	container := update.Labels[source.ContainerLabel]
	if _, ok := cmas.byPod[podUID]; !ok {
		cmas.byPod[podUID] = make(map[string]*ContainerCpuAllocationMetric)
	}

	if _, ok := cmas.byPod[podUID][container]; !ok {
		cmas.byPod[podUID][container] = &ContainerCpuAllocationMetric{
			requestMetric: update,
		}
	} else {
		cmas.byPod[podUID][container].requestMetric = update
	}
}

func (cmas *ContainerCpuAllocationSynthesizer) addUsageMetric(t time.Time, update *metric.Update) {
	if !cmas.isValidUsage(update.Labels) {
		return
	}

	podUID := update.Labels[source.UIDLabel]
	container := update.Labels[source.ContainerLabel]
	if _, ok := cmas.byPod[podUID]; !ok {
		cmas.byPod[podUID] = make(map[string]*ContainerCpuAllocationMetric)
	}

	if _, ok := cmas.byPod[podUID][container]; !ok {
		cmas.byPod[podUID][container] = &ContainerCpuAllocationMetric{
			usageMetric: NewCpuUsageMetric(t, update),
		}
	} else {
		cpuAllocMetric := cmas.byPod[podUID][container]
		if cpuAllocMetric.usageMetric == nil {
			cpuAllocMetric.usageMetric = NewCpuUsageMetric(t, update)
		} else {
			cpuAllocMetric.usageMetric.Push(t, update)
		}
	}
}

func (cmas *ContainerCpuAllocationSynthesizer) isValidRequests(labels map[string]string) bool {
	return labels[source.ResourceLabel] == "cpu" &&
		labels[source.UnitLabel] == "core" &&
		labels[source.ContainerLabel] != "POD" &&
		labels[source.ContainerLabel] != "" &&
		labels[source.NodeLabel] != "" &&
		labels[source.UIDLabel] != ""
}

func (cmas *ContainerCpuAllocationSynthesizer) isValidUsage(labels map[string]string) bool {
	return labels[source.ContainerLabel] != "POD" &&
		labels[source.ContainerLabel] != "" &&
		labels[source.UIDLabel] != ""
}
