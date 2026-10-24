package synthetic

import (
	"maps"

	"github.com/opencost/opencost/core/pkg/source"
)

// mergeAllocationLabels fills missing or empty usage labels from the matching
// request, preserving nonempty usage values. The returned map is independent of
// both inputs and excludes resource and unit labels from the allocation metric.
func mergeAllocationLabels(request, usage map[string]string) map[string]string {
	labels := maps.Clone(usage)
	if labels == nil {
		labels = make(map[string]string, len(request))
	}
	for key, value := range request {
		if labels[key] == "" {
			labels[key] = value
		}
	}
	delete(labels, source.ResourceLabel)
	delete(labels, source.UnitLabel)
	return labels
}
