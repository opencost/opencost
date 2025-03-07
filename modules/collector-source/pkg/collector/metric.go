package collector

import (
	"hash/fnv"
	"strings"
	"time"
)

type Metric interface {
	Name() string
	Update(value float64)
	Value() float64
}

type AverageOverTimeMetric struct {
	name  string
	total float64
	count int
}

func NewAverageOverTimeMetric(name string) *AverageOverTimeMetric {
	return &AverageOverTimeMetric{
		name: name,
	}
}

func (m *AverageOverTimeMetric) Name() string {
	return m.name
}

func (m *AverageOverTimeMetric) Update(value float64) {
	m.total += value
	m.count++
}

func (m *AverageOverTimeMetric) Value() float64 {
	return m.total / float64(m.count)
}

// avg(
// 		avg_over_time(
// 			container_memory_working_set_bytes{
// 				container!="",
// 				container!="POD",
// 				<some_custom_filter>
// 			}[1h]
// 		)
// ) by (container, pod, namespace, instance, cluster_id)

type ContainerWorkingSetBytes struct {
	name    string
	labels  []string
	metrics map[uint64]*AverageOverTimeMetric
}

func NewContainerWorkingSetBytes() *ContainerWorkingSetBytes {
	return &ContainerWorkingSetBytes{
		name:    "container_memory_working_set_bytes",
		labels:  []string{"container", "uid", "pod", "namespace", "instance", "node", "cluster"},
		metrics: make(map[uint64]*AverageOverTimeMetric),
	}
}

// We could also be way more generic than this and do something like: Update(labelValues []string, value float64)
func (cwsb *ContainerWorkingSetBytes) Update(container, uid, pod, namespace, instance, node, cluster string, value float64) {
	// hash key
	key := hash([]string{container, uid, pod, namespace, instance, node, cluster})
	if cwsb.metrics[key] == nil {
		cwsb.metrics[key] = NewAverageOverTimeMetric(metricNameFor(cwsb.name, cwsb.labels, []string{container, uid, pod, namespace, instance, node, cluster}))
	}
	cwsb.metrics[key].Update(value)
}

type NamedMetric struct {
	name    string // RAMUsageAvg
	buckets map[uint64]*ContainerWorkingSetBytes
}

func (nm *NamedMetric) Update(container, uid, pod, namespace, instance, node, cluster string, value float64) {
	bucket := time.Now().Unix()

}

func (nm *NamedMetric) Value() float64 {
	return 0
}

func hash(s []string) uint64 {
	h := fnv.New64a()
	for _, v := range s {
		h.Write([]byte(v))
	}
	return h.Sum64()
}

func metricNameFor(metric string, labels []string, values []string) string {
	var sb strings.Builder
	sb.WriteString(metric)
	sb.WriteRune('{')
	for i := 0; i < len(labels); i++ {
		sb.WriteRune('"')
		sb.WriteString(labels[i])
		sb.WriteString(`"="`)
		sb.WriteString(values[i])
		sb.WriteRune('"')
		if i < len(labels)-1 {
			sb.WriteRune(',')
		}
	}
	sb.WriteRune('}')
	return sb.String()
}
