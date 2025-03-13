package collector

// avg(
// 		avg_over_time(
// 			container_memory_working_set_bytes{
// 				container!="",
// 				container!="POD",
// 				<some_custom_filter>
// 			}[1h]
// 		)
// ) by (container, pod, namespace, instance, cluster_id)

func NewRAMUsageAverageMetricInstance() *MetricCollector {
	return NewMetricCollector(
		RAMUsageAverageID,
		ContainerMemoryWorkingSetBytes,
		[]string{"container", "uid", "pod", "namespace", "instance", "node", "cluster"},
		AverageOverTime,
	)
}
