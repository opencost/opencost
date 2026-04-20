package metrics

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	coreutil "github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

//--------------------------------------------------------------------------
//  KubeModelCollector
//--------------------------------------------------------------------------

// kubeModelMetricNames lists every metric name emitted by KubeModelCollector.
// These are checked against the disabled-metrics map in Describe/Collect.
var kubeModelMetricNames = []string{
	"node_info",
	"cluster_info",
	"pod_info",
	"pod_pvc_volume",
	"namespace_info",
	"deployment_info",
	"deployment_labels",
	"deployment_annotations",
	"statefulset_info",
	"statefulset_labels",
	"statefulset_annotations",
	"daemonset_info",
	"daemonset_labels",
	"daemonset_annotations",
	"job_info",
	"job_labels",
	"job_annotations",
	"cronjob_info",
	"cronjob_labels",
	"cronjob_annotations",
	"replicaset_info",
	"replicaset_labels",
	"replicaset_annotations",
	"resourcequota_info",
}

// KubeModelCollector emits a unified set of info/labels/annotations metrics for
// all Kubernetes resource types. It mirrors the collector-source ClusterCacheScraper:
// indexes are built once per Collect call and per-resource scrapes run concurrently.
type KubeModelCollector struct {
	KubeClusterCache clustercache.ClusterCache
	ClusterInfo      clusters.ClusterInfoProvider
	metricsConfig    MetricsConfig
}

// Describe sends a generic descriptor for each metric emitted by this collector.
func (c KubeModelCollector) Describe(ch chan<- *prometheus.Desc) {
	disabled := c.metricsConfig.GetDisabledMetricsMap()
	for _, name := range kubeModelMetricNames {
		if _, ok := disabled[name]; ok {
			continue
		}
		ch <- prometheus.NewDesc(name, name, []string{}, nil)
	}
}

// Collect fetches all cluster resources, builds cross-reference indexes, then
// emits info/labels/annotations metrics concurrently per resource type.
func (c KubeModelCollector) Collect(ch chan<- prometheus.Metric) {
	disabled := c.metricsConfig.GetDisabledMetricsMap()

	// Fetch all resources from the cache up front.
	nodes := c.KubeClusterCache.GetAllNodes()
	namespaces := c.KubeClusterCache.GetAllNamespaces()
	pods := c.KubeClusterCache.GetAllPods()
	pvcs := c.KubeClusterCache.GetAllPersistentVolumeClaims()
	deployments := c.KubeClusterCache.GetAllDeployments()
	statefulSets := c.KubeClusterCache.GetAllStatefulSets()
	daemonSets := c.KubeClusterCache.GetAllDaemonSets()
	jobs := c.KubeClusterCache.GetAllJobs()
	cronJobs := c.KubeClusterCache.GetAllCronJobs()
	replicaSets := c.KubeClusterCache.GetAllReplicaSets()
	resourceQuotas := c.KubeClusterCache.GetAllResourceQuotas()

	// Build cross-reference indexes.
	nsIndex := make(map[string]types.UID, len(namespaces))
	for _, ns := range namespaces {
		nsIndex[ns.Name] = ns.UID
	}
	nodeIndex := make(map[string]types.UID, len(nodes))
	for _, node := range nodes {
		nodeIndex[node.Name] = node.UID
	}
	pvcIndex := make(map[string]types.UID, len(pvcs))
	for _, pvc := range pvcs {
		pvcIndex[pvcIndexKey(pvc.Namespace, pvc.Name)] = pvc.UID
	}

	// Collect concurrently using a channel.
	type scrapeFn func() []kubeModelMetric
	fns := []scrapeFn{
		func() []kubeModelMetric { return c.scrapeClusterInfo(disabled) },
		func() []kubeModelMetric { return c.scrapeNodes(nodes, disabled) },
		func() []kubeModelMetric { return c.scrapeNamespaces(namespaces, disabled) },
		func() []kubeModelMetric { return c.scrapePods(pods, nsIndex, nodeIndex, pvcIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeDeployments(deployments, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeStatefulSets(statefulSets, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeDaemonSets(daemonSets, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeJobs(jobs, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeCronJobs(cronJobs, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeReplicaSets(replicaSets, nsIndex, disabled) },
		func() []kubeModelMetric { return c.scrapeResourceQuotas(resourceQuotas, nsIndex, disabled) },
	}

	results := make(chan []kubeModelMetric, len(fns))
	for _, fn := range fns {
		fn := fn
		go func() { results <- fn() }()
	}
	for range fns {
		for _, m := range <-results {
			ch <- m
		}
	}
}

// pvcIndexKey returns a map key for a PVC by namespace+name.
func pvcIndexKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

//--------------------------------------------------------------------------
//  kubeModelMetric — generic prometheus.Metric for kube-model emissions
//--------------------------------------------------------------------------

// kubeModelMetric implements prometheus.Metric for any kube-model info/labels metric.
// All labels are stored in a map and emitted via Write; the gauge value defaults to 1.
type kubeModelMetric struct {
	name   string
	help   string
	labels map[string]string
	value  float64
}

func newInfoMetric(name string, labels map[string]string) kubeModelMetric {
	return kubeModelMetric{name: name, help: name, labels: labels, value: 1}
}

func newValueMetric(name string, labels map[string]string, value float64) kubeModelMetric {
	return kubeModelMetric{name: name, help: name, labels: labels, value: value}
}

func (m kubeModelMetric) Desc() *prometheus.Desc {
	return prometheus.NewDesc(m.name, m.help, promutil.LabelNamesFrom(m.labels), prometheus.Labels{})
}

func (m kubeModelMetric) Write(pb *dto.Metric) error {
	pb.Gauge = &dto.Gauge{Value: &m.value}
	pairs := make([]*dto.LabelPair, 0, len(m.labels))
	for k, v := range m.labels {
		pairs = append(pairs, &dto.LabelPair{
			Name:  toStringPtr(k),
			Value: toStringPtr(v),
		})
	}
	pb.Label = pairs
	return nil
}

//--------------------------------------------------------------------------
//  Per-resource scrape helpers
//--------------------------------------------------------------------------

func (c KubeModelCollector) scrapeClusterInfo(disabled map[string]struct{}) []kubeModelMetric {
	if _, ok := disabled["cluster_info"]; ok {
		return nil
	}
	if c.ClusterInfo == nil {
		return nil
	}
	info := c.ClusterInfo.GetClusterInfo()
	labels := map[string]string{
		"uid":              info[clusters.ClusterInfoIdKey],
		"provider":         info[clusters.ClusterInfoProviderKey],
		"account_id":       info[clusters.ClusterInfoAccountKey],
		"provisioner_name": info[clusters.ClusterInfoProvisionerKey],
		"region":           info[clusters.ClusterInfoRegionKey],
	}
	// GCP uses "project" instead of "account"
	if labels["account_id"] == "" {
		labels["account_id"] = info[clusters.ClusterInfoProjectKey]
	}
	return []kubeModelMetric{newInfoMetric("cluster_info", labels)}
}

func (c KubeModelCollector) scrapeNodes(nodes []*clustercache.Node, disabled map[string]struct{}) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "node_info")

	for _, node := range nodes {
		nodeInfo := map[string]string{
			"node":        node.Name,
			"uid":         string(node.UID),
			"provider_id": node.SpecProviderID,
		}
		if instanceType, ok := coreutil.GetInstanceType(node.Labels); ok {
			nodeInfo["instance_type"] = instanceType
		}

		if emitInfo {
			out = append(out, newInfoMetric("node_info", nodeInfo))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeNamespaces(namespaces []*clustercache.Namespace, disabled map[string]struct{}) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "namespace_info")

	for _, ns := range namespaces {
		if emitInfo {
			out = append(out, newInfoMetric("namespace_info", map[string]string{
				"uid":       string(ns.UID),
				"namespace": ns.Name,
			}))
		}
	}
	return out
}

func (c KubeModelCollector) scrapePods(
	pods []*clustercache.Pod,
	nsIndex map[string]types.UID,
	nodeIndex map[string]types.UID,
	pvcIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "pod_info")
	emitPVC := !isDisabled(disabled, "pod_pvc_volume")

	for _, pod := range pods {
		nsUID, ok := nsIndex[pod.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: pod namespace uid missing for namespace '%s'", pod.Namespace)
		}
		nodeUID, ok := nodeIndex[pod.Spec.NodeName]
		if !ok && pod.Spec.NodeName != "" {
			log.Debugf("KubeModelCollector: pod node uid missing for node '%s'", pod.Spec.NodeName)
		}

		if emitInfo {
			out = append(out, newInfoMetric("pod_info", map[string]string{
				"uid":           string(pod.UID),
				"pod":           pod.Name,
				"namespace_uid": string(nsUID),
				"node_uid":      string(nodeUID),
			}))
		}

		if emitPVC {
			for _, vol := range pod.Spec.Volumes {
				if vol.PersistentVolumeClaim == nil {
					continue
				}
				pvcUID := pvcIndex[pvcIndexKey(pod.Namespace, vol.PersistentVolumeClaim.ClaimName)]
				out = append(out, newInfoMetric("pod_pvc_volume", map[string]string{
					"uid":                       string(pod.UID),
					"persistentvolumeclaim_uid": string(pvcUID),
					"pod_volume_name":           vol.Name,
				}))
			}
		}
	}
	return out
}

func (c KubeModelCollector) scrapeDeployments(
	deployments []*clustercache.Deployment,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "deployment_info")
	emitLabels := !isDisabled(disabled, "deployment_labels")
	emitAnno := !isDisabled(disabled, "deployment_annotations")

	for _, d := range deployments {
		nsUID, ok := nsIndex[d.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: deployment namespace uid missing for namespace '%s'", d.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("deployment_info", map[string]string{
				"uid":           string(d.UID),
				"namespace_uid": string(nsUID),
				"deployment":    d.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("deployment_labels", string(d.UID), d.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("deployment_annotations", string(d.UID), d.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeStatefulSets(
	sets []*clustercache.StatefulSet,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "statefulset_info")
	emitLabels := !isDisabled(disabled, "statefulset_labels")
	emitAnno := !isDisabled(disabled, "statefulset_annotations")

	for _, s := range sets {
		nsUID, ok := nsIndex[s.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: statefulset namespace uid missing for namespace '%s'", s.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("statefulset_info", map[string]string{
				"uid":           string(s.UID),
				"namespace_uid": string(nsUID),
				"statefulSet":   s.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("statefulset_labels", string(s.UID), s.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("statefulset_annotations", string(s.UID), s.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeDaemonSets(
	sets []*clustercache.DaemonSet,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "daemonset_info")
	emitLabels := !isDisabled(disabled, "daemonset_labels")
	emitAnno := !isDisabled(disabled, "daemonset_annotations")

	for _, ds := range sets {
		nsUID, ok := nsIndex[ds.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: daemonset namespace uid missing for namespace '%s'", ds.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("daemonset_info", map[string]string{
				"uid":           string(ds.UID),
				"namespace_uid": string(nsUID),
				"daemonset":     ds.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("daemonset_labels", string(ds.UID), ds.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("daemonset_annotations", string(ds.UID), ds.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeJobs(
	jobs []*clustercache.Job,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "job_info")
	emitLabels := !isDisabled(disabled, "job_labels")
	emitAnno := !isDisabled(disabled, "job_annotations")

	for _, j := range jobs {
		nsUID, ok := nsIndex[j.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: job namespace uid missing for namespace '%s'", j.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("job_info", map[string]string{
				"uid":           string(j.UID),
				"namespace_uid": string(nsUID),
				"job":           j.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("job_labels", string(j.UID), j.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("job_annotations", string(j.UID), j.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeCronJobs(
	cronJobs []*clustercache.CronJob,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "cronjob_info")
	emitLabels := !isDisabled(disabled, "cronjob_labels")
	emitAnno := !isDisabled(disabled, "cronjob_annotations")

	for _, cj := range cronJobs {
		nsUID, ok := nsIndex[cj.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: cronjob namespace uid missing for namespace '%s'", cj.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("cronjob_info", map[string]string{
				"uid":           string(cj.UID),
				"namespace_uid": string(nsUID),
				"cronjob":       cj.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("cronjob_labels", string(cj.UID), cj.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("cronjob_annotations", string(cj.UID), cj.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeReplicaSets(
	sets []*clustercache.ReplicaSet,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	var out []kubeModelMetric
	emitInfo := !isDisabled(disabled, "replicaset_info")
	emitLabels := !isDisabled(disabled, "replicaset_labels")
	emitAnno := !isDisabled(disabled, "replicaset_annotations")

	for _, rs := range sets {
		nsUID, ok := nsIndex[rs.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: replicaset namespace uid missing for namespace '%s'", rs.Namespace)
		}

		if emitInfo {
			out = append(out, newInfoMetric("replicaset_info", map[string]string{
				"uid":           string(rs.UID),
				"namespace_uid": string(nsUID),
				"replicaset":    rs.Name,
			}))
		}
		if emitLabels {
			out = append(out, kubeLabelsMetric("replicaset_labels", string(rs.UID), rs.Labels))
		}
		if emitAnno {
			out = append(out, kubeAnnotationsMetric("replicaset_annotations", string(rs.UID), rs.Annotations))
		}
	}
	return out
}

func (c KubeModelCollector) scrapeResourceQuotas(
	quotas []*clustercache.ResourceQuota,
	nsIndex map[string]types.UID,
	disabled map[string]struct{},
) []kubeModelMetric {
	if isDisabled(disabled, "resourcequota_info") {
		return nil
	}
	var out []kubeModelMetric
	for _, rq := range quotas {
		nsUID, ok := nsIndex[rq.Namespace]
		if !ok {
			log.Debugf("KubeModelCollector: resourcequota namespace uid missing for namespace '%s'", rq.Namespace)
		}
		out = append(out, newInfoMetric("resourcequota_info", map[string]string{
			"uid":           string(rq.UID),
			"namespace_uid": string(nsUID),
			"resourcequota": rq.Name,
		}))
	}
	return out
}

//--------------------------------------------------------------------------
//  Helpers
//--------------------------------------------------------------------------

// isDisabled returns true if the named metric appears in the disabled map.
func isDisabled(disabled map[string]struct{}, name string) bool {
	_, ok := disabled[name]
	return ok
}

// kubeLabelsMetric builds a labels metric for a resource, adding the resource
// uid as a fixed label alongside the k8s labels (prefixed with "label_").
func kubeLabelsMetric(name, uid string, k8sLabels map[string]string) kubeModelMetric {
	labelNames, labelValues := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(k8sLabels))
	m := make(map[string]string, len(labelNames)+1)
	m["uid"] = uid
	for i, k := range labelNames {
		m[k] = labelValues[i]
	}
	return newInfoMetric(name, m)
}

// kubeAnnotationsMetric builds an annotations metric for a resource.
func kubeAnnotationsMetric(name, uid string, k8sAnnotations map[string]string) kubeModelMetric {
	annoNames, annoValues := promutil.KubeAnnotationsToLabels(k8sAnnotations)
	m := make(map[string]string, len(annoNames)+1)
	m["uid"] = uid
	for i, k := range annoNames {
		m[k] = annoValues[i]
	}
	return newInfoMetric(name, m)
}

// kubeModelResourceValue converts a Kubernetes resource quantity to a float64 value.
// It mirrors the collector-source toResourceUnitValue logic for the cases we need.
func kubeModelResourceValue(resourceName v1.ResourceName, quantity resource.Quantity) float64 {
	switch resourceName {
	case v1.ResourceCPU:
		return float64(quantity.MilliValue()) / 1000.0
	default:
		return float64(quantity.Value())
	}
}
