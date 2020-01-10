package costmodel

import (
	"regexp"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	invalidLabelCharRE = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

func kubeLabelsToPrometheusLabels(labels map[string]string) ([]string, []string) {
	labelKeys := make([]string, 0, len(labels))
	for k := range labels {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)

	labelValues := make([]string, 0, len(labels))
	for i, k := range labelKeys {
		labelKeys[i] = "label_" + sanitizeLabelName(k)
		labelValues = append(labelValues, labels[k])
	}
	return labelKeys, labelValues
}

func sanitizeLabelName(s string) string {
	return invalidLabelCharRE.ReplaceAllString(s, "_")
}

type StatefulsetCollector struct {
	KubeClientSet kubernetes.Interface
}

func (sc StatefulsetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("statefulSet_match_labels", "statfulSet match labels", []string{}, nil)
}

type DeploymentCollector struct {
	KubeClientSet kubernetes.Interface
}

func (sc DeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewInvalidDesc(nil)
}

func newStatefulsetMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) StatefulsetMetric {
	return StatefulsetMetric{
		fqName:          fqname,
		labelNames:      labelNames,
		labelValues:     labelvalues,
		help:            "statefulSet_match_labels StatefulSet Match Labels",
		statefulsetName: name,
		namespace:       namespace,
	}
}

type StatefulsetMetric struct {
	fqName          string
	help            string
	labelNames      []string
	labelValues     []string
	statefulsetName string
	namespace       string
}

func (s StatefulsetMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"statefulSet": s.statefulsetName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

func (s StatefulsetMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "statefulSet"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.statefulsetName,
	})
	m.Label = labels
	return nil
}

func (sc StatefulsetCollector) Collect(ch chan<- prometheus.Metric) {
	ds, _ := sc.KubeClientSet.AppsV1().StatefulSets("").List(metav1.ListOptions{})
	for _, statefulset := range ds.Items {
		labels, values := kubeLabelsToPrometheusLabels(statefulset.Spec.Selector.MatchLabels)
		m := newStatefulsetMetric(statefulset.GetName(), statefulset.GetNamespace(), "statefulSet_match_labels", labels, values)
		ch <- m
	}
}

func newDeploymentMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) DeploymentMetric {
	return DeploymentMetric{
		fqName:         fqname,
		labelNames:     labelNames,
		labelValues:    labelvalues,
		help:           "deployment_match_labels Deployment Match Labels",
		deploymentName: name,
		namespace:      namespace,
	}
}

type DeploymentMetric struct {
	fqName         string
	help           string
	labelNames     []string
	labelValues    []string
	deploymentName string
	namespace      string
}

func (s DeploymentMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"deployment": s.deploymentName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

func (s DeploymentMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "deployment"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.deploymentName,
	})
	m.Label = labels
	return nil
}

func (sc DeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	ds, _ := sc.KubeClientSet.AppsV1().Deployments("").List(metav1.ListOptions{})
	for _, deployment := range ds.Items {
		labels, values := kubeLabelsToPrometheusLabels(deployment.Spec.Selector.MatchLabels)
		m := newDeploymentMetric(deployment.GetName(), deployment.GetNamespace(), "deployment_match_labels", labels, values)
		ch <- m
	}
}

type ServiceCollector struct {
	KubeClientSet kubernetes.Interface
}

func (sc ServiceCollector) Describe(ch chan<- *prometheus.Desc) {
	return
}

func newServiceMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) ServiceMetric {
	return ServiceMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelvalues,
		help:        "service_selector_labels Service Selector Labels",
		serviceName: name,
		namespace:   namespace,
	}
}

type ServiceMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	serviceName string
	namespace   string
}

func (s ServiceMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"service": s.serviceName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

func (s ServiceMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "service"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.serviceName,
	})
	m.Label = labels
	return nil
}

func (sc ServiceCollector) Collect(ch chan<- prometheus.Metric) {
	svcs, _ := sc.KubeClientSet.CoreV1().Services("").List(metav1.ListOptions{})
	for _, svc := range svcs.Items {
		labels, values := kubeLabelsToPrometheusLabels(svc.Spec.Selector)
		m := newServiceMetric(svc.GetName(), svc.GetNamespace(), "service_selector_labels", labels, values)
		ch <- m
	}
}
